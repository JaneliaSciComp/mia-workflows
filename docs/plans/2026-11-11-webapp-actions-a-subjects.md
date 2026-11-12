# Webapp actions, slice A: subjects — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** buffer-api knows every dataset, buffer spec and preparation request as a *subject* with a stage, a status
word and the actions that stage accepts; existing GitHub issues are adopted as subjects; nothing behaves differently.

**Architecture:** One new table in the ledger (`subjects`), one declared stage table per kind beside each
package's `STAGES`, four read routes and one worker route on buffer-api, an in-process change feed served as
server-sent events, and a one-shot `mia-buffers adopt-issues` command that reads both issue repos and writes
subjects through the registry client. No workflow, activity, poller or page file changes in this slice.

**Tech Stack:** Python 3.12, stdlib `sqlite3` and `http.server`, pydantic, httpx (client and test transport),
pytest. No new dependency.

**Spec:** `docs/specs/2026-11-11-webapp-actions.md` (v0.5): §4.1 subjects, §4.4 routes, §5.1 stage tables, §8.2
needs, §8.4 lookup, §11 step 1 adoption, §14 row A. Later slices: B timeline and mirror, C datasets from the page
(actions table, clock-derived ids, the `action` capability), D buffers and requests, E GitHub input removed.

## Global Constraints

- Nothing behaves differently: do not touch `*/workflows.py`, `*/activities.py`, `*/poll.py` or
  `mia_buffers/ui/index.html`. The replay tests must pass untouched.
- Every `PUT`/`POST`/`DELETE` added here is a `worker` route (bearer token); every `GET` is `read`. Workers call
  the service through `mia_core.registry.Registry`, never a bare httpx call.
- buffer-api deletes nothing and holds no GitHub credential: adoption runs in the `mia-buffers` command, not in
  the service.
- No real paths or hostnames in tracked files: the check in `AGENTS.md` ("Rules that will bite you") is empty.
- Style: descriptive names (no single-letter attributes), a plain-language line on each module, class and
  non-obvious method, type hints on object parameters, line length 120, `# ponytail:` on deliberate shortcuts.
- `mia_core` imports none of the other packages; `mia_buffers` may import `mia_data`.
- Before the last commit: `pixi run fmt`, `pixi run lint`, `pixi run typecheck`, `pixi run test` are clean.
- Commit after each task, explicit paths in `git add`, message ending with
  `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>`. Never push.

## Review Focus

1. Two issues name one record: the one the dataset's registry row names is adopted (else the lowest number), the
   other is reported with its stage and gets no subject. Test in Task 6.
2. A buffer spec name with spaces, a slash or non-ASCII letters is a key: it must round-trip through
   `GET /subjects/buffer/<quoted key>`. Test in Task 4.
3. `adopt-issues` is run again after a workflow has written a stage: the stage must not roll back to the label.
   Test in Task 2.
4. A reader of `/stream` disconnects or falls far behind: no error, the service keeps answering, memory stays
   bounded. Tests in Task 3.
5. An issue with `body: null`, or a record marker that is not at the start of the body, is skipped, not a crash
   and not a subject. Test in Task 6.

## File Structure

| file | responsibility |
|---|---|
| `mia_data/config.py` (modify) | `STATUS_WORDS`, the dataset `STAGE_ACTIONS` |
| `mia_buffers/config.py` (modify) | the buffer `STAGE_ACTIONS` and `PREP_STAGE_ACTIONS` |
| `mia_buffers/ledger.py` (modify) | the `subjects` table and its four methods |
| `mia_buffers/stream.py` (create) | `Changes`: what changed, as a bounded feed readers can follow |
| `mia_buffers/api.py` (modify) | the subject routes, `/needs`, `/lookup`, `/stream`; streaming responses in `make_server` |
| `mia_core/registry.py` (modify) | `adopt_subject`, `subject` |
| `mia_buffers/adopt.py` (create) | `adopt_issues`: both issue repos to subjects, with a report |
| `mia_buffers/cli.py` (modify) | the `adopt-issues` command |
| `docs/deployment.md` (modify) | when and how to run it |

---

### Task 1: The stage tables

**Files:**
- Modify: `mia_data/config.py` (after `HUMAN_LABELS`), `mia_buffers/config.py` (after `PREP_HUMAN_LABELS`)
- Test: `tests/test_mia_config.py`, `tests/test_buffers_config.py`

**Interfaces:**
- Produces: `mia_data.config.STATUS_WORDS: tuple[str, ...]`; `mia_data.config.STAGE_ACTIONS`,
  `mia_buffers.config.STAGE_ACTIONS`, `mia_buffers.config.PREP_STAGE_ACTIONS`, each a
  `dict[str, tuple[str, frozenset[str]]]`: stage to (status word, actions accepted).

- [ ] **Step 1: Write the failing tests**

`tests/test_mia_config.py`:

```python
def test_every_dataset_stage_has_a_rule():
    from mia_data.config import HUMAN_LABELS, STAGE_ACTIONS, STAGES, STATUS_WORDS

    assert set(STAGE_ACTIONS) == set(STAGES)
    for status, actions in STAGE_ACTIONS.values():
        assert status in STATUS_WORDS and actions <= set(HUMAN_LABELS)
    assert STAGE_ACTIONS["review"] == ("waiting", frozenset({"approve", "reject", "go"}))
    assert all(not actions for status, actions in STAGE_ACTIONS.values() if status == "working")
```

`tests/test_buffers_config.py`:

```python
def test_every_buffer_and_prep_stage_has_a_rule():
    from mia_buffers.config import HUMAN_LABELS, PREP_STAGE_ACTIONS, PREP_STAGES, STAGE_ACTIONS, STAGES
    from mia_buffers.poll import LIVE_STAGES
    from mia_data.config import STATUS_WORDS

    assert set(STAGE_ACTIONS) == set(STAGES) and set(PREP_STAGE_ACTIONS) == {"new", *PREP_STAGES}
    vocabulary = (set(HUMAN_LABELS) - {"plan"}) | {"dropped", "stop"}  # plan is an option of go (spec §4.2)
    for status, actions in [*STAGE_ACTIONS.values(), *PREP_STAGE_ACTIONS.values()]:
        assert status in STATUS_WORDS and actions <= vocabulary
    assert STAGE_ACTIONS["idle"] == ("waiting", frozenset({"keep", "retire", "go", "revoke", "stop"}))
    assert PREP_STAGE_ACTIONS["requested"] == ("working", frozenset({"stop"}))
    # stop is offered exactly where closing the issue retires today
    assert {stage for stage, (_, actions) in STAGE_ACTIONS.items() if "stop" in actions} == set(LIVE_STAGES)
```

- [ ] **Step 2: Run them, expect failure**

Run: `pixi run pytest tests/test_mia_config.py tests/test_buffers_config.py -k "has_a_rule" -q`
Expected: 2 failed, `ImportError: cannot import name 'STAGE_ACTIONS'`.

- [ ] **Step 3: Add the tables**

`mia_data/config.py`:

```python
STATUS_WORDS = ("not started", "working", "waiting", "stuck", "done")
# stage -> (status word, the actions it accepts); "working" means a round is in progress, and it accepts nothing
# until cancel is built (spec webapp-actions §5.1). The workflow stays the code that carries a round out.
STAGE_ACTIONS: dict[str, tuple[str, frozenset[str]]] = {
    "new": ("not started", frozenset({"go"})),
    "matching": ("working", frozenset()),
    "developing": ("working", frozenset()),
    "testing": ("working", frozenset()),
    "review": ("waiting", frozenset({"approve", "reject", "go"})),
    "merging": ("working", frozenset()),
    "done": ("done", frozenset({"go", "revoke"})),
    "needs-human": ("stuck", frozenset({"go", "revoke"})),
}
```

`mia_buffers/config.py` (same comment, naming §5.1):

```python
STAGE_ACTIONS: dict[str, tuple[str, frozenset[str]]] = {
    "new": ("not started", frozenset({"go"})),
    "planning": ("working", frozenset({"stop"})),
    "plan-review": ("waiting", frozenset({"approve", "go", "stop"})),
    "sourcing": ("working", frozenset({"dropped", "stop"})),
    "staged": ("waiting", frozenset({"approve", "go", "dropped", "stop"})),
    "ready": ("done", frozenset({"go", "revoke", "retire", "stop"})),
    "idle": ("waiting", frozenset({"keep", "retire", "go", "revoke", "stop"})),
    "retired": ("done", frozenset({"go"})),
    "needs-human": ("stuck", frozenset({"go"})),
}
PREP_STAGE_ACTIONS: dict[str, tuple[str, frozenset[str]]] = {
    "new": ("not started", frozenset({"go"})),  # a request has no `new` label: a subject before its first stage
    "requested": ("working", frozenset({"stop"})),
    "done": ("done", frozenset({"go"})),
    "needs-human": ("stuck", frozenset({"go"})),
}
```

- [ ] **Step 4: Run the tests, expect 2 passed**

- [ ] **Step 5: Commit** (`config: the actions each stage accepts, one table per kind`)

---

### Task 2: The `subjects` table

**Files:**
- Modify: `mia_buffers/ledger.py` (`SCHEMA`; a new section after the datasets methods)
- Test: `tests/test_buffers_ledger.py`

**Interfaces:**
- Produces, on `Ledger`:
  - `adopt_subject(self, kind: str, number: int, key: str, *, stage: str = "new", data: dict | None = None) -> dict`
  - `set_subject_stage(self, kind: str, number: int, stage: str) -> dict | None` (None: no such subject)
  - `subject(self, kind: str, key: str) -> dict | None`
  - `find_subjects(self, *, issue: int | None = None, key: str | None = None) -> list[dict]` (no filter: all;
    ordered by kind, then number)
  - A subject row is `{"kind", "number", "key", "stage", "revision", "issue", "data", "updated"}`.

- [ ] **Step 1: Write the failing tests**

```python
def test_adopt_subject_is_idempotent_and_keeps_a_written_stage(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    row = ledger.adopt_subject("dataset", 234, "rec-a", stage="review", data={"note": 1})
    assert {name: row[name] for name in ("kind", "number", "key", "stage", "revision", "issue", "data")} == {
        "kind": "dataset", "number": 234, "key": "rec-a", "stage": "review", "revision": 0, "issue": 234,
        "data": {"note": 1},
    }
    # until a workflow writes a stage, the label is the truth: a second adoption refreshes stage and data
    assert ledger.adopt_subject("dataset", 234, "rec-a", stage="done")["stage"] == "done"
    assert ledger.set_subject_stage("dataset", 234, "testing")["revision"] == 1
    again = ledger.adopt_subject("dataset", 234, "rec-a", stage="new", data={"note": 2})
    assert (again["stage"], again["revision"], again["data"]) == ("testing", 1, {})


def test_subject_keys_are_unique_within_a_kind(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.adopt_subject("buffer", 8, "em pool")
    with pytest.raises(ValueError, match="em pool"):
        ledger.adopt_subject("buffer", 9, "em pool")
    assert ledger.adopt_subject("prep", 9, "em pool")["number"] == 9  # another kind may hold the same key


def test_find_subjects_by_issue_and_key(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.adopt_subject("dataset", 234, "rec-a")
    ledger.adopt_subject("buffer", 234, "pool")
    assert [row["kind"] for row in ledger.find_subjects(issue=234)] == ["buffer", "dataset"]
    assert [row["number"] for row in ledger.find_subjects(key="pool")] == [234]
    assert len(ledger.find_subjects()) == 2
    assert ledger.subject("dataset", "rec-a")["number"] == 234 and ledger.subject("dataset", "nope") is None
    assert ledger.set_subject_stage("dataset", 999, "new") is None
```

- [ ] **Step 2: Run them, expect failure** (`AttributeError: 'Ledger' object has no attribute 'adopt_subject'`)

Run: `pixi run pytest tests/test_buffers_ledger.py -k subject -q`

- [ ] **Step 3: Implement**

Add to `SCHEMA`:

```sql
CREATE TABLE IF NOT EXISTS subjects (kind TEXT, number INTEGER, key TEXT NOT NULL, stage TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0, issue INTEGER, data TEXT NOT NULL, updated REAL,
    PRIMARY KEY (kind, number), UNIQUE (kind, key));
```

The four methods under a section comment `# --- subjects: a dataset, a buffer spec or a preparation request
(spec webapp-actions §4.1) ---`, each one `_transaction`. Rules the signatures do not settle:

- `adopt_subject` on a new `(kind, number)` inserts with `issue = number`, `revision = 0`. On an existing one it
  leaves `key` and `issue` alone and replaces `stage` and `data` only while `revision == 0`.
- A key held by another number of the same kind raises `ValueError` naming the key and the holder (catch
  `sqlite3.IntegrityError`).
- `set_subject_stage` sets the stage and raises `revision` by one. Slice B is its first caller; it is here so the
  rule above is tested.

- [ ] **Step 4: Run the tests, expect 3 passed**

- [ ] **Step 5: Commit** (`ledger: subjects, adopted from issues; a stage a workflow wrote outlives a re-adoption`)

---

### Task 3: The change feed

**Files:**
- Create: `mia_buffers/stream.py`
- Test: `tests/test_buffers_stream.py`

**Interfaces:**
- Produces: `class Changes` with `KEEP = 1000`, `announce(self, kind: str, number: int) -> None` and
  `stream(self, keepalive: float = 15.0) -> Iterator[bytes]`.

- [ ] **Step 1: Write the failing tests**

```python
"""The page's change feed: what a reader sees, and that it stays bounded."""

import json

from mia_buffers.stream import Changes


def test_stream_yields_changes_made_after_it_was_opened_then_keep_alives():
    changes = Changes()
    changes.announce("dataset", 1)  # before the stream was opened: not replayed, the page fetches on connect
    events = changes.stream(keepalive=0.05)
    changes.announce("buffer", 8)
    assert next(events) == b'data: {"kind": "buffer", "number": 8}\n\n'
    assert next(events) == b": keep-alive\n\n"


def test_a_reader_that_fell_behind_gets_what_is_still_held():
    changes = Changes()
    events = changes.stream(keepalive=0.05)
    for number in range(Changes.KEEP + 10):
        changes.announce("dataset", number)
    assert json.loads(next(events)[len(b"data: ") :])["number"] == 10
```

- [ ] **Step 2: Run them, expect failure** (`ModuleNotFoundError: mia_buffers.stream`)

Run: `pixi run pytest tests/test_buffers_stream.py -q`

- [ ] **Step 3: Implement `Changes`**

A `threading.Condition`, a `deque(maxlen=KEEP)` of `(sequence, kind, number)` and a running sequence.
`stream` is a plain function that reads the current sequence *when called* and returns a generator (a generator
function would read it at the first `next`, after the caller's headers went out, and miss a change in between).
The generator waits on the condition up to `keepalive` seconds, yields one `data:` line per change newer than its
cursor (`json.dumps({"kind": kind, "number": number})`), or `b": keep-alive\n\n"` when the wait timed out.

- [ ] **Step 4: Run the tests, expect 2 passed**

- [ ] **Step 5: Commit** (`buffer-api: a bounded change feed for the page's event stream`)

---

### Task 4: The subject routes and `/stream`

**Files:**
- Modify: `mia_buffers/api.py` (imports, `Response`, a body model, `ROUTES`, `BufferService.__init__`, five
  handlers, `make_server`)
- Test: `tests/test_buffers_api.py`

**Interfaces:**
- Consumes: Task 1's three tables, Task 2's `Ledger` methods, Task 3's `Changes`.
- Produces:
  - `PUT /subjects/<kind>/<number>` (worker), body `{"key": str, "stage": str = "new", "data": dict = {}}`,
    answers the subject view.
  - `GET /subjects/<kind>/<key>` (read): the subject view, `404` when unknown.
  - `GET /needs` (read): `{"needs": [view, ...]}`, the subjects whose status is `waiting` or `stuck`.
  - `GET /lookup?q=` (read): `{"subjects": [view, ...]}`.
  - `GET /stream` (read): `text/event-stream`, one `data: {"kind", "number"}` per change.
  - A *subject view* is the ledger row plus `"status"` (the status word) and `"actions"` (a sorted list).
  - `BufferService.changes: Changes`; later slices call `self.changes.announce(kind, number)` on every write.

- [ ] **Step 1: Write the failing tests** (they use the file's `svc` and `call` helpers)

```python
def _subjects(tmp_path):
    return BufferService(
        Ledger(tmp_path / "l.sqlite"), meta={"datasets_repo": "o/data", "buffers_repo": "o/buffers"}
    )


def test_adopt_then_read_a_subject(tmp_path):
    s = _subjects(tmp_path)
    status, row, _ = call(s, "PUT", "/subjects/dataset/234", payload={"key": "rec-a", "stage": "review"})
    assert status == 200 and row["issue"] == 234
    status, row, _ = call(s, "GET", "/subjects/dataset/rec-a")
    assert (status, row["status"], row["actions"]) == (200, "waiting", ["approve", "go", "reject"])
    assert call(s, "GET", "/subjects/dataset/nope")[0] == 404
    assert call(s, "GET", "/subjects/widget/x")[0] == 404
    assert call(s, "PUT", "/subjects/dataset/235", payload={"key": "rec-b", "stage": "bogus"})[0] == 422
    assert call(s, "PUT", "/subjects/dataset/235", payload={"key": "rec-a"})[0] == 409


def test_a_subject_key_with_spaces_a_slash_and_an_accent_round_trips(tmp_path):
    s, key = _subjects(tmp_path), "EM pool / v2 é"
    call(s, "PUT", "/subjects/buffer/8", payload={"key": key, "stage": "ready", "data": {"spec": "name: x"}})
    status, row, _ = call(s, "GET", "/subjects/buffer/" + quote(key, safe=""))
    assert (status, row["key"], row["data"]) == (200, key, {"spec": "name: x"})


def test_needs_lists_what_waits_on_a_person(tmp_path):
    s = _subjects(tmp_path)
    call(s, "PUT", "/subjects/dataset/1", payload={"key": "rec-a", "stage": "review"})
    call(s, "PUT", "/subjects/dataset/2", payload={"key": "rec-b", "stage": "testing"})
    call(s, "PUT", "/subjects/buffer/8", payload={"key": "pool", "stage": "needs-human"})
    call(s, "PUT", "/subjects/prep/9", payload={"key": "prep-9", "stage": "done"})
    needs = call(s, "GET", "/needs")[1]["needs"]
    assert sorted((row["kind"], row["key"], row["status"]) for row in needs) == [
        ("buffer", "pool", "stuck"),
        ("dataset", "rec-a", "waiting"),
    ]


def test_lookup_by_number_url_and_key(tmp_path):
    s = _subjects(tmp_path)
    call(s, "PUT", "/subjects/dataset/234", payload={"key": "rec-a"})
    call(s, "PUT", "/subjects/buffer/234", payload={"key": "pool"})
    call(s, "PUT", "/subjects/prep/9", payload={"key": "prep-9"})

    def found(text):
        return sorted((row["kind"], row["key"]) for row in call(s, "GET", "/lookup", {"q": text})[1]["subjects"])

    assert found("#234") == found(" 234 ") == [("buffer", "pool"), ("dataset", "rec-a")]
    assert found("https://github.com/o/data/issues/234") == [("dataset", "rec-a")]
    assert found("https://github.com/o/buffers/issues/234#issuecomment-1") == [("buffer", "pool")]
    assert found("https://github.com/o/buffers/issues/9") == [("prep", "prep-9")]
    assert found("rec-a") == [("dataset", "rec-a")]
    assert found("https://github.com/o/other/issues/234") == [] and found("nothing") == []
    assert call(s, "GET", "/lookup")[0] == 422


def test_adopting_needs_the_token_and_reading_does_not(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    assert call(s, "PUT", "/subjects/dataset/1", payload={"key": "rec-a"})[0] == 401
    assert call(s, "GET", "/needs")[0] == 200 and call(s, "GET", "/lookup", {"q": "1"})[0] == 200


def test_stream_over_http_and_a_reader_that_leaves(tmp_path):
    server = make_server(BufferService(Ledger(tmp_path / "l.sqlite")), "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with httpx.stream("GET", url + "/stream", timeout=5) as response:
            assert response.headers["content-type"] == "text/event-stream"
            assert httpx.put(url + "/subjects/dataset/234", json={"key": "rec-a"}).status_code == 200
            assert next(response.iter_lines()) == 'data: {"kind": "dataset", "number": 234}'
        # the reader is gone: the next change must not take the service down
        assert httpx.put(url + "/subjects/dataset/235", json={"key": "rec-b"}).status_code == 200
        assert httpx.get(url + "/subjects/dataset/rec-b").json()["number"] == 235
    finally:
        server.shutdown()
```

- [ ] **Step 2: Run them, expect failure** (404s: `no route PUT /subjects/dataset/234`)

Run: `pixi run pytest tests/test_buffers_api.py -k "subject or needs or lookup or stream or adopting" -q`

- [ ] **Step 3: Implement**

- `Response = tuple[int, dict | bytes | Iterator[bytes] | None, dict[str, str]]`.
- `SUBJECT_RULES = {"dataset": mia_data.config.STAGE_ACTIONS, "buffer": STAGE_ACTIONS, "prep": PREP_STAGE_ACTIONS}`
  and `ISSUE_URL_RE = re.compile(r"^https://github\.com/([^/]+/[^/]+)/issues/(\d+)")`.
- `class AdoptBody(_Strict)`: `key: str`, `stage: str = "new"`, `data: dict = {}`. (`SubjectBody` is taken: it is
  an assessment's subject.)
- `ROUTES`, in this order so the numeric `PUT` is not shadowed:

```python
    ("PUT", re.compile(r"^/subjects/([a-z]+)/(\d+)$"), "adopt_subject", "worker"),
    ("GET", re.compile(r"^/subjects/([a-z]+)/(.+)$"), "get_subject", "read"),
    ("GET", re.compile(r"^/needs$"), "needs", "read"),
    ("GET", re.compile(r"^/lookup$"), "lookup", "read"),
    ("GET", re.compile(r"^/stream$"), "stream", "read"),
```

- `self.changes = Changes()` in `__init__`.
- `_adopt_subject`: unknown kind `404`; stage not in the kind's table `422`; the ledger's `ValueError` `409`;
  then `self.changes.announce(kind, number)`.
- `_lookup`: `q` stripped; missing or empty `422`. A `#`-optional integer matches `issue` in every kind, plus an
  exact key match. A URL matching `ISSUE_URL_RE` matches `issue` in the kinds of its repo
  (`meta["datasets_repo"]`: dataset; `meta["buffers_repo"]`: buffer and prep; any other repo: nothing).
  Anything else is an exact key match in every kind.
- `_stream` answers `200, self.changes.stream(), {"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}`.
- `make_server._dispatch`: when the payload is neither bytes, dict nor None, send the status and headers with no
  `Content-Length`, then write and flush each chunk; an `OSError` (the reader left) ends the response quietly.

- [ ] **Step 4: Run the tests, expect 6 passed**

Then `pixi run pytest tests/test_buffers_api.py tests/test_buffers_ui.py -q`: all pass.

- [ ] **Step 5: Commit**

`buffer-api: subjects with their status and accepted actions, what needs a person, lookup by issue, a change
stream`

---

### Task 5: The registry client

**Files:**
- Modify: `mia_core/registry.py` (after `datasets`)
- Test: `tests/test_core_registry.py`

**Interfaces:**
- Produces, on `Registry`:
  - `adopt_subject(self, kind: str, number: int, key: str, *, stage: str = "new", data: dict | None = None) -> dict`
  - `subject(self, kind: str, key: str) -> dict | None`

- [ ] **Step 1: Write the failing test**

```python
def test_subject_calls():
    seen = []
    reg = Registry("http://svc", "tok", transport=transport(seen))
    reg.adopt_subject("buffer", 8, "EM pool/2", stage="ready", data={"spec": "s"})
    reg.subject("buffer", "EM pool/2")
    assert (seen[0].method, seen[0].url.raw_path) == ("PUT", b"/subjects/buffer/8")
    assert json.loads(seen[0].content) == {"key": "EM pool/2", "stage": "ready", "data": {"spec": "s"}}
    assert (seen[1].method, seen[1].url.raw_path) == ("GET", b"/subjects/buffer/EM%20pool%2F2")
    assert Registry("http://svc", "t", transport=transport([], 404, {"error": "no"})).subject("buffer", "x") is None
```

- [ ] **Step 2: Run it, expect failure** (`AttributeError: 'Registry' object has no attribute 'adopt_subject'`)

Run: `pixi run pytest tests/test_core_registry.py -k subject -q`

- [ ] **Step 3: Implement both** with `self.request` and `self._lookup`, quoting as `put_dataset` does.

- [ ] **Step 4: Run it, expect 1 passed**

- [ ] **Step 5: Commit** (`registry client: adopt and read a subject`)

---

### Task 6: `mia-buffers adopt-issues`

**Files:**
- Create: `mia_buffers/adopt.py`
- Modify: `mia_buffers/cli.py` (`cmd_adopt_issues`, its parser, the `_deployment` table), `docs/deployment.md`
- Test: `tests/test_buffers_adopt.py`, `tests/test_buffers_cli.py` (the help test names the command)

**Interfaces:**
- Consumes: `Registry.adopt_subject`, `Registry.dataset` (Task 5); `GitHub.list_issues(state="all")`, which
  already leaves pull requests out; `mia_data.seed.MARKER_RE`; `mia_buffers.activities.SPEC_FENCE`;
  `mia_buffers.spec.load_spec_text`, `SpecError`; the `STAGES` and `PREP_STAGES` of both configs.
- Produces: `async def adopt_issues(gh_data: GitHub, gh_buffers: GitHub, registry: Registry, *, dry_run: bool =
  False) -> list[str]`, the report, one line per issue; `mia-buffers adopt-issues [--dry-run] [--config PATH]`.

- [ ] **Step 1: Write the failing test**

```python
"""adopt-issues: every dataset, buffer and request issue becomes a subject once; duplicates are reported."""

import asyncio

import httpx

from mia_buffers.adopt import adopt_issues
from mia_buffers.api import BufferService
from mia_buffers.ledger import Ledger
from mia_buffers.spec import load_spec_text
from mia_core.registry import Registry
from mia_data.seed import MARKER
from tests.mia_helpers import FakeGH
from tests.test_buffers_plan import FIXTURES

SPEC = (FIXTURES / "spec.yaml").read_text()
NAME = load_spec_text(SPEC).name


def issue(number, body, *labels):
    return {"number": number, "body": body, "labels": [{"name": label} for label in labels]}


def registry_for(service):
    """A registry client whose requests go straight to `service.handle`."""

    def handler(request):
        status, payload, _ = service.handle(
            request.method, request.url.raw_path.decode().split("?")[0], dict(request.url.params), request.content
        )
        return httpx.Response(status, json=payload)

    return Registry("http://svc", None, transport=httpx.MockTransport(handler))


def repos():
    data = FakeGH(
        "o/data",
        issues={
            1: issue(1, MARKER.format("rec-a") + "\n", "data:review"),
            2: issue(2, MARKER.format("rec-b") + "\n", "data:done", "has-labels"),
            3: issue(3, MARKER.format("rec-a") + "\n", "data:testing"),
            4: issue(4, None),
            5: issue(5, "see " + MARKER.format("rec-c"), "data:new"),
        },
    )
    fence = f"```yaml\n{SPEC}```\n"
    buffers = FakeGH(
        "o/buffers",
        issues={
            8: issue(8, fence, "buffer:ready"),
            9: issue(9, "again\n" + fence, "buffer:needs-human", "buffer:go"),
            10: issue(10, "no spec here", "buffer:new"),
            11: issue(11, "```yaml\nrecords: [rec-a]\n```\n", "prep:requested"),
            12: issue(12, fence),
        },
    )
    return data, buffers


def test_adopts_every_issue_once_and_reports_the_rest(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_dataset("rec-a", {"issue": 3})  # the row names the issue that is the dataset's
    registry = registry_for(BufferService(ledger))
    lines = asyncio.run(adopt_issues(*repos(), registry))
    assert lines == [
        "dataset rec-a #3 testing",
        "duplicate: #1 also names rec-a (adopted #3); it is at review",
        "dataset rec-b #2 done",
        f"buffer {NAME} #8 ready",
        f"buffer {NAME}-9 #9 needs-human",
        "buffer issue-10 #10 new",
        "prep prep-11 #11 requested",
        "skipped: #12 has no buffer or prep label",
    ]
    assert [(row["kind"], row["number"]) for row in ledger.find_subjects()] == [
        ("buffer", 8), ("buffer", 9), ("buffer", 10), ("dataset", 2), ("dataset", 3), ("prep", 11),
    ]
    assert ledger.subject("buffer", NAME)["data"] == {"spec": SPEC}
    assert ledger.subject("buffer", "issue-10")["data"] == {}
    assert ledger.subject("prep", "prep-11")["data"] == {"spec": "records: [rec-a]\n"}
    assert asyncio.run(adopt_issues(*repos(), registry)) == lines  # again: the same report, nothing new
    assert len(ledger.find_subjects()) == 6


def test_without_a_row_the_lowest_issue_is_the_dataset_and_a_dry_run_writes_nothing(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    lines = asyncio.run(adopt_issues(*repos(), registry_for(BufferService(ledger)), dry_run=True))
    assert lines[:2] == [
        "dataset rec-a #1 review",
        "duplicate: #3 also names rec-a (adopted #1); it is at testing",
    ]
    assert ledger.find_subjects() == []
```

- [ ] **Step 2: Run it, expect failure** (`ModuleNotFoundError: mia_buffers.adopt`)

Run: `pixi run pytest tests/test_buffers_adopt.py -q`

- [ ] **Step 3: Implement `adopt_issues`**

The rules the test fixes, in order:

- Data repo: an issue is a dataset when `MARKER_RE.match(body or "")` (the start of the body only). Records in
  id order. Of several issues naming one record, the dataset is the one `registry.dataset(record)["issue"]`
  names, else the lowest number; each other is a `duplicate:` line with its stage and gets no subject.
- A stage is the first label `<prefix><stage>` whose stage is in the kind's `STAGES` (`data:`, `buffer:`,
  `prep:`), else `new`. Human labels such as `buffer:go` are not stages.
- Buffers repo, in number order: an issue with a `prep:` stage label is a request (key `prep-<number>`, `data`
  `{"spec": <the fence>}` when the body has exactly one fence, else `{}`); with a `buffer:` stage label a buffer;
  with neither, a `skipped:` line.
- A buffer's key is its spec's `name` when the body has exactly one fence that `load_spec_text` accepts and no
  lower-numbered issue took the name; `<name>-<number>` when one did; `issue-<number>` with `data` `{}` when
  there is no single valid spec.
- A `RegistryError` on one subject is a line `refused: <kind> #<number> <key>: <message>` and the run goes on.
- `dry_run` builds the same report and calls only `registry.dataset`.

- [ ] **Step 4: Run it, expect 2 passed**

- [ ] **Step 5: The command and its documentation**

`cmd_adopt_issues(cfg, args)` in `mia_buffers/cli.py`: clients from `_gh(cfg, cfg.github.datasets_repo)` and
`_gh(cfg, cfg.github.buffers_repo)`, `Registry.from_config(cfg.service)`, print each line, then
`adopted <n>, <m> to look at` (lines starting `duplicate:`, `refused:` or `skipped:`), exit 0; a `RegistryError`
before any line prints its message and exits 1, as `cmd_catalog_sync` does. Its parser takes `--dry-run` and
`--config` like `retire`'s; add `"adopt-issues": cmd_adopt_issues` to `_deployment`. Extend
`test_help_lists_commands` in `tests/test_buffers_cli.py` to expect `adopt-issues`.

`docs/deployment.md`, a short section "Adopting existing issues": on the buffers worker's host run
`mia-buffers adopt-issues --dry-run` (through `mia exec`, as the other `mia-buffers` commands there), read the
report, run it without `--dry-run`; it is safe to run again; a `duplicate:` line that is at a working
stage has a running workflow, which must be stopped before slice B is deployed (spec §11).

- [ ] **Step 6: Run `pixi run pytest tests/test_buffers_adopt.py tests/test_buffers_cli.py -q`, expect all passed**

- [ ] **Step 7: Commit**

`mia-buffers adopt-issues: every dataset, buffer and request issue becomes a subject; duplicates are reported`

---

### Task 7: Whole-slice check

- [ ] **Step 1:** `pixi run fmt && pixi run lint && pixi run typecheck` — clean.
- [ ] **Step 2:** `pixi run test` — all pass (1071 before this slice, plus the new ones); no `test_replay_*`
  changed.
- [ ] **Step 3:** `git diff autonomous-gates --stat` names no `workflows.py`, `activities.py`, `poll.py` or
  `ui/index.html`; the real-paths check of `AGENTS.md`, run on `git diff autonomous-gates`, is empty.
- [ ] **Step 4:** Commit anything `fmt` changed (`style: format`).

**After merge, by the operator (not part of the tests):** deploy buffer-api, run
`mia-buffers adopt-issues --dry-run` against the live repos, check that every issue appears once and the
duplicates are the expected ones, then run it for real and open `/needs` and `/lookup?q=<an issue number>`.
