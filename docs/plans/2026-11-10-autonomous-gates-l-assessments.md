# Autonomous gates, milestone L: assessments, authorization, revocation, budgets — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** every approval becomes an append-only assessment in the registry and a deterministic policy decides
what it authorizes; a human can revoke a published dataset or a staged volume after the fact, with the blast
radius found and the buffers that drew from it degraded; every job is charged to the root request that caused it
and a root over its caps stops; one switch pauses every workflow at its next gate.

**Architecture:** the registry (buffer-api's SQLite) gains two tables, `assessments` and `budgets`, with worker
`PUT`/`POST` routes and open `GET` routes; `mia_core/judge/policy.py` holds the pure `authorized()` function and
the assessment schema; the mia-data and mia-buffers activities write human labels as assessments and ask the
policy before publishing or writing `approved`; `data:revoke` and `buffer:revoke` write `reject` assessments,
quarantine artifacts and flip the dataset row to `revoked`, and the buffers keeper turns quarantined volumes into a
`revoked` signal that unregisters them; `reserve`/`settle` activities charge each job to its root; `mia pause`
edits `autonomy.pause` in `mia.yaml`, which a `paused` activity re-reads at every stage boundary. Every workflow
change sits behind a `workflow.patched()` marker (`l-budget`, `l-assess`, `l-revoke`); no judge runs yet (that is
milestone M), so the only assessments written here are human ones.

**Tech Stack:** Python 3.12, pydantic 2, SQLite (`json_extract`), Temporal test env, httpx, pytest/ruff/mypy via
`pixi run`. Branch `autonomous-gates` in this repo, on top of milestone K (`a2d49c3`).

**Spec:** `docs/specs/2026-11-09-autonomous-gates.md` v0.2, §5 (assessments and authorization), §6 (revocation
and quarantine), §8 (budgets cover the whole request), §11 rows for `mia_buffers/api.py`, `mia_core/judge/`,
`mia_core/config.py`, `mia_data/*`, `mia_buffers/*`, §12.10, §14 row L.

## Global Constraints

- Activity result dataclasses in `models.py` are wire format: add fields with defaults, never rename or remove.
  New result types in this plan: `Reservation`, `AssessmentWritten`, `Revocation` (mia-data), `BoardAssessed`,
  `RevokedSignal`, `RevokedArtifacts` (mia-buffers). New fields with defaults: `Pins.root`, `BufferPins.root`,
  `PrepPins.root`, `BindingInfo.refused`, `DatasetStatus.artifact`, `BufferState.approved_by`.
- Every workflow change is behind `workflow.patched("l-budget")`, `workflow.patched("l-assess")` or
  `workflow.patched("l-revoke")`. The recorded fixtures replay through the old branches; **no fixture is
  re-recorded in this plan**. `pixi run pytest tests/test_mia_workflows.py tests/test_buffers_workflows.py -k replay`
  must pass unchanged after every task.
- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` must be empty.
- `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test` clean before every commit.
- Worker routes of buffer-api take the bearer token; the page requests only read or consumer routes
  (`tests/test_buffers_ui.py` enforces it). Every new `PUT`/`POST`/`DELETE` route here is `worker`.
- Only the keeper deletes under the roots; nothing in this plan removes a file under `raw_root`, `staged_root` or
  `storage_root`. A revocation writes `revoked.json` beside `manifest.json` and marks rows; it deletes nothing.
- Spec §8 caps are the defaults: `autonomy.root.max_usd` 100, `max_fetch_bytes` 500e9, `max_jobs` 200,
  `max_concurrent_jobs` 20, `max_days` 14; `autonomy.quarantine_days` 30.
- Every rule this plan adds is also written down: `docs/development.md` (rules), `docs/mia-data-workflow.md`
  (`data:revoke`), `mia.yaml.example` (`autonomy:`), `mia_buffers/ui/index.html` (states, troubleshooting).

## Review Focus

Inputs the spec implies but no task's tests exercised before this plan; each has its test in the task named.

1. `data:revoke` on an issue that never published anything (no round, or a round that ended before publication):
   the label is refused with a comment saying there is nothing to revoke; nothing is written, nothing crashes.
   Task 6.
2. A `reserve` retried after a worker crash, with the same reservation id, counts once; a `settle` of a
   reservation the ledger never saw is a no-op that answers the current usage. Task 2.
3. `sync_datasets` runs while a row is `revoked` and the folder on disk still looks prepared (storage restored from
   a backup without `revoked.json`): the row stays `revoked`; and a folder with `revoked.json` is never scanned as
   prepared. Task 2 (ledger), Task 6 (`prepared_row`).
4. A buffer with a quarantined volume of one record and healthy volumes of others: that record's volumes are
   unregistered, the dataset fails with the reason, a group still at its floor keeps serving, and `GET /status`
   names the quarantined volumes while they are still registered. Task 2 (`status`), Task 6 (workflow).
5. `mia pause` on a `mia.yaml` that already has an `autonomy:` block with comments: the comments survive and only
   the flag flips; a round in flight finishes its current job and stops at its next job with "paused by the
   operator". Task 3 (file edit), Task 4 (workflow).

---

### Task 1: the policy module: assessment schema and `authorized()`

**Files:**
- Create: `mia_core/judge/__init__.py` (one docstring line: "The judge: assessments, the acceptance policy;
  judges themselves arrive with milestone M."), `mia_core/judge/policy.py`
- Test: `tests/test_core_policy.py`

**Interfaces:**
- Consumes: nothing in the repo.
- Produces (used by Tasks 2, 5, 6, 7):
  - `POLICY_VERSION: int = 1`; `GATES = ("review", "crops", "code", "staged")`;
    `SUBJECT_KINDS = ("pr_head", "crop_set", "artifact")`; `VERDICTS = ("approve", "reject", "escalate", "audited")`.
  - `assessment_id(at: float) -> str`: `f"{int(at * 1000):013d}-{uuid.uuid4().hex[:12]}"` (sorts by time).
  - `new_assessment(*, gate: str, subject_kind: str, subject_ref: str, verdict: str, by: str, at: float, source_digest: str = "", summary: str = "", findings: Sequence[Mapping] = (), supersedes: str | None = None, confidence: float | None = None, evidence_digest: str = "", checks: Mapping | None = None, model: str = "", prompt_version: str = "", coverage: Mapping | None = None) -> dict`
    returning the spec §5 record: `{"id", "gate", "subject": {"kind", "ref", "source_digest"}, "verdict", "by", "at": <ISO-8601 UTC>, "policy_version": POLICY_VERSION, "confidence", "findings", "summary", "supersedes", "evidence_digest", "checks", "model", "prompt_version", "coverage"}`;
    raises `ValueError` for a gate, kind or verdict outside the tuples or a `by` that is neither `judge` nor `human:<login>` nor `policy:<name>`.
  - `@dataclass(frozen=True) class Authorization: ok: bool; reason: str; assessment: str | None = None`
  - `authorized(assessments: Sequence[Mapping], *, policy_version: int = POLICY_VERSION, enabled_gates: frozenset[str] = frozenset(), checks_green: bool = True, budget_ok: bool = True) -> Authorization`
  - `refused(assessments: Sequence[Mapping]) -> str | None`: when the newest assessment (by `at`) is a `reject`,
    `f"{by} at {at}: {summary or 'no summary'}"`, else `None`.

- [ ] **Step 1: Write the failing tests**

```python
"""mia_core.judge.policy: the acceptance rules a gate's authorization rests on (spec autonomous-gates §5)."""

import pytest

from mia_core.judge.policy import POLICY_VERSION, Authorization, assessment_id, authorized, new_assessment, refused


def a(verdict, by="human:alice", at=1000.0, **kw):
    return new_assessment(gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s-t", verdict=verdict, by=by, at=at, **kw)


def test_new_assessment_has_the_spec_shape_and_validates_its_enums():
    row = a("approve", summary="data:approve")
    assert row["subject"] == {"kind": "crop_set", "ref": "i7-e11:s-t", "source_digest": ""}
    assert row["policy_version"] == POLICY_VERSION and row["at"] == "1970-01-01T00:16:40+00:00"
    assert row["id"].startswith("0000001000000-") and len(row["id"]) == 26
    for bad in ({"gate": "vibes"}, {"subject_kind": "file"}, {"verdict": "maybe"}, {"by": "alice"}):
        with pytest.raises(ValueError):
            new_assessment(**{**dict(gate="crops", subject_kind="crop_set", subject_ref="r", verdict="approve", by="judge", at=1.0), **bad})


def test_ids_sort_by_time():
    assert assessment_id(1000.0) < assessment_id(1000.001) < assessment_id(2000.0)


def test_a_human_approve_authorizes_until_a_later_reject():
    assert authorized([]) == Authorization(False, f"no approve assessment under policy v{POLICY_VERSION}")
    ok = authorized([a("approve", at=10.0)])
    assert ok.ok and ok.assessment and "human:alice" in ok.reason
    out = authorized([a("approve", at=10.0), a("reject", by="human:bob", at=20.0, summary="wrong channel")])
    assert out == Authorization(False, "rejected by human:bob at 1970-01-01T00:00:20+00:00: wrong channel")
    again = authorized([a("approve", at=10.0), a("reject", at=5.0), a("approve", at=30.0)])
    assert again.ok  # a reject older than the newest approve does not count


def test_a_revocation_superseding_the_approve_blocks_whatever_its_time():
    approve = a("approve", at=10.0)
    revoke = a("reject", by="human:bob", at=9.0, supersedes=approve["id"], summary="revoked")
    assert not authorized([approve, revoke]).ok


def test_a_judge_approve_counts_only_for_an_enabled_gate_and_the_current_policy():
    judged = a("approve", by="judge", at=10.0)
    assert not authorized([judged]).ok
    assert authorized([judged], enabled_gates=frozenset({"crops"})).ok
    stale = {**judged, "policy_version": POLICY_VERSION - 1}
    assert authorized([stale], enabled_gates=frozenset({"crops"})).reason == f"no approve assessment under policy v{POLICY_VERSION}"


def test_checks_and_budget_are_inputs_the_policy_cannot_override():
    approve = [a("approve", at=10.0)]
    assert authorized(approve, checks_green=False) == Authorization(False, "deterministic checks are not green")
    assert authorized(approve, budget_ok=False) == Authorization(False, "the root budget does not allow it")


def test_refused_names_the_newest_reject_only():
    assert refused([]) is None and refused([a("approve", at=10.0)]) is None
    assert refused([a("approve", at=10.0), a("reject", by="human:bob", at=20.0, summary="shifted labels")]) == (
        "human:bob at 1970-01-01T00:00:20+00:00: shifted labels"
    )
    assert refused([a("reject", at=5.0), a("approve", at=10.0)]) is None
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pixi run pytest tests/test_core_policy.py -v`
Expected: FAIL: `ModuleNotFoundError: mia_core.judge`.

- [ ] **Step 3: Implement `mia_core/judge/policy.py`**

`authorized`: sort by `at`; `approve` = the newest with `verdict == "approve"`, `policy_version == policy_version`
and (`by.startswith("human:")` or `by.startswith("policy:")` or `gate in enabled_gates`); none → the "no approve"
reason. Then any `reject` with `at >= approve["at"]` or `supersedes == approve["id"]` → the "rejected by" reason
(the newest such reject). Then `checks_green`, then `budget_ok`, with the exact reasons above. Success reason:
`f"approved by {by} at {at}"`. The ISO time: `datetime.fromtimestamp(at, UTC).isoformat()`.

- [ ] **Step 4: Run the tests**

Run: `pixi run pytest tests/test_core_policy.py -v`
Expected: PASS.

- [ ] **Step 5: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add mia_core/judge/__init__.py mia_core/judge/policy.py tests/test_core_policy.py
git commit -m "core: the judge policy module: assessments and the deterministic authorized() rule"
```

---

### Task 2: the registry: assessments and budgets tables and routes, `revoked` rows, quarantined volumes

**Files:**
- Modify: `mia_buffers/ledger.py` (`SCHEMA`, new methods after `datasets`), `mia_buffers/api.py` (`DATASET_STATES`,
  bodies, `ROUTES`, handlers, `_status`), `mia_core/registry.py` (client methods), `mia_buffers/keeper.py`
  (`free_assets`)
- Test: `tests/test_buffers_ledger.py`, `tests/test_buffers_api.py`, `tests/test_core_registry.py`,
  `tests/test_buffers_keeper.py`

**Interfaces:**
- Consumes: Task 1's `GATES`, `SUBJECT_KINDS`, `VERDICTS` (the API body validates against them).
- Produces:
  - `Ledger.put_assessment(assessment: dict) -> dict` (INSERT OR IGNORE on `id`; returns the stored row, so a
    retry is idempotent), `Ledger.assessment(assessment_id: str) -> dict | None`,
    `Ledger.assessments(*, subject_kind: str | None = None, subject_ref: str | None = None, gate: str | None = None, limit: int = 200) -> list[dict]` newest first.
  - `Ledger.reserve(root: str, reservation_id: str, kind: str, usd: float, caps: dict[str, float], now: float | None = None) -> dict`
    → `{"ok": bool, "reason": str, "usage": <budget row>}`; `Ledger.settle(root, reservation_id, *, usd: float | None = None, fetched_bytes: int = 0, staged_bytes: int = 0) -> dict` → the budget row;
    `Ledger.budget(root) -> dict | None`; `Ledger.budgets() -> list[dict]`.
    A budget row: `{"root", "since", "reserved_usd", "spent_usd", "fetched_bytes", "staged_bytes", "jobs", "running", "open": {rid: {"kind", "usd", "at"}}, "closed": [rid, ...]}`.
  - `Ledger.delete_volume(buffer: str, volume_id: str) -> bool` (the volume row, its frozen rows and a staged ref no
    other volume of the buffer still needs), `Ledger.volume_rows(buffer) -> list[dict]` (`{"id", "artifact", "record", "missing"}`),
    `Ledger.quarantined_volumes(buffer) -> list[str]`.
  - `DATASET_STATES = ("in-progress", "prepared", "needs-human", "revoked")`; `sync_datasets` never merges over a
    `revoked` row and reports `"revoked_kept"`.
  - Routes (capability): `PUT /assessments/<id>` worker, `GET /assessments/<id>` read, `GET /assessments` read
    (`?subject_kind=&subject_ref=&gate=&limit=`), `POST /budgets/<root>/reserve` worker, `POST /budgets/<root>/settle`
    worker, `GET /budgets/<root>` read, `GET /budgets` read, `DELETE /buffers/<b>/volumes/<vid>` worker,
    `GET /buffers/<b>/volumes` read. `GET /buffers/<b>/status` gains `"quarantined": [volume ids]`.
  - `Registry.put_assessment(assessment) -> dict`, `Registry.assessments(*, subject_kind=None, subject_ref=None, gate=None) -> list[dict]`,
    `Registry.reserve(root, reservation_id, kind, usd, caps) -> dict`, `Registry.settle(root, reservation_id, *, usd=None, fetched_bytes=0, staged_bytes=0) -> dict`,
    `Registry.budget(root) -> dict | None`, `Registry.delete_volume(buffer, volume_id) -> None`, `Registry.volumes(buffer) -> list[dict]`.
  - `keeper.free_assets(..., quarantine_s: float = 0)`: a row with `quarantined: true` and
    `quarantined_at + quarantine_s > now` is never free; `plan_evictions` takes and passes `quarantine_s`.

- [ ] **Step 1: Write the failing ledger tests**

Add to `tests/test_buffers_ledger.py`:

```python
def test_assessments_are_append_only_and_filterable(tmp_path):
    from mia_core.judge.policy import new_assessment

    led = Ledger(tmp_path / "l.sqlite")
    first = new_assessment(gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s", verdict="approve", by="human:a", at=10.0)
    second = new_assessment(gate="staged", subject_kind="artifact", subject_ref="art1", verdict="reject", by="human:b", at=20.0)
    assert led.put_assessment(first) == first and led.put_assessment({**first, "verdict": "reject"}) == first  # ignored
    led.put_assessment(second)
    assert [row["id"] for row in led.assessments()] == [second["id"], first["id"]]  # newest first
    assert led.assessments(subject_kind="artifact", subject_ref="art1") == [second]
    assert led.assessments(gate="crops") == [first] and led.assessment("nope") is None
    assert led.assessments(limit=1) == [second]


def test_reserve_enforces_the_caps_atomically_and_settles_once(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    caps = {"max_usd": 10, "max_fetch_bytes": 100, "max_jobs": 3, "max_concurrent_jobs": 2, "max_days": 14}
    root = "o/buffers#7"
    first = led.reserve(root, "job-1", "match", 6.0, caps, now=1000.0)
    assert first["ok"] and first["usage"]["reserved_usd"] == 6.0 and first["usage"]["running"] == 1
    assert led.reserve(root, "job-1", "match", 6.0, caps, now=1001.0)["usage"]["reserved_usd"] == 6.0  # a retry
    over = led.reserve(root, "job-2", "match", 5.0, caps, now=1002.0)
    assert not over["ok"] and over["reason"] == "root o/buffers#7: usd 6.00 reserved or spent + 5.00 would exceed max_usd 10"
    assert led.reserve(root, "job-2", "stage", 0.0, caps, now=1002.0)["ok"]  # jobs: 2 running of 2
    busy = led.reserve(root, "job-3", "stage", 0.0, caps, now=1003.0)
    assert busy["reason"] == "root o/buffers#7: 2 jobs running, max_concurrent_jobs 2"
    settled = led.settle(root, "job-1", usd=2.5)
    assert (settled["reserved_usd"], settled["spent_usd"], settled["running"], settled["jobs"]) == (0.0, 2.5, 1, 2)
    assert led.settle(root, "job-1", usd=2.5) == settled  # settling twice changes nothing
    assert led.settle(root, "never", fetched_bytes=5) == settled  # an unknown reservation: a no-op
    assert led.reserve(root, "job-3", "stage", 0.0, caps, now=1004.0)["ok"]
    assert led.settle(root, "job-2", fetched_bytes=60)["fetched_bytes"] == 60
    assert led.reserve(root, "job-4", "fetch", 0.0, caps, now=1005.0)["reason"] == "root o/buffers#7: 3 jobs launched, max_jobs 3"
    assert led.settle(root, "job-3", usd=None)["spent_usd"] == 2.5  # a job without a cost adds nothing to spent
    assert led.settle(root, "job-3", fetched_bytes=50) == led.budget(root)  # closed already: a no-op
    roomy = {**caps, "max_jobs": 99}
    assert led.reserve(root, "job-5", "fetch", 0.0, roomy, now=1006.0)["ok"]
    assert led.settle(root, "job-5", fetched_bytes=50)["fetched_bytes"] == 110
    bytes_over = led.reserve(root, "job-6", "fetch", 0.0, roomy, now=1007.0)
    assert bytes_over["reason"] == "root o/buffers#7: 110 bytes fetched, over max_fetch_bytes 100"
    old = led.reserve(root, "job-9", "stage", 0.0, {**roomy, "max_fetch_bytes": 1e9}, now=1000.0 + 15 * 86400)
    assert old["reason"] == "root o/buffers#7 is 15.0 days old, over max_days 14"
    assert led.budget("nobody") is None and [row["root"] for row in led.budgets()] == [root]


def test_sync_datasets_never_touches_a_revoked_row(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_dataset("bad", {"state": "revoked", "revoked_by": "human:a"})
    out = led.sync_datasets({"bad": {"folder": "f-bad"}, "good": {"folder": "f-good"}})
    assert out == {"prepared": 1, "demoted": 0, "revoked_kept": 1}
    assert led.dataset("bad")["state"] == "revoked" and "folder" not in led.dataset("bad")
    assert led.dataset("good")["state"] == "prepared"


def test_delete_volume_drops_its_frozen_rows_and_an_orphaned_ref(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    led.put_volume("b", vol("rec/a"), refs=[("staged", "art1"), ("raw", "rec")])
    led.put_volume("b", vol("rec/b"), refs=[("staged", "art1")])
    led.extend_frozen("b", "val", [{"volume": "rec/a", "i": 0}, {"volume": "rec/b", "i": 1}])
    assert led.delete_volume("b", "rec/a") and not led.delete_volume("b", "rec/a")
    assert [v.id for v in led.volumes("b")] == ["rec/b"] and led.frozen("b", "val") == [{"volume": "rec/b", "i": 1}]
    assert led.refs("staged", "art1") == ["b"]  # rec/b still lives in art1
    assert led.delete_volume("b", "rec/b") and led.refs("staged", "art1") == []
    assert led.volume_rows("b") == []


def test_quarantined_volumes_are_those_whose_artifact_is_quarantined(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_volume("b", vol("rec/a", artifact="art1"))
    led.put_volume("b", vol("rec/b", artifact="art2"))
    led.put_asset("staged", "art1", {"quarantined": True, "quarantined_at": 1.0})
    assert led.quarantined_volumes("b") == ["rec/a"]
    assert led.volume_rows("b")[0] == {"id": "rec/a", "artifact": "art1", "record": "rec", "missing": False}
```

(`vol` is `tests/test_buffers_sampler.py`'s helper; check whether it takes `artifact`; the `Volume` dataclass has an
`artifact` field, see `_volume_from`. If `vol` has no such keyword, build with `dataclasses.replace(vol(...), artifact=...)`.
A volume's record is its id up to the first `/`, as `missing_records` already assumes.)

- [ ] **Step 2: Run them to verify they fail**

Run: `pixi run pytest tests/test_buffers_ledger.py -k "assessments or reserve or revoked or delete_volume or quarantined" -v`
Expected: FAIL: `AttributeError: 'Ledger' object has no attribute 'put_assessment'` (and the others).

- [ ] **Step 3: Implement the ledger methods**

Schema additions:

```sql
CREATE TABLE IF NOT EXISTS assessments (id TEXT PRIMARY KEY, gate TEXT, subject_kind TEXT, subject_ref TEXT,
                                        verdict TEXT, by TEXT, at TEXT, data TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS assessments_subject ON assessments (subject_kind, subject_ref);
CREATE TABLE IF NOT EXISTS budgets (root TEXT PRIMARY KEY, data TEXT NOT NULL, updated REAL);
```

`reserve` runs inside `_transaction` (BEGIN IMMEDIATE makes the check-and-add atomic): read or create the row
(`since = now`), return the stored reservation when `reservation_id` is in `open` or `closed`; check in this order,
each with the exact reason string of the test: `max_days` (`(now - since) / 86400 > cap`), `max_usd`
(`reserved_usd + spent_usd + usd > cap`), `max_fetch_bytes` (`fetched_bytes > cap`), `max_concurrent_jobs`
(`running + 1 > cap`), `max_jobs` (`jobs + 1 > cap`); a missing cap key skips that check. On pass:
`open[rid] = {kind, usd, at: now}`, `reserved_usd += usd`, `jobs += 1`, `running += 1`. `settle`: when `rid` in
`open`: move its `usd` out of `reserved_usd`, add `usd if usd is not None else 0.0` to `spent_usd`, `running -= 1`,
append to `closed`, add the bytes; otherwise return the row unchanged (`None` row → `{}`-shaped empty row).
`max_fetch_bytes` is checked at `reserve` against what earlier settles recorded, since bytes are known only after
a fetch; a `settle` of a closed or unknown reservation returns the row unchanged.

`sync_datasets`: in the merge loop, skip a record whose stored `state` is `revoked` and count it; the stale scan
already excludes non-prepared rows.

- [ ] **Step 4: Run the ledger tests**

Run: `pixi run pytest tests/test_buffers_ledger.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing API and client tests**

Add to `tests/test_buffers_api.py`:

```python
def test_assessment_routes(tmp_path):
    from mia_core.judge.policy import new_assessment

    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    row = new_assessment(gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s", verdict="approve", by="human:a", at=10.0)
    assert call(s, "PUT", f"/assessments/{row['id']}", payload=row)[0] == 401
    assert worker(s, "PUT", f"/assessments/{row['id']}", row)[:2] == (200, row)
    assert worker(s, "PUT", f"/assessments/{row['id']}", {**row, "gate": "vibes"})[0] == 422
    assert worker(s, "PUT", "/assessments/other-id", row)[0] == 422  # the path and the body must agree
    assert call(s, "GET", f"/assessments/{row['id']}")[:2] == (200, row) and call(s, "GET", "/assessments/x")[0] == 404
    listed = call(s, "GET", "/assessments", {"subject_kind": "crop_set", "subject_ref": "i7-e11:s"})[1]
    assert listed == {"assessments": [row]}
    assert call(s, "GET", "/assessments", {"gate": "staged"})[1] == {"assessments": []}


def test_budget_routes(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    root = quote("o/buffers#7", safe="")
    body = {"reservation_id": "job-1", "kind": "match", "usd": 6.0, "caps": {"max_usd": 10}}
    assert call(s, "POST", f"/budgets/{root}/reserve", payload=body)[0] == 401
    status, out, _ = worker(s, "POST", f"/budgets/{root}/reserve", body)
    assert status == 200 and out["ok"] and out["usage"]["root"] == "o/buffers#7"
    refused = worker(s, "POST", f"/budgets/{root}/reserve", {**body, "reservation_id": "job-2"})[1]
    assert not refused["ok"] and "would exceed max_usd 10" in refused["reason"]
    assert worker(s, "POST", f"/budgets/{root}/settle", {"reservation_id": "job-1", "usd": 1.5})[1]["spent_usd"] == 1.5
    assert call(s, "GET", f"/budgets/{root}")[1]["spent_usd"] == 1.5 and call(s, "GET", "/budgets/nobody")[0] == 404
    assert [row["root"] for row in call(s, "GET", "/budgets")[1]["budgets"]] == ["o/buffers#7"]
    assert worker(s, "POST", f"/budgets/{root}/reserve", {"kind": "x"})[0] == 422


def test_volumes_routes_and_quarantined_status(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    worker(s, "PUT", "/buffers/b", CFG)
    worker(s, "PUT", "/buffers/b/volumes/rec%2Fv0.zarr", {**body("rec/v0.zarr"), "artifact": "art1"})
    worker(s, "PUT", "/buffers/b/volumes/other%2Fv0.zarr", {**body("other/v0.zarr"), "artifact": "art2"})
    worker(s, "PUT", "/assets/staged/art1", {"quarantined": True, "quarantined_at": 1.0, "quarantined_by": "human:a"})
    assert call(s, "GET", "/buffers/b/status")[1]["quarantined"] == ["rec/v0.zarr"]
    rows = call(s, "GET", "/buffers/b/volumes")[1]["volumes"]
    assert [(row["id"], row["artifact"], row["record"]) for row in rows] == [("other/v0.zarr", "art2", "other"), ("rec/v0.zarr", "art1", "rec")]
    assert call(s, "DELETE", "/buffers/b/volumes/rec%2Fv0.zarr")[0] == 401
    assert worker(s, "DELETE", "/buffers/b/volumes/rec%2Fv0.zarr")[:2] == (200, {"deleted": "rec/v0.zarr"})
    assert worker(s, "DELETE", "/buffers/b/volumes/rec%2Fv0.zarr")[0] == 404
    assert call(s, "GET", "/buffers/b/status")[1]["quarantined"] == [] and call(s, "GET", "/assets/staged/art1")[1]["refs"] == []
    assert call(s, "GET", "/buffers/zz/volumes")[0] == 404


def test_datasets_accept_the_revoked_state(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"))
    assert call(s, "PUT", "/datasets/rec", payload={"state": "revoked"})[1]["state"] == "revoked"
    assert call(s, "GET", "/datasets", {"state": "revoked"})[1]["datasets"][0]["id"] == "rec"
```

Add to `tests/test_core_registry.py`:

```python
def test_assessment_budget_and_volume_paths():
    seen = []
    reg = Registry("http://svc", "t", transport=transport(seen, body={"ok": True, "assessments": [], "volumes": []}))
    reg.put_assessment({"id": "a1", "gate": "crops"})
    reg.assessments(subject_kind="artifact", subject_ref="art1")
    reg.reserve("o/r#7", "job-1", "match", 5.0, {"max_usd": 10})
    reg.settle("o/r#7", "job-1", usd=1.0, fetched_bytes=3)
    reg.delete_volume("b", "rec/v0.zarr")
    reg.volumes("b")
    assert [(r.method, r.url.raw_path.decode()) for r in seen] == [
        ("PUT", "/assessments/a1"),
        ("GET", "/assessments?subject_kind=artifact&subject_ref=art1"),
        ("POST", "/budgets/o%2Fr%237/reserve"),
        ("POST", "/budgets/o%2Fr%237/settle"),
        ("DELETE", "/buffers/b/volumes/rec%2Fv0.zarr"),
        ("GET", "/buffers/b/volumes"),
    ]
    assert json.loads(seen[3].content) == {"reservation_id": "job-1", "usd": 1.0, "fetched_bytes": 3, "staged_bytes": 0}
```

Add to `tests/test_buffers_keeper.py`:

```python
def test_a_quarantined_asset_is_held_for_the_quarantine_then_free():
    now = 10_000.0
    rows = [
        {"key": "q", "data": {"state": "ready", "bytes": 40, "last_used": 1.0, "quarantined": True, "quarantined_at": now - 100}, "refs": []},
        {"key": "old", "data": {"state": "ready", "bytes": 40, "last_used": 1.0, "quarantined": True, "quarantined_at": now - 5000}, "refs": []},
    ]
    rules = {"hold_s": 3600, "now": now, "in_progress": set(), "kind": "staged"}
    assert [r["key"] for r in free_assets(rows, **rules)] == ["q", "old"]  # no quarantine window: as before
    assert [r["key"] for r in free_assets(rows, quarantine_s=1000, **rules)] == ["old"]
    picked = plan_evictions(rows, quota_bytes=10, headroom_bytes=0, quarantine_s=1000, **rules)
    assert [r["key"] for r in picked] == ["old"]
```

- [ ] **Step 6: Run them to verify they fail**

Run: `pixi run pytest tests/test_buffers_api.py -k "assessment or budget or volumes_routes or revoked_state" tests/test_core_registry.py -k paths tests/test_buffers_keeper.py -k quarantined -v`
Expected: FAIL: 404 `no route` for the new paths; `AttributeError` on the client; `TypeError` on `quarantine_s`.

- [ ] **Step 7: Implement the API, the client and the keeper rule**

`api.py`: `AssessmentBody(_Strict)` with `id: str`, `gate: Literal[*GATES]` (or a validator against the tuple),
`subject: SubjectBody(kind, ref, source_digest = "")`, `verdict`, `by: str`, `at: str`, `policy_version: int`,
`confidence: float | None = None`, `findings: list[dict] = []`, `summary: str = ""`, `supersedes: str | None = None`,
`evidence_digest: str = ""`, `checks: dict | None = None`, `model: str = ""`, `prompt_version: str = ""`,
`coverage: dict | None = None`; `_put_assessment` 422 when `body.id != <path id>`. `ReserveBody(reservation_id: str, kind: str, usd: float = 0.0, caps: dict[str, float] = {})`,
`SettleBody(reservation_id: str, usd: float | None = None, fetched_bytes: int = 0, staged_bytes: int = 0)`.
Routes added to `ROUTES` before the `GET /$` page route; `_delete_volume` checks `_config(buffer)` first, 404
`no volume {vid} in {buffer}` when nothing was deleted. `_status` adds `"quarantined": self.ledger.quarantined_volumes(buffer)`.
`_keeper_evict` (Task 6 wires `quarantine_s=self.cfg.autonomy.quarantine_days * 86400`; here only the keeper
rule and its parameter).

- [ ] **Step 8: Run the four test files**

Run: `pixi run pytest tests/test_buffers_ledger.py tests/test_buffers_api.py tests/test_core_registry.py tests/test_buffers_keeper.py tests/test_buffers_ui.py -v`
Expected: PASS (`test_buffers_ui` still passes: the page requests nothing new yet).

- [ ] **Step 9: Document the routes**

`docs/development.md`, "The registry (milestone B)": a paragraph **Assessments and budgets.** listing the routes
and capabilities above and the budget row's fields; `DATASET_STATES` sentence gains `revoked`; the "Asset rows"
paragraph gains `quarantined`, `quarantined_at`, `quarantined_by` ("the keeper holds a quarantined row for
`autonomy.quarantine_days` after its references are gone").

- [ ] **Step 10: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add mia_buffers/ledger.py mia_buffers/api.py mia_core/registry.py mia_buffers/keeper.py tests/test_buffers_ledger.py tests/test_buffers_api.py tests/test_core_registry.py tests/test_buffers_keeper.py docs/development.md
git commit -m "buffer-api: assessments and budgets tables and routes; revoked dataset rows; quarantined volumes in status; unregister a volume"
```

---

### Task 3: the `autonomy` config block, `mia pause` / `mia resume`, root ids on the pins

**Files:**
- Modify: `mia_core/config.py` (`RootBudgetCfg`, `AutonomyCfg`), `mia_data/config.py` (`Config.autonomy`, `root_of`),
  `mia_buffers/config.py` (`root_of`), `mia_ops/cli.py` (`pause`, `resume`), `mia.yaml.example`
- Create: `mia_core/autonomy.py`
- Modify: `mia_data/models.py` (`Pins.root`), `mia_buffers/models.py` (`BufferPins.root`, `PrepPins.root`),
  `mia_data/seed.py` (`render_issue(..., root: str = "")`, `ROOT_RE`), `mia_data/activities.py` (`admit`),
  `mia_buffers/activities.py` (`admit`, `prep_admit`, `_nudge`)
- Test: `tests/test_core_config.py`, `tests/test_mia_ops_cli.py`, `tests/test_mia_seed.py`,
  `tests/test_mia_activities.py`, `tests/test_buffers_activities.py`

**Interfaces:**
- Consumes: `mia_core.config.manifest_path`, `read_manifest`.
- Produces:
  - `class RootBudgetCfg(BaseModel): max_usd: float = 100; max_fetch_bytes: float = 500e9; max_jobs: int = 200; max_concurrent_jobs: int = 20; max_days: float = 14`
  - `class AutonomyCfg(BaseModel): pause: bool = False; quarantine_days: float = 30; root: RootBudgetCfg = RootBudgetCfg()`;
    `Config.autonomy: AutonomyCfg = AutonomyCfg()` on mia-data's `Config` (mia-buffers inherits).
  - `AutonomyCfg.caps() -> dict[str, float]`: the five `root` fields as the `caps` dict `Ledger.reserve` takes.
  - `Config.root_of(issue: int) -> str`: `f"{github.data_repo}#{issue}"` (mia-data), `f"{github.buffers_repo}#{issue}"` (mia-buffers).
  - `mia_core/autonomy.py`: `paused(manifest: Path) -> bool` (re-reads the file; a missing or unparsable file is
    `False`, logged), `set_pause(manifest: Path, value: bool) -> None` (a text edit that keeps comments).
  - `Pins.root: str = ""`, `BufferPins.root: str = ""`, `PrepPins.root: str = ""`.
  - `mia_data.seed.ROOT_MARKER = "<!-- mia-data:root={} -->"`, `ROOT_RE = re.compile(r"<!-- mia-data:root=(\S+) -->")`;
    `render_issue(record, entry, repo="", path="", record_id=None, root="")` writes the root marker as the body's
    second line when `root` is given.
  - `mia pause` / `mia resume` commands.

- [ ] **Step 1: Write the failing config and pause tests**

Add to `tests/test_core_config.py`:

```python
def test_autonomy_defaults_and_caps():
    from mia_core.config import AutonomyCfg

    cfg = AutonomyCfg()
    assert (cfg.pause, cfg.quarantine_days) == (False, 30)
    assert cfg.caps() == {"max_usd": 100, "max_fetch_bytes": 500e9, "max_jobs": 200, "max_concurrent_jobs": 20, "max_days": 14}


def test_pause_is_a_text_edit_that_keeps_comments(tmp_path):
    from mia_core.autonomy import paused, set_pause

    manifest = tmp_path / "mia.yaml"
    manifest.write_text("temporal:\n  address: localhost:7233   # keep me\nservices: {}\n")
    assert paused(manifest) is False
    set_pause(manifest, True)
    assert paused(manifest) is True and "# keep me" in manifest.read_text()
    assert manifest.read_text().endswith("autonomy:\n  pause: true\n")
    set_pause(manifest, False)
    assert paused(manifest) is False and manifest.read_text().count("autonomy:") == 1
    manifest.write_text("autonomy:                 # the switches\n  quarantine_days: 30\n  pause: false  # mia pause flips this\nservices: {}\n")
    set_pause(manifest, True)
    text = manifest.read_text()
    assert paused(manifest) is True and "# the switches" in text and "# mia pause flips this" in text
    assert "quarantine_days: 30" in text and text.count("pause:") == 1
    assert paused(tmp_path / "missing.yaml") is False
```

Add to `tests/test_mia_ops_cli.py`:

```python
def test_pause_and_resume_edit_the_manifest(stack, tmp_path, monkeypatch):  # noqa: F811
    manifest = tmp_path / "mia.yaml"
    manifest.write_text("services: {}\n")
    monkeypatch.setenv("MIA_CONFIG", str(manifest))
    assert cli.main(["pause"]) == 0 and "pause: true" in manifest.read_text()
    assert cli.main(["resume"]) == 0 and "pause: false" in manifest.read_text()
```

(`stack` patches `load_manifest`, so `main` builds its `Stack` from the fixture; the commands read
`manifest_path(args.manifest)`, which the env var points at the temp file.)

- [ ] **Step 2: Run them to verify they fail**

Run: `pixi run pytest tests/test_core_config.py -k "autonomy or pause" tests/test_mia_ops_cli.py -k pause -v`
Expected: FAIL: `ImportError` (`AutonomyCfg`, `mia_core.autonomy`), `SystemExit` from argparse on `pause`.

- [ ] **Step 3: Implement the config, the pause module and the commands**

`set_pause`: find a top-level `autonomy:` block (`^autonomy:[^\n]*\n(?:[ \t]+[^\n]*\n?)*`); inside it a
`^\s+pause:\s*\S+` line is rewritten in place (keeping a trailing comment), else `  pause: <value>\n` is inserted
as the block's first child line; with no block, append `autonomy:\n  pause: <value>\n` (preceded by a newline when
the file does not end with one). Values are `true`/`false`. `paused`: `yaml.safe_load`; `True` only when
`data["autonomy"]["pause"] is True`. `mia pause`/`mia resume`: `set_pause(manifest_path(args.manifest), ...)`
then print `paused: every workflow stops at its next job; mia resume lifts it` / `resumed`. Register both in
`COMMANDS` and the parser; `test_help_lists_commands` gains them.

- [ ] **Step 4: Run the config and ops tests**

Run: `pixi run pytest tests/test_core_config.py tests/test_mia_ops_cli.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing root-id tests**

Add to `tests/test_mia_seed.py`:

```python
def test_render_issue_carries_the_root_marker_on_its_second_line():
    from mia_data.seed import ROOT_RE, render_issue

    record = {"id": "rec", "imaging": {"modality": ["EM"]}, "technical": {"arrays": []}}
    _, body, _ = render_issue(record, {"record_commit": "c" * 40}, "o/catalog", "p.yaml", root="o/buffers#7")
    lines = body.splitlines()
    assert lines[0] == "<!-- mia-data:record=rec -->" and lines[1] == "<!-- mia-data:root=o/buffers#7 -->"
    assert ROOT_RE.search(body).group(1) == "o/buffers#7"
    _, plain, _ = render_issue(record, {"record_commit": "c" * 40}, "o/catalog", "p.yaml")
    assert ROOT_RE.search(plain) is None
```

In `tests/test_mia_activities.py`, find the existing `admit` test (search `async def test_admit`) and add beside it:

```python
async def test_admit_pins_the_root_from_the_issue_marker_or_the_issue_itself(tmp_path, gh):
    t = _setup(tmp_path)
    t.gh_data.issues[7]["body"] = "<!-- mia-data:record=rec-1 -->\n<!-- mia-data:root=o/buffers#9 -->\n- Record: x"
    assert (await t.a.admit(7, "11")).root == "o/buffers#9"
    t.gh_data.issues[8] = {**t.gh_data.issues[7], "number": 8, "body": "<!-- mia-data:record=rec-1 -->\n- Record: x"}
    assert (await t.a.admit(8, "12")).root == f"{t.cfg.github.data_repo}#8"
```

(Adapt `_setup`/`gh` to how that file builds its activities and fake issues; the assertion is what matters.)

In `tests/test_buffers_activities.py`:

```python
async def test_admit_and_prep_admit_pin_their_own_issue_as_root(acts):
    pins = await acts.admit(7, "11")
    assert pins.root == f"{acts.cfg.github.buffers_repo}#7"
    assert acts._pins("i7-e11").root == pins.root  # written to pins.json


async def test_nudge_opens_the_dataset_issue_with_the_requests_root(acts, monkeypatch):
    await acts.admit(7, "11")
    created = []
    gh = acts.gh_datasets
    monkeypatch.setattr(gh, "find_issue_by_marker", _async(lambda marker: None))
    monkeypatch.setattr(gh, "create_issue", _async(lambda title, body, labels: created.append(body) or {"number": 5, "labels": []}))
    monkeypatch.setattr(gh, "add_labels", _async(lambda number, labels: None))
    await acts.nudge_dataset("i7-e11", U937)
    assert f"<!-- mia-data:root={acts.cfg.github.buffers_repo}#7 -->" in created[0]
```

(`_async(fn)` wraps a plain callable as a coroutine function; write it at the top of the test file if the file
has no such helper. Check how the file's existing nudge tests fake `gh_datasets`, and follow that instead if they
use `FakeGH`'s own `issues` dict: then assert on the created issue's body in `FakeGH`.)

- [ ] **Step 6: Run them to verify they fail**

Run: `pixi run pytest tests/test_mia_seed.py -k root tests/test_mia_activities.py -k root tests/test_buffers_activities.py -k "root" -v`
Expected: FAIL: `TypeError: render_issue() got an unexpected keyword argument 'root'`; `AttributeError: 'Pins' object has no attribute 'root'`.

- [ ] **Step 7: Implement the root ids**

`Pins.root: str = ""` (last field). mia-data `admit`: after the record marker check, `root = (ROOT_RE.search(body) or [None, None])[1] or self.cfg.root_of(issue)`,
passed as `Pins(..., root=root)`; a `pins.json` from before this change loads with `root == ""` and
`_pins` callers treat `""` as "unknown root" (Task 4: no reservation). `BufferPins.root`, `PrepPins.root`;
buffers `admit`: `BufferPins(..., root=self.cfg.root_of(issue))`; `prep_admit`: the same root in both the
`pins.json` it writes (`BufferPins(spec_hash, ref, "", "", root)`) and `PrepPins(..., root=root)`. `_nudge`
passes `root=self._pins(round_id).root` to `render_issue`. `render_issue`: insert `ROOT_MARKER.format(root)` as
the second line when `root`.

- [ ] **Step 8: Run the activity and seed tests**

Run: `pixi run pytest tests/test_mia_seed.py tests/test_mia_activities.py tests/test_buffers_activities.py -v`
Expected: PASS.

- [ ] **Step 9: Document**

`mia.yaml.example`: a shared `autonomy:` block after `keeper:` with the five caps, `quarantine_days: 30` and
`pause: false   # mia pause / mia resume; every worker re-reads it at each job`. `docs/development.md`
"Configuration": one sentence on the block and that `mia pause` edits the file in place.

- [ ] **Step 10: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add mia_core/config.py mia_core/autonomy.py mia_data/config.py mia_buffers/config.py mia_ops/cli.py mia.yaml.example mia_data/models.py mia_buffers/models.py mia_data/seed.py mia_data/activities.py mia_buffers/activities.py tests/test_core_config.py tests/test_mia_ops_cli.py tests/test_mia_seed.py tests/test_mia_activities.py tests/test_buffers_activities.py docs/development.md
git commit -m "config: the autonomy block (root caps, quarantine, pause); mia pause and resume; every round pins its root request"
```

---

### Task 4: budgets and the pause in the workflows

**Files:**
- Modify: `mia_data/models.py` (`Reservation`), `mia_data/activities.py` (`paused`, `reserve`, `settle`),
  `mia_data/workflows.py` (`_round`, `_job`), `mia_buffers/activities.py` (same three activities),
  `mia_buffers/workflows.py` (`MiaBuffer._round`, `_job`, `_fetch_jobs`/`_fetch`, `MiaPrep._round`)
- Test: `tests/test_mia_activities.py`, `tests/test_mia_workflows.py`, `tests/test_buffers_activities.py`,
  `tests/test_buffers_workflows.py`

**Interfaces:**
- Consumes: `Registry.reserve/settle` (Task 2), `AutonomyCfg.caps()`, `mia_core.autonomy.paused`,
  `manifest_path` (Task 3), `Pins.root` / `BufferPins.root`.
- Produces:
  - `@dataclass class Reservation: ok: bool; reason: str = ""` in `mia_data/models.py` (mia-buffers imports it).
  - Activities on both `Activities` classes: `paused() -> bool`; `reserve(root: str, reservation_id: str, kind: str, usd: float) -> Reservation`;
    `settle(root: str, reservation_id: str, usd: float | None = None, fetched_bytes: int = 0) -> None`.
    With no registry (`self.registry is None` in mia-data) or an empty `root`: `Reservation(True, "no budget")`
    and a no-op settle. A registry error raises (the activity retries).
  - Workflow constants: `PAUSED = "paused by the operator (autonomy.pause); `mia resume`, then add the label again"`.

- [ ] **Step 1: Write the failing activity tests**

In `tests/test_mia_activities.py` (the file has a live-registry fixture or builds `Registry(token_server, "t")`;
follow its pattern):

```python
async def test_reserve_and_settle_charge_the_root(tmp_path, token_server):
    t = _setup(tmp_path)
    t.a.registry = Registry(token_server, "t")
    t.cfg.autonomy.root.max_usd = 7
    assert await t.a.reserve("o/data#7", "mia-7-i7-e11-match-1", "match", 5.0) == Reservation(True, "")
    refused = await t.a.reserve("o/data#7", "mia-7-i7-e11-match-2", "match", 5.0)
    assert not refused.ok and "would exceed max_usd 7" in refused.reason
    await t.a.settle("o/data#7", "mia-7-i7-e11-match-1", usd=1.0)
    assert t.a.registry.budget("o/data#7")["spent_usd"] == 1.0
    assert (await t.a.reserve("", "k", "match", 5.0)) == Reservation(True, "no budget")


async def test_paused_reads_the_manifest_fresh(tmp_path, monkeypatch):
    from mia_core.autonomy import set_pause

    t = _setup(tmp_path)
    manifest = tmp_path / "mia.yaml"
    manifest.write_text("services: {}\n")
    monkeypatch.setenv("MIA_CONFIG", str(manifest))
    assert await t.a.paused() is False
    set_pause(manifest, True)
    assert await t.a.paused() is True
```

The same two tests in `tests/test_buffers_activities.py` with the `acts` fixture (`acts.registry` is live;
`acts.cfg.autonomy.root.max_usd = 7`).

- [ ] **Step 2: Run them to verify they fail**

Run: `pixi run pytest tests/test_mia_activities.py -k "reserve or paused" tests/test_buffers_activities.py -k "reserve or paused" -v`
Expected: FAIL: `ImportError: Reservation`; `AttributeError: ... has no attribute 'reserve'`.

- [ ] **Step 3: Implement the activities**

`reserve`: `out = registry.reserve(root, reservation_id, kind, usd, self.cfg.autonomy.caps())` →
`Reservation(out["ok"], "" if out["ok"] else out["reason"])`. `settle`: `registry.settle(...)` via
`best_effort` (an accounting write never fails a round). `paused`: `autonomy.paused(manifest_path())`. Add the
three to `Activities.all()` in both packages.

- [ ] **Step 4: Run the activity tests**

Run: `pixi run pytest tests/test_mia_activities.py tests/test_buffers_activities.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing workflow tests**

`tests/test_mia_workflows.py` `Mocks`: add `paused=False`, `reservations=None` (job kind → `Reservation`, default
ok) to `__init__`; register activities `paused` (records `("paused",)`, returns `self.paused`), `reserve`
(records `("reserve", root, reservation_id, kind, usd)`, answers `turn(self.reservations, kind, Reservation(True))`;
copy `turn` from the buffers test file if absent) and `settle` (records `("settle", root, reservation_id, usd, fetched_bytes)`).
Tests:

```python
async def test_every_job_is_reserved_against_the_root_and_settled(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
    assert [a[2] for a in m.of("reserve")] == ["match", "smoke", "crop", "verify"]
    assert [a[1] for a in m.of("reserve")] == [a[0] for a in m.of("submit")]  # the reservation id is the job key
    assert [a[0] for a in m.of("reserve")] == [PINS.root] * 4 and m.of("reserve")[0][3] == 5.0  # the match agent's cap
    assert [a[1] for a in m.of("settle")] == [a[1] for a in m.of("reserve")]  # one settle per reservation
    assert m.names().index("reserve") < m.names().index("submit")


async def test_a_refused_reservation_ends_the_round_naming_the_root(env):
    m = Mocks(reservations={"match": Reservation(False, "root o/data#7: usd 99.00 reserved or spent + 5.00 would exceed max_usd 100")})
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
    assert "submit" not in m.names()
    assert m.of("set_stage")[-1][1] == "root budget: root o/data#7: usd 99.00 reserved or spent + 5.00 would exceed max_usd 100"


async def test_a_paused_operator_stops_the_round_before_its_first_job(env):
    m = Mocks(paused=True)
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
    assert m.of("set_stage")[-1][1].startswith("paused by the operator") and "submit" not in m.names()
```

(`PINS` in that file is `Pins("rec-1", ...)`: give it `root="o/data#7"` so the assertions read well; the
`RoundLimits` the mocks return has no agent cap field today, so `_job` passes `usd=5.0` only for `kind == "match"`
from a new `RoundLimits.agent_max_budget_usd: float = 5.0` the `limits` activity fills from
`cfg.limits.agent_max_budget_usd`; update the mock's `RoundLimits(4, 16, 120, self.revisions)` accordingly.)

`tests/test_buffers_workflows.py` `Mocks`: the same three activities (`paused`, `reserve`, `settle`) with the same
recording shape; `PINS` gains `root="o/buffers#7"`. Tests:

```python
async def test_stage_and_fetch_jobs_are_reserved_and_settled_with_their_bytes(env):
    m = Mocks(plan=ONLY_U937, raw={"u937": RawStatus(False, ["https://zenodo.org/f"])}, fetches={"u937": FetchResult(True, bytes_fetched=4096)})
    async with started(env, m, signal="go") as h:
        await idle(h, m, "staged")
    assert [a[2] for a in m.of("reserve")] == ["fetch", "stage"] and all(a[0] == "o/buffers#7" for a in m.of("reserve"))
    assert [(a[1], a[3]) for a in m.of("settle")] == [(m.of("reserve")[0][1], 4096), (m.of("reserve")[1][1], 0)]


async def test_a_refused_reservation_fails_the_dataset_with_the_reason(env):
    m = Mocks(plan=ONLY_U937, reservations={"stage": Reservation(False, "root o/buffers#7: 200 jobs launched, max_jobs 200")})
    async with started(env, m, signal="go") as h:
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "u937").status == "failed" and "max_jobs 200" in dataset(state, "u937").reason
    assert "submit" not in m.names()


async def test_paused_stops_a_buffer_round_and_a_prep_round(env):
    m = Mocks(plan=ONLY_U937, paused=True)
    async with started(env, m, signal="go") as h:
        await idle(h, m, "needs-human")
    assert m.of("set_stage")[-1][1].startswith("paused by the operator") and "plan" not in m.names()
```

(For `MiaPrep`, add the same `paused` check in its `_round` before `prep_admit` and extend the existing
`test_prep_round_opens_nudges_and_closes_done`'s mocks with a `paused` activity returning `False`; a separate
prep pause test is not needed, the branch is one line and the buffer test covers the activity.)

- [ ] **Step 6: Run them to verify they fail**

Run: `pixi run pytest tests/test_mia_workflows.py -k "reserved or reservation or paused" tests/test_buffers_workflows.py -k "reserved or reservation or paused" -v`
Expected: FAIL: no `reserve` calls recorded; the round reaches `done`/`staged` instead of `needs-human`.

- [ ] **Step 7: Implement in the workflows**

mia-data `_round`: after `limits`, `if workflow.patched("l-budget") and await self._act("paused", result_type=bool): raise RoundFailed(PAUSED)`.
`_job`: behind the same marker, before `submit`: `paused` check (`RoundFailed(PAUSED)`), then
`reservation = await self._act("reserve", state.pins.root, key, kind, usd, result_type=Reservation)` with
`usd = limits.agent_max_budget_usd if kind == "match" else 0.0`; `not reservation.ok → raise RoundFailed(f"root budget: {reservation.reason}")`;
after `await_job` returns (in a `finally`), `await self._act("settle", state.pins.root, key, budget=timedelta(minutes=10))`
wrapped so a settle failure is logged, not raised. `_guard` turns `RoundFailed` into `needs-human` with the message.

mia-buffers `MiaBuffer._round`: the paused check after `admit` → `_finish("needs-human", PAUSED)` and return.
`_job` and `_fetch_jobs` (where `submit`/`submit_fetch` are called): behind `l-budget`, a `paused` answer of
`True` or a refused reservation raises a new `class BudgetRefused(Exception)` carrying the reason (`PAUSED`, or
`f"root budget: {reservation.reason}"`); `_source` catches it next to `ActivityError` and calls
`_fail(dataset, reason)`, so a refusal fails that dataset, never the round's other datasets. `_fetch`: after `read_fetch`, settle the
fetch reservation with `fetched_bytes=result.bytes_fetched`; `_job`: settle with `fetched_bytes=0` after the
await. `MiaPrep._round`: paused check before `prep_admit` → `_stage("needs-human", PAUSED)`.

- [ ] **Step 8: Run both workflow files, replay included**

Run: `pixi run pytest tests/test_mia_workflows.py tests/test_buffers_workflows.py -v`
Expected: PASS, every `test_replay_*` unchanged.

- [ ] **Step 9: Document**

`docs/development.md`, "Rules that are not obvious from the code": **Budgets attach to the root request.** one
paragraph (the root marker, `reserve` before every job with the job key as reservation id, `settle` after, the
caps from `autonomy.root`, a refusal fails the round or the dataset naming root, cap and usage; `mia pause` stops
every round at its next job). `docs/mia-data-workflow.md` "Operating it": the same in two sentences.

- [ ] **Step 10: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add mia_data/models.py mia_data/activities.py mia_data/workflows.py mia_buffers/activities.py mia_buffers/workflows.py tests/test_mia_activities.py tests/test_mia_workflows.py tests/test_buffers_activities.py tests/test_buffers_workflows.py docs/development.md docs/mia-data-workflow.md
git commit -m "workflows: every job reserves against its root budget and settles after; autonomy.pause stops a round at its next job"
```

---

### Task 5: human labels are assessments; publication and `approved` ask the policy

**Files:**
- Modify: `mia_data/models.py` (`AssessmentWritten`), `mia_data/activities.py` (`assess`), `mia_data/workflows.py`
  (`_from_review`, `_approve_pr`, `_revise_pr`), `mia_buffers/models.py` (`BoardAssessed`, `BindingInfo.refused`,
  `DatasetStatus.artifact`, `BufferState.approved_by`), `mia_buffers/activities.py` (`assess_board`,
  `unregister_volumes`, `_staged_before`), `mia_buffers/workflows.py` (`_staged_gate`, `_approve_at_gate`,
  `_source_cached`, `_source_steps`)
- Test: `tests/test_mia_activities.py`, `tests/test_mia_workflows.py`, `tests/test_buffers_activities.py`,
  `tests/test_buffers_workflows.py`

**Interfaces:**
- Consumes: Task 1's `new_assessment`, `authorized`, `refused`, `POLICY_VERSION`; Task 2's `Registry.put_assessment`,
  `assessments`, `volumes`, `delete_volume`, `put_dataset`.
- Produces:
  - `@dataclass class AssessmentWritten: id: str; authorized: bool; reason: str = ""` (mia-data).
  - mia-data activity `assess(issue: int, round_id: str, gate: str, subject_kind: str, subject_ref: str, verdict: str, by: str, summary: str = "", supersedes: str | None = None) -> AssessmentWritten`:
    writes the assessment, then `authorized(registry.assessments(subject_kind=..., subject_ref=...))`; on
    `verdict == "approve"` also `put_dataset(record, assessment=id)`. No registry: `AssessmentWritten("", True, "no registry")`.
  - `@dataclass class BoardAssessed: assessments: list[str] = []; refused: dict[str, str] = {}` (record → reason).
  - mia-buffers activity `assess_board(issue: int, round_id: str, buffer_id: str, verdict: str, by: str) -> BoardAssessed`:
    one assessment per distinct artifact among the buffer's volumes (gate `staged`, subject `artifact`, ref = the
    artifact id, `source_digest` = the staged asset row's `source_digest` when present else `""`), then
    `authorized` per artifact; a refused artifact's records go in `refused`.
  - mia-buffers activity `unregister_volumes(buffer_id: str, record: str) -> list[str]`: `DELETE` every volume of
    the buffer whose id starts with `f"{record}/"`; returns the ids.
  - `BindingInfo.refused: str = ""` (`policy.refused(...)` over the artifact's assessments, read in `_staged_before`).
  - `DatasetStatus.artifact: str = ""` (set from `binding.artifact` when the binding is read).
  - `BufferState.approved_by: str = ""` (`human:<login>` of the accepted gate approve; `""` for `plan.auto_approve`).
  - Subject refs: a crop set is `f"{round_id}:{test.test_id}"`; a PR head is its sha; an artifact is its id.

- [ ] **Step 1: Write the failing mia-data activity test**

```python
async def test_assess_writes_the_human_verdict_and_asks_the_policy(tmp_path, token_server):
    t = _setup(tmp_path)
    t.a.registry = Registry(token_server, "t")
    await t.a.admit(7, "11")  # pins rec-1
    first = await t.a.assess(7, "i7-e11", "crops", "crop_set", "i7-e11:s-t", "approve", "human:alice", "data:approve")
    assert first.authorized and first.id and "human:alice" in first.reason
    rows = t.a.registry.assessments(subject_kind="crop_set", subject_ref="i7-e11:s-t")
    assert [r["verdict"] for r in rows] == ["approve"] and rows[0]["by"] == "human:alice" and rows[0]["gate"] == "crops"
    assert t.a.registry.dataset("rec-1")["assessment"] == first.id
    second = await t.a.assess(7, "i7-e11", "crops", "crop_set", "i7-e11:s-t", "reject", "human:bob", "data:reject")
    assert not second.authorized and second.reason.startswith("rejected by human:bob")
    t.a.registry = None
    assert await t.a.assess(7, "i7-e11", "crops", "crop_set", "x", "approve", "human:a") == AssessmentWritten("", True, "no registry")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pixi run pytest tests/test_mia_activities.py -k assess -v`
Expected: FAIL: `AttributeError: ... 'assess'`.

- [ ] **Step 3: Implement `assess`**

`at = time.time()`; `row = new_assessment(gate=..., subject_kind=..., subject_ref=..., verdict=..., by=..., at=at, summary=summary, supersedes=supersedes)`;
`registry.put_assessment(row)`; `auth = authorized(registry.assessments(subject_kind=..., subject_ref=...))`;
approve → `registry.put_dataset(self._pins(round_id).record, assessment=row["id"])` best effort. Return
`AssessmentWritten(row["id"], auth.ok, auth.reason)`.

- [ ] **Step 4: Run the mia-data activity tests**

Run: `pixi run pytest tests/test_mia_activities.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing mia-data workflow tests**

`Mocks`: `assess_authorized=True` and an `assess` activity recording `("assess", gate, subject_kind, subject_ref, verdict, by, summary)`
and returning `AssessmentWritten("as-1", self.assess_authorized, "" if self.assess_authorized else "rejected by human:bob at t: wrong channel")`.

```python
async def test_an_approve_is_written_as_an_assessment_before_the_crops_are_published(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m, eid="12")
        await idle(h, m, "done")
    (assessed,) = m.of("assess")  # the subject is <round id>:<test id>
    assert assessed[:2] == ("crops", "crop_set") and assessed[2].startswith("i7-e11:") and assessed[3:5] == ("approve", "human:alice")
    assert m.names().index("assess") < m.names().index("publish_crops")


async def test_an_approve_the_policy_refuses_publishes_nothing(env):
    m = Mocks(assess_authorized=False)
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "needs-human")
    assert "publish_crops" not in m.names()
    assert m.of("set_stage")[-1][1] == "approve recorded as as-1, but publication is not authorized: rejected by human:bob at t: wrong channel"


async def test_a_reject_is_written_as_an_assessment(env):
    m = Mocks()
    async with started(env, m) as h:
        await reject_review(h, m)
        await until(lambda: m.of("open_snippet_issue"))
    assert [a[3:5] for a in m.of("assess")] == [("reject", "human:alice")]


async def test_pr_head_verdicts_are_assessed_on_the_head_sha(env):
    m = Mocks(outcome="needs_snippet", pr_now=PrUpdate(42, "b" * 40, "open", None, 100, False))
    async with started(env, m) as h:
        await idle(h, m, "review")
        await approve_review(h, m)
        await until(lambda: any(a[0] == "merging" for a in m.of("set_stage")))
    assert m.of("assess")[-1][:5] == ("crops", "pr_head", "b" * 40, "approve", "human:alice")
```

(`ev()` in that file builds events with actor `alice`; check and adjust the login. For the PR-head test, model it
on `test_develop_happy_path`: the point is the `("crops", "pr_head", <sha>, "approve", ...)` tuple before the
`/approve` comment.)

- [ ] **Step 6: Run them to verify they fail**

Run: `pixi run pytest tests/test_mia_workflows.py -k "assess or policy_refuses" -v`
Expected: FAIL: no `assess` calls.

- [ ] **Step 7: Implement in `mia_data/workflows.py`**

A helper `async def _assess(self, verdict: str, event: LabelEvent, subject: tuple[str, str]) -> AssessmentWritten`
that returns `AssessmentWritten("", True)` when `not workflow.patched("l-assess")`, else calls `assess` with
`gate="crops"`, `by=f"human:{event.actor}"`, `summary=f"data:{verdict} by {event.actor}"`. `_from_review`:
approve → `written = await self._assess("approve", event, ("crop_set", f"{state.round_id}:{state.tests[-1].test_id}"))`;
`if not written.authorized: return await self._finish("needs-human", f"approve recorded as {written.id}, but publication is not authorized: {written.reason}")`;
else as before. reject → `await self._assess("reject", event, same subject)` then as before. `_approve_pr`:
`("pr_head", test.sha)` before the `/approve` comment, same refusal path; `_revise_pr`: a reject on
`("pr_head", test.sha)`.

- [ ] **Step 8: Run the mia-data workflow tests**

Run: `pixi run pytest tests/test_mia_workflows.py -v`
Expected: PASS, replays included.

- [ ] **Step 9: Write the failing buffers activity tests**

```python
async def test_assess_board_writes_one_assessment_per_artifact_and_names_the_refused(acts, server):
    """`server` is the file's live buffer-api fixture; stage one artifact per record first (see the
    register tests for `staged_into`/`planned_and_bound`)."""
    await planned_and_bound(acts)
    ...  # register two records' volumes with buffer i7-e11 under artifacts art-u (u937) and art-b (bcss)
    acts.registry.put_assessment(new_assessment(gate="staged", subject_kind="artifact", subject_ref=art_b, verdict="reject", by="human:bob", at=time.time(), summary="revoked"))
    out = await acts.assess_board(7, ROUND, "i7-e11", "approve", "human:alice")
    assert len(out.assessments) == 2 and out.refused == {BCSS: f"rejected by human:bob at {...}: revoked"}  # match on the prefix
    rows = acts.registry.assessments(subject_kind="artifact", subject_ref=art_u)
    assert [r["verdict"] for r in rows] == ["approve"] and rows[0]["gate"] == "staged" and rows[0]["by"] == "human:alice"


async def test_unregister_volumes_deletes_a_records_volumes_only(acts, server):
    ...  # the same two registered records
    assert await acts.unregister_volumes("i7-e11", U937) == [f"{U937}/v0.zarr"]
    assert [row["id"] for row in acts.registry.volumes("i7-e11")] == [f"{BCSS}/v0.zarr"]


async def test_read_binding_reports_a_refused_artifact(acts, server):
    ...  # a staged artifact for U937 with an ok stage.json on disk, as `test_a_pyramid_staged_earlier...` sets up
    acts.registry.put_assessment(new_assessment(gate="staged", subject_kind="artifact", subject_ref=artifact, verdict="reject", by="human:bob", at=time.time(), summary="revoked"))
    binding = await acts.read_binding(ROUND, U937)
    assert binding.staged and binding.refused.startswith("human:bob at ") and binding.refused.endswith(": revoked")
```

(Fill the `...` from the file's existing helpers; the assertions are the contract.)

- [ ] **Step 10: Run them to verify they fail**

Run: `pixi run pytest tests/test_buffers_activities.py -k "assess_board or unregister or refused_artifact" -v`
Expected: FAIL: `AttributeError` / `refused` unknown field.

- [ ] **Step 11: Implement the buffers activities**

`assess_board`: `rows = registry.volumes(buffer_id)`; `artifacts = {row["artifact"]: [records...]}`; per artifact
`source_digest = (registry.asset("staged", artifact) or {}).get("source_digest") or ""`; write, then
`auth = authorized(registry.assessments(subject_kind="artifact", subject_ref=artifact))`; refused →
`refused[record] = auth.reason` for each of its records. `unregister_volumes`: as the interface says.
`_staged_before`: when found, `refused = policy.refused(registry.assessments(subject_kind="artifact", subject_ref=artifact)) or ""`
and return it as a third value; `read_binding` sets `BindingInfo.refused`.

- [ ] **Step 12: Run the buffers activity tests**

Run: `pixi run pytest tests/test_buffers_activities.py -v`
Expected: PASS.

- [ ] **Step 13: Write the failing buffers workflow tests**

`Mocks`: `assess_refused=None` (record → reason), an `assess_board` activity recording
`("assess_board", buffer_id, verdict, by)` and returning `BoardAssessed(["as-1"], self.assess_refused or {})`, and
`unregister_volumes` recording `("unregister_volumes", buffer_id, record)` and returning `[f"{record}/v0.zarr"]`.

```python
async def test_the_gate_approve_is_assessed_per_artifact_before_the_approved_event(env):
    m = Mocks()
    async with started(env, m, signal="go") as h:
        await idle(h, m, "staged")
        await h.signal(MiaBuffer.dropped, "bcss")
        await until(lambda: any("`bcss` | servable" in c[0] for c in staged_comments(m)), m=m)
        await idle(h, m, "staged")
        await approve_staged(h, m)
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
    assert m.of("assess_board") == [("i7-e11", "approve", "human:alice")] and state.approved_by == "human:alice"
    assert m.names().index("assess_board") < m.names().index("buffer_event")


async def test_a_refused_artifact_is_unregistered_and_its_dataset_fails_at_the_approve(env):
    m = Mocks(plan=TWO, assess_refused={"a": "rejected by human:bob at t: revoked"})
    async with started(env, m, signal="go") as h:
        await idle(h, m, "staged")
        await approve_staged(h, m)
        await idle(h, m, "ready")  # TWO's min_fraction is 0.5: b alone keeps the group at its floor
        state = await h.query(MiaBuffer.status)
    assert m.of("unregister_volumes") == [("i7-e11", "a")]
    assert dataset(state, "a").status == "failed" and dataset(state, "a").reason == "refused: rejected by human:bob at t: revoked"
    assert state.accepted == {"TEM": 0.5}


async def test_every_artifact_refused_is_needs_human_not_ready(env):
    m = Mocks(plan=ONLY_U937, assess_refused={"u937": "rejected by human:bob at t: revoked"})
    async with started(env, m, signal="go") as h:
        await idle(h, m, "staged")
        await approve_staged(h, m)
        await idle(h, m, "needs-human")
    assert "buffer_event" not in m.names() and "refused" in m.of("set_stage")[-1][1]


async def test_a_refused_staged_artifact_is_not_reused(env):
    staged = BindingInfo(True, "/s/u937.yaml", "bh", ["zenodo.org"], staged=True, artifact="art-u", refused="human:bob at t: revoked")
    m = Mocks(plan=ONLY_U937, bindings={"u937": staged})
    async with started(env, m, signal="go") as h:
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "u937").status == "failed" and dataset(state, "u937").reason == "staged artifact art-u is refused: human:bob at t: revoked"
    assert "read_stage" not in m.names() and "submit" not in m.names()
```

(`ev()` builds label events with actor `alice` in that file too; check. `auto_approve` rounds skip
`assess_board`: `test_auto_approve_full_set_reaches_ready_without_a_label` must still pass with no `assess_board`
call.)

- [ ] **Step 14: Run them to verify they fail**

Run: `pixi run pytest tests/test_buffers_workflows.py -k "assessed or refused" -v`
Expected: FAIL.

- [ ] **Step 15: Implement in `mia_buffers/workflows.py`**

`_staged_gate`: where an approve event is accepted, `state.approved_by = f"human:{event.actor}"` before
`_approve_at_gate`; the auto-approve branches leave it `""`. `_approve_at_gate`: behind `l-assess`, when
`state.approved_by`: `board = await self._act("assess_board", state.issue, state.round_id, state.buffer_id, "approve", state.approved_by, seconds=600, result_type=BoardAssessed)`;
for each refused record: `await self._act("unregister_volumes", state.buffer_id, record, seconds=300, result_type=list[str])`,
`await self._fail(dataset, f"refused: {reason}")`; then `if not self._ready_now(): return await self._finish("needs-human", "every artifact refused at the approve: " + "; ".join(...))`;
then the existing floor computation and `approved` event. `_source_cached` and `_source_steps`: after the
`binding.found` check, `if binding.refused and workflow.patched("l-assess"): return await self._fail(dataset, f"staged artifact {binding.artifact} is refused: {binding.refused}")`;
set `dataset.artifact = binding.artifact` right after `read_binding` in both.

- [ ] **Step 16: Run the buffers workflow tests**

Run: `pixi run pytest tests/test_buffers_workflows.py -v`
Expected: PASS, replays included.

- [ ] **Step 17: Document**

`docs/development.md` rules: **Approvals are assessments, authorization is a function.** one paragraph (the table,
`by: human:<login>`, `authorized()`, where it is called: `publish_crops`, `/approve <head>`, the `approved` event,
`_reuse`; `plan.auto_approve` writes none until P removes it). `docs/mia-data-workflow.md` §5 "Validation": one
sentence that the label is recorded as an assessment and publication is authorized from it.

- [ ] **Step 18: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add mia_data/models.py mia_data/activities.py mia_data/workflows.py mia_buffers/models.py mia_buffers/activities.py mia_buffers/workflows.py tests/test_mia_activities.py tests/test_mia_workflows.py tests/test_buffers_activities.py tests/test_buffers_workflows.py docs/development.md docs/mia-data-workflow.md
git commit -m "gates: human labels are written as assessments; publication, the approved event and artifact reuse ask the policy"
```

---

### Task 6: `data:revoke`: the dataset row, quarantine, and the buffers that drew from it

**Files:**
- Modify: `mia_data/config.py` (`HUMAN_LABELS["revoke"]`), `mia_data/models.py` (`Revocation`, `LabelEvent` docstring),
  `mia_data/publish.py` (`prepared_row`, `REVOKED_FILE`), `mia_data/activities.py` (`revoke_dataset`, `dataset_fields`, `publish_crops`),
  `mia_data/workflows.py` (`revoke` signal, `_revokes`, `_revoke`, `run`, `_review`, `_refuse_queued`),
  `mia_buffers/models.py` (`RevokedSignal`), `mia_buffers/activities.py` (`_keeper_buffer`, `_keeper_evict`),
  `mia_buffers/workflows.py` (`revoked` signal, `_on_lifecycle`, `_revoked`)
- Test: `tests/test_mia_publish.py`, `tests/test_mia_activities.py`, `tests/test_mia_workflows.py`,
  `tests/test_buffers_activities.py`, `tests/test_buffers_workflows.py`, `tests/test_buffers_sync.py`

**Interfaces:**
- Consumes: Tasks 1, 2, 5 (`assess`-style writes, `Registry.assets(kind, record=)`, `put_asset`, `put_dataset`,
  `volumes`, `delete_volume`, `status()["quarantined"]`, `unregister_volumes`).
- Produces:
  - `HUMAN_LABELS["revoke"] = ("b60205", "Revoke this dataset's published crops: buffers that drew from it are degraded, nothing is deleted")`;
    the poller delivers it as the `revoke` signal (it iterates `HUMAN_LABELS`; `refuse_label` accepts it the same way).
  - `mia_data.publish.REVOKED_FILE = "revoked.json"`; `prepared_row(dataset_dir)` returns `None` when it exists.
  - `@dataclass class Revocation: assessment: str; artifacts: list[str] = []; buffers: list[str] = []` (mia-data).
  - mia-data activity `revoke_dataset(issue: int, round_id: str, actor: str, event_id: str) -> Revocation`.
  - `dataset_fields` returns `{"state": "revoked", "data_stage": stage, "issue": issue}` while `revoked.json` exists;
    `publish_crops` removes `revoked.json` after a successful publish (the repair of §6 step 5).
  - `@dataclass class RevokedSignal: records: list[str]; volumes: list[str]; reason: str; buffer: str = ""` (mia-buffers);
    the `MiaBuffer.revoked(signal)` signal; the keeper sends it for a ready or idle buffer whose `status` lists
    quarantined volumes (`report.signalled` line `"revoked <buffer> <records>"`).
  - `_keeper_evict` passes `quarantine_s=self.cfg.autonomy.quarantine_days * 86400`.

- [ ] **Step 1: Write the failing publish and sync tests**

`tests/test_mia_publish.py`:

```python
def test_a_revoked_folder_is_not_prepared(tmp_path):
    from mia_data.publish import REVOKED_FILE, prepared_row
    from tests.test_buffers_sync import write_prepared

    write_prepared(tmp_path / "f", "rec")
    assert prepared_row(tmp_path / "f")["state"] == "prepared"
    (tmp_path / "f" / REVOKED_FILE).write_text('{"by": "human:a"}')
    assert prepared_row(tmp_path / "f") is None
```

`tests/test_buffers_sync.py`: in `test_scan_storage_skips_dotdirs_and_duplicates`, add a folder `revoked` written
with `write_prepared` plus `revoked.json`, and assert it is not in `found`.

- [ ] **Step 2: Run them to verify they fail**

Run: `pixi run pytest tests/test_mia_publish.py -k revoked tests/test_buffers_sync.py -k scan_storage -v`
Expected: FAIL: `ImportError: REVOKED_FILE`; the revoked folder is found.

- [ ] **Step 3: Implement `prepared_row`**

`if (dataset_dir / REVOKED_FILE).is_file(): return None` before reading the binding.

- [ ] **Step 4: Run them**

Expected: PASS.

- [ ] **Step 5: Write the failing mia-data activity test**

```python
async def test_revoke_dataset_rejects_quarantines_and_finds_the_buffers(tmp_path, token_server):
    t = _setup(tmp_path)
    registry = t.a.registry = Registry(token_server, "t")
    await t.a.admit(7, "11")
    folder = t.a._dataset_dir("i7-e11", t.a._pins("i7-e11"))
    write_prepared(folder, "rec-1")
    approve = await t.a.assess(7, "i7-e11", "crops", "crop_set", "i7-e11:s-t", "approve", "human:alice")
    registry.put_asset("staged", "art1", {"record": "rec-1", "state": "ready", "path": "/x", "bytes": 1})
    registry.put_asset("staged", "art2", {"record": "rec-1", "state": "ready", "path": "/y", "bytes": 1})
    registry.put_asset("staged", "other", {"record": "rec-2", "state": "ready"})
    registry.request("PUT", "/buffers/i3-e4", json=CFG)
    registry.request("PUT", "/buffers/i3-e4/volumes/rec-1%2Fv0.zarr", json={**body("rec-1/v0.zarr"), "artifact": "art1"})
    out = await t.a.revoke_dataset(7, "i7-e11", "alice", "77")
    assert out.artifacts == ["art1", "art2"] and out.buffers == ["i3-e4"]
    row = registry.dataset("rec-1")
    assert (row["state"], row["revoked_by"], row["assessment"]) == ("revoked", "human:alice", out.assessment)
    crop_set = registry.assessments(subject_kind="crop_set", subject_ref="i7-e11:s-t")
    assert crop_set[0]["verdict"] == "reject" and crop_set[0]["supersedes"] == approve.id and crop_set[0]["id"] == out.assessment
    assert registry.assessments(subject_kind="artifact", subject_ref="art1")[0]["verdict"] == "reject"
    art1 = registry.asset("staged", "art1")
    assert art1["quarantined"] is True and art1["quarantined_by"] == "human:alice" and "quarantined" not in registry.asset("staged", "other")
    assert json.loads((folder / "revoked.json").read_text())["by"] == "human:alice"
    assert t.a.dataset_fields(7, "i7-e11", "done")["state"] == "revoked"  # the row stays revoked whatever the stage
    assert registry.sync_datasets()["revoked_kept"] >= 0 and registry.dataset("rec-1")["state"] == "revoked"
    assert await t.a.revoke_dataset(7, "i7-e11", "alice", "77") == out  # a retry: the same assessment, nothing doubled
```

(`CFG`/`body` from `tests/test_buffers_api.py`; `write_prepared` from `tests/test_buffers_sync.py`; the
`token_server` fixture is in `tests/test_buffers_register.py`, already imported where needed. The retry
idempotence rests on the marker: `revoke_dataset` reads `revoked.json` first and returns what it recorded.)

- [ ] **Step 6: Run it to verify it fails**

Run: `pixi run pytest tests/test_mia_activities.py -k revoke_dataset -v`
Expected: FAIL: `AttributeError: ... 'revoke_dataset'`.

- [ ] **Step 7: Implement `revoke_dataset`, `dataset_fields`, `publish_crops`**

`revoke_dataset`: `record = self._pins(round_id).record`; `folder = self._dataset_dir(round_id, pins)`; if
`revoked.json` exists, return `Revocation(**json.loads(...)["revocation"])`. Else: `row = registry.dataset(record) or {}`;
`approve_id = row.get("assessment")`; the subject: the approve assessment's `subject` when `approve_id` resolves,
else `("crop_set", f"{round_id}:revoked")`; `by = f"human:{actor}"`; `at = time.time()`; write the crop-set reject
(`summary=f"data:revoke by {actor} (event {event_id})"`, `supersedes=approve_id`); `assets = registry.assets("staged", record=record)["assets"]`;
for each asset with a `data` row: an artifact reject (`subject artifact`, same summary, `supersedes=None`), then
`put_asset("staged", key, {"quarantined": True, "quarantined_at": at, "quarantined_by": by})`; `buffers = sorted({ref for asset in assets for ref in asset["refs"]})`;
`put_dataset(record, state="revoked", revoked_at=<ISO>, revoked_by=by, assessment=<crop-set reject id>)`;
`write_atomic(folder / REVOKED_FILE, json.dumps({"record", "by", "at", "issue", "event_id", "revocation": asdict(Revocation(...))}))`
last. `dataset_fields`: check `REVOKED_FILE` before `prepared_row`. `publish_crops`: after `crop_publish.publish`
returns, `(dataset_dir / REVOKED_FILE).unlink(missing_ok=True)`.

- [ ] **Step 8: Run the mia-data activity tests**

Expected: PASS.

- [ ] **Step 9: Write the failing mia-data workflow tests**

`Mocks`: a `revoke_dataset` activity recording `("revoke_dataset", round_id, actor, event_id)` and returning
`Revocation("as-r", ["art1"], ["i3-e4"])`.

```python
async def test_revoke_after_done_ends_in_needs_human_and_lists_the_blast_radius(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
        await h.signal(MiaDataDataset.revoke, ev("revoke", "77", "2030-01-01T00:00:00Z"))
        await idle(h, m, "needs-human")
    assert m.of("revoke_dataset") == [("i7-e11", "alice", "77")]
    detail, extra = m.of("set_stage")[-1][1:3]
    assert detail == "revoked by alice: the dataset row is `revoked`, 1 artifact quarantined; a new `data:go` repairs it"
    assert "art1" in extra and "i3-e4" in extra


async def test_revoke_during_review_is_taken_and_ends_the_round(env):
    m = Mocks()
    async with started(env, m) as h:
        await idle(h, m, "review")
        await h.signal(MiaDataDataset.revoke, ev("revoke", "77", "2030-01-01T00:00:00Z"))
        await idle(h, m, "needs-human")
    assert m.of("revoke_dataset") and "publish_crops" not in m.names()


async def test_revoke_with_nothing_published_is_refused(env):
    m = Mocks(outcome="blocked")
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
        await h.signal(MiaDataDataset.revoke, ev("revoke", "77", "2030-01-01T00:00:00Z"))
        await until(lambda: m.of("refuse_label"))
    assert m.of("refuse_label")[-1][:2] == ("data:revoke", "nothing to revoke: this issue has published no crops")
    assert "revoke_dataset" not in m.names()
```

(The third test needs the workflow to know whether anything was published: `DatasetState.published: bool = False`,
set `True` in `_publish_set` after `publish_crops` succeeds (and in `_produce`), carried across rounds; a revoke is
refused while it is `False`. A pre-L history has `published == False` even when it did publish: for those, the
label is refused once with the same message and the operator re-runs `data:go` then revokes; note this in the
runbook.)

- [ ] **Step 10: Run them to verify they fail**

Expected: FAIL: `AttributeError: MiaDataDataset has no attribute 'revoke'`.

- [ ] **Step 11: Implement in `mia_data/workflows.py`**

`REVOKE = "data:revoke"`; signal `revoke(event)` appends to `self.revoke_events`; `_labels()` includes it;
`_refuse_queued` leaves it alone. `async def _revokes(self) -> None`: pops every queued revoke and, unless
`_seen`, runs `await self._guard(self._revoke(event))`. Call it in `run()` right after the `wait_condition`, and
in `_review()` after its `wait_condition`: when a revoke was queued, run `_revokes()` and return `("revoked", None)`,
which `_from_review` ignores (the round is already at `needs-human`). `_revoke(event)`:
`if not workflow.patched("l-revoke"): return`; `if not state.published or not state.round_id: return await self._refuse(REVOKE, "nothing to revoke: this issue has published no crops", event)`;
`out = await self._act("revoke_dataset", state.issue, state.round_id, event.actor, event.event_id, seconds=900, result_type=Revocation)`;
`extra = render of two lists: "**Quarantined artifacts:** art1, ..." and "**Buffers that drew from it:** `i3-e4`, ..."` (empty lists say "none");
`await self._finish("needs-human", f"revoked by {event.actor}: the dataset row is `revoked`, {n} artifact{s} quarantined; a new `data:go` repairs it", extra=extra)`.

- [ ] **Step 12: Run the mia-data workflow tests**

Expected: PASS, replays included.

- [ ] **Step 13: Write the failing keeper and buffers workflow tests**

`tests/test_buffers_activities.py`, extend `test_keeper_signals_degraded_idle_and_retire`: register
`buffer("i16-e17", ["ready"], [("rec-h/v0", True), ("rec-i/v0", True)])` with volume bodies carrying
`"artifact": "art-h"` / `"art-i"`, then `registry.put_asset("staged", "art-h", {"quarantined": True, "quarantined_at": 1.0})`;
expect `"revoked i16-e17 rec-h"` in `report.signalled` and
`(acts.cfg.workflow_id(16), "revoked", [RevokedSignal(["rec-h"], ["rec-h/v0"], "quarantined: rec-h/v0", "i16-e17")])` in `acts.client.sent`.
Also assert in `test_keep_evicts_a_free_staged_asset_under_quota` (or a new sibling) that a free, quarantined
asset younger than `quarantine_days` is not evicted and one older is.

`tests/test_buffers_workflows.py`:

```python
async def test_a_revoked_signal_unregisters_the_record_and_serves_on_at_the_floor(env):
    m = Mocks(plan=TWO)
    async with started(env, m) as h:
        await approve(h, m)
        await ready(h, m)
        await h.signal(MiaBuffer.revoked, RevokedSignal(["a"], ["a/v0.zarr"], "quarantined: a/v0.zarr", "i7-e11"))
        await until(lambda: any("`a` failed" in c[0] for c in m.of("comment")), m=m)
        state = await h.query(MiaBuffer.status)
    assert m.of("unregister_volumes") == [("i7-e11", "a")]
    assert dataset(state, "a").status == "failed" and dataset(state, "a").reason == "revoked: quarantined: a/v0.zarr"
    assert state.stage == "ready" and any("serving without `a`" in c[0] for c in m.of("comment"))


async def test_a_revoked_signal_below_the_floor_is_needs_human(env):
    m = Mocks(plan=ONLY_U937)
    async with started(env, m) as h:
        await approve(h, m)
        await ready(h, m)
        await h.signal(MiaBuffer.revoked, RevokedSignal(["u937"], ["u937/v0.zarr"], "quarantined: u937/v0.zarr", "i7-e11"))
        await idle(h, m, "needs-human")
    assert "revoked" in m.of("set_stage")[-1][1] and m.of("unregister_volumes") == [("i7-e11", "u937")]
```

- [ ] **Step 14: Run them to verify they fail**

Expected: FAIL.

- [ ] **Step 15: Implement the keeper step and the workflow handler**

`_keeper_buffer`: for `ready`/`idle` buffers, `status["quarantined"]` → records (`volume.split("/", 1)[0]`) →
`signals.append(("revoked", [RevokedSignal(records, quarantined, "quarantined: " + ", ".join(quarantined), buffer_id)], ",".join(records)))`.
Workflow: signal `revoked(signal: RevokedSignal)` → `self.lifecycle.append(("revoked", signal, signal.buffer))`;
`_on_lifecycle`: `elif kind == "revoked": return await self._revoked(payload)`; `_revoked`: hit = servable datasets
in `signal.records`; none → `True`; for each: `unregister_volumes`, `_fail(dataset, f"revoked: {signal.reason}")`;
then `if not self._ready_now(): await self._finish("needs-human", f"revoked ({signal.reason}): {records}; these weighting groups are now below their floor: {', '.join(self._below())}"); return False`;
else `_comment(f"serving without {records}: revoked ({signal.reason})", f"revoked {round_id} {' '.join(records)}")` and `True`.
Behind `workflow.patched("l-revoke")` (a history from before never receives the signal, so the marker guards
only the `_on_lifecycle` branch). `_keeper_evict`: pass `quarantine_s`.

- [ ] **Step 16: Run both buffers test files**

Expected: PASS, replays included.

- [ ] **Step 17: Document**

`docs/development.md` rules: **Revocation deletes nothing and stops everything.** one paragraph (`data:revoke` any
time; the reject assessments; `revoked.json`; the `revoked` row that `sync_datasets` never touches; quarantined
assets the keeper holds `quarantine_days`; the keeper's `revoked` signal; unregistered volumes; serving continues
at the floor, `buffer:retire` stops it (§12.10); repair by `data:go`). `docs/mia-data-workflow.md`: a
**Revocation.** paragraph in §5 and `data:revoke` in "How the issue reads". `docs/autodev-runbook.md` is not
touched (autodev has no gate here).

- [ ] **Step 18: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add mia_data/config.py mia_data/models.py mia_data/publish.py mia_data/activities.py mia_data/workflows.py mia_buffers/models.py mia_buffers/activities.py mia_buffers/workflows.py tests/test_mia_publish.py tests/test_mia_activities.py tests/test_mia_workflows.py tests/test_buffers_activities.py tests/test_buffers_workflows.py tests/test_buffers_sync.py docs/development.md docs/mia-data-workflow.md
git commit -m "revocation: data:revoke writes reject assessments, quarantines the artifacts and flips the row to revoked; the keeper degrades the buffers that drew from it"
```

---

### Task 7: `buffer:revoke <record>`: revoking staged volumes from the buffer issue

**Files:**
- Modify: `mia_buffers/config.py` (`HUMAN_LABELS["revoke"]`), `mia_buffers/models.py` (`RevokedArtifacts`),
  `mia_buffers/activities.py` (`revoke_targets`, `revoke_artifacts`), `mia_buffers/workflows.py` (`revoke` signal,
  `_on_lifecycle`)
- Test: `tests/test_buffers_activities.py`, `tests/test_buffers_workflows.py`

**Interfaces:**
- Consumes: Task 6's `_revoked`, `RevokedSignal`; Task 5's `unregister_volumes`; `GitHub.issue_comments`.
- Produces:
  - `HUMAN_LABELS["revoke"] = ("b60205", "Revoke the records named as `/revoke <record>` lines in your comment: their volumes leave this buffer")`;
    the poller delivers it as the `revoke` signal (a label event), queued on `lifecycle` as `("revoke", event, "")`.
  - `REVOKE_RE = re.compile(r"^/revoke\s+([a-z0-9][a-z0-9._-]{0,120})\s*$", re.M)` in `mia_buffers/activities.py`.
  - Activity `revoke_targets(issue: int, actor: str, until: str) -> list[str]`: the records named by `/revoke`
    lines in `actor`'s comments on the issue within 7 days before `until` (the label's time), deduplicated, in
    order of first mention.
  - `@dataclass class RevokedArtifacts: assessments: list[str] = []; volumes: list[str] = []; records: list[str] = []`.
  - Activity `revoke_artifacts(round_id: str, buffer_id: str, records: list[str], by: str, summary: str) -> RevokedArtifacts`:
    for each distinct artifact among the buffer's volumes of those records, a `reject` assessment (gate `staged`,
    subject `artifact`) and `quarantined: true` on the asset row; returns what it touched (records with no volume in
    this buffer are left out).

- [ ] **Step 1: Write the failing activity tests**

```python
async def test_revoke_targets_reads_the_actors_revoke_lines(acts):
    gh = acts.gh_issues
    gh.comments[7] = [  # FakeGH: adapt to how the file seeds comments
        {"user": {"login": "alice"}, "created_at": "2026-11-10T10:00:00Z", "body": "/revoke rec-a\nlooks wrong\n/revoke rec-b"},
        {"user": {"login": "bob"}, "created_at": "2026-11-10T10:05:00Z", "body": "/revoke rec-c"},
        {"user": {"login": "alice"}, "created_at": "2026-11-01T10:00:00Z", "body": "/revoke rec-old"},
        {"user": {"login": "alice"}, "created_at": "2026-11-10T12:00:00Z", "body": "/revoke rec-later"},
    ]
    assert await acts.revoke_targets(7, "alice", "2026-11-10T11:00:00Z") == ["rec-a", "rec-b"]


async def test_revoke_artifacts_rejects_and_quarantines_this_buffers_artifacts_only(acts, server):
    ...  # two records registered with buffer i7-e11 (art-u, art-b), a third artifact art-x of U937 in another buffer
    out = await acts.revoke_artifacts(ROUND, "i7-e11", [U937, "not-here"], "human:alice", "buffer:revoke by alice")
    assert out.records == [U937] and out.volumes == [f"{U937}/v0.zarr"] and len(out.assessments) == 1
    assert acts.registry.asset("staged", art_u)["quarantined"] is True and "quarantined" not in acts.registry.asset("staged", art_x)
    assert acts.registry.assessments(subject_kind="artifact", subject_ref=art_u)[0]["verdict"] == "reject"
```

- [ ] **Step 2: Run them to verify they fail**

Expected: FAIL: `AttributeError`.

- [ ] **Step 3: Implement the two activities**

`revoke_targets`: `comments = await self.gh_issues.issue_comments(issue)`; keep those by `actor` with
`until - 7 days <= created_at <= until` (ISO strings compare as text after normalising `Z`); `REVOKE_RE.findall`;
`dict.fromkeys` for order and dedup. `revoke_artifacts`: `rows = registry.volumes(buffer_id)`; the artifacts of
rows whose `record` is in `records`; per artifact write the reject (`new_assessment(..., at=time.time(), summary=summary)`),
`put_asset` the quarantine fields; collect.

- [ ] **Step 4: Run the activity tests**

Expected: PASS.

- [ ] **Step 5: Write the failing workflow tests**

`Mocks`: `revoke_targets_answer=None` and activities `revoke_targets` (records `("revoke_targets", actor, until)`,
returns `self.revoke_targets_answer or []`) and `revoke_artifacts` (records `("revoke_artifacts", buffer_id, records, by)`,
returns `RevokedArtifacts(["as-r"], [f"{r}/v0.zarr" for r in records], list(records))`).

```python
async def test_buffer_revoke_label_quarantines_the_named_records_and_degrades_the_buffer(env):
    m = Mocks(plan=TWO, revoke_targets_answer=["a", "zz"])
    async with started(env, m) as h:
        await approve(h, m)
        await ready(h, m)
        await h.signal(MiaBuffer.revoke, ev("revoke", "77", "2030-01-01T00:00:00Z"))
        await until(lambda: any("`a` failed" in c[0] for c in m.of("comment")), m=m)
        state = await h.query(MiaBuffer.status)
    assert m.of("revoke_targets") == [("alice", "2030-01-01T00:00:00Z")]
    assert m.of("revoke_artifacts") == [("i7-e11", ["a"], "human:alice")]  # zz is not in the pool: dropped
    assert m.of("unregister_volumes") == [("i7-e11", "a")] and dataset(state, "a").status == "failed"
    assert state.stage == "ready"


async def test_buffer_revoke_without_revoke_lines_is_refused(env):
    m = Mocks(plan=TWO)
    async with started(env, m) as h:
        await approve(h, m)
        await ready(h, m)
        await h.signal(MiaBuffer.revoke, ev("revoke", "77", "2030-01-01T00:00:00Z"))
        await until(lambda: m.of("refuse_label"), m=m)
    assert m.of("refuse_label")[-1][:2] == ("buffer:revoke", "name the records first: `/revoke <record>` lines in a comment of yours, then the label")


async def test_buffer_revoke_outside_the_ready_loop_is_refused(env):
    m = Mocks(plan=ONLY_U937)
    async with started(env, m) as h:
        await idle(h, m, "plan-review")
        await h.signal(MiaBuffer.revoke, ev("revoke", "77", "2030-01-01T00:00:00Z"))
        await until(lambda: m.of("refuse_label"), m=m)
    assert m.of("refuse_label")[-1][0] == "buffer:revoke"
```

- [ ] **Step 6: Run them to verify they fail**

Expected: FAIL.

- [ ] **Step 7: Implement in the workflow**

Signal `revoke(event: LabelEvent)` → `self.lifecycle.append(("revoke", event, ""))` (the existing
`_refuse_lifecycle` refuses label events outside ready/idle, as `keep`/`retire` are). `_on_lifecycle`:
`elif kind == "revoke": return await self._revoke_label(payload)`. `_revoke_label(event)`: behind `l-revoke`;
`targets = await self._act("revoke_targets", state.issue, event.actor, event.at, seconds=300, result_type=list[str])`;
`records = [r for r in targets if any(d.record == r for d in state.datasets)]`; none → `_refuse("buffer:revoke", <the message>, event)`, `True`;
`out = await self._act("revoke_artifacts", state.round_id, state.buffer_id, records, f"human:{event.actor}", f"buffer:revoke by {event.actor}", seconds=600, result_type=RevokedArtifacts)`;
then `return await self._revoked(RevokedSignal(out.records, out.volumes, f"buffer:revoke by {event.actor}", state.buffer_id))`.

- [ ] **Step 8: Run the buffers tests**

Expected: PASS, replays included.

- [ ] **Step 9: Document**

`docs/development.md` revocation paragraph: the `buffer:revoke` sentence (label plus `/revoke <record>` lines by
the same person within a week; records not in the buffer are ignored; the effect is the keeper's `revoked` path
without waiting for a pass). `mia_buffers/ui/index.html` labels table: a `buffer:revoke` row.

- [ ] **Step 10: Full checks and commit**

```bash
git add mia_buffers/config.py mia_buffers/models.py mia_buffers/activities.py mia_buffers/workflows.py tests/test_buffers_activities.py tests/test_buffers_workflows.py docs/development.md mia_buffers/ui/index.html
git commit -m "buffers: buffer:revoke with /revoke <record> lines quarantines those volumes and degrades the buffer"
```

---

### Task 8: the page shows revoked, quarantined and paused; the docs close the loop

**Files:**
- Modify: `mia_buffers/ui/index.html` (STATES text, `BAD` set, the records table's state cell, the staged assets
  table, the help's labels and troubleshooting tables), `docs/development.md` (the "Serving is authorized" rule
  from K gains the assessments sentence; a "Decisions" note pointing at `GET /assessments` until the view lands
  in M), `README.md` (the label lists), `docs/mia-data-workflow.md` ("How the issue reads")
- Test: `tests/test_buffers_ui.py`

**Interfaces:**
- Consumes: `GET /assets/staged` rows' `quarantined`, `GET /datasets` rows' `state == "revoked"`, `GET /buffers/<id>/status`'s
  `quarantined` (consumer route, already requested by the page's buffer view, check `requested_paths()`).

- [ ] **Step 1: Extend the UI test**

```python
def test_page_names_the_new_states_and_labels() -> None:
    for needle in ('"revoked":', "quarantined", "buffer:revoke", "data:revoke", "paused by the operator"):
        assert needle in PAGE, needle
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pixi run pytest tests/test_buffers_ui.py -v`
Expected: FAIL on `"revoked":`.

- [ ] **Step 3: Edit the page**

STATES: `"revoked": "Withdrawn by a human after publication (data:revoke): nothing is selected or reused from it; a new data:go repairs it."`
and `"quarantined": "Kept on disk for autonomy.quarantine_days after a revocation; the keeper does not evict it while an investigation may need it."`;
`BAD` gains `"revoked"` and `"quarantined"`; the state cell shows `assetPill("quarantined")` for a staged asset row
with `data.quarantined`; the staged assets table adds the same pill; the labels table gains `data:revoke` and
`buffer:revoke` rows; troubleshooting gains a row for a `needs-human` whose comment starts "paused by the
operator" (remedy: `mia resume`, then the label again) and one for "revoked by" (remedy: fix the snippet or the
binding, then `data:go`).

- [ ] **Step 4: Run the UI tests**

Expected: PASS (routes unchanged, no token, no new host).

- [ ] **Step 5: Docs**

`README.md`: `data:revoke` and `buffer:revoke` in the label lists; `mia pause`/`mia resume` in the ops commands.
`docs/development.md`: the K rule "Serving is authorized, never inferred" gains "and an approve is recorded as an
assessment (`GET /assessments`), the Decisions view arrives with milestone M". `docs/mia-data-workflow.md` "How
the issue reads": `data:revoke` among the human labels.

- [ ] **Step 6: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add mia_buffers/ui/index.html tests/test_buffers_ui.py docs/development.md README.md docs/mia-data-workflow.md
git commit -m "page and docs: revoked datasets, quarantined artifacts, the revoke labels and the pause"
```

---

### Task 9: live verification on the dev host

Not code; the acceptance row of spec §14 L. After Tasks 1–8 are committed: `mia exec mia-data init-labels`,
`mia exec mia-buffers init-labels` (the new `revoke` labels), then
`pixi run mia restart buffer-api mia-buffers mia-data` with no round in `sourcing` (buffer-api first: the new
routes; a worker calling `PUT /assessments` before the restart gets a 404 and retries).

- [ ] **Step 1: A human approve is an assessment**

Add `data:approve` on a dataset issue in `data:review`. Run:
`curl -s "<service.url>/assessments?subject_kind=crop_set" | python3 -c 'import json,sys; [print(a["id"], a["gate"], a["verdict"], a["by"], a["subject"]["ref"]) for a in json.load(sys.stdin)["assessments"]]'`
Expected: one line, `crops approve human:<your login> <round>:<test id>`; the dataset row (`GET /datasets/<record>`)
carries that `assessment` id and is `prepared`.

- [ ] **Step 2: `data:revoke` quarantines, degrades, and the sync leaves it revoked**

Pick a prepared dataset a ready buffer draws from. Add `data:revoke`. Expected, in order: the dataset issue goes
`data:needs-human` with the artifact ids and the buffer ids; `GET /datasets/<record>` is `revoked`;
`GET /assets/staged?record=<record>` rows carry `quarantined: true`; within one `keeper.minutes` the buffer issue
comments "serving without `<record>`" (or goes `needs-human` when the group fell below its floor);
`GET /buffers/<id>/volumes` no longer lists the record. Then `mia exec mia-buffers datasets sync` and
`GET /datasets/<record>` is still `revoked`.

- [ ] **Step 3: A root over cap stops with the comment**

On a scratch deployment or with `autonomy.root.max_usd: 0.5` in `mia.yaml` (then `mia restart mia-data`), add
`data:go` on a scratch dataset issue. Expected: `data:needs-human` with a comment beginning
`root budget: root <repo>#<n>: usd ... would exceed max_usd 0.5` before any job is submitted. Restore the cap and
restart.

- [ ] **Step 4: Pause**

`mia pause`; add `data:go` on the scratch issue. Expected: `data:needs-human` "paused by the operator" with no
job submitted; `mia resume`; `data:go` again runs.

- [ ] **Step 5: Note the result**

Append one line per step to `notes/` (untracked) with the date, the issue numbers and what was observed. Nothing
to commit.

---

## Self-review notes

- Spec §5: the table and routes (Task 2), the schema and `authorized()` (Task 1), human labels as assessments and
  the callers (publication, `/approve <head>`, `approved`, `_reuse`; Task 5). `policy_version` staleness is in
  `authorized()` (Task 1); the re-judging on bump is M's.
- Spec §6: steps 1–5 are Task 6 (`revoked` row, `sync_datasets`, quarantine, blast radius comment, the keeper's
  `revoked` signal, unregister, serving at the floor, nothing deleted, repair by `data:go` removing `revoked.json`);
  `buffer:revoke` is Task 7 with the `/revoke <record>` comment convention, the plan's call since a label carries
  no argument.
- Spec §8: the ledger per root with reserve/settle (Task 2), root ids on pins and the dataset issue marker
  (Task 3), reservations around every mia-data and mia-buffers job with the caps from `autonomy.root` (Task 4),
  `mia pause`/`resume` and the `paused` activity (Tasks 3, 4). Not in this plan, by design: autodev's agent jobs
  (autodev has no registry client; milestone O adds the snippets path), the per-round plan and prep thresholds
  (§4.4, §4.5: milestone P), and settling a job's actual `total_cost_usd` (`settle` takes the reserved amount;
  M's judge job reads `result.json` and settles the receipt).
- Replay: every workflow change is behind `l-budget`, `l-assess` or `l-revoke`; the three recorded buffer
  histories, the dataset and develop histories replay through the old branches, so no fixture is re-recorded.
  The new paths are covered by live-environment tests, not fixtures; a later milestone records one.
- Type consistency: `Reservation` (Task 4) is defined in `mia_data/models.py` and used by both workers;
  `RevokedSignal` (Task 6) is the payload of both the keeper's signal and Task 7's label path; `BoardAssessed.refused`
  is keyed by record because the workflow fails datasets, not artifacts.
- Review Focus items: 1 → Task 6 Step 9 (refused label); 2 → Task 2 Step 1 (retry and unknown settle); 3 →
  Task 2 Step 1 (`revoked_kept`) and Task 6 Step 1 (`prepared_row`); 4 → Task 2 Step 5 (`quarantined` in status)
  and Task 6 Step 13 (floor kept); 5 → Task 3 Step 1 (comments kept) and Task 4 Step 5 (stops before the job).
