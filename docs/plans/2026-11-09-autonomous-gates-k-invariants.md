# Autonomous gates, milestone K: the invariants — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** before any gate is automated, fix the three behaviours the architecture review showed to be wrong
today: buffer-api serves on a denylist, autodev merges over a pending human change request at the revision
limit, and a lost raw-cache lock leaves its cluster job writing; and restrict the match agent's tools by
availability, not only by permission.

**Architecture:** each fix is local. `BufferService._serving` flips to an allowlist of latest events and
`register()` writes the events a hand-registered buffer needs; `review.feedback()` drops change requests their
author later superseded by approving the head, and `decide()` escalates instead of merging when one is still
pending at the limit; `heartbeat_through` raises `LockLost`, `CsubJobs.wait` kills the job on it, and both
`await_job` activities turn it into a non-retryable failure the workflows already route to `needs-human` or a
failed dataset; `match_job` adds `--tools`, `--restricted` and `--strict-mcp-config`. No workflow module changes.

**Tech Stack:** Python 3.12, pydantic 2, SQLite, Temporal test env, csub client (`kill`), pytest/ruff/mypy via
`pixi run`. Branch `autonomous-gates` in this repo.

**Spec:** `docs/specs/2026-11-09-autonomous-gates.md` v0.2, §3 (the three invariants and the two corrected
claims), §14 row K.

## Global Constraints

- Activity result dataclasses in `models.py` are wire format: add fields with defaults, never rename or remove
  (`Feedback`, `PrSnapshot`, `JobDone`, `JobOutcome` are all replayed).
- No `workflow.patched()` is planned: no workflow module is edited. Every replay test must pass unchanged:
  `pixi run pytest tests/test_autodev_workflows.py tests/test_mia_workflows.py tests/test_buffers_workflows.py -k replay`.
  If `test_replay_recorded_history` fails after Task 2, the recorded history hit the limit-with-pending case; then
  guard the `decide()` call in `autodev/workflows.py` with `workflow.patched("veto-at-limit")` and re-record per
  `docs/development.md`.
- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` must be empty.
- `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test` clean before every commit.
- Every rule this plan changes is also changed where it is written down: `AGENTS.md` ("One writer per raw
  cache"), `docs/development.md` (serving, register, lock loss), `docs/autodev-runbook.md` (known gaps).

## Review Focus

Inputs the spec implies but no task's tests exercised before this plan; each has its test in the task named.

1. A buffer whose latest event is `idle` (no consumer for `idle.hours`) gets a returning consumer: it must serve,
   because `buffer:keep` is only needed once the keeper retires it. Task 1.
2. A buffer with a config and volumes but **no events at all** (an old hand registration, or a round whose
   `created` event write was lost): it must refuse consumers with a message naming the missing event, not serve.
   Task 1.
3. A reviewer who requested changes and then approved the **same** head: their request is superseded and the PR
   merges; a **different** reviewer's request still blocks. A reviewer who approved and then requested changes
   is already handled (`test_later_changes_requested_supersedes_approval`). Task 2.
4. A `/revise` comment followed by the same person's `/approve <sha>` **after** it: superseded; before it: not.
   Task 2.
5. The lock heartbeat fails on the **last** poll, after the job's status is already terminal: the job is dead,
   `kill` is still called (harmless), and the activity still fails, so a round never publishes from a workspace
   whose lock it lost. Task 3.

---

### Task 1: buffer-api serves on an allowlist; `register` writes the events

**Files:**
- Modify: `mia_buffers/api.py:61` (`UNAPPROVED`), `mia_buffers/api.py:277-288` (`_serving`)
- Modify: `mia_buffers/register.py:278-320` (`register`)
- Modify: `docs/development.md` (step 4 "Register" of "Serving a buffer", and the capability paragraph)
- Test: `tests/test_buffers_api.py`, `tests/test_buffers_register.py`

**Interfaces:**
- Consumes: `Ledger.latest_event(buffer) -> dict | None`, `Ledger.add_event`, the events route
  `POST /buffers/<id>/events` (worker token, body `{"kind", "issue", "event_id", "detail"}`).
- Produces: `SERVING = ("approved", "ready", "idle")` in `mia_buffers/api.py`; `_serving` raising
  `HttpError(409, ...)` for anything else but `retired` (410); `register(...)` returning the existing dict plus
  `"events": ["approved", "ready"]`.

- [ ] **Step 1: Write the failing API tests**

In `tests/test_buffers_api.py`, replace `test_an_unapproved_buffer_serves_no_consumer` with:

```python
def test_only_an_approved_buffer_serves(tmp_path):
    """Serving needs an affirmative authorization (spec autonomous-gates §3): the latest event must be
    `approved`, `ready` or `idle`. Anything else is 409 with a reason; `retired` stays 410; no events is 409."""
    s = BufferService(Ledger(tmp_path / "l.sqlite"))
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    no_events = (409, {"error": "buffer has no approved event"})
    assert call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[:2] == no_events
    for kind in ("created", "planned", "sourcing", "staged"):
        worker(s, "POST", "/buffers/b/events", {"kind": kind})
        waiting = (409, {"error": "buffer not approved"})
        assert call(s, "GET", "/buffers/b/next", {"consumer": "x"})[:2] == waiting, kind
        assert call(s, "GET", "/buffers/b/split/val", {"consumer": "x"})[:2] == waiting, kind
        assert call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[:2] == waiting, kind
    worker(s, "POST", "/buffers/b/events", {"kind": "needs-human", "detail": "why"})
    needs = (409, {"error": "buffer needs a human"})
    assert call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[:2] == needs
    assert call(s, "GET", "/buffers/b/status")[0] == 200 and call(s, "GET", "/buffers/b/events")[0] == 200
    worker(s, "POST", "/buffers/b/events", {"kind": "approved"})
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    worker(s, "POST", "/buffers/b/events", {"kind": "ready"})
    assert call(s, "GET", "/buffers/b/next", {"consumer": cid})[0] == 200
    worker(s, "POST", "/buffers/b/events", {"kind": "idle"})  # a returning consumer still draws
    assert call(s, "GET", "/buffers/b/next", {"consumer": cid})[0] == 200
    worker(s, "POST", "/buffers/b/events", {"kind": "needs-human", "detail": "degraded"})
    assert call(s, "GET", "/buffers/b/next", {"consumer": cid})[:2] == needs
    worker(s, "POST", "/buffers/b/events", {"kind": "retired"})
    assert call(s, "GET", "/buffers/b/next", {"consumer": cid})[:2] == (410, {"error": "buffer retired"})
```

In `tests/test_buffers_register.py::test_register_end_to_end`, after the `status` assertions add:

```python
    events = httpx.get(f"{token_server}/buffers/demo/events").json()["events"]
    assert [event["kind"] for event in events] == ["approved", "ready"] and out["events"] == ["approved", "ready"]
    consumer = httpx.post(f"{token_server}/buffers/demo/consumers", json={"name": "n", "host": "h", "pid": 1, "split": "train"})
    assert consumer.status_code == 201  # a hand-registered buffer serves because register authorized it
```

Also delete the `hand` lines at the end of the old test (they asserted the opposite). Check
`test_a_retired_buffer_serves_no_consumer`: it registers a consumer before any event; add
`worker(s, "POST", "/buffers/b/events", {"kind": "approved"})` before the consumer line (the service there has no
token, so use `call(s, "POST", ..., payload={"kind": "approved"})` as that test already does for `retired`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_buffers_api.py::test_only_an_approved_buffer_serves tests/test_buffers_register.py::test_register_end_to_end -v`
Expected: FAIL: the no-events consumer gets 201; `register` returns no `events`.

- [ ] **Step 3: Implement the allowlist in `mia_buffers/api.py`**

Replace `UNAPPROVED` with `SERVING = ("approved", "ready", "idle")  # latest events under which consumers draw`
and keep `UNAPPROVED` only to pick the message. `_serving(self, buffer) -> BufferConfig`: `retired` → 410
`buffer retired`; kind in `SERVING` → return config; `None` → 409 `buffer has no approved event`; kind in
`UNAPPROVED` → 409 `buffer not approved`; otherwise (`needs-human`) → 409 `buffer needs a human`. Update the
docstring: an allowlist, spec autonomous-gates §3.

- [ ] **Step 4: Make `register()` authorize the buffer**

At the end of the `with httpx.Client(...)` block in `mia_buffers/register.py`, after the volumes loop, post
`{"kind": "approved", "detail": "mia-buffers register"}` then `{"kind": "ready", "detail": "mia-buffers register"}`
to `/buffers/{buffer}/events`, `raise_for_status()` each, and set `out["events"] = ["approved", "ready"]`. Update
the docstring ("…and authorizes it to serve").

- [ ] **Step 5: Run the two test files**

Run: `pixi run pytest tests/test_buffers_api.py tests/test_buffers_register.py -v`
Expected: PASS, including the amended `test_a_retired_buffer_serves_no_consumer`.

- [ ] **Step 6: Document**

`docs/development.md`, "Serving a buffer" step 4: add "and writes the `approved` and `ready` events, without
which buffer-api refuses consumers". In the capability-levels paragraph add: "Consumer routes answer `409` unless
the buffer's latest event is `approved`, `ready` or `idle` (an allowlist: a `needs-human` or a missing event never
serves; spec autonomous-gates §3)." Add to "Rules that are not obvious from the code": **Serving is authorized,
never inferred.** one paragraph.

- [ ] **Step 7: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add mia_buffers/api.py mia_buffers/register.py tests/test_buffers_api.py tests/test_buffers_register.py docs/development.md
git commit -m "buffer-api: consumers are served only under an approved, ready or idle event; register authorizes the buffers it creates"
```

---

### Task 2: a pending human change request blocks a merge; an approval supersedes its author's earlier requests

**Files:**
- Modify: `autodev/review.py:93-124` (`feedback`), `autodev/activities.py:302-327` (`_snapshot`)
- Modify: `autodev/decide.py:18-47` (`decide`)
- Modify: `autodev/models.py:31-35` (`Feedback`)
- Modify: `docs/autodev-runbook.md` (table row "approve the PR", "Known gaps")
- Test: `tests/test_autodev_review.py`, `tests/test_autodev_decide.py`, `tests/test_autodev_workflows.py`

**Interfaces:**
- Consumes: `Reviewers.may_command`, `APPROVE_RE`, GitHub review and comment dicts as `rv()`/`cm()` build them.
- Produces: `Feedback.author: str = ""` and `Feedback.at: str = ""` (wire, defaults);
  `feedback(reviews, review_comments, comments, reviewers, head_sha: str | None = None) -> list[Feedback]`,
  which with `head_sha` drops items whose author approved `head_sha` **later** than the item;
  `decide()` returning `(ESCALATE, "approved with unaddressed change requests (revision limit N reached)")`
  when pending feedback exists at the limit and the head is approved.

- [ ] **Step 1: Write the failing review tests**

Add to `tests/test_autodev_review.py`:

```python
def test_an_approval_supersedes_the_approvers_earlier_requests_only():
    """A reviewer who asked for changes and then approved this head has withdrawn the request; another
    reviewer's request still stands (spec autonomous-gates §3)."""
    reviews = [
        rv(3, "alice", "CHANGES_REQUESTED", body="rename foo", at="2026-10-10T01:00:00Z"),
        rv(4, "admin", "CHANGES_REQUESTED", body="split it", at="2026-10-10T01:10:00Z"),
        rv(5, "alice", "APPROVED", at="2026-10-10T01:30:00Z"),
    ]
    assert [f.id for f in feedback(reviews, [], [], BOT, head_sha=HEAD)] == ["review-4"]
    assert [f.id for f in feedback(reviews, [], [], BOT)] == ["review-3", "review-4"]  # no head: nothing dropped
    assert feedback(reviews, [], [], BOT, head_sha="0ld")[0].author == "alice"  # an approval of another head


def test_an_approve_comment_supersedes_an_earlier_revise_not_a_later_one():
    comments = [
        cm(10, "alice", "/revise also docs", at="2026-10-10T00:30:00Z"),
        cm(11, "alice", f"/approve {HEAD}", at="2026-10-10T00:40:00Z"),
        cm(12, "alice", "/revise and tests", at="2026-10-10T00:50:00Z"),
    ]
    assert [f.id for f in feedback([], [], comments, BOT, head_sha=HEAD)] == ["comment-12"]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pixi run pytest tests/test_autodev_review.py -k supersedes -v`
Expected: FAIL: `feedback()` has no `head_sha` parameter.

- [ ] **Step 3: Implement in `autodev/models.py` and `autodev/review.py`**

`Feedback` gains `author: str = ""` and `at: str = ""` after `text` (wire format: defaults, appended). In
`feedback()`, fill both for every item (review: `_login(review)`, `submitted_at`; comment: `_login(comment)`,
`created_at`). With `head_sha`, build `approved_at: dict[str, str]`: for each APPROVED review whose `commit_id ==
head_sha` and each non-ours comment whose `APPROVE_RE` match is a prefix of `head_sha` and `may_command`, the
latest timestamp per login. Drop an item when `approved_at.get(item.author, "") > item.at`. Existing equality
assertions on `Feedback(...)` in other tests keep passing because the new fields default; update
`test_feedback_from_reviews_and_revise_comments_oldest_first` to compare `[(f.id, f.text) for f in fb]` if the
dataclass equality now differs.

- [ ] **Step 4: Pass `head_sha` from `_snapshot`**

`autodev/activities.py::_snapshot`: `feedback(reviews, inline_comments, comments, reviewers, head_sha)`.

- [ ] **Step 5: Run the review tests**

Run: `pixi run pytest tests/test_autodev_review.py tests/test_autodev_activities.py -v`
Expected: PASS.

- [ ] **Step 6: Write the failing decide test**

In `tests/test_autodev_decide.py`, replace `test_revision_limit_escalates_unless_approved` with:

```python
def test_revision_limit_escalates_even_when_approved():
    """A pending human change request blocks a merge regardless of the retry counter (spec autonomous-gates §3);
    `_snapshot` has already dropped requests their author superseded by approving."""
    assert decide(snap(feedback=[F1]), [], 3, 3) == (ESCALATE, "revision limit (3) reached")
    assert decide(snap(approved=True, checks="success", feedback=[F1]), [], 3, 3) == (
        ESCALATE,
        "approved with unaddressed change requests (revision limit 3 reached)",
    )
    assert decide(snap(approved=True, checks="success"), [], 3, 3) == (MERGE, None)  # nothing pending: merges
```

- [ ] **Step 7: Run it to verify it fails**

Run: `pixi run pytest tests/test_autodev_decide.py -v`
Expected: FAIL on the second assertion (`MERGE`).

- [ ] **Step 8: Implement in `autodev/decide.py`**

After the `REVISE` branch, before `if snapshot.approved:`, add: `if pending and snapshot.approved: return
escalate(f"approved with unaddressed change requests (revision limit {max_revisions} reached)")`. Update the
docstring: an approval never outranks a pending request; the activity drops requests the approver withdrew.

- [ ] **Step 9: Update the workflow tests to the new snapshots**

In `tests/test_autodev_workflows.py`, `test_revision_limit_escalates_but_approval_still_merges` and
`test_escalation_is_raised_once_then_waits`: the final snapshot models what `_snapshot` now returns when the
requesters approve, `snap(approved=True)` with no feedback. Rename the first to
`test_revision_limit_escalates_and_the_requesters_approval_merges`. Add one test:

```python
async def test_an_outsiders_pending_request_blocks_the_merge_at_the_limit(env):
    """An escalation leaves the PR in a human's hands (the workflow keeps watching); closing it ends the run."""
    f1, f2 = Feedback("review-1", "a", "alice"), Feedback("review-2", "b", "admin")
    m = Mocks([snap(feedback=[f1]), snap(feedback=[f1, f2], approved=True), snap(state="closed")])
    assert await run(env, m, IssueInput(7, max_revisions=1)) == "closed"
    assert any("unaddressed change requests" in c.body for c in m.of("comment")) and "merge" not in m.names()
```

(An `ESCALATE` does not end the workflow: `decide()` answers `WAIT` for the same head afterwards, so the mock's
third snapshot closes the PR to end the run, as `test_escalation_is_raised_once_then_waits` ends it with an
approval.)

- [ ] **Step 10: Run autodev tests and the replay**

Run: `pixi run pytest tests/test_autodev_decide.py tests/test_autodev_workflows.py -v`
Expected: PASS, including `test_replay_recorded_history` (see Global Constraints if not).

- [ ] **Step 11: Document**

`docs/autodev-runbook.md`: in the "How people use it" table, the approve row gains "(a change request you made
earlier on the same head is withdrawn by your approval; anyone else's still blocks)". In "Known gaps" delete
nothing; add nothing (the gap is closed).

- [ ] **Step 12: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add autodev/models.py autodev/review.py autodev/activities.py autodev/decide.py tests/test_autodev_review.py tests/test_autodev_decide.py tests/test_autodev_workflows.py docs/autodev-runbook.md
git commit -m "autodev: a pending change request blocks the merge at the revision limit; an approval withdraws its author's earlier requests"
```

---

### Task 3: a lost raw-cache lock kills the job and fails the wait

**Files:**
- Modify: `mia_core/locks.py:201-215` (`heartbeat_through`)
- Modify: `autodev/jobs.py:123-145` (`CsubJobs.wait`)
- Modify: `mia_data/activities.py:536-541`, `mia_buffers/activities.py:824-831` (`await_job`)
- Modify: `AGENTS.md:41-43`, `docs/development.md` ("The cache" section, the lock paragraph)
- Test: `tests/test_core_locks.py`, `tests/test_autodev_jobs.py`, `tests/test_autodev_agent.py` (`FakeCsub`),
  `tests/test_mia_activities.py`, `tests/test_buffers_activities.py`

**Interfaces:**
- Consumes: `LockDir.heartbeat(kind, key, owner) -> bool`; csub `Client.kill(job_ids) -> KillResult`;
  `JobDone(job_id, state, exit_code)`.
- Produces: `class LockLost(RuntimeError)` in `mia_core/locks.py` with attributes `kind`, `key`, `owner` and
  message `"{owner} lost the lock {kind}/{key}: it lapsed or someone else holds it"`; `heartbeat_through` raising
  it from the heartbeat callback; `CsubJobs.wait` calling `self.client.kill([job_id])` and re-raising;
  `await_job` raising `ApplicationError(str(error), type="LockLost", non_retryable=True)`.

- [ ] **Step 1: Write the failing lock test**

Add to `tests/test_core_locks.py`:

```python
def test_heartbeat_through_raises_when_the_lock_is_lost(tmp_path):
    """A stale lease is not evidence that its writer stopped: the wait must fail, not log (spec autonomous-gates §3)."""
    from mia_core.locks import LockLost, heartbeat_through

    now, tick = clock()
    locks = LockDir(tmp_path, now)
    assert locks.take("raw", "rec", "round A", 900)
    seen = []
    beat = heartbeat_through(locks, ["raw", "rec", "round A"], seen.append)
    beat("RUN")
    assert seen == ["RUN"]
    tick(1000)
    assert locks.take("raw", "rec", "round B", 900)  # lapsed: another owner takes it
    with pytest.raises(LockLost) as info:
        beat("RUN")
    assert seen == ["RUN", "RUN"] and (info.value.kind, info.value.key, info.value.owner) == ("raw", "rec", "round A")
    assert "round A lost the lock raw/rec" in str(info.value)
```

(`clock()` and `tick` are the file's existing helpers; check `tick`'s signature at the top of the file.)

- [ ] **Step 2: Run it to verify it fails**

Run: `pixi run pytest tests/test_core_locks.py -k lost -v`
Expected: FAIL: `LockLost` does not exist.

- [ ] **Step 3: Implement in `mia_core/locks.py`**

Define `LockLost(RuntimeError)` with `__init__(self, kind: str, key: str, owner: str)` setting the attributes and
the message above. `heartbeat_through`: call `inner(state)` first, then if `locks.heartbeat(...)` is false raise
`LockLost(kind, key, owner)`; drop the `lost` flag and the warning. Docstring: raises, the job runner kills the
job on it.

- [ ] **Step 4: Run the lock tests**

Run: `pixi run pytest tests/test_core_locks.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing job-runner test**

In `tests/test_autodev_agent.py::FakeCsub` add `self.killed = []` in `__init__` and
`def kill(self, job_ids=None): self.killed.extend(job_ids or []); return SimpleNamespace(killed=list(job_ids or []))`.
Add to `tests/test_autodev_jobs.py`:

```python
async def test_wait_kills_the_job_when_the_heartbeat_loses_the_lock(tmp_path):
    from mia_core.locks import LockLost

    fake = FakeCsub(states=["RUN", "RUN", "DONE"])
    beats = []

    def heartbeat(state):
        beats.append(state)
        if len(beats) == 2:
            raise LockLost("raw", "rec", "me")

    with pytest.raises(LockLost):
        await _jobs(tmp_path, fake).wait("777", str(tmp_path), heartbeat)
    assert fake.killed == ["777"] and beats == ["RUN", "RUN"]


async def test_wait_kills_even_when_the_lock_is_lost_on_the_terminal_poll(tmp_path):
    from mia_core.locks import LockLost

    fake = FakeCsub(states=["DONE"])

    def heartbeat(state):
        raise LockLost("raw", "rec", "me")

    with pytest.raises(LockLost):
        await _jobs(tmp_path, fake).wait("777", str(tmp_path), heartbeat)
    assert fake.killed == ["777"]  # harmless on a finished job; the wait still fails, so nothing is read from it
```

- [ ] **Step 6: Run them to verify they fail**

Run: `pixi run pytest tests/test_autodev_jobs.py -k lock -v`
Expected: FAIL: `LockLost` propagates without `kill` being called (`fake.killed == []`).

- [ ] **Step 7: Implement in `autodev/jobs.py`**

In `wait`, wrap `heartbeat(state)` in `try/except LockLost`: `await asyncio.to_thread(self.client.kill, [job_id])`
then `raise`. Import `LockLost` from `mia_core.locks` (autodev may import `mia_core`). Docstring: a heartbeat that
raises `LockLost` kills the job before the error propagates.

- [ ] **Step 8: Run the job tests**

Run: `pixi run pytest tests/test_autodev_jobs.py tests/test_autodev_agent.py -v`
Expected: PASS.

- [ ] **Step 9: Write the failing activity tests**

In `tests/test_mia_activities.py`, replace the last three lines of `test_await_job_heartbeats_the_lock_it_is_given`
(the `caplog` block) with:

```python
    locks.release("raw", "rec-1", "me")
    with pytest.raises(ApplicationError) as info:  # the lock is gone: the wait fails, never retried
        await env.run(t.a.await_job, "j3", "/w", ["raw", "rec-1", "me"])
    assert info.value.non_retryable and info.value.type == "LockLost" and "lost the lock raw/rec-1" in str(info.value)
```

Here the stub `wait` must call the heartbeat, so the raise comes from `heartbeat_through` through the stub (the
stub mirrors `CsubJobs.wait` propagating it). Add the same assertion block to a new
`tests/test_buffers_activities.py::test_await_job_lock_loss_is_permanent(acts, tmp_path)` that sets
`acts.jobs = SimpleNamespace(wait=wait)` with the same stub, takes the lock in `lock_dir(acts.cfg)` (import
`lock_dir` from `mia_data.jobspec`, as `mia_buffers/activities.py` does), releases it, and expects the same
`ApplicationError`. Both test files already import `ApplicationError`; import `ActivityEnvironment` from
`temporalio.testing` and `SimpleNamespace` from `types` in the buffers file.

- [ ] **Step 10: Run them to verify they fail**

Run: `pixi run pytest tests/test_mia_activities.py -k await_job tests/test_buffers_activities.py -k lock_loss -v`
Expected: FAIL: `LockLost` escapes as a plain exception (retryable), not `ApplicationError`.

- [ ] **Step 11: Implement in both `await_job` activities**

Wrap `await self.jobs.wait(...)` in `try/except LockLost as error: raise ApplicationError(str(error),
type="LockLost", non_retryable=True) from error`. Both workflows already turn a failed `await_job` into
`needs-human` (`MiaDataDataset._step`) or a failed dataset (`MiaBuffer`'s per-dataset isolation), with the
message in the comment; no workflow change.

- [ ] **Step 12: Run both activity files**

Run: `pixi run pytest tests/test_mia_activities.py tests/test_buffers_activities.py -v`
Expected: PASS.

- [ ] **Step 13: Document**

`AGENTS.md` "One writer per raw cache": append "A wait whose heartbeat finds the lock gone kills its job
(`csub kill`) and fails the round; a stale lease is never evidence that its writer stopped." `docs/development.md`
"The cache" section, the lock paragraph: the same sentence, plus the `LockLost` type. Add to "Rules that are not
obvious from the code": **A lost lock kills the job.** one paragraph.

- [ ] **Step 14: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green, replay tests included.

```bash
git add mia_core/locks.py autodev/jobs.py mia_data/activities.py mia_buffers/activities.py tests/test_core_locks.py tests/test_autodev_jobs.py tests/test_autodev_agent.py tests/test_mia_activities.py tests/test_buffers_activities.py AGENTS.md docs/development.md
git commit -m "locks: a lost raw-cache lock kills the job and fails the wait instead of logging"
```

---

### Task 4: the match agent's tools are restricted by availability

**Files:**
- Modify: `mia_data/jobspec.py:46` (`MATCH_TOOLS`), `mia_data/jobspec.py:340-360` (`match_job`)
- Modify: `docs/mia-data-workflow.md` step 3 (one sentence)
- Test: `tests/test_mia_jobspec.py::test_match_job`

**Interfaces:**
- Consumes: the installed CLI's `--tools <names>` (availability), `--restricted` (removes code-running tools
  unless `--tools` names them, ignores user/project/local settings, confines file tools to the working
  directories and `--add-dir`), `--strict-mcp-config`, `--add-dir <path>`.
- Produces: `MATCH_TOOLS` unchanged (the permission patterns), new `MATCH_AVAILABLE = "Bash,Read,Write,Glob,Grep"`.

- [ ] **Step 1: Extend the failing test**

In `tests/test_mia_jobspec.py::test_match_job`, after the `--allowedTools` assertion add:

```python
    assert '--tools "Bash,Read,Write,Glob,Grep"' in s.command and "--restricted" in s.command
    assert "--strict-mcp-config" in s.command and f"--add-dir {snap}" in s.command
```

(`snap` is defined two lines below in the test today; move its definition above the new assertions.)

- [ ] **Step 2: Run it to verify it fails**

Run: `pixi run pytest tests/test_mia_jobspec.py::test_match_job -v`
Expected: FAIL on `--tools`.

- [ ] **Step 3: Implement in `mia_data/jobspec.py`**

Add `MATCH_AVAILABLE = "Bash,Read,Write,Glob,Grep"  # --tools: what exists; MATCH_TOOLS is what runs unprompted`.
In `match_job`'s `body`, after `--allowedTools "{MATCH_TOOLS}"` add
`--tools "{MATCH_AVAILABLE}" --restricted --strict-mcp-config --add-dir {quote(str(snapshot_dir))}` before the
redirection. `--add-dir` is needed because `--restricted` confines the file tools to the working directories and
the snippet snapshot is reached through the `snapshot` symlink.

- [ ] **Step 4: Run the jobspec tests**

Run: `pixi run pytest tests/test_mia_jobspec.py -v`
Expected: PASS.

- [ ] **Step 5: Document**

`docs/mia-data-workflow.md` step 3, after "It may run `harness smoke` to try bindings, but may not change snippet
code.": "The job runs Claude Code restricted to five built-in tools, with no settings files and no MCP servers;
the sandbox mounts are the other half of that restriction."

- [ ] **Step 6: Full checks and commit**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all green.

```bash
git add mia_data/jobspec.py tests/test_mia_jobspec.py docs/mia-data-workflow.md
git commit -m "mia-data: the match agent's tools are restricted by availability, not only by permission"
```

---

### Task 5: live verification on the dev host

Not code; the acceptance row of spec §14 K. Run after Tasks 1–4 are committed and the workers restarted
(`pixi run mia restart buffer-api mia-buffers mia-data autodev`, with no round in `sourcing`).

- [ ] **Step 0: Before the restart, list the buffers the allowlist will stop serving, and check the sandbox's CLI**

Run: `curl -s <service.url>/buffers | python3 -c 'import json,sys; print([b["id"] for b in json.load(sys.stdin)["buffers"] if not b.get("latest_event")])'`
Expected: the ids of buffers with no events (hand registrations). Each will answer `409 buffer has no approved event`
after the restart; re-run `mia-buffers register` with the same arguments for the ones still in use.

Run a one-off csub `--claude` job (or `csub run --claude -- claude --help`) with the same agent the match job uses and
check that its output lists `--restricted`, `--tools` and `--strict-mcp-config`.
Expected: all three present. If one is missing, the sandbox's Claude Code predates them and every match job would
fail with an unknown-option error; upgrade it before restarting mia-data.

- [ ] **Step 1: A `needs-human` buffer refuses a consumer**

Pick a buffer whose latest event is `needs-human` on the page (or write one on a scratch buffer with the worker
token: `curl -H "Authorization: Bearer $(cat <service.token_file>)" -X POST <service.url>/buffers/<id>/events -d '{"kind":"needs-human","detail":"K check"}'`).
Run: `curl -s -o /dev/null -w '%{http_code}\n' -X POST <service.url>/buffers/<id>/consumers -d '{"name":"k","host":"h","pid":1,"split":"train"}'`
Expected: `409`.

- [ ] **Step 2: A live buffer still serves**

Run the same `POST .../consumers` against a buffer at `ready`.
Expected: `201`.

- [ ] **Step 3: One live match job**

Trigger a match on a scratch dataset issue (`data:go` on a record whose binding is already known) and check its
`claude.err` is empty of option errors and the round reaches `matched` or `needs_snippet`.
Expected: the agent ran under the restricted tool set and produced a verdict.

- [ ] **Step 4: Note the result**

Append one line to `notes/` (untracked) with the date, the buffer ids, the two status codes and the match job's
outcome. Nothing to commit.

---

## Self-review notes

- Spec §3 rows: Task 1 (serving), Task 2 (veto), Task 3 (lock). Corrected claims: the CODEOWNERS claim is a
  ruleset and docs matter for milestone O (no code here); the `--tools` claim is Task 4 for the match agent (the
  judge job itself is milestone M).
- Task 2 is the only one touching a function the workflow calls (`decide`); the Global Constraints say what to do
  if the recorded history exercises the changed branch.
- `Feedback.author`/`at` appear in Task 2 Step 3 and in the Step 9 workflow test with positional use
  `Feedback("review-1", "a", "alice")`; `Feedback(id, text)` elsewhere keeps working through the defaults.
- Review Focus items 1–2 are asserted in Task 1 Step 1, 3–4 in Task 2 Step 1, 5 in Task 3 Step 5.
