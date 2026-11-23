# Resume or restart: datasets, requests, the cleaner — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A dataset round and a preparation request can be run again as a Resume (every finished step kept,
same pins) or a Restart (today's `go`), from the page, a request's text and the cleaner.

**Architecture:** A new action `resume` beside `go`. buffer-api sends a dataset's `resume` as the workflow's
`restart` signal with an event of kind `resume`; `MiaDataDataset._resume` enters the round without
`_begin_round`'s wipe and starts at the first step whose result is not on `DatasetState` (`resume_step`).
Steps that run again under the same round take the next attempt number. A request passes the choice down to
each dataset; the cleaner's plans send `resume`.

**Tech Stack:** Python 3, Temporal (temporalio), pytest with the time-skipping server, ES modules run under
node by `tests/test_buffers_ui.py`.

**Spec:** `docs/specs/2026-11-18-resume-restart.md` (v0.3). This plan covers §2 to §5, §9 calls 2 to 7 and 9,
and §10. §6 (buffer rounds) gets a plan of its own once this is live.

## Global Constraints

- Work in this worktree (`.claude/worktrees/resume`, branch `resume`). Run scripts with `PYTHONPATH=$PWD`.
- Never push, never restart a service. Commit each task with explicit paths.
- `test_replay_*` must pass unchanged: no recorded history may issue different commands. New behaviour is
  reached only by an event of kind `resume`, or with `DatasetState.attempt > 1`.
- `DatasetState`, `LabelEvent` and activity results are wire format: new fields take defaults, none is
  renamed or removed.
- An activity gets the arguments its signature names, always the same number (Temporal converts an argument
  to its annotated type only when the count matches).
- A new action goes into the stage table first; buffer-api refuses it anywhere else.
- The button words are exactly **Resume** and **Restart** (at `debugging`: **Restart now**; at `new`:
  **Start**). The difference is in tooltips and docs.
- Comments and docs say what the code does, never what it replaced. No real paths or hostnames:
  `git diff judge-decides | grep -n 'rokickik\|/nrs/'` is empty.
- Per task run the covering test files; `pixi run lint` and `pixi run typecheck` before each commit. The full
  suite runs in Task 7.

## Review Focus

1. A `resume` that reaches a round that is working (a double click, the cleaner beside a person): refused as
   busy like a `go`, the state untouched. (Task 2)
2. A resumed step's text under the same round: the ledger keeps one body per marker, so the judge's verdict
   after a "skipped" entry, and crops made again at one commit, need markers of their own. (Task 1)
3. A second Resume, and a redelivered one: the third attempt gets its own keys; the same event id runs
   nothing twice. (Task 2)
4. A round from before this change, or one whose judge is `off`: Resume at `review` runs no judge and says
   nothing is left. (Task 2)
5. The cleaner re-running a request must not restart its prepared datasets. (Task 6)

---

### Task 1: A step that runs again is a new attempt

**Files:**
- Modify: `mia_data/models.py` (`DatasetState`)
- Modify: `mia_data/workflows.py` (`_prepare`, `_job`, `_round`'s `prepare_match`, `_judge_crops`, `_test`,
  `_test_head`, `_merged`, `_debug`)
- Modify: `mia_data/activities.py` (`prepare_debug`)
- Test: `tests/test_mia_workflows.py`, `tests/test_mia_activities.py`

**Interfaces:**
- Produces: `DatasetState.attempt: int = 1` (which attempt of its steps the round is on).
- Produces: `MiaDataDataset._test_id(self, sha: str) -> str`: `f"{round_id}-{sha[:12]}"`, plus `f"-a{attempt}"`
  when `attempt > 1`.
- Produces: `MiaDataDataset._marker(self, text: str) -> str`: `text`, plus `f" a{attempt}"` when `attempt > 1`.
- Produces: activity `prepare_debug(issue, round_id, pins, failure, binding, attempt: int = 1) -> str`; the
  workflow always sends six arguments.

- [ ] **Step 1: Write the failing tests** in `tests/test_mia_workflows.py`

```python
async def test_a_round_on_its_second_attempt_names_every_job_test_and_marker_for_it(env):
    m = Mocks(crop_judge="shadow")
    async with started(env, m, state=DatasetState(7, attempt=2)) as h:
        await idle(h, m, "review")
        st = await h.query(MiaDataDataset.status)
    keys = [a[0] for a in m.of("submit")]
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "verify", "judge"]
    assert all(key.endswith("-2") for key in keys)
    assert all(a[2].workspace.endswith("-2") for a in m.of("submit"))
    assert st.tests[-1].test_id == f"i7-e11-{SHA[:12]}-a2"
    assert any(c[1] == f"judge crops i7-e11 {SHA} a2" for c in m.of("comment"))


async def test_a_first_attempt_keeps_the_names_it_has(env):
    m = Mocks(crop_judge="shadow")
    async with started(env, m) as h:
        await idle(h, m, "review")
        st = await h.query(MiaDataDataset.status)
    assert all(a[0].endswith("-1") for a in m.of("submit")) and st.tests[-1].test_id == f"i7-e11-{SHA[:12]}"
    assert any(c[1] == f"judge crops i7-e11 {SHA}" for c in m.of("comment"))
```

Give the mock `prepare_debug` the sixth parameter and return `f"/w/{round_id}/debug-{attempt}"`; add to an
existing debugging test (`test_a_failed_round_is_debugged_and_run_again`) the assertion that the debug job's
key ends `-debug-1`. In `tests/test_mia_activities.py` add `test_prepare_debug_makes_a_workspace_per_attempt`:
two calls with `attempt=1` and `attempt=2` return paths ending `debug-1` and `debug-2`.

- [ ] **Step 2: Run them and see them fail**

Run: `pixi run pytest tests/test_mia_workflows.py -k "attempt" tests/test_mia_activities.py -k prepare_debug -q`
Expected: FAIL (`DatasetState` takes no `attempt`).

- [ ] **Step 3: Implement**

`state.attempt` replaces every literal attempt `1` the workflow sends: `job_key(..., state.attempt)` in `_job`
and in `_debug`, `prepare_job` in `_prepare`, `prepare_match`, `prepare_judge`, `prepare_debug`. `_test`,
`_test_head` and `_merged` build test ids with `_test_id`. `_marker` wraps the markers `judge crops ...` (all
three uses in `_judge_crops`), `test {round_id} {sha}` (every use) and `judge limit ...`. Nothing changes
`state.attempt` yet: Task 2 raises it on a Resume and resets it in `_begin_round`.

- [ ] **Step 4: Run the tests, the replay tests and the argument check**

Run: `pixi run pytest tests/test_mia_workflows.py tests/test_mia_activities.py tests/test_activity_arguments.py -q`
Expected: PASS, including every `test_replay_*`.

- [ ] **Step 5: Commit** (`mia-data: a round's jobs, tests and markers carry its attempt`)

---

### Task 2: Resume on a matched round

**Files:**
- Modify: `mia_data/models.py` (`resume_step`, `DatasetState.revoked`, `sets_view`)
- Modify: `mia_data/workflows.py` (`_round` split, `_resume`, `_revoke`, `_begin_round`, `run`)
- Modify: `mia_data/activities.py` (`resume_root`), `mia_data/cli.py` if activities are listed there
- Test: `tests/test_mia_workflows.py`, `tests/test_mia_activities.py`, `tests/test_mia_status.py` or wherever
  `sets_view` is tested

**Interfaces:**
- Consumes: `DatasetState.attempt`, `_test_id`, `_marker` (Task 1).
- Produces: `resume_step(state: DatasetState, judge_on: bool) -> str` in `mia_data/models.py`, the one place
  that decides where a Resume starts (spec §8). First that applies:
  `"restart"` (no `pins`), `"follow"` (`snippet_issue`), `"save"` (last test `ok` and `approved_at`),
  `"judge"` (last test `ok`, `judge_on`, no `evidence`), `"review"` (last test `ok`), `"crops"` (`binding`),
  `"match"`.
- Produces: `DatasetState.revoked: bool = False`, set by `_revoke` when it ends the round, cleared by
  `_begin_round`; `sets_view(...)["revoked"]` carries it.
- Produces: activity `resume_root(issue: int, event_id: str) -> str`: the `root` of that action's payload,
  else `cfg.root_of(issue)` (as `admit` reads it).
- Produces: `MiaDataDataset._resume(self, event: LabelEvent) -> None`; `_round` calls it first thing when
  `event.kind == "resume"`.
- Produces, by splitting `_round` with no change to the commands a `go` issues:
  `_match(self, snippet_issue: int | None, snippet_pr: int | None, advised: bool) -> None` (from
  `_stage("matching")` to the end), `_set_for_review(self, notes: tuple) -> None` (from `_stage("testing")` to
  the end) and `_decide(self, test: Test) -> None` (the judge, then grant, send back or `_from_review`).
- Produces: `self.resumed: bool` on the workflow (not durable), true while a resumed round runs; Task 3 reads it.
- Produces: `_begin_round` sets `state.attempt = 1`. Task 1's test that starts a round at attempt 2 goes: the
  Resume tests below assert the same names.
- Produces: a `resume` that arrives while the crop judge looks beside the round (`self.judging` not done) is
  not queued as a go: the `restart` handler puts it on `self.busy_resumes`, and the round refuses each with
  "the crop judge is looking at these crops: its verdict decides" once the judge has ended. A `resume` must
  not turn the judge's verdict into one on record only (`verdict_first`).

**What `_resume` does, in order** (spec §4.2):
1. `state.revoked`: `_refuse("data:resume", "its saved crops were revoked: Restart repairs it", event)`, return.
2. Close a pass the stopped round left open (`_found(state.result or "Ended: the round was resumed.")`,
   `_flush`), reset `debugs` and `reworks` by the actor as `_round` does.
3. `step = resume_step(state, judge_on)`, `judge_on` from `limits` (`crop_judge != "off"`). `"restart"`:
   one entry (below), then the round runs as `_round` does for a `go`.
4. Otherwise: `limits`, `paused` (a pause raises `RoundFailed(PAUSED, blocked=True)`), then
   `state.pins = replace(state.pins, root=await resume_root(...))`, `state.attempt += 1`, `self.resumed = True`.
5. One timeline entry, marker `resume {round_id} {event_id}`, body `Resumed by {actor}: ` plus:

| step | text | then |
|---|---|---|
| `restart` | `the round has nothing to keep, so it starts from the beginning.` | as a `go` |
| `match` | `the pins are kept; the match runs again.` | `_match(None, None, False)` |
| `crops` | `the binding and the pins are kept; the crops are made again.` | `_set_for_review(())` |
| `judge` | `the crops are kept; the crop judge looks at them.` | `_stage("review")` if not there, `_decide(test)` |
| `review` | `nothing is left to run: the crops wait for a verdict.` | at `review`: return, no stage write; elsewhere `_stage("review")`, `_from_review()` |
| `save` | `the approved crops are kept; they are saved.` | `_publish_set()` |
| `follow` | `the reader's pull request and every set made for it are kept.` | Task 3; until then `_follow()` |

- [ ] **Step 1: Write the failing tests**

Unit, for `resume_step` (parametrized over the seven outcomes, plus: judge off with a passing unjudged set is
`"review"`; a state with `snippet_issue` and a passing set is `"follow"`).

Workflow tests, each sending `MiaDataDataset.restart` with `ev("resume", "12")`:

```python
async def test_resume_after_a_skipped_judge_runs_only_the_judge(env):
    m = Mocks(crop_judge="shadow", reservations={"judge": [Reservation(False, "would exceed max_usd 1"), Reservation(True)]})
    async with started(env, m) as h:
        await idle(h, m, "review")
        await h.signal(MiaDataDataset.restart, ev("resume", "12"))
        await until(lambda: len(m.of("read_judge")) == 1)
        st = await h.query(MiaDataDataset.status)
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "verify", "judge"]
    assert m.of("admit") == [("11",)] and (st.round_id, st.attempt, st.pins.snippets_commit) == ("i7-e11", 2, SHA)
    assert m.stages == ["matching", "testing", "review"] and st.tests[-1].evidence
    assert [c[1] for c in m.of("comment") if c[1].startswith("judge crops")] == [
        f"judge crops i7-e11 {SHA}",  # the entry that said it was skipped
        f"judge crops i7-e11 {SHA} a2",  # the verdict: a marker of its own, so the ledger keeps its text
    ]


async def test_resume_after_a_crop_job_died_makes_the_crops_on_the_kept_binding(env):
    m = Mocks(states={f"crop-{SHA}": ("UNKNOWN", None)})
    async with started(env, m) as h:
        await idle(h, m, "failed")
        m.states = {}
        await h.signal(MiaDataDataset.restart, ev("resume", "12"))
        await idle(h, m, "review")
    kinds = [a[1] for a in m.of("submit")]
    assert kinds == ["match", "crop", "crop", "verify"] and m.of("admit") == [("11",)]
    assert m.of("submit")[2][0].endswith("-2") and m.stages == ["matching", "testing", "failed", "testing", "review"]
```

And, in the same style, one test each for:
- `test_resume_after_a_failed_match_matches_again_on_the_same_pins` (match job `EXIT`; one `admit`, two
  `match` submits, the second key ends `-2`).
- `test_resume_after_a_save_that_failed_saves_the_approved_set_and_runs_no_job` (`publish_error` on the first
  save; after Resume `publish_crops` succeeds, no further `submit`).
- `test_resume_of_a_round_admission_refused_starts_from_the_beginning` (`admit_error`, then none; `admit` is
  called with `"12"`, the round id is `i7-e12`, `attempt == 1`).
- `test_resume_at_review_of_a_judged_set_says_nothing_is_left` (judge `shadow`; no new `submit`, no new
  `set_stage`, one comment holding `nothing is left to run`). Same with `crop_judge="off"` (Review Focus 4).
- `test_resume_charges_whoever_asked` (mock `resume_root` returns `"o/data#99"`; the judge's `reserve` after
  the Resume has that root).
- `test_a_resume_under_the_pause_ends_blocked_and_keeps_the_round` (round id and tests unchanged).
- `test_resume_after_a_revoke_is_refused` (`refuse_label` once, no `submit`; `sets_view` says `revoked`).
- `test_a_resume_while_the_round_works_is_refused_as_busy` (Review Focus 1; `submit_gate` holds the match).
- `test_a_second_resume_is_a_third_attempt_and_a_redelivered_one_runs_nothing` (Review Focus 3).
- `test_a_resume_while_the_judge_looks_is_refused_and_the_judges_verdict_decides` (judge `active`, the judge's
  job held by `submit_gate`; after the gate opens the set is saved by the judge and `refuse_label` ran once).
- `test_a_resumed_round_starts_in_a_new_run_and_keeps_its_results` (the run id changes, as
  `test_a_restart_starts_its_round_in_a_new_run` checks for a `go`; `tests` and `binding` are kept).

Add the mock activity `resume_root` to `Mocks.acts` (records its call, returns `PINS.root` unless a
`resume_root` argument of `Mocks` says otherwise). In `tests/test_mia_activities.py`:
`test_resume_root_is_the_actions_root_or_the_datasets_own`.

- [ ] **Step 2: Run them and see them fail**

Run: `pixi run pytest tests/test_mia_workflows.py -k "resume" -q`
Expected: FAIL (a `resume` event runs a whole new round).

- [ ] **Step 3: Split `_round`** into `_match`, `_set_for_review`, `_decide` and run the file's tests and the
replay tests: nothing changes yet.

- [ ] **Step 4: Implement** `resume_step`, `revoked`, `resume_root`, `_resume` as above.

- [ ] **Step 5: Run** `pixi run pytest tests/test_mia_workflows.py tests/test_mia_activities.py tests/test_activity_arguments.py -q`
Expected: PASS.

- [ ] **Step 6: Commit** (`mia-data: a round can be resumed: finished steps are kept, the first missing one runs`)

---

### Task 3: Resume on a round that follows a reader's pull request

**Files:**
- Modify: `mia_data/workflows.py` (`_resume`'s `follow`, `_merged`)
- Modify: `mia_data/activities.py` (`ask_autodev`)
- Test: `tests/test_mia_workflows.py`, `tests/test_mia_activities.py`

**Interfaces:**
- Consumes: `_resume`, `self.resumed`, `_decide`, `_match` (Task 2).
- Produces: activity `ask_autodev(snippet_issue: int, round_id: str) -> None`: one comment per round and
  attempt on the snippets issue ("mia-data asks autodev to try this issue again: its dataset's round was
  resumed.") and the label `snippets:go`, as the cleaner's `snippets_go` step does (`mia_buffers/sweep.py`).
- Produces: `MiaDataDataset._resume_follow(self) -> None`.

**What `_resume_follow` does** (spec §4.2 line 2). It reads `autodev_look(snippet_issue, pr)` and, when there
is a pull request (`look.pr or state.pr`), `pr_state`:

| The pull request is | The round |
|---|---|
| merged | `_merged(pr)` |
| closed, unmerged | clears `snippet_issue`, `pr`, `binding`, `binding_hash`, `snippet_name`; `_match(None, None, False)` |
| open, autodev stopped (`needs_human`) | `ask_autodev`, `_stage("developing", "autodev is asked again ...")`, `_follow()` |
| open, head untested | `_on_pr(pr.pr, pr)` (tests it), then `_follow_pr()` unless that ended the round |
| open, head tested, passing, not approved | `_stage("review", extra=<PR link>)`; if the judge is on and the set has no `evidence`, the judge looks and the round acts on it as `_test_head` does; `_follow()` |
| open, head tested, approved | `_stage("merging")`, `_follow()` |
| open, head tested, failing | `_stage("developing")`, `_follow()` |
| none yet, autodev stopped (`look.stopped`) | `ask_autodev`, `_stage("developing")`, `_follow()` |
| none yet, autodev at work or waiting | `_stage("developing")`, `_follow()` |

`_merged`: where it takes the merge commit's own passing set to `review`, a resumed round (`self.resumed`)
goes on with `_decide(test)`; any other round keeps `_from_review()` (spec §9 call 7).

- [ ] **Step 1: Write the failing tests**

```python
async def test_resume_after_the_pull_request_merged_saves_the_approved_crops_and_runs_no_job(env):
    """The incident: the reader was approved, its merge was refused, the round was blocked, and the merge
    came afterwards."""
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await to_review(h, m)
        await approve_review(h, m, eid="12")
        await idle(h, m, "merging")
        await wake(h, m, head(H1, needs_human=True))
        await idle(h, m, "blocked")
        jobs = len(m.of("submit"))
        m.pr_now = head(H1, "merged", MC)
        await h.signal(MiaDataDataset.restart, ev("resume", "13"))
        await idle(h, m, "done")
    assert len(m.of("submit")) == jobs and len(m.of("publish_crops")) == 1 and m.of("admit") == [("11",)]
```

And one test each for the other rows: `..._tests_a_head_the_round_never_tested`,
`..._brings_a_passing_head_back_to_review_and_has_it_judged` (judge skipped the first time),
`..._asks_autodev_again_when_it_stopped` (one `ask_autodev` call, stage `developing`, no `open_snippet_issue`),
`..._of_a_closed_pull_request_matches_again`, `..._waits_for_a_new_head_after_a_failing_one`,
`test_a_resumed_round_has_the_judge_look_at_the_merge_commits_own_set` and its twin
`test_a_round_that_never_stopped_leaves_the_merge_commits_own_set_to_a_person` (already covered by
`test_merge_with_different_tree_makes_the_merge_commits_set_for_review`: assert there that no `judge` job ran).
In `tests/test_mia_activities.py`: `test_ask_autodev_comments_once_and_labels_the_issue`.

- [ ] **Step 2: Run** `pixi run pytest tests/test_mia_workflows.py -k "resume" -q`: the new ones FAIL.
- [ ] **Step 3: Implement** `ask_autodev`, `_resume_follow`, the `_merged` branch.
- [ ] **Step 4: Run** `pixi run pytest tests/test_mia_workflows.py tests/test_mia_activities.py tests/test_activity_arguments.py -q`: PASS.
- [ ] **Step 5: Commit** (`mia-data: a resumed round picks its reader's pull request up as it is now`)

---

### Task 4: The action, the route and a dataset's buttons

**Files:**
- Modify: `mia_data/config.py` (`STAGE_ACTIONS`, `HUMAN_LABELS`)
- Modify: `mia_buffers/api.py` (`ActionBody.action`, `_rules`, the dataset action route's `starts`, the
  dispatch in `_act_on`, `_action_entry`)
- Modify: `mia_buffers/ui/actions.js` (labels, `BUTTON_HELP`, the host dialog's condition)
- Test: `tests/test_buffers_api.py`, `tests/test_buffers_ui.py`, `tests/test_mia_config.py`

**Interfaces:**
- Consumes: the workflow's handling of an event of kind `resume` (Tasks 2, 3); `sets["revoked"]` (Task 2).
- Produces: action `resume` for datasets at `failed`, `blocked`, `needs-human`, `cancelled`, `review`.
  `_rules` removes it with `go` when the record has gaps, and when `sets.get("revoked")`.
- Produces: the dispatch rule: a dataset's `go` and `resume` are both sent as the signal `restart`; the
  event's kind is the action's name.
- Produces: `resume` is a start action: it may carry `approve_hosts` and `root`, like `go`.

- [ ] **Step 1: Write the failing tests** in `tests/test_buffers_api.py`: `resume` is accepted (202) at each of
the five stages and sends `("dataset", number, "restart", event)` with `event.kind == "resume"`; it is refused
(409) at `new`, `done`, `debugging` and `matching`; a dataset whose subject data holds
`{"sets": {"revoked": True}}` lists `go` but not `resume`; one with record gaps lists neither; `resume` with
`approve_hosts` approves them as `go` does. In `tests/test_buffers_ui.py`: a dataset at `blocked` with actions
`["go", "resume"]` draws two buttons, `Resume` first and primary, then `Restart`; at `debugging` the one
button reads `Restart now`; at `new`, `Start`; the tooltips are the texts below.

- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_api.py -k resume tests/test_buffers_ui.py -k "Resume or Restart" -q`: FAIL.

- [ ] **Step 3: Implement.** Exact copy:
  - `HUMAN_LABELS["resume"]`: `"Resume the round: every step that finished is kept, on the same pins"`.
  - Button `Resume`, tooltip: `Run what is left. Every step that finished is kept (the match, the reader's
    pull request, the crops, the judge's verdict), and the round stays on the catalog, reader and harness
    versions it began with. A step that failed because of a fault in one of those fails again: use Restart.`
  - Button `Restart` (today's `Run again` and `Start again` labels), tooltip: today's text for that stage,
    preceded by `Start over from scratch on today's catalog, readers and harness.`
  - Every other use of "Run again" for a dataset in `mia_buffers/ui/` (`dom.js`, `subjects.js`,
    `operations.js`, `actions.js`) reads `Restart`.

- [ ] **Step 4: Run** `pixi run pytest tests/test_buffers_api.py tests/test_buffers_ui.py tests/test_mia_config.py -q` and `pixi run lint`: PASS.
- [ ] **Step 5: Commit** (`a dataset takes resume beside go: the route, the stage table, its page's buttons`)

---

### Task 5: Requests and the bulk boxes

**Files:**
- Modify: `mia_buffers/spec.py` (the request's `restart` key), `mia_buffers/config.py` (`PREP_STAGE_ACTIONS`),
  `mia_buffers/activities.py` (`prep_admit`, `_nudge`), `mia_buffers/api.py` (`_prep_action`, dispatch)
- Modify: `mia_buffers/ui/actions.js` (`prepareButton`), `mia_buffers/ui/datasets.js` (`chosenBox`),
  `mia_buffers/ui/flow.js` (its call)
- Test: `tests/test_buffers_spec.py`, `tests/test_buffers_activities.py`, `tests/test_buffers_api.py`,
  `tests/test_buffers_ui.py`

**Interfaces:**
- Consumes: a dataset's `resume` action (Task 4).
- Produces: a request's text key `restart: bool = False`; `again` is read as the same key. `PrepPins.again`
  keeps its name and carries the round's mode.
- Produces: `PREP_STAGE_ACTIONS`: `resume` at `done`, `needs-human`, `cancelled` beside `go`. buffer-api sends
  a request's `resume` as the workflow's `go` signal with an event of kind `resume`.
- Produces: `prep_admit` sets `PrepPins.again`: the text's `restart` for the action that filed the request;
  for a later action, `True` for `go` and `False` for `resume`.
- Produces: `_nudge(round_id, record, again)` sends (spec §5): with `again`, `go` to every dataset that
  accepts it; without, `resume` where the dataset accepts it, else `go` only at `new`; nothing to one that is
  working, shelved, prepared, or waiting for an unapproved host. The key stays `<round id>/<record>`.
- Produces: `prepareButton(container, all, blocker, done, text, owner, restart)`: writes `restart: true` only
  when `restart`. `chosenBox` draws `Resume` and `Restart` (then `Create buffer` when `owner == null`), both
  through `prepareButton`.

- [ ] **Step 1: Write the failing tests**: the spec reads `restart: true` and `again: true` alike; `_nudge`
per row of the table in spec §5 (parametrized: stage, actions, `again` → the action sent or none);
`prep_admit` for a filing `go`, a later `go`, a `resume`; the API accepts `resume` on a request at
`needs-human` and signals `go`; `chosenBox` holds two prepare buttons whose requests differ only by
`restart: true`.
- [ ] **Step 2: Run** the four files with `-k "resume or restart or nudge or chosen"`: FAIL.
- [ ] **Step 3: Implement.** Tooltips of the box's buttons: `Resume`: `Prepare the selected datasets: one that
never ran starts, one that stopped runs what is left, one that is prepared is left alone.` `Restart`:
`Prepare every selected dataset from scratch, also one that is prepared or waits for review.`
- [ ] **Step 4: Run** the four files in full and `pixi run lint`: PASS.
- [ ] **Step 5: Commit** (`a request resumes its datasets unless it says restart; the bulk boxes offer both`)

---

### Task 6: The cleaner resumes

**Files:**
- Modify: `mia_buffers/cleaner.py` (`plan_for`, the step texts, `reduce_outcome`'s menu),
  `mia_buffers/sweep.py` (`ACTIONS`, `STARTS_WORK`, `_act`, the guards on `go`), `mia_buffers/diagnose.py`
  (the menu text), `mia_buffers/ui/operations.js` (rule texts)
- Test: `tests/test_buffers_cleaner.py`, `tests/test_buffers_sweep.py`

**Interfaces:**
- Consumes: `resume` on datasets and requests (Tasks 4, 5).
- Produces: plan steps `{"do": "resume"}` for `no run`, `frozen run` (datasets), `autodev's run failed`,
  `merge conflict` after `revise`, `merged since`, `passing cause`, `fixed since`, and the `stopped` plan in
  `sweep.py`. `{"do": "go"}` stays for `input changed`, for `merge conflict` after `close`, and for any step
  that carries a `note` (the diagnosing agent's advice).
- Produces: `sweep._act` treats `resume` as it treats `go`: the root of the last start action, the request
  guard (`_request_may_go`), the weekly limit, no send while a job of the ended round is alive or after a
  person acted. To a request the cleaner sends `resume`, never `go`.
- Produces: the diagnosing agent's menu holds `resume` ("run what is left of the round") beside `go` ("run
  the subject again from the beginning; give a `note` ...").

- [ ] **Step 1: Write the failing tests**: `plan_for` per rule above ends in the step named;
`test_the_cleaner_resumes_a_request_and_never_restarts_its_prepared_datasets` (Review Focus 5: a request at
`needs-human` whose actions hold both gets `resume`); a `resume` step carries the last start action's root;
the weekly limit counts it; `reduce_outcome` keeps a proposed `resume` and drops an unknown step.
- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_cleaner.py tests/test_buffers_sweep.py -q`: the new ones FAIL.
- [ ] **Step 3: Implement**, and bring the rule texts in `operations.js` and `docs/specs/2026-11-15-cleaner.md`
in line (they say what each rule's plan does).
- [ ] **Step 4: Run** the two files and `tests/test_buffers_ui.py`: PASS.
- [ ] **Step 5: Commit** (`cleaner: its plans resume a round; a restart stays where the remedy needs new pins`)

---

### Task 7: Docs and the full checks

**Files:**
- Modify: `AGENTS.md`, `docs/development.md`, `docs/autodev-runbook.md`, `mia_buffers/ui/guide.html`,
  `docs/specs/2026-11-18-resume-restart.md` (status line)

- [ ] **Step 1: `AGENTS.md`**: a rule "**A round is resumed or restarted.**" stating: `resume` keeps the
round id, pins and every result on its state and starts at `resume_step`; `go` wipes and pins anew; a step
that runs again takes the next attempt, so keys, workspaces, test ids and markers are new; a new step of a
round records its result on the state and gets a line in `resume_step`; a force stop loses the round. Bring
the stage-table rule's list of what each stage takes in line.
- [ ] **Step 2: `docs/development.md`, the runbook, the guide**: Resume and Restart where they describe
running a dataset or a request again, with the difference the tooltips state and the one consequence (a fix
in the harness or a reader needs Restart).
- [ ] **Step 3: Run everything**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all clean. `git diff judge-decides | grep -n 'rokickik\|/nrs/'` prints nothing.

- [ ] **Step 4: Commit** (`docs: resume or restart`), set the spec's status to built on branch `resume`.

## After the last task

Stop and report. Merging into the live branch and `pixi run mia restart` are the user's; nothing is pushed.
The harness is not touched, so no deploy order applies. The buffer round (spec §6) is planned next.
