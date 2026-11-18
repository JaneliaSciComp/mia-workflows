# The crop judge decides: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An active crop judge approves a set or sends it back to the developer, at most
`judge.crops.max_reworks` times, and a person can overturn either decision later with one click.

**Architecture:** The judge's answer is reduced to approve / reject / escalate plus its worst finding; the
dataset workflow acts on it the way it acts on a person's verdict. A set that was sent back, and the set that
was saved, are kept in the workflow's state as `CropSet`s that outlive their round, are shown to the page
through the subject's data, and are named by every verdict. autodev gains two things: a validated "nothing to
change" ending and an `/accept <sha>` command that resets its pull request to an earlier head and merges it.

**Tech Stack:** Python 3.12, Temporal workflows and activities, pydantic config, SQLite ledger behind
buffer-api, ES-module page with no build step, pytest.

**Spec:** `docs/specs/2026-11-17-judge-decides.md` (v0.2). Read it first; section numbers below are its.

## Global Constraints

- Activity result dataclasses in `models.py` are wire format: add fields with defaults, never rename or remove.
- Every change to what `MiaData` or autodev's `IssueWorkflow` issues is behind
  `workflow.patched("judge-decides")`; a run without the marker behaves as today. Re-record the replay
  fixtures as `docs/development.md` describes, in the task that changes the workflow.
- `STAGE_ACTIONS` changes first; buffer-api refuses and the page hides whatever the table does not hold.
- The page holds no token and calls action routes only; a module imports only modules below it.
- Output of sandboxed jobs is untrusted: `read_untrusted`, reduce to a typed result, `neutralize` before
  posting.
- No real paths or hostnames in tracked files. Comments and docs say what the code does, never what it
  replaced.
- Run only the tests that cover a task (`pixi run python -m pytest tests/<file> -q -k <name>`), with
  `PYTHONPATH=$PWD` in this worktree; `pixi run lint` and `pixi run typecheck` before each commit; the full
  suite once, in Task 12.
- Defaults from the spec: `judge.crops.max_reworks` = **1**; the judge's worst finding decides: all `ok` →
  approve, any `suspect` or `bad` → sent back; escalate, low confidence, no run → a person.

## Review Focus

1. **A run that began before the marker gets a verdict that names a set.** It must act as today (the set
   under review) and never touch `held`. Test in Task 6.
2. **Accept arrives twice, or while the first is at `merging`.** The second is the same action (its `key`) or
   is refused with a sentence; nothing merges twice. Test in Task 9.
3. **The held set's crop workspace is gone when Accept is carried out.** Refused on the timeline, nothing
   merged or saved, the dataset stays where it was. Test in Task 9.
4. **`no_change.json` is hostile or sloppy:** oversized, not JSON, names another request, answers findings the
   request did not have, leaves one out. Never counted; the round reads as a developer that stopped. Test in
   Task 7.
5. **`max_reworks: 0`.** The judge never sends back; every set that is not all `ok` waits at `review`. Test
   in Task 6.

## File Structure

| File | Responsibility in this plan |
|---|---|
| `mia_core/judge/schema.py` | `effective_verdict` without the cap; `worst_finding` |
| `mia_core/judge/policy.py` | a person outranks the judge and the developer |
| `mia_core/judge/prompts/crops.md` | what `suspect` means |
| `mia_core/config.py` | `GateCfg.max_reworks`; `max_suspect` refused |
| `mia_data/models.py` | `CropSet`; new fields on `LabelEvent`, `JudgeResult`, `AutodevLook`, `RoundLimits`, `DatasetState` |
| `mia_data/config.py` | `STAGE_ACTIONS` |
| `mia_data/activities.py` | `read_judge`, `judge_feedback`, `share_evidence`, `autodev_look`, `close_snippet_issue`, repair by recipe |
| `mia_data/workflows.py` | send-back, the limit, "right as they are", Accept, Reject at `done` |
| `mia_buffers/api.py`, `mia_buffers/ledger.py` | `ActionBody.set`, which set a verdict may name, subject data, overturned decisions |
| `mia_buffers/ui/actions.js`, `subjects.js` | the set under rework with Accept; Reject at `done` |
| `autodev/agent.py`, `review.py`, `decide.py`, `activities.py`, `workflows.py`, `config.py`, `models.py` | `no_change.json`, `/accept`, the evidence mount |
| `AGENTS.md`, `docs/development.md`, `mia.yaml.example` | the rules and the setting |

---

### Task 1: The judge's answer by its worst finding

**Files:**
- Modify: `mia_core/judge/schema.py` (`effective_verdict`), `mia_core/config.py` (`GateCfg`),
  `mia_core/judge/prompts/crops.md`, `mia_data/models.py` (`JudgeResult`), `mia_data/activities.py`
  (`read_judge`, `limits`), `mia.yaml.example`
- Test: `tests/test_core_judge.py`, `tests/test_core_config.py`, `tests/test_mia_activities.py`

**Interfaces:**
- Produces: `effective_verdict(verdict: Verdict, min_confidence: float) -> tuple[str, str]`;
  `worst_finding(findings) -> str` returning `"bad"`, `"suspect"` or `""`;
  `GateCfg.max_reworks: int = 1` (`ge=0`); `JudgeResult.worst: str = ""`;
  `RoundLimits.max_reworks: int = 1`.

- [ ] **Step 1: Write the failing tests**

```python
def test_one_suspect_finding_rejects_and_no_count_of_them_is_needed():
    verdict = Verdict("approve", 0.9, (Finding("crop-3", "suspect", "labels look shifted"),), "fine")
    assert effective_verdict(verdict, 0.6) == ("reject", "a suspect finding: crop-3")
    assert worst_finding(verdict.findings) == "suspect"

def test_a_bad_finding_outranks_a_suspect_one():
    findings = (Finding("crop-1", "suspect", ""), Finding("crop-2", "bad", ""))
    assert effective_verdict(Verdict("approve", 0.9, findings, ""), 0.6) == ("reject", "a bad finding: crop-2")
    assert worst_finding(findings) == "bad"

def test_a_reject_with_every_finding_ok_is_still_an_approve_with_notes():
    verdict = Verdict("reject", 0.9, (Finding("crop-1", "ok", "dim"),), "")
    assert effective_verdict(verdict, 0.6)[0] == "approve" and worst_finding(verdict.findings) == ""

def test_max_suspect_in_a_config_is_refused_with_a_sentence():
    with pytest.raises(ValidationError, match="max_suspect is gone"):
        GateCfg.model_validate({"mode": "active", "max_suspect": 3})
    assert GateCfg().max_reworks == 1
```

In `tests/test_mia_activities.py`, beside the existing `read_judge` tests: a verdict with one `suspect`
finding gives `JudgeResult.effective == "reject"`, `worst == "suspect"`, `authorized is False`.

- [ ] **Step 2: Run them; expect failures on the missing names and the old three-argument call**
- [ ] **Step 3: Implement.** Escalate and the confidence floor first, then `bad`, then `suspect`, else
  approve. `GateCfg` drops `max_suspect` and refuses the key in a `model_validator(mode="before")` with
  "`max_suspect` is gone: one suspect finding sends a set back; `max_reworks` bounds how often".
  `read_judge` stops passing the cap and sets `worst`. `limits` fills `RoundLimits.max_reworks` from
  `cfg.judge.crops.max_reworks`.
- [ ] **Step 4: Rewrite the `suspect` paragraph of `prompts/crops.md`** to: `suspect` means "this may be a
  defect in how the data was read (shifted or swapped labels, a wrong axis, a wrong channel, truncated
  values) and is worth the developer's look"; contrast, mostly-background crops and aesthetics are `ok` with
  a note. Update the expected verdicts of the evaluation fixtures that `tests/test_core_judge.py` and
  `mia judge eval` read (`grep -rn suspect tests/fixtures mia_core/judge`).
- [ ] **Step 5: Run the three test files; lint; typecheck; commit** (`judge: a set's worst finding decides,
  and max_reworks replaces the suspect cap`).

### Task 2: A person outranks the judge and the developer

**Files:**
- Modify: `mia_core/judge/policy.py` (`authorized`, `new_assessment` unchanged)
- Test: `tests/test_core_policy.py`

**Interfaces:**
- Produces: `MACHINE_AUTHORS = ("judge", "policy:developer")`; `authorized()` keeps its signature. When any
  row of the subject is `by` a `human:`, rows by `MACHINE_AUTHORS` are left out before the existing rule runs.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_judge_reject_written_after_a_persons_approve_does_not_undo_it():
    rows = [row("approve", "human:kr", at=10), row("reject", "judge", at=20, gate="crops")]
    assert authorized(rows, enabled_gates=frozenset({"crops"})).ok

def test_a_judge_or_developer_approve_written_after_a_persons_reject_authorizes_nothing():
    for author in ("judge", "policy:developer"):
        rows = [row("reject", "human:kr", at=10), row("approve", author, at=20, gate="crops")]
        assert not authorized(rows, enabled_gates=frozenset({"crops"})).ok

def test_a_developer_approve_stands_over_a_judge_reject_while_no_person_spoke():
    rows = [row("reject", "judge", at=10, gate="crops"), row("approve", "policy:developer", at=20, gate="crops")]
    assert authorized(rows, enabled_gates=frozenset({"crops"})).ok

def test_a_repair_approve_still_follows_a_persons_revoke():
    rows = [row("reject", "human:kr", at=10, gate="staged"), row("approve", "policy:repair", at=20, gate="staged")]
    assert authorized(rows).ok
```

`row` is the file's existing helper for building an assessment; add `gate` and `at` parameters if it lacks them.

- [ ] **Step 2: Run; the first two fail**
- [ ] **Step 3: Implement the filter at the top of `authorized()`**
- [ ] **Step 4: Run `tests/test_core_policy.py`; commit** (`policy: a person's verdict on a set outranks the
  judge's and the developer's, whenever each was written`).

### Task 3: A save repairs by recipe

**Files:**
- Modify: `mia_data/activities.py` (`publish_crops`, `_release_revoked`)
- Test: `tests/test_mia_publish.py` (or where `_release_revoked` is tested: `grep -rn policy:repair tests`)

**Interfaces:**
- Produces: `_release_revoked(round_id: str, record: str, binding_hash: str) -> None`. `binding_hash` is the
  sha256 of the binding file as the save wrote it into the dataset folder: the same bytes a buffer hashes
  into a staged asset's `data["binding_hash"]` (`mia_buffers/activities.py`, `_stage_block`).

- [ ] **Step 1: Write the failing test** `test_a_save_releases_only_the_artifacts_staged_from_its_own_binding`:
  two quarantined staged assets of one record, `binding_hash` `"aaa"` and `"bbb"`; after a save whose binding
  hashes to `"aaa"`, the first has `quarantined: False` and a `policy:repair` approve, the second is still
  quarantined and has no new assessment.
- [ ] **Step 2: Run; it fails (both are released)**
- [ ] **Step 3: Implement:** skip an asset whose `data.get("binding_hash") != binding_hash`.
- [ ] **Step 4: Run the file; update the paragraph on repair in `docs/development.md`; commit** (`datasets: a
  save releases only the artifacts its own binding staged`).

### Task 4: Sets that outlive their round

**Files:**
- Modify: `mia_data/models.py`, `mia_data/workflows.py` (`_publish_set`, `_round`), `mia_data/activities.py`
  (`set_stage`, `_post`), `mia_core/registry.py` (`append_entry`), `mia_buffers/api.py` (`append_entry`
  route), `mia_buffers/ledger.py`
- Test: `tests/test_mia_workflows.py`, `tests/test_buffers_api.py`, `tests/test_core_registry.py`

**Interfaces:**
- Produces, in `mia_data/models.py`:

```python
@dataclass
class CropSet:
    """One set of sample crops with everything a later verdict needs, kept after the round that made it."""

    set_id: str  # "<round_id>:<test_id>"
    round_id: str
    test: Test
    pins: Pins
    binding: str | None = None
    binding_hash: str | None = None
    pr: int | None = None
    snippet_issue: int | None = None
    sent_back_by: str = ""  # "judge" or the person's actor; "" for a set that was not sent back
    worst: str = ""  # the judge's worst finding on it: "bad" | "suspect" | ""
    request: str = ""  # the id of the rework request the developer answers: "<set_id>#<n>"
    opened_issue: bool = False  # the send-back opened the snippets issue: an Accept closes it
```

  `DatasetState.held: CropSet | None = None`, `saved: CropSet | None = None`, `reworks: int = 0`,
  `accepting: str | None = None`.
- Produces: `MiaData._set_of(test: Test) -> CropSet` (from the state as it is now) and
  `set_id(round_id: str, test: Test) -> str`.
- Produces: the subject's `data["sets"]`, written with every stage write:
  `{"review": <set_id or None>, "held": {"set", "by", "worst", "findings", "gallery"} or None,
  "saved": {"set", "approved_by"} or None}`. `Registry.append_entry(..., data: dict | None = None)` and the
  route merge `data` into the subject's data in the transaction that writes the stage.

- [ ] **Step 1: Write the failing tests.** Registry and api: an entry posted with `data={"sets": {...}}`
  shows in `GET /subjects/dataset/<key>` under `data.sets`, and a later entry without `data` leaves it.
  Workflow (model on `test_an_active_judges_authorized_approve_saves_the_set_with_no_person`): after the save,
  `state.saved.set_id == f"{state.round_id}:{state.tests[-1].test_id}"`, `state.held is None`, and a new `go`
  leaves `state.saved` in place while it clears `state.tests`.
- [ ] **Step 2: Run; they fail on the missing fields and parameter**
- [ ] **Step 3: Implement.** `_publish_set` sets `saved` and clears `held` after the save succeeded; `_round`
  does not touch `held` or `saved`; `set_stage` passes `sets` built from the state.
- [ ] **Step 4: Run the three files; commit** (`datasets: the saved set and a set sent back are kept with the
  dataset and shown to the page`).

### Task 5: A verdict names its set

**Files:**
- Modify: `mia_data/config.py` (`STAGE_ACTIONS`), `mia_data/models.py` (`LabelEvent`), `mia_buffers/api.py`
  (`ActionBody`, `_rules`, `_dataset_action`, the `LabelEvent(...)` at the send), `mia_buffers/dispatch.py`
  if a verdict to a dataset at rest is not delivered today
- Test: `tests/test_buffers_api.py`, `tests/test_buffers_dispatch.py`, `tests/test_mia_config.py`

**Interfaces:**
- Produces: `LabelEvent.set: str = ""`; `ActionBody.set: str | None = None`.
- `STAGE_ACTIONS`: `approve` added at `developing`, `testing`, `debugging`, `failed`, `blocked`,
  `needs-human`, `cancelled`; `reject` added at `done`.
- `_rules` for a dataset: `approve` outside `review` only while `data.sets.held` is set; `reject` at `done`
  only while `data.sets.saved` is set.
- `_dataset_action`: an `approve` or `reject` without `set` is 422 ("a verdict names the set it is about:
  `set`"); a `set` that is not one of the sets the stage allows is 409 ("that set is not the one under
  review" / "that set can no longer be accepted: a newer one was approved"). Allowed: at `review` the review
  set (both verdicts) and the held set (approve); elsewhere the held set (approve); at `done` the saved set
  (reject). A `reject` at `done` is a start action: it goes through `_check_hosts` with
  `partial(self._needed, records=[record])` as a `go` does. The payload carries `set`.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_verdict_without_a_set_is_422_and_one_for_another_set_is_409(service):
    at_review(service, record="r1", review="i7-e1:t1")
    assert act(service, "r1", {"action": "approve", "revision": 1})[0] == 422
    assert act(service, "r1", {"action": "approve", "revision": 1, "set": "i7-e0:t9"})[0] == 409

def test_a_dataset_under_rework_takes_an_approve_of_the_held_set_only(service):
    subject = at_stage(service, "r1", "developing", sets={"held": {"set": "i7-e1:t1", "by": "judge"}})
    assert "approve" in view(service, subject)["actions"]
    assert act(service, "r1", {"action": "approve", "revision": 2, "set": "i7-e1:t1"})[0] == 202
    plain = at_stage(service, "r2", "developing", sets={})
    assert "approve" not in view(service, plain)["actions"]

def test_a_reject_at_done_names_the_saved_set_and_checks_hosts_like_a_start(service):
    at_stage(service, "r1", "done", sets={"saved": {"set": "i7-e1:t1", "approved_by": "judge"}})
    status, body = act(service, "r1", {"action": "reject", "revision": 3, "set": "i7-e1:t1", "note": "shifted"})
    assert status == 202 and sent(service)[-1].set == "i7-e1:t1"
```

Build `at_review`, `at_stage`, `act`, `view`, `sent` from the file's existing fixtures for dataset actions.

- [ ] **Step 2: Run; they fail**
- [ ] **Step 3: Implement table, rules, route and the event's `set`**
- [ ] **Step 4: Run the files; write the table change into `AGENTS.md`** (the stage-tables rule gains: "a
  dataset under rework also takes an `approve` of the set that was sent back, and a `done` one a `reject` of
  its saved set; a verdict names its `set`"); **commit** (`buffer-api: a verdict names its set, and a set
  sent back can be approved at any stage`).

### Task 6: The judge sends a set back, at most `max_reworks` times

**Files:**
- Modify: `mia_data/workflows.py` (after `_judged` on both paths, `_round`, `_verdict`),
  `mia_data/activities.py` (new `judge_feedback`)
- Test: `tests/test_mia_workflows.py`, `tests/test_mia_activities.py`, `tests/test_activity_arguments.py`,
  the replay fixtures

**Interfaces:**
- Consumes: `JudgeResult.worst`, `RoundLimits.max_reworks`, `CropSet`, `_set_of`.
- Produces: activity `judge_feedback(issue: int, round_id: str, workspace: str, request: str) -> str`: the
  findings of `<workspace>/verdict.json` as text for the developer, first line `Request: <request>`, then
  one line per finding that is not `ok` (`- <item> [<severity>]: <note>`), then the two endings of spec §4.2
  and the `no_change.json` contract of Task 7. Cleaned like any posted text.
- Produces: `MiaData._sends_back(judged: JudgeResult | None) -> bool` (gate active, `effective == "reject"`,
  no person's verdict queued) and `MiaData._send_back(test: Test, judged: JudgeResult) -> None`.

Behaviour of `_send_back`:
1. `state.reworks >= limits.max_reworks`: `_found("The crop judge found <worst> crops and has sent this
   dataset back <n> time(s) already (judge.crops.max_reworks): the set waits for your verdict.")`; the round
   stays at `review` and waits as today.
2. Otherwise `state.reworks += 1`; `state.held = _set_of(test)` with `sent_back_by="judge"`, `worst`,
   `request=f"{set_id}#{state.reworks}"`; `_found("Sent back by the crop judge: ...")`; then, with
   `feedback = judge_feedback(...)`: a matched binding's set → `_develop({"what_fails": feedback},
   state.snippet_issue, binding=state.binding)` and `held.opened_issue = True` when no issue existed; a pull
   request head's set → the `/revise {feedback}` comment (marker `revise <round> judge <sha>`),
   `state.revised_at`, stage `developing`.

`state.reworks = 0` in `_round` when the `go`'s actor is not `DEBUGGER`, and in `_verdict` when a person's
verdict is taken.

- [ ] **Step 1: Write the failing tests** (model on `test_an_active_judges_authorized_approve_saves_the_set_
  with_no_person` and `test_reject_in_matched_review_asks_for_a_superseding_snippet`; the `judged` fixture
  sets what the judge returns):
  - `test_an_active_judge_that_finds_a_suspect_crop_sends_a_matched_set_to_the_developer`: stage ends
    `developing`; `open_snippet_issue` was called with a draft whose `what_fails` starts `Request: `;
    `state.held.sent_back_by == "judge"`, `state.reworks == 1`.
  - `test_an_active_judge_sends_a_pr_heads_set_back_as_a_revision_request`: a `/revise` comment on the PR,
    stage `developing`.
  - `test_the_second_send_back_waits_at_review_instead`: with `max_reworks=1`, the next head's `suspect`
    verdict leaves stage `review` and no second `/revise`.
  - `test_max_reworks_zero_never_sends_back` (Review Focus 5).
  - `test_a_persons_verdict_starts_the_count_over`.
  - `test_a_run_without_the_marker_leaves_a_rejecting_judge_to_a_person` (Review Focus 1: replay of the
    existing active-judge fixture passes unchanged).
  - activity: `judge_feedback` on a `verdict.json` with one `ok`, one `suspect`, one `bad` finding lists two
    lines and starts with the request line.
- [ ] **Step 2: Run; they fail**
- [ ] **Step 3: Implement behind `workflow.patched("judge-decides")`**
- [ ] **Step 4: Run the tests; re-record the replay fixtures the change touches; commit** (`datasets: an
  active crop judge sends a doubted set back to the developer, max_reworks times`).

### Task 7: autodev can answer "nothing to change", validated

**Files:**
- Modify: `autodev/agent.py` (prompt lines, new `read_no_change`), `autodev/activities.py`
  (`publish_round`), `autodev/models.py` (`PrRef`), `autodev/config.py` (`STAGES`),
  `autodev/workflows.py` (`IssueWorkflow.run`), `mia_data/activities.py` (`autodev_look`),
  `mia_data/models.py` (`AutodevLook`)
- Test: `tests/test_autodev_agent.py`, `tests/test_autodev_activities.py`, `tests/test_autodev_workflows.py`,
  `tests/test_mia_activities.py`

**Interfaces:**
- Produces: `read_no_change(round_dir: Path, request_text: str) -> str`: the request id when
  `<round_dir>/no_change.json` is a valid answer to `request_text`, else `""`. Valid means: read with
  `read_untrusted` (cap 256 KiB), a JSON object `{"request": str, "findings": [{"item": str, "answer": str}]}`,
  `request` equal to the id on the `Request: ` line of `request_text`, and exactly the items of the
  request's finding lines, each with a non-empty `answer`.
- Produces: `PrRef.no_change: str = ""`; `publish_round` sets it when the round made no commits, the job
  ended `DONE` with exit 0 and was not cut off, and `read_no_change` returned an id. Its comment then carries
  the marker `no-change <request id>` and the answers, cleaned.
- Produces: stage `"answered"` in `autodev.config.STAGES` ("autodev: looked into it, nothing to change"). On
  the issue path `IssueWorkflow.run` sets it and returns `"answered"` where it raises "the agent made no
  commits" today; without a valid answer it raises as today.
- Produces: `AutodevLook.no_change: str = ""`: the request id of the newest `no-change` marker by the bot on
  the issue or the pull request.

- [ ] **Step 1: Write the failing tests**

```python
REQUEST = "Request: i7-e1:t1#1\n- crop-3 [suspect]: labels look shifted\n- crop-5 [bad]: empty"

def test_a_complete_answer_to_this_request_counts(tmp_path):
    write(tmp_path, {"request": "i7-e1:t1#1", "findings": [
        {"item": "crop-3", "answer": "the shift is the label's own"}, {"item": "crop-5", "answer": "no cells there"}]})
    assert read_no_change(tmp_path, REQUEST) == "i7-e1:t1#1"

@pytest.mark.parametrize("content", [
    None,                                                     # no file
    "not json",
    {"request": "i7-e0:t9#1", "findings": []},                # another request
    {"request": "i7-e1:t1#1", "findings": [{"item": "crop-3", "answer": "x"}]},            # one left out
    {"request": "i7-e1:t1#1", "findings": [{"item": "crop-3", "answer": ""}, {"item": "crop-5", "answer": "x"}]},
    {"request": "i7-e1:t1#1", "findings": [{"item": "crop-3", "answer": "x"}, {"item": "crop-5", "answer": "x"},
                                           {"item": "crop-9", "answer": "x"}]},           # one too many
    "x" * (300 * 1024),                                       # oversized
])
def test_anything_else_is_not_an_answer(tmp_path, content):   # Review Focus 4
    write(tmp_path, content)
    assert read_no_change(tmp_path, REQUEST) == ""
```

  Activities: a no-commit round with a valid file gives `PrRef.no_change`, and a cut-off or failed job gives
  `""` whatever the file says. Workflow: an issue round with `no_change` ends `"answered"` with the stage
  label set; one without it fails as today. `autodev_look` returns the id of the bot's marker and ignores the
  same text in anyone else's comment.
- [ ] **Step 2: Run; they fail**
- [ ] **Step 3: Implement; add to the agent's instructions:** "If the request says a set of crops was sent
  back and you find the crops right as they are, make no commits and write `no_change.json` in the workspace
  root: the request's id and, for every finding of the request, what you looked at and why it is not a
  defect. Do not write a reader to satisfy a doubt."
- [ ] **Step 4: Run the four files; commit** (`autodev: "nothing to change" is an outcome the agent states
  and autodev validates`).

### Task 8: What the round does with "right as they are"

**Files:**
- Modify: `mia_data/workflows.py` (`_answered`, `_look`)
- Test: `tests/test_mia_workflows.py`

**Interfaces:**
- Consumes: `AutodevLook.no_change`, `state.held.request`, `state.held.worst`, `MACHINE_AUTHORS`.
- Produces: `MiaData._restore(held: CropSet) -> None`: `round_id`, `pins`, `binding`, `binding_hash`, `pr`,
  `snippet_issue` and `tests = [held.test]` become the held set's.
- Produces: `MiaData._approve_held(by: str, at: str) -> None`: writes an approve assessment on the held
  set's subject (`by="policy:developer"` here), calls `_restore`, then saves the set (`_publish_set`) or asks
  autodev to merge its head (`_pr_approved`).

Rule in `_answered` (with the marker): `look.no_change == state.held.request` is required whenever the held
set was sent back by the judge, and on the issue path for anyone's send-back. Then:

| `held.sent_back_by` | `held.worst` | Result |
|---|---|---|
| `judge` | `suspect` | `_found("The developer looked into the judge's findings and changed nothing: ...")`, `_approve_held("policy:developer", now)` |
| `judge` | `bad` | stage `review` on the held set: "the judge found a bad crop and the developer says the crops are right; the set waits for your verdict", both quoted |
| a person | any | stage `review` on the held set with the answer quoted (as today on the pull request path) |

A look that finds autodev stopped, with no valid answer, ends `blocked` as today.

- [ ] **Step 1: Write the failing tests** (model on
  `test_a_revision_autodev_answers_without_a_new_head_comes_back_to_review_with_the_answer`):
  `test_the_developers_nothing_to_change_approves_a_suspect_only_set`,
  `test_the_developers_nothing_to_change_on_a_bad_set_waits_for_a_person`,
  `test_a_no_commit_round_without_a_validated_answer_approves_nothing` (the look has `answer` text but
  `no_change == ""`: the judge-sent set is not approved and not brought to review),
  `test_an_answer_on_an_issue_with_no_pull_request_brings_a_persons_rejected_set_back_to_review`.
- [ ] **Step 2: Run; they fail**
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run; re-record fixtures if touched; commit** (`datasets: a developer's validated "nothing to
  change" approves a suspect-only set and brings any other back to review`).

### Task 9: Accept a set that was sent back

**Files:**
- Modify: `mia_data/workflows.py` (`_guard`, `run`, `_verdict`, `_on_pr`, new `_accept_held`, `_restore`),
  `mia_data/activities.py` (new `close_snippet_issue`), `autodev/review.py`, `autodev/decide.py`,
  `autodev/models.py`, `autodev/activities.py`, `autodev/workflows.py`, `autodev/gitops.py` (the mirror)
- Test: `tests/test_mia_workflows.py`, `tests/test_autodev_review.py`, `tests/test_autodev_decide.py`,
  `tests/test_autodev_gitops.py`, `tests/test_autodev_workflows.py`

**Interfaces (autodev):**
- `ACCEPT_RE = re.compile(r"^/accept\s+([0-9a-f]{40})\b", re.M)`;
  `accepted(comments: list[dict], reviewers: Reviewers, heads: Sequence[str]) -> str`: the full sha of the
  newest `/accept` by someone who `may_command`, when it is one of `heads`; else `""`.
- `PrSnapshot.accepted: str = ""`; `observe_pr` fills it, with `heads` read from the refs
  `refs/autodev/issue-<n>/*` of the mirror (every head autodev pushed for the issue).
- `decide()` gains the action `RESET = "RESET"`: after the merged and closed checks, a snapshot with `accepted` returns `(RESET, accepted)`
  while `head_sha != accepted`, else `MERGE` when checks are `success` or `none`, else the existing
  escalation for failing checks. Pending feedback never yields `REVISE` once `accepted` is set.
- Activity `reset_pr(ResetInput(issue: int, pr: int, sha: str)) -> None`: force-pushes the pull request's
  branch to `sha`; refuses a sha that is not among the issue's refs.
- `_run_round`: after the agent job ends and before `publish_round`, activity `accepted_head(pr: int) -> str`;
  a non-empty answer drops the round's result unpushed and returns `PrRef(state.pr, None)`.

**Interfaces (mia-data):**
- `MiaData._accepts() -> LabelEvent | None`: the oldest queued approve whose `set == state.held.set_id`.
- `_guard` waits on the round's task, a cancel and `_accepts()`; an accept ends the round's task as
  `_cancel_round` does (jobs killed, reservations settled, locks released), writes no `cancelled` stage, and
  runs `_accept_held`. The idle loop in `run` does the same check before `_refuse_queued`.
- Consumes: `MiaData._restore` (Task 8).
- `_verdict` leaves an approve that names the held set to `_accepts()`, takes a verdict whose `set` is the
  review set's or `""` (a sender from before sets), and refuses any other set on the timeline ("that set is
  not the one under review").
- `MiaData._accept_held(event: LabelEvent) -> None`:
  1. refuse on the timeline (`_refuse`) and return when `state.held` is gone or its crop and verify
     workspaces cannot be read (activity `read_verify` raises): "that set can no longer be accepted";
  2. `_assess("approve", event, subject)`; `_found("Accepted by <actor>: the set the <judge|you> sent back.")`;
  3. `_restore(held)`;
  4. no pull request: `close_snippet_issue(held.snippet_issue, None, why)` when `held.opened_issue`, then
     `_publish_set(held.test)`;
  5. a pull request: comment `/accept <held.test.sha>` (marker `accept <round> <sha>`),
     `state.accepting = sha`, stage `merging` with "accepted by <actor>; autodev merges the pull request as
     it was at `<sha>` once its running job has ended", then `_follow_pr()`;
  6. any `ActivityError` or `RoundFailed` in 4 or 5: `_finish("blocked", "The set <id> was accepted by
     <actor>; <step> failed: <error>. Accept can be sent again.", debug=False)`; `state.held` stays.
- `_on_pr`: while `state.accepting`, a head other than it is not tested and its results are ignored; the
  merge of the accepted head goes to `_merged` as today and clears `accepting`.
- Activity `close_snippet_issue(snippet_issue: int, pr: int | None, why: str) -> None`: closes the issue
  (and the pull request when given) with one comment.

- [ ] **Step 1: Write the failing tests**

```python
def test_an_accept_outranks_pending_feedback_and_resets_before_it_merges():
    snap = snapshot(head_sha="b" * 40, accepted="a" * 40, feedback=[fb("f1")], approved=False, checks="success")
    assert decide(snap, [], 0, 3) == (RESET, "a" * 40)
    assert decide(replace(snap, head_sha="a" * 40), [], 0, 3) == (MERGE, None)

def test_accept_counts_only_for_a_head_the_issue_had_and_a_sender_who_may_command():
    heads = ["a" * 40, "b" * 40]
    assert accepted([comment("/accept " + "a" * 40, by=BOT)], reviewers, heads) == "a" * 40
    assert accepted([comment("/accept " + "c" * 40, by=BOT)], reviewers, heads) == ""
    assert accepted([comment("/accept " + "a" * 40, by="stranger")], reviewers, heads) == ""
```

  gitops: `reset` moves the branch to an earlier pushed sha and refuses an unknown one. autodev workflow: an
  `/accept` seen while a revision round's job runs drops that round's result (no `publish_round`), then
  `reset_pr`, then `merge_pr` with the accepted sha.
  mia-data workflow:
  - `test_accepting_a_matched_set_under_rework_cancels_it_and_saves_that_set`: stages end `done`;
    `close_snippet_issue` called; the saved crops are the held set's; no `cancelled` entry.
  - `test_accepting_a_pr_heads_set_asks_autodev_to_merge_that_head_and_ignores_newer_heads`.
  - `test_accept_is_taken_after_the_rework_stopped` (stage `blocked`, idle workflow).
  - `test_a_second_accept_while_merging_is_refused_and_nothing_merges_twice` (Review Focus 2).
  - `test_an_accept_whose_crops_are_gone_is_refused_and_nothing_is_saved` (Review Focus 3).
  - `test_a_failed_merge_after_an_accept_rests_at_blocked_and_keeps_the_approval`.
- [ ] **Step 2: Run; they fail**
- [ ] **Step 3: Implement autodev's side, then mia-data's, both behind the marker**
- [ ] **Step 4: Run the files; re-record fixtures; commit twice** (`autodev: /accept resets the pull request
  to an earlier head and merges it`; `datasets: a set that was sent back can be accepted at any time`).

### Task 10: Reject a saved set

**Files:**
- Modify: `mia_data/workflows.py` (`run`, new `_rework_saved`), `mia_data/activities.py` if `revoke_dataset`
  needs the set's round rather than the state's
- Test: `tests/test_mia_workflows.py`

**Interfaces:**
- Produces: `MiaData._rework_saved(event: LabelEvent) -> None`, run under `_guard` from the idle loop when a
  queued reject's `set == state.saved.set_id`:
  1. `revoke_dataset` as `_revoke` calls it; a refusal is a timeline refusal and ends here;
  2. a new round id `i<issue>-e<event id>` and `admit` (pins, budget, pause) as `_round` does;
  3. `state.held = state.saved` with `sent_back_by=event.actor`, `request=f"{set_id}#r"`; `state.saved = None`;
     `state.reworks = 0`;
  4. `_found("Rejected by <actor>: the saved crops are revoked and the developer is asked for a fix.")`;
     `feedback = _feedback(event)` with the request line prefixed;
     `_develop({"what_fails": feedback}, None, binding=held.binding)`.

- [ ] **Step 1: Write the failing tests** (model on `test_revoke_after_done_ends_in_needs_human_and_lists_
  the_blast_radius`): `test_rejecting_a_saved_set_revokes_it_and_starts_the_rework` (activities called in
  the order `revoke_dataset`, `admit`, `open_snippet_issue`; stage `developing`; the issue's draft carries
  the note); `test_a_reject_of_a_set_that_is_no_longer_the_saved_one_is_refused`;
  `test_accepting_the_revoked_set_again_saves_it_and_releases_its_artifacts` (with Task 3's rule).
- [ ] **Step 2: Run; they fail**
- [ ] **Step 3: Implement behind the marker**
- [ ] **Step 4: Run; re-record fixtures; commit** (`datasets: a reject of the saved set revokes it and starts
  the rework`).

### Task 11: The developer sees what the judge saw

**Files:**
- Modify: `mia_data/activities.py` (new `share_evidence`), `mia_data/workflows.py` (`_send_back`,
  `_rework_saved`), `autodev/config.py`, `autodev/activities.py` (`submit_agent`), `autodev/agent.py`
  (instructions), `mia.yaml.example`
- Test: `tests/test_mia_activities.py`, `tests/test_autodev_jobs.py`, `tests/test_autodev_config.py`

**Interfaces:**
- Produces: activity `share_evidence(issue: int, round_id: str, workspace: str, snippet_issue: int) -> None`:
  copies the judge's evidence directory (the PNGs and each crop's placement) to
  `<work_root>/_evidence/issue-<snippet_issue>/<set id with ":" as "-">/`. Called after the snippets issue is
  known. Best effort: a failure is logged and the send-back goes on.
- Produces: autodev config `evidence_dir: Path | None = None`. `submit_agent` mounts
  `<evidence_dir>/issue-<issue>` read-only when that directory exists, and never a path taken from an issue
  or a comment. The agent's instructions name the mount point.

- [ ] **Step 1: Write the failing tests:** the copy lands under the issue's directory and holds no symlink
  that leaves it; `submit_agent`'s request has the read-only mount when the directory exists and none
  otherwise; a config without `evidence_dir` mounts nothing.
- [ ] **Step 2: Run; they fail**
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run; commit** (`autodev: the developer's job reads the judged evidence of its issue`).

### Task 12: The page, the Decisions tab, the docs

**Files:**
- Modify: `mia_buffers/ui/actions.js`, `mia_buffers/ui/subjects.js`, `mia_buffers/ledger.py`
  (`agreement`), `mia_buffers/api.py` (`/decisions`), `AGENTS.md`, `docs/development.md`, `README.md` where
  it describes the review, `mia.yaml.example`
- Test: `tests/test_buffers_ui.py`, `tests/test_buffers_ledger.py`

**Interfaces:**
- Consumes: the subject's `data.sets` (Task 4) and `actions` (Task 5).
- Page: a verdict's request body carries `set` (`actions.js`, beside `revision`). At `review` the buttons
  send `sets.review`. While `sets.held` is set and `approve` is offered, the subject's page shows a panel
  "Sent back by <by>" with the findings as text, the gallery link and **Accept these crops** (sends
  `approve` with `sets.held.set`; help: "Cancel the rework and save this set as it is."). At `done` with
  `sets.saved`, **Reject…** opens the existing reject dialog and sends `sets.saved.set` (help: "Revoke the
  saved crops and send them back with your feedback."). Findings and notes are inserted as text.
- `Ledger.agreement`: a judge's assessment followed by a person's opposite verdict on the same subject counts
  as a disagreement and is listed with `overturned_by` and `overturned_at`.

- [ ] **Step 1: Write the failing tests:** under node, `actions.js` builds a body with `set` for approve and
  reject; the held panel renders for `sets.held` and not without; `agreement` over a judge reject and a
  later human approve reports one overturned decision.
- [ ] **Step 2: Run; they fail**
- [ ] **Step 3: Implement**
- [ ] **Step 4: Check the page in headless Chromium** as the UI section of `docs/development.md` describes:
  a dataset at `developing` with a held set shows the panel and Accept; one at `done` shows Reject.
- [ ] **Step 5: Docs.** `AGENTS.md`: the stage-table rule (Task 5), the rule on a round's wait on autodev
  (the "nothing to change" answer is validated; `_answered` alone only returns a person's reject), a new
  rule "A verdict names its set, and a person outranks the judge". `docs/development.md`: the review section
  and the revocation section. `mia.yaml.example`: `judge.crops: {mode: active, max_reworks: 1}` with one
  comment line, and autodev's `evidence_dir`.
- [ ] **Step 6: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`; all green; commit**
  (`page and docs: accept a set that was sent back, reject a saved one`).

### Task 13: Rollout (the operator's, not built here)

- [ ] Merge to the live checkout with a worker restart (workers load `workflows.py` per run).
- [ ] In the live `mia.yaml`: `judge.crops: {mode: active, max_reworks: 1}` (remove `max_suspect`), autodev's
  `evidence_dir`; run autodev's `init-labels` for the `answered` label.
- [ ] Restart buffer-api, the mia-data worker and the autodev worker.
- [ ] Run one dataset to `done` under the judge; reject it at `done` and watch it revoke and go to
  `developing`; accept the held set and watch it return to `prepared`.
