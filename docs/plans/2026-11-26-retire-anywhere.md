# Retiring a buffer from any state: Implementation Plan

**Goal:** A buffer takes `retire` at every stage but `retired`, also while shelved, and always ends at `retired`
with every version it still had registered released.

**Architecture:** The stage table accepts the action; buffer-api's `_rules` lets it through a hold and the
ledger ends the hold when `retired` is written. The buffer's workflow keeps a person's retire in a queue of its
own (`retire_events`, beside `cancel_events`) and acts on it at rest (main loop), while it serves (`_steady`)
and in a round (`_guard` → `_cancel_round`), each ending in `_retire_all`. The page builds the confirmation
from the subject view.

**Tech stack:** Python (Temporal workflows, sqlite ledger), ES modules with no build step, pytest.

**Spec:** `docs/specs/2026-11-26-retire-anywhere.md`

## Global constraints

- Activity results and `BufferState` are wire format: add, never rename. No new state field is needed.
- Every new workflow branch runs only for a run that took `workflow.patched("retire-anywhere")`; the recorded
  fixtures replay unchanged.
- An activity can run again: `live_buffers` is a read, `retire_buffer` is idempotent.
- Only buffer-api writes a hold. The page holds no token and has no build step.
- No real paths or hostnames in tracked files.

## Review focus

1. A retire that arrives while a new round has not written `planning` yet stays queued and ends the round once
   the stage is cancellable (Task 3: `test_retire_while_sourcing…` sends it at the first activity).
2. A redelivered retire (same event id) neither retires twice nor ends a serving loop (Task 3:
   `test_a_redelivered_retire_is_dropped`).
3. A retire that starts the workflow itself (no run was open, fresh state) releases what the registry holds
   (Task 3: `test_retire_at_rest_releases_what_the_registry_holds`, state `BufferState(7)`, start signal
   `retire`).
4. A queued retire is not lost to a continue-as-new (Task 3: `retire_events` is in every "nothing queued" test
   of the main loop and `_steady`; asserted by the at-rest test running after a rollover-due state).
5. A run from before the patch refuses a person's retire at rest as it did (the existing replay fixtures, and
   `test_replay_buffer_history_retire` for the new path).

---

### Task 1: the stage table, the hold and the ledger

**Files:** `mia_buffers/config.py`, `mia_buffers/api.py` (`_rules`), `mia_buffers/ledger.py` (`append_entry`);
tests `tests/test_buffers_config.py`, `tests/test_buffers_api.py`, `tests/test_buffers_ledger.py`.

**Produces:** `STAGE_ACTIONS` with `retire` at `new`, `planning`, `plan-review`, `sourcing`, `staged`,
`needs-human`, `cancelled` (and `ready`, `idle`), not at `retired`. `_rules` of a held subject gives
`{"unshelve"} | (actions & {"retire"})`. `Ledger.append_entry(kind="buffer", stage="retired")` deletes the
subject's `shelved` row in its transaction and, when there was one, appends the entry
`unshelved: the buffer was retired` under the marker `<!-- mia: unshelved by retire <revision> -->`.

- [ ] Tests first: every buffer stage but `retired` takes `retire`; `working` stages take nothing beyond
  `cancel`, `retire`, `dropped`, `stop`; a shelved buffer at `cancelled` lists `["retire", "unshelve"]` and a
  shelved dataset `["unshelve"]`; a `retired` write on a shelved buffer leaves no hold and one entry, on an
  unshelved one no entry, and a dataset's or another stage's write leaves the hold.
- [ ] Implement, run those three test files' affected tests, commit.

### Task 2: the `live_buffers` activity

**Files:** `mia_buffers/activities.py`; test `tests/test_buffers_activities.py`.

**Produces:** `Activities.live_buffers(issue: int) -> list[str]`, activity name `live_buffers`, registered with
the worker's activities: the ids (sorted) of the registry's buffers whose id is a round id of `issue`
(`ROUND_RE`, `_issue_of`) and whose latest event is not `retired`.

- [ ] Test with a fake registry: two versions of issue 7 (one retired), one of issue 70 → `["i7-e12"]`.
- [ ] Implement, run, commit.

### Task 3: the workflow

**Files:** `mia_buffers/workflows.py`; tests and fixture in `tests/test_buffers_workflows.py`,
`tests/fixtures/buffers/buffer_history_retire.json`.

**Consumes:** `live_buffers` (Task 2), `retire_buffer`.

**Produces:**
- `self.retire_events: list[LabelEvent]`; the `retire` signal puts an event there when it carries one and
  `workflow.patched("retire-anywhere")`, else queues on `lifecycle` as before.
- `_retire_asked() -> LabelEvent | None`: takes the queue, returns the last event not seen before.
- `_retire_all(event: LabelEvent) -> None`: `live_buffers`, plus the state's own buffer when it is not retired
  and not listed; `retire_buffer` each; state `retired = True`, `idle_since = None`, `superseded = []`; stage
  `retired` with `Retired by <actor>: released <ids with archives>.` or `…: nothing was registered to release.`
- Main loop: `retire_events` counts as queued everywhere `cancel_events` does; at rest a retire runs
  `_guard(_retire_all(event))`, at `retired` it is refused (`buffer:retire`, "the buffer is already retired").
- `_steady`: a retire abandons a rebuild (`RETIRED_WHILE`), runs `_retire_all` and ends the loop.
- `_guard`: also wakes for `retire_events` while the stage is in `CANCELLABLE`; `_cancel_round` then ends at
  `retired` through `_retire_all` instead of `cancelled`, and at `needs-human` with a job it could not kill.

- [ ] Mocks: `live_buffers` (answers `Mocks(live=[...])`, default none).
- [ ] Tests: at rest from a fresh state; at `needs-human` with the round's buffer; at `cancelled`; in
  `sourcing` (job killed, locks released, ends `retired`, never `cancelled`); at `staged`; at `ready` with a
  superseded version (both released, `superseded` empty); a job that cannot be killed (`needs-human`, then a
  second retire ends `retired`); at `retired` (refused); a redelivery; a go after it plans again.
- [ ] Implement; run the buffer workflow tests; record `buffer_history_retire.json`
  (`MIA_BUFFERS_RECORD_HISTORY=1 … -k replay_buffer_history_retire`); run every `replay` test; commit.

### Task 4: the page

**Files:** `mia_buffers/ui/actions.js`, `mia_buffers/ui/guide.html`; test `tests/test_buffers_ui.py`.

**Produces:** exported `retireConfirm(subject) -> string` with the text of spec §3.5, used as
`ACTION_CONFIRM.retire`; the tooltip `ACTION_HELP.buffer.retire`; the guide's Retire row.

- [ ] Test under node: the text for `sourcing`, `staged`, `plan-review`, `ready` (names `serving`), `cancelled`,
  and a shelved subject (says it comes off the shelf).
- [ ] Implement, `pixi run fmt`, run the UI tests that cover it, commit.

### Task 5: docs and the spec's status

**Files:** `AGENTS.md`, `docs/development.md`, `README.md`, `docs/specs/2026-11-26-retire-anywhere.md`.

- [ ] Rules and guide say what the code does now; the spec becomes v0.2 with what the build settled.
- [ ] `pixi run lint`, `pixi run typecheck`, the full suite once (workflow code changed), commit.
