# Webapp actions, slice D: buffers and requests from the page — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a buffer spec and a preparation request are created, run, steered, cancelled and stopped from the
page; a request starts its datasets through the registry, with no label and no issue in the datasets repo.

**Architecture:** the action machinery of slice C (`_act_on`, the stage tables, the gateway) gets the buffer and
request routes; `MiaBuffer` and `MiaPrep` get `cancel`; the buffers activities read the action as mia-data's do;
the nudge becomes a dataset `go` sent by the worker; the mirror opens buffer and request issues; the page gets
*Create buffer*, a spec page, a request page, *Prepare selected* and *Needs you*.

**Spec:** `docs/specs/2026-11-11-webapp-actions.md` (v0.5): §4.2, §4.4, §5 (`cancel`, `stop`, `dropped`), §5.1
(buffers, requests), §6 (table), §7, §8.1, §8.2, §14 row D.

## Global Constraints

- As slices A to C. Labels and the buffers poller still work until slice E.
- `LabelEvent`, `BufferState`, `PrepState` are not changed; new workflow state lives on the workflow object.
- Cancel must replay every recorded buffer and request fixture unchanged; one new fixture per workflow.
- A cancel acts only where the stage table has a round in progress or a gate (`planning`, `plan-review`,
  `sourcing`, `staged`; `requested`): a `ready` or `idle` buffer keeps serving and the cancel is refused.

## Review Focus

1. A cancel while several fetch and stage jobs of one buffer run: every job is killed, every lock released,
   every reservation settled, no dataset task outlives the round.
2. A cancel that reaches a buffer serving (`ready`): refused, the buffer keeps serving.
3. A request cancelled while its datasets run: only the dataset rounds this request started are cancelled.
4. *Files are in place* clicked while a copy is still writing: refused, with the reason.
5. A spec posted under a name that exists: an edit of that subject, never a second subject.
6. A nudge retried by Temporal: one `go` (the key `<round id>/<record>`).
7. `stop` on a run frozen by nondeterminism: the terminate reaches it; the subject ends `retired` or `done`.

## Tasks

### Task 1: stage tables and routes

`mia_buffers/config.py`, `mia_buffers/api.py`, `mia_buffers/ledger.py`, `mia_buffers/dispatch.py`; tests in
`tests/test_buffers_config.py`, `tests/test_buffers_api.py`, `tests/test_buffers_dispatch.py`.

- `cancelled` in `STAGES` and `PREP_STAGES`; the tables of spec §5.1 with `cancel`.
- `ActionBody` gains `spec`, `plan_first`, `records`, `record`.
- `POST /specs` `{spec, action?, plan_first?, key?}`: `load_spec_text`; the subject keyed by the spec's name is
  created, or its stored text replaced; with `action: go` it is run (`plan_first` sends the `plan` signal). The
  go's payload carries the spec text; its timeline entry shows it.
- `POST /specs/<name>/actions`: `go` (the stored spec, or a `spec` in the body, which is stored first),
  `approve`, `keep`, `retire`, `revoke` (`records`, required), `dropped` (`record`), `cancel`, `stop`.
- `POST /prep` `{spec, key?}`: `load_prep_text`; a new subject `prep-<number>` holding the text; `go`.
  `POST /prep/<number>/actions`: `go`, `cancel`, `stop`.
- `dropped` is a plain signal `dropped(record)` to the running workflow; refused (409) when
  `<raw_root>/<record>/incoming/` is empty or holds a file modified in the last 60 seconds.
- `stop`: `Dispatcher.terminate(kind, number, reason) -> bool`; then, for a buffer, every buffer of the subject
  not yet retired is retired with its `retired` event; the stage becomes `retired` (buffer) or `done` (request),
  with an entry saying what was done and what was left running (cluster jobs; a request's dataset rounds).
- `GET /needs` also answers `failed`: the actions not delivered in the last day.
- `Ledger.create_subject(kind, key=None)`: a request's key is made from its number.

### Task 2: cancel in `MiaBuffer` and `MiaPrep`

`mia_buffers/workflows.py`, `mia_buffers/activities.py` (`kill_job`, `prep_cancel`), tests in
`tests/test_buffers_workflows.py`, fixtures `buffer_history_cancel.json`, `prep_history_cancel.json`.

- `MiaBuffer`: `cancel` signal; `_guard` runs its step as a task and loops: a cancel at a cancellable stage
  cancels the step, then the dataset tasks of the sourcing pass, kills every job being waited on, retires the
  round's buffer, and finishes `cancelled`; at any other stage the cancel is refused and the step goes on.
  `_await_job` records the job ids in flight; the sourcing passes record their dataset tasks. A `cancelled`
  round's buffer counts as over for the next go.
- `MiaPrep`: `cancel` signal; the round runs as a task; a cancel stops it, runs `prep_cancel(issue, records)`
  (a dataset `cancel` for each record whose latest `go` this request sent and which is still in a round), and
  ends `cancelled`.

### Task 3: buffers activities read the action; the nudge goes through the registry

`mia_buffers/activities.py`; tests in `tests/test_buffers_activities.py`.

- `admit`, `prep_admit`: the spec is `payload.spec` of the action whose id is the event id; the issue's fence
  when the event is a label's.
- `revoke_targets`: `payload.records` of the subject's revoke action whose `at` is `until`; the comments else.
- `refuse_label`: an action is marked refused.
- `nudge_dataset`, `prep_nudge`: read the dataset's row and subject; when it needs a round, send a dataset `go`
  with the token, actor `buffer:<number>` or `prep:<number>`, the round's root, and the key
  `<round id>/<record>`. No issue is searched, opened or labelled. A dataset in a round is left alone.

### Task 4: the mirror opens buffer and request issues

`mia_buffers/mirror.py` (new: `buffer_issue`, `prep_issue`), `mia_buffers/cli.py`; tests in
`tests/test_buffers_mirror.py`.

### Task 5: the page

`mia_buffers/ui/index.html`, `tests/test_buffers_ui.py`; the browser smoke script.

- Buffers view: *Create buffer* (the spec YAML in an editable box, *Run*, *Show me the plan first*); a spec's
  page `#specs/<name>`: buttons by stage, *Edit and run again*, *Revoke* with the pool's records to tick,
  *Files are in place* per dataset waiting for a drop, its buffers newest first.
- Preparation view: *Prepare selected* from the datasets table; a request's page `#requests/<key>`.
- *Needs you*: the first view, from `GET /needs`, each row with its buttons.

### Task 6: whole-slice check and docs
