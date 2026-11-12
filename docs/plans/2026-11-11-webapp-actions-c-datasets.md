# Webapp actions, slice C: datasets from the page — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a person starts, approves, rejects, revokes and cancels a dataset round from the page; a click reaches
the workflow within the request; the page follows the round live.

**Architecture:** an append-only `actions` table whose ids are the workflow's event ids; one `action` capability
with a JSON and same-origin guard; the route checks the stage table and the revision, records the action, sends
it through the Temporal gateway and appends it to the timeline. `MiaDataDataset` gets a `cancel` signal that ends
the running round. `admit`, `read_feedback` and `refuse_label` read the action when the event is one. The mirror
opens an issue for a dataset that has none. The page gets a dataset view, live updates and a health banner.

**Spec:** `docs/specs/2026-11-11-webapp-actions.md` (v0.5): §4.1 (numbers), §4.2, §4.4, §5, §5.1 (datasets),
§6, §7 step 1, §8.1 (datasets), §8.3, §8.4, §10, §14 row C.

## Global Constraints

- As slices A and B. Labels still work: an event id the registry does not know is a label event and the old
  source applies (§6).
- Action routes take no token, JSON only, and refuse a foreign `Origin` (§10). A named actor needs the worker
  token.
- `LabelEvent` gets no new field. `DatasetState` is not changed.
- The `cancel` signal and the task that makes a round cancellable must replay every recorded fixture unchanged;
  a new fixture records a cancelled round.
- `tests/test_buffers_ui.py` holds: every route the page calls exists and is `read`, `consumer` or `action`; the
  page holds no token.

## Review Focus

1. The same key sent twice (a double click, a retry after a lost answer): one action, one start.
2. An approve carrying the revision of a set that has since been replaced: 409, nothing recorded or sent.
3. Temporal away: the action is `failed`, the answer 503, and a retry with the same key sends the same id.
4. A cancel for a dataset whose workflow is not running (terminated by hand): the subject must not stay
   "working" forever; the route sets it `cancelled` itself.
5. A ledger restored from a backup: no number and no action id is issued twice.
6. A `go` for a record that has a GitHub issue the registry never adopted: its number is that issue's, not a
   new one (one workflow per dataset).

## Tasks

### Task 1: ledger — actions, clock-derived ids, new subjects

`mia_buffers/ledger.py`; tests in `tests/test_buffers_ledger.py`.

- Table `actions (id INTEGER PRIMARY KEY, kind, number, action, actor, at TEXT, key TEXT UNIQUE, payload TEXT,
  state TEXT, detail TEXT)`.
- `_issue(db, name, clock) -> int`: `max(clock, last + 1)`, the last one kept in `kv`. A subject number is the
  whole seconds since 2026-01-01T00:00:00Z; an action id the milliseconds since the epoch.
- `create_subject(kind, key, *, data=None, number=None) -> dict`: a new subject (stage `new`, no issue, a number
  from the clock unless one is given, in which case it is also the issue); the existing one when the key is taken.
- `set_subject_data(kind, number, data)`, `set_subject_issue(kind, number, issue)`.
- `add_action(kind, number, action, actor, *, key=None, payload=None) -> tuple[dict, bool]`: (row, created); a
  key seen before returns that row (ValueError when it names another subject or action).
- `settle_action(action_id, state, detail="", *, entry=None)`: set the state; with `entry` (body, marker) append
  it to the subject's timeline in the same transaction.
- `action(action_id)`, `actions(kind, number)`.

### Task 2: the gateway sends actions

`mia_buffers/dispatch.py`; tests in `tests/test_buffers_dispatch.py`.

- `Target(namespace, task_queue, workflow, workflow_id, make_state)` per kind.
- `send(kind, number, signal, event) -> None`: start-with-signal, ten-second limit, `DispatchError` on any
  failure. `signal_running(kind, number, signal, event) -> bool`: a plain signal; False when no run is open.
  `run_status(kind, number) -> str | None`. `health() -> dict` (`{temporal: bool, queues: {name: bool}}`).

### Task 3: the dataset action route

`mia_buffers/api.py`, `mia_core/registry.py`; tests in `tests/test_buffers_api.py`.

- Capability `action`: `_guard_action(headers)` (JSON content type; `Origin`, when sent, is this service's).
  `actor_of(headers, body)` is `page`, or the body's `actor` when the worker token is sent.
- `POST /datasets/<record>/actions` body `{action, key?, revision?, note?, root?, actor?}`; steps 1–6 of §5.
  A first `go` creates the subject: the record must be in the newest synced catalog; the number is the dataset
  row's `issue` when it has one, else new.
- `cancel`: a plain signal; when no run is open the route writes stage `cancelled` and an entry saying so.
- `GET /actions/<id>` (read), `PUT /actions/<id>` (worker; `{state: refused, detail}`), `GET /timeline/...`
  answers `actions` too, `GET /health`, `GET /run/<kind>/<number>`, `PUT /subjects/<kind>/<number>/issue` (worker).
- Registry client: `action`, `refuse_action`, `set_subject_issue`, `send_action(kind, key, action, **fields)`.

### Task 4: cancel in the dataset workflow

`mia_data/config.py` (`cancelled` in `STAGES` and `STAGE_ACTIONS`, `cancel` in the working stages and review),
`mia_data/workflows.py`, `mia_data/activities.py` (`kill_job`, `dataset_fields`), `tests/test_mia_workflows.py`,
fixture `dataset_history_cancel.json`.

- `cancel(event)` signal. `_guard` runs its step as a task and waits for the step or a cancel; on a cancel it
  cancels the task, kills the round's job by id (`kill_job`), and finishes in `cancelled`. A cancel with no round
  running is refused through `refuse_label`.
- `dataset_fields`: a cancelled dataset that is not prepared reads `needs-human` to whatever follows it.

### Task 5: activities read the action

`mia_data/activities.py`; tests in `tests/test_mia_activities.py`.

- `admit`: when the event is an action, the record is the subject's key and the root the action's
  `payload.root` (default `cfg.root_of(number)`); no issue is read.
- `read_feedback`: the notes of the subject's reject actions with `since < at <= until`, neutralized and capped
  as today; the comments as before when there is none.
- `refuse_label`: an action is marked refused; a label is removed as before.
- `open_snippet_issue`: the body links the dataset's page.

### Task 6: the mirror opens dataset issues

`mia_data/mirror.py`, `mia_data/cli.py`; tests in `tests/test_mia_mirror.py`.

- A dataset subject without an issue: find one by the record marker, else create it (title and facts as
  `seed.render_issue`, a first line after the marker saying it is a read-only log with the page link), write the
  number back. The record comes from the catalog repo at `main`.

### Task 7: the page

`mia_buffers/ui/index.html`, `tests/test_buffers_ui.py`.

- `#datasets/<record>`: status word, stage, run status, the actions the stage allows as buttons (reject with a
  note box; approve and reject send the revision; a 409 reloads and says so), the timeline as preformatted
  text, undelivered actions with *Retry*, a link to the issue and to the round's gallery.
- `EventSource("/stream")`: refetch what an event names; `/health` on load and reconnect, a banner when a queue
  has no worker; the lookup box in the top bar.

### Task 8: whole-slice check and docs

`docs/development.md`, `AGENTS.md` (the page is no longer read-only; action routes).
