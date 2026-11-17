# Buffer flow, slices 1 and 2: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The page shows a buffer as six steps over today's stages, a preparation belongs to the buffer that asked
for it, and a saved buffer's definition can be edited while no round is open and it does not serve.

**Architecture:** No workflow changes. buffer-api gains one action it settles itself (`edit`), a `buffer` owner on
a preparation request, and two read conveniences. On the page the designer becomes steps 1 and 2 of a new buffer
view (`flow.js`), which replaces the spec's page; steps 3 to 6 are today's actions and tables arranged by stage.

**Tech stack:** Python 3 (pydantic, the ledger's SQLite), plain ES modules with no build step, pytest with node
for the page's modules.

**Spec:** [`docs/specs/2026-11-17-buffer-flow.md`](../specs/2026-11-17-buffer-flow.md) v0.4, §7 slices 1 and 2.
The mockup beside it is the picture of every step.

## Global constraints

- `pixi run fmt`, `lint`, `typecheck` clean; a task runs only the tests that cover it (`pytest tests/<file> -k ...`).
- No workflow file changes (`mia_buffers/workflows.py`, `activities.py`): no replay fixture is touched.
- The page holds no token: a new call is a read route or an action route (`tests/test_buffers_ui.py` enforces it).
- No build step; a module imports only modules above it in the table of `docs/development.md`, never in a circle;
  importing a module touches no DOM beyond what `dom.js` does.
- What a job or a record wrote is inserted as text, never parsed as HTML.
- Stage tables first: a new action is in `STAGE_ACTIONS` before any code offers it.
- No real paths or hostnames in tracked files. Comments and docs say what the code does, not what it replaced.
- Words on the page: *selected* (steps 1 to 3), *in this version* (4 to 6), *on disk* (5, 6). Never "in the buffer",
  never "spec" for the thing a person opens.

## Decisions this plan makes

- A saved buffer's address is `#buffer/<key>` and `#buffer/<key>/<step>`; an unsaved one is `#buffers/new` and
  `#buffers/new/datasets`. The key is what every list already holds; slice 4 moves addresses to the number with
  the action routes. `#specs/<key>` redirects to `#buffer/<key>`.
- Until slice 3 (`unserve`), a changed definition is refused only at `ready` and `idle`; `go`, `revoke` and
  `retire` there stay as they are and sit in *More* or on step 6.
- The Buffers tab lists buffers only. A version's page (`#buffers/<id>`) stays and is reached from step 6.
- List columns: name, step, status, serving id, changed. Datasets and train crops per row are left out: they
  need a plan per row.
- Step 4 shows the plan as the round wrote it to the timeline; the staged size against the disk is slice 3.

## Review focus

1. The stream redraws the buffer view while a person edits the definition: the edit is kept (task 6).
2. YAML that does not parse: the controls keep their values, the way forward is disabled, nothing is sent (task 6).
3. YAML that holds what no control shows: the controls are read-only and say what they cannot show (task 6).
4. Two tabs: an `edit` or a `go` with a stale `revision` is refused and the typed definition stays (tasks 2, 6).
5. A hash that names a step the stage has not reached opens the stage's step (tasks 4, 5).

---

### Task 1: a preparation request knows its buffer

**Files:** `mia_buffers/api.py` (`SpecBody`, `_file_prep`, `_list_subjects`), `tests/test_buffers_api.py`.

**Produces:** `POST /prep` body field `buffer: int | None`; `GET /subjects/prep?buffer=<number>`.

- [ ] Tests: `test_a_request_is_filed_for_a_buffer_and_listed_under_it` (a request posted with `buffer` = a buffer
  subject's number has `data["buffer"]` and is the only row of `/subjects/prep?buffer=<n>`; one posted without is
  not in it); `test_a_request_names_only_a_buffer_that_exists` (unknown number: 422, no subject created).
- [ ] Implement: `SpecBody.buffer`; `_file_prep` checks it with `_numbered("buffer", ...)` (as 422) and stores
  `{"spec": text, "buffer": n}`; `_list_subjects` filters `kind == "prep"` rows by `data.get("buffer")`.
- [ ] Run the two tests; commit.

### Task 2: `edit`, and a definition that changes until it serves

**Files:** `mia_buffers/config.py` (`STAGE_ACTIONS`), `mia_buffers/api.py` (`ActionBody`, `_buffer_action`, new
`_edit`, `_subject_view`, `_list_subjects`), `mia_buffers/ledger.py` (`spec_has_buffer` goes if unused),
`tests/test_buffers_api.py`.

**Produces:** action `edit` with `spec` and optional `revision`, answered 202 with the settled action; subject
views of kind `buffer` without `frozen`; listed buffer subjects carry `serving: <version id> | None`.

- [ ] Tests: `test_an_edit_stores_the_text_and_the_name_and_says_so_in_the_timeline`;
  `test_an_edit_is_refused_while_a_round_is_open_or_the_buffer_serves` (each of `planning`, `plan-review`,
  `sourcing`, `staged`, `ready`, `idle`: 409, text unchanged); `test_an_edit_that_is_not_a_spec_or_is_stale_changes_nothing`
  (422 for bad text, 409 for an old `revision`); `test_a_go_takes_a_changed_text_after_a_buffer_was_built_unless_it_serves`
  (a subject with a buffer row at `retired`: accepted and stored; at `ready`: 409);
  `test_the_buffers_list_names_the_version_being_served`.
- [ ] `STAGE_ACTIONS`: add `edit` to `new`, `retired`, `needs-human`, `cancelled`.
- [ ] `_edit(subject, parsed, actor)`: `_accepts`; 422 without `spec` or when `_spec_name` refuses it; `add_action`
  with the text as payload; `set_subject_data`; `settle_action(id, "sent", entry=_action_entry(action))`;
  `_changed`. No dispatcher, as `_hold`.
- [ ] `go`: replace the `spec_has_buffer` refusal with one for `stage in ("ready", "idle")`.
- [ ] `_list_subjects` (kind `buffer`): one `ledger.buffers()` read; `serving` is the newest buffer `i<number>-e*`
  whose latest event is in `SERVING`.
- [ ] Run the tests and `-k "spec or action"` of the file; commit.

### Task 3: the plan's rows that are left to prepare

**Files:** `mia_buffers/explore.py` (`plan_view`), `mia_buffers/api.py` (the plan body's `page`), `tests/` of
`plan_view`.

**Produces:** `page.to_prepare: bool` on `POST /plan`: only rows that fit and are not `prepared`.

- [ ] Test: `test_the_plan_can_list_only_what_is_left_to_prepare` (rows prepared, in progress, unprepared, not
  fitting: the page holds the middle two, `total` is 2).
- [ ] Implement beside `prepared_only`; run; commit.

### Task 4: steps and exclusions, without the DOM

**Files:** create `mia_buffers/ui/steps.js`; modify `mia_buffers/ui/spec.js`; `tests/test_buffers_ui.py`.

**Produces:**
- `steps.js`: `STEPS = ["define","datasets","prepare","plan","source","serve"]`, `STEP_TITLES`,
  `stepOf(stage) -> step` (`planning`, `plan-review` → `plan`; `sourcing`, `staged` → `source`; `ready`, `idle` →
  `serve`; anything else → `prepare`), `stepToShow(stage, asked) -> step` (the asked step when it is not after
  `stepOf(stage)`, else `stepOf(stage)`), `statusWord(subject) -> string`, `editable(subject) -> bool`
  (`subject.actions.includes("edit")`).
- `spec.js`: the controls' `exclude` (a Set of ids) written as `select.exclude.ids` and read back by `fromSpec`.

- [ ] Tests (node): `test_a_stage_opens_at_its_step` (every stage of `STAGE_ACTIONS` → the step above; a hash asking
  for `serve` at `plan-review` gets `plan`, asking for `define` gets `define`);
  `test_an_unticked_dataset_is_written_as_an_exclusion_and_read_back` (through `load_spec_text`: `select.exclude.ids`
  equals the set; `fromSpec` of the parsed text gives the same set; a spec with `exclude.reasons` shows in
  `specDiff`). Extend `filled`/`listed` of the two existing spec tests with `exclude`.
- [ ] Implement; run; commit.

### Task 5: the buffer view's shell

**Files:** create `mia_buffers/ui/flow.js`; modify `index.html`, `app.css`, `app.js`, `dom.js` (`SUBJECT_HASH`,
`SUBJECT_VIEWS`, `SUBJECT_TAB`), `buffers.js` (`viewBuffers` list), `subjects.js` (export `timelineCard`,
`shelvedNotice`; `specControls` and the designer import go), `actions.js` (`onScreen.redraw`), `operations.js`,
`datasets.js` (a link to the requests), `mia_buffers/mirror.py` (the link), `tests/test_buffers_ui.py`.

**Produces:** `viewFlow(key, step, refresh)` exported by `flow.js`; `[data-view="buffer"]` with `#bufferHead`,
`#bufferSteps`, `#bufferNotes`, one `[data-step]` pane per step, `#bufferFoot`, `#bufferTimeline`.

- [ ] Tests (rules over the source): no Preparation tab and a link to `#preparation` on the Datasets view; the
  Buffers view has one table (`pageOf("/subjects/buffer"`) and a `#buffers/new` button; `#specs/` only in the
  redirect; the strip marks steps by `stepToShow`; the heading's *More* is `actionBar` with the step's own actions
  skipped; the mirror links `#buffer/`. Update the tests that named the spec page.
- [ ] Implement the shell: heading (name, key, stage pill, links, *More*), strip, notices (shelved, undelivered,
  a round the registry calls working whose workflow is not running), the step's pane and footer, the timeline.
  Steps 3 to 6 render a placeholder line until their tasks.
- [ ] Routing: `#buffers/new[/datasets]`, `#buffer/<key>[/<step>]`, `#specs/<key>` → `location.replace`.
- [ ] Run the page tests; commit.

### Task 6: steps 1 and 2

**Files:** `mia_buffers/ui/designer.js`, `index.html`, `app.css`, `flow.js`, `tests/test_buffers_ui.py`.

**Produces (designer.js):** `definition() -> string`, `changed() -> bool`, `openDefinition(text | null, preset?)`
(async; null starts a new one from the defaults or `preset`), `setLocked(bool)`, `problem() -> string | null`
(why the way forward is closed), `planSummary() -> object | null`, `showDefine()`, `showDatasets()`,
`specFromDatasets(ids)`.

- [ ] Tests, as rules over the source: the YAML box feeds `POST /specs/parse`; a control change clears the kept text; a parse failure
  sets the problem and sends nothing; with something lost the controls' fieldset is disabled; the stream's
  redraw returns early while `changed()`; Save is `POST /specs` with no action; a changed saved buffer sends
  `edit` with `revision` before it leaves steps 1 and 2. `count("plan_first: true") == 1`.
- [ ] Implement: the pane with two tabs; the stat column (`Datasets that fit`, `Prepared`) beside the two cards;
  the other tiles, the funnel, the by-modality chart and the table on step 2 with the *Selected* tick; footers.
- [ ] Run the page tests; commit.

### Task 7: step 3, Prepare

**Files:** `flow.js`, `actions.js` (`prepareButton` takes the owning buffer and stays on the page),
`tests/test_buffers_ui.py`.

- [ ] Tests: the request posted from step 3 carries `buffer: subject.number` and does not change the hash; the
  step lists `/subjects/prep` with `buffer`; the table asks `POST /plan` with `to_prepare`; Continue sends
  `go` with `plan_first` and `revision` and is disabled with no prepared dataset.
- [ ] Implement; run; commit.

### Task 8: steps 4 to 6, and back is a cancel

**Files:** `flow.js`, `buffers.js` (export what step 5 and 6 reuse), `tests/test_buffers_ui.py`.

- [ ] Tests: Run is `approve` with the host check and `revision`; *Approve and serve* is offered at `staged` only;
  back from steps 4 and 5 confirms and sends `cancel`; step 6 shows the id, the `BufferDataset` line with
  `buffer=<id>` and the versions table; `dropped` is on step 5.
- [ ] Implement; run; commit.

### Task 9: the documents, the four checks, and the page in a browser

**Files:** `docs/development.md` (the page, the action routes), `AGENTS.md` (the page's rules that name the spec
page), `mia_buffers/ui/guide.html`, `architecture.html` where they describe the buffer pages.

- [ ] Update the documents; `test_the_docs_describe_the_system_as_it_is` passes.
- [ ] `pixi run fmt && pixi run lint && pixi run typecheck`; `pytest tests/test_buffers_ui.py tests/test_buffers_api.py`.
- [ ] Drive the page in headless Chromium against a service with a small ledger: new buffer through Save, an
  edit, Prepare, and a saved buffer at each stage opening at its step; no page error.
- [ ] Commit.
