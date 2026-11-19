# Overview and the status "needs human" (slices 1 and 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A subject has one status, computed by buffer-api, that alone says a human is needed; the repair
agent's cases only record repairs; one Overview page replaces the tabs "Needs you" and "Cleaner".

**Architecture:** The stage tables hold a phase per stage. buffer-api puts `phase`, `status` and `repair` on
every subject view; workers decide by the phase and the hold, the page shows the status. A case ends
`gave-up` and records the subject's revision. One read route lists subjects of every kind by status. The page
gets a module `overview.js` whose sections are native `<details>` folds.

**Tech Stack:** Python 3 (stdlib `sqlite3`, pydantic), ES modules without a build step, pytest with node for
the page's modules.

**Spec:** `docs/specs/2026-11-19-overview-and-needs-human.md` (v0.3). This plan builds its slices 1 and 2
(§8): §3.1, §3.2 without the judge's look, §4, §5, and the texts of §6. Slices 3 to 6 are not in this plan.

## Global Constraints

- The stage `needs-human`, the dataset row's state `needs-human` and a version's event `needs-human` keep
  their names in this plan (slices 3 and 4 rename them).
- The identifiers `cleaner`, `MiaCleaner`, `/cleaner`, `cfg.cleaner` and the actor id `cleaner` stay (slice 6).
  Every text a person reads says "repair agent".
- The six status words, exactly: `shelved`, `in repair`, `needs human`, `done`, `not started`, `working`.
- The five phase words, exactly: `not started`, `working`, `waiting`, `stopped`, `done`.
- The case states, exactly: `open`, `working`, `waiting`, `resolved`, `dropped`, `gave-up`.
- No worker decision reads `status`. Each reads `standing(view)`: the phase, or `shelved` for a held subject.
- Workflow code does not change: no replay fixture is re-recorded.
- The page has no build step, a module imports only modules below it, and nothing a job or record wrote is
  parsed as HTML. The addresses `#needs` and `#cleaner` are not mapped.
- Comments and docs say what the code does, never what it replaced. No real paths or hostnames:
  `git diff judge-decides | grep -n 'rokickik\|/nrs/'` prints only the plan's own quotation of this check.
- Scripts and tests in this worktree run with `PYTHONPATH=$PWD`.

## Review Focus

1. A subject that is shelved while a case is in work: it reads `shelved`, the sweep leaves it alone, and no
   case is opened on it (`standing` must give `shelved`, not the phase).
2. A `gave-up` case from before this change (no `ended_revision`) on a subject that has since moved: it must
   not make a `done` dataset read `needs human`.
3. The repair agent in `report` mode: an `open` case makes nothing `in repair`.
4. A buffer whose round is working and whose dataset waits for files: still listed under Needs human with its
   button, although its status is `working`.
5. A left-behind thing the agent gave up on, in a sweep whose pass for that kind did not run (a worker on old
   code, the snippets service away): its case must not be resolved as gone.

---

### Task 1: The phase, on the view and in every worker decision

**Files:**
- Modify: `mia_data/config.py`, `mia_buffers/config.py` (the word `stuck` in both `STAGE_ACTIONS` tables and
  `PREP_STAGE_ACTIONS` becomes `stopped`; comments say "phase")
- Modify: `mia_buffers/api.py` (`_rules`, `_subject_view`, `SHELVABLE`)
- Modify: `mia_buffers/cleaner.py` (new `standing`; `sighting`, the left-over rule)
- Modify: `mia_buffers/sweep.py`, `mia_buffers/activities.py` (every `view["status"]`, `subject["status"]`)
- Modify: `mia_buffers/ui/flow.js`, `mia_buffers/ui/subjects.js`, `mia_buffers/ui/buffers.js` (`status ===
  "working"` becomes `phase === "working"`)
- Test: `tests/test_buffers_api.py`, `tests/test_buffers_cleaner.py`, `tests/test_buffers_sweep.py`,
  `tests/test_mia_config.py`, `tests/test_buffers_config.py`, `tests/test_buffers_ui.py`

**Interfaces:**
- Produces: `mia_buffers.cleaner.standing(view: dict) -> str`: `"shelved"` when `view.get("shelved")`, else
  `view["phase"]`.
- Produces: `BufferService._rules(row, hold, gaps) -> tuple[str, frozenset[str]]` returns `(phase, actions)`;
  the phase is the table's also for a held subject, whose actions are `{"unshelve"}`.
- Produces: every subject view has `"phase"`. In this task `"status"` is still `"shelved"` for a held subject
  and the phase otherwise (Task 3 replaces it).

- [ ] **Step 1: Write the failing tests**

`tests/test_buffers_cleaner.py`:
```python
def test_a_decision_reads_the_phase_and_a_held_subject_stands_shelved():
    assert standing({"phase": "stopped", "shelved": None}) == "stopped"
    assert standing({"phase": "stopped", "shelved": {"by": "ann", "at": 1.0, "reason": "later"}}) == "shelved"
```
`tests/test_buffers_api.py`, beside the shelving tests: a held dataset at `failed` has
`(view["phase"], view["status"], view["actions"]) == ("stopped", "shelved", ["unshelve"])`; an unheld one has
phase `stopped`; a dataset at `review` has phase `waiting`.

`tests/test_mia_config.py` / `tests/test_buffers_config.py`: the set of phases over each table is a subset of
`{"not started", "working", "waiting", "stopped", "done"}`.

- [ ] **Step 2: Run them and see them fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_cleaner.py tests/test_buffers_api.py tests/test_mia_config.py tests/test_buffers_config.py -q -x`
Expected: FAIL (`standing` is not defined; no `phase` on a view).

- [ ] **Step 3: Implement**

Tables: `stuck` to `stopped`. `_rules` returns the phase. `_subject_view` adds `"phase"`. `SHELVABLE =
("not started", "stopped", "done")`. In `cleaner.py`, `sweep.py` and `activities.py` every read of a view's
`status` becomes `standing(view)` and every `"stuck"` becomes `"stopped"`. The page's three "is a round in
progress" checks read `phase`.

- [ ] **Step 4: Run the covering tests**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_cleaner.py tests/test_buffers_sweep.py tests/test_buffers_api.py tests/test_buffers_activities.py tests/test_mia_config.py tests/test_buffers_config.py tests/test_buffers_ui.py -q`
Expected: PASS, after the fixtures that build a view by hand carry `phase` and `shelved`.

- [ ] **Step 5: Commit** (`status: the stage tables hold a phase, and every worker decision reads it`)

### Task 2: A case ends `gave-up` and says where its subject stood

**Files:**
- Modify: `mia_buffers/ledger.py` (after the schema: stored `needs-person` becomes `gave-up`)
- Modify: `mia_buffers/api.py` (`CASE_STATES`, `CASES_OPEN`, `CaseState`)
- Modify: `mia_buffers/sweep.py` (`gave-up`; `_save` records `ended_revision`; left-behind cases; the entry text)
- Modify: `mia_buffers/ui/operations.js` (`CASE_STATUS`, `caseRows`)
- Test: `tests/test_buffers_ledger.py`, `tests/test_buffers_api.py`, `tests/test_buffers_sweep.py`,
  `tests/test_buffers_ui.py`

**Interfaces:**
- Produces: case state `gave-up`; `case["data"]["ended_revision"]: int` on a `gave-up` case about a subject:
  the subject's revision read after the agent's last write.
- Produces: `Sweep.ran: set[str]`: the kinds of left-behind things (`run`, `job`, `snippets`) whose pass ran in
  this sweep.

- [ ] **Step 1: Write the failing tests**

`tests/test_buffers_ledger.py`:
```python
def test_a_case_stored_as_needs_person_is_read_as_gave_up(tmp_path):
    ledger = Ledger(tmp_path / "l.db")
    case = ledger.open_case("dataset", 7, "rec-a", "diagnosis", "open", {})
    ledger._db.execute("UPDATE cases SET state = 'needs-person' WHERE id = ?", (case["id"],))
    ledger._db.commit()
    again = Ledger(tmp_path / "l.db")
    assert [row["state"] for row in again.cases()] == ["gave-up"]
```
`tests/test_buffers_api.py` (`test_case_routes`): a PUT with `state: "needs-person"` is 422, `gave-up` is 200,
and `GET /cases` lists a `gave-up` case of any age.

`tests/test_buffers_sweep.py`:
- `test_a_case_that_gives_up_records_the_revision_its_subject_stands_at`: a case whose plan is one `needs`
  step ends `gave-up` with `data["ended_revision"]` equal to the subject's revision after the agent's entry.
- `test_a_left_behind_thing_the_agent_gave_up_on_is_resolved_once_it_is_gone`: a `gave-up` case of kind `job`
  whose job is not seen in a sweep that looked for jobs becomes `resolved` with the note `it is gone`.
- `test_a_gave_up_left_behind_case_stays_when_its_pass_did_not_run`: the same case of kind `run`, in a sweep
  with a `restart` finding, stays `gave-up`.

`tests/test_buffers_ui.py` (the case rows test): state `gave-up` gives status `gave up` and keeps `needs`.

- [ ] **Step 2: Run them and see them fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_ledger.py tests/test_buffers_api.py tests/test_buffers_sweep.py tests/test_buffers_ui.py -q -x`
Expected: FAIL.

- [ ] **Step 3: Implement**

- Ledger: one idempotent `UPDATE cases SET state = 'gave-up' WHERE state = 'needs-person'` run with the schema.
- Sweep: `needs-person` to `gave-up` throughout. `_save`: for `gave-up` and a case about a subject, read the
  subject (`registry.subject(kind, key)`) and set `data["ended_revision"]`. Each left-behind pass adds its
  kind to `self.ran` when it runs to its end; after the record-gap cases, every `gave-up` case of a kind in
  `self.ran` whose `(kind, key)` is not in `self.present` is saved `resolved`, note `it is gone`.
- The entry the agent writes when it gives up: `could not repair this: <needs>`. The refusal in
  `_request_may_go`: ``its dataset `<key>` could not be repaired: <needs>``.
- Page: `"gave-up": ["gave up", "warn", "The repair agent could not repair it; the case says what it takes."]`.

- [ ] **Step 4: Run the covering tests**

Run: the command of Step 2 without `-x`. Expected: PASS.

- [ ] **Step 5: Commit** (`cases: a repair that cannot go on ends gave-up, with the revision its subject stood at`)

### Task 3: The status

**Files:**
- Modify: `mia_buffers/api.py` (new `status_of`, `BufferService._repairs`; `_subject_view`; `_needs` drops its
  `cleaner` note; `NEEDS_A_PERSON` goes)
- Modify: `mia_buffers/ui/steps.js`, `mia_buffers/ui/buffers.js`, `mia_buffers/ui/dom.js` (the six words)
- Test: `tests/test_buffers_api.py`, `tests/test_buffers_ui.py`

**Interfaces:**
- Consumes: Task 1's `phase`, Task 2's `gave-up` and `ended_revision`.
- Produces:
```python
IN_WORK = ("open", "working", "waiting")  # a case that has not ended

def status_of(phase: str, shelved: bool, repair: dict | None, acting: bool) -> str:
    """Who moves the subject next (spec overview §3.2)."""
```
  `repair` is the view's: `{"id": int, "rule": str, "state": str, "needs": str | None}` or None.
- Produces: `BufferService._repairs() -> dict[tuple[str, int], dict]`: the newest case of each subject among
  those in work and those `gave-up`, read once for many views.
- Produces: every subject view has `"status"` (one of the six) and `"repair"`. A `gave-up` case is on the view
  only while `data["ended_revision"] == row["revision"]`, or, with none recorded, `case["updated"] >=
  row["updated"]`. `acting` is `(ledger.kv("cleaner") or {}).get("mode") == "act"`.

- [ ] **Step 1: Write the failing tests**

`tests/test_buffers_api.py`:
```python
@pytest.mark.parametrize(
    "phase, shelved, state, acting, status",
    [
        ("stopped", True, "working", True, "shelved"),
        ("done", False, "working", True, "in repair"),  # the agent's own cancel, before its resume
        ("stopped", False, "open", True, "in repair"),
        ("working", False, "open", True, "in repair"),
        ("done", False, "open", True, "done"),
        ("stopped", False, "waiting", True, "in repair"),
        ("working", False, "waiting", True, "working"),
        ("stopped", False, "open", False, "needs human"),  # report mode repairs nothing
        ("done", False, "gave-up", True, "needs human"),  # given up where the agent's own step left it
        ("working", False, "gave-up", False, "needs human"),
        ("done", False, None, True, "done"),
        ("not started", False, None, True, "not started"),
        ("working", False, None, True, "working"),
        ("waiting", False, None, True, "needs human"),
        ("stopped", False, None, True, "needs human"),
    ],
)
def test_who_moves_a_subject_next(phase, shelved, state, acting, status):
    repair = {"id": 1, "rule": "diagnosis", "state": state, "needs": None} if state else None
    assert status_of(phase, shelved, repair, acting) == status
```
And through the service: a dataset at `blocked` reads `needs human`; with an `open` case and a stored report
of mode `act` it reads `in repair` and `view["repair"]["id"]` is the case's; with the case `gave-up` at the
subject's revision it reads `needs human` with `repair["needs"]`; after a stage write the same `gave-up` case
is off the view; a `done` dataset with an old `gave-up` case that records no revision reads `done`.

`tests/test_buffers_ui.py` (the status words test): `"saved, not started", "needs human", "working",
"serving", "serving", "needs human", "retired", "shelved"`, plus a buffer with status `in repair` reading
`in repair`.

- [ ] **Step 2: Run them and see them fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_api.py tests/test_buffers_ui.py -q -x`
Expected: FAIL (`status_of` is not defined).

- [ ] **Step 3: Implement**

`status_of` as the table of spec §3.2, without the judge's look. `_subject_view(row, holds=None,
repairs=None)` reads `_repairs()` when none is passed; every caller that builds many views reads it once.
`_needs` lists views whose status is `needs human`, or that have `drops`. The page: `statusWord` shows the
status as it is, `saved, not started` for `not started` and `serving` for a serving buffer; the pill is
`warn` for `needs human`; `PILL_HELP` gains one line each for `needs human` and `in repair`.

- [ ] **Step 4: Run the covering tests**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_api.py tests/test_buffers_ui.py tests/test_buffers_sweep.py tests/test_buffers_activities.py -q`
Expected: PASS.

- [ ] **Step 5: Commit** (`status: one word says who moves a subject next, and needs human is one of them`)

### Task 4: One route lists the subjects of every kind by status

**Files:**
- Modify: `mia_buffers/api.py` (route `GET /subjects`, read; `GET /needs` goes; `_needs` becomes `_by_status`)
- Modify: `mia_buffers/ui/subjects.js` (its list reads the new route until Task 5 moves it)
- Test: `tests/test_buffers_api.py`, `tests/test_buffers_ui.py`

**Interfaces:**
- Produces: `GET /subjects?status=<one of the six>&kind=<kind>&limit=&offset=&sort=&dir=` returns
  `{"subjects": [...], "total": n (when paged), "counts": {kind: {status: n}}, "kinds": {kind: n}, "failed":
  [...]}`. `status` defaults to `needs human`; under it a buffer whose dataset waits for files is listed too,
  with `drops`. `counts` is over every subject; `kinds` is the listed status's share per kind. An unknown
  status or kind is 422.

- [ ] **Step 1: Write the failing tests**

Rewrite the `/needs` tests of `tests/test_buffers_api.py` against `/subjects` (key `subjects`; `status=shelved`
where they passed `shelved=1`; `counts` where they read `shelved`), and add:
```python
def test_subjects_of_every_kind_are_listed_by_status_with_their_counts(tmp_path):
    ...  # a dataset at blocked, one at matching, one done, a buffer at plan-review, one shelved request
    out = call(s, "GET", "/subjects")[1]
    assert sorted(row["key"] for row in out["subjects"]) == ["rec-blocked", SPEC_NAME]
    assert out["counts"]["dataset"] == {"needs human": 1, "working": 1, "done": 1}
    assert out["counts"]["prep"] == {"shelved": 1} and out["kinds"] == {"dataset": 1, "buffer": 1, "prep": 0}
    assert [row["key"] for row in call(s, "GET", "/subjects", {"status": "working"})[1]["subjects"]] == ["rec-run"]
    assert call(s, "GET", "/subjects", {"status": "stuck"})[0] == 422
    assert call(s, "GET", "/needs")[0] == 404
```
`tests/test_buffers_ui.py`: `/subjects` is a read route; no module requests `/needs`.

- [ ] **Step 2: Run them and see them fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_api.py tests/test_buffers_ui.py -q -x`
Expected: FAIL (404 for `/subjects`).

- [ ] **Step 3: Implement** the route as the Interfaces block says; the page's list asks it.

- [ ] **Step 4: Run the covering tests** (the command of Step 2 without `-x`). Expected: PASS.

- [ ] **Step 5: Commit** (`buffer-api: the subjects of every kind are listed by status`)

### Task 5: Overview

**Files:**
- Create: `mia_buffers/ui/overview.js`
- Modify: `mia_buffers/ui/index.html` (tabs, sections), `mia_buffers/ui/app.js` (views, the landing view, the
  stream, the banner), `mia_buffers/ui/subjects.js` (its Needs you view goes), `mia_buffers/ui/operations.js`
  (its Cleaner view goes; `caseList` and `caseRows` are exported), `mia_buffers/ui/dom.js` (`stageHelp`),
  `mia_buffers/ui/app.css` (the fold)
- Modify: `docs/development.md` (the page's module table)
- Test: `tests/test_buffers_ui.py`

**Interfaces:**
- Consumes: `GET /subjects` (Task 4), `GET /cases?days=7`, `GET /cleaner`, the view's `status` and `repair`.
- Produces: `export function viewOverview(arg, refresh)`; `export const SECTIONS = ["system", "counts",
  "needs", "repair", "working", "repairs", "shelved"]`; `export function fold(id, title, count, sub, ...kids)`:
  a `<details class="card fold" data-fold="<id>">` whose `<summary>` holds the title and the count, open unless
  `localStorage["mia-fold-<id>"] === "0"`, and that writes `"0"` or `"1"` there when it is toggled;
  `export function overviewCases(cases, subjects, acting, now)` returning `{system, repair, ended}`: the
  `gave-up` cases with no subject; the cases in work shown under In repair (in `act` mode those of subjects
  whose status is `in repair` and of left-behind things, otherwise every case in work); the cases that ended
  within seven days.
- The address `#overview/<kind>` narrows Needs human to a kind.

- [ ] **Step 1: Write the failing tests** in `tests/test_buffers_ui.py`

- `test_overview_is_the_landing_page_and_names_seven_folding_sections`: `index.html` has
  `data-tab="overview"` first among the tabs and no `data-tab="needs"` or `data-tab="cleaner"`; `app.js` has
  `overview: viewOverview` and falls back to `"overview"`; `SECTIONS` is the seven ids.
- `test_a_section_folds_and_stays_as_it_was_left`: with the fake DOM and a `localStorage` stub, `fold("needs",
  "Needs human", 3, "")` is open; after its `toggle` with `open = false` the store holds `mia-fold-needs = "0"`
  and a second `fold("needs", …)` is closed.
- `test_overview_sorts_the_cases_by_what_a_person_does_with_them`: `overviewCases` over a `gave-up` job case,
  an `open` dataset case whose subject is `in repair`, a `waiting` dataset case whose subject is `working`, a
  `resolved` case of yesterday and a `gave-up` dataset case of last month gives `system` = the job case,
  `repair` = the open case, `ended` = the resolved case; with `acting` false, `repair` holds both cases in work.
- `test_what_needs_a_human_says_why_and_offers_its_answer`: the row of a subject with `repair.state ===
  "gave-up"` shows `repair.needs`; one without shows its stage's sentence.
- Rewrite `test_page_has_a_cleaner_view_and_the_banner_says_its_findings` and
  `test_needs_you_is_narrowed_by_toggle_buttons_not_a_drop_down` for the new module; the module table of
  `test_every_file_the_page_loads_is_one_of_its_own` still orders.

- [ ] **Step 2: Run them and see them fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_ui.py -q -x`
Expected: FAIL (no `overview.js`).

- [ ] **Step 3: Implement**

Sections, in order, each a `fold`:

| id | Title | Body |
|---|---|---|
| `system` | System | the last sweep's findings or "All services operating, no problems found.", when it was checked, the mode when it is not `act`, the last sweep's errors, and `caseList` of `system` cases |
| `counts` | Counts | one row per kind, one column per status; `needs human`, `in repair`, `working` and `shelved` open their section (and narrow Needs human to the kind); the others link to `#datasets`, `#buffers`, `#preparation` |
| `needs` | Needs human | the kind toggles, the undelivered actions, and a paged table from `/subjects?status=needs human`: what, name, stage, since, why, answer |
| `repair` | In repair | `caseList` of `repair` cases |
| `working` | Working | a paged table from `/subjects?status=working`: what, name, stage, since |
| `repairs` | Repairs in the last week | `caseList` of `ended` cases |
| `shelved` | Shelved | a paged table from `/subjects?status=shelved`: what, name, stage, reason, Unshelve |

The count in a section's heading is its rows' total. The view redraws on the stream as the Needs you view
did, and keeps what is being typed.

- [ ] **Step 4: Run the covering tests**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_ui.py -q && pixi run lint`
Expected: PASS, lint clean.

- [ ] **Step 5: Commit** (`page: Overview is the landing page; the Needs you and Cleaner tabs are gone`)

### Task 6: The texts say "repair agent" and "needs human"

**Files:**
- Modify: `mia_buffers/ui/*.js`, `mia_buffers/ui/guide.html`, `mia_buffers/ui/architecture.html` (every
  "Cleaner", "the cleaner", "Needs you", "needs you", "waits for you" a person reads)
- Modify: `mia_buffers/sweep.py`, `mia_buffers/cleaner.py`, `mia_buffers/diagnose.py`, `mia_buffers/api.py`,
  `mia_buffers/activities.py` (texts written to a timeline, an issue, a refusal or a log)
- Modify: `AGENTS.md`, `README.md`, `docs/development.md`
- Test: `tests/test_buffers_ui.py`, `tests/test_buffers_sweep.py`, `tests/test_buffers_cleaner.py`

**Interfaces:**
- Consumes: nothing new. The marker `<!-- mia: cleaner <id> <mark> -->` stays: it is how an entry is found
  again.

- [ ] **Step 1: Write the failing tests**

`tests/test_buffers_ui.py`:
```python
def test_the_page_names_the_repair_agent_and_one_status_for_a_humans_turn():
    shown = "\n".join(text for name, text in FILES.items() if name != "app.css")
    said = re.sub(r"//[^\n]*|<!--.*?-->", "", shown, flags=re.S)  # comments may name the module
    for old in ("Cleaner", "the cleaner", "Needs you", "needs you", "waits for you", "needs a person"):
        assert old not in said, old
    assert "Repair agent" in said and "needs human" in said
```
`tests/test_buffers_sweep.py`: the entry a case writes begins `**Repair agent:**`.

- [ ] **Step 2: Run them and see them fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_ui.py tests/test_buffers_sweep.py -q -x`
Expected: FAIL.

- [ ] **Step 3: Implement** the texts. `docs/development.md` and `AGENTS.md` describe the three words of spec
  §3, the status, the case states and Overview, and name the repair agent with its present identifiers in
  backticks.

- [ ] **Step 4: Run everything**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && PYTHONPATH=$PWD pixi run test`
Expected: all clean.

- [ ] **Step 5: Commit** (`texts: the repair agent, and needs human as the one status for a human's turn`)
