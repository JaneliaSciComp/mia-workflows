# Job records and stage results — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** every cluster job leaves a gzipped record (an agent's with its prompt and conversation), the page shows
it, and every stage pass of a round writes a start entry and a result entry that links its jobs.

**Architecture:** `CsubJobs` writes the record in two halves (submit, end of wait) through `mia_core/jobrecord.py`;
buffer-api serves the file as it is; the page renders it at `#job/<address>`. The workflows keep the open pass in
their durable state (stage, pass number, jobs, result text) and the stage activity writes the pass's result
entry before the next stage's entry, or as the entry that moves the subject to the stage it rests at.

**Tech stack:** Python 3.12, Temporal (replay fixtures), stdlib `gzip`/`json`/`subprocess`, the page's ES modules.

**Spec:** `docs/specs/2026-11-14-job-records.md` (v0.6).

## Global constraints

- Run checks from the worktree with the main checkout's environment: `<main>/.pixi/envs/default/bin/python -m
  pytest tests/<file> -q` (and `ruff`, `mypy` from the same `bin/`). A small change gets the tests that cover it;
  the full suite runs once at the end.
- No env value in a record; every job-derived string passes `SECRET_RE` (`[redacted]`).
- Recording is best effort: no exception from `jobrecord` reaches `submit` or `wait`.
- Job output is untrusted: `read_untrusted`, size caps, defensive parsing; the page inserts it as text.
- Result dataclasses in `models.py` are untouched; state dataclasses gain defaulted fields only.
- Every changed workflow command sits behind `workflow.patched("stage-results")`; older replay fixtures stay as
  they are and one new fixture per workflow is recorded alone.
- No real paths or hostnames in tracked files; line length 120; a plain-language line on every new module, class
  and non-obvious function; `# ponytail:` on deliberate shortcuts.
- Commit each task with explicit paths.

## Review focus

1. A stream that is cut off, empty or not JSON (a killed agent): the record still gets `outcome` and `logs`, and
   `agent.turns` holds what parsed. Test in Task 1.
2. A workspace outside `work_root` (a hand-run job, autodev's judge dir): no record, no error. Test in Task 1.
3. A record path with `..`, an absolute segment or a symlink: 404. Test in Task 2.
4. A run that rolled over mid-round before the deploy and takes the patch mid-pass (no pass open in its state):
   stage writes behave as before the patch. Test in Task 3.
5. A cancel while a job runs: the cancelled pass's result names the killed job and the subject rests at
   `cancelled`. Test in Task 3.

---

### Task 1: the record (`mia_core/jobrecord.py`, `CsubJobs`, the agent commands)

**Files:** create `mia_core/jobrecord.py`, `tests/test_core_jobrecord.py`; modify `autodev/jobs.py`,
`autodev/agent.py`, `mia_core/judge/evidence.py`, `mia_data/jobspec.py`, `mia_data/cli.py`, `mia_buffers/cli.py`,
and the tests that assert those scripts (`tests/test_autodev_jobs.py`, `test_autodev_agent.py`,
`test_core_judge.py`, `test_mia_jobspec.py`).

**Produces:**

- `RECORDS_DIR = "_jobs"`
- `address(work_root: Path, workspace: str | Path) -> str | None`: the workspace relative to `work_root`, posix;
  None when it is not under it (or is `_jobs` itself).
- `record_path(work_root: Path, address: str) -> Path`: `<work_root>/_jobs/<address>.json.gz`.
- `write_submitted(work_root, workspace, *, service: str, key: str, job_id: str, request: dict, submit: dict |
  None, prompt_file: str | None, transcript_file: str | None) -> None`
- `write_finished(work_root, workspace, *, job_id: str, outcome: dict) -> None`: adds `outcome`, `usage`
  (`lsf_usage`), `logs` (last 64 kB each of `.csub/jobs/<job id>/stdout|stderr`), and `agent` when the first half
  names a transcript; with no first half it writes what it has.
- `reduce_stream(text: str) -> dict`: `{model, cost_usd, num_turns, duration_ms, turns}`; a turn is `{"role":
  "assistant", "text"}`, `{"role": "thinking", "text"}` or `{"role": "tool", "name", "input", "error": bool}`,
  plus `"sub": true` for a sub-agent's (`parent_tool_use_id`). Tool results only set `error` on their call;
  `system`, `rate_limit_event` and unparseable lines are dropped; each input string is cut at 2,000 characters
  with `… (<n> more characters)`.
- `lsf_usage(job_id: str) -> dict | None`: `bjobs -o "max_mem avg_mem memlimit cpu_used run_time slots" -json`,
  10 s timeout, None on any failure. `# ponytail:` csub reporting usage is the upgrade.
- `JobSpec.prompt_file`, `JobSpec.transcript_file` (workspace-relative, default None);
  `CsubJobs(..., work_root: Path | None = None)`.

**Steps**

- [ ] Tests first (`tests/test_core_jobrecord.py`): `reduce_stream` over a fixture stream (init, thinking, a
  tool call with a 5,000-character input, its result, an errored result, text, a junk line, the result line)
  asserts no tool output anywhere in the dump, the cut input, `error`, `model`/`cost_usd`; a planted `sk-ant-…`
  in a text block comes out `[redacted]`; `write_submitted` + `write_finished` on a tmp `work_root` produce one
  gzip JSON with `schema: 1`, both halves, `env` as names; an env value never appears in the bytes; a workspace
  outside `work_root` writes nothing; an unwritable `_jobs` raises nothing.
- [ ] Implement `mia_core/jobrecord.py`.
- [ ] `CsubJobs.submit` calls `write_submitted` after a submit (with `SubmitResult` as a dict) and after a
  found-by-workspace reattach (`submit=None`); nothing on an id-file reattach. `CsubJobs.wait` calls
  `write_finished` before returning. Extend `tests/test_autodev_jobs.py`: both halves with the fake client; a
  failing writer leaves `submit` and `wait` results unchanged.
- [ ] The three agent commands write `--output-format stream-json --verbose` to `claude.jsonl` (autodev:
  `../claude.jsonl`) and then `tail -n 1` it into `claude.json` / `../result.json`, keeping `rc` the agent's; their
  specs set `prompt_file` and `transcript_file` (`prompt.md`/`claude.jsonl`; judge `evidence/question.md`;
  autodev author `prompt.md`/`claude.jsonl` in the round dir). Update the script assertions.
- [ ] Every `CsubJobs(...)` construction passes `work_root=cfg.paths.work_root`.
- [ ] A test in `test_mia_jobspec.py`/`test_buffers_jobspec.py`: no spec builder mounts `work_root` itself or
  anything under `_jobs`.
- [ ] Run the touched test files; commit.

### Task 2: serving and showing a record

**Files:** modify `mia_buffers/api.py`, `mia_buffers/ui/app.js` (route `#job/…`), `mia_buffers/ui/index.html` if a
container is needed, `docs/development.md` (the page's module table, the route list); create
`mia_buffers/ui/job.js`; tests in `tests/test_buffers_api.py`, `tests/test_buffers_ui.py`.

**Consumes:** `mia_core.jobrecord.record_path`, the record of spec §3.1 with Task 1's `agent.turns` shape.

**Steps**

- [ ] Route `("GET", r"^/jobs/(.+)$", "job_record", "read")`: every `/`-separated segment matches `UI_NAME_RE`
  (no `..`, no empty segment), the file is read with `read_untrusted(work_root / "_jobs", "<address>.json.gz",
  limit, binary=True)` (no symlink), served with `Content-Type: application/json`, `Content-Encoding: gzip`,
  `Cache-Control: no-cache`. 404 otherwise; 503 without `work_root`. Tests: served bytes gunzip to the record;
  `..`, an absolute path, a symlinked file and a missing one are 404.
- [ ] `job.js` exports `viewJob(address)`: fetches `/jobs/<address>` through `api.js`, renders the header (name,
  state, exit code, queue, host, times, usage, cost), for an agent the prompt in a closed `<details>` and the
  turns (assistant text as pre-wrapped text; thinking in a closed `<details>`; a tool call as `<details>` whose
  summary is the tool's name and a one-line input, marked when it failed; sub-agent turns indented), then the
  script, csub's answer and the log ends in `<pre>`. A record without `outcome` says "still running, or its end
  was not recorded". Everything through `el(...)` text nodes; no `innerHTML`. `app.js` routes `#job/<address>`.
- [ ] `tests/test_buffers_ui.py`: the module imports under node, renders a sample record with a `<script>` string
  in a turn as text, and the page still calls read and action routes only.
- [ ] Run the two test files and `biome ci`; commit.

### Task 3: the dataset workflow writes a start and a result per pass

**Files:** modify `mia_data/models.py` (`DatasetState`), `mia_data/config.py` (`STAGE_SENTENCES`),
`mia_data/status.py` (`render_stage`, `render_result`, `render_jobs`), `mia_data/activities.py` (`set_stage`,
`stage_result`, `read_judge`'s link), `mia_data/workflows.py`, `tests/test_mia_workflows.py`,
`tests/test_mia_activities.py`, `tests/test_mia_status.py`; new fixture `tests/fixtures/mia/dataset_history_r.json`.

**State (defaulted, reset with the round):** `passes: dict[str, int]`, `pass_stage: str = ""` (the pass whose
result is due), `pass_jobs: list[str]` (workspaces), `result: str = ""`, `result_binding: bool = False`.

**Activity rules (`set_stage`, shared with Task 4 through helpers in `mia_data/status.py`):**

- A pass stage is one `round_open(stage)` says is working or waiting.
- With `state.pass_stage` set, first write that pass's result: `**Result:** \`data:<stage>\`` + the text, the
  binding when `result_binding`, `**Jobs:**` links (`<service.url>/#job/<address>` for each workspace whose
  record exists, in a `<details>` beyond 4), marker `result <round> <stage> <n>`.
  - target stage is a pass stage: the text is `state.result`; then the start entry.
  - target is a resting stage: the text is `**<stage>**: <detail>` + `extra`; the same entry carries
    `stage=<target>` and ends `The dataset rests at \`<target>\`.`; no further entry.
- Without `state.pass_stage`: as today, a start entry gaining the stage's sentence.
- Start marker `stage <round> <stage>`, with ` <n>` from the second pass (`n = state.passes.get(stage, 0) + 1`).
- `stage_result(issue, state)` writes only the result of `state.pass_stage` (for a pass no stage write follows).
- `read_judge` appends `[The judge's conversation](<service.url>/#job/<address>)` to the comment it renders.

**Workflow rules (all under `workflow.patched("stage-results")`; without it nothing below runs):**

- `_job`: after `submit` returns, append the workspace to `state.pass_jobs`.
- `_stage`: after the write, a pass stage sets `passes[stage] += 1`, `pass_stage = stage`, clears `pass_jobs`,
  `result`, `result_binding`; a resting stage clears all four.
- The crops reports (`_round`, `_test_head`, `_merged`) and the debugger's `found` go to `state.result`
  (`result_binding` for the reports) instead of a `comment` on the dataset; the PR's copy of a report stays.
- Results set before the next stage write: matched (`Matched: …reasons`), `needs_snippet` (what fails), review
  rejected, PR approved (`/approve` sent), merged.
- `_finish("failed")` with a debugging cycle left: `state.result = "**failed**: <detail>"`, and `debugging`
  starts with `DEBUG_NEXT` alone.
- `_test_head`: starts a `testing` pass unless one is open (`pass_stage == "testing"`); a head whose test does
  not move the stage calls `stage_result`. `_debug` on `rerun` calls `stage_result` in place of the comment.
- `_round`: a pass still open from the round before (a restart at `review`, a go during `debugging`) gets
  `result = "ended: a new round was started"` and `stage_result` before the round id changes.

**Steps**

- [ ] Tests first, in `tests/test_mia_workflows.py` (the mock `set_stage` records `state.pass_stage`,
  `state.result`, `state.pass_jobs`; a mock `stage_result`): a matched round to `done` (each pass's result is
  carried by the next stage write; the `done` write carries `review`'s pass and no data comment holds a
  report); a round whose crops fail (the `failed` write carries `pass_stage == "testing"`, the report and both
  jobs); a cancel mid-match (Review focus 5); a develop round with a failed head then a passing one (two
  `testing` starts, `stage_result` after the first); a state with the patch on and `pass_stage == ""` at
  `review` behaves as before (Review focus 4).
- [ ] `tests/test_mia_activities.py` / `test_mia_status.py`: `set_stage` with an open pass writes result then
  start (markers, `Jobs:` only for existing records); to a resting stage writes one entry with `stage=`; a
  second pass gets ` 2` in its start marker; a retry posts nothing new; `stage_result`; the judge link.
- [ ] Implement models, config sentences, status renderers, activities, workflow.
- [ ] Adapt existing workflow tests whose assertions name the report comment or the debugging detail.
- [ ] Record the new fixture alone (`MIA_DATA_RECORD_HISTORY=1 … -k replay_dataset_history_r`) with its
  `test_replay_dataset_history_r`; all replay tests pass.
- [ ] Run `tests/test_mia_workflows.py`, `test_mia_activities.py`, `test_mia_status.py`; commit.

### Task 4: the buffer and request workflows, the same way

**Files:** `mia_buffers/models.py` (`BufferState`, `PrepState`), `mia_buffers/config.py` (sentences),
`mia_buffers/activities.py` (`set_stage`, `set_prep_stage`, `stage_result`), `mia_buffers/workflows.py`,
`tests/test_buffers_workflows.py`, `tests/test_buffers_activities.py`; new fixtures `buffer_history_r.json`,
`prep_history_r.json`.

**Consumes:** Task 3's renderers and rules, unchanged. Job workspaces are appended where `submit` (stage jobs)
and the fetch jobs' submit return. The plan comment is `planning`'s result; `comment_prep`'s outcomes table is
`requested`'s. Jobs submitted while the subject rests (a rebuild at `ready`) belong to no pass.

**Steps:** tests first (a round to `ready`; one that ends `needs-human` in `sourcing`; a cancel; a request to
`done`), implement, adapt assertions, record the two fixtures alone, run the two test files, commit.

### Task 5: documents and the whole branch

- [ ] `AGENTS.md`: one rule ("A stage pass writes its start and its result … the result links its jobs; a job's
  record is `<work_root>/_jobs/<workspace>.json.gz`, written by `CsubJobs`, never deleted"), and `mia_core`'s
  line in the opening paragraph. `docs/development.md`: A dataset round / A buffer round. The spec's status and
  §3.6 "How it is written" as built.
- [ ] `git diff main... | grep -n 'rokickik\|/nrs/'` is empty; `ruff check`, `ruff format --check`, `biome ci`,
  `mypy`, the full `pytest tests/`.
- [ ] One fresh reviewer over the branch; fix what is serious; commit.
