# Sample filters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A buffer's definition names the tests a crop must pass (`sampling.filters`), and the stage job stages
only samples that pass them on the real data.

**Architecture:** The worker adds `filters` to the plan rule only when the definition has some; the harness's
stage job keeps its seeded draw and tests each draw's full-resolution box (label box first, then raw) until the
split's count is met or 2000 draws in a row were rejected. stage.json carries the per-split counts; registration
scales a short volume's weight by staged / wanted and the dataset's "servable" timeline line names the shortfall.

**Tech Stack:** Python 3.12 (pydantic, numpy, zarr, pytest) in both repositories; ES modules for the page,
tested under node from pytest.

**Spec:** `docs/specs/2026-11-21-sample-filters.md` (this repository).

## Global Constraints

- Two repositories, each on a branch `sample-filters` in its own worktree: this one, and mia-snippets (the
  harness, based on its `origin/main`). **The harness deploys first.**
- A rule without filters hashes to today's digest on both sides and stages today's sample lists.
- `filters` appears in the rule, in its digest text and in the staging step's `parameters.plan` only when the
  list is not empty; entries are in name order.
- `GIVE_UP = 2000` rejected draws in a row; a repeated position counts as a rejection.
- `labelled`: `min_fraction` required, `0 < min_fraction <= 1`; a volume with no paired label passes unread.
- Both filters measure the full-resolution box: `patch` × `patch` at `starts["1"]` on the sample's plane.
- No workflow issues a new command; `RegisterResult` gains a defaulted field only.
- No `/nrs/` or user name in a tracked file. Line length 120. No `noqa` or `type: ignore` without a reason.
- Harness tests: in the harness worktree, `PYTHONPATH=$PWD/src <dev checkout>/harness/.pixi/envs/default/bin/python
  -m pytest tests/<file> -q`. Workflows tests: `PYTHONPATH=$PWD pixi run python -m pytest tests/<file> -q`. Run the
  files a task touches, not the full suite, until the last task.

## Review Focus

1. **A label stored with a `c` axis, or y and x transposed.** `labelled` counts a pixel annotated when any
   channel of any paired label is nonzero; pinned in Task 3 (`test_labelled_is_the_union_over_labels_and_channels`).
2. **A volume one footprint wide, with filters.** One position exists; the search must end after `GIVE_UP`
   repeats without a read per repeat; pinned in Task 2 (`test_a_volume_with_one_position_ends`).
3. **A stage.json `selection` of the wrong shape** (untrusted job output). Registration must fall back to the
   unscaled weight and no shortfall text, never raise; pinned in Task 6
   (`test_a_malformed_selection_scales_nothing`).
4. **A percentage that is not exact in floats** (7 % ↔ 0.07) must not lock the page's controls; pinned in Task 7.
5. **A plane-access reader in the search.** Each draw must not decode a plane twice for label and raw of the
   same plane; pinned in Task 4 (`test_a_plane_reader_is_read_once_per_plane_in_a_row`).

---

## Part A — harness (mia-snippets worktree, `harness/`)

### Task 1: the rule carries filters

**Files:** Modify `src/mia_harness/plan.py`, `src/mia_harness/stage.py` (the step's `parameters`);
Test `tests/test_plan.py`, `tests/test_stage.py`.

**Interfaces — produces:**
- `PlanRule.filters: tuple[dict, ...] = ()` (each `{"name": str, **parameters}`).
- `PlanRule.document() -> dict`: `dataclasses.asdict` without `filters` when it is empty, `filters` as a list
  otherwise. `digest()` hashes `document()`; `stage.py` records `document()` as `parameters["plan"]`.
- `load_plan` reads `rule.get("filters", ())` into a tuple.

- [ ] Tests: `test_plan_digest_golden` unchanged and passing; `test_plan_digest_with_filters_golden` pins the hex
  of the golden rule plus `filters=({"name": "content"}, {"name": "labelled", "min_fraction": 0.05})` (compute
  once, copy into Task 5's test); `test_a_rule_without_filters_documents_none` (`"filters" not in
  rule.document()`); `test_load_plan_reads_filters`. In `tests/test_stage.py` the provenance assertion compares
  with `plan.rule.document()`.
- [ ] Run, see them fail; implement; run `tests/test_plan.py tests/test_stage.py`; commit.

### Task 2: the search

**Files:** Modify `src/mia_harness/plan.py`; Test `tests/test_plan.py`.

**Interfaces — produces:**
- `GIVE_UP = 2000`, with a `# ponytail:` comment (ceiling: thousands of draws on a sparse volume; upgrade: one
  coarse scan of the label, then draw from where it is annotated).
- `select_samples(rule, record, volume, axes, shape, voxel_size_nm, counts, keep=None) -> tuple[dict[str,
  list[dict]], dict[str, dict]]`. `keep: Callable[[dict], str | None] | None` gets a sample descriptor and
  returns the name of the filter that rejects it, or None. The second value is the selection, per split:
  `{"wanted": int, "staged": int, "draws": int, "rejected": {reason: int}}` (reason `"repeat"` or a filter name).
- `derive_samples(...)` keeps its signature and returns `select_samples(...)[0]`.

One loop per split, over the generator `derive_samples` uses today (same seed text, same order of `choice` and
`randrange`): with `keep` None it takes exactly `counts[split]` draws; with `keep` it runs until `wanted` are
accepted or `GIVE_UP` draws in a row were rejected. A repeat is rejected before `keep` is called.

- [ ] Tests: `test_select_without_keep_is_derive` (same lists as `derive_samples` for `RULE`);
  `test_keep_rejections_are_redrawn_until_the_count` (a `keep` rejecting every other call: `staged == wanted`,
  `rejected` counts it); `test_the_search_gives_up_after_a_run_of_rejections` (a `keep` that always rejects:
  `staged == 0`, `draws == GIVE_UP`, patched to a small number); `test_a_volume_with_one_position_ends` (a
  volume exactly one footprint wide, `keep` accepting: one sample, `keep` called once, the rest `repeat`);
  `test_a_larger_count_extends_a_smaller_under_a_keep` (prefix property with a deterministic `keep`).
- [ ] Fail, implement, pass, commit.

### Task 3: the filters

**Files:** Create `src/mia_harness/filters.py`; Test `tests/test_filters.py`.

**Interfaces — produces:**
- `content(raw: np.ndarray) -> bool` → `png.content_ok(raw)`.
- `labelled(labels: list[np.ndarray], *, min_fraction: float) -> bool`: True for an empty list; else the share
  of pixels where any array, on any leading (channel) axis, is nonzero `>= min_fraction`.
- `FILTERS: dict[str, tuple[Callable, Literal["label", "raw"]]]`, in evaluation order: `labelled` (reads
  `"label"`), then `content` (reads `"raw"`).
- `ordered(filters: tuple[dict, ...]) -> list[tuple[str, Callable, str, dict]]`: the rule's filters in
  `FILTERS` order as (name, function, reads, parameters); `ValueError(f"unknown filter {name!r}")` for a name
  not in `FILTERS`.

- [ ] Tests: `test_content_rejects_a_constant_patch_and_keeps_noise`; `test_labelled_threshold_is_inclusive`
  (exactly 5 % annotated passes at 0.05, one pixel fewer fails; 1.0 passes only a full box);
  `test_labelled_is_the_union_over_labels_and_channels`; `test_labelled_passes_a_volume_without_labels`;
  `test_ordered_reads_labels_first_and_refuses_an_unknown_name`.
- [ ] Fail, implement, pass, commit.

### Task 4: the stage job selects

**Files:** Modify `src/mia_harness/stage.py`, `src/mia_harness/provenance.py`; Test `tests/test_stage.py`.

**Interfaces — consumes:** `select_samples`, `ordered`. **Produces (stage.json, read by Task 6):**
- `VolumeSummary.selection: dict[str, dict] | None = None` — Task 2's selection, set only when the rule has
  filters.
- top-level `"unstaged": [{"volume": "v<i>.zarr", "selection": {...}}]`, present only when a volume with a
  share accepted nothing.
- A record that staged nothing with filters set: `errors == ["no crop passed the filters (<names>): " + per
  volume and split "v0 train: 2000 draws, rejected labelled 1990, repeat 10"]`. Without filters the error stays
  `"no samples planned"`.

`_plan_volume` builds `keep` when `rule.filters` is not empty: for each filter in `ordered` order, read what it
`reads` at the sample's full-resolution box (all channels; labels = the label arrays paired with this raw) and
return the first failing filter's name. A box is read through `_read_yx`; for an array whose `access` is
`"plane"` the last plane read is kept per array, as `write_pyramid` does. A volume whose samples are all empty
returns no `_Volume`; its selection goes to `unstaged`.

- [ ] Tests (arrays built with `test_smoke.write`, then the `.npy` files overwritten: a raw that is constant
  except one textured region, a label nonzero only there):
  `test_stage_without_filters_writes_no_selection`; `test_filters_keep_only_crops_on_content_and_labels` (every
  staged sample's level-1 box read back from the `.npy` passes both; `selection` counts add up: `staged +
  sum(rejected) == draws`); `test_a_rejected_label_draw_reads_no_raw` (a label of zeros, raw served over HTTP:
  the server saw no request for the raw file); `test_a_volume_without_labels_passes_labelled`;
  `test_a_record_with_nothing_passable_fails_naming_the_filters`;
  `test_a_volume_that_accepted_nothing_is_unstaged_and_check_stage_passes` (two raws, one blank);
  `test_an_unknown_filter_is_a_stage_error`; `test_a_plane_reader_is_read_once_per_plane_in_a_row`.
- [ ] Fail, implement, pass `tests/test_stage.py tests/test_check.py tests/test_provenance.py`, commit.

### Task 5: harness documents

- [ ] `SPEC.md` and `README.md` where they describe `stage` and stage.json: the filters, the search, `selection`
  and `unstaged`. Run the whole harness suite once (`pytest tests/`). Commit.

---

## Part B — workflows (this worktree)

### Task 6: the definition, the rule, the weight and the timeline line

**Files:** Modify `mia_buffers/spec.py`, `mia_buffers/plan.py`, `mia_buffers/register.py`,
`mia_buffers/models.py`, `mia_buffers/activities.py` (`register_dataset`), `mia_buffers/workflows.py`
(`_register`'s comment text); Test `tests/test_buffers_spec.py`, `tests/test_buffers_plan.py`,
`tests/test_buffers_register.py`, `tests/test_buffers_activities.py`, `tests/test_buffers_workflows.py`.

**Interfaces — produces:**
- `spec.ContentFilter` (`name: Literal["content"]`), `spec.LabelledFilter` (`name: Literal["labelled"]`,
  `min_fraction: float`, `gt=0, le=1`), `Sampling.filters: list[ContentFilter | LabelledFilter] = []`
  (discriminated by `name`); a validator refuses a repeated name and returns the list in name order.
- `plan.plan_rule` adds `"filters": [entry.model_dump() for entry in sampling.filters]` only when not empty.
- `plan._planned_cell` and the summary line read `up to` before the count when the rule has filters.
- `register.staged_share(volume: dict) -> float`: `sum(staged) / sum(wanted)` over the splits of a well-formed
  `selection`, else `1.0`; `volume_bodies` multiplies the weight by it.
- `register.shortfall(report: dict) -> str`: `""` when no split of a volume or `unstaged` entry is short; else
  ` Short of its plan: v0.zarr val 0 of 4 (labelled 2000); v1.zarr train 96 of 128 (labelled 1490, content 3).`
- `models.RegisterResult.short: str = ""`, set by `register_dataset`; `_register` appends it to the
  "is servable" line.

- [ ] Tests: grammar (accepts the spec's example; refuses an unknown name, a repeat, `min_fraction` 0 and 1.5,
  a missing `min_fraction`; returns name order); `test_plan_digest_golden` unchanged,
  `test_plan_digest_with_filters_golden` with Task 1's hex, `test_a_rule_without_filters_has_no_filters_key`;
  `test_planned_reads_up_to_with_filters`; `test_a_short_volume_weighs_less`,
  `test_a_malformed_selection_scales_nothing`, `test_shortfall_names_short_and_unstaged_volumes`;
  `register_dataset` returns `short`; the workflow's servable comment carries it; the replay tests pass
  unchanged.
- [ ] Fail, implement, pass the five files plus `tests/test_replay_*`, `pixi run lint`, `pixi run typecheck`,
  commit.

### Task 7: the page

**Files:** Modify `mia_buffers/ui/index.html`, `mia_buffers/ui/designer.js`, `mia_buffers/ui/spec.js`;
Test `tests/test_buffers_ui.py`.

**Interfaces:** controls `filterContent` (checkbox, "Skip blank crops"), `filterLabelled` (checkbox) with
`labelledPercent` (number, 1–100, default 5, "At least … % annotated"). `spec.js` writes
`  filters: [{name: content}, {name: labelled, min_fraction: 0.05}]` (only the set ones, name order; no line when
none) and reads the parsed definition back with `percent = Math.round(min_fraction * 1e4) / 100`.

- [ ] Tests: the written YAML for none, one and both; a definition with both round-trips through the controls
  with nothing lost, 7 % included; a `min_fraction` the controls cannot show (0.123456) makes them read-only.
- [ ] Fail, implement, pass, `pixi run lint`, check the page in headless Chromium, commit.

### Task 8: documents and the whole suite

- [ ] `docs/development.md` (the plan rule; the stage job; registration's weight), `AGENTS.md` (the artifact-id
  rule: filters are in the plan digest; the buffer-reads rule: a stage job's search reads boxes it may not
  stage, within `limits.job_bytes`), the spec's status line. `pixi run fmt`, `lint`, `typecheck`, `test`. Commit.

## After the build (by hand, not part of this plan's tasks)

Deploy the harness, then the worker; run a buffer with both filters on the incident's dataset and read its
stage.json: four `val` samples on tissue, no `train` sample under 5 % annotated.
