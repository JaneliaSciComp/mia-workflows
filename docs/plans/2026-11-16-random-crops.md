# Random Crops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A buffer's crops are placed at random (overlaps allowed), and how many each dataset gets is its weight's
share of one buffer-wide total the spec states.

**Architecture:** The spec gains `sampling.crops` (distinct crops per split in the whole buffer). The planner turns
each pool row's weight into a per-record count and writes it into the row's plan rule, where the harness already
reads `samples`. The harness shares a record's count among its volumes in proportion to their planes and draws each
crop's plane and position uniformly; the grid and the capacity cap go away. Serving, registration and the split
of planes do not change.

**Tech Stack:** Python (stdlib `random`, pydantic), pytest through pixi, in two repos: `mia-snippets`
(`harness/`) and `mia-workflows` (`mia_buffers/`).

**Spec:** the Design section below. It amends §2.1, §2.2 and §2.5 of
`docs/specs/2026-11-01-sparse-ome-zarr-0.6-staging.md`, which stays as the record of the grid.

## Design

Decided with the user on 2026-11-15/16:

- Crops are fully random. Two crops of a split may overlap. No grid, no capacity cap.
- The scientist sets the size of the set, not a per-volume count:
  ```yaml
  sampling:
    crops: {train: 100000, val: 2000, test: 2000}   # distinct crops in the whole buffer, shared out by weight
    max_crops_per_dataset: 4096                     # no dataset stages more than this in one split
  ```
- A dataset's count in a split is `weight × crops[split]`, at least 1, rounded up to a power of two, at most
  `max_crops_per_dataset`. The rounding keeps a record's count, and so its artifact id, the same when the pool
  changes a little; an artifact may hold up to twice the crops asked for.
- Splits are unchanged: whole z-planes per split, shuffled per volume; a one-plane volume lands whole in one split.
  Crops of a split lie only on its planes, so two splits never share a pixel.
- Serving is unchanged: a volume by weight, then one of its crops.

Calls made while planning (not yet confirmed by the user):

- **`max_crops_per_dataset`, default 4096.** One staged artifact is capped at `limits.stage_bytes` (5 GB by
  default); a crop touches up to 4 chunks per level, so a heavy dataset's full share would fail staging. The cap
  leaves its weight alone: its crops are drawn more often.
- **`sampling.plan` is accepted and ignored, with a warning.** Activities of a live buffer re-read the round's
  stored `spec.yaml`, so refusing the key would break buffers that exist.
- **A volume too small for the coarsest footprint gets no crops** and is not staged; the record fails only when no
  volume gets any.
- **Defaults** `{train: 100000, val: 2000, test: 2000}` are placeholders until a plan over the real pool sets them.

## Global Constraints

- The plan rule's keys stay `seed, patch, levels, samples, split, channel_recipe`; `samples` now means crops per
  record per split. `plan_digest` hashes the same canonical JSON in both repos.
- A record's crop list is a prefix sequence: a larger count extends the list, never redraws it.
- Sample descriptors keep their keys: `plane`, `centre_nm`, `levels`, `patch`, `starts`.
- A crop's level-0 start is a multiple of the coarsest factor and the footprint lies inside the volume.
- Activity result dataclasses in `models.py` are wire format: add fields with defaults, never rename or remove.
- No real paths or hostnames in tracked files. Line length 120, type hints on object parameters, a plain-language
  line on each new function.
- Comments and docs say what the code does, never what it replaced.
- Run only the test files a task names, then `pixi run fmt`, `pixi run lint` and `pixi run typecheck` once at the
  end of each part. Commit per task with explicit paths. Never push.
- The harness deploys before the workflows.

## Review Focus

1. A record whose every volume is smaller than the footprint: the stage report says `no samples planned`; nothing
   raises from `random.choices` over zero weights. (Task 1, Task 2)
2. A record with more volumes than crops (thousands of fields, 16 crops): only volumes that drew a crop are
   written, `stage.json` lists only those, `check-stage` passes. (Task 2)
3. A volume whose extent equals the footprint exactly: every crop starts at 0. (Task 1)
4. A pool of single-image datasets with small counts: no false "no record can supply it" error when the pool's
   expected crops in the split reach 1. (Task 4)
5. A stored spec that still has `sampling.plan`: it loads with a warning and uses `sampling.crops`. (Task 3)

---

## Part A: mia-snippets, branch `random-crops` off `main`

### Task 1: Random crops and a record's share per volume

**Files:**
- Modify: `harness/src/mia_harness/plan.py` (`derive_samples`, `capacity`)
- Test: `harness/tests/test_plan.py`

**Interfaces:**
- Produces:
  - `planes_per_split(rule: PlanRule, record: str, volume: int, axes: str, shape: tuple[int, ...]) -> dict[str, int]`:
    how many of the volume's planes each split holds (`split_planes` of `f"{record}/v{volume}.zarr"`); every
    split 0 when `rule.patch * max(rule.levels)` exceeds the y or x extent.
  - `share_out(rule: PlanRule, record: str, planes: list[dict[str, int]]) -> list[dict[str, int]]`: per volume,
    how many of `rule.samples[split]` crops it gets.
  - `derive_samples(rule, record, volume, axes, shape, voxel_size_nm, counts: dict[str, int]) -> dict[str, list[dict]]`:
    `counts[split]` crops per split; `[]` for a split that holds no plane.
- Removes: `capacity` from `plan.py` (`sample.capacity`, used by `run.py`, is another function and stays).

- [ ] **Step 1: Write the failing tests.** Replace `test_capacity_counts_the_planes_of_each_split`,
  `test_capacity_caps_and_a_split_fills_it_exactly` and `test_no_two_samples_of_a_split_overlap_at_any_level`;
  give every other `derive_samples` call in the file a `counts` argument equal to the rule's `samples`.

```python
COUNTS = {"train": 10, "val": 3, "test": 3}


def test_planes_per_split_is_zero_for_a_volume_under_the_footprint():
    assert planes_per_split(RULE, "r", 0, "zyx", (10, 600, 700)) == {"train": 6, "val": 2, "test": 2}
    assert planes_per_split(RULE, "r", 0, "yx", (200, 200)) == {"train": 0, "val": 0, "test": 0}   # footprint 256


def test_share_out_follows_planes_and_only_adds():
    planes = [{"train": 6, "val": 2, "test": 2}, {"train": 60, "val": 20, "test": 20}, {"train": 0, "val": 0, "test": 0}]
    rule = PlanRule(**{**RULE.__dict__, "samples": {"train": 1100, "val": 3, "test": 3}})
    shares = share_out(rule, "r", planes)
    assert [sum(s[name] for s in shares) for name in ("train", "val", "test")] == [1100, 3, 3]
    assert shares[2] == {"train": 0, "val": 0, "test": 0}
    assert 60 <= shares[0]["train"] <= 140   # 1/11 of 1100
    more = share_out(PlanRule(**{**RULE.__dict__, "samples": {"train": 2200, "val": 3, "test": 3}}), "r", planes)
    assert all(m["train"] >= s["train"] for m, s in zip(more, shares))
    assert share_out(rule, "r", [planes[2]]) == [{"train": 0, "val": 0, "test": 0}]   # nothing fits: no crops


def test_derive_is_deterministic_and_a_prefix():
    a = derive_samples(RULE, "r", 0, "zyx", (10, 600, 700), VOX, COUNTS)
    assert a == derive_samples(RULE, "r", 0, "zyx", (10, 600, 700), VOX, COUNTS)
    more = derive_samples(RULE, "r", 0, "zyx", (10, 600, 700), VOX, {**COUNTS, "train": 25})
    assert more["train"][:10] == a["train"] and len(more["train"]) == 25


def test_crops_are_random_inside_the_volume_on_their_splits_planes():
    wanted = {"train": 400, "val": 50, "test": 50}   # far over the 2x2 footprints a plane holds side by side
    planes = split_planes("r/v0.zarr", 10, RULE.split)
    drawn = derive_samples(RULE, "r", 0, "zyx", (10, 600, 700), VOX, wanted)
    for split, samples in drawn.items():
        assert len(samples) == wanted[split]
        for sample in samples:
            assert sample["plane"]["z"] in planes[split]
            y, x = sample["starts"]["4"]
            assert 0 <= y <= (600 - 256) // 4 and 0 <= x <= (700 - 256) // 4
    assert len({tuple(s["starts"]["4"]) for s in drawn["train"]}) > 4   # positions, not four tiles


def test_a_volume_exactly_one_footprint_wide_starts_every_crop_at_zero():
    drawn = derive_samples(RULE, "r", 0, "yx", (256, 256), {"y": 10.0, "x": 10.0}, {"train": 5, "val": 5, "test": 5})
    assert sum(len(s) for s in drawn.values()) == 5   # the one plane is in one split; the others get []
    assert all(s["starts"] == {"1": [96, 96], "2": [32, 32], "4": [0, 0]} for split in drawn.values() for s in split)
```

  Also `test_crops_of_a_split_may_overlap`: the same boxes the replaced overlap test builds, for
  `(2, 512, 512)` with `{"train": 50, "val": 50, "test": 50}`, asserting each non-empty split has 50 crops and at
  least one pair with `overlap_frac(a, b, size) > 0`.

- [ ] **Step 2: Run** `cd harness && pixi run python -m pytest tests/test_plan.py -q`. Expected: FAIL on the
  import of `planes_per_split` and `share_out`.

- [ ] **Step 3: Implement.**
  - `share_out`: per split, `Counter(Random(f"{rule.seed}:{record}:{split}").choices(range(len(planes)),
    weights=[p[split] for p in planes], k=rule.samples.get(split, 0)))`; a split whose weights are all 0 gives
    every volume 0. `choices` draws in order from one `Random`, which is what makes a larger count only add.
  - `derive_samples`: per split, `rng = Random(f"{rule.seed}:{record}:v{volume}:{split}")`; per crop, in this
    order: `z = rng.choice(planes_of_split)`, then for y and x
    `start = rng.randrange((extent - footprint) // coarsest + 1) * coarsest` and
    `centre_nm = (start + footprint / 2) * voxel_size_nm[axis]`. `starts` come from `start_index` as now.
  - Delete `capacity`.

- [ ] **Step 4: Run** the same command. Expected: PASS.

- [ ] **Step 5: Commit** `harness: crops are placed at random and a record's count is shared among its volumes`.

### Task 2: The stage job shares the record's count and skips volumes without crops

**Files:**
- Modify: `harness/src/mia_harness/stage.py` (`_plan_volume`, `stage`)
- Test: `harness/tests/test_stage.py`

**Interfaces:**
- Consumes: `planes_per_split`, `share_out`, `derive_samples(..., counts)` from Task 1.
- Produces: `_plan_volume(i, raw, binding, labels, rule_input, voxel, errors, counts: dict[str, int])`;
  `stage.json` lists only the volumes that got a crop, each still named `v<i>.zarr` by its raw's index.

- [ ] **Step 1: Write the failing tests.**
  - Change `test_stage_with_nothing_to_sample_says_so` to expect `rep["errors"] == ["no samples planned"]`.
  - Give the `derive_samples` call near line 120 its `counts`.
  - `test_stage_shares_a_records_crops_among_its_volumes`: a binding with two raws of shapes `(3, 600, 700)` and
    `(30, 600, 700)`, `plan_input((1, 2, 4), 64, {"train": 40})`. Assert the train samples over
    `rep["volumes"]` sum to 40 and the 30-plane volume holds more than the 3-plane one.
  - `test_stage_writes_only_volumes_that_drew_a_crop`: the same binding with `{"train": 1}`. Assert
    `len(rep["volumes"]) == 1`, exactly one `v*.zarr` directory exists under `out`, `rep["errors"] == []`, and
    `check_stage(out)["ok"]`.

- [ ] **Step 2: Run** `pixi run python -m pytest tests/test_stage.py -q`. Expected: the four tests above FAIL.

- [ ] **Step 3: Implement.** In `stage`, after `open_arrays`: `shares = share_out(plan.rule, binding.record,
  [planes_per_split(plan.rule, binding.record, i, _canonical(raw.axes), _shape(raw)) for i, raw in
  enumerate(raws)])`; plan only the raws with `any(shares[i].values())`; when none is planned and `errors` is
  empty, append `"no samples planned"`. `_plan_volume` passes `counts` to `derive_samples` and no longer reports
  a volume without samples.

- [ ] **Step 4: Run** `pixi run python -m pytest tests/test_stage.py tests/test_plan.py tests/test_check.py -q`.
  Expected: PASS.

- [ ] **Step 5: Commit** `harness: a stage job stages the volumes that drew a crop`.

---

## Part B: mia-workflows, branch `random-crops` in a worktree off the checkout's HEAD

### Task 3: The spec states the buffer's size

**Files:**
- Modify: `mia_buffers/spec.py` (`SamplePlan`, `Sampling`, `Spec._collect_warnings`, `Spec._plan_names_the_splits`)
- Modify: `tests/fixtures/buffers/spec.yaml`, `docs/specs/buffer-2d-gray.example.yaml`,
  `docs/specs/buffer-live-test.example.yaml` (replace the `plan:` block with `crops:`)
- Test: `tests/test_buffers_spec.py`

**Interfaces:**
- Produces: `Sampling.crops: dict[str, PositiveInt] = {"train": 100000, "val": 2000, "test": 2000}`,
  `Sampling.max_crops_per_dataset: PositiveInt = 4096`, `Sampling.plan: SamplePlan | None = None` (ignored).

- [ ] **Step 1: Write the failing test** in place of `test_sample_plan_defaults_and_must_match_splits`:

```python
def test_crops_default_and_must_match_splits():
    data = yaml.safe_load((FIXTURES / "spec.yaml").read_text())
    data["sampling"].pop("crops", None)
    spec = load_spec_text(yaml.safe_dump(data))
    assert spec.sampling.crops == {"train": 100000, "val": 2000, "test": 2000}
    assert spec.sampling.max_crops_per_dataset == 4096
    data["splits"]["fractions"] = {"train": 0.8, "val": 0.1, "test": 0.0, "extra": 0.1}
    with pytest.raises(SpecError, match="sampling.crops must name exactly the splits"):
        load_spec_text(yaml.safe_dump(data))


def test_a_stored_spec_with_a_sample_plan_loads_with_a_warning():
    data = yaml.safe_load((FIXTURES / "spec.yaml").read_text())
    data["sampling"]["plan"] = {"samples": {"train": 1000, "val": 20, "test": 20}}
    spec = load_spec_text(yaml.safe_dump(data))
    assert spec.sampling.crops == data["sampling"]["crops"]
    assert any(w.startswith("sampling.plan is ignored") for w in spec.warnings)
```

- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_spec.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement.** The warning text: `sampling.plan is ignored: sampling.crops sets the buffer's size`.
  Set the fixture and both example specs to `crops: {train: 1000, val: 20, test: 20}` with the comment
  `# distinct crops in the whole buffer per split, shared out by weight`.
- [ ] **Step 4: Run** the same command. Expected: PASS (other test files fail until Task 4).
- [ ] **Step 5: Commit** `buffers: a spec states the buffer's crops per split`.

### Task 4: The planner gives each dataset its share

**Files:**
- Modify: `mia_buffers/plan.py` (`plan_rule`, `_planned`, `_policy_errors`, `build_plan`, the `Row` comments)
- Test: `tests/test_buffers_plan.py`

**Interfaces:**
- Consumes: `spec.sampling.crops`, `spec.sampling.max_crops_per_dataset` from Task 3.
- Produces:
  - `crops_for(weight: float, total: int, cap: int) -> int`
  - `plan_rule(row, spec)["samples"] == {name: crops_for(row.weight, total, cap) for name, total in crops.items()}`
  - `_planned(row, f, rule) -> tuple[dict[str, float], int, bool]`: the expected crops per split, the byte bound,
    the unknown-shape flag. `Row.planned` holds the expectations rounded to int.
  - `_policy_errors(pool, spec, expected: dict[str, float])`: `expected` is the pool's summed expectation per split.

- [ ] **Step 1: Write the failing tests.**

```python
def test_crops_for_is_the_share_rounded_up_to_a_power_of_two():
    assert crops_for(0.004, 1_000_000, 1 << 20) == 4096
    assert crops_for(0.0038, 1_000_000, 1 << 20) == 4096   # a pool that grew a little: the same count
    assert crops_for(0.003, 1000, 4096) == 4
    assert crops_for(0.0, 1000, 4096) == 1                 # every dataset gets at least one
    assert crops_for(0.5, 1_000_000, 4096) == 4096         # the cap
    assert crops_for(0.5, 1_000_000, 3000) == 3000
```

  Rewrite these existing tests against the new rule (same fixtures):
  - `test_rows_carry_a_record_local_rule_and_digest` → `test_rows_carry_their_share_as_a_rule_and_digest`:
    `row.plan_rule["samples"] == {name: crops_for(row.weight, total, spec_2d.sampling.max_crops_per_dataset)
    for name, total in spec_2d.sampling.crops.items()}`; the key set and digest assertions stay.
  - `test_rule_json_matches_the_harness_dump`: build the expected bytes with U937's three counts formatted in.
  - `test_planned_counts_are_capped_by_capacity_and_reported` → `test_planned_is_the_share_a_record_can_supply`:
    BCSS (151 one-plane images) plans its full `samples` in every split; U937 (25 one-plane items) plans
    `round(samples[name] * (1 - (1 - fraction) ** 25))`.
  - `test_planned_cuts_planes_exactly_like_the_harness`: the 5-plane volume plans its full `samples` for train
    and val and 0 for test; the `split 'test'` error stays.
  - `test_raw_array_without_extents_counts_the_full_request`: planned equals `samples` in every split; the note
    stays.
  - `test_holdout_only_pool_cannot_supply_train`: U937 plans `{"train": 0, "val": 0, "test": samples["test"]}`.
  - `test_planned_caps_each_volume_like_the_harness` → delete (no capacity cap).
  - `test_split_unsupplied_by_every_record_is_an_error`: use one single-plane record and
    `crops: {train: 8, val: 1, test: 1}`; `split 'test'` and `split 'val'` are errors (expected 0.1 each).
  - New `test_a_pool_of_single_images_supplies_every_split`: the single-plane records with
    `crops: {train: 1000, val: 200, test: 200}`; no `split ...` error (Review Focus 4).
  - New `test_a_heavy_dataset_stops_at_the_cap`: `max_crops_per_dataset: 8`; no row's `samples` value exceeds 8
    and every row's `weight` equals its weight without the cap.
  - Leave `test_plan_digest_golden` as it is: the rule's shape did not change.

- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_plan.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement.**

```python
def crops_for(weight: float, total: int, cap: int) -> int:
    """A dataset's crops in one split: its share of the buffer's total, at least 1, rounded up to a power of two
    (so a pool that changes a little leaves the count, and the artifact id, alone), at most `cap`."""
    share = max(1, math.ceil(weight * total - 1e-9))
    return min(1 << (share - 1).bit_length(), cap)
```

  `_planned`, per split: the count times the chance the record holds a plane in the split. The chance is 1 for
  `test` and 0 otherwise on a holdout row; 1 when an array has no shape (the flag) or when an array that fits
  has at least `len(fractions)` planes per item and `split_counts(planes, fractions)[name] > 0`; otherwise
  `1 - (1 - fraction) ** n`, with `n` the planes over all items of the arrays that fit. The byte bound is the
  smaller of `levels × crops × 4 × patch² × channels × itemsize` and the bytes of the record's whole pyramids
  (`items × planes × y × x × channels × itemsize × Σ 1/factor²` over the arrays with a shape).
  `_policy_errors` reports a split when the pool's summed expectation is under 1.

- [ ] **Step 4: Run** `pixi run pytest tests/test_buffers_plan.py tests/test_buffers_spec.py -q`. Expected: PASS.
- [ ] **Step 5: Commit** `buffers: a dataset's crops are its weight's share of the buffer's total`.

### Task 5: The plan table shows crops and coverage

**Files:**
- Modify: `mia_buffers/plan.py` (`Plan`, `Plan.render_markdown`, `_planned_cell`, `Row.volumes`' comment)
- Test: `tests/test_buffers_plan.py`

**Interfaces:**
- Produces: `Plan.crops: dict[str, int] = field(default_factory=dict)` (the spec's totals, set by `build_plan`);
  table columns `… | weight | split preview | crops | coverage | est. bytes |`.

- [ ] **Step 1: Write the failing test.**

```python
def test_the_plan_table_shows_each_datasets_crops_and_coverage(plan_2d, spec_2d):
    row = plan_2d.rows_by_id[U937]
    md = plan_2d.render_markdown()
    cell = " · ".join(f"{name} {row.planned[name]}" for name in spec_2d.sampling.crops)
    coverage = sum(row.planned.values()) / row.positions
    assert f"| {cell} | {coverage:.2g}× |" in md
    planned = sum(sum(r.planned.values()) for r in plan_2d.rows if r.in_pool)
    assert f"planned {planned} of requested {sum(spec_2d.sampling.crops.values())} crops; ~" in md
    assert plan_2d.crops == spec_2d.sampling.crops
```

- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_plan.py -q`. Expected: this test FAILS.
- [ ] **Step 3: Implement.** Coverage is the row's planned crops over all splits divided by `row.positions`,
  formatted `f"{value:.2g}×"`, empty for a row out of the pool. The footer keeps its `(planned is an expectation
  over each record's items)` tail.
- [ ] **Step 4: Run** `pixi run pytest tests/test_buffers_plan.py tests/test_buffers_activities.py
  tests/test_buffers_register.py tests/test_buffers_api.py tests/test_buffers_ui.py tests/test_buffers_workflows.py -q`.
  Expected: PASS; a test in these files that pinned `sampling.plan.samples`, a per-volume count or the old table
  columns is updated to the new rule in this step.
- [ ] **Step 5: Commit** `buffers: the plan table shows each dataset's crops and coverage`.

### Task 6: The page and the docs say how a buffer is sized

**Files:**
- Modify: `mia_buffers/ui/spec.js` (emit `  crops: {train: 100000, val: 2000, test: 2000}` under `sampling:`,
  after `levels`)
- Modify: `mia_buffers/ui/guide.html` (New buffer paragraph: the spec's `sampling.crops` is the buffer's size)
- Modify: `README.md` (the sentence listing what a spec holds: "how many crops the buffer holds per split")
- Modify: `docs/development.md` (the sparse staging and Samples paragraphs: random placement, the share rule, the
  cap, a volume without crops is not staged)
- Modify: `mia.yaml.example` (the `stage_bytes` comment: it bounds `sampling.max_crops_per_dataset`)
- Test: `tests/test_buffers_ui.py`

- [ ] **Step 1:** Make the edits. Describe the system as it is; no mention of the grid.
- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_ui.py tests/test_buffers_spec.py -q`, then
  `pixi run fmt && pixi run lint && pixi run typecheck`. Expected: PASS and clean.
- [ ] **Step 3: Run** the real-paths check from `AGENTS.md` (Rules that will bite you) on the diff from the commit
  the branch started from. Expected: no output.
- [ ] **Step 4: Commit** `docs: a buffer's size is its crops per split, shared out by weight`.

---

## Rollout (on the user's word, not part of the build)

1. Merge the mia-snippets branch and update the checkout at `harness.dir`. From then on a stage job of any buffer
   places crops at random; a buffer planned earlier reads its stored per-volume count as a per-record count.
2. Merge the mia-workflows branch and restart the buffers worker and buffer-api.
3. A buffer created afterwards stages every dataset anew (new harness commit, new counts) and draws new frozen
   val and test lists. Buffers that are already staged keep serving what they hold.
4. Plan one buffer over the real pool and set `sampling.crops` and `max_crops_per_dataset` from its table.
