# MIA: sample filters — Spec

**Status:** v0.1 · 2026-11-21 · not approved. The direction (§4, approach A: one filter list, and a threshold
for the annotated share of a crop) is the user's decision. Everything marked *call* is the author's and the
user's to change. Rests on the training-buffers spec, the sparse OME-Zarr staging spec, the random-crops plan
and the stage-on-demand spec.

## 1. Intent

A buffer feeds crops to training. Which crops is decided by a random draw over each volume, and nothing looks
at what a drawn crop holds. A buffer's definition should be able to say what a crop must hold to be worth
feeding, and the crops a buffer serves should all pass.

The incident: the staged artifact of `aws-dharani-fetal-brain-nissl-annotated-sections` in the
`p256-l1-2-4-8` geometry. Its first volume is a 64000 × 64000 section whose tissue covers about 1 % of the
page. The plan gave that volume four `val` samples, and all four are blank slide: every raw value 255, every
label value 0. In the second volume (80000 × 80000, 37 % annotated) 63 of the 128 `train` samples hold no
annotated pixel at full resolution. So this record's whole `val` contribution is four white squares, and half
of its `train` crops carry no label. The stage job and `check-stage` passed: they check that the planned
chunks exist and sit where the plan says, which they do.

## 2. Today

- **The stage job places the samples and reads no voxel to do it.** `derive_samples` (harness `plan.py`)
  draws `counts[split]` positions per volume from a seeded generator: a plane of the split, then a position
  whose coarsest footprint lies inside the volume. A position drawn twice is listed once. The list is written
  to the volume's `attributes.mia.plan.samples`, registered, and buffer-api serves those samples verbatim
  (`mia_buffers/sampler.py`).
- **The worker's planner estimates.** `plan_rule` (`mia_buffers/plan.py`) gives each record its crops per
  split (`crops_for`), and `_planned` turns that into expected crops and an upper bound on staged bytes from
  the catalog's facts. The rule's canonical JSON is the plan digest, one input of the artifact id; the
  harness's `PlanRule.digest()` hashes the same text.
- **mia-data's crop job does filter.** `harness crop` rejects a candidate whose raw patch fails `content_ok`
  (`png.py`: too many non-finite values, a spread too small, or one value filling the patch) and moves every
  other crop onto an annotated pixel. Its budget is ten candidates per crop. That is why the reviewed crops of
  this dataset all came from the second section, while the buffer drew from both.
- **A crop has several levels.** A sample is one centre with a `patch`-sized box at each level of
  `sampling.levels`. The full-resolution box lies inside every coarser one.

## 3. Goals

1. A buffer's definition names the tests a crop must pass: `sampling.filters`, a list, empty by default.
2. Two filters exist: `content` (the crop job's test) and `labelled` (at least a stated share of the crop is
   annotated).
3. Every sample a buffer stages, and so every crop it serves, passed every filter of its definition, measured
   on the real data.
4. A new filter is one more entry of the list with its own parameters: one function in the harness, one line
   of the grammar, nothing else in the path.
5. A buffer with no filters gets the plan digests it gets today and stages the same samples.
6. A record that stages fewer crops than planned says so where a person follows the round, and takes a
   smaller part in training.

**Non-goals.** No change to mia-data's crop job or to the thresholds of `content_ok`. No per-dataset filter
settings: the list belongs to the buffer. No redistribution of a shortfall. No filter that needs a model.
No re-evaluation of filters by `check-stage` or at serving time.

**Anti-goals.** The planner must not pretend to know what the filters will keep: its numbers stay an upper
bound and say so. A filter must never turn a round into an unbounded search over a volume that holds nothing
passable. A dataset without labels must not vanish from a pretraining buffer because a label filter is on.

## 4. Approaches

- **A. One filter list (picked by the user).** `sampling.filters` holds entries `{name, …parameters}`. A crop
  is staged only if it passes each. The annotated-share test is one of them. Placement is not an option of the
  definition: how the stage job finds passing crops is the stage job's business.
- **B. Two options.** `place_by_label_extent: 0.05` beside `filters: [content]`. Same behaviour, but the
  label test sits apart from the other tests under a name that says "place" while it acts as a filter. Two
  encodings of one concept.
- **C. Aimed placement with a one-pixel guarantee**, as the crop job does it. Rejected by the user: one
  annotated pixel says nothing about a crop; the threshold has to be a share.

## 5. Design

### 5.1 The definition

```yaml
sampling:
  filters:
    - {name: content}                        # not blank, not constant: the crop job's content_ok
    - {name: labelled, min_fraction: 0.05}   # at least 5 % of the crop's pixels are annotated
```

- `filters` is optional and defaults to `[]`. A name appears at most once. An unknown name or parameter is a
  validation error of the definition (the grammar is strict, as the rest of `Sampling`).
- `content` takes no parameters.
- `labelled` takes `min_fraction`, required, greater than 0 and at most 1. *Call:* no default; the definition
  states its number.

### 5.2 What each filter measures

Both are measured on the sample's **full-resolution box** (`patch` × `patch` at level 1, on its plane). It
lies inside every coarser level's box, so a sample that passes there has content and annotation at every
level it is served at. *Call.*

- **`content`**: `content_ok` over the raw values of the box, all channels together.
- **`labelled`**: a pixel is annotated when any label array paired with the volume's raw is nonzero there
  (the union over the paired labels). The sample passes when annotated pixels / box pixels ≥ `min_fraction`.
  A volume with **no paired label array** passes `labelled` unread: the filter does not apply to it, so a
  pretraining buffer keeps its raw-only datasets. *Call.* A volume that has labels, all of them zero, fails
  every draw and stages nothing.

### 5.3 The plan rule and the artifact id

- `plan_rule` (`mia_buffers/plan.py`) adds a `filters` key **only when the list is not empty**. Its value is
  the list in name order, each entry with its parameters, so reordering the list in a definition changes
  nothing. *Call.*
- The harness's `PlanRule` gains `filters` (default empty); `digest()` leaves the key out when it is empty.
  The two digests stay byte for byte the same text, with and without filters.
- So a buffer without filters keeps its plan digests and its sample lists (goal 5). Its artifact ids change
  only as they do with any harness deploy, since the id holds the harness commit. A buffer whose definition
  gains, loses or changes a filter gets new plan digests: every record restages under a new artifact id, and
  its frozen `val` and `test` lists are drawn anew. That is the existing rule for a changed plan, not a new
  one.
- The harness refuses a rule that names a filter it does not know (a stage error naming it). The harness
  deploys first, as always.

### 5.4 The stage job selects

The draw stays `derive_samples`' draw: the same generator, the same sequence of (plane, position) per volume
and split. What changes is how many draws are taken and which are kept.

- **No filters:** exactly today's loop: `counts[split]` draws, a repeated position skipped. Same lists.
- **With filters:** draws continue until `counts[split]` samples are accepted or the search gives up. Each
  draw is tested in this order, stopping at the first failure: a repeated position; `labelled`; `content`.
  The label box is read before the raw box, so a draw that falls on unannotated slide costs a label read and
  no raw read. With only `content` set, only the raw box is read.
- **Giving up:** after `GIVE_UP` rejected draws in a row (a constant of the harness; *call:* 2000) the split
  of that volume ends with what it has. A repeated position counts as a rejection, so a small volume ends.
  On the incident's first volume the annotated region's bounding box is about 1.6 % of the page, so a sample
  costs at least sixty label reads; a volume with nothing passable costs 2000 label reads per split and stops.
  The constant is marked `# ponytail:` with the upgrade: scan the label coarsely once and draw from where it
  is annotated.
- **A larger count still extends a smaller count's list**: acceptance is sequential over one fixed sequence,
  and giving up depends only on that sequence.
- **Nothing is redistributed.** A volume's share of the record's crops (`share_out`) and a record's share of
  the buffer's (`crops_for`) are fixed before any read. A volume or record that comes up short stays short.
  *Call.*
- **A volume that accepted nothing in every split is not written.** A record none of whose volumes accepted
  anything fails its stage job with an error that names the filters and the rejections per filter; the round
  treats it as any other failed stage: the dataset is `failed` with the error quoted, and a later round
  searches again, since a failed build is never reused.
- **Bytes.** Every read of the search counts against the job's byte cap (`limits.job_bytes`), rejected draws
  included. The cap on staged bytes (`stage_bytes`) is checked on the accepted samples, after the search and
  before any voxel is written. Reading for a rejected draw is a transfer beyond what the staged crops touch;
  it is the price of the filters and is bounded by the same cap as every other read of the job. A reader
  error or an exceeded cap during the search fails the job as it does today.
- **Where the filters live.** One module in the harness maps a filter's name to a function of (raw box or
  label boxes, parameters) → pass or fail. `content` calls `content_ok`.

### 5.5 What is recorded, and what a shortfall does

- **stage.json**, per written volume: for each split the crops wanted, the samples staged, the draws taken
  and the rejections by reason (`repeat`, and each filter's name). A volume that accepted nothing is not in
  `volumes` (`check-stage` compares that list with the directories on disk, and registration refuses a sparse
  volume without samples); its counts go under a key of their own. Written only when the rule has filters.
- **The volume itself** gains nothing: the rule, filters included, is already recorded as the staging step's
  parameters in its provenance.
- **`check-stage`** is unchanged: it checks coverage and geometry of the samples that were staged.
- **A short volume weighs less in training.** *Call, open (§9.1).* When the rule has filters, registration
  multiplies a volume's weight by staged / wanted for the split's crops (`register.volume_bodies`, from the
  counts in stage.json). Without that, `sampler.draw` picks a volume by its unchanged weight and then one of
  its samples: a volume that kept 3 of 128 `train` crops would serve each about forty times as often. With
  it, a crop of a short volume is served about as often as any other, and the record's part of training
  shrinks with what it holds. `val` and `test` need nothing: their frozen lists run out (`draw_frozen`,
  `capacity`), so a pool that holds fewer samples than `sampling.crops` asks for gets a shorter list.
- **The timeline says who came up short.** *Call, open (§9.2).* The result entry of the staging pass lists
  each record that staged fewer crops than its rule asked for, with staged / wanted per split and the
  rejections by filter. The counts reach the worker in stage.json, which it already reads; a field an
  activity result needs for them is added with a default. No new column on the page.

### 5.6 Planning is an upper bound

- `_planned`, the quota check and the bytes estimate are unchanged. With filters set, a record's planned
  crops are the most it can stage, not what it will.
- Planned crops are shown in the plan's text only (the plan table's cell, `_planned_cell`, and the summary
  line). For a definition with filters both read **"up to N"**. The page's `split` columns show items per
  split, not crops, and stay as they are.
- The planner's policy errors (`_policy_errors`: a split the pool cannot serve) keep using the expectation.
- **A split the filters emptied adds no workflow logic.** *Call.* Auto-approve keeps looking at the datasets
  servable per weighting group; the staged board shows a person the volumes per split, and the timeline
  entry of §5.5 names the short records. A buffer approved with an empty split serves nothing for it, as one
  does today whose pool holds no plane of that split.

### 5.7 The page

- The designer's sampling controls gain two: a checkbox **"Skip blank crops"** (`content`) and a checkbox
  with a percentage **"At least [5] % annotated"** (`labelled`, `min_fraction` = percent / 100).
- `spec.js` writes the `filters:` line only when one is set, and reads it back into the controls. A
  definition with a filter or parameter the controls do not show makes them read-only through the existing
  mechanism (`LOST`), and the YAML tab stays the way to edit it. That mechanism compares a list of objects in
  order, so the parsed definition holds its filters in name order, and a percentage is rounded on the way
  back (0.07 × 100 is not 7 in floats).

### 5.8 Documents

`docs/development.md` (the plan rule, the stage job), the artifact-id rule of `AGENTS.md` (filters are part of
the plan digest; a stage pass may read more than it stages), the harness's `SPEC.md` and README where they
describe `stage`.

## 6. Constraints

- **Two repositories, harness first.** A worker that sends `filters` to a harness without them fails every
  stage job of that buffer; a harness with them and a worker without changes nothing.
- **The plan rule is mirrored.** Both sides hash the same canonical JSON; a test on each side pins the digest
  of one rule without filters (unchanged from today) and one with.
- **Determinism.** The same rule, binding, harness and data give the same samples. Nothing in the search
  depends on time, order of volumes or cache state.
- **No workflow change.** The rule travels in the plan file to the stage job's workspace; no workflow issues a
  new command, so no replay fixture is re-recorded.
- **Tests.** Harness: the selection (accepts, rejects per filter, label read before raw, giving up, a volume
  without labels, a larger count extending a smaller, no filters = today's list). Workflows: the grammar, the
  rule and its digest, the "up to" wording, a short volume's weight, the timeline's line for a short record,
  and the page's controls round-tripping through `spec.js`.

## 7. Left to the builder

The module and function names; the field names in stage.json; the value of `GIVE_UP` if a measurement on a
real record argues for another; how the search shares reads with the writing that follows (a reader's own
cache may or may not make the second read free; a reader that hands back whole planes wants its last plane
kept, as the crop job does); the exact wording on the page and in the timeline entry.

## 8. Later

- A coarse scan of the label, so that a sparse volume costs one pass and not thousands of draws.
- More filters: noise, background, "interesting" data. Each is an entry and a function.
- The live record: after the harness and the worker are deployed, a buffer with both filters set is run on the
  incident's dataset and its stage.json is read: four `val` samples on tissue, and no `train` sample under
  5 % annotated.

## 9. Open decisions

1. **Does a short volume weigh less in training?** §5.5 says yes, by staged / wanted, only when the rule has
   filters. The other answer is to leave weights alone and accept that a short volume's crops repeat.
2. **Where does a shortfall show?** §5.5 says in the staging pass's result entry on the timeline, and in
   stage.json. The other answer adds staged / wanted crops per dataset to the page's board, which is a slice
   of its own (a read from buffer-api, a column, its tests).
