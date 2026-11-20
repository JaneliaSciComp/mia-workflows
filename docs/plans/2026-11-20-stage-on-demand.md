# A stage reads on demand — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A buffer round stages a record by reading what its planned crops touch, with no whole-file download
and no quota charge for the record's size upstream.

**Architecture:** `harness stage` stops fetching every binding file before it reads, so its readers decide what
to transfer, as in a crop job. `MiaBuffer` decides once per round, at admission, whether the round is on
demand; such a round has no fetch phase and runs its stage job online. `check_quota` looks only at what the
raw cache holds now.

**Tech Stack:** Python, Temporal (`workflow.patched`, replay fixtures), pytest, the page's ES modules.

**Spec:** `docs/specs/2026-11-20-stage-on-demand.md` (v0.2, approved 2026-11-20).

## Global Constraints

- Two repositories. Tasks 1, 3 and 4 are in mia-workflows, in the worktree `.claude/worktrees/stage-on-demand`
  (branch `stage-on-demand`). Task 2 is in mia-snippets: a new worktree and branch `stage-on-demand` cut from
  `origin/main` of the developer checkout (which is behind origin; do not build on its `main`).
- Run only the tests that cover a change, never the full suite, until the last step of Task 4.
- Activity result dataclasses in `mia_buffers/models.py` are wire format: add fields with defaults, never
  rename or remove (`QuotaCheck.expected`, `FetchResult`, `RawStatus` stay).
- A round begun before the change replays as recorded: `buffer_history*.json` fixtures already in
  `tests/fixtures/buffers/` are not re-recorded and must keep passing.
- The fetch job spec, the fetch activities, `limits.fetch_*` and `limits.parallel_fetch` stay (spec §5.5): a
  round admitted before the change still uses them.
- No real paths or hostnames in tracked files: the check in AGENTS.md comes back empty.
- Comments and docs say what the code does, never what it replaced.
- Commit each task when its tests pass; stage explicit paths; never push.

## Review Focus

1. **A round in flight at the deploy.** It pinned a harness that still prefetches, so it must keep its fetch
   phase to the end, across replays and continue-as-new. Test in Task 3 (`on_demand` False runs the fetch).
2. **A reader that cannot use ranges.** The file comes whole into the raw cache and the raw row's bytes count
   toward the quota. Test in Task 2 (whole file in the cache) and Task 3 (`_record_assets` runs after an
   online stage, already covered by the servable assertions).
3. **A record whose files a person dropped.** Offline, read-only cache, and `RAW_GONE` on a rebuild when the
   files left. Tests in Task 3.
4. **A stage over its byte cap.** The dataset fails with the harness's message naming the url and the cap; no
   retry, no fetch. Test in Task 3.
5. **A raw row left with `wanted` by an earlier block** (the incident's row holds 3.07 TB). The keeper must
   not evict toward it. Test in Task 1.

---

### Task 1: The quota looks at the cache, not at the record (mia-workflows)

Deployable on its own: after a buffers worker restart the incident's round leaves `blocked` and the keeper
stops evicting toward a 3 TB demand.

**Files:**
- Modify: `mia_buffers/activities.py` (`check_quota`, about line 1310; the keeper's `plan_evictions` call,
  about line 1797)
- Modify: `mia_buffers/keeper.py` (delete `demand`; delete `plan_evictions`' `demand_bytes` parameter and its
  use)
- Test: `tests/test_buffers_activities.py` (`test_check_quota`, line 952), `tests/test_buffers_keeper.py`

**Interfaces:**
- Produces: `check_quota(round_id, record) -> QuotaCheck` with `ok` False only while the raw cache's total is
  over `quota.raw_bytes`; `over_by = total - limit`; `expected` always 0; no `wanted`/`blocked_at` written.
  `plan_evictions(assets, *, quota_bytes, headroom_bytes, hold_s, now, in_round, kind, quarantine_s=0)`.

- [ ] **Step 1: Rewrite the quota test and add the incident's**

```python
async def test_check_quota(acts, server):
    # quota 0: no limit
    assert await acts.check_quota(ROUND, U937) == QuotaCheck(True)
    # the cache within its quota: ok whatever the record's size upstream, and nothing written on its row
    ...  # quota 100, other rows 90, the plan row's bytes 20
    assert await acts.check_quota(ROUND, U937) == QuotaCheck(True)
    assert "wanted" not in raw_row(server, U937) and "blocked_at" not in raw_row(server, U937)
    # the cache over its quota: blocked by the excess
    ...  # other rows 110
    assert await acts.check_quota(ROUND, U937) == QuotaCheck(False, over_by=10)


async def test_a_record_far_larger_upstream_than_the_quota_is_not_blocked(acts, server):
    """buffer-27716818: a 3.07 TB record against a 200 GB quota with 77 GB in the cache."""
    ...  # quota.raw_bytes 200e9, raw rows summing 76_974_997_628, the plan row's bytes 3_065_477_030_677
    assert await acts.check_quota(ROUND, U937) == QuotaCheck(True)
```

Keep the existing case that a `RegistryError` propagates. Use the fixtures the current test uses for the
quota, the rows and the plan row; `raw_row` is whatever the file already uses to read a raw asset row.

- [ ] **Step 2: Run them, expect FAIL** — `pixi run pytest tests/test_buffers_activities.py -k "check_quota or larger_upstream" -q`
- [ ] **Step 3: Change `check_quota`** to `over_by = int(listing["bytes"]) - limit`, return
  `QuotaCheck(over_by <= 0, max(over_by, 0))`, drop the `_row` read, the own-row arithmetic and the
  `mark_asset` call. Docstring: whether the raw cache is within `quota.raw_bytes` (0 is no limit); a
  RegistryError propagates.
- [ ] **Step 4: Keeper test** — in `tests/test_buffers_keeper.py` delete the `demand` assertions and import,
  and add:

```python
def test_a_row_marked_wanted_by_an_earlier_block_asks_for_nothing():
    rows = [...]  # as the file's eviction test builds them, one row with data {"wanted": 3e12, "blocked_at": now}
    picked = plan_evictions(rows, quota_bytes=100, headroom_bytes=10, **rules)
    assert live_bytes(rows) - live_bytes(picked) <= 90  # down to the quota minus the headroom
    assert len(picked) < len([row for row in rows if is_free(row)])  # and no further
```

Shape it on the test at line 42; the point is that the evictions stop at quota minus headroom.

- [ ] **Step 5: Delete `demand` and `demand_bytes`** from `keeper.py` and the keeper's call in
  `activities.py`. `plan_evictions` evicts until under `quota_bytes - headroom_bytes`.
- [ ] **Step 6: Run** `pixi run pytest tests/test_buffers_activities.py tests/test_buffers_keeper.py -q` and
  `pixi run pytest tests/test_buffers_workflows.py -k "blocked or quota" -q` — PASS (the workflow tests mock
  `check_quota`, so they are untouched).
- [ ] **Step 7: Commit** — `mia-buffers quota: a record waits only while the raw cache is over its quota`

### Task 2: `harness stage` reads on demand (mia-snippets)

**Files:**
- Modify: `harness/src/mia_harness/stage.py` (delete `prefetch` and its call in `stage`; `stage`'s docstring)
- Modify: `SPEC.md` and `harness/` docs wherever they say a stage fetches every source first (grep `prefetch`,
  "Fetch every source")
- Test: `harness/tests/test_stage.py`

**Interfaces:**
- Produces: `stage(...)` unchanged in signature. Its report's `bytes_fetched` is what the readers transferred.

- [ ] **Step 1: Write the tests** (in `test_stage.py`, with the file's `io`, `make_io`, `server` fixtures)

```python
def test_stage_transfers_what_its_crops_touch_and_keeps_no_whole_file(tmp_path, io, server):
    # a source of at least 8 MiB read by range (BLOCK is 1 MiB) and a plan of one crop
    rep = stage(...)
    assert rep["ok"] and 0 < rep["bytes_fetched"] < source_bytes
    assert io._local(url) is None   # no whole file in the dataset cache


def test_stage_takes_a_file_whole_where_ranges_will_not_do(tmp_path, io, server):
    # the server ignores Range for this url: the reader falls back to io.fetch
    rep = stage(...)
    assert rep["ok"] and io._local(url) is not None
    assert url in json.loads((cache_dir / "files.json").read_text())


def test_an_offline_stage_reads_the_cache(tmp_path, make_io, server):
    fetch(binding, record, make_io())              # `harness fetch`, by hand
    gets = len(server.gets())
    offline = make_io(hosts=())
    rep = stage(binding, ..., offline, ...)
    assert rep["ok"] and offline.bytes_fetched == 0 and len(server.gets()) == gets
```

The third replaces `test_prefetch_expands_member_globs_so_an_offline_stage_reads_the_cache` (line 315) and
keeps its `<zip>::<glob>` binding. If `rangeserver.py` cannot ignore Range per url, add that switch to it.
Fix the two assertions that expect every source fetched once (lines 74 and 127): the stage transfers at most
the source, `0 < bytes_fetched <= source_bytes`.

- [ ] **Step 2: Run, expect FAIL** — the repository's harness test command on `tests/test_stage.py` (see its
  README; from `harness/`).
- [ ] **Step 3: Delete `prefetch` and its call.** `stage`'s docstring: open the arrays, share the record's
  crops among its raws, plan, refuse a plan over `stage_bytes` before any voxel is written, then write; each
  read goes where the reader sends it (`io.open` by range, `io.fetch` whole).
- [ ] **Step 4: Run** `tests/test_stage.py`, `tests/test_fetch.py`, `tests/test_io.py` — PASS.
- [ ] **Step 5: Commit** — `harness stage: the readers decide what is transferred`

### Task 3: An on-demand round has no fetch phase (mia-workflows)

**Files:**
- Modify: `mia_buffers/models.py` (`BufferState`)
- Modify: `mia_buffers/workflows.py` (admission at line 631, the rebuild check at line 1144,
  `_source_locked`, `_quota`, `_job`; the docstring of `_source_cached`)
- Test: `tests/test_buffers_workflows.py`; new fixture `tests/fixtures/buffers/buffer_history_o.json`

**Interfaces:**
- Consumes: `check_quota` of Task 1.
- Produces: `BufferState.on_demand: bool = False`, set once per round where the pins are:
  `state.on_demand = workflow.patched("on-demand")` directly after the `admit` activity returns. Every other
  place reads `state.on_demand`; `workflow.patched("on-demand")` is called nowhere else.

Behaviour of a round with `on_demand` (spec §5.2):

1. `_source_locked` calls neither `raw_status` nor `_fetch`. For a binding that is not `manual` it calls
   `self._quota(dataset, owner)` and returns when that says the dataset was dropped.
2. The stage job gets `hosts=binding.hosts, cache_writable=True`; for a `manual` binding `hosts=[]`,
   `cache_writable=False`.
3. `_quota` words the block for such a round: reason `raw cache over its quota by {over_by} bytes`; comment
   "`<record>` is blocked: <reason>. It is staged once the keeper frees space or the quota is raised."
4. The rebuild check: `raw_ready` is asked, and `RAW_GONE` given, only for `dataset.manual`.
5. The stage job's reservation is closed with what it transferred: `_job` takes `settle: bool = True`; an
   on-demand call passes False and the caller closes it, with `staged.bytes_fetched` once `read_stage`
   returned and with 0 on every earlier exit (a job that did not end `DONE`/`EXIT`, an exception). A closed
   reservation ignores a second settle, so the reservation must still be open when the bytes are known.
6. No dataset of such a round is set to `fetching`.

- [ ] **Step 1: Write the live tests** (they run with the patch taken, as every non-replay test does)

```python
async def test_an_on_demand_round_stages_online_with_no_fetch(env):
    m = Mocks(plan=ONLY_U937)
    ...  # go, ready
    assert not m.of("submit_fetch") and not m.of("raw_status")
    args = m.of("submit")[0][1]                      # StageJobArgs
    assert args.hosts == U937_HOSTS and args.cache_writable is True
    assert "fetching" not in statuses_seen(m, "u937")


async def test_a_dropped_record_is_staged_offline(env):
    ...  # bcss, dropped by hand
    assert args.hosts == [] and args.cache_writable is False


async def test_a_record_waits_while_the_cache_is_over_quota_then_stages(env):
    m = Mocks(plan=ONLY_U937, quota=[QuotaCheck(False, over_by=10), QuotaCheck(True)])
    ...
    assert blocked_reason(m, "u937") == "raw cache over its quota by 10 bytes"
    assert not m.of("submit_fetch") and m.of("submit")


async def test_a_stage_over_its_byte_cap_fails_the_dataset_with_the_harness_message(env):
    m = Mocks(plan=ONLY_U937, staged=StageResult(False, errors=["<url>: 6000000000 more bytes would pass the cap"]))
    ...
    assert "would pass the cap" in failed_reason(m, "u937") and not m.of("submit_fetch")


async def test_the_stage_reservation_is_settled_with_what_it_transferred(env):
    m = Mocks(plan=ONLY_U937, staged=StageResult(True, volumes=1, bytes_fetched=1234))
    ...
    assert [call for call in m.of("settle") if call[3] == 1234]   # (root, key, usd, fetched_bytes)


async def test_a_rebuild_of_an_on_demand_record_does_not_ask_for_the_raw_cache(env):
    ...  # as the d_rebuild scenario, raw_ready False
    assert not m.of("raw_ready") and status(m, "u937") == "servable"


async def test_a_rebuild_of_a_dropped_record_without_its_files_is_raw_gone(env):
    ...  # manual record, raw_ready False
    assert failed_reason(m, "bcss") == RAW_GONE


async def test_a_round_admitted_before_on_demand_keeps_its_fetch_phase(env):
    ...  # start the workflow from a BufferState at stage plan-review with pins set and on_demand False
    assert m.of("submit_fetch")
```

Use the helpers the file has (`Mocks`, `started`, `approve`, `ready`, `until`); the accessor names above
stand for whatever it already uses to read statuses, reasons and a mock's calls.

- [ ] **Step 2: Sort the existing live tests of the fetch phase.** Delete those whose subject is the fetch job
  (lines 1233, 1259, 1272, 1292, 1413: fetch then offline stage, stream urls, partial fetch, host not allowed
  in a fetch, fetch slots). Adapt those whose subject survives: the blocked/timeout/go-while-blocked tests
  (1304, 1359, 1373, 1463) lose their fetch expectations; 2279 keeps the stage half; 2518 is renamed
  without "fetched". The recorded fixtures keep covering the fetch path.
- [ ] **Step 3: Run, expect FAIL** — `pixi run pytest tests/test_buffers_workflows.py -k "on_demand or dropped_record or over_quota or byte_cap or reservation or rebuild" -q`
- [ ] **Step 4: Implement** the six points above. `_source_cached`'s docstring says both shapes in the
  present tense: an on-demand round goes binding → (drop) → lock → quota wait → online stage → register →
  release.
- [ ] **Step 5: Run** `pixi run pytest tests/test_buffers_workflows.py -q` — PASS, the replay tests included
  (none re-recorded).
- [ ] **Step 6: Record the new fixture** — add `test_replay_buffer_history_o` beside the others (a `go` round
  of `ONLY_U937` to `ready`, written to `buffer_history_o.json`, docstring telling how to re-record it
  alone), then `MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay_buffer_history_o`
  and once more without the variable — PASS. `git status` shows no other fixture changed.
- [ ] **Step 7: Commit** — `mia-buffers: an on-demand round stages online and has no fetch phase`

### Task 4: The page and the docs say what happens (mia-workflows)

**Files:**
- Modify: `mia_buffers/explore.py:404` (drop `bytes_to_fetch`), `tests/test_buffers_explore.py:120`
- Modify: `mia_buffers/ui/designer.js:522-523`, `mia_buffers/ui/dom.js:129` and the `staging`/`blocked`
  entries beside it, `mia_buffers/ui/flow.js:576`, `mia_buffers/ui/buffers.js:117,401`
- Modify: `AGENTS.md` (line 12 and the quota/keeper wording), `README.md:56`, `docs/development.md`, the
  `limits:` and `quota:` comments of the example `mia.yaml`
- Test: `tests/test_buffers_explore.py`, `tests/test_buffers_ui.py`

- [ ] **Step 1: Explore summary** — the test asserts `"bytes_to_fetch" not in summary`; remove the key.
- [ ] **Step 2: Designer line** — `designer.js` shows `bytes_pool / bytes_fitting` under the caption
  "size upstream: the prepared datasets now / every fitting dataset".
- [ ] **Step 3: Help texts** — `staging`: "A stage job reads what the planned crops need from the source and
  writes the pyramid." `blocked`: "The raw cache is over its quota; the dataset is staged once the keeper
  frees space or the quota is raised." `fetching` keeps its text. The datasets tables' captions list
  "staging, blocked by quota" without "fetching with its job" for what a round does now.
- [ ] **Step 4: Docs** — AGENTS.md "How the pieces meet": a buffer plans over prepared rows only and stages
  one sparse pyramid per volume, the stage job reading from the source what the planned samples touch; a
  file its reader takes whole lands in the shared raw cache. README and `docs/development.md` likewise. The
  `mia.yaml` example: `job_bytes` caps what a crop or stage job transfers; `headroom_bytes` should stay above
  `limits.parallel` × `limits.job_bytes`.
- [ ] **Step 5: Run** `pixi run pytest tests/test_buffers_explore.py tests/test_buffers_ui.py -q`, then
  `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test` — clean.
- [ ] **Step 6: Commit** — `page and docs: a stage reads from the source; no fetch figure`

## Rollout (the user's to start; nothing here is pushed or deployed by the builder)

1. Task 1 alone can go live first (merge, restart the buffers worker): buffer-27716818's dharani leaves
   `blocked` and takes its recorded path, a whole-file fetch of the binding's 13 GB.
2. mia-snippets Task 2 merged and the live harness checkout updated.
3. mia-workflows Tasks 3 and 4 merged, workers restarted. A round admitted after that is on demand.
4. Check on the first on-demand round: no fetch job among its job records, `bytes_fetched` on a stage record
   far below the binding's file sizes, the raw cache not grown by a range-read record.
