# A stage reads on demand — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A buffer round stages a record by reading what its planned crops touch, with no whole-file download
and no quota charge for the record's size upstream.

**Architecture:** `harness stage` stops fetching every binding file before it reads, so its readers decide what
to transfer, as in a crop job. `MiaBuffer` loses its fetch phase outright and runs the stage job online behind
a quota wait that looks only at what the raw cache holds. The raw row is written by the stage and by the scan
of the roots, and the stage job's reservation is settled after its report is read.

**Tech Stack:** Python, Temporal (replay fixtures), pytest, the page's ES modules.

**Spec:** `docs/specs/2026-11-20-stage-on-demand.md` (v0.3).

## Global Constraints

- **No backward compatibility** (the user's decision, 2026-11-20): no `workflow.patched` for this change, no
  second path, no kept wire fields. What the fetch phase needed is deleted; the buffer replay fixtures are
  recorded again. Patches and fields that belong to other changes are left alone.
- Two repositories. Tasks 1 and 3 to 5 are in mia-workflows, in the worktree
  `.claude/worktrees/stage-on-demand` (branch `stage-on-demand`). Task 2 is in mia-snippets: a new worktree
  and branch `stage-on-demand` cut from `origin/main` of the developer checkout (its `main` is behind).
- Run only the tests that cover a change, never the full suite, until the last step of Task 5.
- No real paths or hostnames in tracked files: the check in AGENTS.md comes back empty.
- Comments and docs say what the code does, never what it replaced.
- Commit each task when its tests pass; stage explicit paths; never push.

## Review Focus

1. **Two jobs pass the quota check together.** The cache goes over by what each takes whole; the next dataset
   to reach a stage slot is checked again and waits. Test in Task 4 (the second check).
2. **A stage that dies with no report.** Dataset failed, reservation closed with 0, raw row brought up to date
   by the scan. Tests in Tasks 3 and 4.
3. **A whole-file fallback that fails part way.** No cache entry; the failed stage's raw row is `ready` with
   the bytes on disk. Tests in Tasks 2 and 3.
4. **Raw rows left `partial` or `fetching`.** The scan makes them `ready`, so the keeper can take them. Test
   in Task 3.
5. **A hand-dropped record.** Offline, read-only, no quota wait, and `RAW_GONE` on a rebuild once its files
   left. Tests in Task 4.

---

### Task 1: The quota looks at the cache, not at the record (mia-workflows)

**Files:**
- Modify: `mia_buffers/activities.py` (`check_quota`, about line 1310; the keeper's `plan_evictions` call,
  about line 1797; `OVER_QUOTA` wording if it speaks of a fetch)
- Modify: `mia_buffers/keeper.py` (delete `demand` and `plan_evictions`' `demand_bytes`)
- Modify: `mia_buffers/models.py` (`QuotaCheck`: `ok`, `over_by`; `expected` goes)
- Test: `tests/test_buffers_activities.py` (`test_check_quota`, line 952), `tests/test_buffers_keeper.py`

**Interfaces:**
- Produces: `check_quota(round_id: str, record: str) -> QuotaCheck`, `QuotaCheck(ok: bool, over_by: int = 0)`;
  `ok` False only while the raw cache's registry total is over `quota.raw_bytes`, `over_by = total - limit`;
  nothing written on the record's row. `plan_evictions(assets, *, quota_bytes, headroom_bytes, hold_s, now,
  in_round, kind, quarantine_s=0)`.

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

- [ ] **Step 2: Run, expect FAIL** — `pixi run pytest tests/test_buffers_activities.py -k "check_quota or larger_upstream" -q`
- [ ] **Step 3: Change `check_quota`** to `over_by = int(listing["bytes"]) - limit` and
  `QuotaCheck(over_by <= 0, max(over_by, 0))`; drop the `_row` read, the own-row arithmetic and the
  `mark_asset` call. Docstring: whether the raw cache is within `quota.raw_bytes` (0 is no limit); a
  RegistryError propagates.
- [ ] **Step 4: Keeper test** — delete the `demand` assertions and import in `tests/test_buffers_keeper.py`
  and add, shaped on the eviction test at line 42:

```python
def test_a_row_marked_wanted_asks_the_keeper_for_nothing():
    rows = [...]  # one row carries data {"wanted": 3e12, "blocked_at": now}
    picked = plan_evictions(rows, quota_bytes=100, headroom_bytes=10, **rules)
    assert live_bytes(rows) - live_bytes(picked) <= 90          # down to the quota minus the headroom
    assert len(picked) < len(free_assets(rows, **rules))        # and no further
```

- [ ] **Step 5: Delete `demand` and `demand_bytes`**; `plan_evictions` evicts until under
  `quota_bytes - headroom_bytes`.
- [ ] **Step 6: Run** `pixi run pytest tests/test_buffers_activities.py tests/test_buffers_keeper.py -q` — PASS.
- [ ] **Step 7: Commit** — `mia-buffers quota: a record waits only while the raw cache is over its quota`

### Task 2: `harness stage` reads on demand (mia-snippets)

**Files:**
- Modify: `harness/src/mia_harness/stage.py` (delete `prefetch` and its call in `stage`; `stage`'s docstring)
- Modify: `SPEC.md` and the harness docs wherever they say a stage fetches every source first (grep
  `prefetch`, "Fetch every source")
- Test: `harness/tests/test_stage.py`, `harness/tests/rangeserver.py` if it lacks the switches below

**Interfaces:**
- Produces: `stage(...)` unchanged in signature. Its report's `bytes_fetched` is what the readers
  transferred, in a failed report too.

- [ ] **Step 1: Write the tests** (with the file's `io`, `make_io`, `server` fixtures)

```python
def test_stage_transfers_what_its_crops_touch_and_keeps_no_whole_file(tmp_path, io, server):
    # a source of at least 8 MiB read by range (BLOCK is 1 MiB) and a plan of one crop
    rep = stage(...)
    assert rep["ok"] and 0 < rep["bytes_fetched"] < source_bytes
    assert io._local(url) is None                      # no whole file in the dataset cache


def test_stage_takes_a_file_whole_where_ranges_will_not_do(tmp_path, io, server):
    # the server ignores Range for this url: the reader falls back to io.fetch
    rep = stage(...)
    assert rep["ok"] and io._local(url) is not None
    assert url in json.loads((cache_dir / "files.json").read_text())


def test_a_whole_file_that_fails_part_way_leaves_no_cache_entry(tmp_path, io, server):
    # the server ignores Range and cuts the body short on every attempt
    rep = stage(...)
    assert not rep["ok"] and rep["errors"] and io._local(url) is None
    assert not [p for p in cache_dir.iterdir() if p.name.startswith(".tmp-")]


def test_an_offline_stage_reads_the_cache(tmp_path, make_io, server):
    fetch(binding, record, make_io())                  # `harness fetch`, by hand
    gets = len(server.gets())
    offline = make_io(hosts=())
    rep = stage(binding, ..., offline, ...)
    assert rep["ok"] and offline.bytes_fetched == 0 and len(server.gets()) == gets
```

The last replaces `test_prefetch_expands_member_globs_so_an_offline_stage_reads_the_cache` (line 315) and
keeps its `<zip>::<glob>` binding. Fix the two assertions that expect every source fetched once (lines 74
and 127): `0 < bytes_fetched <= source_bytes`. Patch `time.sleep` in the failing-download test so the retry
waits cost nothing.

- [ ] **Step 2: Run, expect FAIL** — the repository's harness test command on `tests/test_stage.py` (its
  README; from `harness/`).
- [ ] **Step 3: Delete `prefetch` and its call.** `stage`'s docstring: open the arrays, share the record's
  crops among its raws, plan, refuse a plan over `stage_bytes` before any voxel is written, then write; each
  read goes where the reader sends it (`io.open` by range, `io.fetch` whole).
- [ ] **Step 4: Run** `tests/test_stage.py`, `tests/test_fetch.py`, `tests/test_io.py` — PASS.
- [ ] **Step 5: Commit** — `harness stage: the readers decide what is transferred`

### Task 3: The raw row is the stage's and the scan's (mia-workflows)

**Files:**
- Modify: `mia_buffers/activities.py` (`_record_assets`, about line 1231: the raw row's state)
- Modify: `mia_buffers/ledger.py` (`sync_assets`, line 1210: `becomes_ready`; the state list at line 36 and
  the skip list at line 1216 lose `fetching`)
- Modify: `mia_buffers/keeper.py` (`BUILDING`, line 13; the stale-job rule at line 115 if it names `fetching`)
- Test: `tests/test_buffers_activities.py` (beside `test_read_stage_records_the_staged_and_raw_assets`, line
  869), `tests/test_buffers_ledger.py` (beside line 209)

**Interfaces:**
- Produces: after `read_stage` of any readable report, the record's raw row holds `path`, `bytes`, `files`,
  `last_used` and `state == "ready"`. `sync_assets` sets a raw row found on disk to `ready` when it has no
  row or its row is at none, `missing`, `evicted`, `partial` or `fetching`.

- [ ] **Step 1: Write the tests**

```python
async def test_read_stage_of_a_failed_stage_leaves_the_raw_row_ready_with_its_bytes(acts, server):
    ...  # a failed stage.json; 3 bytes of a whole file and a files.json entry in the record's cache
    await acts.read_stage(ROUND, U937)
    row = raw_row(server, U937)
    assert row["state"] == "ready" and row["bytes"] > 0 and U937_URL in row["files"]


def test_sync_assets_readies_a_raw_row_left_partial_or_fetching(tmp_path):
    ...  # raw rows "a" at partial and "b" at fetching, both directories on disk with a file
    ledger.sync_assets(found, ...)
    assert ledger.asset("raw", "a")["state"] == ledger.asset("raw", "b")["state"] == "ready"


def test_sync_assets_brings_a_raw_row_up_to_date_after_a_stage_with_no_report(tmp_path):
    ...  # row with bytes 3 and an old scanned_at; the scan finds 10 bytes
    assert ledger.asset("raw", "a")["bytes"] == 10 and ledger.asset("raw", "a")["state"] == "ready"


def test_a_staged_row_at_partial_stays_partial(tmp_path):   # a failed build is not an artifact
    ...
```

- [ ] **Step 2: Run, expect FAIL** — `pixi run pytest tests/test_buffers_activities.py -k "raw_row_ready" tests/test_buffers_ledger.py -k sync_assets -q`
- [ ] **Step 3: Implement.** `_record_assets` writes `raw["state"] = "ready"` in place of deleting the state;
  its docstring says the raw cache is recorded as it is on disk, at rest. `sync_assets`: `becomes_ready` also
  for a raw row at `partial` or `fetching`. `BUILDING` is `("staging",)`.
- [ ] **Step 4: Run** the two test files — PASS.
- [ ] **Step 5: Commit** — `mia-buffers raw: a raw cache at rest is ready, whoever wrote it`

### Task 4: The round has no fetch phase (mia-workflows)

**Files:**
- Modify: `mia_buffers/workflows.py`: `_source_locked` (line 1420), `_quota` (line 1563), `_job` (line 1722),
  the rebuild check (line 1144), the three `fetch_slots` semaphores (lines 765, 839, 1153) and every
  signature that passes one; delete `_fetch` and `_fetch_jobs`; docstrings of `_source_cached` and the
  module's header where they tell the flow
- Modify: `mia_buffers/activities.py`: delete `prepare_fetch`, `submit_fetch`, `read_fetch`, the `raw_status`
  activity (keep what `raw_ready` needs as a private helper), the limits at lines 590 to 592, and their
  registrations in the worker
- Modify: `mia_buffers/jobspec.py` (delete `fetch_job`), `mia_buffers/models.py` (delete `FetchJobArgs`,
  `FetchResult`, `RawStatus` if nothing else uses it, the three fetch fields at lines 219 to 221, `fetching`
  in the status list at line 242), `mia_buffers/config.py:152` (`parallel_fetch`), `mia_data/config.py:199-201`
  (`fetch_bytes`, `fetch_walltime_min`, `fetch_attempts`) and whatever key builder made a fetch job's key
- Test: `tests/test_buffers_workflows.py`, `tests/test_buffers_jobspec.py`, `tests/test_buffers_config.py`,
  `tests/test_buffers_activities.py`; fixtures `tests/fixtures/buffers/buffer_history*.json`

**Interfaces:**
- Consumes: `check_quota` and `QuotaCheck` of Task 1; the raw row of Task 3.
- Produces: the sourcing of one record under its raw lock (spec §5.2, §5.3, §5.5):

```
binding = read_binding
manual: stage offline (hosts [], cache_writable False), no quota wait
else, repeat:
    _quota(dataset, owner)            -> False: dropped, return
    waited = semaphore.locked()
    with the stage slot:
        _retake(dataset, owner)
        if waited and not check_quota.ok: give the slot back, repeat
        stage online (hosts binding.hosts, cache_writable True)
register
```

  `_quota` sets the reason `raw cache over its quota by {over_by} bytes` and says once: "`<record>` is
  blocked: <reason>. It is staged once the keeper frees space or the quota is raised."

  The stage job's reservation is closed exactly once, by the code that submits the job: with the report's
  `bytes_fetched` when the job ended `DONE` or `EXIT` and `read_stage` returned (a failed report included),
  with 0 on every other exit (another job state, a cancel, an activity error).

  The rebuild check asks `raw_ready` and gives `RAW_GONE` only for `dataset.manual`.

- [ ] **Step 1: Write the live tests**

```python
async def test_a_round_stages_online_with_no_fetch(env):
    m = Mocks(plan=ONLY_U937)
    ...  # go, ready
    args = m.of("submit")[0][1]                              # StageJobArgs
    assert args.hosts == U937_HOSTS and args.cache_writable is True
    assert "fetching" not in statuses_seen(m, "u937")


async def test_a_dropped_record_is_staged_offline_without_a_quota_wait(env):
    ...  # bcss, dropped by hand; check_quota would say over
    assert args.hosts == [] and args.cache_writable is False and status(m, "bcss") == "servable"


async def test_a_record_waits_while_the_cache_is_over_quota_then_stages(env):
    m = Mocks(plan=ONLY_U937, quota=[QuotaCheck(False, over_by=10), QuotaCheck(True)])
    ...
    assert blocked_reason(m, "u937") == "raw cache over its quota by 10 bytes" and m.of("submit")


async def test_a_dataset_that_waited_for_a_stage_slot_is_checked_again_and_waits_again(env):
    # limits.parallel 1, two records: the first holds the slot; when the second gets it the cache is over
    m = Mocks(quota={"second": [QuotaCheck(True), QuotaCheck(False, over_by=5), QuotaCheck(True)]})
    ...
    assert blocked_reason(m, "second") == "raw cache over its quota by 5 bytes"
    assert len(m.of("submit")) == 2                          # and it staged once the cache had room


async def test_a_blocked_dataset_leaves_its_stage_slot_to_others(env): ...   # from the fetch-slot test, line 1413


async def test_a_stage_over_its_byte_cap_fails_the_dataset_with_the_harness_message(env):
    m = Mocks(plan=ONLY_U937, staged=StageResult(False, errors=["<url>: 6000000000 more bytes would pass the cap"]))
    ...
    assert "would pass the cap" in failed_reason(m, "u937")


async def test_a_stage_reservation_is_settled_once_with_what_the_report_says(env):
    m = Mocks(plan=ONLY_U937, staged=StageResult(False, errors=["x"], bytes_fetched=1234))   # a failed report
    ...
    assert [call[3] for call in m.of("settle")] == [1234]    # (root, key, usd, fetched_bytes)


async def test_a_stage_killed_with_no_report_fails_the_dataset_and_settles_zero(env):
    m = Mocks(plan=ONLY_U937, jobs={"u937": JobDone(state="KILLED")})
    ...
    assert status(m, "u937") == "failed" and not m.of("read_stage")
    assert [call[3] for call in m.of("settle")] == [0]


async def test_a_rebuild_does_not_ask_for_the_raw_cache(env):
    ...  # as the d_rebuild scenario, raw_ready False
    assert not m.of("raw_ready") and status(m, "u937") == "servable"


async def test_a_rebuild_of_a_dropped_record_without_its_files_is_raw_gone(env):
    ...  # manual record, raw_ready False
    assert failed_reason(m, "bcss") == RAW_GONE
```

Use the helpers the file has (`Mocks`, `started`, `approve`, `ready`, `until`); the accessor names above
stand for whatever it already uses to read statuses, reasons and a mock's calls. Add to the ledger's tests,
if it has none, that a second `settle` of a closed reservation leaves `fetched_bytes` unchanged.

- [ ] **Step 2: Sort the existing tests.** Delete those whose subject is the fetch job (lines 1233, 1259,
  1272, 1292: fetch then offline stage, stream urls, partial fetch, host not allowed in a fetch) and the
  activity, jobspec and config tests of what is deleted. The blocked, timeout and go-while-blocked tests
  (1304, 1359, 1373, 1463) lose their fetch expectations; 2279 becomes the settle test above; 2518 loses
  "fetched" from its name.
- [ ] **Step 3: Run, expect FAIL** — `pixi run pytest tests/test_buffers_workflows.py -k "not replay" -q`
- [ ] **Step 4: Implement** the Interfaces block and the deletions. Then `grep -rn "fetch_slots\|submit_fetch\|read_fetch\|prepare_fetch\|fetch_attempts\|parallel_fetch\|FetchResult\|FetchJobArgs" mia_buffers mia_data mia_core tests`
  finds nothing, and `grep -rn '"fetching"' mia_buffers --include=*.py` finds nothing.
- [ ] **Step 5: Record the fixtures again** — drop `raw=RawStatus(...)` from the recording scenarios (lines
  1038, 1068 and the `d` ones) and reword their docstrings to the flow as it is, then
  `MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay`. The prep
  fixtures (`prep_history*.json`) are recorded by the same command; `git diff --stat` on them should show
  them unchanged in substance, and if a prep replay now differs, find out why before keeping it.
- [ ] **Step 6: Run** `pixi run pytest tests/test_buffers_workflows.py tests/test_buffers_activities.py tests/test_buffers_jobspec.py tests/test_buffers_config.py -q`
  and `pixi run typecheck` — PASS.
- [ ] **Step 7: Commit** — `mia-buffers: a round stages online behind the quota wait; the fetch phase is gone`

### Task 5: The page and the docs say what happens (mia-workflows)

**Files:**
- Modify: `mia_buffers/explore.py:404` (drop `bytes_to_fetch`), `tests/test_buffers_explore.py:120`
- Modify: `mia_buffers/ui/designer.js:522-523`, `mia_buffers/ui/dom.js:129` and the `staging`/`blocked`
  entries beside it, `mia_buffers/ui/flow.js:576`, `mia_buffers/ui/buffers.js:117,401`
- Modify: `mia_buffers/plan.py:72` (the comment on `bytes`: the record's size upstream)
- Modify: `AGENTS.md` (line 12 and the keeper and quota wording), `README.md:56`, `docs/development.md`, the
  `limits:` and `quota:` blocks of the example `mia.yaml`
- Test: `tests/test_buffers_explore.py`, `tests/test_buffers_ui.py`

- [ ] **Step 1: Explore summary** — the test asserts `"bytes_to_fetch" not in summary`; remove the key.
- [ ] **Step 2: Designer line** — `bytes_pool / bytes_fitting` under the caption "size upstream: the
  prepared datasets now / every fitting dataset".
- [ ] **Step 3: Help texts** — `fetching` is removed. `staging`: "A stage job reads what the planned crops
  need from the source and writes the pyramid." `blocked`: "The raw cache is over its quota; the dataset is
  staged once the keeper frees space or the quota is raised." The datasets tables' captions list "staging,
  blocked by quota" and no fetching.
- [ ] **Step 4: Docs** — AGENTS.md "How the pieces meet": a buffer plans over prepared rows only and stages
  one sparse pyramid per volume, the stage job reading from the source what the planned samples touch; a
  file its reader takes whole lands in the shared raw cache. Its quota rule: the quota is where staging
  waits and the keeper evicts, not a ceiling. README and `docs/development.md` likewise. The `mia.yaml`
  example: the four fetch limits go; `job_bytes` caps what a crop or stage job transfers.
- [ ] **Step 5: Run** `pixi run pytest tests/test_buffers_explore.py tests/test_buffers_ui.py -q`, then
  `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test` — clean.
- [ ] **Step 6: Commit** — `page and docs: a stage reads from the source; no fetch figure`

## Rollout (the user's to start; the builder pushes and deploys nothing)

As spec §7: cancel buffer-27716818's round now; harness first; before the mia-workflows deploy no buffer
round is sourcing or rebuilding and the live `mia.yaml` loses the four fetch limits; then merge, restart,
start the buffer again, and check the first round as §7.5 says.
