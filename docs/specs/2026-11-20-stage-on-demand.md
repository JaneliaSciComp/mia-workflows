# MIA: a stage reads on demand — Spec

**Status:** v0.1 · 2026-11-20 · not approved. The direction (§4, approach A) is the user's decision of
2026-11-20; §6 lists the calls that are the writer's and the user's to change. Rests on the training-buffers
spec, the sparse OME-Zarr staging spec and the prep-and-buffering spec, whose fetch phase (§5.2) and quota
estimate (§5.4, "the record's expected bytes") it replaces.

## 1. Intent

A buffer exists so that training gets a few hundred crops of a dataset without anyone downloading the dataset.
The pyramid a buffer stages is already sparse: it holds the chunks of the planned samples and nothing else. The
way to it is not. Before a record is staged, every file its binding names is downloaded whole, and before that
download starts the raw cache must have room for the whole record as the catalog sizes it.

The incident: buffer-27716818 left `aws-dharani-fetal-brain-nissl-annotated-sections` at `blocked`, "raw quota
over by 2942339611569 bytes". The record is 3.07 TB upstream (2,818 sections). Its binding names two TIFFs
(461 MB and 12.7 GB) and two GeoJSON files. Its reader takes a TIFF tile by tile with range requests; the plan
estimates 428 MB of staged crops. In that round 20 of the 129 planned records are larger upstream than the
room left in a 200 GB raw cache and 17 are larger than the whole quota: 53.5 TB upstream for a pool whose
planned crops are estimated at 8.5 GB.

## 2. Today

- **The quota check charges the record.** `check_quota` adds the plan row's `bytes`, the catalog's size of the
  whole record, to the raw cache's total. What a binding names is not looked at.
- **The fetch phase downloads the binding whole.** `harness fetch` puts every url of the binding into the raw
  cache in full (`limits.fetch_bytes` a job, `limits.fetch_attempts` jobs). The stage job then runs with no
  hosts and reads the cache.
- **`harness stage` does the same.** Its first step, `prefetch`, fetches every url of the binding whole; after
  a fetch phase each is a cache hit.
- **The readers already read on demand.** A snippet reads through `io.open` (range requests, kept as 1 MiB
  blocks in the job's workspace) and falls back to `io.fetch` (the whole file, into the raw cache) when ranges
  will not do: 95 of the 104 snippets handle `NeedsFetch` this way. mia-data's crop jobs run them like that,
  under `limits.job_bytes`, and every prepared dataset has had its crops made so.

So the whole-file download is something the buffer's path adds; no reader asks for it.

## 3. Goals

- A stage job transfers what the reads of its planned crops touch. A file comes whole only where its reader
  asks for it: a server that ignores Range, a deflated zip member, a format with no random access.
- No record is blocked, dropped or refused for its size upstream.
- The raw quota guards what lands in the raw cache, and nothing else.

**Non-goals**

- Keeping range blocks from one job to the next. A rebuild, and a later buffer of the same record, read
  upstream again.
- Changing a reader, or teaching one to read less.
- A download that continues over several jobs. What a stage job needs it gets within its own byte cap.
- mia-data's crop jobs, which stay as they are.

**Anti-goals**

- An estimate of a download nobody makes deciding what a round does.
- A stage that quietly downloads everything after all. What a stage job transferred is on its record
  (`bytes_fetched` in stage.json), and a file taken whole is in the raw cache's `files.json`.

## 4. Approaches

- **A. A stage reads on demand, as a crop job does** (picked). The fetch phase goes; the stage job has the
  binding's hosts and the readers decide what to transfer. One path for every job that runs a reader.
- **B. Keep the fetch phase, charge the binding's files.** Unblocks the incident (13 GB fits) and still
  downloads 12.7 GB for a few tiles. The worker does not know a file's size before a fetch, so something has
  to record sizes first.
- **C. Keep the fetch phase for sources that cannot be read by range.** `harness fetch` probes each url and
  reports a range-readable one as `stream`, which the workflow already hands to the stage job to read in
  place. The workflow stays as it is, but a server that answers Range says nothing about how the reader
  reads, and two paths remain. Rejected.

## 5. Design

### 5.1 Harness (mia-snippets, deployed first)

`harness stage` no longer calls `prefetch`. It opens the binding's arrays and reads the planned regions, and
each read goes where the reader sends it: `io.open` to range blocks in the job's workspace, `io.fetch` to the
raw cache, with `files.json` recording the validators as today. A stage with no hosts (a record whose files a
person dropped, `manual://`) reads the cache as it does now, and a file that is not there is the error.

Nothing else in the harness changes: the byte cap, the host check on every redirect hop and the validators
are `IO`'s, and they apply to a stage job as to a crop job. The harness commit is part of an artifact id, so
a round after the deploy stages its volumes anew, as after any harness change.

### 5.2 The buffer round (`MiaBuffer`)

Behind a patch, so a round begun before it replays as recorded:

1. **No fetch phase.** A record goes from the quota wait (§5.3) to its stage slot. No `harness fetch` job is
   submitted and no dataset is at `fetching`.
2. **The stage job is online.** It gets `binding.hosts` and the record's raw cache read-write, which is what
   a record with `stream` urls gets today. A binding whose files a person dropped stays offline, read-only.
3. **Its byte cap is the job's.** `limits.job_bytes`, as for a crop job. A stage that would pass it fails the
   dataset with the harness's message, which names the url and the cap.
4. **A rebuild reads upstream.** A pyramid that vanished is staged again without asking whether the raw cache
   still holds the record's files; `RAW_GONE` remains for a record whose files a person dropped, which exist
   nowhere else.
5. **The budget counts what a stage transferred.** The job's `bytes_fetched` is settled against the root's
   `max_fetch_bytes`, as a fetch job's was.

The record's raw lock is held by the stage job as it is today, since the job may write the raw cache.

### 5.3 The raw quota

`check_quota` charges the record nothing ahead of time: a record waits at `blocked` only while the raw cache
is over `quota.raw_bytes`, and it writes no `wanted` on the raw row. The keeper frees the cache down to the
quota minus `quota.headroom_bytes` as it does now, which ends the wait. A stage job can add to the raw cache
at most its byte cap, and only when a reader takes a file whole; the headroom is what absorbs that, so it
should stay above `limits.parallel` × `limits.job_bytes`.

The plan row keeps `bytes` as the record's size upstream. Nothing decides by it.

### 5.4 The page

- The designer's line "N not in the raw cache" (the sum of the catalog sizes of records with no ready cache)
  goes. The catalog sizes stay, named as the datasets' size upstream.
- The help texts of `staging` and `blocked` say what happens now: the stage job reads what the planned crops
  need from the source; a record is blocked while the raw cache is over its quota. `fetching` stays in the
  status table for the rows of versions built before.

### 5.5 What becomes unused

`limits.fetch_bytes`, `limits.fetch_walltime_min`, `limits.fetch_attempts`, `limits.parallel_fetch`, the
fetch job spec and the fetch activities are read only by a round begun before the patch. They are removed
together with the unpatched path once no such round is open. `harness fetch` stays in the harness: it is how
a person fills a record's cache by hand.

## 6. The writer's calls

1. **The stage job's cap is `limits.job_bytes`** (5 GB in this deployment), with no limit of its own. A record
   whose planned crops need more than that over the network fails and says so; raising the limit is the
   remedy.
2. **Range blocks are not kept.** They live in the job's workspace and go with it. Crops are placed at
   random, so a later plan touches other blocks, and a rebuild of the same plan costs what the first stage
   cost.
3. **No download path for large whole-file sources.** Today a record can take 4 fetch jobs of 50 GB. After
   this a record whose reader must take more than the job's cap whole cannot be staged. Such a record is what
   the buffer is not for; if one matters, its reader is the thing to change.
4. **The quota charges nothing ahead.** An exact figure would need each file's size and how its reader reads
   it, known only once the job runs. The cap bounds the overshoot.
5. **A rebuild depends on upstream.** With no raw copy, a pyramid can be staged again only while the source
   answers. That is already so for a record whose raw cache the keeper evicted.
6. **`harness fetch` stays**, unused by the workflow.

## 7. Rollout

1. mia-snippets: `harness stage` without `prefetch`, merged and the live harness checkout updated.
2. mia-workflows: §5.2 to §5.4, merged, workers restarted.
3. A round begun before step 2 keeps its recorded path: buffer-27716818 is cancelled and started again, and
   the record is staged by the new path.

## 8. Left to the builder

The patch's name and the replay fixtures; whether the quota wait keeps its place in `_fetch` or moves beside
the stage slot; the wording of the timeline and help texts; when the unpatched path and §5.5 are removed; the
tests, one of them named after this incident: a record far larger upstream than the quota is staged.
