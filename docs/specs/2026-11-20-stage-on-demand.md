# MIA: a stage reads on demand — Spec

**Status:** v0.4 · 2026-11-20 · v0.3 approved 2026-11-20 and built; v0.4 records what the review of the built
branch changed (§5.2.4, §5.4, §7.3) and is the user's to change. v0.2 was approved as it stood for now, with §9
owed later; v0.3 followed the user's decision that nothing here keeps compatibility with rounds or histories
from before it, and a review of v0.2. The direction (§4, approach A) and §9 are the user's decisions. Rests on
the training-buffers spec, the sparse OME-Zarr staging spec and the prep-and-buffering spec, whose fetch phase
(§5.2) and quota estimate (§5.4, "the record's expected bytes") it replaces.

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
are `IO`'s, and they apply to a stage job as to a crop job. A file taken whole is written beside its place and
renamed, so a download that fails part way leaves no entry. The harness commit is part of an artifact id, so
a round after the deploy stages its volumes anew, as after any harness change.

### 5.2 The buffer round (`MiaBuffer`)

The fetch phase is deleted, with no patch and no second path: a round that is sourcing when the change is
deployed is cancelled first and started again (§7).

1. **No fetch phase.** No `harness fetch` job, no `fetching` status, no fetch slots. A record goes binding →
   (drop) → raw lock → quota wait → stage slot → stage → register → release.
2. **The stage job is online.** It gets `binding.hosts` and the record's raw cache read-write. A binding
   whose files a person dropped is staged offline, the cache read-only, and has no quota wait: it adds
   nothing to the cache.
3. **Its byte cap is the job's.** `limits.job_bytes`, as for a crop job. A stage that would pass it fails the
   dataset with the harness's message, which names the url and the cap.
4. **A rebuild reads upstream.** A pyramid that vanished is staged again without asking whether the raw cache
   still holds the record's files. `RAW_GONE` remains where the binding is staged offline (files a person
   dropped: a `manual://` url, or a host the policy refuses), since those files exist nowhere else. The
   binding decides this, as it decides online or offline; the plan's mark of a record's download method does
   not.

### 5.3 The raw quota is where staging waits and the keeper evicts, not a ceiling

- **The check is about occupancy alone.** `check_quota` says whether the raw cache's total, as the registry
  has it, is over `quota.raw_bytes`. It charges the record nothing and writes nothing on its row. A dataset
  at `blocked` reads "raw cache over its quota by N bytes".
- **Where it is asked.** Before the stage slot, holding none; and again once the slot is taken after a wait,
  since other jobs ran meanwhile. Over again: the slot is given back and the dataset waits again. The
  record's lock is released while it waits, as now.
- **What the quota does not bound.** A stage job writes the raw cache only when a reader takes a file whole,
  and at most `limits.job_bytes` of it. Jobs that passed the check together, in this buffer or another, and
  mia-data's crop jobs, which write the same cache and ask nothing, can together take the cache past the
  quota by `limits.job_bytes` for each job running. The installation has no cap on that number and this
  spec adds none; the registry's total also lags what a running job has written. The keeper brings the cache
  back under the quota minus `quota.headroom_bytes` on its next pass, as far as rows are free.
- **When too little is free.** The keeper evicts a raw cache only when no buffer references it, no round of
  its record is open and it was unused for `quota.hold_hours` (§5.4 says which rows that now covers). While
  the rest still exceeds the quota, a blocked dataset stays blocked, and at the round's day limit it is
  dropped as any waiting dataset is, "still blocked after N days". That drop is about what the cache holds;
  no dataset is blocked or dropped for its size upstream. The remedy is the operator's: a larger
  `quota.raw_bytes`, or retiring buffers.
- **The keeper takes no demand.** It evicts to the quota minus the headroom and never further for a waiting
  record.

The plan row keeps `bytes` as the record's size upstream. Nothing decides by it.

### 5.4 The raw row belongs to whoever last wrote the cache

With no fetch phase nothing says a raw cache is `partial` or `fetching`, and nothing completes one.

- **After a stage job's report is read** (ok or failed), the record's raw row is written from the cache as
  it is: path, bytes, `files.json` entries, `last_used`, and the state `ready`, which for a raw cache means
  only that it is at rest on disk. mia-data's crop jobs write the row the same way
  (`mia_core.assets.raw_asset`), so no writer says `partial` of a raw cache.
- **A drop is marked just used.** When a person's files are verified into the cache the row becomes `ready`
  with `last_used` now: nothing else touches it before the stage, and the keeper's hold rule must spare files
  that exist nowhere else while the record waits for its slot.
- **After a stage with no report** (killed, cancelled, a worker restart before the read) nothing is written.
  The scan of the roots finds the directory, sums it again when it changed and merges the row; a directory
  with no row becomes `ready`.
- **Rows written before.** The scan sets a raw row it finds at `partial` or `fetching` to `ready`, so the
  keeper may evict it under its usual rules. (At the time of writing 39 of the 86 raw caches on disk are `partial`,
  62 GB of 77 GB, and none of them can be evicted.)
- **While a job writes** the record's lock is what keeps the keeper off, as now; the row's state says nothing
  about it.

A buffer keeps referencing the raw cache of every record it registered. A rebuild then finds what its
readers took whole, and such a cache is not evicted while the buffer lives.

### 5.5 The budget counts what a stage transferred

A stage job's reservation is closed once, after its report is read:

- the job ended `DONE` or `EXIT` and stage.json is readable: with its `bytes_fetched`, whether the stage
  succeeded or failed (the harness writes the count in both);
- no readable report, a job that ended any other way, a cancel, an activity error: with 0. A transfer
  nobody reported is not counted; `max_fetch_bytes` is a budget on reported transfers.

A closed reservation ignores a second settle, and a rebuild is a job with a key and a reservation of its own.

### 5.6 The page

- The designer's line "N not in the raw cache" (the sum of the catalog sizes of records with no ready cache)
  goes. The catalog sizes stay, named as the datasets' size upstream.
- `fetching` leaves the status table and its help text. `staging`: the stage job reads what the planned crops
  need from the source and writes the pyramid. `blocked`: the raw cache is over its quota.

### 5.7 What is removed

`limits.fetch_bytes`, `limits.fetch_walltime_min`, `limits.fetch_attempts`, `limits.parallel_fetch`, the fetch
job spec, the fetch activities and their result types, the `fetching` status, the keeper's demand and the
`wanted`/`blocked_at` marks. `harness fetch` stays in the harness: it is how a person fills a record's cache
by hand.

## 6. The writer's calls

1. **The stage job's cap is `limits.job_bytes`** (5 GB in this deployment), with no limit of its own. A record
   whose planned crops need more than that over the network fails and says so; raising the limit is the
   remedy.
2. **Range blocks are not kept.** They live in the job's workspace and go with it. Crops are placed at
   random, so a later plan touches other blocks, and a rebuild of the same plan costs what the first stage
   cost.
3. **No download path for large whole-file sources.** A record whose reader must take more than the job's
   cap whole cannot be staged, until §9 gives such a file range reads.
4. **The quota is soft, and no reservation is built.** A shared reservation of raw bytes would bound the
   overshoot only for buffers: crop jobs write the same cache unasked. What is built is the second check at
   the stage slot.
5. **A rebuild depends on upstream.** With no raw copy, a pyramid can be staged again only while the source
   answers. That is already so for a record whose raw cache the keeper evicted.
6. **`harness fetch` stays**, unused by the workflow.
7. **A raw cache has no `partial`.** Whatever is in it is what jobs left there; the keeper may take it when
   nothing holds it.
8. **Buffers keep their raw references.** Dropping them for records that are not hand-dropped would free
   more, and needs buffer-api to know which records those are; not done here.
9. **An unreported transfer counts as 0.** There is no second source for what a killed job moved.

## 7. Rollout

1. Now: the round of buffer-27716818 is cancelled. While dharani stays `blocked`, its raw row asks the keeper
   for 3.07 TB and the keeper evicts every free raw cache toward it.
2. mia-snippets: `harness stage` without `prefetch`, merged and the live harness checkout updated.
3. Before the mia-workflows deploy: no buffer round is sourcing or rebuilding, and **every buffer whose
   workflow run has sourced is dealt with**, resting ones included. A buffer at `ready`, `idle`, `staged`,
   `needs-human` or `cancelled` still carries its sourcing pass in its run's history (a run rolls over only when
   Temporal suggests it), and that history does not replay on the new workflow: the first signal after the
   restart (the keeper's `idle`, `degraded` or `retire`, a `go` from the page) fails its workflow task for
   good. Either it is force-stopped and run again after the deploy, which retires it and stages it anew; or,
   for a buffer at `ready` or `idle` that should keep serving, its run is replaced by hand before the deploy
   (query `status`, terminate, start `MiaBuffer` with that state), which leaves a history with no sourcing in
   it. `mia.yaml` may lose the four `limits` named in §5.7; unknown keys are ignored.
4. mia-workflows merged, workers and buffer-api restarted, buffer-27716818 started again.
5. Check on that round: no fetch job among its job records, `bytes_fetched` on a stage record far below the
   binding's file sizes, the raw cache not grown by a range-read record, and after the next keeper pass no
   raw row at `partial`.

## 8. Left to the builder

The wording of the timeline and help texts; the shape of the stage step's code; the tests. These are owed:

- the incident: a record far larger upstream than the quota is staged;
- a dataset that waited for a stage slot is checked again and waits again when the cache filled meanwhile;
- a whole-file fallback that fails part way leaves no cache entry, and the failed stage's raw row is `ready`
  with the bytes on disk;
- a stage killed with no report: the dataset fails, the reservation closes with 0, and the scan brings the
  raw row up to date;
- a failed stage with a report settles its `bytes_fetched`, and a second settle of the same job changes
  nothing;
- a hand-dropped record: staged offline, and `RAW_GONE` on a rebuild once its files left.

## 9. Later: range reads for large files

Accepted for now (2026-11-20): a file its reader must take whole, and that is larger than the job's cap, is
out of reach. It is to get range reads later through a reference index of the file: a key-value map from each
chunk to where its bytes are (url, offset, length), built once and kept, so a job reads the chunks its crops
touch and never the file. Not designed here; it gets a spec of its own.
