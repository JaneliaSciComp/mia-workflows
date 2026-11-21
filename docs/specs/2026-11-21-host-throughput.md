# MIA: what each host gives us — Spec

**Status:** v0.1 · 2026-11-21 · draft for approval. The direction (§4) and the decisions marked *user* are the
user's; everything marked *call* is the author's and is the user's to change. Rests on the stage-on-demand spec
(the harness's `IO` is where every byte of a job arrives) and the job-records spec (the job runner is where every
job's end is seen).

## 1. Intent

A buffer is filled from other people's servers, and how fast it can be filled is decided there. Nothing records
it. The later work of keeping a serving buffer filled (buffer-flow spec §3.9, "throughput and underflow") has to
know, per host, how fast data arrives, how that changes with the load we put on the host, and when the host
starts to refuse. This spec records those facts from now on. It decides nothing from them.

The incident: the first round of buffer-27716818 on stage-on-demand (2026-11-21) took 4.5 hours. Two records on
`ftp.ebi.ac.uk` were killed at the 120-minute walltime after about 1.2 and 1.7 GB of range reads, roughly
0.2 MB/s, while a record on AWS read 4.9 GB in 13 minutes. One job failed on a `403` from EBI for a file the
host serves at other times. All of this was worked out by hand from job start and end times and directory sizes.

## 2. Today

- **A job reports one number.** `stage.json` carries `bytes_fetched`: the bytes the job kept, in all. No time,
  no host. A body the server cut short is taken off the count.
- **A killed job reports nothing.** `stage.json` is written when `harness stage` ends. A job killed at its
  walltime, the slowest case, leaves no report.
- **Every request goes through one place.** `IO._send` sends each request and follows each redirect hop;
  `IO._get` receives every body; `IO._retry` decides what is asked again: a dropped connection, a timeout, a
  short body or a `5xx` after 1, 4 and 16 seconds; a `429` after the wait the server names, for 900 seconds in
  all; any other `4xx` is final. A job has at most `PARALLEL` (4) requests in flight.
- **Hosts refuse in different ways.** A `429` naming a wait (Zenodo); a `503` under load; a `403` that at other
  times is a `200` (EBI); a dropped connection or a short body (EBI, about one request in seven); or no signal
  at all, the bytes only come slowly.
- **A job that ends is seen in one place.** `CsubJobs.wait` sees each job of each worker that ends by itself or
  is killed by the cluster (its walltime, its memory), and calls `write_finished`, which writes the job's record
  from files of its workspace read with `read_untrusted`. A job we kill is not seen there: a cancelled round and
  the repair agent call `kill_job`, which only kills, and a wait that finds its raw-cache lock gone kills its
  job and raises before `write_finished`. Such a job's record has no second half.
- **A reader development job runs the harness from anywhere.** In mia-data and mia-buffers jobs the harness runs
  in the job's workspace. In an autodev developer round the agent types the harness call itself, from whatever
  directory it is in (`../probe`, `/tmp/...`), so `work`, which defaults to the current directory, is rarely the
  workspace.
- **A host sees all of us.** What a host counts is every request from our address: buffer stage jobs, dataset
  preparation jobs and reader development jobs alike. A stage round runs `sourcing.per_source` jobs per host.

## 3. Goals

1. Per host and clock minute, what we asked of the host and what came back: load, bytes, answers, waits.
2. Complete for the jobs that matter most: a job that is killed or fails leaves its minutes up to the last.
3. Every job that reads through the harness is counted, whichever worker ran it. (*user*)
4. A host's behaviour can be read against the load on it: "fine at 3 connections, refusing at 4".
5. Nothing stored says what an answer meant. A `403` is stored as a `403`; what counts as a refusal is decided
   by whoever reads, and can change without losing history. (*user*: other hosts use other codes)
6. One request that fails with a `4xx` is asked once more, so that "failed, then served" is a measured fact
   and not a guess. (*user*)

**Non-goals.** A view on the page. Anything that acts on the numbers: no refill planning, no change to
`per_source` or `PARALLEL`, no alert. Transfers outside the harness (package installs, git, the Claude API).
Finding a host's limit by raising the load on purpose: the data supports it, running it is a person's choice.

**Anti-goals.** A list of "refusal codes", in the code or per host in `mia.yaml`. A stored label such as
`throttled`. A job that fails, waits or slows down because recording did (other than goal 6's one repeat).

## 4. Approaches

**What is stored about a refusal.**

- *A code table*: a list of statuses that count as refusals, perhaps per host. Simple, but it is a guess that
  someone maintains, it is wrong for the `403` above, and it sees neither dropped connections nor slow bytes.
- *What happened, uninterpreted* (chosen): every answer counted under its own status, every transport failure
  under its kind, and two facts that mean the same on every host: the seconds we waited because the host named a
  wait, and the requests that failed and were then served.
- *Behaviour alone*: only load and bytes, a limit being read from where throughput stops growing. It is the one
  way to see silent throttling, and it needs runs at several loads before it says anything.

The second is stored at the grain the third needs (below), so the third is a way of reading the same rows.

**The grain.**

- *One row per job and host.* Small, but a job that ran alone for an hour and beside three others for ten
  minutes averages the two, and a limit counted per window (Zenodo: per minute and per hour) cannot be traced
  back to the load before it.
- *One row per job, host and clock minute* (chosen). Minutes of different jobs line up, so the load on a host is
  the sum over the jobs of a minute.

**Where it is measured.** From job records (start, end, `bytes_fetched`) nothing in the harness changes, but
compute time is mixed in, a job on two hosts cannot be split, and a killed job has no bytes. In the harness
(chosen) the count is exact and survives a kill.

**Where it is collected.** In the buffers worker's stage activity it would cover stage jobs only. In the job
runner (chosen) it covers every job of every worker in the one place that already writes each job's record.

## 5. Design

### 5.1 The harness measures (mia-snippets)

`IO` keeps a meter. For each host and each clock minute (`floor(unix time / 60)`, UTC, the node's clock) in which
the process had a request open to the host or got an answer from it, the meter holds:

| Field | Meaning |
|---|---|
| `requests` | requests sent to the host in the minute; each redirect hop is a request to the host it went to |
| `connection_seconds` | seconds summed over the requests open in the minute; a request is open from the moment it is sent until its response is closed. Waiting for one of the job's `PARALLEL` slots, and sleeping before a repeat, is not open |
| `peak` | the most requests open to the host at once in the minute |
| `bytes` | body bytes received from the host in the minute, whether or not the body was kept. This is what the host delivered, so it is not `bytes_fetched` |
| `answers` | a count per answer: the status as text (`"200"`, `"206"`, `"302"`, `"403"`) or, where no status came or the body failed, one of `timeout`, `connect`, `dropped` (any other transport error), `truncated` (a body shorter than announced, or cut off) |
| `recovered` | a count per answer of requests that failed with it and were served when asked again |
| `final` | a count per answer of requests that failed with it for good |
| `asked_wait_seconds` | seconds the process slept because the host named a wait: a `Retry-After` or rate-limit reset on a failed answer, and the pause `_send` takes when a host says its window is nearly used up |
| `backoff_seconds` | seconds the process slept on its own schedule before a repeat |

A wait is counted in the minute it began and against the host that caused it. A `HEAD` is a request with no
bytes. A read served from the raw cache or the job's blocks sends no request and is not counted.

**Written as it goes.** `from_env(work)` gives the meter the directory named by `MIA_HARNESS_TRANSFERS_DIR`, or
`<work>/transfers` where the variable is not set. The job runner sets the variable for every job (§5.2), so a
harness call from any directory of a job writes to the job's workspace. A timer thread closes
each minute when it ends and appends one JSON line per host to `<work>/transfers/<pid>-<start>.jsonl`
(`{"minute", "host", ...the fields above}`); the process's end closes the open minute. One file per harness
process, so the several harness calls of one job never write the same file. A job killed at any moment has every
closed minute on disk and loses at most the one that was open. Writing is best effort: a failure to write is
logged once and never fails or slows a read. An `IO` built without `from_env` (tests) has no meter file unless
given one.

**Two changes to what is asked again** (`IO._retry`):

A failed answer is handled by the first of these that fits:

1. *It names a wait* (`Retry-After`, whatever its status), or it is a `429`: asked again after the wait, within
   `RATE_WAIT_MAX` per wait and `RATE_PATIENCE` in all. This is today's `429` rule, widened to any status:
   "the host named a wait" is the one refusal signal that means the same everywhere. (*call*)
2. *A `404` or a `410`*: final at once. They say the file is not there, and a repeat would only delay every
   round on a dead link.
3. *Any other `4xx`*: asked once more after 16 seconds (the last `RETRY_WAIT`). Served: `recovered`. Failed
   again: final. (*user*: one repeat; *call*: the wait and the two codes left out)
4. *A `5xx`, a dropped connection, a timeout, a short body*: as today, after 1, 4 and 16 seconds.

Nothing else about retries, caps or hosts changes. One known cost: `harness describe` probes five metadata names
and reads a `403` as "not there", as a bucket without listing answers for a missing key; on such a host each
probe now waits 16 seconds and repeats, up to 80 seconds more per `describe`. Accepted. (*call*)

### 5.2 The job runner collects (mia-workflows, `mia_core`)

`CsubJobs.submit` adds `MIA_HARNESS_TRANSFERS_DIR=<workspace>/transfers` to every job's environment; the
brokers' `env_allow` already covers `MIA_HARNESS_*`.

When `CsubJobs.wait` sees a job end, beside writing its record it reads the job's transfers (`mia_core`, a
function next to `write_finished`). A wait that kills its own job because the raw-cache lock is gone does the
same before it raises. (*call*) It reads:

- every `*.jsonl` under `<workspace>/transfers`, each with `read_untrusted`, at most 64 files and 1 MiB each
  (*call*); what is over is dropped and the record says so;
- each line reduced to the typed row of §5.1: counts are non-negative integers, seconds non-negative numbers,
  `answers` keys are three digits or one of the four kinds; a line that does not parse or fit is dropped;
- a row is kept only if its host is one the job was allowed (`allow_hosts` of its record: every redirect hop is
  checked against that list, so every host a job reached is in it) and its minute lies between the job's
  submission and its end;
- rows of one host and minute from several files are added up (`peak` too: an upper bound).

The rows go to two places:

- **The job's record** gets `transfers`: the rows. The record is the authority, written once, never deleted.
- **The ledger** gets them through a sink the worker gives its `CsubJobs`
  (`registry.put_transfers`), so `mia_core` imports nothing above it. Each worker's `Activities` sets the sink
  once it has its registry (the job runner is built before it); autodev with no `service` block has no
  registry and sets none, so its jobs' rows are in their records alone.

Both are best effort, as the record is: neither fails a wait. The ledger's rows are an index of the records'
and can be rebuilt from them; no rebuild command is built now (*call*: add it the first time rows are lost).

A job's output is untrusted. A job could write false rows, but only for hosts it was allowed, within its own
run time, and each row names its job, so a reader can leave a job out.

### 5.3 The ledger and its routes (mia-workflows, `mia_buffers`)

Table `transfers`: `host`, `minute` (integer), `job` (the record's address), `service`, the numeric fields of
§5.1 as columns, `answers`, `recovered` and `final` as JSON; primary key (`host`, `minute`, `job`). A row put
again replaces itself, so a wait that runs twice changes nothing.

- `PUT /transfers/<job address>`: the rows of one job. Worker token. `Registry.put_transfers(job, rows)`.
- `GET /hosts?days=7`: per host over the window: minutes with load, bytes, bytes per second over those minutes,
  bytes per connection-second, requests, `answers`, `recovered`, `final`, both wait totals, the last minute seen.
- `GET /hosts/<host>?days=7`: the same host by load level. The rows of each minute are added over jobs; a
  minute's level is its `connection_seconds / 60` rounded up to a whole connection. Per level: minutes, jobs
  per minute (mean), bytes per second, bytes per connection-second, requests, the share of requests whose answer
  was neither `2xx` nor `3xx`, `answers`, `recovered`, `final`, both wait totals.
- `GET /hosts/<host>/minutes?from=&to=`: the minutes themselves, added over jobs, for whoever wants another
  reading (requests per hour against a limit counted per hour, for one).

The routes compute; they label nothing. "The share of requests not answered `2xx` or `3xx`" is arithmetic over
`answers`, not a stored column. These are read routes: no token.

Rows are kept. (*call*: no retention now; a round of 129 jobs writes a few thousand rows, so a rule for old
rows is added when the table is large enough to slow these routes.)

### 5.4 What it cannot say

- Only loads we have run at. A host's limit is found by raising `sourcing.per_source` for a while.
- A pattern, not a cause: a host's own state changes too. Many minutes at a level make a row trustworthy.
- `peak` summed over jobs is an upper bound; the mean (`connection_seconds / 60`) is exact.
- Load from our address that does not go through the harness, or through another installation, is not in it.

## 6. Constraints

- **The harness deploys first.** A round pins the harness HEAD; a job on an older harness writes no transfers
  and has no rows. Nothing waits for them.
- **No workflow code changes.** Collection is in the job runner (activity side); no activity result dataclass
  changes; no replay fixture changes.
- **Untrusted output**: `read_untrusted`, typed reduction, bounds (§5.2).
- **The page holds no token** and is not changed. The new read routes are not used by the page.
- **No real paths or hostnames** in tracked files; public archive hosts in examples are fine.

## 7. Tests and the check on the installation

Harness: the meter with a fake clock and a mock transport: bytes land in the minute they arrive; a request open
across a minute boundary gives each minute its seconds; `peak`; answers by status and by kind; `recovered` and
`final`; a `403` then `200` is one repeat and `recovered`; a `403` twice is final after one repeat; a `404` is
not repeated; a `503` with `Retry-After` waits as named and counts `asked_wait_seconds`; closed minutes are on
disk while the process still runs.

Workflows: the reduction drops a malformed line, a host the job was not allowed, a minute outside the job, and
what is over the bounds; rows of two files are added; the record holds the rows; a sink that raises does not
fail the wait; the table replaces a row put twice; each route's arithmetic on a small fixed set of rows; `PUT`
without the token is refused.

On the installation, after the harness and the workers are restarted: run a round, then `GET /hosts` lists the
round's hosts with bytes that agree with the jobs' `bytes_fetched` to within what cut-short bodies explain; a
job that was killed has rows up to its last minute.

## 8. Open decision

**Jobs we kill ourselves** (a cancelled round, the repair agent's kill; §2). As written they leave no rows: their
transfer files stay in the workspace, unread. A round cancelled because a host is slow is a case goal 2 is
about. To cover them, the kill itself has to finish the job's record: `kill_job` knows only the job id, so it
asks csub for the job's workspace, kills, and writes the second half of the record (outcome `KILLED`, the end
being the time of the kill) with its transfers. No workflow code changes, but every reader of job records then
sees a kind of ending it has not seen before. In scope now, or a follow-up?

## 9. Left to the builder

How the meter integrates `connection_seconds` over a minute boundary; the names of the module and functions;
the JSON shape of the routes beyond the fields named; whether the timer is a thread or a check on each event,
as long as a minute with a request open and no event is still written.
