# MIA: what each host gives us — Spec

**Status:** v0.2 · 2026-11-21 · draft for approval; v0.2 takes in a review of v0.1 without widening it. The
direction (§4) and the decisions marked *user* are the
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
2. A job that fails, or that the cluster kills (its walltime, its memory), leaves its minutes up to the last.
   A job we kill ourselves does not yet (§5.4).
3. Every job that reads through the harness is counted, whichever worker ran it. (*user*)
4. A host's behaviour can be read against the load on it in the same minute: its answers at a mean of three
   connections open beside its answers at a mean of four.
5. Nothing stored says what an answer meant. A `403` is stored as a `403`; what counts as a refusal is decided
   by whoever reads, and can change without losing history. (*user*: other hosts use other codes)
6. A request that fails with a `4xx` naming no wait is asked once more, so that "failed, then served" is a
   measured fact and not a guess. (*user*)

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

A *request* is one thing the harness wants of a host (a file's validators, a range, a whole file). It is made
in *attempts*: each time something is sent, a redirect hop included, is one attempt at the host it goes to. An
attempt has exactly one *outcome*:

- the status as text (`"200"`, `"206"`, `"302"`, `"403"`), when a status came and, for a `2xx`, the body
  arrived whole;
- `timeout`, `connect` or `dropped` (any other transport error), when no status came;
- `truncated` (a body shorter than announced), `timeout` or `dropped`, when a `2xx` came and the body then
  failed. The kind alone is counted, not the status: a body is read only after a `2xx`.

`IO` keeps a meter. Everything is counted in the minute it happens, and a minute that has been written is never
changed. For each host and each clock minute (`floor(unix time / 60)`, UTC, the node's clock) in which any of
the following is not zero, the meter holds:

| Field | Meaning |
|---|---|
| `requests` | attempts sent to the host in the minute: what a limit counted per window counts |
| `connection_seconds` | seconds summed over the attempts open in the minute; an attempt is open from the moment it is sent until its response is closed. Waiting for one of the job's `PARALLEL` slots, and sleeping before a repeat, is not open |
| `peak` | the most attempts open to the host at once in the minute |
| `bytes` | body bytes received from the host in the minute, whether or not the body was kept. This is what the host delivered, so it is not `bytes_fetched` |
| `answers` | a count per outcome of the attempts that ended in the minute. An attempt sent in one minute and ended in the next is in the first minute's `requests` and the second's `answers` |
| `recovered` | a count per outcome of failed attempts whose request was then served, written in the minute the request was served |
| `final` | a count per outcome of failed attempts whose request then failed for good, written in the minute it gave up |
| `asked_wait_seconds` | seconds the process slept because the host named a wait: a `Retry-After` or rate-limit reset on a failed answer, and the pause `_send` takes when a host says its window is nearly used up |
| `backoff_seconds` | seconds the process slept on its own schedule before a repeat |

A failed outcome is any but a `2xx` or a `3xx`. Every failed attempt is counted twice: at once in `answers`,
and once more in `recovered` or `final` when its request ends, under its own outcome. So a request that met a
`403`, then a `503`, then was served adds one `"403"` and one `"503"` to `recovered` in the minute it was
served, and that minute need not hold the failures themselves. Over any stretch of time the failed outcomes in
`answers` equal `recovered` plus `final`, less the requests still going at its end.

A wait is counted in the minute it began and against the host that caused it. A `HEAD` is an attempt with no
bytes. A read served from the raw cache or the job's blocks sends nothing and is not counted.

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

Each failed attempt is handled by the first of these that its outcome fits:

1. *A `404` or a `410`*: final at once, whatever else the answer carries. They say the file is not there, and a
   repeat would only delay every round on a dead link.
2. *It names a wait* (`Retry-After`, on any other status), or it is a `429`: asked again after the wait, within
   `RATE_WAIT_MAX` per wait and `RATE_PATIENCE` in all. This is today's `429` rule, widened to any status:
   "the host named a wait" is the one refusal signal that means the same everywhere. So a `403` that names a
   wait is asked again for as long as a `429` is, not once. (*call*)
3. *Any other `4xx`*: asked once more after 16 seconds (the last `RETRY_WAIT`). (*user*: one repeat; *call*:
   the wait and the two codes left out)
4. *A `5xx`, a dropped connection, a timeout, a short body*: as today, after 1, 4 and 16 seconds.

The allowances are the request's, and none is renewed: the one repeat of rule 3, the three waits of rule 4 and
the patience of rule 2 are each used up across all the attempts of one request, whatever order the outcomes
come in. A `403`, then a `503`, then a `403` again is final at the second `403`.

Nothing else about retries, caps or hosts changes. One known cost: `harness describe` probes five metadata names
and reads a `403` as "not there", as a bucket without listing answers for a missing key; on such a host each
probe now waits 16 seconds and repeats, up to 80 seconds more per `describe`. Accepted. (*call*)

### 5.2 The job runner collects (mia-workflows, `mia_core`)

`CsubJobs.submit` adds `MIA_HARNESS_TRANSFERS_DIR=<workspace>/transfers` to every job's environment; the
brokers' `env_allow` already covers `MIA_HARNESS_*`.

When `CsubJobs.wait` sees a job end, beside writing its record it reads the job's transfers (`mia_core`, a
function next to `write_finished`):

- the `*.jsonl` files under `<workspace>/transfers` in order of name, each with `read_untrusted`: at most 256
  files, 8 MiB in all and 20,000 rows after adding up (*call*; the most harness calls seen in one agent job is
  17, and a day-long job on three hosts writes about 4,000 rows). A job over any of these keeps no rows: its
  record says `transfers_refused` and why;
- each line reduced to the typed row of §5.1, or dropped. Numbers are finite (`NaN` and infinities do not
  parse). Counts are integers from 0 to 1,000,000, `bytes` an integer from 0 to 2^50, each of the three seconds
  fields a number from 0 to 86,400. `answers`, `recovered` and `final` hold at most 32 keys each, a key being
  three digits or one of the four kinds. The record counts the lines dropped (`transfers_dropped`);
- a row is kept only if its host is one the job was allowed, compared in lower case (`request.allow_hosts` of
  its record: every hop is checked against that list, so every host a job reached is in it). A record with no
  first half names no hosts, and its job keeps no rows (`transfers_refused`);
- a row is kept only if its minute is not after the moment of collection and not more than 7 days before it.
  The record's `submitted_at` is not used: a job found again after a worker restart is given a new one, later
  than its first minutes (*call*: 7 days is longer than any job runs);
- rows of one host and minute from several files are added up (`peak` too: an upper bound); a sum over a
  field's limit is cut to it.

The rows go to two places:

- **The job's record** gets `transfers`: the rows. The record is the authority, written once, never deleted.
- **The ledger** gets them through a sink the worker gives its `CsubJobs`
  (`registry.put_transfers`), so `mia_core` imports nothing above it. Each worker's `Activities` sets the sink
  once it has its registry (the job runner is built before it); autodev with no `service` block has no
  registry and sets none, so its jobs' rows are in their records alone.

Both are best effort, as the record is: neither fails a wait. The ledger's rows are an index of the records',
and a delivery that failed is made again: a worker that starts puts the rows of every record of its work root
that ended in the last 7 days, in a thread beside its work. A put replaces (§5.3), so this repeats nothing.
Until that worker next starts, the minutes of a job whose delivery failed read lower than they were (§5.4).

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
  minute's level is its *mean connections open*, `connection_seconds / 60`, rounded to the nearest whole number
  (level 0: under half a connection, which is where a job's first and last minute mostly fall). Per level:
  minutes, jobs per minute (mean), bytes per second, bytes per connection-second, requests, the share of
  attempts that failed (the failed outcomes in `answers` over all of `answers`: attempts that ended in those
  minutes, not attempts sent), `answers`, `recovered`, `final`, both wait totals.
- `GET /hosts/<host>/minutes?from=&to=`: the minutes themselves, added over jobs, for whoever wants another
  reading (requests per hour against a limit counted per hour, for one).

The routes compute; they label nothing. The share of attempts that failed is arithmetic over `answers`, not a
stored column. These are read routes: no token.

Rows are kept. (*call*: no retention now; a round of 129 jobs writes a few thousand rows, so a rule for old
rows is added when the table is large enough to slow these routes.)

### 5.4 What it cannot say

- Only loads we have run at. A host's limit is found by raising `sourcing.per_source` for a while.
- A pattern, not a cause: a host's own state changes too. Many minutes at a level make a row trustworthy.
- A level is the mean over a minute, not the connections open at an instant: three connections open for
  twenty seconds and one open for the whole minute are both level 1. Rounds that hold a steady load for many
  minutes are what make the levels mean something. `peak` summed over jobs is only an upper bound on the true
  peak, which the rows of separate jobs cannot give.
- Load from our address that does not go through the harness, or through another installation, is not in it.
- **Jobs we kill ourselves leave no rows**: those of a cancelled round, those the repair agent kills, and one
  whose wait found its raw-cache lock gone. Nothing finishes such a job's record today. Their transfer files
  stay in their workspaces. Covering them means the kill finishing the record once the job has stopped
  writing, which is a change to job records and a follow-up to this spec.
- **Rows can be missing without a sign.** A job whose delivery to the ledger failed is missing from its minutes
  until its worker next starts; a job that kept no rows (`transfers_refused`), ran on an older harness, or ran
  under an autodev with no `service` block is missing for good. The minutes it shared with other jobs then
  show less load than there was. The records say which jobs these are; the routes do not.

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
`final`; an attempt sent in one minute and ended in the next; a `206` whose body is cut short is one `truncated`
and no `206`; a failure in one minute served in a later one puts `recovered` in the later minute and changes
nothing already written; a `403` then `200` is one repeat and `recovered`; a `403` twice is final after one
repeat; a `403`, a `503`, a `403` is final at the second `403` and adds all three to `final`; a `404` is not
repeated, with `Retry-After` or without; a `403` and a `503` with `Retry-After` wait as named, within the
patience, and count `asked_wait_seconds`; closed minutes are on disk while the process still runs.

Workflows: the reduction drops a malformed line, a `NaN`, a number over its limit, a map of too many keys, a
host the job was not allowed and a minute in the future or over 7 days old; a job over the file, size or row
bounds keeps none and its record says why; a job found again after a restart keeps the minutes from before its
new `submitted_at`; a record with no first half keeps none; rows of two files are added; a worker's start puts
again the rows of a record whose delivery failed; the record holds the rows; a sink that raises does not
fail the wait; the table replaces a row put twice; each route's arithmetic on a small fixed set of rows; `PUT`
without the token is refused.

On the installation, after the harness and the workers are restarted: run a round, then `GET /hosts` lists the
round's hosts with bytes that agree with the jobs' `bytes_fetched` to within what cut-short bodies explain; a
job that was killed has rows up to its last minute.

## 8. Left to the builder

How the meter integrates `connection_seconds` over a minute boundary; the names of the module and functions;
the JSON shape of the routes beyond the fields named; whether the timer is a thread or a check on each event,
as long as a minute with a request open and no event is still written.
