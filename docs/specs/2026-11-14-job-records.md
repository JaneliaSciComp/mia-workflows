# MIA: a record of every job and every agent conversation — Spec

**Status:** v0.2 · 2026-11-14 · awaiting the user's approval. v0.2: no list of jobs on the page; a job's link
sits next to what the job produced (the user's ruling on v0.1).

## 1. Intent

Looking at a crop judge's verdict on a dataset's page, there is no way to see what the judge was asked or how
it got to its answer. The same holds for the agent that develops a reader, the one that debugs a failed round,
and the review judge; and for any cluster job, the script that was sent and what the cluster said about it are
buried in a workspace. Each job should leave a small permanent record, and the page should show it on request.

**Goals**

- Every job submitted through `CsubJobs` (all three services, every kind) leaves one record: the script and
  resources that were sent, what the cluster answered at submit, how the job ended, what it used (memory, CPU,
  run time), and the end of its stdout and stderr.
- An agent job's record also holds its prompt and its whole conversation: every assistant text and thinking
  block, and every tool call. A tool's output is not kept; the call stands for it.
- Records persist: one gzipped JSON file per job, in a directory no job can write, never deleted.
- buffer-api serves a record as it is on disk (gzip passed through); the page renders it in the browser.
- On the page a job's link sits next to what the job produced: `Stage: data:matching` links to the job that
  did the matching, a crop judge's verdict to the judge's conversation. There is no separate list of jobs.

**Non-goals**

- autodev on the page. Its jobs (the author, the review judge) are recorded exactly the same way under its own
  `work_root`; nothing serves or links them yet.
- Records for jobs that ran before this is deployed. Their conversations were never written down.
- Following a running job live, searching across records, copying records to GitHub, retention or deletion.

**Anti-goals**

- A secret in a record. Records are served without a token and kept for good. A job's `env` can carry an API
  key: a record holds the names of the variables, never a value. An agent can be talked into printing a
  credential: every string that comes from a job is scrubbed of anything that looks like one (§3.3).
- A round that fails, or waits, because a record could not be written or read. Recording is best effort and
  logged.
- A workflow change. Nothing here alters a command a workflow issues, a result dataclass in `models.py`, or a
  replay fixture.
- Tool output in a record or on the page.

## 2. Approaches

1. **The worker writes one file per job; buffer-api serves it (picked).** `CsubJobs` is the one place every
   job passes through, at submit and at the end of the wait, and both calls have the workspace.
2. **Serve the workspaces as they are.** No new file, but a workspace is writable by its job, an agent's raw
   stream holds every tool output (the judge reads images: megabytes of base64), buffer-api would parse
   untrusted output on every view, and nothing is a record once a workspace is cleaned.
3. **Store records in the ledger through a worker route.** Blobs in SQLite and in every backup; autodev has
   no registry client.
4. **Carry them in Temporal histories.** Payload limits, and result dataclasses are wire format.

## 3. Design

### 3.1 The record

One file per job: `<work_root>/_jobs/<the job's workspace, relative to work_root>.json.gz`. A workspace is
unique per job, and everything that handles a job knows its workspace, so a record's address needs no id
passed around. Each service writes under its own `paths.work_root`; mia-data and mia-buffers share one, which
buffer-api already reads. No job mounts `work_root` itself or `_jobs` (a test asserts it for every job spec
builder).

```
{ "schema": 1,
  "service": "<deployment>", "key": "<job key>", "job_id": "…", "workspace": "<relative path>",
  "submitted_at": "<ISO>", "finished_at": "<ISO>",
  "request":  { name, command, cpus, mem_mb, walltime_min, allow_hosts, agent, mounts, env: [names] },
  "submit":   { …csub's SubmitResult: queue, slots, job_group, estimated_max_cost_usd, billing_group, … },
  "outcome":  { state, exit_code, exec_host, queue },
  "usage":    { max_mem, avg_mem, mem_limit, cpu_used, run_time, slots },
  "logs":     { stdout, stderr },
  "agent":    { prompt, model, cost_usd, turns: [ … ] } }
```

`request` and `submit` are written when the job is submitted; the rest when the wait sees it finished. A
record without `outcome` is a job that is still running, or one whose end no worker saw (a cancelled round, a
dead worker); the page says that much and no more. `agent` is absent for a job that is not an agent.

### 3.2 Writing it

`CsubJobs` (`autodev/jobs.py`) takes the service's `work_root` and writes the record itself:

- `submit`, when it submits: `request` (from the spec; env names only) and `submit` (csub's answer). When it
  reattaches through the id file it writes nothing; when it finds the job by its workspace on the broker it
  writes `request` alone.
- `wait`, once the job is terminal or forgotten: `outcome`, `usage`, `logs`, and for an agent `agent`. A
  retried wait writes the same thing again. `wait` has the job id and the workspace, no spec: it learns the
  prompt and transcript file names from the half `submit` wrote, so no activity signature changes.

Every write is a temp file and a rename, inside a `try` that logs and goes on.

`usage` comes from LSF, asked once at the end: `bjobs -o "max_mem avg_mem memlimit cpu_used run_time slots"
-json <job id>`, run by the worker. csub's own status carries no usage and its captured stdout holds only what
the script printed. *(My call: asking LSF directly. No `bjobs` on the worker's host, an error, or a job LSF has
forgotten leaves `usage` out. Marked `# ponytail:`; the upgrade is csub reporting usage in its status.)*

`logs` are the last 64 kB of the job's stdout and stderr, from `<workspace>/.csub/jobs/<job id>/`.

### 3.3 The conversation

The four agent commands (`autodev/agent.py` `job_script`, `mia_core/judge/evidence.py` `command`,
`mia_data/jobspec.py` `_agent_job`, and through `command` both judges) run Claude Code with
`--output-format stream-json --verbose` into `claude.jsonl`, and the script then copies the stream's last line
to the file the result is read from today (`claude.json`; autodev's `result.json`). That last line is the same
result object `--output-format json` prints (`result`, `total_cost_usd`, `modelUsage`; checked against the
installed CLI on 2026-11-14), so every reader of a result stays as it is. *(My call.)*

`JobSpec` gains two optional workspace-relative names, `prompt_file` and `transcript_file`; a job with a
`transcript_file` is an agent job. At the end of the wait the worker reduces the stream to `agent.turns`, in
order:

- an assistant text block: kept whole;
- a thinking block: kept whole;
- hook, rate-limit and other bookkeeping lines: dropped;
- a tool call: the tool's name and its input, each string of the input cut at 2,000 characters with a count of
  what was cut *(my call)*; marked failed when its result was an error;
- a tool result: dropped;
- a message of a sub-agent: kept in order, marked as a sub-agent's;
- the final result line: `model`, `cost_usd`, turns and duration.

`agent.prompt` is the prompt file's text, whole.

Every string that comes from the job (turns, tool inputs, the prompt file, the log tails) passes
`mia_core.untrusted.SECRET_RE` on its way into the record, each match replaced by `[redacted]`: the one
exception to "kept whole". *(My call, from the reviewer's question.)*

The stream, the logs and the prompt file are output of a sandboxed job: read with `read_untrusted` under a size
cap, parsed defensively (any shape may be there; a line that does not parse is skipped), reduced to the typed
record above. An agent with a shell can rewrite its own stream while it runs, so a record is as trustworthy as
the result read from the same workspace, no more; the judges have no tool that writes.

### 3.4 Serving it

Two read routes on buffer-api, no token, nothing written:

- the jobs of a subject, which the page needs to place its links (§3.5): for each round in the subject's
  timeline, the records under that round's workspace directory, each with its address, name (the workspace's
  basename), whether it is an agent's, state, exit code and times. *(ponytail: opens each record of the subject
  to list it; an index when a subject has thousands.)*
- one record: the file's bytes with `Content-Type: application/json` and `Content-Encoding: gzip`. The path is
  checked segment by segment, must stay inside `_jobs`, and no symlink is followed.

buffer-api never writes or deletes a record. Nothing deletes one: the keeper's rule is unchanged, and `_jobs`
is not among the things it removes.

### 3.5 The page

There is no card or tab that lists jobs. A link to a job's record sits where the page shows what that job did:

- **An entry of the timeline shows the jobs that ran under it:** the jobs of the entry's round that were
  submitted from that entry until the round's next one. `Stage: data:matching` so carries the match agent's job,
  a testing or cropping entry its smoke, crop and verify jobs, a debugging entry the debugging agent's, a
  buffer round's staging entry its stage jobs. Each is a short link beside the entry's folders (name, state,
  how long), folded when an entry has many; the folder link to the workspace stays. The page works this out
  from the entry's `round` and `at` and the records' `submitted_at`: no entry stores a job, no workflow or
  activity hears of it, and it holds for every kind of subject and for rounds of any shape. *(My call. A stage's
  entry is written and awaited before its job is submitted, so the order is the workflow's; ponytail: it
  compares buffer-api's clock with the worker's, so two hosts a few seconds apart can put a job one entry
  early. The upgrade is the record carrying the stage.)*
- **What is rendered from a job's output links to that job.** The crop judge's comment ends with a link to the
  judge's record. `read_judge` renders that comment and knows both the workspace and the page's URL, so no
  workflow hears of it, and the copy on GitHub carries the same link. *(My call: the link is in the comment's
  text, so judges' comments posted before this have none.)*
- **A record's view** (`#job/<address>`), where every such link leads: what ran and how it ended (state, exit
  code, queue, host, times, usage, cost); for an agent the prompt (folded) and the conversation, turn by turn,
  a tool call as one line that opens to its input, thinking folded; then the script, csub's answer, and the
  ends of stdout and stderr.

A request's entries get no job link: a request runs no job of its own, and the datasets it starts carry
theirs.

Everything from a record is inserted as text, never parsed as HTML or as markdown. The new module follows the
page's rules: no build step, imports only modules below it, read routes only.

## 4. What does not change

Workflows, activity signatures, `models.py`, replay fixtures, `mia.yaml` (no new key), the harness, the
registry's tables. Deploying is a restart of the workers and buffer-api; jobs submitted before it have no
record, and jobs running across it get the second half only if their spec was built by the new code.

## 5. Left to the builder

Module layout (the reduction belongs in `mia_core`), exact field names beyond §3.1, the size caps, the route
patterns, how a subject's rounds map to workspace directories (`_gallery_png` does it for dataset rounds), the
card's and the view's layout, and the tests. Expected at least: the reduction over a fixture stream (no tool
output, long inputs cut, junk lines survived, a planted credential redacted), no env value in a record, both
halves written by `CsubJobs` with
the fake client, a write that fails leaving submit and wait unharmed, the record route refusing a path outside
`_jobs`, and the existing check that the page calls read and action routes only.
