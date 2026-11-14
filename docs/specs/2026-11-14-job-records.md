# MIA: a record of every job and every agent conversation — Spec

**Status:** v0.6 · 2026-11-14 · awaiting the user's approval. Rulings of the user on the drafts before it: no
list of jobs on the page, a job's link sits next to what the job produced; a stage says what happened in it;
one entry per pass of a stage; and (v0.6) a stage writes two entries, its start and its result, the result
carrying its jobs.

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
- A stage of a round writes two entries to the subject's timeline: one when it starts, saying what it is
  doing, and one when it ends, saying what happened. The result entry links the jobs the stage ran. There is
  no separate list of jobs, and nothing on the page works out which job or which report belongs to which
  stage: the entry says it.

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
- A stage that started and never says how it ended: failed, cancelled and refused passes write a result too.
- A change to a result dataclass in `models.py`, or a workflow command changed outside a patch (§3.6).
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

One read route on buffer-api, no token, nothing written: a record, as the file's bytes with `Content-Type:
application/json` and `Content-Encoding: gzip`. The path is checked segment by segment, must stay inside
`_jobs`, and no symlink is followed.

buffer-api never writes or deletes a record. Nothing deletes one: the keeper's rule is unchanged, and `_jobs`
is not among the things it removes.

### 3.5 The page

- **A record's view** (`#job/<address>`): what ran and how it ended (state, exit code, queue, host, times,
  usage, cost); for an agent the prompt (folded) and the conversation, turn by turn, a tool call as one line
  that opens to its input, thinking folded; then the script, csub's answer, and the ends of stdout and stderr.
- **Links to it are in the timeline's text** (§3.6), which the page already renders: a `**Jobs:**` line of
  links, written the way `**Folders:**` is, each to `<service.url>/#job/<address>`. The same line works in the
  copy on GitHub. The page has no card, tab or route that lists jobs, and groups nothing.

Everything from a record is inserted as text, never parsed as HTML or as markdown. The new module follows the
page's rules: no build step, imports only modules below it, read routes only.

### 3.6 A stage writes its start and its result

A round is a series of stage passes. Each pass writes two entries, both naming the round and the stage:

- **Start**, when the pass begins: the stage line (`Stage: data:matching`), one sentence saying what the stage
  does (a sentence per stage, kept beside the stage table), and the folders. Writing it is what sets the
  subject's stage, as now.
- **Result**, when the pass ends: `Result: data:matching` and one sentence saying what happened, then whatever
  the stage has to show (the crops table and the binding for a test, a debugging agent's diagnosis, a snippet
  PR's link), then `**Jobs:**`, a link to the record of every job the pass submitted, folded when there are
  many. A result does not set a stage.

What the result says, for a dataset: matching, which snippet the binding uses and the agent's reasons, or what
fails with the existing ones; testing, the sentence its report opens with today (`Crops at <sha>: passed, 12
of 12 verified …`) with the table; review, who decided what; developing and merging, what became of the
snippet PR; debugging, the diagnosis and whether a new round follows. A buffer round and a request follow the
same rule with their own stages. A pass that fails, is cancelled or is refused writes its result too, with
the reason and the jobs it got to: the round's final stage (`failed`, `blocked`, `cancelled`) then says where
the round rests, and no longer has to carry the story of the stage that broke.

**Which stages.** The stage tables already say it (`STAGE_ACTIONS`): a stage where a round is `working` or
`waiting` is a pass and writes both entries. A stage a subject rests at (`done`, `failed`, `blocked`,
`cancelled`, …) is where a round ended, not work: it writes one entry, as now.

**What this replaces.** The comments a round writes today that are really a stage's result (the crops report,
the debugging diagnosis, the PR notices) become the body of that stage's result entry; they are not written a
second time. The judge's verdict stays an entry of its own, written while the subject waits at `review`, and
ends with the link to the judge's record: it is the report of that one job. The rule for every link is the
same: the entry that reports what a job did links the job.

**One pair per pass.** A pass is each time the round begins a stage's work: it enters the stage from another
one (`testing`, `review`, `testing` again for a revised snippet PR), or it tests another PR head while still at
`testing`. The workflow counts the passes of each stage within the round (a defaulted field of its state,
reset with the round); the pass is in both markers from the second pass on, so a first start keeps today's
marker. A retried activity writes nothing twice.

**How it is written.** The workflow knows when a pass ends and which jobs it submitted (their workspaces, from
which a record's address follows), so the workflow asks for the result: one activity call per pass, which
renders the entry and posts it through `_post` like every other. Each of those calls is a new command. They
all sit behind one patch per workflow (`stage-results`), the replay fixtures are re-recorded, and a round in
flight across the deploy finishes the way it started, without results. *(My calls: the patch, the counter in
the workflow's state, jobs as a line of links in the entry's text and not a stored list. The builder goes
through every stage of the three workflows and every path out of it.)*

**What it costs to know.** A job's link appears when its stage ends, not while the job runs; its record is
only complete then. The copy on GitHub gets two comments per pass.

## 4. What changes and what does not

Unchanged: result dataclasses in `models.py`, `mia.yaml` (no new key), the harness, the registry's tables and
routes other than the one that serves a record, and the page's timeline code. Changed: the three workflows
write a result per pass and count passes (behind `stage-results`; state gains a defaulted field, so a state
carried across `continue_as_new` still loads), the stage activities render a start sentence and pass markers,
and the replay fixtures.

Deploying is a merge into the live checkout with a restart of the workers and buffer-api, built in a worktree
until then: workers load workflow code from the checkout for each run, and the page's files are served from it.
Jobs submitted before the restart have no record, and jobs running across it get the second half only if their
spec was built by the new code.

## 5. Left to the builder

Module layout (the reduction belongs in `mia_core`), exact field names beyond §3.1, the size caps, the route
pattern, the wording of each stage's two sentences, the view's layout, how the workflow gathers a pass's jobs,
and the tests. Expected at least: the reduction over a fixture stream (no tool output, long inputs cut, junk
lines survived, a planted credential redacted), no env value in a record, both halves written by `CsubJobs`
with the fake client, a write that fails leaving submit and wait unharmed, the record route refusing a path
outside `_jobs`, a round whose every pass has a start and a result (one that passes, one that fails in a
stage, one that is cancelled, one with a second pass of a stage), a retry writing neither twice, the replay
tests old and new, and the existing check that the page calls read and action routes only.
