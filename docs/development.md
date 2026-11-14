# Developing in this repository

How to change the code here without breaking what is running. The README explains what autodev, mia-data and
mia-buffers do; this page is about working on them. Agents start from [`AGENTS.md`](../AGENTS.md), which is the short
version.

## Setup

Everything runs through [pixi](https://pixi.sh). `pixi install` creates the environment (Python 3.12, Temporal
SDK, httpx, pydantic, csub, and the test and lint tools). No GitHub token, LSF or Temporal server is needed for the
checks below; they are only needed to run a deployment ([`docs/autodev-local-setup.md`](autodev-local-setup.md)).

## Configuration

One file configures every deployment: `mia.yaml` (copy `mia.yaml.example`; `MIA_CONFIG` names another file). Its
top-level sections are shared; each block under `services` overlays them for one service, and the service name is its
`deployment`. Run an application by hand with `MIA_SERVICE=<name> <app> check`; through `mia`, `mia exec <service> check`
sets `MIA_SERVICE` for you.

- **`paths.local_root`** is needed by the mia-data and mia-buffers services and must be host-local disk, not NFS
  (`mia up` refuses otherwise). It holds `locks/` and buffer-api's SQLite ledger,
  `<local_root>/service/buffers.sqlite`, which is backed up on a timer under `<state_dir>/backups/`.
- **`service`** names buffer-api (`url`). mia-data and mia-buffers both need it: every stage, timeline entry and
  action of a round lives there, and a worker without it refuses to start.
- **`service.token_file`** (mode 600; `mia init-token` creates it) holds the buffer-api worker token and is mounted
  read-only into the workers. mia-buffers needs it; `mia up` refuses to start it without.
- **Three blocks bound three scopes.** `limits` is per job (one csub job's bytes, walltime and attempts, one agent
  run's cost, one round's PR revisions). `quota` is per disk root (the shared caches' size, kept by the keeper's
  evictions). `autonomy` is per request (what every job of one root adds up to) plus the operator's switches:
  `pause` (`mia pause` and `mia resume` edit it in place, comments kept; every worker re-reads the file before each
  job), `quarantine_days`, and the `root` caps a request may spend in all (`max_usd`, `max_fetch_bytes`, `max_jobs`,
  `max_concurrent_jobs`, `max_days`).

## The four checks

Run all of these before pushing. CI runs the last three in this order on every pull request and push to main.

| command | what it does |
|---|---|
| `pixi run fmt` | formats with ruff and applies its safe autofixes; Biome does the same for the page's modules |
| `pixi run lint` | `ruff check` plus a formatting check, and Biome's lint and formatting check of the page's modules; fails on drift |
| `pixi run typecheck` | mypy over `autodev/`, `mia_core/`, `mia_data/`, `mia_ops/` and `mia_buffers/` with `check_untyped_defs` |
| `pixi run test` | pytest over `tests/`, including the Temporal replay tests |

Configuration for all four is in `pyproject.toml`, and Biome's in `biome.jsonc`. Keep them clean rather than
suppressing: a `type: ignore` or `noqa` comment needs a reason next to it, and so does a rule turned off in
`biome.jsonc`.

## Layout

| path | |
|---|---|
| `mia_core/` | what every package shares: the `mia.yaml` loader and shared config models (`config.py`), the host-local-disk and secret-file checks (`fs.py`), the lock directory (`locks.py`), the worker-side client of buffer-api's registry (`registry.py`), the untrusted-output helpers (`untrusted.py`), and the judge (`judge/`: the acceptance policy, the verdict schema, the evidence builder, the prompts, the comment, `mia judge eval`). Imports no application package; every package may import it |
| `autodev/` | the issue → PR loop. Also home to the pieces the others reuse: the GitHub client (`github.py`), auth (`auth.py`), the csub job runner (`jobs.py`), git operations (`gitops.py`), and the Temporal connection and logging (`cli.py`) |
| `mia_data/` | the dataset → crops loop (`MiaDataDataset`), the poll of snippet PRs, and the mirror that copies timelines to GitHub issues (`mirror.py`). Imports from `autodev`, never the other way round |
| `mia_buffers/` | training buffers: the `MiaBuffer` workflow (a spec → plan → sourcing → staged → ready), `MiaPrep` (a preparation request) and `MiaCacheKeeper`; the planner (`mia-buffers plan`); buffer-api (`mia-buffers serve`: the sampler, the SQLite ledger, the registry, the action routes and the page under `ui/`); `mia-buffers register` (plan + staged volumes → a buffer); and `mia_buffers.client.BufferDataset` (a MuViT dataset over a buffer). Imports from `mia_data` and `autodev`, never the other way round |
| `mia_ops/` | `pixi run mia`: the services manifest (`mia.yaml`), the Temporal and app-service recipes under tmux and agentic-sandbox, and `mia exec` for app commands |
| `tests/` | one `test_<package>_<module>.py` per module; `autodev_helpers.py` and `mia_helpers.py` build configs and fake repos; `fixtures/` holds recorded workflow histories and sample catalog data |
| `docs/` | setup guides, [the mia-data workflow overview](mia-data-workflow.md), this page, `autodev-runbook.md` (operations and known gaps), `agentic-dev-infrastructure.md` (why); `specs/` and `plans/` are dated records of design decisions and describe the code as it was planned when they were written |

Within each package the split is the same: `workflows.py` (deterministic Temporal code, no I/O), `activities.py`
(everything that touches the registry, GitHub, the filesystem or the cluster), `models.py` (the dataclasses that
cross between them), `config.py` (pydantic models for the YAML), `cli.py`.

## Rules that are not obvious from the code

**Activity results are wire format.** The dataclasses in `models.py` that activities *return* (`PrSnapshot`,
`VerifyResult`, `MatchResult`, ...) are serialized into Temporal workflow histories. Renaming or removing a field
breaks replay of every in-flight workflow and fails the replay tests. Add fields with defaults; never rename.
Activity *inputs* (`JobArgs`, `StageInput`, ...) are not replayed, so they can change when worker and workflow are
deployed together. `VerifyResult.n` is the standing example of a name kept for this reason.

**Workflow code must stay deterministic, and the replay tests enforce it.** `tests/fixtures/*history.json` are
recorded histories of the workflows. If a change alters the sequence of commands a workflow issues,
`test_replay_*` fails. When the change is intentional, guard the new behaviour with `workflow.patched()` so a
running workflow keeps the path it started on, then re-record:

```
AUTODEV_RECORD_HISTORY=1 pixi run pytest tests/test_autodev_workflows.py -k recorded_history
MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay
MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay
```

**Keep the workflows thin.** Decisions that only need data in hand (`decide.py`, `crop_test.judge`) are plain
functions, tested directly. Everything with a side effect is an activity, called by name so the workflow module
never imports the activity implementations.

**No real paths or hostnames in tracked files.** Configs use placeholders; the real `mia.yaml` is git-ignored.
Before committing, `git diff | grep -n 'rokickik\|/nrs/'` should find nothing.

**Anything a sandboxed job wrote is untrusted.** Read it through `read_untrusted` (size-limited, no symlinks),
reduce it to a small typed result before it enters workflow history, and run it through `neutralize` before
posting it to GitHub.

**The stage tables are the state machine.** `STAGE_ACTIONS` beside each `STAGES` (`mia_data/config.py`,
`mia_buffers/config.py`) says which actions a stage accepts and whether a round is in progress there. buffer-api
refuses any other action and the page offers only these; a round in progress accepts only `cancel` (and, for a
buffer, what feeds it: `dropped`, and the force stop; for a dataset at `debugging`, the `go` of its next round). A new stage or action goes into the table first.

**Every stage write is awaited.** `set_stage` writes the subject's stage, its timeline entry and the dataset's
registry row, and the workflow waits for it: the registry's stage is what buffer-api checks an action against and
what the page shows as waiting on a person, so it may not lag the round. With buffer-api away the activity fails
and Temporal retries it.

**A round writes the registry, never its issue; nothing reads an issue.** Stages, comments and refusals are
timeline entries (`Activities._post`); the `MiaMirror` workflow copies them to GitHub. No label, comment, body,
close or reopen of a data or buffers issue is input, and a new feature gets an action route and a button, never a
label. autodev and the snippets repo stay on GitHub.

**The page holds no token.** Its buttons call `action` routes: no token, JSON only, this origin only, checked
against the stage table. Every other route the page needs is a read route (`tests/test_buffers_ui.py` enforces
it). An action can ask a workflow to do something; it cannot write what a workflow found, which stays behind the
worker token.

**Serving is authorized, never inferred.** `BufferService._serving` is an allowlist: a consumer draws only while
the buffer's latest event is `approved`, `ready` or `idle`. `retired` is 410; a round before its approve is `409
buffer not approved`; `needs-human` is `409 buffer needs a human`; no event at all is `409 buffer has no approved
event`. Nothing serves because no one said no, so a new event kind is unservable until it is added to `SERVING` on
purpose. `mia-buffers register` writes `approved` and `ready` itself; running it again with the same arguments
re-puts the config and volumes (frozen lists survive under the same artifact) and writes the two events. The
client's `BufferNotApproved` message repeats the service's reason.

**A lost lock kills the job.** `heartbeat_through` raises `LockLost` when the raw lock lapsed or changed hands;
`CsubJobs.wait` kills the job (`csub kill`) before the error propagates, and both `await_job` activities turn it
into a non-retryable `ApplicationError` of type `LockLost`, which the workflows route to `needs-human` or a failed
dataset. The job is killed even when the loss shows on the terminal poll: a workspace whose lock was lost is never
read from.

**Shared helpers take a `Deployment`, not a `Config`.** autodev's auth, Temporal connection, logging and ops
helpers are annotated with the `Deployment` protocol in `autodev/config.py`, which every package's config
satisfies. If a helper needs a new config field, add it to the protocol and to the configs.

**Budgets attach to the root request.** Every round is charged to one root, `<repo>#<number>`, pinned at
admission: the subject that caused it. A dataset round that a buffer or a preparation request started is charged
to that requester, whose root travels in the `go` it sent. Before every job the workflow asks `paused`, then
`reserve` (the job key is the reservation id, so a retry charges once) against the caps in `autonomy.root`; after
the job, `settle` records what it cost and fetched (a job whose cost is unknown is charged its reservation). A
refusal names the root, the cap and the usage and ends the round (mia-data) or fails that dataset alone
(mia-buffers); the concurrency cap alone makes the job wait (`poll_minutes`, doubling to 30) and ask again. The
caps bound an episode of work: a root with nothing running whose budget is older than `max_days` starts a fresh
epoch at its next job, while one busy for `max_days` straight is stopped. `mia pause` stops every round at its
next job with "paused by the operator"; a buffer round met mid-sourcing leaves its datasets `pending`, not failed.

**Approvals are assessments, authorization is a function.** Every human verdict is written to buffer-api's
`assessments` table (`by: human:<who>`, gate `crops` for a crop set or PR head, `staged` for an artifact) before
anything acts on it. A dataset verdict carries `action`, the id of the action it came from, and `/decisions` reads
the note the person wrote from that action. The action is allowed only when
`mia_core.judge.policy.authorized()` says so over
everything written about that subject: `publish_crops` and the `/approve <head>` comment in mia-data, the
`approved` event in mia-buffers (one assessment per staged artifact; a refused artifact's volumes are
unregistered and its dataset fails, the buffer serves at its floor or goes needs-human). A staged pyramid is never
reused (`_reuse`) while its newest assessment is a reject. `plan.auto_approve` writes no assessment.

**A judge sees evidence, never the author's reasoning.** A judge job (`mia_core/judge`) is `claude -p` with
`Read,Glob,Grep` only, no hosts of its own, over an evidence directory the worker builds (`evidence.build`): the
gate's question (`prompts/<gate>.md`, versioned by its hash), the artifacts under judgement and the deterministic
facts about them, listed with sha256 in `evidence.json`. Nothing from the author's workspace enters unless the
caller names that exact file (`IndependenceError` otherwise); the binding goes in without its `reasons`,
agent-written text is fenced and labelled as data. Its final message is one JSON verdict (`schema.parse_verdict`);
policy (`schema.effective_verdict`) turns a `reject` without a `bad` finding into an approve with notes and a
confidence under `judge.min_confidence` into an escalate, and the result is an assessment `by: judge` with the
evidence digest, model and prompt version. A `judge.model` other than `agent.model` is a precaution, not a rule.

**Shadow never blocks.** With `judge.<gate>.mode: shadow` the judge runs on every live round (the crop judge after
a passing set, the review judge after a pushed head), its assessment and comment are written, and the round goes
on to the human gate: `authorized()` ignores a judge's approve for a gate that is not `active`, and a judge job
that fails or writes garbage is an `escalate` on record plus a comment, never the round's end. Only the operator's
switches (the pause, the root budget) outrank it. The Decisions tab pairs each judge verdict with the newest human
verdict on the same subject (`Ledger.agreement`); `mia judge eval <gate> <dir>` scores a judge over an evaluation
set (`mia exec mia-data collect-judge-set <dir>` builds the crops set from human-reviewed rounds on disk).

**Revocation deletes nothing and stops everything.** `revoke` on a dataset (the page's *Revoke*) is accepted once
the dataset has saved crops: the activity looks for `manifest.json` in the dataset folder, and a dataset with
none has the action refused. The `revoke_dataset` activity writes a `reject` assessment superseding the
dataset's approve and one per staged artifact of the record, marks those artifact rows `quarantined`
(`quarantined_at`, `quarantined_by`), flips the dataset row to `revoked` (`sync_datasets` never merges over it)
and writes `revoked.json` beside the manifest last, so the folder is not prepared whatever else is on disk; the
dataset ends in `blocked` naming the artifacts and the buffers that drew from the record. From that moment
buffer-api draws nothing from a quarantined volume and counts it out of `servable` (`Ledger.live_volumes`),
whatever the buffer's workflow or the keeper have done yet. The keeper holds a quarantined asset for
`autonomy.quarantine_days` after its references are gone and, for a ready or idle buffer whose status lists
quarantined volumes, sends the `revoked` signal: the buffer unregisters those volumes, fails the dataset with the
reason and serves on at its floor, or ends in needs-human below it (*Retire* stops it altogether). A board approve
never writes over an artifact whose newest assessment is a reject: only a repair supersedes a revocation. A new
`go` on the dataset that saves its crops removes `revoked.json`, writes a `policy:repair` approve on each
quarantined artifact of the record and clears their quarantine marks, and the next stage write makes the row
prepared again. From the buffer side, `revoke` on a ready or idle spec carries the `records` to withdraw (the
page's *Revoke the ticked*; records not in the buffer are ignored), rejects and quarantines those records'
artifacts and takes the keeper's `revoked` path at once.

## Style

- Descriptive names everywhere except loop variables; no single-letter instance attributes.
- A one-line module docstring saying what the module is for, a sentence or two per class, and a short plain-language
  note on any method whose purpose is not clear from its name. Not full docstrings: this is not a public API.
- Comments and docs say what the code does. They do not say what it replaced or how it once worked.
- Type hints on parameters that take objects (`Config`, `GitHub`, `Pins`, ...). Import them under `TYPE_CHECKING`
  when a runtime import would create a cycle.
- Line length 120, enforced by the formatter. Long-line errors are ignored by ruff; keep lines readable by eye.
- A deliberate shortcut with a known ceiling gets a `# ponytail:` comment naming the ceiling and the upgrade path.
  `grep -rn "ponytail:" autodev mia_core mia_data mia_buffers` lists them.

## Pull requests

Branch from `main`, keep one topic per PR, and let CI run. Workflows run for days, so a PR that changes activity
or workflow behaviour should say in its description whether in-flight workflows are affected and what happens to
them on deploy. The autodev bot opens PRs here too; review them like any other.

## The registry

buffer-api's ledger (`<local_root>/service/buffers.sqlite`) is the registry both pipelines share. It holds:

- one row per **dataset**: state `in-progress`, `needs-human`, `prepared` or `revoked`, with the folder, binding
  hash and crop count of its saved crops. The state is the word a person reads, and the stage decides it
  (`mia_data.config.dataset_state`): `in-progress` while a round works by itself, `needs-human` whenever a person
  has to act (`review`, `failed`, `blocked`, `cancelled`, or a `done` round whose folder has no binding with a matching
  manifest, with the row's `reason` saying so), `prepared` after a `done` round with its crops saved. A new round
  on a prepared dataset takes it out of `prepared` until that round ends `done`. Code that asks whether a round
  is open reads the stage (`round_open`: the keeper before it evicts a raw cache, the list of datasets a
  request could prepare), never the state; the planner's pool is the `prepared` rows. mia-data writes the row
  as its stages change (`mia_core.registry.Registry`); `datasets sync` rebuilds the prepared rows from
  `storage_root` for records whose last round ended `done` or that have none, gives the others the folder's
  facts only, never merges over a `revoked` row and reports it as `revoked_kept`;
- the **catalog** records of each synced commit;
- the **assets** (raw caches and staged artifacts), the **buffers** with their volumes, events, boards, consumers
  and draws;
- the **subjects**, their **timelines** and the **actions** sent to them;
- **assessments** and **budgets**.

**Capability levels**, enforced per route (`ROUTES` in `mia_buffers/api.py`). Consumer routes (register a
consumer, `next`, `split`, `status`) and read routes (every other `GET`, and `POST /plan`) are open. Action routes
are the page's buttons: no token, `application/json` only, and a request whose `Origin` is another site's is
refused. Worker routes (every other `PUT`, `POST`, `DELETE`) need `Authorization: Bearer <contents of
service.token_file>`, else `401 {"error": "unauthorized"}`; use `mia_core.registry.Registry`, never a bare httpx
call. Consumer routes answer `409` unless the buffer's latest event is `approved`, `ready` or `idle`.

**Assessments and budgets.** Every gate decision is an append-only assessment, shaped by
`mia_core.judge.policy.new_assessment`: `PUT /assessments/<id>` (worker; the body's `id` must be the path's, and
`gate`, `subject.kind` and `verdict` must be in the policy's tuples; a repeated id is ignored, so a retry is
harmless), `GET /assessments/<id>` and `GET /assessments?subject_kind=&subject_ref=&gate=&limit=` (read, newest
first). Jobs are charged to their root request: `POST /budgets/<root>/reserve` (worker; `{reservation_id, kind,
usd, caps}` → `{ok, reason, usage}`, atomic against the caps `max_days`, `max_usd`, `max_fetch_bytes`,
`max_concurrent_jobs`, `max_jobs`; a known reservation id answers `ok` without charging again),
`POST /budgets/<root>/settle` (worker; `{reservation_id, usd, fetched_bytes, staged_bytes}`; a closed or unknown
reservation is a no-op), `GET /budgets/<root>` and `GET /budgets` (read). A budget row is `{root, since,
reserved_usd, spent_usd, fetched_bytes, staged_bytes, jobs, running, open: {id: {kind, usd, at}}, closed: [id]}`.
A volume leaves a buffer with `DELETE /buffers/<b>/volumes/<vid>` (worker; drops its frozen rows and a staged
reference no other volume needs); `GET /buffers/<b>/volumes` (read) lists `{id, artifact, record, missing}` rows,
and `GET /buffers/<b>/status` names the `quarantined` volumes (those whose artifact row is quarantined).

**Sync commands**, for a service with an empty ledger or one that missed writes:

```
mia exec mia-buffers catalog sync [--ref <catalog sha>]   # load the catalog records of a commit
mia exec mia-buffers datasets sync                        # rebuild the prepared rows from storage_root
```

**Subjects.** A dataset, a buffer spec and a preparation request are each a *subject*: `{kind, number, key, stage,
revision, issue, data}`, with the status word (`not started`, `working`, `waiting`, `stuck`, `done`) and the
actions its stage accepts, both read from one table per kind (`STAGE_ACTIONS` in `mia_data/config.py`,
`STAGE_ACTIONS` and `PREP_STAGE_ACTIONS` in `mia_buffers/config.py`). `GET /subjects/<kind>/<key>` reads one,
`GET /subjects/<kind>` lists a kind, `GET /needs` lists those waiting on a person or stuck (and the actions of the
last day that did not reach Temporal), `GET /lookup?q=` finds subjects from an issue number, an issue URL or a
key, and `GET /stream` is a server-sent event per change (`{kind, number}`; the reader fetches the subject again).
A subject's number comes from the clock (whole seconds since 2026-01-01; action ids are milliseconds since the
epoch), so a ledger restored from a backup issues no id twice.

**The timeline and the mirror.** `set_stage`, `comment`, `comment_plan`, `comment_prep`, `comment_drop` and
`refuse_label` write the subject's stage and a *timeline entry* to the registry (`POST /timeline/<kind>/<number>`,
worker; body `{body, marker, round, stage}`): an entry is appended once per marker, a stage always becomes the
subject's and raises its `revision`. `GET /timeline/<kind>/<number>` (read) answers `{subject, entries}`.

Every write kicks the subject's **mirror**: buffer-api starts the `MiaMirror` workflow (id
`mirror/<kind>/<number>`), or signals it when it runs. The mirror opens the subject's GitHub issue when it has
none, posts each entry not yet posted as a comment, in order, once (the entry's marker is in the comment, so a
pass that died before its write-back, `PUT /timeline/<kind>/<number>/<seq>`, finds its comment again), then
leaves the subject's stage as the issue's only stage label. It retries without limit, so GitHub being slow or
away only delays the issue; nothing reads the issue back. mia-data's worker runs the mirror for datasets,
mia-buffers' for buffers and requests. buffer-api therefore needs to reach Temporal: it uses `temporal.address`
of its own service block, its own namespace and task queue for buffers and requests, and for datasets the
namespace and name of the service whose `app` is `mia-data`. It connects on first use, so it starts and serves
with Temporal away; a kick that fails is logged, and a start kicks every subject that has an entry not yet
posted. `init-labels` of each app creates the stage labels the mirror sets.

**Actions.** A button on the page is a `POST` to an action route; for a dataset,
`POST /datasets/<record>/actions` with `{action, key, revision, note}`. With `curl`:

```
curl -X POST -H 'Content-Type: application/json' -d '{"action": "go"}' <service>/datasets/<record>/actions
```

In one request the service: checks the action against the subject's stage table (`409` with what the stage does
take); for `approve` and `reject`, compares `revision` with the subject's (`409` when the subject moved on since
the page showed it); records the action (`pending`), whose id is the workflow's event id; starts the kind's
workflow with the action as its signal (`cancel` is a plain signal to the running workflow); marks it `sent` and
appends it to the timeline, or marks it `failed` and answers `503`. Nothing retries in the background: a `key`
sent again is the same action sent again under its id, which the workflow drops if it already handled it, and the
page's *Retry* does exactly that. A `key` sent again within two minutes of its click is not checked twice; later,
a Retry of an undelivered action is checked against the stage table like a new click. A first `go` on a record of
the synced catalog creates its subject. A worker may send an action with the token and name its `actor` and the
`root` the round is charged to.

A round reads the registry: `admit` takes the record from the subject and the root from the action,
`read_feedback` returns the reject's note, `refuse_label` marks the action `refused` (`PUT /actions/<id>`) with
the reason.

| subject | routes | actions |
|---|---|---|
| dataset (`#datasets/<record>`) | `POST /datasets/<record>/actions` | `go` (*Start*, *Run again*, at `review` *Restart*), `approve`, `reject` with `note` (*Reject…* opens a dialog for it), `revoke`, `cancel` |
| buffer spec (`#specs/<name>`) | `POST /specs`, `POST /specs/<name>/actions` | `go` (*Plan*), `approve` (*Run* at the plan, *Approve* at the board), `keep`, `retire`, `revoke` with `records`, `dropped` with `record` (*Files are in place*), `cancel`, `stop` |
| preparation request (`#requests/<key>`) | `POST /prep`, `POST /prep/<number>/actions` | `go`, `cancel`, `stop` |

`POST /specs` with `{spec}` saves a buffer spec under its `name` (a name that exists is an edit of that subject,
never a second one); with `action: go` it also runs it, and `plan_first: true` makes it a round that waits at the
plan (the workflow's `plan` signal). The page always sends `plan_first`, so a round a person starts stops at its
plan and the approve there is the button *Run*. The go's payload is the spec text, the round runs exactly that
(`admit` reads the action), and its timeline entry shows it. `POST /prep` with `{spec}` files a preparation
request as a new subject `prep-<number>` and runs it.

- `cancel` ends a round in progress (`planning`, `plan-review`, `sourcing`, `staged`; a request at `requested`; a
  dataset from `matching` to `merging`): its dataset tasks are cancelled, every cluster job it waits on is killed by
  id, locks are released, reservations settled, a buffer round's buffer is retired, and the stage is `cancelled`.
  A buffer that is `ready` or `idle` is being served: the route refuses the cancel, and the workflow refuses one
  that crossed. Cancelling a request also cancels the dataset rounds it started (`prep_cancel`: the datasets whose
  latest go this request sent and that are still in a round), no others. If the registry says a round is in
  progress and Temporal has no run for it, the cancel route writes `cancelled` itself.
- `stop` is the force stop for a run that does not answer a cancel: the route terminates the workflow (a
  terminate needs no replay), retires the spec's buffers with their `retired` events and sets `retired` (`done`
  for a request). It does not kill a cluster job already running, nor a request's dataset rounds; its entry says
  so.
- `dropped` sends the workflow's `dropped(record)` signal. It is refused while `<raw_root>/<record>/incoming/` is
  empty or holds a file modified in the last minute: the workflow takes any non-empty file and records its hash,
  so a click during a copy would record a truncated file.

**Between a click and the workflow's answer.** A sent action changes no stage: its workflow does, a moment later.
Until then a subject's view carries `asked` (`{id, action, at, wait_s}`): the action that was sent and that the
workflow has not answered. Any stage or entry the workflow writes, a refusal, or `ASKED_WAIT_S` (60 s, so a
worker that is away cannot hold the buttons) ends it; a cancel or a stop the service settles itself is never
asked. The page disables every action button of a subject that has `asked` and marks the one that was pressed.
It is kept in memory (`BufferService._asked`), and the stage table is not bent: `actions` lists what the stage
accepts.

**How a cancel ends a round** (all three workflows; `_guard`, `_cancel_round`, `_act`). The round runs as a task
beside the workflow's main coroutine. When a cancel arrives, the round starts no new activity: `_act` stops the
task there. An activity already in flight is left to answer (up to `CANCEL_PATIENCE`), so a job it submitted, a
lock it took or a reservation it opened is known to the code that undoes it. A task that is waiting (a job, a
lock, quota, a drop, a verdict, a snippet PR) is cancelled at its wait. The job is killed first (`kill_job`),
then its reservation is settled and its raw lock released: the one-writer rule holds until the job is dead. A
job that cannot be killed ends the round in `failed` (a dataset) or `needs-human` (a buffer, a request) naming
it, never in `cancelled`. A round that reached
its end while the cancel was on its way keeps its stage and the cancel is refused. Clean-up activities pass
`cleanup=True`; anything a best-effort `except ActivityError` wraps cannot swallow a cancel, because the cancel
is raised as `asyncio.CancelledError`. A queued cancel is never rolled over (`_roll_due`).

**Health.** `GET /health` says whether Temporal answers and whether a worker polls the task queue of each service
in `mia.yaml` (a queue carries its service's name). The page asks every minute and keeps a line at its top for as
long as one is away, with the `mia restart` that brings it back. Temporal remembers a poller for about five minutes,
so a worker that stops is reported that long after; one that never started is reported at once.
`GET /run/<kind>/<number>` is the Temporal status of a subject's latest run.

**Planning from outside.** `POST /plan` plans a spec against the synced catalog and the registry; datasets that
are not `prepared` are out of the pool with the rule `not prepared`. The body is `{"spec": "<yaml text>"}`; read
the spec from a file:

```
python -c 'import json,sys; print(json.dumps({"spec": open(sys.argv[1]).read()}))' spec.yaml |
  curl -s -XPOST http://host.example.org:8765/plan --data-binary @-
```

**Restart order.** buffer-api first, then the workers: `BufferConfig` is strict, so a worker that sends a config
key the running service does not know gets 422 on `PUT /buffers/<id>`.

## A dataset round

[`mia-data-workflow.md`](mia-data-workflow.md) walks through a dataset's round: match, crops, review,
saving, and the snippet PR loop.

## A buffer round

The deployment reads its `mia-buffers` block of `mia.yaml` (`mia.yaml.example`; `buffers_repo` is the repo of the
mirrored issues, `storage_root` is mia-data's, `raw_root` defaults to mia-data's `<work_root>/_cache` so downloads
are shared). `mia up mia-buffers` starts the broker (on the generated csub policy, below), buffer-api (`mia-buffers
serve` on the host, bound to the host and port of `service.url`, log `mia logs mia-buffers api`) and the sandboxed
worker, which may reach that host through the sandbox proxy. `service.url` must name the host (not `localhost`: the
sandbox bypasses its proxy for loopback names, and inside it they point at the sandbox's own empty loopback). By hand:
`mia-buffers init-labels`, `mia-buffers serve`, `mia-buffers worker`, `mia-buffers schedule apply`.

**Hosts.** Every broker `mia up` starts runs `csub-broker --policy <csub.policy_dir>/broker.toml`, a file
`mia_core.hosts` generates from the operator's `csub.base_policy` plus `<policy_dir>/approved-hosts.jsonl` (one line per
host: `host`, `at`, `by`, `kind`, `number`). Before a run starts, the page posts to `/hosts/missing` and lists the hosts
the run needs that the policy lacks: the hosts of the URLs its catalog records name (a dataset round, a request) or its
saved bindings name that its catalog records also name (a buffer at `plan-review`, over its round's `plan.json`
pool), their `network.redirect_hosts` targets and the package hosts. Ticked hosts travel as `approve_hosts` on a
dataset's `go`, filing a request (`POST /prep`), a request's `go` and a buffer's `approve` at `plan-review`; any other
action carrying a non-empty `approve_hosts` (a buffer's `go` too) is refused with 422. buffer-api, the only appender,
accepts only hosts that run needs (never a GitHub host, an IP literal or the host of `service.url` or
`temporal.address`), appends, regenerates (503 if it cannot) and posts a timeline entry. A broker loads its policy file
on every request, so an approved host needs no restart: the next job sees it. Every generation checks that `policy_dir`
is under home, apart from every read-write sandbox mount, and that its `broker.toml` is not the base policy file.
Withdrawing an approval is a hand edit: remove the host's lines, then `mia up` (or the next approval) regenerates. A
redirect target still needs `network.redirect_hosts`, and a pattern or a host wanted before any run is added to the base
policy by hand. `mia exec <service> ...` goes through the service's broker when it is running. An autodev agent job
names its `agent.allow_hosts` plus the exact hosts its broker's policy allows, at most 64 (`autodev.agent.agent_hosts`).

1. **The go.** A `go` action carries the spec text (the page's *Plan*). With `plan_first` the round waits at its
   plan; without it the round stages first and asks once, at `staged`.
2. **Planning.** The worker pins the round (`state_dir/rounds/<round>/`: `spec.yaml`, `pins.json`), archives the
   catalog at `catalog.ref` under `work_root/_catalog/<ref>/` and builds the plan from the catalog's records and
   the registry's prepared datasets, so the plan, the page and the worker agree on "prepared" (one row per record
   selection admitted, the rest as a count per rule under `excluded`; a record that fits but is not prepared is
   out with the rule `not prepared`). When the registry has never seen the round's catalog ref, the activity
   syncs it first. `plan.md` and `plan.json` land in the round directory and in the browsable
   `work_root/issue-<n>/<round>/`. Everything sourcing needs is checked here: a record mia-data could not place
   (no modality family, the rule behind `<dataset folder>`; a record without one organism is filed under
   `unknown`, one of modality `other` under the family `other`) or whose voxel size is unknown stays out of the
   pool with that reason, and each pool row's voxel size travels to its stage job (1 nm for the plane axis a 2D
   stack keeps). The plan is posted to the timeline (pool table, cut at 60k characters); an
   invalid plan, or no `tensorizers/<name>` in the harness checkout, ends in `needs-human`. The plan's hash
   (sha256 of the `plan.json` text) goes to the buffer.
3. **The plan gate** (a `plan_first` round). The stage is `plan-review`. `approve` (the page's *Run*) starts
   sourcing; a new `go` with an edited spec plans again.
4. **Sourcing.** The buffer is created in buffer-api (id = round id, `i<number>-e<action id>`). Per dataset, in
   parallel (`sourcing.parallel` stage jobs at a time, heaviest first): the binding is read from
   `<storage_root>/<dataset folder>/binding.yaml`, prepared only with the `manifest.json` mia-data saved for
   it (`mia_data.publish.prepared_row`, the registry's rule too); the raw files are fetched and the pyramid staged
   (see "The cache"); then every volume is registered. `read_stage` registers what the job left with the registry
   (`PUT assets/staged/<staged dir>` with the binding hash it was built from, `PUT assets/raw/<record>` from
   `files.json`), and registering a volume references both for the buffer (`refs`); `read_binding` asks the
   registry first, and a pyramid staged earlier under the same artifact id and still on disk is re-read and
   registered without a job. A dataset whose hosts are not allowed to jobs, or whose binding uses `manual://`
   URLs, waits instead: a timeline entry lists `incoming/<name>` ← url pairs, a person places the files under
   `raw_root/<record>/incoming/` and presses *Files are in place*, and the worker verifies them (regular,
   non-empty files), moves them into the cache under the harness's key and lists them in
   `raw_root/<record>/files.json`, which the harness reads instead of a HEAD request. A dataset with no prepared
   binding fails with "not prepared". Each dataset ends `servable`, `failed` or `dropped`, with an entry.
5. **Staged, then ready.** Sourcing runs until no dataset has a job left: every one servable, failed, dropped, or
   waiting on a drop, quota or lock. With something servable the stage is `staged` and the board is posted: each
   dataset's outcome (a failure with its `stage.json` error), each weighting group against `ready.min_fraction`,
   volumes per split, bytes, who is waiting. `approve` serves the buffer as it is: each group's servable fraction
   becomes its floor (`accepted` in the state, what a later degraded rebuild is judged against), the `approved`
   and `ready` events are written, the stage is `ready` and its entry holds the `BufferDataset` cell, with a short
   group named there. No job runs between the approve and `ready`. buffer-api answers consumers `409 buffer not
   approved` until the `approved` event. A waiting dataset keeps its place and joins when it lands (a new board
   entry follows); the day limit (`sourcing.dataset_timeout_days`, default 14) drops waiters only until the
   approve. `plan.auto_approve` approves the board without a person when every group meets the fraction. A `go`
   at the gate starts a new round and retires the staged buffer; the keeper retires a staged buffer nobody
   approves for `retire_after_days`. Nothing servable once the day limit has dropped the waiters is
   `needs-human`.

**The status board and events.** The workflow writes one row per dataset on every status change
(`PUT /buffers/<id>/datasets/<record>`, `GET /buffers/<id>/datasets`; the `DatasetStatus` values), and each stage
posts a lifecycle event (`POST`/`GET /buffers/<id>/events`; kinds `created`, `planned`, `sourcing`, `staged`,
`approved`, `ready`, `idle`, `retired`, `needs-human`). Events need no buffer config: `created` and `planned` precede registration.
`GET /records?ref=` serves the catalog's records (default: the latest synced ref).

**The recipe pin.** `PUT /buffers/<id>` carries `recipe` (tensorizer, params, `harness_commit`, `env_digest`) and
`plan_hash`. `BufferDataset` compares the recipe's `harness_commit` with its checkout (`MIA_HARNESS_COMMIT`, else
`git rev-parse HEAD` in the tensorizers directory) and raises on a difference.
`BufferDataset(..., allow_recipe_mismatch=True)` warns instead and records the checkout's commit in each sample's
`meta["harness_commit"]`.

**The split policy.** Each pool row of the plan carries `split = {policy, fractions, seed}`: `holdout` (a record
in `splits.holdout_ids`) puts every plane in test, `fractions` is a shuffle seeded by `splits.seed` and the volume
id, cut by the largest-remainder rule. Registration applies the row; a row without a `split` gets the spec's
fractions and seed with a logged warning. A non-empty `splits.stratify_by` is a plan error.

**One ticker.** Lock and quota waits share one timer per round: it ticks every `poll_minutes`, doubling while no
dataset changes status, to at most 30 minutes, and runs only while someone waits. mia-data keeps its own backoff.

## The cache

**The raw lock.** No job writes a record's raw cache unless its worker holds `<local_root>/locks/raw/<record>`, a
file with `{owner, taken_at, heartbeat_at, ttl_s}` (TTL 900 s). Every `await_job` poll heartbeats it; a stale file
is renamed aside as `<key>.stale-<time>`. A wait whose heartbeat finds the lock gone (`mia_core.locks.LockLost`)
kills its job (`csub kill`) and fails the round; a stale lease is never evidence that its writer stopped. mia-data
takes it around trusted crop jobs (owner `mia-data <round> crop`); mia-buffers from before the
fetch through the stage (owner `mia-buffers <round> <record>`), released while the dataset is blocked on quota and
re-taken after; the keeper takes it (owner `keeper`) before evicting a record's assets and skips a held record.
Snapshot extraction uses `snapshot/<sha>`. `GET /locks` lists the holders. A worker waiting for a lock (or for
quota) polls after `poll_minutes`, doubling each try up to 30 minutes. A re-take that had to wait for another
owner re-reads the binding and the raw cache before going on. Hand-run harness commands bypass all of this: run
them only on records no round is touching. `assets sync` on a host other than the workers' sees none of their
locks, so there it refuses to run without `--dry-run`.

**The fetch phase.** Per dataset the worker runs `harness fetch binding.yaml --record record.yaml --out
fetch.json` with the cache read-write, the byte cap `limits.fetch_bytes` and the walltime
`limits.fetch_walltime_min`, up to `limits.fetch_attempts` attempts that resume from the cache (`partial`),
`sourcing.parallel_fetch` fetch jobs per buffer. `fetch.json` lists one outcome per URL: `cached`, `fetched`,
`stream`, `pending` or `failed`; a missing `fetch.json` counts as partial. The fetch is skipped when `files.json`
already lists every binding url: a plain file is listed by the HEAD that fetched it, a `<zip>::<member or glob>`
url only once `harness fetch` has every matching member in the cache (source `members`); the zip's own entry
proves nothing about members, as mia-data's verify reads only a sample of them. The harness also caches the zip's
directory, so the offline stage expands a glob without a request. The binding is re-read under the lock: if the
previous holder built the artifact, it is reused ("staged by <holder> while this buffer waited"). A fetch still
partial after its attempts names the last job's id, state and exit code; `mia-buffers check` verifies the harness
checkout has `harness fetch`.

**Staging.** The stage job runs `harness stage`, then `harness check-stage`, offline (cache read-only, no hosts),
unless a URL streams: then the stage keeps the cache read-write and the hosts. A dropped (hand-placed) dataset
stages offline. The job writes one sparse OME-Zarr 0.6 pyramid per volume, holding only the chunks the planned
samples touch, with its derivation recorded in the Zarr's attributes; `check-stage` verifies it against the 0.6
schemas. The stage job reads its byte cap from `MIA_HARNESS_STAGE_BYTES` (config `limits.stage_bytes`, default
`limits.job_bytes`).

**An artifact id is a recipe hash** (`jobspec.artifact_id`): binding hash, levels key, voxel size, harness commit,
env digest and plan digest. A pyramid on disk is reused only when all of them match. Changing how the harness
derives samples changes plan digests, so every volume restages; a buffer that re-registers a volume under a new
artifact loses its frozen eval lists by design.

**Save by rename.** A stage job writes `<staged root>/<levels key>/<record>/.building-<artifact>-<job key>`,
never the final directory. When `stage.json` says `ok`, the worker checks that its `artifact` block (written by
the harness: every input the artifact id hashes) names this job's artifact id, refuses a build with no block or a
foreign id, then renames the build to `<artifact>`. The saved directory is never written again. A rebuild
lands under the same id through the same rename; a retry that finds the final directory reuses it.

**Dataset statuses** during sourcing: `pending`, `waiting-dataset`, `waiting-lock`, `fetching`, `staging`,
`blocked` (quota), `waiting-drop`, `servable`, `failed`, `dropped`. `blocked` and `waiting-drop` are dropped at
`sourcing.dataset_timeout_days`, `waiting-lock` is not. A blocked dataset holds no fetch slot (it waits for quota
first) and re-checks the quota once it has waited for one. `check_quota` records `wanted` (the record's catalog
size) and `blocked_at` on the raw row; the next fetch clears them.

**Asset rows.** `state` is one of `fetching`, `staging`, `waiting-drop`, `ready`, `partial`, `missing`, `evicted`;
rows also carry `bytes`, `job`, `last_used` and `scanned_at`, and raw rows `stream` (URLs that stream) and
`files`; a revoked staged row carries `quarantined`, `quarantined_at` and `quarantined_by`.
`PUT /assets/<kind>/<key>` merges fields into the row. `GET /assets/<kind>?state=&record=` filters and adds a
`bytes` total. `POST /sync/assets` (worker token) scans the roots, skips a missing or empty root, lists
`.building-*` directories (staged builds and eviction asides in both roots) and sets staged `last_used` from the
draws; a row without `last_used` gets its first scan's time. `raw_status` and a reusable staged find touch
`last_used`.

**The keeper.** `MiaCacheKeeper` runs on the schedule `<deployment>-keeper` every `keeper.minutes`
(`mia-buffers schedule`, which `mia up` runs). One pass:

1. datasets sync and assets sync (each with a 10-minute timeout);
2. stale builds: a `fetching` or `staging` row whose csub job is gone becomes `partial` or `missing`;
3. `.building-*` directories older than `quota.hold_hours` are removed (a build whose artifact row says `staging`
   is spared, whatever its age), and so are empty raw directories (a job's mount point that never received a file;
   the scan does not register one, and drops the row of one it had);
4. eviction per root until usage is under `quota.<kind>_bytes - quota.headroom_bytes` (for raw, minus the largest
   `wanted` of a fetch blocked within `hold_hours` when that is more than the headroom), oldest `last_used` first,
   free assets only: no refs, `ready`, unused for `hold_hours`, not within its quarantine hold, and for raw no
   mia-data round in progress on the record and the record's lock not held. An evicted row keeps `state: evicted`,
   `bytes: 0`. A directory is renamed to `.building-evicted-*` before removal, so a half-removed artifact is
   never reused;
5. the lifecycle signals of ready, idle and staged buffers (see "After ready");
6. a catalog sync of `main` when older than `keeper.catalog_sync_hours` (mark file
   `<state_dir>/keeper/catalog-synced-at`).

The activity heartbeats every minute from a background task, whatever step it is in. Its report goes to the
registry (`PUT /keeper`) and the Cache tab shows it.

`quota.*_bytes: 0` means unlimited (the default). When nothing is evictable and a root is still over quota, the
report says by how much. Only the buffers worker's keeper deletes under the roots; buffer-api never does. Keep
`quota.hold_hours` above the queue wait plus the stage walltime, for a build whose row write was lost.

```
mia exec mia-buffers assets sync --dry-run   # report what a pass would do
mia exec mia-buffers assets sync             # a pass by hand; refused on a host without <local_root>/locks
```

`--dry-run` still runs the two syncs (reads and idempotent writes) and the catalog sync with its mark, but deletes
nothing and writes no eviction, stale-build or over-quota state.

## After ready

**Idle, keep, retire.** The keeper reads `GET /buffers` for each ready or idle buffer of a round (one buffer's
failure, such as buffer-api refusing its status, is an error entry; the others are still watched). No consumer
seen and no newer event for `idle.hours` (spec, default 72) signals `idle`: the stage becomes `idle`. `keep`
returns it to ready. After `retire_after_days` (spec, default 14) of idleness, and only when no consumer was seen
since the idle event, the keeper signals `retire`; the workflow acts on it only while the stage is `idle` (a keep
since makes it stale). A `retire` action retires a ready or idle buffer. Retire calls `DELETE /buffers/<id>/refs`:
the buffer's refs are released (the keeper may evict the data once the quota asks) and its draws are gzipped to
`<state_dir>/archive/<buffer>.draws.jsonl.gz`, from a read-only connection outside the service's lock, then
deleted up to the last seq seen at the start (a draw recorded meanwhile stays for the next retire). The worker
then writes the `retired` event, after which `next`, `split` and `POST consumers` answer `410 buffer retired`.
Volumes and the config stay for the page. Both thresholds travel in the buffer config.
`mia exec mia-buffers retire <buffer>` does it by hand (refs, archive, then the `retired` event, so consumers get
410; the spec's stage stays as it is) and exits 1 when buffer-api refuses.

**One live buffer per spec.** A new go after a `needs-human` or cancelled round retires that round's buffer
before planning (refs released, `retired` event, an entry); a go at a gate retires the staged buffer. The page
lists a spec's newest round first and marks the older ones superseded. `GET /buffers` lists each buffer with its
latest event, volume and `missing` counts, servable volumes per split, the newest consumer `last_seen` and
today's draws.

**Degraded.** `GET /buffers/<id>/status` stats each volume's pyramid directory (when the service has a staged
root) and lists the missing ones, which `draw`, `next` and `split` skip. The keeper signals `degraded(records,
reason, buffer)`. The workflow asks buffer-api which of those records are still missing (`missing_records`; a
re-send for one rebuilt, rebuilding or failed is ignored) and stages them again from the raw cache, under the
same artifact id, with a job key and build directory of their own; the save sets the broken final directory
aside as `.building-stale-*`. The rebuild runs alongside the ready loop: a `go` or a retire abandons it. The spec
goes `needs-human` only if a weighting group falls below its floor; a record whose raw cache is gone fails.

**Whose signal.** The keeper's `idle`, `retire` and `degraded` name the buffer they are about; the workflow
ignores one about another buffer, but for a superseded one: a `go` from ready or idle records the buffer it
leaves registered in `BufferState.superseded`, and the keeper's `retire` of it releases it (entry "superseded
buffer `<id>` retired: refs released"); its `idle` writes the `idle` event on that buffer (no stage), so the
keeper's retire rule runs `retire_after_days` later. A `keep` or `retire` that reaches the workflow while there is
no ready or idle buffer is refused at once: while datasets source with "N datasets still sourcing; ask for
`<action>` again once they settle", otherwise with "this spec has no ready or idle buffer".

**Rebuilds of a shared artifact.** Two buffers whose datasets share an artifact both see it degraded; the first
to take the raw lock rebuilds it. The second asks buffer-api again under the lock and registers the rebuilt
pyramid as it is. A failed build next to a saved artifact (not a rebuild of it) leaves that artifact's row
`ready`.

## Preparation requests

A buffer never asks mia-data for a dataset; a preparation request does. The page files one (`POST /prep`) for the
datasets ticked on the Datasets list or the ones a new buffer's plan needs. Its spec is YAML, with `records` or
`select` (exactly one):

```yaml
schema_version: 1
kind: preparation
catalog: {repo: <owner/catalog>, ref: <40-hex commit>}
records: [<record id>, ...]     # an explicit list, or
select: {...}                   # the buffer spec's select block, run over the git catalog snapshot at catalog.ref
again: false                    # optional; true also gives a new round to datasets that are prepared or that stopped
timeout_days: 14                # optional, default 14
max_records: 25                 # optional; a request for more records must say so here (each is an agent run)
```

A `select` only selects: no fit, no weights; it reads the catalog's records at `ref`, not the registry.
`PrepSpec` in `mia_buffers/spec.py` is the model. An id that is not in the catalog at `ref`, a `select` that
admits nothing, more records than `max_records` (default 25), or more than 200 (`MAX_PREP_RECORDS`, a
`# ponytail:`: split a larger request) ends the request in `needs-human` at once. Admission reads the spec its
action carries, once.

**Stages.** `requested`: the records are admitted and handed to mia-data. `done`: every record is `prepared`.
`needs-human`: the request stopped, and its entry names the records that did not settle, each with its last
registry state. `cancelled`: a person cancelled it.

**What it does.** Per record, `prep_nudge` reads the registry first: a row that is `prepared` or `needs-human`
settles the record, unless the request says `again`: then the dataset is sent a `go` all the same, and its row is
set to `in-progress` at once, so the request follows the new round instead of settling on what the last one
left. A dataset whose crops wait for a review is `needs-human`, so it settles too. Otherwise the dataset's
subject says where it stands: one in a round is left to it (`in-progress`), one at `done` whose folder has no binding with a matching manifest is `done-unpublished`, and
any other is sent a `go` through buffer-api with the worker token (`Registry.send_action`): actor
`prep:<number>`, the request's root (who is charged), and the key `<round id>/<record>`, so a retried activity
sends no second go. The outcome is `opened` for a dataset started for the first time and `nudged` otherwise, and
`unknown` for a go that failed (its error is the note). One entry lists each record with its outcome. A failed go
and a `done-unpublished` record settle as `needs-human` at once (a person must act either way); the others are
followed. The workflow then polls `GET /datasets` on a backoff (`poll_minutes` doubling to 30 minutes). A record
settles when its row is `prepared` or `needs-human`. The request ends `done` when all records are settled and
none is `needs-human`. Otherwise it ends `needs-human`, but only once every record has settled or `timeout_days`
have passed: a `needs-human` record is reported then, not when it turns so.

Workflow id `mia-buffers/<buffers repo>/prep-<number>`, round id `p<number>-e<action id>`. A buffer round hands
nothing to mia-data: a dataset that was prepared when the plan was made and is not when it is sourced fails with
"not prepared".

**`evaluate.enabled`.** mia-data's config has `evaluate: {enabled: false}`. It is a switch only: no evaluate job
exists.

## Serving a buffer by hand

The `MiaBuffer` workflow runs these steps for a round; each can also be run by hand.

1. **Plan.** `mia-buffers plan docs/specs/buffer-2d-gray.example.yaml --catalog-dir <catalog checkout> --json
   plan.json` writes the data pool with a weight, a voxel size (documented or assumed) and a channel recipe per
   record.
2. **Stage** each dataset you want to serve with the mia-snippets harness (see that repo's `SPEC.md` §7):
   `harness stage binding.yaml --snippets <snippets dir> --record record.yaml --plan plan-rule.json
   --voxel-size voxel_size.json --out <staged root>/<record id>/`. The output holds `stage.json` and one sparse
   OME-Zarr 0.6 image `v<i>.zarr` per raw volume, with only the chunks of the planned samples written. The worker
   path (`prepare_stage`) writes `plan-rule.json` and `voxel_size.json` next to the job; for a manual stage write
   them by hand:

   ```
   plan-rule.json    {"artifact_id": "<artifact id>", "levels_key": "<levels key>", "harness_commit": "<sha>",
                      "env_digest": "<digest>", "rule": {<the plan row's plan_rule>}}
   voxel_size.json   {"x": {"nm": 8.0, "source": "record"}, "y": {...}, "z": {"nm": 8.0, "source": "assumed",
                      "basis": "<why>"}}      # source: record | assumed | synthetic | file; basis is optional
   ```

   `file` is the harness's own (it reads the size from the image file); the worker never writes it.
3. **Serve.** `mia-buffers serve` runs buffer-api from the configuration: host and port from `service.url`, the
   ledger at `<local_root>/service/buffers.sqlite`, backups under `<state_dir>/backups/` every `keeper.minutes`,
   and every worker write needs the token from `service.token_file`. It holds no GitHub credential and never
   reads pixels.
4. **Register.** `mia-buffers register plan.json <spec> --staged-root <staged root> --url http://host:8765
   --buffer <id> --config mia.yaml [--plan-hash <hash>] [--val-size 256 --test-size 256]` creates the buffer and
   registers every pooled, staged volume. Planes are split by each plan row's `split` policy (split key = volume
   plus plane). The buffer pins its recipe (tensorizer, params, the harness commit and env digest read from the
   configuration's harness pin) and the plan's hash (default: sha256 of the plan file's text). Unstaged records
   are listed as skipped, volumes the coarsest crop does not fit as refused. It then writes the `approved` and
   `ready` events, without which buffer-api refuses consumers.
5. **Consume.** In an environment with the harness importable (the mia-snippets `harness/` pixi env, with this
   repository on `PYTHONPATH`), plus torch and muvit for training:

   ```python
   from mia_buffers.client import BufferDataset

   URL, TENSORIZERS = "http://host:8765", "<mia-snippets checkout>"  # or export MIA_BUFFERS_TENSORIZERS
   train_ds = BufferDataset(URL, "<id>", "train", n_samples=4000, tensorizers=TENSORIZERS)
   val_ds = BufferDataset(URL, "<id>", "val", tensorizers=TENSORIZERS)
   ```

   `MuViTMAE2d(in_channels=train_ds.n_channels, levels=train_ds.levels, ...)` takes it as it is; the `DataLoader`
   takes `collate_fn=BufferDataset.collate`, because a buffer mixes datasets whose label layers differ and
   torch's default collate needs identical samples (the batch keeps the keys every sample has, `meta` as a list).
   Train is a stream of crops (`idx` is ignored, `len` is `n_samples`); val and test replay a frozen list that is
   identical every epoch. Each DataLoader worker registers its own consumer. `GET /buffers/<id>/status` shows
   servable volumes per split, draws per volume and consumers.

**Samples.** A volume's `samples` (from `stage.json`) register as candidates. Train draws a candidate by weight;
val and test lists are drawn from the candidates without replacement, so a list can run out: the response says
`exhausted` and `size` is capped by the candidates available.

**Normalisation statistics.** `harness stage` records, per raw volume and channel, a sorted sample of up to 4,096
level-0 values from 8 written level-0 chunks per channel (`stage.json` → `volumes[].intensity_sample`). At
registration, for `sampling.normalize.scope: volume`, each volume body carries `normalize: {lo: [per channel],
hi: [per channel]}` at the spec's percentile range from its own sample; for `scope: dataset` the samples of the
record's volumes are pooled first; for `scope: patch` (the default) the field is `null`. Descriptors carry the
volume's `normalize`, `BufferDataset` passes it to the tensorizer as `stats` (with `scope` from the buffer
config), and `muvit-2d` then scales by those bounds instead of the patch's own percentiles
(`meta.normalize_scope` says which). A staged directory without a sample has its volumes refused at registration
under a non-`patch` scope, with a reason that says to re-stage.

## The page

buffer-api serves one page at `/` on `service.url` (static files under `/ui/`). It holds no token: it fetches
read routes (and `POST /plan`, which carries the spec text and writes nothing), and its buttons call the action
routes. `tests/test_buffers_ui.py` fails if the page requests a route that does not exist or is a worker route,
uses an `Authorization` header, or calls a host other than github.com and Google Fonts.

The page is plain files under `mia_buffers/ui/`, with no build step: `index.html` (the shell), `app.css`, the two
documents of the Docs view, and ES modules that the browser loads as they are. A module imports only modules
above it in this table, never in a circle:

| module | |
|---|---|
| `spec.js` | the YAML the page writes (a buffer spec, a preparation request); no DOM |
| `dom.js` | nodes built from text (never parsed HTML), numbers and times, links and addresses, pills, the tooltip, cards, a lifecycle's line |
| `api.js` | the page's one `fetch`, the banner its errors go to, and what the service said at boot |
| `table.js` | a table paged by the server |
| `markdown.js` | a timeline entry's markdown as DOM nodes |
| `actions.js` | what each button says and does, the dialogs before a send, a subject's action bar, the Prepare button |
| `designer.js`, `buffers.js`, `datasets.js`, `operations.js`, `subjects.js` | the views |
| `app.js` | the Docs view, health, the stream, hash routing and boot |

Every file is sent with `Cache-Control: no-cache`, so a browser never runs the modules of two deployments
together. `tests/test_buffers_ui.py` imports the modules under node (a test dependency in `pyproject.toml`) over a
small fake DOM, and reads all the files as one text for the rules about the page as a whole; a rule about the
code is compared without whitespace (`has()`, `count()`), so it holds however Biome lays the code out.
`ui/package.json` only tells node that the files are modules; the service does not serve it.

Each tab is one kind of thing, and each thing has one page:

- **Needs you** reads `/needs`: everything that waits on a person, with the buttons that answer it.
- **Datasets** reads `/catalog/datasets`: every record of the newest catalog joined with its preparation row, raw
  download, staged artifacts and buffers; filters by state, modality, dims and licence. Every row has a tick (the
  header's and *Select all* tick the page that is showing). While something is ticked, a box says how many datasets are
  selected and offers *Prepare* and *Create buffer*. The ticked are prepared in one request (one
  `POST /prep` that says `again`, so a prepared dataset gets a new round), or opened in New buffer as a spec that
  selects exactly them (`specFromDatasets`: one `*/<id>.yaml` glob per dataset in `select.paths`). `#datasets?state=<state>` opens the list with a filter set.
- **A dataset's page** (`#datasets/<record>`) reads `/catalog/datasets/<record>`, `/subjects/dataset/<record>`,
  its timeline, `/decisions?record=`, `/assets/staged?record=` and `/buffers?record=`: its record, preparation,
  crops, decisions, cache, buffers, buttons and timeline.
- **Preparation** reads `/subjects/prep`: the requests. A request's page lists its datasets
  (`/catalog/datasets?request=<number>`: the records its stored spec names).
- **Buffers** reads `/subjects/buffer` (specs) and `/buffers`. **New buffer** (`#buffers/new`) is the designer:
  its controls are a spec, run through `POST /plan` with `page` (debounced 300 ms) for the funnel, the charts and
  one page of the plan's rows. A spec's page (`#specs/<name>`) has its buttons, its timeline and its buffers; a
  buffer's page reads `/buffers/<id>/status`, `/events`, `/datasets`, `/volumes`, `/consumers` and `/usage/<id>`.
- **A lifecycle** tops the page of a dataset, a spec, a request and a buffer: the kind's stages as boxes and arrows
  with the current one filled. The stages are `lines` of `/meta`, which is each stage table in its own order
  without the stages a round can end in from any step (`failed`, `blocked`, `needs-human`, `cancelled`); those
  hang under the step the round stopped at.
- **Cache** reads `/assets/raw|staged`, `/locks`, `/keeper` and the quotas from `/meta`.
- **Decisions** reads `/decisions` (newest first, by gate, with the judge/human agreement); a human approve that a
  later reject overrules is marked by the server.
- **Docs** is static: the user guide (`#docs`) and the architecture overview (`#docs/architecture`). Each is a file
  of the page (`ui/guide.html`, `ui/architecture.html`), read when it is first shown.

The page opens `GET /stream` and refetches what an event names.

**Every table is paged by the server.** A list route called with `limit` answers one page: `limit` (1–200),
`offset`, `sort` (a column the route names; an unknown one is a 422), `dir` (`asc`|`desc`), `q` (searched in the
row's text), plus the route's own filters; the answer is the route's usual envelope with its list cut to the
page, and `total`. Without `limit` the route answers its whole list, which is what the workers call: keep it so.
`mia_buffers/explore.py` holds the paging (`page_query`, `paged`), the dataset join (`dataset_row`) and the
plan's summary (`plan_view`); a new list gets a `*_SORTS` table in `api.py` and one `paged(...)` call. The page's
one component is `pagedTable` (sort on a header click, a search box, a pager; its state lasts the session, per
table id), fed by `pageOf(path, rowsKey, filters)`.

**How a table names a dataset.** The dataset is the first column: its id links to its page and is the default
sort. Then `issue` (a GitHub mark linking the issue; empty without one), then `artifact` where there is one, then
`files`. A registry number is never shown as `#<number>`: the thing's name links to its page.

**A link to GitHub is the GitHub mark.** `link()` puts the mark before the text of any `https://github.com/` link
(the catalog record, a round's crop gallery, a link in a timeline entry), and `ghIcon` is an issue: the mark
alone in a table, the mark and `#<issue>` on the subject's own page, with a tooltip saying that the issue is a
copy of the timeline. No link to GitHub is spelled out in words.

**A subject's page says where it stands with its lifecycle.** Under the name comes one row with the links to what the
workflows made (its GitHub issue, its workflow in Temporal, its round's crops), then the line of stages; the step a
round is working at is marked (`subject.status === "working"`). There is no status word on the page or in a
table: a table shows the stage as a pill, and the buttons say whose turn it is.

**A button with more to enter ends in an ellipsis.** *Reject…* opens a dialog for the feedback and sends nothing
until some is typed; the dialog hangs off the body, so a redraw of the view behind it keeps what was typed.

**A dataset's page lists every address of its catalog record in its Links card.** The row under the name has only
what the workflows made: the GitHub issue, the workflow in Temporal, the round's crops and the saved crops. The
Links card has the dataset's own page (`landing_url`), its DOI, each paper (`publications`), the download
(`data.download_url`), baseline code, the record in the catalog repository and the catalog PR, then the sample
files, evidence and inspected addresses, each address once. Only an `http(s)` address is a link; any other is
shown as text. The catalog PR is a search for the pull requests opened from the record's branch
(`claude/dataset/<id>`, the catalog agents' convention); a record that came into the catalog another way finds
none.

With an empty registry the views say so ("No catalog synced; run `mia-buffers catalog sync`.").

**Health line.** `empty / (requests + empty)` over the last hour (`requests` counts `next` calls that served
draws, one `ts` each; a successful `split` call is not counted), against the buffer's
`health.max_empty_fraction`; nothing acts on it. The empty count of the last hour is in memory, so a buffer-api
restart resets it.
