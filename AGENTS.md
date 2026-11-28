# Working in this repository

Four Python packages: `mia_core/` (what every package shares: the `mia.yaml` loader, filesystem checks,
untrusted-output helpers, the job record), `autodev/` (GitHub issue → PR by Claude Code) and `mia_data/` (dataset →
saved crops), both Temporal workflows with activities that talk to GitHub and the LSF cluster via csub, and
`mia_buffers/` (training buffers: the `MiaBuffer` and `MiaPrep` workflows, the planner, the cache keeper, the
buffer-api service with its ledger, registry and page, and the `BufferDataset` client). `mia_data` imports from
`autodev`, `mia_buffers` from both, never the reverse; every package may import `mia_core`, which imports none of them.
Full guide: [`docs/development.md`](docs/development.md); what each part is for: the [README](README.md).

How the pieces meet: mia-data prepares a dataset (approved binding, reviewed crops) and writes its row to buffer-api's
registry; a buffer plans over prepared rows only and stages one **sparse
OME-Zarr 0.6 pyramid per volume** (only the chunks of the planned samples; provenance under `attributes.mia`; the
stage job runs `harness stage` then `harness check-stage`, reading from the source what the samples touch; a file
its reader takes whole lands in the shared raw cache), registers the volumes, and buffer-api serves planned
samples by weight and split. The harness (writer, reader, planner, tensorizer) lives in mia-snippets at `harness.dir`;
a round pins its HEAD, so the harness deploys first. Dated specs and plans are under `docs/specs/` and `docs/plans/`;
`notes/` is local scratch and is not tracked.

## Before you push

```
pixi run fmt        # format + safe autofixes (ruff; Biome for the page's modules)
pixi run lint       # ruff check + format check, Biome's lint + format check
pixi run typecheck  # mypy
pixi run test       # pytest, incl. Temporal replay tests; needs no GitHub, LSF or Temporal server
```

CI runs lint, typecheck and test. Keep them clean; do not add `type: ignore` or `noqa` without a reason.

## Rules that will bite you

- **Activity result dataclasses in `models.py` are wire format.** They live in recorded Temporal histories. Add
  fields with defaults; never rename or remove one. (`VerifyResult.n` keeps its name for this reason.)
- **Workflow code is deterministic.** If `test_replay_*` fails, your change alters the commands a workflow issues
  and will break in-flight runs. Guard with `workflow.patched()` and re-record the fixture (see the guide).
- **A worker is stopped, never killed, and every activity can run again.** `mia down` sends the worker SIGTERM
  and waits; the worker hands its running activities back to Temporal (`mia_core.temporal`: `DRAIN`,
  `until_stopped`), which gives them to the next worker at once. So an activity starts what it starts at most
  once per key and finds what it waits for by id. One that a workflow gives a heartbeat timeout (`beats=True`
  in `_act`) carries `@heartbeats`, never one without the other. buffer-api stops last and starts first, and
  `mia restart` leaves Temporal alone unless it is named.
- **No real paths or hostnames in tracked files.** `git diff | grep -n 'rokickik\|/nrs/'` must be empty.
- **One config file.** Deployments read their block of `mia.yaml` (`MIA_SERVICE`, `MIA_CONFIG`); the per-app YAML
  files are deprecated. `paths.local_root` and a managed `temporal.state_dir` must be host-local disk.
- **Worker routes of buffer-api take the bearer token.** Every PUT/POST/DELETE other than the consumer and action
  routes needs `Authorization: Bearer <service.token_file>`; use `mia_core.registry.Registry`, never a bare httpx
  call.
- **The stage tables are the state machine.** `STAGE_ACTIONS` beside each `STAGES` (`mia_data/config.py`,
  `mia_buffers/config.py`) says which actions a stage accepts and whether a round is in progress there.
  buffer-api refuses any other action and the page offers only these; a round in progress accepts only `cancel`
  and, for a run that cannot answer one, `stop` (a dataset at `debugging` also the `go` of its next round; a
  buffer's also `retire`, which ends the round as a cancel does). A
  dataset's verdict names its `set`, and buffer-api refuses one for a set the stage does not let it be about
  (`_names_its_set`: at `review` the set under review, at `done` a `reject` of the saved set). Any
  action may name the `revision` it was decided at and is refused once the subject moved. A buffer's `edit`
  (a changed definition, where the buffer rests) is in the table too, and buffer-api carries it out itself.
  A new stage or action goes into the table first. Every stage write is awaited, so the registry's stage is the
  workflow's.
- **A buffer is retired from wherever it is.** `retire` is in every buffer stage but `retired`, and a person's
  retire always ends there. The workflow keeps it in `retire_events`, beside `cancel_events`: a round is ended
  as a cancel ends it, a serving or resting buffer has nothing to end, and `_retire_all` then releases every
  version the registry still holds for the subject (`live_buffers`), not only the one in the workflow's state.
  A new place where a buffer's workflow waits counts `retire_events` as queued, so a continue-as-new cannot
  drop one. The keeper's retire is about one version and stays a lifecycle signal.
- **A round is resumed or restarted.** A dataset and a request take two actions that run them again. `go`
  (*Restart*) wipes the round's state and pins anew. `resume` (*Resume*) keeps the round id, the pins and every
  result on the state, and starts at the first step with none (`resume_step` in `mia_data/models.py`, the one
  place that decides it); it is accepted where a round stopped or waits for its verdict, never after a revoke.
  A step that runs again takes the next attempt (`DatasetState.attempt`), which names its job key, workspace,
  test id and timeline markers: the ledger keeps one body per marker. So a new step of a round records its
  result on the state and gets a line in `resume_step`, and builds its keys and markers through `_job`,
  `_test_id` and `_marker`. The round's results live in its workflow: a force stop loses them, and a resume
  after one starts from the beginning. A request passes the choice to each dataset (`_nudge`), and the repair agent
  resumes; it sends a `go` only where the remedy needs more than the round's pins hold, and never to a request.
- **Only a subject's status says that a human is needed.** A stage table gives each stage a phase (`not started`,
  `working`, `waiting`, `stopped`, `done`: what a round does there). buffer-api alone computes the status a person
  reads (`status_of` in `mia_buffers/api.py`: `shelved`, `in repair`, `needs human`, `done`, `not started`,
  `working`) from the phase, the hold and the repair agent's case about the subject as it stands. A worker decides
  by the phase and the hold (`mia_buffers.cleaner.standing`), never by the status. A case records a repair and ends
  `resolved`, `dropped` or `gave-up`; no case state is a call for attention. Anything new that wants a person's
  attention moves the subject to a stage whose phase is `waiting` or `stopped`: it adds no flag, state or list of
  its own.
- **Shelving is beside the stage tables, and only buffer-api writes it.** A subject with no round open also takes
  `shelve` (with a reason); a shelved one reads the status `shelved` and takes `unshelve` alone (`_rules` in
  `mia_buffers/api.py`), a buffer also `retire`: writing `retired` ends its hold. The subject's workflow hears
  of neither, except that a shelve of a subject whose round
  waits for a verdict (a dataset at `review`) sends that round a `cancel` first; a request resting at `needs-human` is told to
  look again (`recheck`) when a dataset it names is shelved or prepared and when buffer-api starts, and is `done`
  when each is one of the two. The hold is a row of the ledger's `shelved` table; a shelved dataset reads `state: shelved` from
  `Ledger.dataset(s)` while its stored row stays, so no plan and no request takes it. Anything new that selects
  or starts datasets reads their state through those two.
- **A stage pass writes its start and its result.** A stage a round works or waits at (`STAGE_ACTIONS`) writes
  a start entry and, when it ends, a result entry that says what happened and links its jobs' records (each
  record once, as a `**Jobs:**` line: a job with an entry of its own, the crop judge's, is linked there). The
  workflow keeps the open pass in its state (`pass_stage`, `passes`, `pass_jobs`, `result`) and the stage write
  carries its result out (`mia_data.status.stage_entries`); a failed or cancelled pass's result is also the entry
  that moves the subject to the stage it rests at, so no stage ends without saying how. A new ending or a new
  stage sets the result (`_found`) before its stage write, or calls `_flush` where none follows.
- **Every job leaves a record, and only the job runner writes it.** `CsubJobs` writes
  `<work_root>/_jobs/<workspace relative to work_root>.json.gz` (`mia_core.jobrecord`): the request with `env` as
  names only, csub's answer, the outcome, LSF usage, log ends, and for an agent its prompt and conversation with
  tool output dropped and credentials scrubbed. No job gets `_jobs` writable (`CsubJobs` refuses the mount),
  nothing deletes a record, buffer-api serves the file as it is (`GET /jobs/<address>`) and the page shows it at
  `#job/<address>`. Writing one is best effort and never fails a submit or a wait. An agent command prints
  `stream-json` to `claude.jsonl` and `keep_result` copies its last line to the file the result is read from.
  The job runner also reads `<workspace>/transfers` into the record and, where the worker has a registry (an
  autodev with no `service` block has none), into the ledger's `transfers`; nothing stored names what an answer
  meant.
- **A round's wait on autodev is timed.** While a dataset round waits for a snippet PR, a new head or a merge it
  looks at the snippets issue itself every 15 minutes (`autodev_look`, `_look` in `mia_data/workflows.py`) and
  ends `blocked` when autodev stopped or `limits.autodev_quiet_hours` passed with no agent round running; the
  poller's `pr_update` only makes it faster. autodev says a stop with the `needs-human` label, never a comment
  alone. A reviewer's revision request that autodev answers with a round of no commits brings no new head: the
  look takes the dataset back to `review` with autodev's answer as the pass's result (`_answered`). That a
  round made no commits never says the crops are right: only autodev's validated `no-change <request>` marker
  does (`read_no_change`), and only that answers the crop judge's findings or a request on an issue with no
  pull request. A round that ends because autodev stopped quotes why (`autodev_said`): what the agent wrote
  when its round made no commits, else autodev's last comment. An agent
  job Claude Code itself cut off (a lapsed login, an API error) is run once more by autodev before it stops.
  A new wait on something outside the round gets the same: a look and a limit.
- **A new snippet is named after its issue.** `<what-it-reads>-i<K>`, K being the mia-snippets issue its pull
  request is for: one issue has one pull request, which adds one snippet, so no two pull requests can add the
  same name and a snippet PR cannot conflict with main. `adopt_pr_binding` fails an open PR's head whose
  snippet is named otherwise, and the developer renames it; a merged snippet keeps the name it has. Nothing
  reads a name for more than identity: lineage is `supersedes`.
- **A person outranks the judge, and a verdict names its set.** An active crop judge approves a set or sends it
  back to the developer (`judge.crops.max_reworks` times since a person last acted). A person's verdict is
  about the set under review, or at `done` a reject of the saved set (`DatasetState.saved`), which revokes it
  and starts its rework. The set that was sent back (`DatasetState.held`) is kept for the developer's answer:
  no verdict names it.
  `authorized()` ignores what the judge and `policy:developer` wrote about a set once a person gave a verdict
  on it, whenever each was written. Anything new that approves or rejects crops names the set and writes an
  assessment; it never infers approval from silence.
- **The repair agent asks, like anyone, and service problems come first.** `MiaCleaner` (the buffers worker's `sweep`;
  rules in `mia_buffers/cleaner.py`) names what is wrong with the installation before it looks at a subject, and
  leaves alone every subject behind a finding: it never cancels or re-runs what waits on a worker that is down or
  on old code. What it does to a subject goes through the action routes as the actor `cleaner`, with the
  revision it just read, a key per step and for a `stop` the run it saw; it writes no stage. It sends no `go`
  while a job of a round it ended is alive, none to a subject a person acted on since the case opened, and
  none to a request that would start a dataset round. Its diagnosing agent is read-only and proposes;
  `reduce_outcome` keeps only steps of the menu. A new remedy is a rule with a test named after its incident.
- **A round writes the registry, never its issue.** Stages, comments and refusals are timeline entries
  (`Activities._post`); the `MiaMirror` workflow copies them to GitHub.
- **Nothing reads a data or buffers issue.** No label, comment, body, close or reopen is input: nothing polls
  those repos, and a new feature gets an action route and a button, never a label. The one exception is the mirror,
  which looks for its own issue and comments. autodev and the snippets repo stay on GitHub.
- **One writer per raw cache.** No job writes a record's raw cache unless its worker holds
  `<local_root>/locks/raw/<record>` (`mia_core.locks`; TTL 900 s, heartbeat with every `await_job` poll). A wait
  whose heartbeat finds the lock gone kills its job (`csub kill`) and fails the round; a stale lease is never
  evidence that its writer stopped. Hand-run harness commands bypass it.
- **A buffer reads what its crops touch.** No job of a buffer round downloads a record or a file ahead of its
  reads: the stage job is online and its readers decide (`io.open` by range, `io.fetch` whole), within
  `limits.job_bytes`. `check_quota` looks at what the raw cache holds, never at a record's size upstream, and the
  raw quota is where staging waits and the keeper evicts, not a ceiling (one record's path and the caps:
  `docs/development.md`, Sourcing on demand). A raw cache on disk is `ready` whatever it holds (`raw_asset`): the
  record's lock, not the row, says a job is writing. Anything new that transfers data is sized by what the planned
  crops touch; the one read beyond them is a filter's test of a draw it rejects, within the same cap.
- **Only the keeper deletes.** The buffers worker's `MiaCacheKeeper` is the only code that removes anything under
  `raw_root` or `staged_root` (and takes the record's lock first); buffer-api never deletes.
- **A staged artifact is saved by the worker's rename and never written again.** Stage jobs write
  `.building-<artifact>-<job key>`; a rebuild lands under the same id through the same rename.
- **An artifact id is a recipe hash.** Binding, levels key, voxel size, harness commit, env digest and plan digest.
  Changing how the harness derives samples (`plan.py`) changes plan digests, so every volume restages; a buffer that
  re-registers a volume under a new artifact loses its frozen eval lists by design. The plan digest holds the
  record's crop count, its weight's share of `sampling.crops` rounded up to a power of two (`plan.crops_for`):
  change that rounding and every pool change restages every record. A definition's `sampling.filters` are in
  the rule only when it has some: never write an empty `filters` key, or every buffer restages.
- **The plan refuses what is already known; only the stage job knows what a filter keeps.** A dataset that its
  record or its registry row says cannot be staged under the definition is out of the plan with the reason
  (`plan.fit`: no array that holds the footprint, no voxel size, under a `labelled` filter no label on such an
  array), and the designer's plan says the same before a round exists. A new check of that kind goes there, with
  the fact it needs on the prepared row (`mia_data.publish.prepared_row`), never into sourcing.
  `sampling.filters` (`content`, `labelled`) are tested by the stage job on each draw, the labels over its whole
  footprint (the coarsest level's box) before the raw's full-resolution box; a rejected draw is drawn again until
  the count or 2000 rejections in a row. The plan's crops are then an upper bound (`up to`), stage.json's
  `selection` says what was kept, registration scales a short volume's weight by staged / wanted, and the
  record's servable line names the shortfall. A new filter is a function in the harness's `filters.py` and an
  entry of the grammar (`mia_buffers/spec.py`); it adds no planner estimate and hands no share to another volume.
- **A buffer never asks mia-data for a dataset; a preparation request does.** `MiaBuffer` fails an unprepared
  record; `MiaPrep` sends each dataset a `go` through buffer-api (worker token, the request's root, a key per
  round and record) and follows it through the registry. Nothing opens or labels an issue in the datasets repo.
- **The page holds no token.** Its buttons call `action` routes: no token, JSON only, this origin only, checked
  against the stage table. Every other route the page needs is a read route (`tests/test_buffers_ui.py` enforces
  it). An action can ask a workflow to do something; it cannot write what a workflow found, which stays behind the
  worker token.
- **The page has no build step.** `mia_buffers/ui/` is `index.html`, `app.css` and ES modules the browser loads as
  they are. A module imports only modules above it in the table of `docs/development.md` (The page), never in a
  circle; the tests import the modules under node, so a module must not touch the DOM when it is imported beyond
  what `dom.js` does. What a job or a record wrote is inserted as text, never parsed as HTML.
- **Only a person's click approves a host.** Brokers read `<csub.policy_dir>/broker.toml`, generated from the operator's
  `csub.base_policy` plus `approved-hosts.jsonl`, which buffer-api alone appends to, on a start action, with hosts
  that run's jobs need (records' URL hosts, bindings' too if the record names them, redirect targets, package hosts,
  the hosts a dataset's jobs were refused; never an IP or the deployment's own host); nothing writes the operator's
  `broker.toml`. A job that is refused a host notes it for its dataset; noting approves nothing, and the dataset's
  jobs, and the job of the reader autodev writes for it (its snippets issue names them), get the host only once
  the policy allows it; until then a request sends that dataset no `go` once it has
  stopped, and the repair agent says which hosts wait. No job reaches a GitHub host, except that a person may approve
  `raw.githubusercontent.com` for one dataset at a time: the policy allowing it grants it to no other dataset's
  jobs, and autodev grants it only to the reader's job of an issue that names it. A round asks for no reader
  whose job could reach none of its record's files: it ends `blocked` naming the host to approve
  (`NoReachableFiles`, raised where the issue's host line is written). `policy_dir` is under home, apart from every directory a worker sandbox binds read-write, and its
  `broker.toml` is not the base policy file.
- **Output of sandboxed jobs is untrusted.** Read it with `read_untrusted`, reduce it to a small typed result,
  `neutralize` it before posting to GitHub.

## Style

Descriptive names (no single-letter attributes), a plain-language line on each module, class and non-obvious
method, type hints on object parameters, line length 120. Mark deliberate shortcuts with a `# ponytail:` comment
naming the ceiling and the upgrade path. Comments and docs say what the code does, never what it replaced or how
it once worked; dated specs and plans are the record of that.
