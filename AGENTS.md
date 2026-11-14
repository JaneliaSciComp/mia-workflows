# Working in this repository

Four Python packages: `mia_core/` (what every package shares: the `mia.yaml` loader, filesystem checks,
untrusted-output helpers, the job record), `autodev/` (GitHub issue → PR by Claude Code) and `mia_data/` (dataset →
saved crops), both Temporal workflows with activities that talk to GitHub and the LSF cluster via csub, and
`mia_buffers/` (training buffers: the `MiaBuffer` and `MiaPrep` workflows, the planner, the cache keeper, the
buffer-api service with its ledger, registry and page, and the `BufferDataset` client). `mia_data` imports from
`autodev`, `mia_buffers` from both, never the reverse; every package may import `mia_core`, which imports none of them.
Full guide: [`docs/development.md`](docs/development.md); what each part is for: the [README](README.md).

How the pieces meet: mia-data prepares a dataset (approved binding, reviewed crops) and writes its row to buffer-api's
registry; a buffer plans over prepared rows only, fetches raw files into the shared raw cache, stages one **sparse
OME-Zarr 0.6 pyramid per volume** (only the chunks of the planned samples; provenance under `attributes.mia`; the
stage job runs `harness stage` then `harness check-stage`), registers the volumes, and buffer-api serves planned
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
- **No real paths or hostnames in tracked files.** `git diff | grep -n 'rokickik\|/nrs/'` must be empty.
- **One config file.** Deployments read their block of `mia.yaml` (`MIA_SERVICE`, `MIA_CONFIG`); the per-app YAML
  files are deprecated. `paths.local_root` and a managed `temporal.state_dir` must be host-local disk.
- **Worker routes of buffer-api take the bearer token.** Every PUT/POST/DELETE other than the consumer and action
  routes needs `Authorization: Bearer <service.token_file>`; use `mia_core.registry.Registry`, never a bare httpx
  call.
- **The stage tables are the state machine.** `STAGE_ACTIONS` beside each `STAGES` (`mia_data/config.py`,
  `mia_buffers/config.py`) says which actions a stage accepts and whether a round is in progress there.
  buffer-api refuses any other action and the page offers only these; a round in progress accepts only `cancel`
  (a dataset at `debugging` also the `go` of its next round).
  A new stage or action goes into the table first. Every stage write is awaited, so the registry's stage is the
  workflow's.
- **Shelving is beside the stage tables, and only buffer-api writes it.** A subject with no round open also takes
  `shelve` (with a reason); a shelved one reads the status `shelved` and takes `unshelve` alone (`_rules` in
  `mia_buffers/api.py`). The subject's workflow hears of neither; a request resting at `needs-human` is told to
  look again (`recheck`) when a dataset it names is shelved or prepared, and is `done` when each is one of the
  two. The hold is a row of the ledger's `shelved` table; a shelved dataset reads `state: shelved` from
  `Ledger.dataset(s)` while its stored row stays, so no plan and no request takes it. Anything new that selects
  or starts datasets reads their state through those two.
- **A stage pass writes its start and its result.** A stage a round works or waits at (`STAGE_ACTIONS`) writes
  a start entry and, when it ends, a result entry that says what happened and links its jobs' records. The
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
- **A round writes the registry, never its issue.** Stages, comments and refusals are timeline entries
  (`Activities._post`); the `MiaMirror` workflow copies them to GitHub.
- **Nothing reads a data or buffers issue.** No label, comment, body, close or reopen is input: nothing polls
  those repos, and a new feature gets an action route and a button, never a label. The one exception is the mirror,
  which looks for its own issue and comments. autodev and the snippets repo stay on GitHub.
- **One writer per raw cache.** No job writes a record's raw cache unless its worker holds
  `<local_root>/locks/raw/<record>` (`mia_core.locks`; TTL 900 s, heartbeat with every `await_job` poll). A wait
  whose heartbeat finds the lock gone kills its job (`csub kill`) and fails the round; a stale lease is never
  evidence that its writer stopped. Hand-run harness commands bypass it.
- **Only the keeper deletes.** The buffers worker's `MiaCacheKeeper` is the only code that removes anything under
  `raw_root` or `staged_root` (and takes the record's lock first); buffer-api never deletes.
- **A staged artifact is saved by the worker's rename and never written again.** Stage jobs write
  `.building-<artifact>-<job key>`; a rebuild lands under the same id through the same rename.
- **An artifact id is a recipe hash.** Binding, levels key, voxel size, harness commit, env digest and plan digest.
  Changing how the harness derives samples (`plan.py`) changes plan digests, so every volume restages; a buffer that
  re-registers a volume under a new artifact loses its frozen eval lists by design.
- **A buffer never asks mia-data for a dataset; a preparation request does.** `MiaBuffer` fails an unprepared
  record; `MiaPrep` sends each dataset a `go` through buffer-api (worker token, the request's root, a key per
  round and record) and follows it through the registry. Nothing opens or labels an issue in the datasets repo.
- **The page holds no token.** Its buttons call `action` routes: no token, JSON only, this origin only, checked
  against the stage table. Every other route the page needs is a read route (`tests/test_buffers_ui.py` enforces
  it). An action can ask a workflow to do something; it cannot write what a workflow found, which stays behind the
  worker token.
- **The page has no build step.** `mia_buffers/ui/` is `index.html`, `app.css` and ES modules the browser loads as
  they are. A module imports only modules below it, never in a circle (the table in `docs/development.md`, The
  page); the tests import the modules under node, so a module must not touch the DOM when it is imported beyond
  what `dom.js` does. What a job or a record wrote is inserted as text, never parsed as HTML.
- **Only a person's click approves a host.** Brokers read `<csub.policy_dir>/broker.toml`, generated from the operator's
  `csub.base_policy` plus `approved-hosts.jsonl`, which buffer-api alone appends to, on a start action, with hosts
  that run's jobs need (records' URL hosts, bindings' too if the record names them, redirect targets, package hosts;
  never an IP or the deployment's own host); nothing writes the operator's `broker.toml`. `policy_dir` is under home,
  apart from every directory a worker sandbox binds read-write, and its `broker.toml` is not the base policy file.
- **Output of sandboxed jobs is untrusted.** Read it with `read_untrusted`, reduce it to a small typed result,
  `neutralize` it before posting to GitHub.

## Style

Descriptive names (no single-letter attributes), a plain-language line on each module, class and non-obvious
method, type hints on object parameters, line length 120. Mark deliberate shortcuts with a `# ponytail:` comment
naming the ceiling and the upgrade path. Comments and docs say what the code does, never what it replaced or how
it once worked; dated specs and plans are the record of that.
