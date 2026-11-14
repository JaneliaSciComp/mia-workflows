# The mia-data workflow

How a dataset in the MIA catalog becomes a set of verified, saved image crops, and which component does
each step. The setup checklist is the mia-data section of [`docs/autodev-runbook.md`](autodev-runbook.md); the
registry, the action routes and the page are described in [`docs/development.md`](development.md).

```mermaid
flowchart TD
    catalog[("mia-agentic-search<br/>catalog records")]

    subgraph page["the page (buffer-api)"]
        go{{"Start / Run again"}}
        approve{{"Approve"}}
        reject{{"Reject… with feedback"}}
        timeline[("the dataset's timeline<br/>stages, crops, verdicts")]
    end

    catalog -->|synced into the registry| go
    go -->|action route, stage table| wf["MiaDataDataset<br/>(one Temporal workflow per dataset)"]

    subgraph cluster["LSF via csub, each job in its own sandbox"]
        match["match: Claude agent<br/>picks a snippet, writes the binding"]
        tcrop["crops: harness crop --n 16<br/>+ harness verify"]
    end

    wf -->|matching| match
    match -->|matched| tcrop
    match -->|no snippet fits| dev
    tcrop -->|at least half verified| review["review"]
    tcrop --> gallery[("galleries branch<br/>the round's PNGs")]
    review --> timeline
    review --> approve
    review --> reject
    approve -->|matched binding| publish["save the reviewed crops<br/>to paths.storage_root"]
    approve -->|snippet PR head: /approve| merge
    reject -->|matched binding: supersede the snippet| dev
    reject -->|snippet PR head: /revise with the feedback| pr
    publish -->|done| done["timeline entry:<br/>totals, folder links"]

    subgraph snippets["GitHub: mia-snippets"]
        dev["mia-snippets issue<br/>snippets:go"]
        pr["PR adding snippets/&lt;name&gt;/<br/>(autodev's Claude agent)"]
        merge{{"autodev merges"}}
    end

    dev --> pr
    pr -->|each new head: the round's 16 crops, private cache| tcrop
    merge -->|tree matches the approved head| publish
    timeline -.->|MiaMirror: a copy| issue["GitHub issue<br/>mia-workflow-data"]
```

## Overview

Every record of the synced [mia-agentic-search](https://github.com/AI-HHMI/mia-agentic-search) catalog is a row on
the page's Datasets tab. A person starts work on one with *Start* on its page, or a preparation request starts
several. From there a durable [Temporal](https://temporal.io) workflow owns the dataset: it runs cluster jobs,
writes each result to the dataset's timeline, and waits, for hours or days, for a person to approve before
anything is saved. The result is a folder of 256×256 PNG crops per dataset, with matching label overlays,
verified by a job that runs no snippet code, and a registry row that says the dataset is `prepared`.

The pieces, and where they live:

| component | repo | role |
|---|---|---|
| `mia_data/` package, `mia-data` CLI (`pixi run mia exec mia-data ...`) | [mia-workflows](https://github.com/JaneliaSciComp/mia-workflows) (this repo) | the workflow, its activities, the poll of snippet PRs, the mirror and devops commands |
| buffer-api and its page | this repo (`mia_buffers/`) | the registry (each dataset's stage, timeline and actions), the action routes behind the page's buttons, and the page itself |
| `autodev/` package | this repo | GitHub client and App auth, the csub job runner, the await-queue pattern; also the agent that writes new snippets |
| snippets and harness | [mia-snippets](https://github.com/JaneliaSciComp/mia-snippets) | one small immutable reader per source format, and the `harness` CLI (`smoke`, `crop`, `verify`, `preview`) |
| issue copies and galleries | [mia-workflow-data](https://github.com/JaneliaSciComp/mia-workflow-data) | one issue per dataset holding a copy of its timeline, written by the mirror and read by nothing; a `galleries` branch of PNGs for review |
| catalog | [mia-agentic-search](https://github.com/AI-HHMI/mia-agentic-search) | dataset records on `main`; read-only here |
| job submission | [csub](https://github.com/krokicki/csub) | submits each job to LSF through a broker that enforces the service account's policy: allowed hosts, roots, agents |
| sandboxing | [agentic-sandbox](https://github.com/JaneliaSciComp/agentic-sandbox) | wraps the worker and every job: read-only code, read-write only its own directories, a network allowlist |
| orchestration | [Temporal](https://temporal.io) | one dev server shared with autodev; namespace `mia-data`, task queues `mia-data` and `mia-data-await` |
| folder links | [Fileglancer](https://github.com/JaneliaSciComp/fileglancer) | timeline entries link the round's workspace and the dataset's crops folder when `paths.*.url` is set |

## The steps

### 1. Start

*Start* on a dataset's page (`#datasets/<record>`) is a `go` sent to buffer-api's action route. The service
checks it against the dataset's stage (`STAGE_ACTIONS` in `mia_data/config.py`), creates the dataset's subject on
its first `go`, records the action and starts the dataset's `MiaDataDataset` workflow with the action as its
signal. A preparation request (*Prepare these datasets*) sends the same `go` for each of its datasets, with the
worker token, and is charged for their jobs. Preparation judges nothing about who may use a dataset: licence and
size are a buffer spec's filters, so a GPL dataset or one of 60-pixel images is prepared like any other. A round
in progress accepts only *Cancel the round*; `debugging`, between a failed round and its re-run, also takes
*Run again now*. Components: the page, buffer-api, Temporal.

The round pins the record at the catalog's commit: the commit in the catalog's downloads state when the record
has an entry there, else the head of `main`.

### 2. Match

The workflow sets the stage `matching` and submits the **match job**: a Claude agent, run through csub in a
sandbox with the record, its downloads entry when the catalog has one, a read-only snapshot of mia-snippets at
a pinned commit and the harness CLI. It picks a
snippet for each raw and label array and writes a **binding**, a small YAML saying which files to read with which
snippet and parameters, with reasons. It may run `harness smoke` to try bindings and `harness list` to see a
directory URL's files, but may not change snippet code. The job runs Claude Code restricted to five built-in
tools, with no settings files and no MCP servers; the sandbox mounts are the other half of that restriction. Its
outcome is `matched`, `needs_snippet`, `blocked` or `failed`. The agent chooses between the last two by what a
person would have to do: `blocked` when something has to be decided or changed before any run can bind the dataset,
`failed` when running the match again may pass.

The worker never trusts the agent's claim. It checks the binding (right record and commit, URLs from the
record, https only, snippets exist), and the crop job of step 4, a plain `harness crop` with no agent, opens
every array itself and makes `harness smoke`'s checks before it samples: a binding that cannot be read, or a
label that is not on its raw's grid, ends the round `failed` with the crop job's errors. Components:
`autodev.jobs` (the csub runner), csub broker and LSF, agentic-sandbox, the harness from mia-snippets. The match
agent mounts no writable cache and takes no lock.

### 3. Develop (only when no snippet fits)

When the agent answers `needs_snippet`, the worker opens an issue in mia-snippets labelled `snippets:go`, with
the draft binding and what failed. A second autodev deployment, `snippets`, turns that issue into a PR adding
`snippets/<name>/` and an example binding. The dataset sits in `developing` and wakes up whenever the PR changes:
`MiaDataPoll`, a Temporal schedule, looks at the snippets repo's pull requests and signals the dataset whose PR
moved. Every new PR head gets the round's crops (step 4), made by an untrusted job with a private cache, and a
required `mia-data/crops` commit status on that exact commit. A head whose crops pass goes to `review` like a
matched binding's; one whose crops fail gets the report as a `/revise` on the PR, and autodev fixes it. Nobody
merges the PR by hand: *Approve* on the dataset's page makes the worker post `/approve <head>` on the PR (stage
`merging`), autodev merges, and if the merged snippet tree is the approved head's, the approved crops are
saved; otherwise the merge commit gets its own crops, reviewed again. If the round stops (`failed` or `blocked`)
while the PR is still open (a lost job, a failed status write), *Run again* resumes that PR: the current head is
tested again under a new round, and no second snippets issue is opened. A PR closed or merged by then means a new
match. Components: autodev (its own namespace; the two workflows interact only through GitHub, and autodev takes
`/approve` and `/revise` from the worker because its login is in autodev's `github.trusted_bots`), mia-snippets
CI, the GitHub client.

### 4. Crops, review and saving

One set of crops per round. Two plain jobs at the pinned snippets commit: `harness crop --n 16`
(`crops.per_round`) with the round's seed and `--existing` pointing at the dataset's current manifest, so a later
round adds crops rather than duplicating them; then `harness verify` and `harness preview` in a separate
workspace that loads no snippet code. Verify checks each crop is a square 16-bit PNG of the crop edge (256, or
the image's own extent when it is smaller: a small dataset is cropped whole, never refused) with matching labels
and writes overlay PNGs. At least half of the set must verify (verify exits 1 whenever one crop fails, so its
report is the judge); fewer ends the round `failed` with nothing saved. The passing crops' `raw.png`
and `overlay.png` are committed to the `galleries` branch of mia-workflow-data, and a timeline entry shows them
with the binding and the agent's reasons. The stage is `review`, and the dataset is listed under *Needs you*.

**The crop judge (shadow).** With `judge.crops.mode: shadow`, a second model looks at the same set before the
person does: the worker builds an evidence directory under the round's workspace (`judge-crops-1/evidence/`: the
question, every verified crop's raw and overlay PNG, `verify.json`, the binding without the agent's reasons, the
record) and runs a read-only Claude Code job over it, budgeted against the root like the match agent. Its verdict
is written as an assessment `by: judge` on the same subject the person's verdict will use, and posted as a second
entry under the crop table (verdict, findings per crop, confidence, model, prompt version, spend, and the line
that it decides nothing). A judge job that fails leaves a one-line entry instead. The dataset still waits at
`review` for the person; the Decisions tab shows the agreement. `mode: off` skips all of it; `mode: active` is
not available.

**Validation.** A person approves the set with *Approve* on the dataset's page, or rejects it with *Reject…*,
which opens a dialog for the feedback. Either verdict carries the revision the page was showing, and buffer-api
refuses one about a set that is not the newest. The verdict is recorded as an assessment in the registry (`by:
human:<who>`), and the save is authorized from the assessments on record, never from the click alone. On
a reject the feedback goes to the agent: as `/revise` on the snippet PR when the round is following one, else in
a new mia-snippets issue asking autodev to supersede the binding's snippet (a merged snippet is never edited).
The next crops come back to `review`; the loop runs until an approve.

**Revocation.** *Revoke* on the dataset's page, once it has saved crops, withdraws them: a `reject`
assessment supersedes the approve, every staged artifact of the record is quarantined and rejected, the registry
row becomes `revoked` and `revoked.json` is written beside the manifest, so no buffer plans or reuses the dataset
and the buffers that drew from it unregister its volumes (the keeper tells them). Nothing is deleted; the dataset
ends in `blocked` listing the artifacts and buffers, and *Run again* with a round that saves its crops repairs
it.

**Saving.** The approve saves the very crops that were reviewed: the worker reads the round's verify
report back from its workspaces and copies every crop that passed into `paths.storage_root/<dataset>/<crop_id>/`
by an idempotent activity (a retry never duplicates or overwrites), beside `binding.yaml`, and
rebuilds the dataset's `manifest.json`. The final entry carries the totals and links to the crops folder, its
gallery and the round's workspace. The stage is `done` and the registry row `prepared`; *Run again* later starts
another round and adds more crops.

The trusted crop job writes the record's raw download cache (`paths.work_root/_cache/<record>`), so the worker
first takes the record's **raw lock**, the lock file `paths.local_root/locks/raw/<record>` with the owner
`mia-data <round> crop`. The waits for the crop and verify jobs heartbeat it (TTL 900 s) and the lock is released
after the verify report is read, whatever it says. When another job holds it (a buffers stage or fetch, another
round), the timeline gets one entry per round, ``waiting for the raw cache of `<record>`: held by <owner>``, and
the workflow tries again every `temporal.poll_minutes`. After the verify report the worker puts the cache's `raw`
asset row (files, bytes, `ready` or `partial`, `last_used`) to buffer-api. A trusted crop job that was refused a
host (a redirect to one that is not allowed) ends the round `blocked`, naming the host. A PR head's crop job mounts
the cache read-only, writes a private one and takes no lock.

## When a round fails

A round that fails does not stop at once. While it has a debugging cycle left (`limits.debug_cycles`, default 1,
counted since the last go that was not the debugger's), its stage is `debugging`, which is a round in progress:
nothing is asked of a person. After `limits.debug_wait_min` (default 5) the worker submits the **debug job**, a
Claude agent with the match agent's tools, mounts and budget. Its workspace holds what the match agent reads,
`failure.md` (what stopped the round and the limits its jobs ran under), the round's checked binding when it got
that far, and `round/<job>/` with what each job left: its reports, an agent's `claude.json`, its exit code and the
end of its stdout and stderr, each read as untrusted and cut to size. The agent finds the cause, may test it with
small `harness smoke` and `harness list` probes, and writes one of two outcomes:

- `rerun`: a new round can pass. The worker posts the diagnosis and sends the dataset a `go` as `debugger` through
  buffer-api (the debugged round's root, the advice as its note). That go starts a round like any other; its match
  agent finds the advice as `advice.md` and is told to read it first, as a colleague's notes to check, not
  instructions. The advice can steer the match to another snippet or other params, or to `needs_snippet`.
- `blocked`: no round can pass until a person changes something outside a round (a host, a limit, a harness
  defect, data that is gone). The stage is `blocked`, with the failure, the diagnosis and what it needs.

A debugger that does not run or leaves no readable outcome ends the round `failed`, with both reasons. A re-run
that fails stops as `failed` too, saying it was the re-run: the cycles are used up. *Run again now* at `debugging`
ends the cycle and starts a person's round at once, which has its own cycles. A round a person cancelled is never
debugged. A request that is cancelled cancels the debugger's round like its own.

## How the timeline reads

Every stage change is a new entry, never an edit: the stage, what it produced, and the folders to look at. The
dataset is in exactly one stage at a time, written only by the workflow: `matching`, `developing`, `testing`,
`review`, `merging`, `debugging`, `done`, `failed`, `blocked` or `cancelled`. The buttons on its page are the actions its stage
accepts. A round that cannot go on stops with an entry saying why, in one of two stages: `failed` when a job or a
check failed and running it again may pass, `blocked` when a person has to decide or change something first (the
match agent says so, a host is not approved, admission refuses the record, the policy refuses an approve, a
revocation, a pause). `needs-human` is the stage of a stop that says neither;
*Run again* afterwards retries from the start with a new seed, or, when the round was following a snippet PR that
is still open, picks that PR up again. At `review` the same action is *Restart*: the set under review is dropped
unsaved and the dataset is prepared again (a snippet PR's current head is tested again). buffer-api delivers a
dataset's `go` as the workflow's `restart` signal: a run that has carried a round continues as new first, so every
round begins in a run that takes all of today's code paths, the crop judge among them (a run keeps the patches it
started with). *Cancel the round* ends a round in progress: its jobs are killed and the
stage is `cancelled`.

The `MiaMirror` workflow copies each entry to the dataset's GitHub issue as a comment and sets the stage as the
issue's `data:<stage>` label. Nothing done to the issue reaches the system.

## The registry hears every stage

`set_stage` writes the subject's stage, its timeline entry and the dataset's row (`PUT /datasets/<record>`), and
the workflow waits for the write: the registry's stage is what buffer-api checks an action against. `prepared`
means a round that ended `done` with `binding.yaml` plus a `manifest.json` whose binding hash matches it. Until
then the row says `in-progress` while the round works by itself and `needs-human` whenever a person has to act,
which includes `review`; a new round on a prepared dataset starts that over. `mia-buffers datasets sync` rebuilds
the prepared rows from `storage_root`.

## Trust boundaries in one paragraph

Agents never hold GitHub credentials or reach GitHub; the worker does every GitHub write. Each job asks csub
only for the hosts in its binding, and the broker's policy is the ceiling: a host missing from the policy fails
the round before submission, with an entry naming it (a person can approve the host from the page when starting
the run). Unreviewed snippet code from a PR head runs only inside
job sandboxes, and the verify job that judges its output loads no snippet code. Merging a snippet needs a
person's approve of the crops its exact head made, plus the required status; so does saving them. The page's
buttons need no login: they can ask the workflow to act, and cannot write what a workflow found.

## Operating it

```
pixi run mia exec mia-data check    # repos, Temporal, csub policy, directories, harness pin
pixi run mia up                     # Temporal, then every service in mia.yaml; or: mia up mia-data
pixi run mia ps | logs mia-data -f | attach mia-data | down mia-data | restart mia-data
pixi run mia exec mia-data init-labels   # the stage labels the mirror sets on the issue copies
```

`mia pause` stops every round at its next job (after `mia resume`, press the button again); every job is first
reserved against its root request's caps in `autonomy.root`, and a refusal ends the round naming the cap. Config
is the `mia-data` block of `mia.yaml` (see `mia.yaml.example`). `paths.work_root` and `paths.storage_root` take
a path or `{path, url}`; with a Fileglancer URL the entries link the folders they mention.
