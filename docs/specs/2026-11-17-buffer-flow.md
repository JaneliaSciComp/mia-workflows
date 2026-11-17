# MIA: a buffer is one flow — Spec

**Status:** v0.4 · 2026-11-17 · draft, not approved. v0.4 takes in an architecture review of v0.3 (2026-11-17),
read for one thing: that this flow can later be upgraded to a moving window without being changed. It adds the
four nouns (§3.1), what stopping does to training, the stopped version's space and its recovery (§3.6), and
§3.9: what this flow keeps open for the window and what the window's own spec must define. v0.2 settled what a builder's read of v0.1 asked. v0.3
(user, 2026-11-17): datasets that wait for a person when the buffer starts serving join the next version
(§3.6); what is fixed about a serving buffer is what it is, not what is on disk, so a buffer may be larger
than the disk (§3.9). Brainstormed with the user 2026-11-17; a clickable picture of
every screen is beside this file, [`2026-11-17-buffer-flow-mockup.html`](2026-11-17-buffer-flow-mockup.html)
(fake data, sends nothing; its yellow boxes are design notes). §4 lists what the user decided and §5 what is the
author's call.

## 1. Intent

Creating a buffer on the page is confusing. The page has two nouns, *spec* and *buffer*, and does not say which
one a person is looking at or where to go next. *Create buffer* is a fold-out between two buttons of the
designer. *Prepare* files a request from the designer's text and the definition is never saved, so once the
datasets are prepared the person builds the definition again.

A buffer becomes one thing with one path through it: define it, look at its datasets, prepare the ones that are
missing, plan, source, serve. At every step one button moves it forward and one goes back.

**Goals**

- The page shows buffers. A person never has to know what a spec is.
- *New buffer* is one button on the Buffers list.
- A buffer is six steps in a fixed order, and opening a buffer shows the step it is at.
- The definition is edited through controls or as YAML, two views of the same text.
- Preparing datasets belongs to a saved buffer and never loses its definition. The buffer waits at Prepare until
  the person goes on, and can always go on with what is prepared at that moment.
- A buffer can be changed at any step until it serves. A serving buffer is fixed. Serving can be stopped at any
  time, and a stopped buffer can be changed again.
- What is fixed is what the version is: its definition, its pool of datasets and their weights, and its planned
  crops (the candidates the plan chose). What is on disk at a moment is not. A buffer whose plan is larger than
  the disk will serve a moving window of its plan, and nothing in this flow has to change for that (§3.9).
- A changed buffer that serves again is a new version with a new id. Consumers name the id.

**Non-goals**

- How a dataset is prepared, how a plan is computed, how volumes are staged. The rounds do what they do today.
- The automatic paths that change a serving buffer: the keeper's `revoked`, `degraded`, `idle` and `retire`
  signals stay as they are.
- A new budget root: a preparation request stays the root of the dataset rounds it starts.
- The mechanics of the moving window: what counts as used, eviction from a serving version, refill, order,
  what a consumer gets when refill falls behind. That is its own spec; §3.9 says what this one holds for it.
- Keeping an unsaved definition across a page reload.
- Identity (who clicked) beyond what the action routes record today.

**Anti-goals**

- A serving version whose definition, pool or planned crops change because a person clicked something.
- Storage deciding what a training run sees without anyone being told: where what is on disk differs from the
  plan, the page says so.
- A flow that assumes a version is wholly on disk before it serves, or ever.
- Any route, client call or workflow that finds a buffer by its name.
- A step whose way forward is not one obvious button.

## 2. What a person sees

**Buffers** (the tab): one table, one row per buffer: number, name, step, status (waits for you, working,
serving, stopped), the id being served if any, datasets, train crops, last change. *+ New buffer* at the top
right. A row opens the buffer at its step. There is no list of specs and no Preparation tab.

**A buffer** is a heading (name, number, a pill: not saved, saved, serving, stopped), the strip of six steps, and
the view of one step. Steps already passed are ticked and can be opened. The step a buffer opens at is a function
of its stage:

| Step | Shows | Forward | Back | Stage of the registry |
| --- | --- | --- | --- | --- |
| 1 Define | Controls and YAML as two tabs of one pane. Under it, a narrow column with *Datasets that fit* and *Prepared*, then *Weight share by modality* and *Heaviest datasets*. | Next: the N datasets | (the list) | none before the save; a resting stage after |
| 2 Datasets | Every dataset the definition admits, with its state and weight and a tick *Selected*. Where the others left, behind a toggle. | Save buffer and continue (first time); Next: prepare | Change the definition | as step 1 |
| 3 Prepare | The fitting datasets that are not prepared, a tick each, *Prepare selected*; the requests this buffer filed and where each dataset is. | Continue with N prepared datasets | Datasets | `new`, `stopped`, `cancelled`, `needs-human`, `retired` |
| 4 Plan | The plan: pool, weights, crops, bytes to fetch, its staged size against the disk limit, hosts to approve. | Run: fetch and stage | Prepare more datasets | `planning`, `plan-review` |
| 5 Source | The sourcing board, then the wait for an approve. | Approve and serve | Change the buffer (cancels the jobs) | `sourcing`, `staged` |
| 6 Serve | Version and id, how much of the plan is on disk now, the planned mix beside the mix on disk, usage, consumers, the line to paste into a training script, earlier versions. | (none) | Stop serving | `ready`, `idle` |

A saved buffer at a resting stage opens at step 3: that is where it waits for the person.

**Three words, kept apart everywhere on the page.** A dataset is *selected* when the definition admits it and
its tick is on (steps 1 to 3). It is *in this version* when the version's plan holds it (steps 4 to 6). It is
*on disk now* when its data is staged and can be drawn from (steps 5 and 6). No label says "in the buffer".

Below the step, on every step, is the buffer's timeline as the subject view shows it today.

**What is not a step.** A step has one way forward and one back; everything else a stage accepts sits where it
belongs and never beside the forward button:

- A stopped buffer shows one line above steps 1 to 3: which version is stopped, how many fitting datasets were
  prepared since it was planned, and the buttons *Serve version N again* and *Retire* (§3.6).
- *Keep* is on step 6 of an idle buffer, in the line that says no consumer has drawn.
- `dropped` is on step 5, in the row of the dataset that waits for its files. It has no place on step 6 (§3.6).
- Clone, Shelve or Unshelve and the force stop are in a *More* menu at the heading, which offers only what the
  stage accepts.

## 3. Design

### 3.1 One noun, and what identifies it

A buffer in the list is a subject of kind `buffer`, as today's spec is: a number, a stage, a timeline, and the
definition text in its data. What today's code calls a buffer (the row a round creates when sourcing starts, with
volumes and events) is a **version** of it.

Four things are kept apart, and the last two are not this spec's to build:

- **Buffer:** the editable intent. A subject and its definition text.
- **Version:** a fixed population and recipe: the datasets, weights and planned crops of one plan, under one id.
- **Residency:** which of a version's data is on disk now.
- **Training run:** one consumer's progress through a version.

Residency and a run's progress get state of their own with the moving window (§3.9). Nothing in this spec
stores either: no stage, version event or id says what is on disk or how far a consumer is.

- The **number** identifies the buffer on the page and in the action routes: `POST /specs/<number>/actions`
  for every buffer.
- The **name** is a label. It need not be unique, can change with any edit, and nothing computes with it. A new
  buffer's key is `buffer-<number>` whatever its name. A subject that has a name as its key keeps the stored
  value, and no route resolves it.
- The **id** of a version is what a consumer names (`BufferDataset(url, buffer=<id>)`, the `/buffers/<id>/...`
  routes). §3.7.

The search box may still find buffers by a word of their name: that is a search, and it returns numbers.

### 3.2 The stage table

`STAGE_ACTIONS` in `mia_buffers/config.py` after this change. One stage and two actions are new.

| Stage | Status | Accepts |
| --- | --- | --- |
| `new` | not started | `edit`, `go` |
| `planning` | working | `cancel`, `stop` |
| `plan-review` | waiting | `approve`, `go`, `cancel`, `stop` |
| `sourcing` | working | `cancel`, `dropped`, `stop` |
| `staged` | waiting | `approve`, `go`, `dropped`, `cancel`, `stop` |
| `ready` | done | `edit` (the name only), `unserve`, `stop` |
| `idle` | waiting | `keep`, `edit` (the name only), `unserve`, `stop` |
| `stopped` | done | `edit`, `go`, `approve`, `retire` |
| `retired` | done | `edit`, `go` |
| `needs-human` | stuck | `edit`, `go`, `approve`, `retire` |
| `cancelled` | done | `edit`, `go` |

- **`stopped`** (new): the buffer has a version that is not serving and whose volumes are kept.
- **`edit`** (new): replaces the definition text. §3.3.
- **`unserve`** (new; the button reads *Stop serving*): §3.6. Not named `stop`, which is the force stop of a run
  that does not answer.
- `ready` and `idle` no longer take `go`, `revoke`, `retire` or (`ready`) `dropped` from a person: a serving buffer is fixed, and the
  way to change or retire it starts with `unserve`.
- `approve` and `retire` at `needs-human` act on a stopped version the buffer still holds (§3.6) and are refused
  when it holds none.
- Shelving stays beside the table as it is; a `stopped` buffer has no round open, so it takes `shelve`.

### 3.3 Saving and editing the definition

Before the save a buffer exists only in the browser tab. *Save buffer and continue* on step 2 is `POST /specs`
with no action, which exists: a new subject at `new`.

After the save the definition changes through the action `edit` on `POST /specs/<key>/actions`, carrying `spec`
(the text) and the `revision` it was decided at. buffer-api checks that the text is a buffer spec, stores it and
the name in it, and writes a timeline entry; the workflow hears nothing, as with `shelve`. The page sends `edit`
when the person leaves step 1 or 2 with a changed definition, not on every keystroke.

The rule that a spec which has built a buffer is frozen and must be cloned goes: `go` no longer refuses a changed
text for that reason. *Clone* stays as a convenience: it opens *New buffer* with the values of an existing one.

**The name is not part of what a version is.** Two texts are the same definition when they parse to the same
spec apart from `name`. An `edit` that changes only the name is accepted at every stage, serving included, and
makes no new version; any other `edit` is refused while a round is in progress or the buffer serves.

**Controls and YAML.** One text is the definition. The Controls tab writes it (`toSpecYaml`); the YAML tab is
read back through `POST /specs/parse` and sets the controls (`fromSpec`). The stats under the pane come from
`POST /plan` over that text. While the YAML does not parse, the controls keep their last values, the error shows
in the tab bar and Next is disabled.

**Taking a dataset out** is unticking it on step 2, which adds its id to `select.exclude.ids`. So a person's
revoke is one more edit of the definition, visible in the YAML tab, and the next plan leaves the dataset out.

### 3.4 Prepare

*Prepare selected* is `POST /prep` as today (a request of the ticked records, with the host approval dialog),
plus one new field, `buffer: <number>`, stored in the request's data. The request is its own subject and
`MiaPrep` is unchanged. Step 3 lists the requests whose `buffer` is this one, with their stage and *Cancel*, and
for each fitting dataset that is not prepared its state as the registry has it.

The buffer does not wait on its requests: nothing in `MiaBuffer` knows of them. *Continue with N prepared
datasets* is the `go` the page sends today (`plan_first`), enabled when at least one fitting dataset is prepared.
A dataset that is prepared later joins at the next plan.

A request filed from the Datasets tab has no `buffer`. With the Preparation tab gone, the list of all requests
is reached by a link on the Datasets tab; one request is also found through *Needs you* when it is stuck and
through the search box by its number. A stuck request that has a `buffer` links to that buffer's step 3.

### 3.5 Forward and back before serving

Forward is today's actions: `go` (plan), `approve` at `plan-review` (run, with hosts), `approve` at `staged`
(serve).

Back from step 4 or 5 is `cancel`, after which the buffer rests and steps 1 to 3 edit it; the next `go` plans
again from the text and the prepared datasets of that moment. The page says before it sends the cancel that
running jobs are killed. Staged artifacts that the new plan still names are reused, as `_reuse` does today.

On steps 1 to 3 a change clears the ticks of the later steps: they are done again.

### 3.6 Serving is fixed; stop serving

At `ready` and `idle` every earlier step can be opened and none can be changed; the page shows the controls
disabled with a line that says why. The one button is *Stop serving*.

**Stopping interrupts training.** A consumer of a stopped version gets `409 buffer stopped` on its next request,
and `BufferDataset` raises it (`BufferNotApproved`): a run does not wait, it fails. So *Stop serving* asks first.
It names the consumers that drew in the last minutes, says their runs will fail until a version serves again, and
says that a changed buffer comes back under another id, which each run must be given. Replacing a version is not
a seamless deployment and the page does not suggest it is.

`unserve` asks the workflow to stop serving: it writes the buffer event `stopped` on the version (not in
`SERVING`), keeps its volumes registered, so what is on disk stays, and sets the stage `stopped`. From there:

- **Serve it again:** `approve` at `stopped` writes `ready` on the same version, and the buffer's definition is
  that version's again. When the definition was edited since, the page says before it sends the approve that the
  edits are discarded.
- **Change it:** `edit`, then `go`. The round plans and sources a new version (§3.7) while the stopped one stays
  as it is. When the new version serves, the stopped one is retired.
  - Cancelled, the buffer rests at `stopped` again, not at `cancelled`.
  - Failed, it rests at `needs-human` as every failed round does, so it is listed under *Needs you*. The buffer
    still holds the stopped version: `approve` there serves it again with its own definition, `retire` releases
    it. The failed round's own unfinished version is retired as a failed round's is today.
- **Retire:** `retire` at `stopped` releases the version's staged data, as retire does today.

**Space for the replacement.** The stopped version holds its data until the replacement serves, and a changed
recipe may reuse little of it, so both may not fit. Step 4 of a stopped buffer shows what the stopped version
holds, what the new plan needs (less what it reuses, where that is known), and what the staged limit
(`quota.staged_bytes`) leaves. buffer-api makes the same check when the approve at `plan-review` arrives:

- They fit together, or there is no limit: the round runs and the stopped version is kept until the new one
  serves.
- They do not: the approve is refused unless it carries `release`. The forward button then reads *Release
  version N and run*: the stopped version is retired first and cannot be served again without staging it anew.
  The step says that its space is free only once the keeper may evict it (`quota.hold_hours`).

`# ponytail:` the check is advisory. Nothing reserves the space, so two buffers can pass it at once. A hard limit
over all buffers, with reservations taken before stage jobs run, comes with the moving window (§3.9) and goes
behind this same check.

The line of a stopped buffer says how many fitting datasets were prepared since the stopped version was
planned; step 4 says which datasets the new plan adds and takes out against it. Whether a dataset's staged
artifact is reused is known when sourcing runs (`_staged_before`), so step 5 says it per dataset.

`unserve` abandons a rebuild of a lost volume, as a `go` or a retire does today. The force `stop` retires every
version of the buffer, a stopped one too, as it does today.

The keeper counts a `stopped` version as it counts an `idle` one: after `retire_after_days` it is retired.

**Datasets that are not staged when the buffer starts serving.** The approve at `staged` settles what the
version is. A planned dataset that is not staged at that moment falls on one of two sides, by what it waits for:

- **It waits for a person** (`waiting-drop`: files someone copies in; `waiting-dataset`): it is dropped from this
  version, with an entry that names it, and joins the next version if it is ready by then. `dropped` is not
  accepted at `ready`.
- **It waits for space or a lock** (`blocked`: the raw cache is at its quota; `waiting-lock`): it stays in the
  version. It is part of the plan that is not on disk yet, and is staged when it can be, with no click. This is
  the one way the data of a serving version grows, and where the moving window of §3.9 starts from.

While part of a version is not on disk, crops are drawn from what is: the mix a consumer gets follows the disk,
not the plan's weights. Step 6 shows both mixes. Val and test lists behave as they do today: a list grows at
its end as datasets arrive and is never reordered, so it is not complete when serving starts.

### 3.7 Versions and ids

- A version's id is the id its round has today, `i<buffer number>-e<go>`, made when the round starts: the plan
  and its events are already written under it. The form does not change, and a consumer treats it as opaque.
- The version **number** the page shows (version 2) is a count: this is the second version of the buffer that
  served. It is computed from the buffer's versions and their events when the page asks, and nothing stores it.
- At most one version of a buffer serves at a time.
- An earlier version stays readable: step 6 lists the buffer's versions (number, id, when it served, how it
  ended), each opening the read-only view a buffer id has today, with the definition its round ran.
- A consumer that names a retired version gets 410 as today, and the message names the version that replaced it
  when there is one.
- No route takes a name where it takes an id or a number.

### 3.8 The page

- Tabs: Needs you, Datasets, Buffers, Cache, Cleaner, Decisions, Docs.
- `#buffers` is the list. `#buffer/new` is an unsaved buffer at step 1 (the Datasets tab's *buffer from these
  datasets* opens it prefilled). `#buffer/<number>` opens the step of the stage; `#buffer/<number>/<step>` a
  step already passed.
- The rules of the page hold: no build step, modules import downward only, no token, action routes checked
  against the stage table, a job's or record's text inserted as text.

### 3.9 A buffer larger than the disk, now and with a moving window

The case this flow must hold for, and a common one: a diverse buffer over many datasets whose plan needs far more
staged data than the disk allows, say 1 PB planned against 100 TB. Such a buffer stages up to the limit and
serves; as its consumers use what is staged, that data is evicted and the next part of the plan is brought in. A
pipeline trains on millions of samples through a small, temporary amount of disk, and the disk must be large
enough that replacements arrive as fast as they are used.

**Now.** A serving version holds a reference to every volume it registered, the keeper evicts only rows nothing
references, and a dataset that does not fit stays `blocked`. Until the window is built, a version larger than the
disk serves what fits, and the rest waits for space that only another buffer's retirement frees. Step 4 says so in
those words and step 6 shows the planned mix beside the mix on disk. Nothing rotates, and nothing is hidden.

**What this flow settles so that the window is added without changing it.**

| This flow | Why the window leaves it alone |
| --- | --- |
| A version is a population and a recipe under an opaque id (§3.1, §3.7). | The window changes residency, never the version: ids, routes and a consumer's `buffer=` stay. |
| The approve does not wait for the whole plan; `staged` means nothing more can run now (§3.6). | A version larger than the disk is approved the same way. |
| No stage, event or id stores residency or a run's progress (§3.1). | Both get state of their own; the stage table is not touched. |
| The page reads what is on disk through one read: on disk, waiting, the two mixes (§2). | The window changes what is behind that read; step 6 gains numbers, not structure. |
| This spec adds no deletion. The keeper alone deletes, and `delete_volume` stays for a revoke and a retire. | Eviction from a serving version is a new operation of the keeper. It is not `delete_volume`, which drops the volume's frozen rows. |
| One version of a buffer serves at a time (§3.7). | There is one population per buffer to walk. |
| Stop serving keeps what is on disk (§3.6). | A stopped window resumes from where it was. |
| The replacement's space is a check with a stated policy (§3.6). | Reservations go behind the same check. |
| The consumer's contract is today's: 409 when stopped, `BufferEmpty` when nothing can be drawn. | Waiting for refill is added to the client; nothing here promised it. |

**What the window's own spec must define.** From the review; this spec settles only the first sentence of 1.

1. **Sampling.** The population is the plan's fixed candidates: the window never serves a crop the plan did not
   choose. Open: one traversal, drawing with replacement (today's sampler), or a bounded number of repeats; over
   what span the weights are kept (a batch, a window, a pass, a run); whether progress is shared by consumers or
   kept per training run; what is served when part of the mix is not on disk. The delivered mix is measured
   beside the planned one.
2. **Read lifetime, apart from sampling progress.** A draw is not evidence that data may be deleted: the client
   queues crop descriptors and reads their files later, and a loader prefetches. Outstanding reads need a lease
   or the like, also for a client that crashed; the keeper deletes on that evidence; an artifact shared by
   buffers counts every buffer and reader that needs it.
3. **Evaluation independent of residency.** Which crops are in a val or test list, and their order, do not depend
   on what is on disk; their data is pinned, or staged again by a stated rule; eviction never removes a list's
   rows.
4. **Capacity.** A hard limit on staged space over all buffers, with reservations taken before stage jobs run
   (today `check_quota` guards the raw cache only), and space set aside for builds, reads and evaluation.
5. **Throughput and underflow.** Refill is queueing, download, staging, checking and saving, and the window
   absorbs the variation of all of it. Step 6 then shows the time until the data on disk is used up, refill
   latency and throughput, the time consumers starved, unique samples against repeats, and the delivered mix.
   What a consumer gets when refill falls behind is defined before the feature is called ready.
6. **Reproducing against replaying.** A version id reproduces a population. Replaying a run's sequence needs
   state of the run.

**Its proof** is a small integration scenario, not stage transitions alone: a plan several times the disk, two
independent training runs, a slow reader, a client that crashes, evaluation during eviction, and a replacement
version that reuses little. It checks the sampling mix, read safety, recovery and capacity.

## 4. Decided by the user (2026-11-17)

1. The page shows buffers only; specs are not a thing a person sees.
2. A buffer is a linear flow in the browser, and a saved buffer opens where it was.
3. Step 1 holds the definition and four stats only; the dataset table is step 2, where the buffer is saved.
4. Controls and YAML are a tab pane over one definition, in sync. *Datasets that fit* and *Prepared* are a narrow
   first column beside *Weight share by modality*.
5. The buffer waits at Prepare until the person goes on; they choose what to prepare and can always continue
   with what is prepared.
6. A buffer can be changed until it serves. Serving is static. Serving can be stopped at any time; then datasets
   can be taken out, newly prepared ones taken in, and so on.
7. A changed buffer served again is a different version. The API names buffers by id, never by name; the name is
   a label, not unique, with no computational value.
8. Datasets that wait for a person when the buffer starts serving are left out and join the next version.
9. "Static" must not mean the bytes on disk: a buffer far larger than the disk stages up to the limit, evicts
   what was used and brings in new data, and that use must be supported.

Shown to the user as guesses in the mockup and not objected to: the Preparation tab goes; taking a dataset out is
an untick that writes `select.exclude.ids`; `stopped` is a stage; earlier versions stay readable; an unchanged
buffer served again keeps its id; Retire is on the stopped buffer. The mockup first showed ids as
`b<number>-v<n>`; §3.7 keeps today's ids instead, which is the author's call.

10. This flow must be upgradable to the moving window; the architecture review of 2026-11-17 is the guide to
    what the window needs (§3.9).

This replaces the ruling of 2026-11-16 that a spec is frozen once it has a buffer and is changed by cloning.

## 5. The author's calls

- The action is named `unserve` and `edit` is settled by buffer-api alone (§3.2, §3.3).
- A saved buffer at rest opens at step 3 (§2); the step is derived from the stage, nothing stores it.
- `approve` at `stopped` serves the stopped version and puts its definition back; edits made since are
  discarded after the page said so (§3.6).
- A cancelled round of a stopped buffer returns it to `stopped`; a failed one rests at `needs-human`, listed
  under *Needs you*, where `approve` and `retire` then act on the version still held (§3.6).
- A replacement keeps the stopped version when both fit and needs the person to release it when they do not;
  the check is advisory until the window brings reservations (§3.6).
- Stop serving asks first and names the consumers it will interrupt (§3.6).
- A version fixes the plan's candidates, so the window never serves crops outside them (§3.9).
- Val and test lists are promised only as they behave today (§3.6).
- The keeper retires a `stopped` version like an idle one (§3.6).
- Ids keep today's form and the version number is a count of the versions that served (§3.7): a new id form
  would have to exist from the go, when planning already writes under it, and would number every plan.
- A change of the name alone is not a change of the definition and is allowed while serving (§3.3).
- The action route takes the number for every buffer (§3.1).
- What is not a step has a fixed place: the stopped line, step 6, a row of step 5, the *More* menu (§2).
- Step 4 names datasets added and taken out and does not count what is staged again (§3.6).
- Which waits leave a dataset out at the approve and which keep it planned (§3.6): the user said that late
  datasets join the next version and that a buffer larger than the disk must work; the split by what a
  dataset waits for is how both hold.
- The moving window is its own spec, and this one only keeps the flow true for it (§3.9).
- Requests keep their own budget root and stay reachable without a tab (§3.4).
- An unticked dataset is excluded with no reason asked (§3.3).
- The timeline sits under every step (§2).
- While the YAML holds something no control can show, the Controls tab names it and is read-only, so a control
  cannot drop it.

## 6. Left to the builder

- The modules of the page and the table in `docs/development.md` that orders them.
- How the page finds a buffer's requests (a filter on the list route, or over the list it already reads) and a
  buffer's versions (a spec's buffers are found by the id prefix `i<number>-e` today, which still holds).
- The workflow changes (`unserve`, `approve` and `go` at `stopped`, a round of a stopped buffer that ends
  without serving) behind `workflow.patched`, with re-recorded replay fixtures.
- The one read behind step 6 (on disk, waiting, the planned mix and the mix on disk); `GET /buffers/<id>/status`
  and the asset rows may be enough.
- The cleaner's rules against the new table: it sends `cancel`, `stop` and `go`.
- The wording of every button, refusal and timeline entry beyond what the mockup shows.

**To verify while building**

- A `cancel` retires its round's buffer. A `go` right after must reuse the staged artifacts the new plan names
  and not find them released or deleted.
- A pool change moves every dataset's share of `sampling.crops`, and the plan digest holds the rounded share:
  what step 5 calls reused comes from what sourcing found, never from which datasets were added.
- How soon a released version's data is free: the keeper evicts rows nothing references only after
  `quota.hold_hours`, and a blocked fetch may wait that long.
- The name enters no digest (spec hash, plan digest, artifact id), so a rename restages nothing.
- The plan can say its staged size before sourcing (sparse staging writes only the planned crops' chunks). If
  it cannot, step 4 shows the bytes to fetch and says the staged size is not known yet.

## 7. Build order

Each slice works on its own and is shipped with its tests.

1. **The flow over today's routes.** The Buffers list with *New buffer*, the strip, steps 1 to 6 mapped to the
   stages, Controls and YAML tabs, the stat column, step 2's table and ticks, Save. `POST /prep` takes `buffer`
   and step 3 shows the buffer's requests. The Preparation tab and the designer's fold-out go. No workflow change.
2. **Editing.** The `edit` action; the frozen rule goes; Back from steps 4 and 5 cancels.
3. **Stop serving.** `unserve`, `stopped`, serve again, retire from stopped; `ready` and `idle` stop taking `go`,
   `revoke`, `retire` and `dropped`; the approve at `staged` drops the datasets that wait for a person; the
   keeper's rule; the question before a stop; the space check at Run; what steps 4 to 6 say about the disk.
4. **Versions.** One serving version, the replaced-by message, the versions list with its numbers, action
   routes by number.

Slices 1 and 2 do not touch what the moving window will change. Its spec is best written before slice 3 is
built, since both change what the round does while a version serves.

## 8. Tests

- The stage table: each new stage and action accepted where §3.2 says and refused elsewhere, with a stale
  `revision` refused.
- `edit`: stores text and name, refuses what is not a spec, refused at every stage a round is in progress or
  serving.
- `unserve` then a consumer draw is 409; `approve` at `stopped` serves the same id, and after an edit also puts
  that version's definition back; `go` from `stopped` serves a new id and retires the old; that round cancelled
  rests at `stopped`, failed at `needs-human` and is listed under *Needs you*, and from both the old version
  serves again.
- A stopped version and a new plan that do not fit the staged limit together: the approve at `plan-review` is
  refused, and with `release` retires the stopped version first.
- The approve at `staged` with one dataset waiting for a drop and one blocked on quota: the first is dropped
  with its entry and a later `dropped` is refused; the second stays planned and is staged when space allows.
- A name-only `edit` at `ready` is accepted and the served id is unchanged; any other `edit` there is refused.
- No name resolves: a request for `/buffers/<name>` and an action on `/specs/<name>/actions` are 404.
- The page (`tests/test_buffers_ui.py` and the node tests): step from stage; controls to YAML and back give the
  same text; an untick writes `select.exclude.ids`; every route the page calls is a read or an action route.
- Replay tests for every workflow change.
