# MIA: a buffer is one flow — Spec

**Status:** v0.2 · 2026-11-17 · draft, not approved; one question is open (§3.6). v0.2 settles what a
builder's read of v0.1 asked. Brainstormed with the user 2026-11-17; a clickable picture of
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
- A changed buffer that serves again is a new version with a new id. Consumers name the id.

**Non-goals**

- How a dataset is prepared, how a plan is computed, how volumes are staged. The rounds do what they do today.
- The automatic paths that change a serving buffer: the keeper's `revoked`, `degraded`, `idle` and `retire`
  signals stay as they are.
- A new budget root: a preparation request stays the root of the dataset rounds it starts.
- Keeping an unsaved definition across a page reload.
- Identity (who clicked) beyond what the action routes record today.

**Anti-goals**

- A serving version whose contents change because a person clicked something.
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
| 2 Datasets | Every dataset the definition admits, with its state and weight and a tick *In buffer*. Where the others left, behind a toggle. | Save buffer and continue (first time); Next: prepare | Change the definition | as step 1 |
| 3 Prepare | The fitting datasets that are not prepared, a tick each, *Prepare selected*; the requests this buffer filed and where each dataset is. | Continue with N prepared datasets | Datasets | `new`, `stopped`, `cancelled`, `needs-human`, `retired` |
| 4 Plan | The plan: pool, weights, crops, bytes to fetch, hosts to approve. | Run: fetch and stage | Prepare more datasets | `planning`, `plan-review` |
| 5 Source | The sourcing board, then the wait for an approve. | Approve and serve | Change the buffer (cancels the jobs) | `sourcing`, `staged` |
| 6 Serve | Version and id, usage, consumers, the line to paste into a training script, earlier versions. | (none) | Stop serving | `ready`, `idle` |

A saved buffer at a resting stage opens at step 3: that is where it waits for the person.

Below the step, on every step, is the buffer's timeline as the subject view shows it today.

**What is not a step.** A step has one way forward and one back; everything else a stage accepts sits where it
belongs and never beside the forward button:

- A stopped buffer shows one line above steps 1 to 3: which version is stopped, how many fitting datasets were
  prepared since it was planned, and the buttons *Serve it again* and *Retire* (§3.6).
- *Keep* is on step 6 of an idle buffer, in the line that says no consumer has drawn.
- `dropped` is on step 5, in the row of the dataset that waits for its files.
- Clone, Shelve or Unshelve and the force stop are in a *More* menu at the heading, which offers only what the
  stage accepts.

## 3. Design

### 3.1 One noun, and what identifies it

A buffer in the list is a subject of kind `buffer`, as today's spec is: a number, a stage, a timeline, and the
definition text in its data. What today's code calls a buffer (the row a round creates when sourcing starts, with
volumes and events) is a **version** of it.

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
| `ready` | done | `edit` (the name only), `unserve`, `dropped` (open, §3.6), `stop` |
| `idle` | waiting | `keep`, `edit` (the name only), `unserve`, `stop` |
| `stopped` | done | `edit`, `go`, `approve`, `retire` |
| `retired` | done | `edit`, `go` |
| `needs-human` | stuck | `edit`, `go` |
| `cancelled` | done | `edit`, `go` |

- **`stopped`** (new): the buffer has a version that is not serving and whose volumes are kept.
- **`edit`** (new): replaces the definition text. §3.3.
- **`unserve`** (new; the button reads *Stop serving*): §3.6. Not named `stop`, which is the force stop of a run
  that does not answer.
- `ready` and `idle` no longer take `go`, `revoke` or `retire` from a person: a serving buffer is fixed, and the
  way to change or retire it starts with `unserve`.
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

`unserve` asks the workflow to stop serving: it writes the buffer event `stopped` on the version (not in
`SERVING`, so consumers are refused with `409 buffer stopped`), keeps its volumes registered, and sets the stage
`stopped`. From there:

- **Serve again unchanged:** `approve` at `stopped` writes `ready` on the same version. buffer-api refuses it
  when the stored definition is not the one that version was built from (§3.3: the name aside).
- **Change it:** `edit`, then `go`. The round plans and sources a new version (§3.7) while the stopped one stays
  as it is. When the new version serves, the stopped one is retired. If the round is cancelled or fails, the
  buffer rests at `stopped` again, not at `cancelled` or `needs-human`: the old version can still serve. The
  round's result entry and the step it ended at say what happened, and the round's own unfinished version is
  retired as a failed round's is today.
- **Retire:** `retire` at `stopped` releases the version's staged data, as retire does today.

The line of a stopped buffer says how many fitting datasets were prepared since the stopped version was
planned; step 4 says which datasets the new plan adds and takes out against it. Whether a dataset's staged
artifact is reused is known when sourcing runs (`_staged_before`), so step 5 says it per dataset.

`unserve` abandons a rebuild of a lost volume, as a `go` or a retire does today. The force `stop` retires every
version of the buffer, a stopped one too, as it does today.

The keeper counts a `stopped` version as it counts an `idle` one: after `retire_after_days` it is retired.

**Open: datasets that are not staged yet when the buffer starts serving.** Today the approve at `staged` makes
the buffer `ready` while datasets that still wait (for files a person copies in, for quota, for a lock) keep
their place and join the serving version when they land (`_await_stragglers`); `dropped` at `ready` is the click
that tells the round a dataset's files are in place. That changes a serving version. Either the approve leaves
those datasets out of this version and they join the next one (the stage table then has no `dropped` at `ready`,
and the workflow abandons them at the approve), or they are the one exception to "serving is fixed". The user
decides; the author recommends the first.

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

Shown to the user as guesses in the mockup and not objected to: the Preparation tab goes; taking a dataset out is
an untick that writes `select.exclude.ids`; `stopped` is a stage; earlier versions stay readable; an unchanged
buffer served again keeps its id; Retire is on the stopped buffer. The mockup first showed ids as
`b<number>-v<n>`; §3.7 keeps today's ids instead, which is the author's call.

This replaces the ruling of 2026-11-16 that a spec is frozen once it has a buffer and is changed by cloning.

## 5. The author's calls

- The action is named `unserve` and `edit` is settled by buffer-api alone (§3.2, §3.3).
- A saved buffer at rest opens at step 3 (§2); the step is derived from the stage, nothing stores it.
- `approve` at `stopped` is refused once the definition changed (§3.6).
- A cancelled or failed round of a stopped buffer returns it to `stopped` (§3.6). Such a failure is not listed
  under *Needs you* and the cleaner does not retry it: the person who pressed Continue sees it on the step.
- The keeper retires a `stopped` version like an idle one (§3.6).
- Ids keep today's form and the version number is a count of the versions that served (§3.7): a new id form
  would have to exist from the go, when planning already writes under it, and would number every plan.
- A change of the name alone is not a change of the definition and is allowed while serving (§3.3).
- The action route takes the number for every buffer (§3.1).
- What is not a step has a fixed place: the stopped line, step 6, a row of step 5, the *More* menu (§2).
- Step 4 names datasets added and taken out and does not count what is staged again (§3.6).
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
- The cleaner's rules against the new table: it sends `cancel`, `stop` and `go`.
- The wording of every button, refusal and timeline entry beyond what the mockup shows.

**To verify while building**

- A `cancel` retires its round's buffer. A `go` right after must reuse the staged artifacts the new plan names
  and not find them released or deleted.
- A pool change moves every dataset's share of `sampling.crops`, and the plan digest holds the rounded share:
  what step 5 calls reused comes from what sourcing found, never from which datasets were added.
- The name enters no digest (spec hash, plan digest, artifact id), so a rename restages nothing.

## 7. Build order

Each slice works on its own and is shipped with its tests.

1. **The flow over today's routes.** The Buffers list with *New buffer*, the strip, steps 1 to 6 mapped to the
   stages, Controls and YAML tabs, the stat column, step 2's table and ticks, Save. `POST /prep` takes `buffer`
   and step 3 shows the buffer's requests. The Preparation tab and the designer's fold-out go. No workflow change.
2. **Editing.** The `edit` action; the frozen rule goes; Back from steps 4 and 5 cancels.
3. **Stop serving.** `unserve`, `stopped`, serve again, retire from stopped; `ready` and `idle` stop taking `go`,
   `revoke` and `retire`; the keeper's rule.
4. **Versions.** One serving version, the replaced-by message, the versions list with its numbers, action
   routes by number.

## 8. Tests

- The stage table: each new stage and action accepted where §3.2 says and refused elsewhere, with a stale
  `revision` refused.
- `edit`: stores text and name, refuses what is not a spec, refused at every stage a round is in progress or
  serving.
- `unserve` then a consumer draw is 409; `approve` at `stopped` serves the same id; an edit makes that approve
  a refusal; `go` from `stopped` serves a new id and retires the old; that round cancelled or failed rests at
  `stopped` with the old version still able to serve.
- A name-only `edit` at `ready` is accepted and the served id is unchanged; any other `edit` there is refused.
- No name resolves: a request for `/buffers/<name>` and an action on `/specs/<name>/actions` are 404.
- The page (`tests/test_buffers_ui.py` and the node tests): step from stage; controls to YAML and back give the
  same text; an untick writes `select.exclude.ids`; every route the page calls is a read or an action route.
- Replay tests for every workflow change.
