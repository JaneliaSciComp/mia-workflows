# MIA: the page acts, GitHub records — Spec

**Status:** draft v0.4 · 2026-11-11 · not yet approved. v0.4: the service accepts only the actions a subject's
stage allows, and a round in progress accepts only `cancel`, which is new (§5; the user's ruling). v0.2 took a
reviewer's pass against the code: the claim that workflows keep their commands holds, with two named changes
(§6); ids survive a ledger restore (§4.1);
adoption runs with the first slice (§11). v0.3 takes a second review, on delivery and recovery: an action is
`pending` until Temporal has it and a retry reuses its id (§5); a decision names the state the person saw (§5);
the mirror catches up after a crash (§7); adoption carries the stage and slice E waits for label rounds to drain
(§11). Decided by the user 2026-11-11: datasets, buffers and
preparation requests are started and steered from the web page, never from GitHub; a click starts or signals the
workflow at once, with no poller in between; the page always shows the state of the system; identifiers are issued
by the registry and a GitHub issue is a read-only view of a workflow, written whenever GitHub answers; a person
can get from a GitHub issue number to the page; autodev stays interactive on GitHub and knows nothing about
mia-data; no identity, no notifications and no mentions for now (Okta or GitHub sign-in is later work).
Calls marked *my call* were made without the user and are listed again in §13.
Written from the code on `autonomous-gates` (18ac4e1), which this builds on (the `assessments` table of
[autonomous gates](2026-11-09-autonomous-gates.md) §5), and from
[prep and buffering](2026-10-25-prep-and-buffering.md) §7, whose last paragraph names this as the one change that
spec was shaped to allow.
**Repos:** this repo (`mia_buffers/`, `mia_data/`, `mia_core/`, `mia_ops/`); the data and buffers issue repos
(human labels removed). `autodev/` and mia-snippets are not changed.

## 0. Summary

Today GitHub does three jobs for mia-data and mia-buffers: it notifies, it carries discussion, and it stores state
and commands. The third is what hurts: labels are an unordered set, comments are a protocol with parsers and
windows, every gate costs a poll interval, and a buffer fans out through issues in another repo.

After this spec:

- **The page acts.** A button is a `POST` to buffer-api, which records the action and starts or signals the
  Temporal workflow in the same request. Nothing polls GitHub for a command.
- **The registry holds the state.** Identifiers, buffer specs, every action a person took and the timeline of
  every round live in buffer-api's ledger. The page reads them and is told when they change.
- **GitHub records.** A mirror writes each dataset's, buffer's and request's timeline to its issue, in order,
  retrying for as long as GitHub is away. No round waits for it and nothing reads the issue back.
- **The workflows barely change.** They already take signals and call activities by name. The signals now come
  from the service instead of a poller, and the activities read and write the registry instead of GitHub. The
  commands a workflow issues stay the same, so recorded histories replay and running rounds carry on. Two things
  are added: stage writes are awaited, and a round can be cancelled (§6).
- **Only valid actions.** Each stage has a declared set of actions it accepts. The page offers those and the
  service refuses the rest; a round in progress accepts only *Cancel* (§5).

What stays on GitHub is autodev: its issues, its PRs, and the snippet PR that a dataset round follows (§9).

## 1. Why

The project lead's feedback on the first large preparation test (22 datasets from one request): many steps, unclear
what happens next, human input needed in many places, unclear what to do to unstick a round, too many places to
look. The causes in the code:

- A label is a command with no arguments and no order. `data:approve` beside `data:revoke` needed a table to
  decide; each wrong label needs its own refusal rule.
- Arguments travel as comments: `/revoke <record>` lines, review feedback read from a time window, a root marker
  in an issue body, a buffer spec in a YAML fence that anyone may edit between two runs.
- Every label waits for the next poll, and the poll stops when its worker's checkout is switched.
- One buffer or request opens issues in another repo and a person has to follow each of them.
- GitHub's own limits take the system down (a label description over 100 characters stopped `init-labels`).

## 2. Goals, non-goals, anti-goals

**Goals**

1. Every action a person takes on a dataset, a buffer or a preparation request is a button on the page.
2. A click reaches its workflow within the request; the page shows the action and what followed without a reload.
3. One place shows everything that waits on a person and the action that unblocks it.
4. Nothing in `mia_data/` or `mia_buffers/` reads a GitHub label, issue body, issue state or issue comment of the
   data or buffers repo as input.
5. No round waits for, or fails on, a write to a data or buffers issue; the issue catches up afterwards. (A
   round still reads the catalog and snippets repos and pushes the crop gallery branch: those are code and data,
   not the issue, and are out of scope here.)
6. Recorded workflow histories replay unchanged, and rounds running at cutover continue.

**Non-goals**

- Identity, permissions, notifications, mentions. One function names the actor of a request (§10) so sign-in has
  a single place to plug in.
- Any change to autodev, to how a snippet PR is reviewed or merged, or to mia-snippets.
- A command-line client. The action routes are plain JSON `POST`s; `curl` is the client.
- New review screens per gate. Judges are taking the gates over (autonomous gates §14); the page gets the actions
  that stay human and one generic decision on an item that waits.
- A runbook of stuck states. The page shows the reason a round wrote; a table of remedies is separate work.

**Anti-goals** (failures even if the rest works)

- A click that looks accepted while nothing started.
- A state on the page that says "working" for a round nothing is working on.
- Two sources of truth: any path where a person changing something on GitHub changes what the system does.
- autodev importing from, signalling or naming mia-data.
- A round blocked or failed because GitHub was slow, rate-limited or down.

## 3. Every GitHub input today, and what replaces it

| input today | read by | replaced by |
|---|---|---|
| `data:go` / `approve` / `reject` / `revoke` label | mia-data poller, `deliver_label` | dataset actions (§4.2, §5) |
| `buffer:go` / `plan` / `approve` / `revoke` / `keep` / `retire` label | mia-buffers poller | buffer actions |
| `prep:go` label | mia-buffers poller | prep action |
| who added the label and whether they have write access | `deliver_label` | nothing (no identity, §10) |
| buffer spec in the issue body's YAML fence | `admit` (buffers) | the spec text carried by the action |
| preparation request in the issue body's YAML fence | `prep_admit` | the request text carried by the action |
| record marker on line 1 of a dataset issue | `admit` (data) | the dataset's registry row (§4.1) |
| root marker on line 2 of a dataset issue a buffer or request opened | `admit` (data) | `root` carried by the action |
| reviewer's comments between the report and the reject | `read_feedback` | the note carried by the reject action |
| `/revoke <record>` comment lines | `revoke_targets` | the records carried by the revoke action |
| closing a buffer or request issue | mia-buffers poller, `retire_closed` | the `stop` action |
| stage labels listed to find waiting rounds | mia-buffers poller | not needed: nothing polls |
| files appearing in `<raw_root>/<record>/incoming/` | mia-buffers poller, `notice_drops` | the `dropped` action |
| the dataset issue found by its record marker, its `data:*` stage label, whether `data:go` is on it | `_nudge` | the dataset's registry row |
| `data:go` added by a trusted bot on another repo's issue | `_nudge`, `trusted_bots` | a dataset action sent by the worker |
| every issue body of the data repo, to find datasets that have none | `seed` | not needed: the mirror opens issues (§7) |
| the newest open buffer issue, for the keeper's over-quota comment | `_over_quota_comment` | a notice on the Cache view |

The writers also read (current labels before a swap, existing comments before a once-only post); those reads move
into the mirror with the writes. Not replaced: mia-data's watch on snippet PRs and its `/approve <head>` and
`/revise` comments there (§9).

## 4. The registry

Three additions to buffer-api's ledger. All are host-local SQLite like the rest of it.

### 4.1 Subjects and their numbers

A **subject** is a dataset, a buffer spec or a preparation request. It has:

- a **kind**: `dataset`, `buffer`, `prep`;
- a **key**, unique within the kind: the catalog record id; the spec's `name`; `prep-<number>`;
- a **number**, an integer issued by the registry;
- its current **stage**, set by every stage write, and a **revision**, an integer every stage write raises;
- for a buffer spec, its current spec text;
- an **issue**: the GitHub issue number of its mirror, empty until the mirror has opened one (§7).

The number is what the code calls `issue` today. Workflow ids (`mia-data/<repo>/issue-<number>`), round ids
(`i<number>-e<event>`, `p<number>-e<event>`), buffer ids, round directories, job keys, budget roots
(`<repo>#<number>`) and every activity signature keep their form and carry the number. *My call:* this is how
"registry ids" is delivered. The alternative, string keys through every signature and a new round id format,
renames wire-format fields and invalidates every recorded history for no gain a person would see: the page
addresses subjects by key, and the number shows only inside round ids.

- A subject that exists today keeps its issue number as its number, so its workflow, rounds and buffers are
  found as before and `issue == number`.
- A new subject gets a number from one sequence shared by all kinds. Its issue, once opened, has a different
  number. One sequence, because buffers and requests share a budget root form (`<buffers_repo>#<number>`) and two
  counters would hand a buffer and a request the same root.
- **Numbers and action ids never repeat, even after the ledger is restored from a backup** (*my call*). The ledger
  is backed up every `keeper.minutes` and the documented recovery copies the newest backup back; a plain counter
  would then reissue ids. A reissued action id is dropped by the workflow as a redelivery, or finds the earlier
  round's pins; a reissued number lands on another subject's workflow. So both are taken from the service clock
  and bumped past the last one issued: a number is the seconds since 2026-01-01, an action id the milliseconds
  since the epoch. Both are digits only, as every existing pattern requires, and far from any issue number or
  GitHub event id in use. An action newer than the restored backup is gone with its payload: a round that asks
  for it fails saying so, and the person clicks again.
- The word "issue" in old names (`DatasetState.issue`, `issue-<n>` in workflow ids, the `issue` field of a dataset
  row) is historical and holds the number. New code says `number`.
- **A number is never rendered as a GitHub reference.** Today the code builds `<repo>#<issue>` links from it in the
  snippet issue's body, in a request's comments, in the dataset row the page links, and from buffer ids. After
  this spec every such reference is a link to the subject's page, and the page links to the issue it reads from
  the subject.

A buffer spec's name is the long-lived thing a person sees (*my call*, as today's issue is): each run registers a
new buffer whose id is the round's, training jobs keep the buffer they connected to, and the spec's page lists its
buffers newest first. Creating a spec under a name that exists is an edit of that subject.

### 4.2 Actions

An **action** is one thing a person (or a worker on a request's behalf) asked for. Append-only.

```yaml
action:
  id: <integer, §4.1>          # the workflow's event id: the <event> of i<number>-e<event>
  kind: dataset | buffer | prep
  number: <subject number>
  action: go | approve | reject | revoke | keep | retire | dropped | cancel | stop
  actor: page | prep:<number> | buffer:<number>     # §10
  at: <iso time>               # stored exactly as sent in the signal: two activities find their action by it (§6)
  key: <text | null>           # the sender's own name for this request; a repeat is the same action (§5)
  payload:            # what a label could not carry
    spec: <YAML text>            # buffer go, prep go: exactly what runs
    plan_first: <bool>           # buffer go: stop at the plan and wait for an approve before staging
    note: <text>                 # dataset reject: the feedback for the agent
    records: [<record id>, ...]  # buffer revoke; dropped (one record)
    root: <root request id>      # dataset go sent for a buffer or a request: who is charged for this round
  state: pending | sent | failed | refused   # pending until Temporal has answered
  detail: <why it failed or was refused>
```

| kind | actions |
|---|---|
| dataset | `go`, `approve`, `reject` (note), `revoke`, `cancel` |
| buffer | `go` (spec, plan first), `approve`, `revoke` (records), `keep`, `retire`, `dropped` (record), `cancel`, `stop` |
| prep | `go` (spec), `cancel`, `stop` |

Which of these a subject accepts depends on its stage (§5).

Today's `buffer:go` and `buffer:plan` are one action (*my call*). They start the same round and differ in one
thing: a `plan` round stops at the plan and waits for an approve before it fetches anything, a `go` round stages
first and asks at the staged gate. That is an option of `go` (`plan_first`), not a second edge in the state
machine. The workflow keeps its two signals, and the route sends `plan` when the option is set.

The root travels with the action. Today it is fixed when a buffer or a request opens the dataset's issue, so a
dataset that already had an issue is charged to itself whoever asks; from now on each round is charged to whoever
asked for it. That is a change in behaviour, and the intended one.

### 4.3 Timeline

A subject's **timeline** is the ordered list of what happened to it: each action, each stage a round reached with
the text its comment carries today, each refusal, each report. It is what the page shows and what the mirror
copies to GitHub.

```yaml
entry:
  kind, number, seq         # seq orders one subject's entries
  at: <iso time>
  round: <round id | null>
  stage: <stage | null>     # set on a stage change
  body: <markdown, already neutralized>
  marker: <text>            # today's comment marker: a second write with the same marker appends nothing
  mirrored_at, comment_url  # written back by the mirror
```

The marker keeps today's rule that a stage is announced once per round, which is also what makes a retried
activity harmless. A round can enter a stage twice (testing, review, developing, testing again; ready, idle, ready
again): the second entry appends nothing, as it posts no second comment today, but it still sets the subject's
current stage, as it swaps the label today. So the current stage is the subject's own field, not its latest entry.

The existing `buffer_events` table is a different thing (it authorizes serving one buffer) and is not touched.

### 4.4 Routes

A third capability joins `read`, `consumer` and `worker`: **`action`**, the routes the page's buttons call. They
take no token (§10).

| route | capability | what it does |
|---|---|---|
| `POST /datasets/<record>/actions` | action | record the action; allocate the subject on a first `go`; start or signal (§5) |
| `POST /specs` | action | validate a buffer spec; create or update the subject named by `name`; with `action: go`, run it |
| `POST /specs/<name>/actions` | action | a buffer action |
| `POST /prep` | action | validate a preparation request; create the subject; `go` |
| `POST /prep/<number>/actions` | action | `go`, `stop` |
| `GET /subjects/<kind>/<key>` | read | the subject, its current stage, status word (§8.3), valid actions, Temporal run status |
| `GET /subjects/<kind>/<key>/timeline` | read | its entries and actions |
| `GET /needs` | read | every subject that waits on a person, with the actions that unblock it (§8.2) |
| `GET /lookup?q=` | read | subjects matching an issue number, an issue URL, a record id or a spec name (§8.4) |
| `GET /stream` | read | server-sent events: `{kind, number}` whenever a subject, action, timeline or board row changes |
| `GET /health` | read | whether Temporal answers and whether a worker polls each task queue |
| `POST /timeline/<kind>/<number>` | worker | append an entry (the activities, §6); kicks the mirror (§7) |
| `PUT /subjects/<kind>/<number>/issue` | worker | the mirror's write-back |
| `PUT /actions/<id>` | worker | mark an action refused, with the reason |

The worker token on an `action` route is honoured: it lets the sender name its actor (`prep:<number>`), which is
how a preparation request starts datasets (§6).

## 5. From click to workflow

buffer-api gets a Temporal client for the `mia-data` and `mia-buffers` namespaces. It reads their namespaces and
task queues from the `services` block of the one `mia.yaml`, which the loader strips from a service's own view
today. An action route:

1. checks the request: the subject exists (or the action creates it), a spec or request parses, and **the
   subject's stage allows the action** (decided by the user). Today no one place says which action is valid in
   which stage: the stages are named in each package's `STAGES`, and the edges exist only as the workflows' own
   branches and refusals. So each kind gets one table beside its `STAGES`, stage to the actions it accepts and
   whether a round is in progress there, written from what its workflow accepts today. This check, the page's
   buttons, *Needs you*, the status word and `DATA_IN_PROGRESS` all read it. The workflow stays the code that
   carries a round out; a test holds that every stage has a row and that each refusal the workflow tests cover
   agrees with the table. Anything else is answered `409` with the reason, and nothing is recorded or sent.
   **While a round is in progress the table allows only `cancel`** (and `dropped`, which answers a wait the
   round itself opened, and `stop`, below): a new `go`, a revoke or a retire waits until the round has ended or
   been cancelled. The check
   can trust the stage because every stage write is awaited (§6);
2. for an approve or a reject, compares the `revision` the request carries with the subject's and answers `409`
   when they differ, recording nothing (*my call*). The workflow takes any verdict newer than the set under
   review, and a click is always newer, so a page that still showed the previous set could approve crops nobody
   looked at. The registry is never ahead of the workflow, so this check cannot refuse a verdict on the set the
   person did see;
3. inserts the action row (`pending`), which issues its id. A request may carry a `key`; a key the registry has
   seen is that same action: nothing is inserted, and unless it is already `sent` it is sent again under its own
   id (*my call*);
4. calls Temporal exactly as `deliver_label` does today, with a ten-second limit: `start_workflow(<type>,
   <State>(number), id=cfg.workflow_id(number), start_signal=<action>, start_signal_args=[LabelEvent(action,
   str(id), actor, at)])`. `LabelEvent` gets no new field; what a label could not carry is read from the action
   row by its id (§6);
5. on success marks the action `sent` and appends it to the timeline, in one transaction, and answers `202` with
   the action;
6. on any Temporal failure marks the action `failed` with the error and answers `503`.

There is no retry queue and no background delivery. An action that is `failed`, or still `pending` ten seconds
after it was made (the service died between steps 3 and 5, or the answer was lost), is shown as not delivered
with a *Retry* button, which sends the same key. The workflows already drop an event id they have handled, so a
retry of an action that did arrive starts nothing twice. The page makes one key per click.

Three actions are not a start-with-signal:

- **`cancel`** is new, for all three kinds (decided by the user: a round in progress is cancelled before anything
  else is asked of its subject). It is a plain signal to the running workflow. The round ends now: its
  outstanding tasks are cancelled, its cluster job is killed by id (the call the job runner already makes when a
  lock is lost), its reservation is settled and its raw lock released as for any failed round, and the subject
  goes to a new final stage `cancelled`, from which `go` is allowed again. *My calls:* the job is killed, since
  "cancelled" over a job that keeps running would be a false state; whatever follows a dataset reads `cancelled`
  as it reads a round that did not prepare it; a buffer round's unfinished buffer is retired as a failed round's
  is; a round that was following a snippet PR stops following it, and the PR, which is autodev's, stays open;
  cancelling a request also cancels the dataset rounds that request started and that have not settled, and no
  others.
- **`stop`** is the force stop, for a run that does not answer a `cancel` (one frozen by nondeterminism). It is
  `retire_closed`, moved from the poller to the route and otherwise unchanged: it terminates the
  workflow (a terminate reaches a run that nondeterminism froze), retires the subject's buffers with their
  `retired` events, and sets the stage it sets today (`retired` for a buffer, `done` for a request). It is offered
  in the stages the poller handles today (the live buffer stages and a requested preparation). As today, it does
  not kill a cluster job that is already running and does not stop the dataset rounds a request started; the
  button's confirmation says both. Stopping those is separate work.
- **`dropped`** sends the existing plain `dropped(record)` signal to a running workflow. The poller sent it only
  once the `incoming/` listing had not changed for a whole interval, because the workflow accepts any non-empty
  file and records its hash; a click during a copy would record a truncated file as good. The route therefore
  refuses while any file under `incoming/` was modified in the last minute, and says so (*my call*).

The workflow's own refusal stays as the backstop for a click that crossed a stage change (two people, or a second
`go` before the round's first stage is written). The existing `refuse_label` activity marks the action `refused`
and appends the reason to the timeline, where the page shows it against the click.

The Temporal client connects lazily: buffer-api starts and serves reads with Temporal down, and `/health` says so.

## 6. Activities read the registry; workflows keep their commands

The rule for this whole change: **an activity keeps its name and signature and changes what it talks to.** A
workflow that issues the same commands in the same order replays its recorded histories, so no `workflow.patched`
guard and no re-recorded fixture is needed, and a round running at cutover simply finds its next activity reading
the registry. Where a step here cannot be done without changing a workflow's commands, it is guarded and the
fixture re-recorded as `AGENTS.md` requires; the plan names each such place.

| activity | today | after |
|---|---|---|
| `admit` (data) | record from the issue body's marker, root from a marker on its second line | record from the subject, root from the action's payload |
| `admit` (buffers), `prep_admit` | YAML fence of the issue body | `payload.spec` of the action whose id is the event id |
| `set_stage` (both), `set_prep_stage` | registry row best effort, then label swap and comment | the subject's stage, the registry row and a timeline entry; no GitHub |
| `comment` on a data or buffers issue, `comment_plan`, `comment_prep`, `comment_drop`, the staged board | a GitHub comment, once per marker | a timeline entry, once per marker |
| `comment` with `repo="snippets"` | a comment on the snippet issue or PR | unchanged (§9) |
| `refuse_label` | remove the label, comment why | mark the action refused, timeline entry |
| `read_feedback(issue, since, until)` | reviewer comments in the window, filtered by write access | the notes of the subject's reject actions with `since < at <= until` |
| `revoke_targets(issue, actor, until)` | `/revoke` lines in the actor's comments of the week before `until` | `payload.records` of the subject's revoke action whose `at` is `until` |
| `nudge_dataset`, `prep_nudge` | find or open the dataset issue, read its stage label, add `data:go` as a trusted bot | read the dataset's row; `POST /datasets/<record>/actions` `go` with the worker token, actor, root and the key `<round id>/<record>`, so a retried activity sends no second `go` |
| `_over_quota_comment` | comment on the newest open buffer issue | a registry notice the Cache view shows; not mirrored (*my call*) |
| `poll` (buffers) | labels, drops, closed issues | deleted, with its schedule |
| `poll` (data) | labels, snippet PR watch, status on PRs without a snippet | the last two only (§9) |

`read_feedback` and `revoke_targets` are not given the event id, so they find their action by subject and time;
this is why an action's `at` is stored exactly as the signal carries it. `refuse_label` checks the label against
`HUMAN_LABELS`, and the workflows keep passing `data:go`, `buffer:keep` and the rest: those sets stay as the
vocabulary of action names, and only `init-labels` stops creating them on GitHub.

The registry write becomes the required one. Today it is best effort and GitHub is required; the roles swap, so an
activity that cannot reach buffer-api retries as it retries GitHub today.

**The two places a workflow changes.**

- *Stage writes are awaited again* (*my call*). In mia-data a stage write inside a round runs beside the round,
  is cancelled by the next stage write, and its failure is only logged; only a final stage is awaited. That was
  built because GitHub's write budget once held rounds for 25 minutes. With GitHub out of the activity the write
  is a local call, and a lost or late one would make the registry's stage wrong, which the action check (§5) and
  *Needs you* both rely on. So every stage write is awaited, behind one `workflow.patched` guard with a
  re-recorded fixture. The cost: a round waits at its next stage while buffer-api is away.
- *The `cancel` signal* (§5) is a new handler and a new way for a round to end, in `MiaDataDataset`, `MiaBuffer`
  and `MiaPrep`. No recorded history holds a cancel, so the fixtures replay as they are; each workflow gets a new
  one for a cancelled round.

Everything else replays as recorded.

During the build (§14, slices B to D) labels keep working beside the page: an event id the registry does not know
is a GitHub label event and the old source applies, and a label on an issue the registry has no subject for
adopts that issue first (§11). Both are deleted in the last slice.

## 7. The mirror

One `MiaMirror` workflow per subject (id `mirror/<kind>/<number>`), run by the worker that owns the kind. The
service signals it, starting it if needed, whenever a timeline entry is appended (*my call*: one place, and the
service's own entries are mirrored like the workers'). It runs one activity until the subject has no unmirrored
entry:

1. If the subject has no issue: open one in the kind's repo. Title as today. The body opens with a marker naming
   the subject (for a dataset, today's record marker), so a create that crashed before its write-back is found
   again and not repeated, for all three kinds. Then a line saying the issue is a read-only log, a link to the
   subject's page, and the description (for a dataset, the record facts `mia-data seed` writes today). Write the
   issue number back.
2. Post each unmirrored entry in `seq` order as a comment, once per entry (the existing once-per-marker comment).
   A buffer's `go` entry carries the spec it ran.
3. Set the stage label to the latest stage. Stage labels stay because they filter issue lists; they are output.
4. Write `mirrored_at` and the comment URL back.

The activity retries without limit with a capped backoff. A round never calls GitHub for a data or buffers issue
and never waits for the mirror. Appending an entry and signalling the mirror are two steps: a kick that fails is
logged, and the service kicks every subject that has an unmirrored entry when it starts, so an entry appended
just before a crash is not left behind. `mia-data seed` is retired: an issue appears the first time its dataset has
something to say.

Nothing reads these issues back. A label, a comment, an edit or a close by a person changes nothing; the issue body
says where to act. The human labels (`data:go` and the rest) are removed from `init-labels` and deleted from the
repos at cutover.

## 8. The page

Still one HTML file served by buffer-api, no build step, no token.

### 8.1 Actions

- **Datasets view.** Each row can be selected. *Prepare selected* files one preparation request for the
  selection (`POST /prep`), so the set is followed as one thing under one budget root. A row opens the dataset's
  own page (`#datasets/<record>`): state, timeline, and its buttons: *Prepare* or *Run again* (`go`), *Approve*,
  *Reject* with a feedback box, *Revoke*. *Approve* and *Reject* send the revision the page is showing (§5); on
  a `409` the page reloads the subject and says it changed. (*My call:* one dataset is a direct `go`, a
  selection is a request; both exist today as `data:go` and `prep:go`.)
- **Crops under review** are reached by a link from the dataset's page to the round's gallery on the data repo's
  gallery branch, where every verified set is already pushed before review (*my call*). The crops of a round in
  review sit in its workspace, which buffer-api does not serve; it serves published crops only. Showing them on
  the page needs a read route over the workspaces, which is the upgrade if a person still reviews often once the
  crop judge is active.
- **Buffers view.** *Create buffer* replaces the prefilled new-issue link: the current controls as spec YAML in
  an editable box, validated by the server, then one *Run* button with a tick box, *Show me the plan first*. A
  spec's page (`#specs/<name>`) lists its buffers and offers *Edit and run again*, *Approve*, *Revoke* (tick
  records of the pool), *Keep*, *Retire*,
  *Stop*, and *Files are in place* on a dataset that waits for a manual drop. A single buffer's page
  (`#buffers/<id>`) stays as it is.
- **Preparation view.** Requests with their records and where each stands; *Run again* and *Stop*.

Only the actions the subject's stage allows are offered, and the server refuses any other (§5). A subject with a
round in progress offers *Cancel* and nothing else that changes its state; *Force stop* sits behind a
confirmation on buffers and requests.

### 8.2 Needs you

A new first view: every subject waiting on a person, what it waits for, and the button that answers it, inline.
A dataset in review or needs-human; a buffer at its plan gate, its staged gate, idle, waiting for a drop, or
needs-human; a request that needs a human; an action that failed. Backed by `GET /needs`.

### 8.3 State, always

Every subject shows one status word, derived from its stage by one function: **working**, **waiting on you:
`<action>`**, **done**, **stuck: `<the reason the round wrote>`**. The page opens `GET /stream` and refetches the
part an event names, so a click is followed by its action, then each stage, without a reload. A lost stream
reconnects (the browser's `EventSource` does this) and refetches.

So that "working" is true: `GET /health` is read on load and on reconnect and a banner names a task queue no
worker polls; a subject's page shows its workflow's run status from Temporal beside the stage, so a failed or
terminated run does not read as working. *My call:* liveness is checked when someone looks, not pushed; a worker
that dies while a page sits open shows on the next load or action.

Timeline bodies are shown as their markdown source in preformatted text, inserted as text and never as HTML
(*my call*; a renderer is the upgrade). The buffer board and volumes keep their structured views.

### 8.4 From an issue to the page

Each mirrored issue links to its subject's page in the first line of its body. The top bar has one lookup box
that takes `#234`, a pasted issue URL, a record id or a spec name. Issue numbers repeat across the two repos, so a
bare number can match a dataset and a buffer spec: both are listed; a URL goes straight to its subject. Every
subject's page links to its issue once it exists.

## 9. What stays on GitHub

autodev's surface is GitHub and that does not change: its issues, its PRs, `/approve <head>` and `/revise`
comments, its own poll. A dataset round that needs a snippet still opens a snippet issue in the snippets repo and
still comments on the PR; those are required calls, not mirrored ones, since autodev cannot act without them.

mia-data keeps its scheduled pass over snippet PRs, with label delivery removed: it wakes the dataset workflow
whose PR got a new head, merged or went needs-human, and passes the `mia-data/crops` status on PRs that add no
snippet. This is the one GitHub poll left in the two pipelines. It carries no human command for a dataset or a
buffer; it reports what happened to an autodev PR a round is waiting on, and autodev stays unaware of who watches.
The snippet issue's marker carries the dataset's number, as it carries the issue number today.

Two things on this path touch the data repo and are settled so:

- The snippet issue's body links back to the data issue today. It links to the dataset's page instead (§4.1): a
  new dataset may have no issue yet when the snippet issue is opened.
- The PR report's images live on the data repo's gallery branch. That push stays, and stays a required call on
  the snippet path: it is output a PR reader needs, and nothing reads it back.

## 10. Trust boundaries

- **No identity.** Anyone who can reach buffer-api on the cluster network can take any action: start work,
  approve, revoke, retire, stop. The user accepted this for now. Budgets and the global pause (autonomous gates
  §8) still bound what a click can spend.
- **One function names the actor.** `actor_of(headers)` returns `page` today. An approval by click is an
  assessment `by: human:page`. Sign-in later changes this function and nothing else.
- **The worker token still guards state.** Registry rows, assessments, timeline entries and write-backs stay
  `worker` routes. An open action route can ask a workflow to do something; it cannot forge what the workflow
  found. A named actor (`prep:<number>`) is honoured only with the worker token.
- **Action routes take JSON only** and refuse a request whose `Origin` is not the service's own. Without this a
  web page open in a browser on the same network could submit a form to the service. This is not identity; it
  closes that one hole.
- **The service gains a Temporal client.** It can start, signal and terminate workflows in two namespaces; before,
  it held no credential but the worker token. It still holds no GitHub credential and deletes nothing.
- **Less untrusted input.** Issue bodies and comments of the data and buffers repos are no longer read, so the
  marker, fence, `/revoke` and feedback parsers and the write-access lookups go away. Spec and request text now
  arrives by `POST` and is validated by the same parsers as before; a reject note is neutralized and capped as
  `read_feedback` does today before it reaches an agent.

## 11. Cutover

1. `mia adopt-issues` (in a worker, since it needs GitHub) runs when slice A is deployed, not at the end: from
   slice B on, every running round needs a subject to write to. It can be run again at any time and changes
   nothing it already adopted. Every issue of the data repo with a record marker becomes a `dataset` subject,
   every buffer and request issue a `buffer` or `prep` subject with the spec text of its body; number and issue
   are both the issue number. The subject's stage is taken from the issue's stage label, so a round that is
   already waiting at a gate, and writes no further stage until someone answers, shows under *Needs you*.
   - A buffer's key is its spec's `name`, which is free text and not unique today. The lowest-numbered issue
     takes the name; a later issue with the same name gets `<name>-<number>`; a body without exactly one valid
     spec gets `issue-<number>`. From then on `POST /specs` keeps names unique.
   - Two issues naming one record: the one the dataset's registry row names is adopted. The other is reported and
     gets no subject; if it has a running workflow, the run is reported too and the operator stops it before
     slice B is deployed, since its next stage write would have nowhere to go.
2. Until slice E, an issue opened on GitHub after adoption is adopted when its first label is delivered (§6).
3. Rounds running when slice B is deployed continue: their number is their issue number, and their next activity
   writes the timeline. Their earlier comments are already on the issue; the mirror posts what comes after.
4. Slice E is two deploys. First the pollers stop delivering labels; a human label still sitting on an issue is
   listed by `adopt-issues` and the operator clicks the action on the page instead. The fallback of §6 is deleted
   only once no running round was started by a label event (its event id is not an action's): such a round can
   still reach `admit` and need the issue. Then the human labels are deleted from both repos and the mia-buffers
   poll schedule is removed.

Until the last slice ships, labels and the page both work, so cutover is the moment labels stop, not the moment
the page starts.

## 12. Changes to existing code

| file | change |
|---|---|
| `mia_buffers/ledger.py` | `subjects`, `actions`, `timeline` tables; number counters; notices |
| `mia_buffers/api.py` | the routes of §4.4; the `action` capability, `actor_of`, the JSON and Origin guard; the Temporal client; the stream |
| `mia_core/registry.py` | client methods for the worker routes and for sending an action as a worker |
| `mia_data/activities.py`, `mia_buffers/activities.py` | the table of §6 |
| `mia_data/poll.py` | label delivery removed; `deliver_label` deleted with its last caller |
| `mia_buffers/poll.py` | deleted |
| `mia_data/seed.py` | the issue renderer moves to the mirror; `seed`, the root marker and `trusted_bots` go |
| new: the mirror workflow and activity | in `mia_data` (mia-buffers imports it, as it imports `deliver_label` today) |
| `mia_data/config.py`, `mia_buffers/config.py` | `HUMAN_LABELS` and `PREP_HUMAN_LABELS` stay as the action vocabulary; the table of actions each stage accepts (§5); the `cancelled` stage; `init-labels` creates stage labels only |
| `mia_data/workflows.py`, `mia_buffers/workflows.py` | stage writes awaited behind a guard; the `cancel` signal (§6) |
| `mia_core/config.py` | buffer-api's view of `mia.yaml` keeps the two pipelines' namespaces and task queues |
| `mia_buffers/ui/index.html` | §8; the Docs tab's steps |
| `tests/test_buffers_ui.py` | every route the page calls exists and is `read`, `consumer` or `action`; no token; no stray host |
| `AGENTS.md` | the rules "the page is read-only" and "a buffer never asks mia-data; a `prep:go` issue does" are rewritten; a new rule: nothing reads a data or buffers issue |
| `docs/development.md`, `docs/deployment.md`, `docs/mia-data-workflow.md`, the runbook | labels out, page in; buffer-api needs to reach Temporal |
| `autodev/` | none |

`pixi run test` still needs no GitHub, LSF or Temporal server: the service's Temporal client is injected and faked
like the registry's transport.

Autonomous gates milestone P (a buffer files its own preparation request) files it through `POST /prep` with the
buffer's round id as the key, which replaces "found by marker on retry". Its way back, "`buffer:go` again if the
spec is unchanged", is a buffer `go` sent with the worker token and the spec hash the request was filed under; the
route runs the subject's stored spec when the hash still matches and otherwise appends an entry saying the spec
changed.

## 13. Calls made without the user (defaults in bold)

1. **Numbers, not string keys, inside the code** (§4.1); existing subjects keep their issue number.
2. **Numbers and action ids come from the clock**, so a ledger restored from a backup cannot reissue one (§4.1).
3. **Each run of a spec is a new buffer**; the spec's name is the long-lived key (§4.1); the naming rules for
   issues that share a name today (§11).
4. **Crops under review are a link to the gallery on GitHub**, not shown on the page (§8.1).
5. **Goal 5 is about issues.** A round still needs GitHub for the catalog, the snippets repo and the gallery (§2).
6. **Every stage write is awaited again**, so the stage the action check reads is the workflow's (§6). (This
   replaces an earlier call, "the workflow decides, the route checks shape only", which the user overruled.)
7. **The service kicks the mirror**, one mirror workflow per subject (§7).
8. **One dataset is a direct `go`; a selection is a preparation request** (§8.1).
9. **`stop` is today's "close the issue"**, unchanged in what it does, kept as the force stop (§5). Datasets get
   no `stop`: they have none today.
10. **`dropped` is refused while files are still being written** (§5).
11. **A round is charged to whoever asked for it**, not to whoever first opened the dataset's issue (§4.2).
12. **Liveness is checked on load**, not pushed (§8.3).
13. **Timeline bodies as markdown source** (§8.3).
14. **The keeper's over-quota notice lives on the Cache view** and is not mirrored (§6).
15. **JSON-only and same-origin on action routes** although identity is out of scope (§10).
16. **No command-line client** (§2).
17. **Labels keep working until the last slice** (§6, §11) rather than one switch-over.
18. **The snippet PR watch stays a poll** in mia-data (§9), as agreed when autodev signalling was rejected; listed
    here because "no polling" was stated without exception.
19. **A retry is the same action.** Keys, the `pending` state and a *Retry* button; no retry queue (§5).
20. **An approve or reject names the revision the person saw** and is refused if the subject moved on (§5).
21. **`stop` leaves running cluster jobs and a request's dataset rounds alone**, as closing the issue does today,
    and says so (§5).
22. **A request still learns that its datasets finished by asking the registry on a timer** (every
    `poll_minutes`, doubling to 30). Each dataset shows live on the page; only the request's own `done` can lag.
23. **What `cancel` does** (§5): it kills the round's cluster job; `cancelled` is a new final stage; cancelling a
    request cancels the dataset rounds it started and no others.
24. **What is refused mid-round that is taken today** (§5): a dataset `revoke` (queued until the next gate today)
    and a buffer `retire` (cancels the rebuild today) now need the round cancelled first. `dropped` is still
    taken mid-round.
25. **`buffer:go` and `buffer:plan` become one action** with a `plan_first` option and one button (§4.2).

## 14. Build order

Each slice is one PR and one plan under `docs/plans/`, stacked in this order on `autonomous-gates`. That branch is
not on the remote yet: it has to be pushed, or merged, before the first PR can be opened.

| # | slice | done when |
|---|---|---|
| A | **registry**: subjects, actions, timeline; read routes, `/lookup`, `/needs`, `/stream`; the `action` capability and guard; `adopt-issues` | tests pass; `adopt-issues` against the live repos adopts every issue once and reports duplicates; a restored backup issues no id twice; nothing behaves differently |
| B | **timeline and mirror**: the stage and comment activities of §6 write entries; `MiaMirror` posts them; rounds stop calling GitHub for data and buffers issues | replay fixtures pass, with the one guarded change §6 names (stage writes awaited); a test round completes while every issue call of a GitHub fake fails, and the issue is complete once the fake recovers; a live round's issue reads as before |
| C | **datasets from the page**: the Temporal client, dataset actions, `admit` and `read_feedback` from the registry, the dataset page, live updates, `/health` | live: *Prepare* on a dataset with no issue shows it working within two seconds, its issue appears with the link back, and a review is approved by click and publishes; in tests, a repeated key starts one round and an approve carrying an old revision is refused; live: *Prepare* on a dataset whose round is running is refused, *Cancel* ends the round and its cluster job is gone from the queue |
| D | **buffers and requests from the page**: specs, buffer and prep actions, `stop`, `dropped`, `nudge` through the registry, *Prepare selected*, *Needs you* | live: a buffer created on the page reaches ready with no GitHub input; a request for several records starts them with no `data:go` label |
| E | **GitHub input removed**: the buffers poller and schedule, label delivery, `trusted_bots`, markers, parsers, `seed`, human labels, the fallback of §6; docs and `AGENTS.md` | no read of a data or buffers issue remains outside the mirror and `adopt-issues`; labelling, commenting on or closing an issue changes nothing |
