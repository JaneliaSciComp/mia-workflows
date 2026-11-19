# MIA: one Overview, one "needs human", the repair agent — Spec

**Status:** v0.4 · v0.3 approved 2026-11-19; what v0.4 adds (§10 calls 11 to 14, from the review of slices 1
and 2) is the writer's and the user's to change · §2 holds the user's decisions of 2026-11-19; §10 lists the calls
the writer made, which the user approved with the spec. Plan for slices 1 and 2:
`docs/plans/2026-11-19-overview-status.md`. Rests on the stage tables and action routes of the webapp-actions
spec, on the shelved-subjects spec, on the cleaner spec (whose actor this spec renames) and on the
judge-decides spec.

## 1. Intent

The page asks for a person's attention in six wordings and from two lists that disagree. This spec leaves one
status that means "a human needs to look at this", one page that shows the whole system, and one name for the
automation that repairs what stopped.

What is wrong today:

- **Six wordings.** The tab "Needs you", the pills "needs you" and "waits for you", the stage `needs-human`,
  the stage `review`, the dataset row's state `needs-human` ("whenever a person has to act") and a buffer
  version's event `needs-human`.
- **Two lists.** The tab "Needs you" lists subjects by their stage. The Cleaner's "Needs you" card lists the
  cleaner's cases in the state `needs-person`. Such a case never ends: on 2026-11-19 it listed 172 cases about
  122 subjects, 94 of them datasets that were `done`. Seven subjects that waited for a verdict or an approval
  were on the tab and not on the card.
- **A stopped subject reads "needs you" while the cleaner is repairing it.**
- **"Cleaner"** names a janitor, and the automation is more than that.

**Goals**

- `needs human` is one status of a subject, computed in one place, and nothing else in the code or on the page
  says that a human is needed.
- One landing page, Overview, shows the whole system; each of its sections folds.
- The actor is the **repair agent** everywhere a person or a reader of the code meets it.

**Non-goals**

- No change to what the repair agent does to a subject, to its rules or to its limits.
- No change to which actions a stage accepts.
- autodev's `needs-human` label on a snippets issue stays. It is autodev's own status on GitHub and means the
  same there: a human needs to look.

**Anti-goals**

- A word renamed and its concept kept twice. Where two things carry one name today, one of them goes or gets
  the name of what it is.
- A subject listed as needing a human while automation still has a move.
- A subject that stopped with no person having decided, and that is on no list.

## 2. Decisions (the user's, 2026-11-19)

1. The tabs "Needs you" and "Cleaner" go. A new **Overview** is the landing page. It is built on the Cleaner
   page and covers the whole system. Each section can be collapsed.
2. The actor is called the **Repair agent**.
3. One status means a human needs to look: **needs human**. The name says that automation is the default.
4. A subject **in repair** does not need a human. **Anything stopped and unshelved that is not done needs a
   human.**
5. The subject's status is the only thing that encodes "needs human". The repair agent keeps only the result
   of its repair, and that result is merged with the subject's state when the subject is shown.

## 3. Three words, three concepts

| Word | What it says | Where it lives |
|---|---|---|
| **stage** | where the subject is in its lifecycle | the stage tables (`STAGES`), the registry row |
| **phase** | what a round does at a stage | the stage tables (`STAGE_ACTIONS`), beside the actions |
| **status** | who moves the subject next | computed by buffer-api for each subject view; nowhere stored |

### 3.1 Phase

The word the stage tables hold beside a stage's actions (today called the "status word"). Five values:

| Phase | Meaning |
|---|---|
| `not started` | no round has run |
| `working` | a round is in progress and automation has the next move |
| `waiting` | a round is open and waits for an answer only a person gives |
| `stopped` | no round is open and the subject is not at its lifecycle's end (today `stuck`) |
| `done` | the lifecycle's end, or a round a person ended (`cancelled`, `retired`) |

The subject view carries the phase beside the status. Every worker decision that asks whether a round is open,
waiting or stopped reads the phase: the repair agent's sweep (which subjects it looks at, whether a subject
moved on, whether a remedy helped), its rules, and what a request sends a dataset. None of them reads the
status, so none of them changes when a case changes what a person is shown. Whether a subject is shelved they
read from its hold.

The page shows the status. It reads the phase only where it asks whether a round is in progress (the run's
state beside a round, the cancel of a round).

### 3.2 Status

What a person reads, and the only field that says a human is needed. buffer-api computes it in one function
(`_rules`) from the subject's stage, its hold and the repair agent's case about it (§4). Six values, the first
row that holds:

| Status | Holds when |
|---|---|
| `shelved` | a person set the subject aside |
| `in repair` | the repair agent is in `act` mode and has a case in work about the subject, as the table below says |
| `needs human` | the repair agent gave up on the subject and it has not moved since (§4), whatever its phase |
| `done` | phase `done` |
| `not started` | phase `not started` |
| `working` | phase `working`; also a dataset at `review` whose set the crop judge is still looking at (§3.5) |
| `needs human` | everything else: phase `stopped` or `waiting` |

When a case in work makes its subject `in repair`:

| Case state | The subject is `in repair` when its phase is |
|---|---|
| `working` (the agent has begun and has steps left) | any. A stage the agent's own step produced, such as `cancelled` before its resume, is part of the repair. |
| `open` (found, no step taken yet) | `working`, `waiting` or `stopped` |
| `waiting` (the remedy is sent; the agent watches) | `waiting` or `stopped`: the subject has not answered the remedy yet |

So a round the agent cancelled and could not resume needs a human, a round the agent is still working on is in
repair, and a round a person cancelled is `done`: no case stands behind it.

A subject that just stopped reads `needs human` until the repair agent's next sweep opens a case (minutes),
then `in repair`. A subject a person acted on within `hands_off_hours` gets no case and stays `needs human`.

### 3.3 Stages and a buffer version's events

- The stage `needs-human` of a buffer and of a preparation request is renamed **`stopped`**. It is where a
  round rests when it stopped; whether a human is needed is the status's to say.
- The stage `needs-human` of a dataset is removed. Nothing writes it: a dataset's round stops at `failed` or
  `blocked`.
- A buffer version's event `needs-human` is renamed **`stopped`**. A consumer that draws from such a version
  is answered `409 buffer stopped`.
- `review`, `plan-review`, `staged`, `failed`, `blocked` keep their names. Beside the status `needs human` the
  stage says why: a verdict is wanted, an approval is wanted, a step failed, a decision or a record change is
  needed.

**The hand-over.** One `mia restart` carries the rename: buffer-api starts first, on the new code, and the
workers after it.

- **Stored rows** are renamed when buffer-api starts: a buffer's or a request's stage `needs-human` and a
  version's event `needs-human` become `stopped`; a dataset subject's stage `needs-human`, and a dataset row
  that holds it as its stage, become `failed`. The rename can run any number of times.
- **Writes that still say the old word.** An activity that a run scheduled before the restart is carried out
  after it with the input it was scheduled with. buffer-api therefore takes `needs-human` on every route that
  writes a stage or an event and stores the new word. It never returns the old word.
- **Recorded histories.** A run that began before the restart is replayed by the new code. The commands the
  new code issues differ from the recorded ones only in an activity's input, which a replay does not compare.
  What a replay does feed back is recorded results and state carried across continue-as-new: wherever such a
  value holds a stage, the workflow reads `needs-human` as the new word before it compares or stores it.
- Both readings stay for as long as a run that began before the restart can be open.
- **Tests.** For each kind of subject: a replay of a history recorded before the rename that passes through the
  old word, and a round whose stage write is retried across the restart.

Timeline entries stay as they were written.

### 3.4 A dataset's row

The dataset row's state (`datasets.state`) says today "whenever a person has to act": a second encoding of
needs human, computed by mia-data from the stage alone. It becomes a fact about the data and nothing more:

| Row state | Meaning |
|---|---|
| `in-progress` | a round is open: working, or waiting at `review` |
| `prepared` | a round ended `done` with its crops saved |
| `stopped` | a round ended and the dataset is not prepared (today `needs-human`) |
| `revoked` | its saved crops were withdrawn |

Whoever asks whether a dataset needs a human reads its subject's status:

- **A preparation request** settles a dataset as needing a human when the dataset's status is `needs human`,
  and follows it while it is `working` or `in repair`. What it records for that dataset is the status word.
- **The Datasets list**, its filter, the buffer's Datasets step and the counts show `prepared`, `revoked` and
  `unprepared` as facts, and for every other dataset its subject's status.

Stored rows and the settled words a request carries are handed over as in §3.3.

### 3.5 A set the judge is looking at

A dataset reaches `review` before the crop judge has looked. While an active judge still owes its verdict on
the set under review, automation has the next move, so the subject reads `working`. A judge in shadow mode
decides nothing, so it sets nothing.

The round says so in the subject's data: `sets.judging` is the id of the set the judge owes a verdict on, or
nothing. The subject reads `working` only at `review` and only while `sets.judging` is the set under review.
The round writes it whenever it changes:

- set when the round has the judge look at a set, also on a resume that has it look;
- cleared when the judge's look ends in any way (approved, sent back, left to a person, failed, cancelled);
- cleared when a person's verdict on that set is taken, although the judge's job may still run;
- of no effect once the stage leaves `review`, whatever it holds.

The round's state is the workflow's, so a worker restart loses none of it. The judge's look is bounded by its
job's time limit, so the flag cannot stay set by a job that hangs.

## 4. The repair agent's cases

A case is the record of one repair: what was found, the steps taken, and how it ended.

| Case state | Meaning |
|---|---|
| `open`, `working`, `waiting` | in work (unchanged) |
| `resolved` | the repair worked, or the subject moved on by itself (unchanged) |
| `dropped` | somebody acted on the subject, or it is gone (unchanged) |
| `gave-up` | the repair agent could not repair it; the case says what it takes (today `needs-person`) |

- `gave-up` is an ending, like `resolved`. Stored cases are renamed when buffer-api starts.
- The repair agent still reads its `gave-up` cases where it does today, at any age: a subject it gave up on is
  not looked at again as it stands, a record-gap case is resolved when the record changes, and a request is
  not resumed while a dataset it names has one.
- buffer-api puts the repair result on the subject view as `repair`: the subject's newest case when it is in
  work, or when it is `gave-up` and the subject has not moved since the case ended. A case about an earlier
  stop is history and is not on the view. The status reads the same `repair` (§3.2).
- "Has not moved" is read from the subject's revision. When a case ends, the agent records on it the subject's
  revision as it stands after the agent's own last write. The result applies while the subject still has that
  revision: a stage write, an action taken or a shelve since then makes it history. A case that ended before
  it could record one applies while its last update is not older than the subject's.
- The timeline entry the agent writes when it gives up says "The repair agent could not repair this: <what it
  takes>".
- A `gave-up` case with no subject (a run, a job, a snippets issue or pull request left behind) is a problem
  with the installation. Overview lists it under System, and the sweep resolves it once the thing is gone, as
  it does a case in work.

## 5. Overview

The landing page. The tabs read Overview, Datasets, Buffers, Cache, Decisions, Docs.

Each section is a fold with its title and a count in the heading. A section stays as the viewer left it (kept
in the browser).

| Section | Holds |
|---|---|
| **System** | Whether every service is up, with each problem the last sweep found, when it was last checked, the repair agent's mode, and left-behind things the agent could not remove. Subjects behind a problem are left alone until it clears. |
| **Counts** | Datasets, buffers and requests by status. A count of `needs human`, `in repair`, `working` or `shelved` opens that section narrowed to the kind; any other opens the kind's list. |
| **Needs human** | Subjects with the status `needs human`: what, name, stage, since, why, and the buttons that answer it. Why is the repair result when the agent gave up (it opens the case), else the stage's sentence. Narrowed by kind. Also the actions of the last day that were not delivered, with Retry, and a buffer's datasets that wait for files to be copied in. |
| **In repair** | Subjects with the status `in repair`, and left-behind things the agent is removing. Each opens its case: what was found, each step and its result. |
| **Working** | Subjects with the status `working`: what, name, stage, since. |
| **Repairs in the last week** | Cases that ended: repaired, dropped, gave up. |
| **Shelved** | Subjects a person set aside, with the reason and Unshelve. |

Everywhere else on the page a subject's status is shown with the same six words. The Buffers list keeps
`serving` for a buffer that serves.

One read route gives the subjects of every kind with a given status, paged, with the counts per kind and
status. `GET /needs` becomes that route's `needs human` case.

## 6. The name

The actor is the **repair agent** on the page, in timeline entries it writes from now on, in the guide, the
architecture page, `docs/development.md` and `AGENTS.md`.

In the code the concept is `repair`: the rules module, the workflow, the report route, the config block, the
work directory of its diagnosing agent, and the actor id it sends with an action. Stored actions by `cleaner`
are renamed when buffer-api starts; timeline entries stay as written. A run that recorded the actor `cleaner`
reads it as the same actor. The operator's `mia.yaml` renames its `cleaner:` block to `repair:`.

**The workflow's hand-over.** A sweep of the old workflow type may be open when the workers stop, and its
activity would be carried out by the new worker beside the new sweep. So before the buffers worker creates the
new schedule it deletes the old one and terminates every open run of the old type; only then does it create
the new schedule. Each of the three can be done again, so a start that was interrupted finishes the hand-over
at the next start. A sweep that was terminated lost nothing: every step of a case is sent at most once per
key, and the next sweep reads the cases as they are.

## 7. What a person sees change

- One list of what needs a human, on Overview. It no longer lists a subject the repair agent is working on, a
  dataset the judge is looking at, or a dataset that is `done`. It newly lists a subject the agent gave up on
  where its own cancel or stop left it.
- A stopped buffer or request shows the stage `stopped` and the status `needs human` or `in repair`.
- The pills "needs you" and "waits for you" read `needs human`. The Datasets list reads `needs human`, `in
  repair` and `working` where it read `needs-human` and `in-progress`.
- A request keeps following a dataset that is in repair or that the judge is looking at, and rests once every
  dataset is prepared, shelved or needs a human.
- "Cleaner" reads "Repair agent".

## 8. Build order

Each slice leaves the system working and is merged with a restart. Slices 1 and 2 are planned and built
first; each later slice gets its own plan.

1. **Status and cases.** Phase on the view and status as in §3.1 and §3.2, with every worker reader moved to
   the phase; `gave-up` cases, the revision a case ends at and `repair` on the view (§4); the read route of §5.
   The page's pills use the six words. Tests cover a case that is open, working, waiting, retried and given up,
   and the agent in `report` mode.
2. **Overview.** The page of §5; the two tabs go; texts say "repair agent".
3. **A dataset's row** (§3.4).
4. **Stages and events.** `needs-human` to `stopped`, the dataset stage removed, the hand-over (§3.3).
5. **The judge's look** (§3.5).
6. **The name in the code** (§6).

Until slice 4 is live the page shows the stage `needs-human` under its present name.

## 9. What stays out

- autodev's stages and labels.
- The cache page, spend and the decisions page: they keep their tabs and are not summarised on Overview.
- Any mapping of the addresses `#needs` and `#cleaner`: they are addresses the page does not know.

## 10. Calls that are the writer's

1. **A round a person cancelled does not need a human.** `cancelled` and `retired` keep the phase `done`: a
   person looked and decided. A round the repair agent cancelled is in repair, and needs a human if the agent
   gives up there (§3.2). Giving `cancelled` the phase `stopped` instead would have the repair agent open
   cases on rounds a person cancelled. Cost if wrong: a dataset a person cancelled and meant to pick up again
   is on no list but Datasets.
2. **An `idle` buffer needs a human**, as today: it asks for Keep, though the keeper retires it by itself.
3. **A dataset the judge is looking at is `working`** (§3.5). It costs a registry write when the judge begins
   and one when it ends. Without it every dataset passes through Needs human for the minutes of the judge's
   look.
4. **The dataset stage `needs-human` is removed**, not renamed, because nothing writes it; a stored one
   becomes `failed`.
5. **A subject that just stopped reads `needs human` until the next sweep.** The alternative, reading `in
   repair` on the expectation of a case, would say so of subjects the agent then leaves alone.
6. **In `report` or `off` mode nothing is `in repair`**: a case nobody works on repairs nothing.
7. **The dataset row stops saying who has to act** (§3.4), so a request and the Datasets list read the status.
   Cost: the Datasets list computes a status per dataset that has a subject. The cheaper alternative renames
   the row's `needs-human` to `stopped` and shows a dataset awaiting a verdict as `stopped` there.
8. **A buffer version's event is renamed and consumers read `409 buffer stopped`** (§3.3). Cost: training
   code that matches the old text no longer does.
9. **The rename reaches the code and the config** (§6). Cost: one edit to the operator's `mia.yaml` at the
   restart of slice 6. The cheaper alternative keeps `cleaner` in code and wire names and leaves one concept
   with two names.
10. **A left-behind thing the agent could not remove is a System problem**, and its case ends when the thing
    is gone (§4). That is the one change to what the sweep does.

11. **The agent acts only while its sweeps report.** `in repair` needs the last report to be in `act` mode and
    not overdue by three of its periods: with the schedule off or the worker away nothing is repaired.
12. **A case that gives up before its own last action is answered stays in work until it is**, so that it ends
    at the revision the answer leaves (a cancel's stage write comes seconds after the cancel).
13. **A round the agent ended and runs nothing after ends its case `gave-up`**, not `resolved`: the subject
    needs a human, with what the diagnosis asked of one.
14. **Overview follows the stream at most once in ten seconds**, and each health check redraws it. On the live
    installation one list of subjects takes about 0.4 s and the cases 0.2 to 0.5 s.

## 11. Left to the builder

- How buffer-api reads the cases once per request for many subject views.
- The read route's name and query (§5), and whether Counts comes from it or from the lists' facets.
- Which recorded results and carried state hold a stage, an event or a settled word (§3.3, §3.4), and the
  replay fixtures.
- What the client raises for `409 buffer stopped`.
- How the fold remembers its state.
- The order of work inside slice 6.
