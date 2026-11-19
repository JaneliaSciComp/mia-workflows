# MIA: one Overview, one "needs human", the repair agent — Spec

**Status:** v0.1 draft, not approved · §2 holds the user's decisions of 2026-11-19; §9 lists the calls that are
the writer's and the user's to change. Rests on the stage tables and action routes of the webapp-actions spec,
on the shelved-subjects spec, on the cleaner spec (whose actor this spec renames) and on the judge-decides
spec.

## 1. Intent

The page asks for a person's attention in five wordings and from two lists that disagree. This spec leaves one
status that means "a human needs to look at this", one page that shows the whole system, and one name for the
automation that repairs what stopped.

What is wrong today:

- **Five wordings.** The tab "Needs you", the pills "needs you" and "waits for you", the stage `needs-human`
  and the stage `review` all ask for attention, and the dataset's preparation row has a state `needs-human` of
  its own.
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

## 2. Decisions (the user's, 2026-11-19)

1. The tabs "Needs you" and "Cleaner" go. A new **Overview** is the landing page. It is built on the Cleaner
   page and covers the whole system. Each section can be collapsed.
2. The actor is called the **Repair agent**.
3. One status means a human needs to look: **needs human**. The name says that automation is the default.
4. A subject **in repair** does not need a human. **Anything stopped and unshelved that is not done needs a
   human.**
5. The subject's status is the only thing that encodes "needs human". The repair agent keeps only the result
   of its repair, and the page merges that result with the subject's status when it shows the subject.

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

Workers that need to know whether a round is open or stopped read the phase from the stage tables, by the
subject's stage. The phase is not sent to the page as a second status.

### 3.2 Status

What a person reads, and the only field that says a human is needed. buffer-api computes it in one function
(`_rules`) from the subject's stage, its hold and the repair agent's case about it. Six values, the first that
holds:

| Status | Holds when |
|---|---|
| `shelved` | a person set the subject aside |
| `done` | phase `done` |
| `not started` | phase `not started` |
| `in repair` | the repair agent is in `act` mode and has a case in work about the subject (§4), and the phase is `stopped` or `waiting`, or it is `working` and the case has not sent its remedy yet |
| `working` | phase `working`; also a dataset at `review` whose set the crop judge is still looking at (§3.4) |
| `needs human` | everything else: phase `stopped` or `waiting`, unshelved, not in repair |

A subject that just stopped reads `needs human` until the repair agent's next sweep opens a case (minutes),
then `in repair`. A subject a person acted on within `hands_off_hours` gets no case and stays `needs human`.

### 3.3 Stages

- The stage `needs-human` of a buffer and of a preparation request is renamed **`stopped`**. It is where a
  round rests when it stopped; whether a human is needed is the status's to say.
- The stage `needs-human` of a dataset is removed. Nothing writes it: a dataset's round stops at `failed` or
  `blocked`.
- The dataset's preparation row state `needs-human` (`DATASET_STATES`, what a request settles a dataset as) is
  renamed **`stopped`**.
- `review`, `plan-review`, `staged`, `failed`, `blocked` keep their names. Beside the status `needs human` the
  stage says why: a verdict is wanted, an approval is wanted, a step failed, a decision or a record change is
  needed.

Stored rows are renamed once, when buffer-api starts (`subjects.stage`, `datasets.state`). A workflow run that
carries the old word in its state reads it as the new one. Timeline entries stay as they were written.

### 3.4 A set the judge is looking at

A dataset reaches `review` before the crop judge has looked. Until the judge has approved the set, sent it
back or left it to a person, automation has the next move, so the subject reads `working`. The round says so
in the subject's data (`sets.judging`), written with the stage and cleared when the judge ends.

## 4. The repair agent's cases

A case is the record of one repair: what was found, the steps taken, and how it ended. It says nothing about
whether a human is needed.

| Case state | Meaning |
|---|---|
| `open`, `working`, `waiting` | in work (unchanged) |
| `resolved` | the repair worked, or the subject moved on by itself (unchanged) |
| `dropped` | somebody acted on the subject, or it is gone (unchanged) |
| `gave-up` | the repair agent could not repair it; the case says what it takes (today `needs-person`) |

- `gave-up` is an ending, like `resolved`. Stored cases are renamed once, when buffer-api starts.
- The repair agent still reads its `gave-up` cases where it does today: a record-gap case is resolved when the
  record changes, and a request is not resumed while a dataset it names has one.
- buffer-api puts the repair result on the subject view as `repair`: the subject's newest case when it is in
  work, or when it is `gave-up` and the subject has not moved since the case ended. A case about an earlier
  stop is history and is not on the view.
- The timeline entry the agent writes when it gives up says "The repair agent could not repair this: <what it
  takes>".
- A `gave-up` case with no subject (a run, a job, a snippets issue or pull request left behind) is a problem
  with the installation. Overview lists it under System.

## 5. Overview

The landing page. The tabs read Overview, Datasets, Buffers, Cache, Decisions, Docs. `#needs` and `#cleaner`
open Overview.

Each section is a fold with its title and a count in the heading. A section stays as the viewer left it (kept
in the browser).

| Section | Holds |
|---|---|
| **System** | Whether every service is up, with each problem the last sweep found, when it was last checked, the repair agent's mode, and left-behind things the agent could not remove. Subjects behind a problem are left alone until it clears. |
| **Counts** | Datasets, buffers and requests by status. Each count opens the list it counts. |
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
are renamed once; timeline entries stay as written. The operator's `mia.yaml` renames its `cleaner:` block to
`repair:`. The schedule of the old workflow type is replaced by the new one's when the workers start.

## 7. What a person sees change

- One list of what needs a human, on Overview. It no longer lists a subject the repair agent is working on, a
  dataset the judge is looking at, or a dataset that is `done`.
- A stopped buffer or request shows the stage `stopped` and the status `needs human` or `in repair`.
- The pills "needs you" and "waits for you" read `needs human`.
- "Cleaner" reads "Repair agent".

## 8. Build order

Each slice leaves the system working and is merged with a restart.

1. **Status and cases.** Phase and status as in §3.1 and §3.2, `gave-up` cases and `repair` on the view (§4),
   the read route of §5. The page's pills use the six words.
2. **Overview.** The page of §5; the two tabs go; texts say "repair agent".
3. **Stages.** `needs-human` to `stopped` for buffers, requests and the dataset row; the dataset stage removed
   (§3.3).
4. **The judge's look** (§3.4).
5. **The name in the code** (§6).

Until slice 3 is live the page shows the stage `needs-human` under its present name.

## 9. Calls that are the writer's

1. **A round a person cancelled does not need a human.** `cancelled` and `retired` keep the phase `done`: a
   person looked and decided. The repair agent's own cancel is a step of a case, so that subject is `in
   repair`. Cost if wrong: a cancelled dataset that should be picked up again is on no list but Datasets.
2. **An `idle` buffer needs a human**, as today: it asks for Keep, though the keeper retires it by itself.
3. **A dataset the judge is looking at is `working`** (§3.4). It costs one registry write when the judge ends.
   Without it every dataset passes through Needs human for the minutes of the judge's look.
4. **The dataset stage `needs-human` is removed**, not renamed, because nothing writes it.
5. **A subject that just stopped reads `needs human` until the next sweep.** The alternative, reading `in
   repair` on the expectation of a case, would say so of subjects the agent then leaves alone.
6. **In `report` or `off` mode nothing is `in repair`**: a case nobody works on repairs nothing.
7. **The rename reaches the code and the config** (§6). Cost: one edit to the operator's `mia.yaml` at the
   restart of slice 5, and the running `MiaCleaner` schedule is replaced. The cheaper alternative keeps
   `cleaner` in code and wire names and leaves one concept with two names.
8. **autodev's label stays** (§1, non-goals).
9. **A left-behind thing the agent could not remove is a System problem**, since it has no subject to carry a
   status.
10. **Old addresses** `#needs` and `#cleaner` open Overview; no redirect is kept for `#needs/shelved`.

## 10. Left to the builder

- How buffer-api reads the cases once per request for many subject views.
- How "the subject has not moved since the case ended" is read from the row and the case (§4).
- The read route's name and query (§5), and whether Counts comes from it or from the lists' facets.
- Where a workflow run normalises a stage word carried from an earlier run (§3.3), and the replay fixtures.
- How the fold remembers its state.
- The order of work inside slice 5, and the hand-over from the old schedule to the new one.
