# MIA: resume or restart — Spec

**Status:** v0.3 · approved 2026-11-19 · nothing built. Rests on the stage tables and action routes
of the webapp-actions spec, on the round's pins (`Pins`, `BufferPins`), on the crop judge of the judge-decides
spec and on the cleaner spec. §2 and §3 are the user's decisions of 2026-11-18; §9 lists the calls that are
the writer's and still the user's to change. It replaces the ruling of 2026-11-14 that running a dataset again
is always from scratch.

## 1. Intent

Every pipeline here (a dataset's preparation, a preparation request, a buffer round) can be run again, and
whoever runs it again chooses how, as with Nextflow's `-resume`:

- **Resume** keeps every result the round already has and runs only what has no result.
- **Restart** redoes all the processing, from scratch.

Today each of the three rounds begins by wiping its state and pinning anew, so running anything again repeats
work that had finished: a set the judge was never shown is made again to get it judged, crops a person approved
are thrown away because their reader's pull request merged a minute after the round gave up, and a crop job
that died on a full queue costs a second match agent.

**Goals**

- Resume never repeats a step that finished, and never drops a set of crops.
- Restart stays exactly what running again is today.
- Both are offered wherever running again is offered: a subject's page, the bulk boxes, a request's text, the
  cleaner.
- What Resume kept and what it ran is on the timeline.

**Non-goals**

- Invalidation. A result is not checked against anything that changed since (a setting, a model, a record).
  §8 says where that check would go.
- Choosing the step to start from. Resume starts at the first step with no result; a step that finished is run
  again only by Restart.
- A result store beside the workflows, and Temporal's reset. §7 says why not.

**Anti-goals**

- A Resume that runs a match agent, asks autodev for a reader, or makes crops that the round already has.
- A Resume that moves a round off its pins.
- Any change to what a Restart does to a dataset.

## 2. The rule

A step **finished** when it ended with a result the next step can use. Resume reuses it whatever has changed
since. A step that failed, was refused (the budget, the pause, a host), was cancelled, was skipped, or whose
job a dead worker took away has no result, and Resume runs it.

A round's results live in its workflow's state. A force stop ends the workflow and the state with it, so a
Resume after one finds nothing to keep and runs as a Restart, and its first entry says so (§9, call 2).

**Resume keeps the round.** The round id, the seed, the pins (record commit, snippets commit, harness commit,
environment) and the work folders stay. So a step that failed because of a fault in the pinned harness or
reader fails again on Resume: a fix is picked up by Restart, which pins anew.

Settings are read when a step runs, as they are today (`limits`, the judge's mode). A changed setting changes
the steps Resume runs; it invalidates no result.

## 3. The two actions

| | Action | A dataset's workflow hears | Where it is accepted |
|---|---|---|---|
| Resume | `resume` (new) | `restart` signal, event kind `resume` | §4.3, §5, §6 |
| Restart | `go` | `restart` signal, event kind `go` | as today |

`resume` goes into each `STAGE_ACTIONS` table and through the action routes like any action: its revision, its
key, its actor. It is a start action: from the page it carries the hosts a person approved, and a request's or
the cleaner's carries the root. `_rules` withholds it wherever it withholds `go` (a record that cannot be run).

A dataset at `new` has nothing to resume: its one button stays **Start** (`go`).

## 4. A dataset round

### 4.1 Steps and their results

| Step | Its result, on `DatasetState` | No result when |
|---|---|---|
| admit | `pins` | admission failed or refused |
| match | `binding` (a reader fits), or `snippet_issue` (none fits and the developer was asked) | the match job failed, was refused or cancelled; the agent said `failed` or `blocked` |
| developer | the pull request's current head, or its merge | autodev stopped (`snippets:needs-human`), or was quiet past the limit |
| crops | a passing `Test` for the binding, the head or the merge commit | the jobs failed or were refused; fewer crops verified than needed |
| judge | `Test.evidence` | the judge was skipped (budget, pause) or its job did not run to the end |
| verdict | `Test.approved_at` | it waits for the judge or a person |
| merge | the merge commit | autodev's merge was refused |
| save | `saved`, stage `done` | saving failed |

A match that ended `failed` or `blocked` did not finish, although its agent ran to the end: it produced
nothing the next step can use.

### 4.2 What Resume does

`_resume(event)` beside `_round(event)`. It does not call `_begin_round`: nothing of the state is cleared. It
re-reads the limits and the pause, charges the round to whoever asked for the resume (`Pins.root` is replaced;
nothing else of the pins), counts as a person's or a sender's action exactly as a `go` does (`debugs`,
`reworks`, `handled`), and closes a pass the stopped round left open. Then the first line that applies:

1. **No pins.** The round has no result: it runs as a Restart does.
2. **A snippets issue is followed** (`snippet_issue`). The round looks at the pull request as it is now and acts
   on it as a round that never stopped would (`_on_pr`, `_follow`): a merge saves the approved head's crops
   under the merge commit, or makes the merge commit's own; an untested head is tested; a tested, passing,
   current head waits at `review`. If autodev has stopped
   and the pull request is open (or there is none yet), the round asks autodev again as the cleaner's
   `snippets_go` step does, and follows. A pull request closed unmerged leaves the round with no reader: line 6.
   A set this leaves at `review` with no verdict (a head's, or the merge commit's own) is line 4's.
3. **A passing set, approved, not saved** (a matched binding's). It is saved.
4. **A passing set with no verdict.** The judge looks if it has not (`evidence` empty and the judge is on); the
   round goes on from the judge's verdict as any round does, or waits at `review`.
5. **A binding and no passing set.** Crops, then on as any round.
6. **Otherwise** (pins, no binding, no snippets issue). Match, then on as any round.

When no line runs anything (a set at `review` the judge has looked at) the resume is answered with one
timeline entry, "nothing is left to run: the crops wait for a verdict", and the stage stays.

From its first step on, a resumed round is a round: it fails, is blocked, is debugged and is judged like one.
The sets that outlive a round (`held`, `saved`) are untouched.

### 4.3 Where it is accepted

`resume` at `failed`, `blocked`, `needs-human`, `cancelled` and `review`. Not at `new` (Start), not at `done`
(nothing is missing), not at `debugging` (the debugger's own run follows; **Restart now** stays), not while a
round works.

Not after a revoke. A dataset whose saved crops a person revoked rests at `blocked` with the approved set still
on its state, and saving that set again would undo the revoke with no new verdict. `_rules` withholds `resume`
from it, the workflow refuses one that arrives all the same, and Restart repairs it as today.

At `review`, `go` keeps its meaning (the set is dropped and the round starts over). Resume there is the way to
get a set judged that the judge was never shown.

### 4.4 A step that runs again is a new attempt

Jobs are submitted at most once per key, and the key, the workspace and the job record are named by round,
kind and attempt, with the attempt fixed at 1. A step Resume runs again under the same round takes the next
attempt number, so it gets its own workspace, key, reservation and record, and the earlier attempt's record
stays linked from its own entry. A set of crops made again at the same commit is a new `Test` with its own id;
a set id never names two sets.

### 4.5 The debugger

Unchanged. Its run after a failed round stays a `go`: its advice is read by the match agent, and a resumed
round whose match finished would not read it.

## 5. A preparation request

The request's text says `restart: true` for a Restart; without it the request resumes. (`restart` is the
`again` flag under the name the page uses; a stored request that says `again` reads the same.)

| The dataset is | Resume sends | Restart sends |
|---|---|---|
| never started | `go` | `go` |
| stopped (`failed`, `blocked`, `needs-human`, `cancelled`) | `resume` | `go` |
| waiting at `review` | `resume` | `go` |
| prepared | nothing | `go` |
| working, shelved, or waiting for a host nobody approved | nothing | nothing |

A request as a subject takes both actions where it takes `go` today: `resume` runs its round as a Resume and
`go` as a Restart, whatever its text said for its first round.

## 6. A buffer round

A buffer's `go` plans a new version over the datasets that are prepared now, pins anew, and stages each
pyramid whose recipe has no artifact yet. That stays the buffer's Restart (§9, call 1).

`resume` is new, accepted at `staged` and at `needs-human`: the round keeps its id (the buffer's id), its
pins, its plan and the approval of that plan. Every dataset that is `servable` stays registered; every other
dataset of the plan (failed, or dropped at the timeout) is sourced again, and the round's timeout starts over.
At `staged`, where a round with failures rests when the gate is on, it is refused while a late dataset still
has a job running, as a `go` is, and the round comes back to the gate when nothing runs. A round that stopped
before its plan was approved has nothing to keep and resumes as a Restart. A cancelled or retired round's
buffer is gone: it takes `go` only.

## 7. Approaches

1. **The round keeps its state and skips what it has** (chosen). Every result is already on the round's state;
   Resume is the round entered without the wipe. The paths that take a round over after a rollover (`run`:
   `_from_review`, `_follow`) are the model. No recorded history holds a `resume`, so no path of an in-flight
   run changes.
2. **A result store keyed by step and inputs.** Nextflow's cache proper, and where invalidation would live. It
   is a second record of what a round has, to be kept in step with the first, for a need that does not exist
   while everything is pinned.
3. **Temporal's reset to the failed step.** Runs roll over, so the history that holds the step is often gone;
   a reset replays the code paths of the old run; and a wait at `review` is not a point to reset to.

## 8. Where invalidation would go

Each pipeline decides "which step runs first" in one function (§4.2 for a dataset). A later rule such as "a
changed judge model voids the judge's result" is a condition there, plus what the step has to record to check
it. Nothing records that now.

## 9. The writer's calls

1. **A buffer's Restart reuses pyramids.** A pyramid's id is the hash of its whole recipe, other buffers serve
   the same artifact, and a new version over newly prepared datasets is the common case; restaging 150 volumes
   to add three is not what anyone means by it. This departs from "redo all the processing" for buffers only.
2. **A force stop loses the round.** Resume after one is a Restart (§2). Keeping it would mean storing the
   round's state outside Temporal on every stage write and restoring it on start: the largest piece of work
   here, for the last-resort button. A worker restart, a cancel and a failure all keep the state.
3. **A dataset's Restart is today's `go`**, including that it picks up its own open pull request and tests the
   current head anew, and that raw files in the shared cache are not fetched again (only the keeper deletes).
4. **Resume charges whoever asks.** The root is who pays, not an input of any result; a round whose first
   request ran out of budget can be resumed by another.
5. **The cleaner resumes.** The run that ends these plans becomes `resume`: `no run`, `frozen run` (both end
   the run, so by call 2 they restart in effect), `autodev's run failed`, `merge conflict` after a `revise`,
   `merged since`, `passing cause`, `fixed since`. These stay `go`: `input changed` (the round needs new
   pins), `merge conflict` after the cleaner closed the pull request, and any run that carries the diagnosing
   agent's advice. The guards on its `go` (no job of the ended round alive, no person acted since, the weekly
   limit) hold for `resume` too.
6. **No Resume at `done` or `debugging`, and none after a revoke** (§4.3).
7. **A resumed round has the judge look at a merge commit's own set** (§4.2, lines 2 and 4). A round that
   never stopped takes that set to `review` unjudged, and that stays.
8. **A buffer resumes at `staged` too** (§6): that is where a round with failed datasets rests.
9. **A request's `restart` key** replaces `again` in what the page writes (§5).

## 10. The page

A subject's page, where its actions hold `resume`:

```
[ Resume ]  [ Restart ]
```

- **Resume**: "Run what is left: every step that finished is kept, on the same pins." Primary.
- **Restart**: today's Run again text, which says it is from scratch and pins anew. At `debugging` it reads
  **Restart now**; at `new`, **Start**.

The buttons carry one word each. What sets the two apart is in their tooltips, the guide and
`docs/development.md`.

The blue box of the Datasets list and of a buffer's Prepare table:

```
3 selected   [ Resume ]  [ Restart ]  [ Create buffer ]
```

Both file one preparation request over the selection, the second with `restart: true`; hosts are approved in
the same dialog as today.

A resumed round's first timeline entry names what it kept and what it runs, for example "Resumed by <actor>:
the binding and the pins are kept; the crops are made again."

## 11. Left to the builder

- The names of the new activities, and whether `DatasetState.attempts` carries the attempt numbers of §4.4.
- What autodev does with `snippets:go` on an issue whose pull request is approved and unmerged; if it does not
  try the merge again, Resume sends what does.
- The order of work: datasets (workflow, table, route, a subject's buttons), then requests and the bulk boxes
  and the cleaner, then buffers. Each is usable alone.
- Tests: one workflow test per line of §4.2, named after its incident (the judge that was skipped, the pull
  request that merged after the round was blocked, the crop job that died), each asserting which jobs did not
  run. The replay fixtures do not change.
- Docs: the rule in `AGENTS.md`, `docs/development.md`, the guide, the cleaner's rule texts.
