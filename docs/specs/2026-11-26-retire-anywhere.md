# MIA: retiring a buffer from any state — Spec

**Status:** v0.2 · 2026-11-26 · approved by the user 2026-11-26 (as v0.1) and built. v0.2 says what the build
settled: the patch is taken where the signal arrives (§3.2), the entry of a round the retire ended is worded as
a cancel's is (§3.2), a ready buffer whose late datasets still source counts as a round to end (§3.2), a
redelivered retire is dropped where it arrives (§3.2), a rebuild's job is not killed (§3.6), and the
confirmation names the serving version only where the page has it (§3.5).

## 1. Intent

*Retire* is a person's "I am done with this buffer: release whatever it holds". Today it is accepted only where
a buffer serves (`ready`, `idle`), so a buffer that was cancelled, never started, stopped at `needs-human` or
shelved cannot be retired, and its page keeps a state that says less than the person has decided.

**Goals**

- A buffer takes `retire` at every stage but `retired`, and while it is shelved.
- A retire always ends at `retired`, also when there was nothing to release.
- A retire releases every version of the buffer that is still registered: the one being served, the one a
  stopped round left behind, and any that an earlier plan left serving.
- The page asks before it sends a retire, and the question says what this retire will do to this buffer.

**Non-goals**

- Deleting a subject or its timeline. A retired buffer stays listed and takes `edit` and `go` as now.
- `retire` for a dataset or a request.
- A retire of one version alone by a person. The keeper's retire of one idle version stays as it is.
- The repair agent sending a retire: its menu does not grow.

**Anti-goals**

- A page that reads `retired` while a job of the buffer's round runs or a version of it is still registered.
- A stage in the registry that the buffer's workflow does not also hold.

## 2. Approaches

1. **The workflow carries it out everywhere (picked).** The stage table accepts `retire` at every stage; the
   buffer's workflow acts on a person's retire wherever it is: at rest it releases and writes `retired`, in a
   round it takes the cancel path and ends at `retired`. Cost: workflow code changes, so a `workflow.patched`
   guard and a new replay fixture.
2. **buffer-api retires a resting buffer itself**, as it carries out `edit` and `shelve`. No workflow change,
   but the workflow would go on holding the stage and the unretired buffer it had, against "the registry's stage
   is the workflow's", and a round in progress would still need the workflow.
3. **Force stop under the name Retire.** It already retires every version and writes `retired`, but it
   terminates the workflow, loses the round's state and kills no cluster job.

Picked 1: it is the only one that keeps one writer of the stage and leaves nothing running.

## 3. Design

### 3.1 The stage table and the hold

`mia_buffers.config.STAGE_ACTIONS` gains `retire` at `new`, `planning`, `plan-review`, `sourcing`, `staged`,
`needs-human` and `cancelled`; `ready` and `idle` have it; `retired` does not take it. A round in progress
(`working`) therefore accepts `cancel`, `retire`, what feeds it (`dropped`) and the force stop.

A shelved buffer takes `unshelve` and, where its stage does, `retire` (`_rules` in `mia_buffers/api.py`). A
shelved dataset or request takes `unshelve` alone, as now.

### 3.2 What the workflow does with a person's retire

A person's retire is the `retire` signal that carries the action's event; the keeper's carries a buffer id and
no event and is handled as a lifecycle signal. The signal handler takes `workflow.patched("retire-anywhere")`
and puts a person's retire in a queue of its own (`retire_events`), which no step of a round refuses. A run
whose history already holds a person's retire as a lifecycle signal keeps that handling until it rolls over.

| Where the buffer is | What happens |
|---|---|
| a round works or waits at a gate (`planning`, `plan-review`, `sourcing`, `staged`) | The round is ended as a `cancel` ends it (`_cancel_round`: jobs killed, locks released, reservations settled, the round's buffer retired), then §3.3, then the stage is `retired`. A job that could not be killed ends the round at `needs-human` saying so, as a cancel does; a retire sent there finishes it. |
| it is `ready` and late datasets of its round still source | As the row above: their tasks are cancelled, their jobs killed and their locks released, then §3.3, then `retired`. |
| it serves (`ready`, `idle`) | A rebuild in progress is abandoned, then §3.3, then `retired`. |
| it rests (`new`, `needs-human`, `cancelled`) | §3.3, then `retired`. |
| `retired` | Refused: the click crossed a retire. |

A cancel and a retire that are both queued end the round once, at `retired`. A `go` sent after the retire starts
a round from `retired` as it does today.

The entry that writes `retired` says who retired the buffer and what was released: each version with the
archive of its draws, or that nothing was registered. Where a round was open, the stage write carries the open
pass's result as a cancel's does, so the pass ends saying how. The round's own buffer is released with the
others and named in the same entry. A run with no round (the retire started it) writes the sentence as an entry
of its own, since its stage write carries none.

A retire whose event the workflow has handled before (a stale Retry) is dropped by the signal handler: it ends
no round and retires nothing.

Afterwards the state holds `retired = True`, no `superseded` versions and no `idle_since`.

### 3.3 Releasing every version

The workflow asks the registry which versions of this buffer are still registered (the read activity
`live_buffers`: the buffers whose id names this subject, `_issue_of`, and whose latest event is not `retired`)
and runs the `retire_buffer` activity for each, and for the round's own buffer when it was never registered. It reads the registry, not its own state, so a workflow that the retire
itself started (no run was open) releases what an earlier run registered. Running it again finds nothing, so
the step can run again at any point.

### 3.4 Reaching `retired` ends a hold

When a buffer's stage is written as `retired`, buffer-api deletes the buffer's row of the `shelved` table in the
same transaction and adds a timeline entry that says the buffer came off the shelf because it was retired. It
holds whoever wrote the stage: a person's retire, the keeper's retire of an idle buffer, a force stop. The
workflow still hears of no hold, and buffer-api stays the only writer of one. A retired buffer then reads the
status `done`.

### 3.5 The page

The *Retire* button needs no logic of its own: the page offers what the subject's `actions` list holds. Its
confirmation is the page's usual `confirm()`, and its text is built from what the subject view already carries
(`stage`, `shelved`, and `serving` where a list gave it), one sentence per fact that applies
(`retireConfirm` in `mia_buffers/ui/actions.js`):

```
Retire this buffer?

<one of>
  Its round in progress is cancelled first: its cluster jobs are killed and the work so far is not kept.
  The round at its approval gate is ended and what it staged is released.
  The plan under review is dropped; no job has run for it.
  Version <serving> stops serving.            (or: The version being served stops serving.)
  No round is open: this marks the buffer retired and releases what earlier rounds left registered,
  if anything.

Every version of it that is still registered is released, also one an earlier plan left serving: training
jobs that read one are refused from now on. The draws are archived and the keeper may reclaim the staged data.
<if shelved> It comes off the shelf.
It can be planned again at any time.
```

The button's tooltip and the `retire` line of the guide say the same in one sentence.

### 3.6 What stays as it is

The keeper's `idle` and `retire` signals and their rules; the force stop; `cancel`, which still ends a round at
`cancelled` (and that buffer then takes a retire); the repair agent's rules and menu; datasets and requests.

**Known gap.** A retire of a serving buffer abandons a rebuild as a go does: the rebuild's tasks are cancelled,
but a stage job one of them had running is not killed and runs to its end under a buffer that reads `retired`.
Ending it needs the rebuild to unwind as a round does (`_cancel_round`), which is not part of this change.

## 4. Build notes

- **Workflow determinism.** Every new branch runs only for an event in `retire_events`, which only a patched
  signal fills; the recorded buffer fixtures replay unchanged, and `buffer_history_retire.json` records a
  retire of a round in progress followed by a retire of the retired buffer.
- **Tests.** The stage table (`working` takes nothing beyond `cancel`, `retire`, `dropped`, `stop`; every stage
  but `retired` takes `retire`); `_rules` for a shelved buffer; the hold ending with the `retired` write; the
  workflow at `new`, at `needs-human` with a buffer registered, at `cancelled`, in `sourcing`, at `staged`, at
  `ready` with a superseded version, with a job that cannot be killed, and at `retired`; the confirmation text
  per state, imported under node.
- **Docs.** `AGENTS.md` (the stage-table rule's sentence on a round in progress; the shelving rule),
  `docs/development.md` (the action table; Idle, keep, retire; One live buffer per spec; the cancel
  paragraph), the README's sentence on a buffer's life, the page's guide.
- **Deployment.** The buffers worker and buffer-api restart together; the change is built in a worktree and
  merged with that restart.

## 5. Calls made while writing this

Each is the builder's call; none was corrected before the build.

1. A retire is not offered at `retired`.
2. A cancel and a retire queued together end at `retired`, not `cancelled`.
3. The versions to release are read from the registry, not from the workflow's state (§3.3).
4. buffer-api writes the entry that says the hold ended (§3.4).
5. The confirmation names the version being served but does not list the other versions it releases: the
   subject view does not carry them, and the general sentence covers them.
6. A go and a retire that both wait while the buffer rests are taken retire first: the round then starts from
   `retired`. The page sends one action at a time, so the two do not meet there.
