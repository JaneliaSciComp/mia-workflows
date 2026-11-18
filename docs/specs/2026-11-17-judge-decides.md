# MIA: the crop judge decides — Spec

**Status:** v0.2 · approved 2026-11-17 · nothing built (plan: `docs/plans/2026-11-17-judge-decides.md`). Rests on the crop judge of the autonomous-gates
spec (`2026-11-09-autonomous-gates.md` §4.2, §9), on the action routes and stage tables of the webapp-actions
spec, and on revocation (`data:revoke`, `docs/development.md`). §3 to §6 are the user's decisions of
2026-11-17; §8 lists the calls that are the writer's and still the user's to change.

## 1. Intent

A dataset round makes sample crops and then waits at `review` for a person to approve or reject them. The
crop judge (a vision model with read-only tools) already looks at every passing set, and in `active` mode its
approve ends the review with no person; anything else it says still waits. The user approved all ten sets the
judge's approve was overruled on, and most rounds of a preparation request sit at `review` until somebody has
time.

The judge is to decide in both directions, so that a round goes from crops to `prepared`, or to rework and
then to `prepared`, with no person. The person keeps the last word: whatever the judge decided can be
overturned afterwards, in either direction, with one click.

**Goals**

- A set the judge finds in order is saved with no person.
- A set the judge doubts goes to the developer (autodev), which looks into the findings and either fixes the
  reader or answers that the crops are right as they are.
- Automatic rework is bounded: once per dataset by default, a setting.
- A person can accept a set the judge sent back, and reject a set the judge approved, at any later time while
  that still means something (§5).
- Everything that was decided, and by whom, is on the dataset's timeline and in the Decisions tab.

**Non-goals**

- The review judge of snippet pull requests (`judge.review`) does not change.
- The debugging agent does not change. It stays what a round gets when it **failed** before there were crops
  to judge (a job crashed, a host refused, a rate limit). It is never given a judge's findings.
- No new agent. The judge, the developer and the debugging agent are the three there are.
- Binding-only mistakes get no cheaper path here (§7).

**Anti-goals**

- A loop of judge and developer that a person finds days later, having spent money on a set that was fine.
- A person's click that is refused because a round is "busy" reworking what the judge sent back.
- A set that is saved, or stays saved, against the newest verdict of a person.

## 2. What exists

- `mia_core/judge/`: the judge's schema, evidence, comment and `effective_verdict`, which turns the judge's
  answer into `approve | reject | escalate`: low confidence abstains, a `bad` finding rejects, a reject with no
  `bad` finding is an approve with notes, and more `suspect` findings than `judge.crops.max_suspect` reject.
- `judge.crops.mode`: `off`, `shadow` (recorded, decides nothing) or `active` (an approve authorizes). The live
  deployment runs `shadow`.
- `MiaData` (`mia_data/workflows.py`): `_judged` and `_judge_crops` run the judge on a passing set;
  `_judge_grants` lets an authorized approve save the set (`_publish_set`) or tell autodev to merge the pull
  request's head (`_pr_approved`). A person's verdict that arrives while the judge looks is acted on at once.
- A person's reject: on a matched binding, `_develop` opens a snippets issue asking for a reader superseding
  the binding's, with the person's comments; on a pull request's head, `_revise_pr` posts them as `/revise`.
- A revision request autodev answers without a commit (`_answered`) brings the same crops back to `review` with
  autodev's answer quoted.
- A round that failed goes to `debugging` (`limits.debug_cycles`, default 1).
- `revoke` on a dataset at rest: the saved set is rejected in the assessments, the row reads `revoked`, staged
  artifacts are quarantined and buffers stop serving them.
- `authorized()` (`mia_core/judge/policy.py`): the newest approve stands unless a reject follows it. A person's
  later verdict therefore already outranks the judge's earlier one.

## 3. What the judge's answer does

With `judge.crops.mode: active`, for a passing set at `review`:

| The judge's findings | What happens |
|---|---|
| every crop `ok` | **Approve.** The set is saved, or the pull request's head is merged and then saved. |
| a `suspect` or a `bad` crop | **Sent back.** The findings go to the developer (§4). |
| the judge is unsure (`escalate`, confidence under `judge.min_confidence`), did not run, or was skipped | The set waits at `review` for a person, as today. |

- `judge.crops.max_suspect` goes away: one `suspect` is enough to send a set back, and no count of them
  rejects by itself.
- A judge that writes `reject` with every finding `ok` is still read as an approve with notes.
- The judge's instructions (`mia_core/judge/prompts/crops.md`) change with this: `suspect` means "this may be a
  defect in how the data was read, and is worth the developer's look". What a careful viewer would merely
  glance at (contrast, a crop that is mostly background, aesthetics) is `ok` with a note. The prompt version
  and the evaluation set's expected verdicts change with it.
- A person's verdict that is already there when the judge answers wins, as today.
- `shadow` and `off` behave as today.

## 4. Rework

### 4.1 The findings go to the developer

A set the judge sent back is handled as a person's reject is, with the judge as the author:

- **A matched binding's set:** a snippets issue asks for a reader superseding the binding's (`_develop` with
  the binding), carrying the judge's findings.
- **A pull request head's set:** the findings go to the pull request as `/revise`.

The request names the set (§5.4) and carries, per finding, the crop it is about, its severity and the judge's
note, and says which the developer may conclude (§4.2). The developer's job gets the judged evidence (the
PNGs the judge looked at, and where each crop was placed) read-only: its answer can let a set be saved over
a visual doubt, so it looks at what the judge saw. The dataset goes to `developing`; its timeline says the judge sent the set
back, with the findings, and names the set.

### 4.2 What the developer can conclude

The developer investigates before it writes anything, and ends in one of two ways:

1. **A fix:** a new head. Its crops are made and judged like any head's.
2. **"The crops are right as they are":** no commit, and an answer saying why each finding is not a defect.
   This ending must exist for both shapes of request: a revision of a pull request (it exists: `_answered`)
   and a snippets issue with no pull request yet (it does not: today autodev can only stop there, which reads
   as `blocked`). autodev says it in a way a round can tell from a stop.

The developer's instructions say that the second ending is a full answer and that it is not to write a reader
to satisfy a doubt (the user's rule: serious issues only, process the data as it is).

**A round with no commits is not that answer.** "Right as they are" is an outcome the developer states and
autodev validates, never something a round infers from silence. It counts only when all of these hold:

- it is the explicit outcome of the agent run that answered **this** request, and names the set the request
  named;
- it answers every finding of the request, one by one, saying what the developer looked at;
- the run ended by itself: not cut off, not out of budget or time, not an error.

Anything else with no commits (a run that could not investigate, an interrupted session, a no-commit round
for another reason) is a developer that stopped: the round ends `blocked` as today and approves nothing. This
holds on the issue path and on the pull request path alike; the pull request path's present rule (`_answered`:
any answer after the request, at rest) stays good only for bringing a person's own reject back to them.

What the round does with "right as they are":

| The set the judge sent back had | Then |
|---|---|
| `suspect` findings only | The set is **approved** and saved (or its head merged), with the developer's answer on the timeline. |
| a `bad` finding | The judge and the developer disagree: the set waits at `review` for a person, with both shown. |

The same ending answers a **person's** reject, and there it never approves: the set goes back to `review` with
the developer's answer, whether the reject was of a pull request's head (as today), of a matched binding's
set, or of a saved set (§5.2). In the last case the set under review is the revoked one; accepting it saves it
again, which repairs the revocation as any save does.

### 4.3 The limit

`judge.crops.max_reworks` (default **1**) is how many times the judge may send a dataset's sets back before a
person is asked. When the judge would send a set back and the count is used up, the set waits at `review`
instead, with the findings shown; the page reads `needs-human` there, as for any review.

- The count is per dataset and starts over with every `go` that a person or a preparation request sent, and
  with every verdict of a person.
- Only the judge's send-backs count. A person's reject is never refused for the count.
- A developer that stops, or says nothing for `limits.autodev_quiet_hours`, ends the round `blocked` as today.

## 5. A person overrides

### 5.1 Accept a set the judge sent back

From the moment a set is sent back, the page shows that set, the judge's findings and an **Accept** button:
while the rework runs (stages `developing` and `testing`, and `review` when a later head waits there) and
after it stopped (`blocked`, `failed`, `cancelled`, `needs-human`, and `debugging`). Accept:

1. cancels what the send-back started, if it still runs: a test of a newer head and the judge looking at one
   are killed as a `cancel` kills them. A snippets issue or pull request that only the send-back opened is
   closed: the reader it asks for is not wanted any more. (A plain `cancel` is unchanged and leaves both open
   for the next `go`.)
2. merges and saves **that** set: a matched binding's set is saved at once; a pull request head's set is
   merged as it was at that head, later commits discarded, then saved.

**The developer's running job is waited for, not killed** (the user's decision): autodev reads the Accept when
its agent job ends, which can take up to that job's walltime. The dataset reads `merging` meanwhile and says
what it waits for.

**When the superseded work can no longer land.** autodev resets and merges only after its own job has ended
and its result has been dropped unpushed, so nothing of it can be pushed afterwards; from the Accept on,
autodev starts no new agent round on that issue. The round ignores every head, test result and judge's answer
that is not the accepted set's, whenever it arrives.

**When it fails.** The person's approval is recorded when the click is taken and stands whatever follows. If
the reset, the merge or the save fails, the dataset rests at `blocked` saying that the set was accepted, by
whom, and which step failed; nothing reads `prepared` until the save happened. Accept can be sent again.

The user's words: "cancel the new work and merge the old set", and "no matter what it does, I should be able
to override the decision it made later". It is one action, not a cancel followed by an approve, and it is
taken whether or not a round is in progress. A set that was sent back therefore outlives the round that made
it: its crops, its binding and its verdicts stay with the dataset until a newer set is approved.

Accept is offered until a newer set of the dataset has been approved; after that there is nothing to
override. The same Accept works for a set the person themselves rejected and thinks better of, a saved one
included (§4.2).

### 5.2 Reject a set the judge approved

At `done` the page shows the saved set, who approved it and a **Reject** button that takes a reason. Reject:

1. does what *Revoke* does: the saved set is rejected in the assessments, the dataset no longer reads
   `prepared`, no plan and no request takes it, and buffers stop serving volumes staged from it;
2. starts the rework of §4.1 with the person's reason, as a new round of the dataset.

When the reworked set is saved, the dataset is `prepared` again; what that save releases from quarantine is
§5.5's. The user's words: "revoke and start the rework".

### 5.3 What stays

- Approve and reject at `review` are unchanged.
- **A person outranks the judge and the developer, whenever each spoke.** For one set, the newest verdict of
  a person decides. The judge's verdict and an approval that follows from a developer's answer count only
  while no person has given a verdict on that set, also when they are written after the person's (a judge
  that was still looking, a developer's answer that arrives late). This is a rule of `authorized()`, not a
  matter of which row is newest; how the rows encode it is the builder's.

### 5.4 A verdict names its set

A dataset can now have two sets a person may approve at the same moment: the newest head's, waiting at
`review`, and an earlier one that was sent back. Every approve, reject and Accept therefore names the set it
is about, and the page sends it with the click.

- A set is identified by the round and test that made it, and holds together, unchanged from then on: the
  snippets commit (the pull request's head, or the matched commit), the binding and its hash, the crops and
  the verify report, the judge's evidence, and every assessment written about it.
- buffer-api takes a verdict only for a set it applies to (the one under review; one that was sent back and
  not superseded by an approved set; the saved one, for a reject at `done`) and refuses any other with a
  sentence.
- The round checks again when it carries the verdict out. A set that stopped being eligible between the
  click and then (a newer set was approved meanwhile, its crops are gone) is refused on the timeline, and
  nothing is merged or saved.
- An action may still name the subject's `revision`; that says the page was current, not which set was meant.

### 5.5 What a save repairs

Today a save after a revoke clears the quarantine of **every** staged artifact of the record
(`_release_revoked`). Artifacts are immutable and were staged by the recipe that was rejected, so after a fix
that would put volumes read by the broken reader back into service. The save repairs by recipe:

- **The same set is accepted again** (§4.2, §5.1: its binding is the one that was revoked): the revocation is
  undone, and the artifacts staged from that binding are released.
- **A corrected set is saved** (another binding): the dataset is `prepared` again, and artifacts staged from
  any other binding of the record stay quarantined. Buffers stage the record anew under the new recipe; the
  keeper evicts the old artifacts after `autonomy.quarantine_days` as for any quarantined asset.

This also changes what a plain `go` after a *Revoke* repairs, for the same reason.

## 6. What a person sees

- **Timeline:** the judge's entry says what it decided and why (approve; sent back with the findings; left to
  a person and why). A send-back, the developer's answer, an approval that follows from it, and each override
  are entries of their own, each naming the set and who acted.
- **The dataset's page:** the set under review or rework with its findings beside the crops; Accept and Reject
  as §5 says, and only when they would be carried out.
- **Decisions tab:** a judge's decision that a person overturned is shown as overturned, by whom, and counts as
  a disagreement in the agreement figures.

## 7. Known ceilings

- **Binding-only mistakes.** When the reader is right and the match agent bound the files wrongly, the
  developer can only answer with a new reader, because a snippet pull request adds exactly one directory. That
  works and is wasteful. Revisit when the timeline shows it is common.

## 8. The writer's calls

Each is assumed above and is the user's to change.

1. Reject at `done` is offered for every saved set, also one a person approved.
2. Accept during rework is also offered for a set the person themselves rejected.
3. At `merging` nothing new is offered: a person who disagrees with an approve waits for `done` and rejects
   there.
4. A "right as they are" answer to a set with a `bad` finding waits for a person (§4.2).
5. The count of §4.3 starts over with a person's verdict, not only with a `go`.
6. The setting is `judge.crops.max_reworks`; `max_suspect` is removed, and a config that still sets it is
   refused with a sentence saying it is gone.
7. Merging an earlier head (§5.1): autodev resets its pull request's branch to that head and merges it. The
   branch is autodev's own.

8. The judge's instructions are rewritten so that `suspect` means a possible reading defect (§3).
9. Accept is also offered after the rework stopped (§5.1). This is the larger half of the override: a set has
   to outlive its round, and a dataset at rest has to take an approve.
10. A developer's "right as they are" to a person's reject always comes back to `review` (§4.2).

## 9. Left to the builder

- How a send-back and a "right as they are" approval are recorded as assessments, so that `authorized()` and
  the Decisions tab read them rightly (a `policy:` author exists for approvals no person and no judge made).
- How autodev states and validates "right as they are" (§4.2) on an issue with no pull request and on a pull
  request, within the rule that a stop is the `needs-human` label and never a comment alone.
- How the judged evidence reaches the developer's job read-only: autodev never mounts a path it read from an
  issue or a comment.
- The stage table: `approve` at `developing` and `testing` and `reject` at `done` go into `STAGE_ACTIONS`
  first; buffer-api offers and takes them only when there is a set they apply to. `AGENTS.md`'s rule that a
  round in progress accepts only `cancel` and `stop` gets this exception written into it.
- A reject at `done` starts jobs, so it is a start action for host approval and for the root budget.
- Workflow changes are guarded with `workflow.patched` and the replay fixtures re-recorded; the result
  dataclasses only gain fields with defaults.
- The page's wording and layout, within §6.

## 10. Rollout

The harness is not touched. After the merge: `judge.crops` in the live `mia.yaml` becomes
`{mode: active, max_reworks: 1}` (it reads `{mode: shadow, max_suspect: 3}` today), buffer-api and the workers
are restarted, and one dataset is run to `done` with a person overriding in each direction.
