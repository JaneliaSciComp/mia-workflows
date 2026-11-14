# MIA: shelving a subject — Spec

**Status:** v0.1 · 2026-11-14 · approach picked by the user 2026-11-14; this text awaits approval.

## 1. Intent

Some datasets stop for reasons that take real effort to correct. Once a person has read the issue and decided
it can wait, the dataset should stop asking for attention and stay out of everything automatic until the
person takes it back. *Shelving* is that decision: one click with a reason, undone by one click.

**Goals**

- A shelved subject is not listed under *Needs you*.
- A shelved dataset is inadmissible to every buffer plan and is never offered to, or started by, a preparation
  request.
- The stage it stopped at, and its timeline, stay as they are: shelving records a person's decision, not a
  new result.
- The reason is required and shown wherever the subject is listed.

**Non-goals**

- Removing a shelved dataset from buffers that already serve it. `revoke` does that.
- Snooze dates, priorities, bulk shelving, shelving a catalog record that was never started (it has no
  subject to press a button on).

**Anti-goal:** a shelved dataset that comes back by itself: through a request's `go`, the debugging agent, a
folder scan (`datasets sync`) or a catalog sync.

## 2. Approaches

1. **A hold beside the stage (picked).** buffer-api records the hold; the stage is untouched; no workflow
   changes. Works for every kind of subject. Cost: a second axis beside the stage table, kept to one rule (§3.2).
2. **A `shelved` stage** in `mia_data.config.STAGE_ACTIONS`. The purest fit for "the stage tables are the state
   machine", but stages are the workflow's to write: a new signal, a `workflow.patched` guard, a re-recorded
   replay fixture and a worker restart, and the stage it stopped at survives only in the timeline. Datasets only.
3. **A triage field** (`now / later / never`, or snooze-until). Two concepts where one was asked for.
4. **A `deferred` field on the catalog record**, surfaced through `record_gaps`. A commit in another repo and
   a sync instead of a button; triage state in the catalog.
5. **`revoke`.** Needs saved crops, degrades serving buffers, and still lists the dataset as `blocked`.

Picked 1: it is the only one that needs no workflow change, keeps why the subject stopped, and covers buffer
specs and requests for free.

## 3. Design

### 3.1 What is stored

One table in the ledger, written by buffer-api alone:

```
shelved (kind, number, by, at, reason NOT NULL, PRIMARY KEY (kind, number))
```

Its own table, not a key of `subjects.data`: `adopt_subject` and a spec edit replace that column whole, and a
`CREATE TABLE IF NOT EXISTS` needs no migration of the live database.

### 3.2 The actions and the one rule

`shelve` and `unshelve` join `ActionBody.action` and go through the existing action routes of all three kinds
(`/datasets/<record>/actions`, `/specs/<name>/actions`, `/prep/<number>/actions`), so the page still holds
no token. `shelve` carries its reason in `note`; an empty one is a 422.

The rule, in one function that `_subject_view` and `_accepts` both call (it also takes over the `gaps` rule
that removes `go`):

- a subject that is not shelved and whose status is `not started`, `stuck` or `done` also accepts `shelve`;
  one with a round in progress or at review does not (cancel first);
- a shelved subject has the status `shelved` and accepts `unshelve` and nothing else, whoever asks.

`shelved` joins `STATUS_WORDS`. It is in neither `NEEDS_A_PERSON` nor a stage table.

Both actions are recorded like any other (`add_action`, then settled `sent` with the timeline entry
`**Action:** shelve by page: <reason>`, in the transaction that writes or deletes the hold), announced on the
stream, and never sent to Temporal. The mirror copies the entry to the issue; no label changes.

### 3.3 What reads it

| Reader | Effect |
| --- | --- |
| `_subject_view` | `status: "shelved"`, `actions: ["unshelve"]`, `shelved: {by, at, reason}` |
| `GET /needs` | leaves shelved subjects out by status; returns their count as `shelved`; `?shelved=1` lists them instead |
| `Ledger.dataset`, `Ledger.datasets` | a shelved dataset reads `state: "shelved"` with the hold's reason (a row is made up for one that has none); the stored row is not touched, so unshelving restores it and `sync_datasets` and mia-data's writes need no change |
| buffer plans (`POST /plan`, the `plan` activity) | read `prepared` through the above: a shelved dataset is `not prepared` |
| `to_prepare` (`explore.py`) | leaves out rows whose state is `shelved` |
| a worker's `go` (`_nudge`) | sends none for a shelved subject and returns the reason as the `DatasetIssue` note |
| `_followed_state` | a request follows a shelved dataset as `needs-human`: it settles, and the request says so |

`shelved` is not in `DATASET_STATES`: no worker can write it.

### 3.4 The page

- **Subject page:** a *Shelve* button beside the others, which asks for the reason (as *Reject* asks for its
  note). A shelved subject shows a notice with who, when and why, and one button, *Unshelve*.
- **Needs you:** a *Shelved (n)* toggle beside the kind toggles; pressed, the list is the shelved subjects, each
  with its reason and *Unshelve*. Rows of the normal list get *Shelve* with their other buttons.
- **Datasets:** `shelved` is a state pill and a filter value; the reason column shows the hold's reason.

## 4. Left to the builder

- The exact wording of buttons, notices and refusals.
- Whether the made-up row of a never-run shelved dataset is built in the ledger or in `_dataset_rows`.
- Tests: one for the rule (each status, shelved and not), one for `/needs`, one for a plan that loses a
  shelved prepared dataset and gets it back, one for `_nudge` and `_followed_state`, and the page's route
  check in `tests/test_buffers_ui.py`.
- `AGENTS.md` and `docs/development.md`: one rule line (only buffer-api writes a hold; a shelved subject
  accepts only `unshelve`).

## 5. Rollout

No workflow code changes, so no replay fixture and no patch guard. buffer-api and the buffers worker restart
to load the new routes and activities; the table appears on first start.
