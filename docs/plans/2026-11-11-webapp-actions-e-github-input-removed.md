# Webapp actions, slice E: GitHub input removed — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** labelling, commenting on or closing a data or buffers issue changes nothing: nothing polls those
issues, and the commands that existed only to feed GitHub are gone.

**Architecture:** deletion. The spec makes this slice two deploys (§11 step 4). **This plan is the first
deploy**: the pollers stop delivering labels, the buffers poller and its schedule go, `mia-data seed` goes,
`init-labels` creates stage labels only. The **second deploy** deletes the label fallbacks, and the spec gates
it on a live condition no test can check: no running round was started by a label event. It is listed under
"Left for the second deploy" with the exact code to delete, and is not done here.

**Spec:** `docs/specs/2026-11-11-webapp-actions.md` (v0.5): §3, §6 (last two rows of the table, last
paragraph), §7 (last paragraph), §9, §11 step 4, §12, §14 row E.

## Global Constraints

- mia-data keeps its scheduled pass over snippet PRs: it wakes the dataset workflow whose PR changed and passes
  the `mia-data/crops` status on PRs that add no snippet (§9). Only label delivery leaves it.
- Activity names and signatures do not change. `MiaBufferPoll` leaves the worker; `mia-buffers schedule apply`
  deletes its schedule, so an old schedule does not keep starting a workflow type nobody registers.
- Nothing here deletes a label from a GitHub repo: that is the operator's step at cutover (documented).
- The keeper still signals `idle`, `retire`, `degraded` and `revoked`: those never came from GitHub.

## Tasks

### Task 1: the pollers

- `mia_data/poll.py`: the label loop and `deliver_label` go; the PR watch and the status pass stay.
- `mia_buffers/poll.py` is deleted, with `MiaBufferPoll`, the `poll` activity, and the poll schedule (deleted by
  `schedule apply`; the keeper's stays).
- Tests of label delivery, of drops noticed by the poller and of the closed-issue retire go; the PR-watch tests
  stay.

### Task 2: commands and config

- `mia-data seed` and `seed()` go; `render_issue` loses its `root` argument and the root marker (the mirror is
  its one caller). `init-labels` of both packages creates stage labels only.
- `github.trusted_bots` leaves mia-data's config (it only told the poller whose `go` label counted).
- The keeper's over-quota comment goes: the Cache view already shows the keeper's `over_quota`.

### Task 3: docs and the page's Docs tab

`docs/development.md`, `docs/mia-data-workflow.md`, `docs/mia-data-handoff.md`, `docs/autodev-runbook.md`,
`mia.yaml.example`, `AGENTS.md` (a new rule: nothing reads a data or buffers issue), the Docs tab's steps.

### Task 4: whole-slice check

## Left for the second deploy (not in this plan)

Delete once no running round was started by a label event (its event id is not an action's):

- `mia_data/activities.py`: the issue-marker branch of `admit` (and `ROOT_RE`), the comments branch of
  `read_feedback`, the `remove_label` branch of `refuse_label`, the adopt-on-404 branch of `_post`, and the
  whole no-`service` path of `_post` with `_swap_label` and `_comment_once` on the data repo (`service` becomes
  required for mia-data).
- `mia_buffers/activities.py`: the issue-fence branch of `_spec_text`, the comments branch of `revoke_targets`,
  the `remove_label` branch of `refuse_label`, `_adopt`.
- Then the human labels are deleted from both repos by the operator.
