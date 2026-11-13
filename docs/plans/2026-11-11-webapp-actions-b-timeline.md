# Webapp actions, slice B: timeline and mirror — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a round writes what happened to the registry's timeline and never to a data or buffers issue; a
`MiaMirror` workflow copies each subject's timeline and stage label to its issue whenever GitHub answers.

**Architecture:** one `timeline` table beside `subjects`; three routes; the two packages' `_comment_once` and label
swap become one timeline write each; buffer-api gets a Temporal gateway (`Dispatcher`) and kicks the mirror on
every append; the mirror is one workflow and one activity in `mia_data/mirror.py`, registered by both workers.
mia-data's stage writes are awaited behind one patch.

**Tech Stack:** as slice A, plus `temporalio` in buffer-api (already a dependency of the package).

**Spec:** `docs/specs/2026-11-11-webapp-actions.md` (v0.5): §4.3, §4.4, §6, §7, §11 steps 2–3, §14 row B.

## Global Constraints

- Activities keep their names and signatures (§6). The only workflow change is `stage-write-awaited` in
  `mia_data/workflows.py`, with a new recorded fixture; the older fixtures are not re-recorded.
- The registry write is the required one: an activity that cannot reach buffer-api raises and is retried.
- mia-data without a `service` section keeps today's GitHub writes (its tests and a dev set-up run that way); that
  path is deleted in slice E. mia-buffers always has a service.
- Labels still start rounds in this slice. A stage or comment for an issue the registry has no subject for adopts
  the issue first (§11 step 2). `refuse_label` still removes the label.
- The keeper's over-quota comment is not touched here (slice E).
- Rules of slice A's plan on routes, tokens, paths, style, commits apply.

## Review Focus

1. A stage entered twice in one round (same marker): no second entry, but the subject's stage and revision move.
2. buffer-api is down when a round writes a stage: the activity raises (retried), nothing is posted to GitHub.
3. The mirror crashes after posting a comment and before the write-back: the next pass finds the comment by its
   marker and posts no second one.
4. An issue labelled on GitHub after `adopt-issues` ran: its first stage write adopts it.
5. Temporal is unreachable from buffer-api: the append still succeeds; the kick is logged; a restart kicks.

## Tasks

### Task 1: ledger — the timeline

`mia_buffers/ledger.py`, tests in `tests/test_buffers_ledger.py`.

- Table `timeline (kind, number, seq, at TEXT, round TEXT, stage TEXT, body TEXT NOT NULL, marker TEXT NOT NULL,
  mirrored_at TEXT, comment_url TEXT, PRIMARY KEY (kind, number, seq), UNIQUE (kind, number, marker))`.
- `find_subjects` gains `kind` and `number` filters.
- `append_entry(kind, number, *, body="", marker="", round_id=None, stage=None) -> tuple[dict, dict | None] | None`:
  None when there is no such subject; else (subject, entry). With `stage`: the subject's stage is set and its
  revision raised, whether or not an entry is added. With `body`: an entry unless one with this marker exists
  (then the existing entry is returned). One transaction.
- `timeline(kind, number) -> list[dict]` in seq order; `mark_mirrored(kind, number, seq, comment_url) -> bool`;
  `unmirrored_subjects() -> list[tuple[str, int]]`.

### Task 2: routes, registry client, the kick

`mia_buffers/api.py`, `mia_core/registry.py`; tests in `tests/test_buffers_api.py`, `tests/test_core_registry.py`.

- `POST /timeline/<kind>/<number>` (worker) body `{body, marker, round, stage}` (all optional; body and marker go
  together; nothing given is 422; unknown stage 422; no subject 404). Answers `{subject, entry}`; announces the
  change; kicks the mirror.
- `GET /timeline/<kind>/<number>` (read): `{subject, entries}`. (By number, not the spec's by key: a key may
  hold a slash, and the mirror knows only the number.)
- `PUT /timeline/<kind>/<number>/<seq>` (worker) body `{comment_url}`: the mirror's write-back.
- `BufferService(dispatcher=None)`: anything with `kick_mirror(kind, number)`; `kick_unmirrored()` is called by
  `serve` at start.
- `Registry.append_entry(kind, number, *, body="", marker="", round_id=None, stage=None) -> dict`,
  `Registry.timeline(kind, number) -> dict | None`, `Registry.mark_mirrored(kind, number, seq, comment_url)`.

### Task 3: the Temporal gateway

`mia_buffers/dispatch.py` (new), `mia_core/config.py`, `mia_buffers/config.py`, `mia_buffers/api.py` (`serve`);
tests in `tests/test_buffers_dispatch.py`, `tests/test_core_config.py`.

- `service_config` gives a service whose app is `mia-buffers` a `datasets_pipeline: {namespace, task_queue}`
  section read from the first service whose app is `mia-data` (task queue: that service's name). `Config` holds
  `datasets_pipeline: PipelineCfg = PipelineCfg(namespace="mia-data", task_queue="mia-data")`.
- `Dispatcher(connect, targets)`: `targets` maps kind to `(namespace, task_queue)`; `connect(namespace)` is an
  async function returning a Temporal client (injected; `serve` passes one built on `autodev.cli._client`). One
  daemon thread runs an event loop; clients are made on first use and kept. `kick_mirror(kind, number)` schedules
  `start_workflow("MiaMirror", args=[kind, number], id=f"mirror/{kind}/{number}", task_queue=..., start_signal=
  "kick")` and returns at once; a failure is logged.

### Task 4: the mirror

`mia_data/mirror.py` (new); `mia_data/cli.py` and `mia_buffers/cli.py` (worker registration); tests in
`tests/test_mia_mirror.py`.

- `MiaMirror` workflow: `run(kind, number)`, signal `kick`; runs the `mirror` activity (no retry limit, backoff
  capped at 10 minutes, 5 minute start-to-close) and again while a kick arrived during a pass.
- `Mirror(gh, registry, bot, labels)` with activity `mirror(kind, number) -> int` (entries posted): reads
  `registry.timeline`; posts each unmirrored entry once per marker (the bot's own comment carrying the marker
  counts as posted) and writes it back; then leaves the subject's stage label as the only one of the kind's stage
  labels. `labels` maps kind to (label function, stages). A subject without an issue is an error in this slice
  (slice C opens issues).
- mia-data's worker registers it for `dataset` on `gh_data`; mia-buffers' for `buffer` and `prep` on `gh_issues`.

### Task 5: activities write the timeline

`mia_data/activities.py`, `mia_buffers/activities.py`, `mia_buffers/adopt.py` (one-issue helpers); tests in
`tests/test_mia_activities.py`, `tests/test_buffers_activities.py`.

- mia-data: `set_stage`, `comment` (repo `data`), `refuse_label` write an entry when the registry is configured;
  `set_stage` passes the stage. `comment` then returns None. On 404 the issue is adopted from its marker.
- mia-buffers: `set_stage`, `set_prep_stage`, `comment`, `comment_plan`, `comment_prep`, `comment_drop`,
  `refuse_label` write entries (`buffer` or `prep`; `comment` and `refuse_label` find the kind by the subject
  that exists). On 404 the issue is adopted as `adopt-issues` would.
- Test: with every issue call of the GitHub fake failing, a stage, a comment and a refusal succeed; the mirror,
  run once the fake recovers, leaves the comments and the label on the issue.

### Task 6: stage writes awaited

`mia_data/workflows.py`, `tests/test_mia_workflows.py`, a new fixture `tests/fixtures/mia/dataset_history_b.json`.

- `_stage`: under `workflow.patched("stage-write-awaited")` every stage write is `await self._act("set_stage",
  ...)` (after settling one an older code path left pending).
- `test_replay_dataset_history_b`, recorded alone with the scoped `-k`.

### Task 7: whole-slice check and docs

`pixi run fmt`, `lint`, `typecheck`, `test`; `docs/development.md` (timeline routes, the mirror, the gateway);
`mia.yaml.example` comment on buffer-api reaching Temporal.
