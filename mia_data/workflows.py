"""The Temporal workflows that take a dataset from its `go` action to published crops.

`MiaDataDataset` lives as long as the dataset's subject does. Each `go` starts a round: an agent matches the
dataset to a loader snippet, the round's crops are made by the worker's own job and shown in the
dataset's timeline, and a human approves them (published) or rejects them with feedback (spec §14: autodev
gets the feedback and a superseding snippet's PR is followed). When no snippet fits, the round instead opens an issue in mia-snippets
for autodev to write one and follows that PR: every head's crops are reviewed the same way, the approve merges
the PR, and the approved crops are published.

Workflow code does no I/O: every registry, GitHub, filesystem and cluster interaction is an activity called
by name (see activities.py). The durable state is a `DatasetState`, handed to continue_as_new at quiet points.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import math
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

with workflow.unsafe.imports_passed_through():
    from autodev.workflows import QUEUE_SLACK, RETRY_MAX_INTERVAL, error_message

    from .config import round_open
    from .crop_test import describe_failure, job_succeeded, judge, review_request, rework_request
    from .models import (
        ADVISERS,
        DEBUGGER,
        RERUNNERS,
        AssessmentWritten,
        AutodevLook,
        CropSet,
        DatasetState,
        DebugResult,
        GalleryResult,
        JobArgs,
        JobDone,
        JudgeResult,
        LabelEvent,
        LockTaken,
        MatchResult,
        Pins,
        PrUpdate,
        Reservation,
        Revocation,
        RoundLimits,
        SmokeResult,
        Test,
        VerifyResult,
        job_key,
        set_id,
    )

GO, APPROVE, REJECT, REVOKE = "data:go", "data:approve", "data:reject", "data:revoke"
JUDGE = "judge"  # the crop judge as the author of a verdict
JUDGE_NAME = "the crop judge"  # ... and as a timeline entry names it
CANCEL = "data:cancel"
NO_ROUND = "there is no round in progress to cancel"
ROUND_ENDED = "the round ended before the cancel reached it"
CANCEL_PATIENCE = timedelta(minutes=15)  # how long a cancel waits for the round's activity in flight to answer
NOTHING_TO_REVOKE = "nothing to revoke: this dataset has no saved crops"
REVOKED = "revoked by {actor}: the dataset row is `revoked`, {count} artifact{plural} quarantined; running it again repairs it"
BUSY = "busy, ignored: a round is already running. Cancel it first, or ask again once it has ended."
NOT_REVIEW = "an approve or a reject counts only while the dataset is at `review`, for the set under review."
DEBUG_NEXT = (
    "A debugging agent looks into this in {minutes} minutes and, if a new round can pass, runs one. Nothing is "
    "needed from you; *Run again* skips the wait and the debugger."
)
DEBUGGED = "This round was the re-run after debugging; nothing more is tried automatically."
LOOK_EVERY = timedelta(minutes=15)  # how often a round that waits for autodev looks at the snippets issue itself
GO_PATIENCE = timedelta(minutes=30)  # how long the debugger's own go may take to arrive as a signal
MAX_ROUNDS_PER_EXECUTION = 20  # roll the history over between rounds at the latest after this many
DEVELOP = ("developing", "testing", "pr-ready", "merging")  # the stages spent following a snippet PR
NOT_AUTHORIZED = "approve recorded as {id}, but saving the crops is not authorized: {why}"
PAUSED = "paused by the operator (autonomy.pause); `mia resume`, then run it again"
RAW_LOCK_TTL_S = 900  # a job lock lapses this long after its last heartbeat (await_job beats every poll)


STOPPED = ("failed", "blocked", "needs-human")  # a round that stopped; `needs-human` is a stop of unrecorded kind
# activity errors a person has to resolve: the same run would stop on them again
BLOCKING = ("Conflict", "HostsNotAllowed")


class RoundFailed(Exception):
    """The round cannot continue. `blocked`: a person has to decide or change something before a new round can
    pass this point (a pause, a budget, a refused record); otherwise something failed."""

    def __init__(self, message: str, blocked: bool = False) -> None:
        super().__init__(message)
        self.blocked = blocked


def stop_stage(error: BaseException) -> str:
    """The stage a round stops in on `error`: `blocked` when a person has to resolve it first, else `failed`."""
    if isinstance(error, RoundFailed):
        return "blocked" if error.blocked else "failed"
    while isinstance(error, ActivityError) and error.cause:
        error = error.cause
    return "blocked" if getattr(error, "type", None) in BLOCKING else "failed"


def _rollover_due() -> bool:
    return workflow.info().is_continue_as_new_suggested()


def _head_link(pr: PrUpdate) -> str:
    """A snippet PR's head as an entry names it: a link to that commit in the PR, the bare sha of a PR with no url."""
    return f"[`{pr.head_sha[:7]}`]({pr.url}/commits/{pr.head_sha})" if pr.url else f"`{pr.head_sha}`"


def _parse_time(iso: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp as UTC; None if missing or malformed."""
    if iso is None:
        return None
    try:
        parsed = datetime.fromisoformat(iso)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@workflow.defn(name="MiaDataDataset")
class MiaDataDataset:
    """One workflow per dataset issue, forever: each new `go` runs a round (match → crops → review →
    approve → publish those crops) that ends in a dataset stage (done, failed or blocked), then waits for the next
    `go`. One set of crops per round (spec §13): the crops a human validated are the crops published. Size is no
    verdict of this pipeline: a small raw is cropped whole, and a buffer plan decides what fits it (spec §12)."""

    @workflow.init
    def __init__(self, state: DatasetState) -> None:
        self.state = state  # durable: carried into continue_as_new
        self.go_events: list[LabelEvent] = []  # signals not yet acted on
        self.approve_events: list[LabelEvent] = []
        self.reject_events: list[LabelEvent] = []
        self.revoke_events: list[LabelEvent] = []
        self.cancel_events: list[LabelEvent] = []
        self.restarts: set[str] = set()  # ids of the gos that came as `restart`: each starts its round in a new run
        # a cancel in progress (see _cancel_round): the round starts no new activity, the one in flight answers
        self.cancelling = False
        self.unwinding = False  # the round's task has been told to stop: what it runs from here on is clean-up
        self.in_flight = 0  # the round's activities in flight (clean-up not counted)
        self.round_task: asyncio.Future[None] | None = None
        # the crop judge while it looks beside the round's task (see _judged), and whether the round went on to a
        # person's label before it ended: its verdict is then on record only
        self.judging: asyncio.Future[JudgeResult | None] | None = None
        self.verdict_first = False
        self.unkilled: list[str] = []  # jobs a cancel could not kill, each with the reason
        self.limits: RoundLimits | None = None  # read from config at the start of a round
        self.stage_write: Any = None  # the handle of a stage write still running beside the round (see _stage)
        self.failure: str | None = None  # why the round just failed, while its debugging cycle is still to run

    @workflow.signal
    def go(self, event: LabelEvent) -> None:
        self.go_events.append(event)

    @workflow.signal
    def restart(self, event: LabelEvent) -> None:
        """A go whose round starts in a new run of this workflow (see `run`). A run keeps the code paths it began
        with for as long as it lives (its patches), so only a new run does everything today's code does. At
        `review` it also ends the round that waits there, whichever path made its crops."""
        self.restarts.add(event.event_id)
        self.go_events.append(event)

    @workflow.signal
    def approve(self, event: LabelEvent) -> None:
        self.approve_events.append(event)

    @workflow.signal
    def reject(self, event: LabelEvent) -> None:
        self.reject_events.append(event)

    @workflow.signal
    def revoke(self, event: LabelEvent) -> None:
        """Withdraw this dataset's saved crops (spec autonomous-gates §6); taken at any stage."""
        self.revoke_events.append(event)

    @workflow.signal
    def cancel(self, event: LabelEvent) -> None:
        """End the round in progress now (spec webapp-actions §5): see `_guard`. With no round running it is
        refused."""
        self.cancel_events.append(event)

    @workflow.signal
    def pr_update(self, update: PrUpdate) -> None:
        """A wake-up for the develop path. The workflow re-reads the PR rather than trusting the signal."""
        if update not in self.state.pending:
            self.state.pending = (self.state.pending + [update])[-50:]

    @workflow.query
    def status(self) -> DatasetState:
        return self.state

    @workflow.run
    async def run(self, state: DatasetState) -> None:
        rounds = 0
        worked = True  # whether this run has carried a round yet: one it started, or one it took over
        if state.restart:  # rolled over for this go: its round is the run's first step
            event, state.restart = state.restart, None
            await self._guard(self._round(event, restarted=True))
        elif state.stage == "review" and state.tests and not state.pr:  # rolled over while waiting for a verdict
            await self._guard(self._from_review())
        elif (state.stage in DEVELOP or state.stage == "review") and state.snippet_issue:  # ... or following the PR
            await self._guard(self._follow_pr())
        else:
            worked = False
        while True:
            await workflow.wait_condition(lambda: bool(self._labels() or self.cancel_events) or self._roll_due())
            if not (self._labels() or self.cancel_events):
                await self._rollover()  # idle, nothing queued
            await self._refuse_cancels()  # idle: no round to cancel
            await self._revokes()  # a revoke counts at any stage
            await self._refuse_queued(gos=False)  # idle: a verdict has no round under review to apply to
            if not self.go_events:
                continue
            event = self.go_events.pop(0)
            if self._seen(event):  # redelivery
                continue
            restarted = event.event_id in self.restarts
            if restarted and worked and not (self._labels() or self.cancel_events):
                state.restart = event  # this run has carried a round: the next one starts in a new run
                await self._rollover()
            rounds += 1
            worked = True
            await self._guard(self._round(event, restarted))
            if (rounds >= MAX_ROUNDS_PER_EXECUTION or self._roll_due()) and not (self._labels() or self.cancel_events):
                await self._rollover()

    # --- helpers ---------------------------------------------------------------------------

    async def _act(
        self,
        name: str,
        *args: Any,
        seconds: int = 60,
        budget: timedelta = timedelta(hours=6),
        result_type: Any = None,
        cleanup: bool = False,
    ) -> Any:
        """Run an activity with a per-attempt timeout and an overall retry budget.

        Transient failures retry until the budget runs out; activities raise a non-retryable
        ApplicationError for failures that a retry cannot fix.

        While a cancel is in progress the round's task starts no new activity: it is stopped here instead (the
        round's own clean-up, `cleanup` or anything after it was told to stop, goes through). An activity
        already in flight is left to answer, so what it did (a job submitted, a lock taken, a reservation
        opened) is known and undone; `_cancel_round` waits for that. The crop judge's task (`_judged`) is waited
        for the same way and stopped after the round's."""
        task = asyncio.current_task()
        of_round = task is self.round_task and not self.unwinding and not cleanup
        if of_round and self.cancelling:
            self.unwinding = True
            raise asyncio.CancelledError
        of_round = of_round or (task is self.judging and not cleanup)  # counted in flight, stopped by _judge_ended
        self.in_flight += of_round
        try:
            return await workflow.execute_activity(
                name,
                args=list(args),
                start_to_close_timeout=timedelta(seconds=seconds),
                schedule_to_close_timeout=budget,
                retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
                result_type=result_type,
            )
        except ActivityError as error:
            if self.cancelling and isinstance(error.cause, CancelledError) and not cleanup:
                # the cancel landed in the activity after all (the wait for it ran out): never a failed
                # activity to a best-effort caller, which would carry the round on
                raise asyncio.CancelledError from error
            raise
        finally:
            self.in_flight -= of_round

    def _roll_due(self) -> bool:
        """Whether to continue as new now: never with a cancel queued, which a new run would not hear, and never
        while the crop judge still looks, which a new run would not wait for."""
        return _rollover_due() and not self.cancel_events and not (self.judging and not self.judging.done())

    async def _limits(self) -> RoundLimits:
        """The round's config values. They are not part of the durable state, so a rollover re-reads them."""
        if self.limits is None:
            self.limits = await self._act("limits", result_type=RoundLimits)
        return self.limits

    async def _guard(self, step: Coroutine[Any, Any, None]) -> None:
        """Run a step of the round; any failure stops the round (`stop_stage`) instead of failing the workflow.

        The step runs as a task so a `cancel` can end it wherever it is waiting (a job, a lock, a verdict, a PR):
        see `_cancel_round` (spec webapp-actions §5)."""
        self.cancel_events = [event for event in self.cancel_events if not self._seen(event)]
        for stale in self.cancel_events:  # sent before this round began: it was not aimed at it
            await self._refuse(CANCEL, NO_ROUND, stale)
        self.cancel_events = []
        round_task = self.round_task = asyncio.ensure_future(step)
        cancel = asyncio.ensure_future(workflow.wait_condition(lambda: bool(self.cancel_events)))
        try:
            await workflow.wait([round_task, cancel], return_when=asyncio.FIRST_COMPLETED)
            if round_task.done():
                await round_task  # re-raises what ended it: a failure below, or a continue-as-new
            else:
                await self._cancel_round(round_task)
        except (ActivityError, RoundFailed) as error:
            await self._finish(stop_stage(error), error_message(error))
        except asyncio.CancelledError:  # the workflow itself is being cancelled: let the round clean up
            self.unwinding = True
            round_task.cancel()
            await workflow.wait([round_task])
            raise
        finally:
            cancel.cancel()
            self.round_task, self.cancelling, self.unwinding = None, False, False
        await self._judge_ended()  # a judge a person's verdict overtook: the round closes when its record is written
        await self._settle_stage_write(cancel=False)  # quiet point: the last stage write lands before a rollover
        if (failure := self.failure) is not None:  # the step failed with a debugging cycle left: that runs now
            self.failure = None
            await self._guard(self._debug(failure))

    async def _cancel_round(self, round_task: asyncio.Future[None]) -> None:
        """End the round a `cancel` was sent for, leaving nothing of it behind.

        From now on the round starts no activity (`_act`). The one in flight is left to answer, so that a job it
        submitted, a lock it took or a reservation it opened is known: the round's task then stops at its next
        step and cleans up as it unwinds. A task that is waiting (a job, a lock, a verdict, a snippet PR) is
        cancelled there. Its cluster job is killed first, then its reservation settled and its raw lock
        released (`_job`): one writer per raw cache holds until the job is dead. A job that cannot be killed
        makes the dataset `failed`, saying so, never `cancelled` over a job that runs on."""
        events, self.cancel_events = self.cancel_events, []
        self.cancelling, self.unkilled = True, []
        if self.in_flight:
            with contextlib.suppress(TimeoutError):  # after this long the cancel goes in anyway (see _act)
                await workflow.wait_condition(lambda: not self.in_flight or round_task.done(), timeout=CANCEL_PATIENCE)
        if not round_task.done() and not self.unwinding:  # waiting somewhere: stop it there
            self.unwinding = True
            round_task.cancel()
        await workflow.wait([round_task])
        await self._judge_ended(stop=True)  # while `cancelling`: its job is killed like the round's (`_job`)
        self.cancelling = self.unwinding = False
        if not round_task.cancelled() and round_task.exception() is None:
            # the round reached its end by itself while the cancel was on its way: its stage stands
            for event in events:
                if not self._seen(event):
                    await self._refuse(CANCEL, ROUND_ENDED, event)
            return
        for event in events:
            self._seen(event)
        if not round_task.cancelled():
            workflow.logger.warning(
                "issue #%d: the cancelled round ended with: %s", self.state.issue, round_task.exception()
            )
        if self.unkilled:
            jobs = "; ".join(self.unkilled)
            return await self._finish(
                "failed",
                f"cancelled by {events[-1].actor}, but a job could not be killed and may still be running: {jobs}",
                debug=False,  # a person ended this round
            )
        await self._finish("cancelled", f"cancelled by {events[-1].actor}")

    async def _kill(self, job_id: str) -> None:
        """Kill a job of a round being cancelled; a failure is kept for the round's last word, never raised."""
        try:
            await self._act("kill_job", job_id, budget=timedelta(minutes=30), cleanup=True)
        except ActivityError as error:
            self.unkilled.append(f"{job_id}: {error_message(error)}")

    async def _refuse_cancels(self) -> None:
        """A cancel that arrived with no round in progress (a click that crossed the round's end): refuse it."""
        events, self.cancel_events = self.cancel_events, []
        for event in events:
            if not self._seen(event):
                await self._refuse(CANCEL, NO_ROUND, event)

    def _labels(self) -> bool:
        """Whether any label signal is queued."""
        return bool(self.go_events or self.approve_events or self.reject_events or self.revoke_events)

    async def _revokes(self) -> bool:
        """Act on every queued `data:revoke`; True when one ended the round (a refused label leaves it alone)."""
        ended = False
        while self.revoke_events:
            event = self.revoke_events.pop(0)
            if self._seen(event):
                continue
            try:
                ended = await self._revoke(event) or ended
            except (ActivityError, RoundFailed) as error:
                await self._finish(stop_stage(error), error_message(error))
                ended = True
            await self._settle_stage_write(cancel=False)
        return ended

    async def _revoke(self, event: LabelEvent) -> bool:
        """Withdraw the dataset: the activity writes the reject assessments, quarantines the staged artifacts and
        flips the row; the dataset ends `blocked` naming the blast radius. Returns whether the round ended: an
        issue that never published anything (no round, or no manifest on disk) only has the label refused."""
        state = self.state
        if not workflow.patched("l-revoke"):
            return False
        if not state.round_id:
            await self._refuse(REVOKE, NOTHING_TO_REVOKE, event)
            return False
        out = await self._act(
            "revoke_dataset",
            state.issue,
            state.round_id,
            event.actor,
            event.event_id,
            seconds=900,
            result_type=Revocation,
        )
        if not out.assessment:
            await self._refuse(REVOKE, NOTHING_TO_REVOKE, event)
            return False
        artifacts = ", ".join(f"`{artifact}`" for artifact in out.artifacts) or "none"
        buffers = ", ".join(f"`{buffer}`" for buffer in out.buffers) or "none"
        extra = f"**Quarantined artifacts:** {artifacts}\n**Buffers that drew from it:** {buffers}\n"
        count = len(out.artifacts)
        detail = REVOKED.format(actor=event.actor, count=count, plural="" if count == 1 else "s")
        await self._finish("blocked", detail, extra=extra)
        return True

    def _seen(self, event: LabelEvent) -> bool:
        """True if this label event was handled before; otherwise record it as handled now."""
        if event.event_id in self.state.handled:
            return True
        self.state.handled = (self.state.handled + [event.event_id])[-50:]
        return False

    async def _stage(
        self, stage: str, detail: str = "", budget: timedelta = timedelta(hours=6), extra: str = "", wait: bool = False
    ) -> None:
        """Move the dataset to `stage`: `set_stage` writes the registry row, the subject's stage and its entry.

        Every stage write is awaited (`stage-write-awaited`; spec webapp-actions §6): the registry's stage is what
        buffer-api checks an action against and what the page shows as waiting on a person, so it may not lag
        the round. The write is a call to buffer-api. A history without that patch replays
        `stage-write-async`: a stage inside a round runs beside it and only a final stage (`wait`) is
        awaited."""
        await self._write_stage(stage, detail, budget, extra, wait)
        if self._results():
            self._pass_begun(stage)

    def _results(self) -> bool:
        """Whether this run writes a result entry for each stage pass (`stage-results`; spec job-records §3.6): the
        state carries the open pass, and the stage write carries its result out."""
        return workflow.patched("stage-results")

    def _pass_begun(self, stage: str) -> None:
        """The stage was written, and with it the result of the pass that was open: a stage the round works or
        waits at opens the next pass, a stage the dataset rests at leaves none open."""
        state = self.state
        if round_open(stage):
            state.passes = {**state.passes, stage: state.passes.get(stage, 0) + 1}
        state.pass_stage = stage if round_open(stage) else ""
        state.pass_jobs, state.result, state.result_binding = [], "", False

    def _found(self, text: str, binding: bool = False) -> None:
        """What the open pass found, for its result entry; the next stage write, or `_flush`, writes it."""
        if self._results():
            self.state.result, self.state.result_binding = text, binding

    async def _flush(self) -> None:
        """Write the open pass's result now, where no stage write follows that would carry it. Never raises: a
        result that could not be written is logged."""
        state = self.state
        if not state.pass_stage:
            return
        try:
            await self._act("stage_result", state.issue, state)
        except ActivityError as error:
            workflow.logger.warning(
                "issue #%d: the result of %s was not written: %s", state.issue, state.pass_stage, error_message(error)
            )
        state.pass_stage, state.pass_jobs, state.result, state.result_binding = "", [], "", False

    async def _write_stage(self, stage: str, detail: str, budget: timedelta, extra: str, wait: bool) -> None:
        """The `set_stage` call of `_stage`, awaited or beside the round as the run's patches say."""
        self.state.stage = stage
        args = [self.state.issue, stage, self.state, detail, extra]
        if not workflow.patched("stage-write-async"):
            return await self._act("set_stage", *args, budget=budget)
        await self._drop_stage_write()
        if wait or workflow.patched("stage-write-awaited"):
            return await self._act("set_stage", *args, budget=budget)
        self.stage_write = workflow.start_activity(
            "set_stage",
            args=args,
            start_to_close_timeout=timedelta(seconds=60),
            schedule_to_close_timeout=budget,
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
            # stage-write-wait: a cancelled write already inside its registry call finishes before the next write
            # starts, so an older stage never lands after a newer one
            cancellation_type=(
                workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED
                if workflow.patched("stage-write-wait")
                else workflow.ActivityCancellationType.TRY_CANCEL
            ),
        )

    async def _rollover(self) -> None:
        """Continue as new with the durable state, once the pending stage write has landed (a new run has no handle)."""
        await self._settle_stage_write(cancel=False)
        workflow.continue_as_new(self.state)

    async def _drop_stage_write(self) -> None:
        """Cancel the stage write still pending, so a late retry cannot write the older stage back, and wait for
        an attempt already inside its registry call to finish; a write that failed meanwhile is logged (the next
        write carries the stage)."""
        await self._settle_stage_write(cancel=True)

    async def _settle_stage_write(self, cancel: bool) -> None:
        handle, self.stage_write = self.stage_write, None
        if handle is None:
            return
        if cancel:
            handle.cancel()
        try:
            await handle
        except ActivityError as error:
            if not isinstance(error.cause, CancelledError):
                workflow.logger.warning("issue #%d: stage write failed: %s", self.state.issue, error_message(error))

    async def _refuse_queued(self, gos: bool = True, approves: bool = True) -> None:
        """Every label queued now arrived while it couldn't count: answer each new one with refuse_label."""
        queued: list[tuple[str, str, LabelEvent]] = []
        if approves:  # both verdicts
            queued = [(APPROVE, NOT_REVIEW, event) for event in self.approve_events]
            queued += [(REJECT, NOT_REVIEW, event) for event in self.reject_events]
            self.approve_events, self.reject_events = [], []
        if gos:
            queued, self.go_events = queued + [(GO, BUSY, event) for event in self.go_events], []
        for label, reason, event in queued:
            if not self._seen(event):
                await self._refuse(label, reason, event)

    async def _refuse(self, label: str, reason: str, event: LabelEvent) -> None:
        try:
            await self._act("refuse_label", self.state.issue, label, reason, event.event_id)
        except ActivityError as error:  # best effort: the event is handled either way
            workflow.logger.warning(
                "issue #%d: could not refuse %s %s: %s", self.state.issue, label, event.event_id, error_message(error)
            )

    async def _finish(self, stage: str, detail: str = "", extra: str = "", debug: bool = True) -> None:
        """End the round in a dataset stage, then refuse whatever queued up while it ran. Never raises.

        A round that failed goes to `debugging` instead while a debugging cycle is left (`debug`: unless the
        caller rules that out): `_guard` then runs `_debug`, which ends in a new round or in a stop stage."""
        if stage == "failed" and debug and await self._debuggable():
            self.failure = detail
            minutes = (await self._limits()).debug_wait_min
            if self._results() and self.state.pass_stage:  # the pass that failed says so in its own result
                self._found(
                    "\n\n".join(part for part in (self.state.result, f"**failed**: {detail}") if part),
                    self.state.result_binding,
                )
                stage, detail = "debugging", DEBUG_NEXT.format(minutes=minutes)
            else:
                stage, detail = "debugging", f"{detail}\n\n{DEBUG_NEXT.format(minutes=minutes)}"
        elif stage == "failed" and self.state.debugs:
            detail = f"{detail}\n\n{DEBUGGED}"
        try:
            await self._stage(stage, detail, budget=timedelta(hours=24), extra=extra, wait=True)
        except ActivityError as error:
            workflow.logger.error("issue #%d: could not set %s: %s", self.state.issue, stage, error_message(error))
        await self._refuse_queued()

    async def _debuggable(self) -> bool:
        """Whether the round that just failed gets a debugging cycle: it was admitted (a debugger's job needs its
        pins) and `limits.debug_cycles` are not used up since the last go that was not the debugger's."""
        state = self.state
        if not (state.pins and state.round_id) or not workflow.patched("auto-debug"):
            return False
        try:
            return state.debugs < (await self._limits()).debug_cycles
        except ActivityError:
            return False

    async def _go_within(self, patience: timedelta) -> bool:
        """Wait up to `patience` for a go; whether one is queued."""
        with contextlib.suppress(TimeoutError):
            await workflow.wait_condition(lambda: bool(self.go_events), timeout=patience)
        return bool(self.go_events)

    async def _debug(self, failure: str) -> None:
        """One debugging cycle of a round that failed with `failure`, at stage `debugging`: wait
        `limits.debug_wait_min`, run the debugging agent over what the round's jobs left, and act on its outcome.
        `rerun`: its go starts a new round, whose match agent reads its advice. `blocked`, or a debugger that did
        not finish: the round stops there, with the failure and what the debugger said. A go that arrives
        meanwhile (a person's *Run again*) ends the cycle at once: that round is what runs next."""
        state = self.state
        assert state.round_id and state.pins  # _debuggable
        state.debugs += 1
        if await self._go_within(timedelta(minutes=(await self._limits()).debug_wait_min)):
            return
        try:
            workspace = await self._act(
                "prepare_debug",
                state.issue,
                state.round_id,
                state.pins,
                failure,
                state.binding,
                seconds=600,
                result_type=str,
            )
            job = await self._job("debug", workspace)
            result = await self._act(
                "read_debug", state.issue, state.round_id, workspace, seconds=300, result_type=DebugResult
            )
        except (ActivityError, RoundFailed) as error:
            if self.go_events:
                return
            why = f"{failure}\n\nThe debugging agent did not run: {error_message(error)}"
            return await self._finish(stop_stage(error), why, debug=False)
        if self.go_events:
            return
        if result.outcome == "failed":
            why = result.diagnosis if job_succeeded(job) else describe_failure("debug", job)
            return await self._finish("failed", f"{failure}\n\nThe debugging agent did not finish: {why}", debug=False)
        found = f"**Debugger:** {result.diagnosis}"
        if result.outcome == "blocked":
            needs = f"\n\n**What it needs:** {result.advice}" if result.advice else ""
            return await self._finish("blocked", f"{failure}\n\n{found}{needs}", debug=False)
        try:
            if self._results():
                self._found(f"{found}\n\nA new round follows, with the debugger's advice for its match agent.")
                await self._flush()
            else:
                await self._act("comment", state.issue, found, f"debug {state.round_id}")
            await self._act(
                "debug_go", state.issue, state.round_id, result.advice, seconds=120, budget=timedelta(minutes=30)
            )
        except ActivityError as error:
            why = f"{failure}\n\nThe debugger's new round could not be asked for: {error_message(error)}"
            return await self._finish("failed", why, debug=False)
        if not await self._go_within(GO_PATIENCE):
            await self._finish("failed", f"{failure}\n\nThe debugger's new round did not start.", debug=False)

    async def _prepare(self, kind: str) -> str:
        """Make the workspace for a smoke/crop/verify job. Returns its path."""
        state = self.state
        return await self._act(
            "prepare_job", state.issue, state.round_id, kind, 1, state.binding, seconds=300, result_type=str
        )

    async def _job(
        self,
        kind: str,
        workspace: str,
        workspace_kind: str | None = None,
        lock: list[str] | None = None,
        own_entry: bool = False,
        **job_args: Any,
    ) -> JobDone:
        """Submit a job (at most once per key) and wait for it on the await queue.

        The wait is its own activity on a separate queue so a worker restart reattaches to the running job.
        `workspace_kind` distinguishes several jobs of one kind in a round (crop-<sha>); it defaults to `kind`.
        `lock` ([kind, key, owner], from _hold_raw) is heartbeated by the wait while the job runs.
        The open pass's result links the job's record, unless the caller writes an entry of its own that reports
        the job and links it (`own_entry`): a record is linked once, by the entry that says what the job found."""
        state = self.state
        assert state.round_id and state.pins  # a round is admitted before any job runs
        key = job_key(state.issue, state.round_id, workspace_kind or kind, 1)
        limits = await self._limits()
        budgeted = workflow.patched("l-budget")
        if budgeted:  # the operator's switches first: the pause, then the root budget (spec autonomous-gates §8)
            agent = limits.agent_max_budget_usd
            usd = {"match": agent, "debug": agent, "judge": limits.judge_max_budget_usd}.get(kind, 0.0)
            await self._reserve(key, kind, usd)
        walltime = timedelta(minutes=limits.walltime_min)
        job_id = None
        try:
            job_id = await self._act(
                "submit",
                key,
                kind,
                JobArgs(workspace, state.round_id, state.pins, **job_args),
                seconds=600,
                result_type=str,
            )
            if self._results() and not own_entry and workspace not in state.pass_jobs:
                state.pass_jobs = [*state.pass_jobs, workspace]  # the pass's result links the job's record
            done = await workflow.execute_activity(
                "await_job",
                args=[
                    job_id,
                    workspace,
                    *([lock] if lock else []),
                ],  # the lock is a third argument only when there is one
                result_type=JobDone,
                task_queue=workflow.info().task_queue + "-await",
                start_to_close_timeout=walltime + QUEUE_SLACK,
                schedule_to_close_timeout=walltime + timedelta(hours=24),
                heartbeat_timeout=timedelta(minutes=3),
                retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
            )
        except (ActivityError, asyncio.CancelledError) as error:
            if self.cancelling and job_id:  # first the job, then its reservation; the caller's lock comes last
                await self._kill(job_id)
            if budgeted:
                await self._settle(key)
            if self.cancelling:  # never a failed job to a best-effort caller (the crop judge), which would go on
                self.unwinding = self.unwinding or asyncio.current_task() is not self.judging
                raise asyncio.CancelledError from error
            raise
        if budgeted:  # the crop job is the one that downloads: its bytes count against the root's max_fetch_bytes
            await self._settle(key, done.bytes_fetched if kind == "crop" else 0, done.cost_usd)
        return done

    async def _reserve(self, key: str, kind: str, usd: float) -> None:
        """The pause, then the root budget (spec autonomous-gates §8). A cap refusal ends the round; the concurrency
        cap alone is a wait: poll_minutes, doubling up to 30 minutes, then ask again."""
        state, tries = self.state, 0
        assert state.pins
        while True:
            if await self._act("paused", result_type=bool):
                raise RoundFailed(PAUSED, blocked=True)
            reservation = await self._act("reserve", state.pins.root, key, kind, usd, result_type=Reservation)
            if reservation.ok:
                return
            if not reservation.wait:
                raise RoundFailed(f"root budget: {reservation.reason}", blocked=True)
            poll_minutes = (await self._limits()).poll_minutes
            await workflow.sleep(timedelta(minutes=min(poll_minutes * 2**tries, 30)))
            tries += 1

    async def _settle(self, key: str, fetched_bytes: int = 0, usd: float | None = None) -> None:
        """Close the job's reservation with what it cost (None when nobody knows: a job that never ended, or no
        agent's) and downloaded; an accounting write that fails is logged, never the round's end."""
        assert self.state.pins
        try:
            await self._act(
                "settle", self.state.pins.root, key, usd, fetched_bytes, budget=timedelta(minutes=10), cleanup=True
            )
        except ActivityError as error:
            workflow.logger.warning("issue #%d: settle %s failed: %s", self.state.issue, key, error_message(error))

    async def _hold_raw(self, kind: str) -> list[str]:
        """Take the lock on the round record's raw cache for a `kind` (smoke, crop) job that writes it, waiting while
        another job holds it; the issue says so once per round. Returns the lock as [kind, key, owner]."""
        state = self.state
        assert state.round_id and state.pins  # a round is admitted before any job runs
        record, owner, commented, tries = state.pins.record, f"mia-data {state.round_id} {kind}", False, 0
        while not (
            taken := await self._act("take_lock", "raw", record, owner, RAW_LOCK_TTL_S, result_type=LockTaken)
        ).ok:
            if not commented:
                waiting = f"waiting for the raw cache of `{record}`: held by {taken.holder}"
                await self._act("comment", state.issue, waiting, f"lock {state.round_id}")
                commented = True
            # ponytail: poll_minutes, doubling each try up to 30 min (~20 history events an hour once backed off);
            # a wait of many days wants a wake-up signal from the releasing worker.
            poll_minutes = (await self._limits()).poll_minutes
            await workflow.sleep(timedelta(minutes=min(poll_minutes * 2**tries, 30)))
            tries += 1
        return ["raw", record, owner]

    async def _release_raw(self, lock: list[str] | None) -> None:
        """Release a lock _hold_raw took; never raises: a lock left behind lapses after its TTL."""
        if lock is None:
            return
        try:
            await self._act("release_lock", *lock, budget=timedelta(minutes=30), cleanup=True)
        except ActivityError as error:
            workflow.logger.warning("issue #%d: could not release %s: %s", self.state.issue, lock, error_message(error))

    async def _crop(
        self,
        tag: str,
        sha: str,
        n_crops: int,
        seed: str,
        batch: int,
        trusted: bool = True,
        existing: bool | None = None,
    ) -> tuple[str, str, VerifyResult, JobDone, JobDone]:
        """Run crop → verify (+ preview) → read_verify. Returns (crop workspace, verify workspace, verify
        result, crop job, verify job).

        The workspaces are crop-<tag> and verify-<tag>; `tag` is the tested SHA, or "production". The jobs are
        returned so the caller can judge their exit codes; a job whose state is uncertain fails the round.
        `trusted=False` (a PR head: unreviewed code) mounts the dataset cache read-only. A trusted crop job writes the
        cache, so it holds the record's raw lock from before the crop job until after read_verify. `existing`
        (default: batch > 0) points the jobs at the dataset's published manifest so a later round adds crops."""
        assert self.state.pins  # a round is admitted before any job runs
        existing = batch > 0 if existing is None else existing
        lock = await self._hold_raw("crop") if trusted and workflow.patched("c-raw-lock") else None
        try:
            crop_workspace = await self._prepare(f"crop-{tag}")
            crop_job = await self._job(
                "crop",
                crop_workspace,
                f"crop-{tag}",
                lock,
                sha=sha,
                n_crops=n_crops,
                seed=seed,
                batch=batch,
                trusted=trusted,
                existing=existing,
            )
            if crop_job.state not in ("DONE", "EXIT"):
                raise RoundFailed(describe_failure("crop", crop_job))
            verify_workspace = await self._prepare(f"verify-{tag}")
            verify_job = await self._job(  # mounts no cache; the wait keeps the lock alive until it is released
                "verify", verify_workspace, f"verify-{tag}", lock, crop_workspace=crop_workspace, existing=existing
            )
            if verify_job.state not in ("DONE", "EXIT"):
                raise RoundFailed(describe_failure("verify", verify_job))
            verify = await self._act(
                "read_verify",
                verify_workspace,
                crop_workspace,
                *([self.state.pins.record] if lock else []),  # with the lock: put the raw asset row
                result_type=VerifyResult,
            )
        finally:
            await self._release_raw(lock)
        return crop_workspace, verify_workspace, verify, crop_job, verify_job

    async def _report(self, report: str, marker: str) -> str | None:
        """Say what a test of the crops found: it is the result of the `testing` pass (written with the round's
        binding by the stage write that follows, or by `_flush`); a run without `stage-results` posts it as an
        entry of its own."""
        state = self.state
        if self._results():
            self._found(report, binding=True)
            return None
        return await self._act("comment", state.issue, report, marker, "data", state.binding, state.binding_hash)

    async def _gallery(
        self, tag: str, crop_workspace: str, verify_workspace: str, verify: VerifyResult
    ) -> GalleryResult:
        state = self.state
        return await self._act(
            "publish_gallery",
            state.issue,
            state.round_id,
            tag,
            crop_workspace,
            verify_workspace,
            verify.crops,
            seconds=300,
            result_type=GalleryResult,
        )

    async def _test(
        self, sha: str, seed: str, trusted: bool = True, notes: tuple = (), n_crops: int | None = None
    ) -> tuple[Test, list[str], str]:
        """Make crops at `sha`, judge them and publish their gallery. With `n_crops` this is the dataset's own set
        (spec §13: the crops a human validates and the round publishes; at least half must verify, and the jobs
        dedup against a published manifest); without, a pre-§14 PR head's test crops (`test_n`, every one must
        verify), replayed only.

        Returns (the Test record, descriptions of failed jobs, the markdown report for the issue)."""
        state = self.state
        n = n_crops or (await self._limits()).test_n
        crop_workspace, verify_workspace, verify, crop_job, verify_job = await self._crop(
            sha, sha, n, seed, 0, trusted, existing=bool(n_crops)
        )
        if n_crops:
            n_effective = n if verify.n_effective is None else min(n, verify.n_effective)
            needed = max(1, math.ceil(n_effective / 2))
            passed, failed_jobs = judge(verify, [crop_job], needed=needed)
        else:
            needed = None
            passed, failed_jobs = judge(verify, [crop_job, verify_job])
        if trusted and not passed and verify.host_not_allowed:  # a host waits for a person's approval
            raise RoundFailed(
                f"{describe_failure('crop', crop_job, verify.crop_errors)} "
                f"(host not allowed: {verify.host_not_allowed})",
                blocked=True,
            )
        gallery = await self._gallery(sha, crop_workspace, verify_workspace, verify) if verify.crops else None
        test = Test(
            sha,
            f"{state.round_id}-{sha[:12]}",
            passed,
            workflow.now().isoformat(),
            gallery.prefix if gallery else None,
            crop_workspace=crop_workspace,
            verify_workspace=verify_workspace,
        )
        lines = [
            f"{'Crops' if n_crops else 'Test crops'} at `{sha}`: **{'passed' if passed else 'failed'}**, "
            f"{verify.ok_count} of {verify.n} verified"
            + (f" (at least {needed} needed)" if needed else "")
            + f"; the crop job fetched {verify.bytes_fetched} bytes.",
            *failed_jobs,
            *(f"- {error}" for error in verify.errors),
            *notes,
            "",
            gallery.table if gallery else "",
        ]
        return test, failed_jobs, "\n".join(lines)

    # --- the round -------------------------------------------------------------------------

    async def _round(self, event: LabelEvent, restarted: bool = False) -> None:
        """One round from a `go`: admit, match, test crops, then wait for review.

        After a develop round stopped, a person's `go` resumes its snippet PR instead when that PR is still
        open (see _resume_pr); a `go` with advice (ADVISERS) matches, so its advice is read. The snippets issue is
        remembered while its PR is open or not yet opened, so a retry never opens a second one; once that PR
        was merged or closed, a round that needs a reader opens a new issue."""
        state = self.state
        snippet_issue, snippet_pr = None, None  # what the previous round's develop path left behind, if anything
        # (a cancelled develop round left its snippet issue and PR open too: autodev is not asked twice)
        # (a restart at review leaves them open as well)
        left = (*STOPPED, "cancelled", "debugging", *(("review",) if restarted else ()))
        if event.actor != DEBUGGER:
            state.debugs = 0  # somebody asked for this round: it has its own debugging cycles
        if event.actor not in RERUNNERS:
            state.reworks = 0  # ... and the crop judge may send its sets back anew (spec judge-decides §4.3)
        if state.stage in left and state.snippet_issue and workflow.patched("resume-snippet-pr"):
            snippet_issue, snippet_pr = state.snippet_issue, state.pr
        if self._results() and state.pass_stage:  # left open by the round before: a restart at review, a go
            self._found(state.result or "Ended: a new round was started.")  # while the debugger was still to run
            await self._flush()
        state.round_id, state.seed = f"i{state.issue}-e{event.event_id}", str(workflow.random().getrandbits(64))
        state.pins = state.binding = state.binding_hash = state.snippet_name = state.snippet_issue = state.pr = None
        state.tests, state.attempts, state.pending, state.waiting_since = [], {}, [], None
        state.passes, state.pass_stage, state.pass_jobs, state.result, state.result_binding = {}, "", [], "", False
        try:
            state.pins = await self._act("admit", state.issue, event.event_id, seconds=300, result_type=Pins)
        except ActivityError as error:
            if isinstance(error.cause, ApplicationError) and error.cause.non_retryable:
                # a record admission refuses (not in the catalog, no folder, another record's folder) is refused
                # again until someone changes it
                raise RoundFailed(error_message(error), blocked=True) from error
            raise
        self.limits = await self._act("limits", result_type=RoundLimits)
        if workflow.patched("l-budget") and await self._act("paused", result_type=bool):
            raise RoundFailed(PAUSED, blocked=True)
        # a go with advice (the debugger's, the cleaner's after a diagnosis) starts from the match, whose agent
        # reads the advice: any other go resumes the PR
        advised = bool(snippet_issue and snippet_pr and event.actor in ADVISERS) and workflow.patched(
            "debugger-go-matches"
        )
        if snippet_issue and snippet_pr and not advised:
            if await self._resume_pr(snippet_issue, snippet_pr):
                return
            snippet_issue = None  # its PR was merged or closed: autodev is done there, a reader is asked for anew
        await self._stage("matching")

        # Match: the agent picks a snippet and writes a binding; the worker's crop job is what tests that binding.
        workspace = await self._act(
            "prepare_match", state.issue, state.round_id, state.pins, 1, seconds=600, result_type=str
        )
        if not job_succeeded(job := await self._job("match", workspace)):
            return await self._finish("failed", describe_failure("match", job))
        match = await self._act("read_match", workspace, seconds=300, result_type=MatchResult)
        no_size_verdict = workflow.patched(
            "h-no-eligibility"
        )  # a history without the patch carries an `ineligible` outcome
        if match.outcome == "needs_snippet":
            fails = (match.draft or {}).get("what_fails")
            self._found("No existing reader snippet fits this dataset" + (f": {fails}" if fails else "."))
            if advised and snippet_issue and snippet_pr:
                if await self._resume_pr(snippet_issue, snippet_pr):
                    return  # the reader it asks for is the open PR's: its head is tested, as a person's go would
                snippet_issue = None  # as for a person's go
            return await self._develop(match.draft or {}, snippet_issue)
        if match.outcome not in (("matched",) if no_size_verdict else ("matched", "ineligible")):
            stopped = "failed" if match.outcome == "failed" else "blocked"  # the agent says which of the two
            return await self._finish(stopped, f"match {stopped}: {match.reason}")
        state.binding, state.binding_hash = match.binding_path, match.binding_hash
        reasons = "\n".join(f"- {reason}" for reason in match.reasons)
        self._found(
            "Matched: an existing reader snippet fits." + (f" The agent's reasons:\n{reasons}" if reasons else "")
        )
        if not workflow.patched("crop-checks-binding") and await self._smoke(match, no_size_verdict):
            return  # a history without the patch ran a smoke job first

        # The round's crops at the matched snippets commit, shown on the issue for review; approved, they are published.
        sha = state.pins.snippets_commit
        await self._stage("testing")
        test, failed_jobs, report = await self._test(
            sha,
            state.seed + "-t",
            notes=("", "Binding reasons:", *(f"- {reason}" for reason in match.reasons)),
            n_crops=(await self._limits()).per_round,
        )
        state.tests.append(test)
        await self._report(report, f"test {state.round_id} {sha}")
        if not test.ok:
            return await self._finish("failed", "; ".join(["the crops failed; nothing is saved", *failed_jobs]))
        await self._stage("review")
        await self._refuse_queued(approves=False)  # gos that came while the round ran; verdicts are judged below
        # after the gate opens, and a person's verdict that comes while the judge looks is acted on at once
        judged = await self._judged(test)
        if self._judge_grants(judged):
            test.approved_at, test.approved_by = workflow.now().isoformat(), JUDGE
            self._found("Approved by the crop judge.")
            return await self._publish_set()
        if judged and self._sends_back(judged) and await self._send_back(test, judged):
            return  # the round went on as a develop round and ended there
        await self._from_review()

    async def _smoke(self, match: MatchResult, no_size_verdict: bool) -> bool:
        """A smoke job on the matched binding, before the crops; True when it ended the round. Replayed only: a
        history without the `crop-checks-binding` patch ran one."""
        state = self.state
        assert state.pins  # a round is admitted before any job runs
        if workflow.patched("c-raw-lock"):  # smoke writes the record's raw cache: under its lock
            lock = await self._hold_raw("smoke")
            try:
                workspace = await self._prepare("smoke")
                job = await self._job("smoke", workspace, lock=lock)
                ran = job_succeeded(job)
                if ran:
                    smoke = await self._act("read_smoke", workspace, state.pins.record, result_type=SmokeResult)
            finally:
                await self._release_raw(lock)
            if not ran:
                return await self._stopped("failed", await self._smoke_failure(job, workspace))
        else:
            workspace = await self._prepare("smoke")
            if not job_succeeded(job := await self._job("smoke", workspace)):
                return await self._stopped("failed", await self._smoke_failure(job, workspace))
            smoke = await self._act("read_smoke", workspace, result_type=SmokeResult)
        if not smoke.ok:
            why = "; ".join(smoke.errors) or "no errors reported"
            return await self._stopped(
                "blocked" if smoke.host_not_allowed else "failed",  # a host waits for a person's approval
                f"smoke failed: {why}"
                + (f" (host not allowed: {smoke.host_not_allowed})" if smoke.host_not_allowed else ""),
            )
        if not no_size_verdict:  # only a history without the patch reads smoke's `eligible`
            if smoke.eligible != (match.outcome == "matched"):
                return await self._stopped(
                    "failed",
                    f"the match agent says {match.outcome}, but smoke found the data "
                    f"{'eligible' if smoke.eligible else 'ineligible'}",
                )
            if not smoke.eligible:
                return await self._stopped("ineligible", f"no raw volume is at least 256×256; shapes: {smoke.shapes}")
        return False

    async def _stopped(self, stage: str, detail: str) -> bool:
        """End the round in `stage`; True, for a caller that reports whether the round ended."""
        await self._finish(stage, detail)
        return True

    async def _smoke_failure(self, job: JobDone, workspace: str) -> str:
        """Why a smoke job failed: its end state and the errors of its own smoke.json, when it wrote one. `harness
        smoke` exits 1 whenever `ok` is false, so the exit code alone told the issue nothing."""
        if not workflow.patched("job-report-errors"):
            return describe_failure("smoke", job)
        smoke = await self._act("read_smoke", workspace, result_type=SmokeResult)
        errors = [error for error in smoke.errors if not error.startswith("smoke.json is missing")]
        if smoke.host_not_allowed:
            errors.append(f"host not allowed: {smoke.host_not_allowed}")
        return describe_failure("smoke", job, errors)

    async def _from_review(self) -> None:
        """At `data:review` without a PR to follow: approve publishes the set; reject asks autodev for a snippet
        superseding the binding's, with the reviewer's comments, and the round goes on as a develop round
        (spec §14); a new go ends the round and starts the next from matching."""
        kind, event = await self._review()
        state = self.state
        subject = ("crop_set", f"{state.round_id}:{state.tests[-1].test_id}")
        if kind == "approve" and event:
            written = await self._assess("approve", event, subject)
            if not written.authorized:
                return await self._finish("blocked", NOT_AUTHORIZED.format(id=written.id, why=written.reason))
            state.tests[-1].approved_at, state.tests[-1].approved_by = event.at, event.actor
            self._found(f"Approved by {event.actor}.")
            await self._publish_set()
        elif kind == "reject" and event:
            await self._assess("reject", event, subject)
            self._found(f"Rejected by {event.actor}: autodev is asked for a snippet that takes the comments in.")
            feedback = await self._feedback(event)
            if workflow.patched("judge-decides"):  # the set stays acceptable, and the request can be answered
                held = self._hold(state.tests[-1], event.actor, f"#{event.event_id}")
                held.opened_issue = state.snippet_issue is None
                feedback = review_request(feedback, held.request)
            await self._develop({"what_fails": feedback}, state.snippet_issue, binding=state.binding)

    async def _judged(self, test: Test) -> JudgeResult | None:
        """The crop judge's verdict on a passing set, or None as soon as a person's label is queued
        (`verdict-first`): the round acts on the label at once, and the judge goes on beside it to the end of its
        record, where it decides nothing (`_judge_crops` reads its verdict as shadow). The round closes when that
        is written (`_guard`), and a cancel stops it (`_cancel_round`). A history without the patch waits for the
        judge first."""
        if not workflow.patched("verdict-first"):
            return await self._judge_crops(test)
        await self._judge_ended()  # an earlier head's judge still looks: one at a time
        self.verdict_first = False
        judging = self.judging = asyncio.ensure_future(self._judge_crops(test))
        await workflow.wait_condition(lambda: judging.done() or self._labels())
        if not judging.done():
            self.verdict_first = True
            return None
        self.judging = None
        return judging.result()  # or what ended the judge: a refusal at an active gate, a cancel

    async def _judge_ended(self, stop: bool = False) -> None:
        """Wait for the crop judge that looks beside the round, if one does; `stop` ends it now, for a cancel: its
        job is killed and its reservation settled (`_job`). Its verdict is on record only by now, so what it
        raised is logged, never the round's failure."""
        judging = self.judging
        if judging is None:
            return
        if stop:
            judging.cancel()
        await workflow.wait([judging])
        self.judging = None
        if not judging.cancelled() and (error := judging.exception()) is not None:
            workflow.logger.warning("issue #%d: the crop judge ended with: %s", self.state.issue, error)

    async def _judge_crops(self, test: Test) -> JudgeResult | None:
        """The crop judge over a passing set (spec autonomous-gates §4.2, §9): evidence, the job, the assessment,
        one comment. Off, or a run from before `m-crop-judge`: nothing. In shadow nothing here changes the round's
        course: a failed job or an unreadable verdict is an `escalate` on record and a comment, a refused reservation
        (the pause, the root budget) skips the judge with a comment, and the human label decides."""
        if not workflow.patched("m-crop-judge"):
            return None
        limits = await self._limits()
        if limits.crop_judge == "off" or not (test.crop_workspace and test.verify_workspace):
            return None
        state = self.state
        assert state.round_id
        marker, failed, workspace = f"judge crops {state.round_id} {test.sha}", "", ""
        try:
            verify = await self._act(
                "read_verify", test.verify_workspace, test.crop_workspace, result_type=VerifyResult
            )
            workspace = await self._act(
                "prepare_judge",
                state.issue,
                state.round_id,
                "crops",
                test.crop_workspace,
                test.verify_workspace,
                verify.crops,
                1,
                test.sha[:12],
                seconds=600,
                result_type=str,
            )
            try:
                # the judge's entry (read_judge's comment) links the job's record
                job = await self._job("judge", workspace, workspace_kind=f"judge-{test.sha[:12]}", own_entry=True)
            except RoundFailed as refused:  # the operator's switches: the judge is skipped, the round goes on
                if limits.crop_judge != "shadow":
                    raise
                await self._act(
                    "comment",
                    state.issue,
                    f"The crop judge was skipped: {refused}. A person's verdict decides.",
                    marker,
                )
                return None
            if not job_succeeded(job):
                failed = describe_failure("judge", job)  # read_judge still records the escalate
            result = await self._act(
                "read_judge",
                state.issue,
                state.round_id,
                workspace,
                "crops",
                "crop_set",
                f"{state.round_id}:{test.test_id}",
                "shadow" if self.verdict_first else limits.crop_judge,  # a person decided: the judge grants nothing
                seconds=300,
                result_type=JudgeResult,
            )
        except ActivityError as error:
            if self._results() and workspace and workspace not in state.pass_jobs:
                state.pass_jobs = [*state.pass_jobs, workspace]  # no judge's entry: review's result links the job
            await self._act(
                "comment",
                state.issue,
                f"The crop judge did not run to the end: {error_message(error)}. A person's verdict decides.",
                marker,
            )
            return None
        body = (
            f"The crop judge's {failed}; its verdict is recorded as an escalation.\n\n" if failed else ""
        ) + result.comment
        await self._act("comment", state.issue, body, marker)
        return result

    def _judge_grants(self, judged: JudgeResult | None) -> bool:
        """Whether the crop judge's verdict approves the set under review with no person: the gate is active and
        the policy took its approve (`authorized`), and nobody's verdict, go or revoke came while it looked. A
        reject, an escalate and a judge that did not run leave the set to a person."""
        return bool(judged and judged.authorized and not self._labels())

    def _sends_back(self, judged: JudgeResult) -> bool:
        """Whether the crop judge's verdict sends the set under review back for rework with no person (spec
        judge-decides §3): the gate is active, the judge found a `suspect` or a `bad` crop (the policy's
        `reject`), and nobody's verdict, go or revoke came while it looked."""
        if not workflow.patched("judge-decides") or self.limits is None or self.limits.crop_judge != "active":
            return False
        return judged.effective == "reject" and not self.verdict_first and not self._labels()

    async def _send_back(self, test: Test, judged: JudgeResult) -> bool:
        """The crop judge sends `test`'s set back: its findings go to the developer as a person's reject would,
        and the set is held so that a person can still accept it (spec judge-decides §4.1, §5.1). False when the
        judge has done so `limits.max_reworks` times since a person last acted: the set then waits at review
        (§4.3)."""
        state, limits = self.state, await self._limits()
        if state.reworks >= limits.max_reworks:
            await self._act(
                "comment",
                state.issue,
                f"The crop judge found {judged.worst or 'doubtful'} crops and has sent this dataset back "
                f"{state.reworks} time(s) already (`judge.crops.max_reworks` is {limits.max_reworks}): the set "
                "waits for your verdict.",
                f"judge limit {state.round_id} {test.sha}",
            )
            return False
        state.reworks += 1
        held = self._hold(test, JUDGE, f"#{state.reworks}")
        held.worst = judged.worst
        held.findings = [finding for finding in judged.findings if finding.get("severity") != "ok"]
        feedback = rework_request(held.findings, held.request)
        self._found(
            f"Sent back by the crop judge, which found {judged.worst or 'doubtful'} crops: the developer is asked "
            "to look into its findings."
        )
        if state.pr is None:  # a matched binding's set: a snippet superseding the binding's
            held.opened_issue = state.snippet_issue is None
            await self._develop({"what_fails": feedback}, state.snippet_issue, binding=state.binding)
            return True
        state.revised_at = workflow.now().isoformat()
        await self._act(
            "comment", state.pr, f"/revise {feedback}", f"revise {state.round_id} judge {test.sha}", "snippets"
        )
        await self._stage(
            "developing",
            "the crop judge's findings went to the snippet PR as a revision request; the next head's crops are "
            "judged again",
        )
        return True

    async def _assess(self, verdict: str, event: LabelEvent, subject: tuple[str, str]) -> AssessmentWritten:
        """Record a human verdict as an assessment (spec autonomous-gates §5) and ask whether the subject is
        authorized; a run from before `l-assess` takes the label alone as the authorization."""
        if not workflow.patched("l-assess"):
            return AssessmentWritten("", True)
        state, (kind, ref) = self.state, subject
        return await self._act(
            "assess",
            state.issue,
            state.round_id,
            "crops",
            kind,
            ref,
            verdict,
            f"human:{event.actor}",
            f"data:{verdict} by {event.actor}",
            None,  # supersedes nothing
            event.event_id,  # the action: what the person wrote with the verdict is read from it
            seconds=300,
            result_type=AssessmentWritten,
        )

    async def _feedback(self, event: LabelEvent) -> str:
        """The notes of the rejects since the set's report and up to this one (spec §14)."""
        state = self.state
        return await self._act(
            "read_feedback", state.issue, state.tests[-1].finished_at, event.at, seconds=300, result_type=str
        )

    async def _publish_set(self, test: Test | None = None, sha: str | None = None) -> None:
        """Publish the set that was validated: the round's own crops (spec §13), read back from their workspaces;
        `test` is the approved PR head's on the develop path (spec §14), else the round's last, and `sha` the
        mia-snippets commit to publish under (the merge commit for a PR head's crops). A history without
        `one-crop-set`, or a Test record without workspaces, takes the production path instead."""
        state = self.state
        test = test or state.tests[-1]
        if not (workflow.patched("one-crop-set") and test.crop_workspace and test.verify_workspace):
            return await self._produce()
        verify = await self._act("read_verify", test.verify_workspace, test.crop_workspace, result_type=VerifyResult)
        published = await self._act(
            "publish_crops",
            state.issue,
            state.round_id,
            test.crop_workspace,
            verify.crops,
            sha or test.sha,
            state.binding,
            seconds=300,
            result_type=dict,
        )
        state.published = True
        if workflow.patched("judge-decides"):  # the saved set outlives its round; a set sent back is superseded
            state.saved, state.held = self._set_of(test), None
        extra = (
            f"**Totals:** {len(published['published'])} crops saved this round ({verify.ok_count} verified); "
            f"{published['total']} in the dataset\n"
        )
        await self._finish("done", extra=extra)

    def _hold(self, test: Test, by: str, suffix: str) -> CropSet:
        """`test`'s set was sent back for rework by `by` (the judge, or a person's actor): it is kept as the
        dataset's held set, which an approve can still name (spec judge-decides §5.1), under a request id the
        developer answers."""
        held = self.state.held = self._set_of(test)
        held.sent_back_by, held.request = by, f"{held.set_id}{suffix}"
        return held

    def _restore(self, held: CropSet) -> None:
        """The held set's round becomes the state's again: what a verdict on that set acts on (its crops, its
        binding, its pull request) is the set's own, whatever a later round or head left in the state."""
        state = self.state
        state.round_id, state.pins, state.binding, state.binding_hash = (
            held.round_id,
            held.pins,
            held.binding,
            held.binding_hash,
        )
        state.pr, state.snippet_issue, state.tests = held.pr, held.snippet_issue, [held.test]

    async def _approve_held(self, by: str, at: str, said: str, found: str) -> None:
        """The held set is approved by `by`, an author that is not a person (`policy:developer`: the developer
        looked into the judge's doubts and found the crops right, spec judge-decides §4.2): the approval is
        written as an assessment, and the set is saved, or its pull request's head merged, as after a person's
        approve. `said` is the assessment's summary, `found` what the timeline says of it. A person's verdict on
        the set, whenever it was given, outranks it (`authorized`)."""
        state, held = self.state, self.state.held
        assert held is not None  # the caller checked
        kind, ref = ("pr_head", held.test.sha) if held.pr else ("crop_set", held.set_id)
        written = await self._act(
            "assess",
            state.issue,
            held.round_id,
            "crops",
            kind,
            ref,
            "approve",
            by,
            said,
            seconds=300,
            result_type=AssessmentWritten,
        )
        if not written.authorized:
            return await self._finish("blocked", NOT_AUTHORIZED.format(id=written.id, why=written.reason))
        self._restore(held)
        if held.pr:
            return await self._pr_approved(held.test, "the developer's answer to the crop judge", at, by, found)
        held.test.approved_at, held.test.approved_by = at, by
        self._found(found)
        await self._publish_set(held.test)

    def _set_of(self, test: Test) -> CropSet:
        """The round's set `test` with what a later verdict on it needs (spec judge-decides §5.4)."""
        state = self.state
        assert state.round_id and state.pins  # a set is made by an admitted round
        return CropSet(
            set_id(state.round_id, test),
            state.round_id,
            test,
            state.pins,
            state.binding,
            state.binding_hash,
            state.pr,
            state.snippet_issue,
        )

    async def _produce(self) -> None:
        """Pre-I (spec §13): a second, production set after the approve. Replayed by histories that hold one; no
        new round takes this path. Passing crops stay published even when the round ends `failed`."""
        state = self.state
        assert state.pins and state.seed  # set at admission; carried in the durable state across rollovers
        sha = state.pins.snippets_commit
        await self._stage("cropping")
        n_crops = (await self._limits()).per_round
        crop_workspace, verify_workspace, verify, crop_job, _ = await self._crop(
            "production", sha, n_crops, state.seed + "-p", 1
        )
        # verify's exit is not checked: harness verify exits 1 whenever a crop fails; its report decides
        published = await self._act(
            "publish_crops",
            state.issue,
            state.round_id,
            crop_workspace,
            verify.crops,
            sha,
            state.binding,
            seconds=300,
            result_type=dict,
        )
        state.published = True
        table = (
            (await self._gallery("production", crop_workspace, verify_workspace, verify)).table if verify.crops else ""
        )
        n_effective = n_crops if verify.n_effective is None else min(n_crops, verify.n_effective)
        needed = max(1, math.ceil(n_effective / 2))
        extra = (
            f"{table}\n**Totals:** {len(published['published'])} new crops saved this round "
            f"({verify.ok_count} of {n_effective} passed); {published['total']} in the dataset\n"
        )
        if not job_succeeded(
            crop_job
        ):  # a limit or a crash: the surviving crops are published, the round needs a human
            return await self._finish(
                "failed",
                f"{describe_failure('crop', crop_job, verify.crop_errors)}; "
                f"the {verify.ok_count} crops that passed are saved",
                extra,
            )
        if verify.ok_count >= needed:
            return await self._finish("done", extra=extra)
        await self._finish(
            "failed",
            f"only {verify.ok_count} of {n_effective} crops passed (at least {needed} "
            f"needed); the ones that passed are saved",
            extra,
        )

    def _tested_at(self) -> datetime:
        """When the set under review was made; a verdict counts only if it is newer."""
        return _parse_time(self.state.tests[-1].finished_at) or workflow.now()  # unparseable: nothing older counts

    async def _verdict(self) -> tuple[str, LabelEvent] | None:
        """The oldest approve or reject newer than the set under review, as ("approve" | "reject", event); older
        ones are refused as stale, and any queued behind the one taken as not-in-review."""
        tested_at, found = self._tested_at(), None
        queued = [("approve", event) for event in self.approve_events] + [
            ("reject", event) for event in self.reject_events
        ]
        self.approve_events, self.reject_events = [], []
        for kind, event in sorted(queued, key=lambda pair: pair[1].at):  # buffer-api's ISO-8601 UTC strings sort
            if self._seen(event):
                continue
            label = APPROVE if kind == "approve" else REJECT
            if found:
                await self._refuse(label, NOT_REVIEW, event)
            elif (at := _parse_time(event.at)) and at > tested_at:
                found = (kind, event)
                self.state.reworks = 0  # a person decided: the judge's send-backs are counted from here
            else:
                await self._refuse(label, f"it is older than the round's test crops; ask again to {kind} them", event)
        return found

    async def _review(self) -> tuple[str, LabelEvent | None]:
        """Wait at review for a verdict newer than the set (see _verdict), or a new go: ("go", None)."""
        while True:
            await workflow.wait_condition(lambda: self._labels() or self._roll_due())
            if not self._labels():
                await self._rollover()  # stage review: the next run resumes here
            if await self._revokes():
                return "revoked", None  # the round ended `blocked`; nothing under review any more
            if verdict := await self._verdict():
                return verdict
            self.go_events = [go for go in self.go_events if go.event_id not in self.state.handled]
            if self.go_events:
                return "go", None

    # --- the development path ---------------------------------------------------------------

    async def _develop(self, draft: dict, snippet_issue: int | None = None, binding: str | None = None) -> None:
        """Open the mia-snippets issue (autodev writes the snippet), then follow its PR: no snippet fits (`draft`
        is the match agent's), or, with `binding`, a reviewer rejected that binding's crops and `draft` carries
        their comments: the issue asks for a snippet superseding the binding's (spec §14).

        `snippet_issue` is the issue an earlier round opened for this dataset, when the workflow still knows it:
        the activity reuses it while it is open rather than asking autodev for the same snippet twice."""
        state = self.state
        extra: list[Any] = [snippet_issue, binding] if binding else ([snippet_issue] if snippet_issue else [])
        state.snippet_issue = await self._act(
            "open_snippet_issue", state.issue, state.round_id, draft, *extra, seconds=300, result_type=int
        )
        if state.held and state.held.opened_issue and state.held.snippet_issue is None:
            state.held.snippet_issue = state.snippet_issue  # the issue this send-back opened: an accept closes it
        source = "the crop judge's findings" if state.held and state.held.sent_back_by == JUDGE else "your comments"
        what = f"a snippet superseding the binding's, from {source}," if binding else "the snippet"
        await self._stage(
            "developing",
            f"autodev is writing {what} in mia-snippets issue #{state.snippet_issue}; each PR head's crops come "
            "back here for review",
        )
        await self._follow_pr()
        if state.stage == "review" and not state.pr and state.held:
            # the developer answered that nothing needs changing (`_answered`): the same set waits for a verdict
            await self._from_review()

    async def _resume_pr(self, snippet_issue: int, pr_number: int) -> bool:
        """A `go` after a develop round stopped: pick its snippet PR up again if it is still open,
        and test the current head afresh under the new round and pins (spec §7.4, "comment and re-trigger").

        Returns False when the PR was merged or closed meanwhile, or is not this snippets issue's: the round
        then matches as usual (a merged snippet is on main now) and no longer goes back to that issue."""
        state = self.state
        pr = await self._act("pr_state", pr_number, result_type=PrUpdate)
        if pr.state != "open" or pr.snippet_issue != snippet_issue:
            return False
        state.snippet_issue, state.pr = snippet_issue, pr_number
        await self._stage(
            "testing",
            f"resuming snippet PR {pr.url or f'#{pr.pr}'} from the previous round; its current head gets test crops again",
        )
        if not await self._on_pr(pr_number, pr):
            await self._follow_pr()
        return True

    async def _follow_pr(self) -> None:
        """The develop round's one wait: wake on pr_update and on labels until the round ends. At `review` (the
        PR head's set passed) an approve or reject is the verdict (spec §14) and a `restart` ends the round; any
        other label is refused (a plain go is busy throughout). Rolls over only here, idle.

        Away from `review` the round waits for autodev, and that wait is timed: every LOOK_EVERY it looks at the
        snippets issue itself (see _look), so a signal that never came cannot hold it."""
        state = self.state
        await self._limits()
        loop = workflow.patched("review-loop")
        timed = workflow.patched("autodev-look")
        while True:
            looks = timed and state.stage not in ("review", "pr-ready")  # there a person is awaited
            if looks and not state.waiting_since:
                state.waiting_since = workflow.now().isoformat()
            try:
                await workflow.wait_condition(
                    lambda: bool(state.pending) or self._labels() or self._roll_due(),
                    timeout=LOOK_EVERY if looks else None,
                )
            except TimeoutError:
                if await self._look():
                    return
                continue
            if not (state.pending or self._labels()):
                await self._rollover()  # the develop state is all in `state`: the next run resumes here
            state.waiting_since = None  # something came: the quiet starts over
            if await self._revokes():
                return  # the round stopped
            if loop and state.stage == "review" and state.pr and (verdict := await self._verdict()):
                kind, event = verdict
                await (self._approve_pr(event) if kind == "approve" else self._revise_pr(event))
            if state.stage == "review" and any(go.event_id in self.restarts for go in self.go_events):
                return  # a restart of the set under review: the round ends here, and `run` starts the next
            await self._refuse_queued()
            while state.pending:
                if await self._on_pr(state.pending.pop(0).pr):
                    return

    async def _look(self) -> bool:
        """The round looks at autodev itself; True when that ended the round. A PR is acted on as it is now (a
        head, a merge or a stop the round was not told of). A revision request autodev answered without a new
        head goes back to review (`_answered`). Otherwise the round ends `blocked`, with autodev's
        last comment, when autodev stopped before it opened a PR, or when `limits.autodev_quiet_hours` passed
        with nothing from it and no agent round running."""
        state = self.state
        look = await self._act("autodev_look", state.snippet_issue, state.pr, result_type=AutodevLook)
        before = (state.stage, len(state.tests))
        if look.pr and await self._on_pr(look.pr):
            return True
        if await self._answered(look):
            # the answer may have ended the round (a matched set was saved), or ended this wait for autodev (a
            # matched set is back at review, where `_from_review` waits for the verdict)
            return state.stage in (*STOPPED, "done") or (state.stage == "review" and not state.pr)
        where = f"mia-snippets issue #{state.snippet_issue}" + (f" (PR #{look.pr})" if look.pr else "")
        said = f' Its last comment: "{look.last_word}"' if look.last_word else ""
        if look.stopped and not look.pr:
            await self._finish("blocked", f"{where}: autodev stopped before it opened a pull request.{said}")
            return True
        now, hours = workflow.now(), (await self._limits()).autodev_quiet_hours
        if look.working or not state.waiting_since or (state.stage, len(state.tests)) != before:
            state.waiting_since = now.isoformat()  # at work, or the look itself found something to do
        elif now - datetime.fromisoformat(state.waiting_since) >= timedelta(hours=hours):
            await self._finish("blocked", f"{where}: nothing from autodev for {hours:g} h.{said}")
            return True
        return False

    async def _answered(self, look: AutodevLook) -> bool:
        """Whether autodev answered a request for rework without changing the snippet, and it is at rest. Then no
        new head will come, and the answer is this pass's result.

        What counts as an answer depends on who asked and how (spec judge-decides §4.2). A person's revision
        request on a pull request is answered by any round of autodev's that made no commits after it. The crop
        judge's findings, and any request on an issue with no pull request, are answered only by autodev's
        validated "nothing needs changing" for that very request (`look.no_change`): a round with no commits
        alone says nothing about the crops.

        What follows: a set the judge doubted (`suspect` findings only) is approved, as the developer found it
        right; a set with a `bad` finding, and any set a person rejected, goes back to review with the answer,
        for a person's verdict."""
        state = self.state
        if look.working or look.stopped or state.stage != "developing" or not (state.tests and state.tests[-1].ok):
            return False
        held = state.held if workflow.patched("judge-decides") else None
        validated = bool(held and held.request and look.no_change == held.request)
        judged = bool(held and held.sent_back_by == JUDGE)
        if state.pr:
            asked, answered = _parse_time(state.revised_at), _parse_time(look.answer_at or None)
            if not (asked and answered and answered > asked) or (judged and not validated):
                return False
        elif not validated:
            return False
        state.revised_at = None
        quoted = "\n".join(f"> {line}" for line in look.answer.splitlines())
        if held and judged and held.worst == "suspect":
            found = (
                "The developer looked into the crop judge's findings and changed nothing in the snippet: the "
                f"crops are approved as they are. Its answer:\n\n{quoted}"
            )
            said = "the developer found the crops right as they are"
            await self._approve_held("policy:developer", workflow.now().isoformat(), said, found)
            return True
        who = "the crop judge's findings" if judged else "your comments"
        self._found(f"autodev changed nothing in the snippet. Its answer to {who}:\n\n{quoted}")
        detail = (
            "the crop judge found a bad crop and the developer says the crops are right as they are; the set "
            "waits for your verdict"
            if judged
            else "autodev answered your comments without changing the snippet; the same crops wait for your verdict"
        )
        extra = ""
        if state.pr:
            pr = await self._act("pr_state", state.pr, result_type=PrUpdate)
            extra = f"**Snippet PR:** {pr.url or f'#{pr.pr}'}\n"
        else:
            state.snippet_issue = None  # that issue is answered and done with: a further reject opens a new one
        await self._stage("review", detail, extra=extra)
        return True

    async def _approve_pr(self, event: LabelEvent) -> None:
        """The reviewer approved the PR head's set: `/approve <head>` tells autodev to merge it. The merge wakes
        the round (_merged), which publishes these very crops when the merged snippet is the reviewed one."""
        test = self.state.tests[-1]
        written = await self._assess("approve", event, ("pr_head", test.sha))
        if not written.authorized:
            return await self._finish("blocked", NOT_AUTHORIZED.format(id=written.id, why=written.reason))
        await self._pr_approved(test, event.actor, event.at)

    async def _pr_approved(self, test: Test, by: str, at: str, author: str = "", note: str = "") -> None:
        """The PR head's set is approved, by a person, by the crop judge or (`author`: who the approval is
        recorded as, when `by` only names it for the timeline; `note`: what else the timeline says of it) after
        the developer's answer: autodev is told to merge that head."""
        state = self.state
        test.approved_at, test.approved_by = at, author or (JUDGE if by == JUDGE_NAME else by)
        await self._act("comment", state.pr, f"/approve {test.sha}", f"approve {state.round_id} {test.sha}", "snippets")
        self._found(f"Approved by {by}: the crops at `{test.sha}`." + (f"\n\n{note}" if note else ""))
        await self._stage(
            "merging", f"the crops at `{test.sha}` are approved; autodev merges the snippet PR, then they are saved"
        )

    async def _revise_pr(self, event: LabelEvent) -> None:
        """The reviewer rejected the PR head's set: their comments go to the PR as `/revise`; autodev's next head
        brings its crops back to review."""
        state = self.state
        await self._assess("reject", event, ("pr_head", state.tests[-1].sha))
        state.revised_at = workflow.now().isoformat()
        if workflow.patched("judge-decides"):
            self._hold(state.tests[-1], event.actor, f"#{event.event_id}")  # an approve can still name this set
        feedback = await self._feedback(event)
        await self._act(
            "comment", state.pr, f"/revise {feedback}", f"revise {state.round_id} {event.event_id}", "snippets"
        )
        self._found(f"Rejected by {event.actor}: the comments went to the snippet PR as a revision request.")
        await self._stage(
            "developing",
            "your comments went to the snippet PR as a revision request; the next head's crops come back for review",
        )

    async def _on_pr(self, pr_number: int, pr: PrUpdate | None = None) -> bool:
        """Act on the PR as it is now (never on the signal's copy) until nothing is left to do.

        `pr` is the PR as the caller just read it; otherwise it is read here. Returns True when the round ended
        (merged, closed, needs-human), False when waiting for a new head."""
        state = self.state
        pr = pr or await self._act("pr_state", pr_number, result_type=PrUpdate)
        while pr.snippet_issue == state.snippet_issue:  # else another round's PR
            state.pr = pr.pr
            where = f"snippet PR {pr.url or f'#{pr.pr}'}"
            if pr.needs_human or pr.state == "closed":
                why = "its issue has `snippets:needs-human`" if pr.needs_human else "it was closed without merging"
                await self._finish("blocked", f"{where}: {why}{await self._autodev_said(pr.pr)}")
                return True
            if pr.state == "merged":
                await self._merged(pr)
                return True
            if any(test.sha == pr.head_sha for test in state.tests):
                return False  # tested: its status stands until a new head
            if len(state.tests) >= (await self._limits()).max_pr_revisions:
                await self._finish(
                    "failed", f"{where}: revision limit, {len(state.tests)} heads tested and none merged"
                )
                return True
            pr = await self._test_head(pr)
        return False

    async def _autodev_said(self, pr: int) -> str:
        """Why autodev stopped, as the sentence that ends the round's `blocked` entry: its last comment on the
        snippets issue or the PR (a merge refused for conflicts, a failed round). Empty when it left none, and in
        a history without the `blocked-quotes-autodev` patch."""
        if not workflow.patched("blocked-quotes-autodev"):
            return ""
        look = await self._act("autodev_look", self.state.snippet_issue, pr, result_type=AutodevLook)
        return f'. Its last comment: "{look.last_word}"' if look.last_word else ""

    async def _test_head(self, pr: PrUpdate) -> PrUpdate:
        """Make the dataset's set at an untested PR head with the example binding the PR ships, posting the
        `mia-data/crops` status. Returns the PR as it is after the test: only a passing result for a head that is
        still current moves the stage to `review` (spec §14); a failing one goes to the PR as `/revise`, so autodev
        fixes it. Pre-§14 histories made `test_n` test crops and moved to `pr-ready`."""
        state, head = self.state, pr.head_sha
        state.revised_at = None  # a head came: whatever was asked for, this is autodev's answer
        loop = workflow.patched("review-loop")
        results = self._results()
        if results and state.pass_stage != "testing":  # every head tested is a pass of its own
            if state.pass_stage and not state.result:
                self._found(f"The snippet PR has a new head, {_head_link(pr)}.")
            await self._stage("testing", f"making the sample crops at the snippet PR's head {_head_link(pr)}")
        elif not results and state.stage != "testing":
            await self._stage("testing")
        await self._act("set_crop_status", head, "pending", None)
        try:
            binding = await self._act("adopt_pr_binding", state.round_id, pr.pr, head, seconds=600, result_type=dict)
            if binding["errors"]:
                test = Test(head, f"{state.round_id}-{head[:12]}", False, workflow.now().isoformat())
                report = "\n".join(
                    [
                        f"Test crops at `{head}`: **failed**, the PR's example binding was not adopted:",
                        *(f"- {error}" for error in binding["errors"]),
                    ]
                )
            else:
                state.binding, state.binding_hash, state.snippet_name = (
                    binding["path"],
                    binding["hash"],
                    binding["name"],
                )
                n_crops = (await self._limits()).per_round if loop else None
                test, _, report = await self._test(head, f"{state.seed}-t{head[:7]}", trusted=False, n_crops=n_crops)
                test.tree_sha = await self._act("tree_sha", head, binding["name"])
        except (ActivityError, RoundFailed):
            with contextlib.suppress(ActivityError):  # never leave the required status pending
                await self._act("set_crop_status", head, "error", None, budget=timedelta(minutes=10))
            raise
        state.tests.append(test)
        marker = f"test {state.round_id} {head}"
        pr_report = (
            report if test.ok or not loop else f"/revise the crops at `{head}` failed; make them pass:\n\n{report}"
        )
        pr_url = await self._act("comment", pr.pr, pr_report, marker, "snippets", state.binding, state.binding_hash)
        issue_url = await self._report(report, marker)
        url = pr_url or issue_url  # the commit status links the PR comment
        current = await self._act("pr_state", pr.pr, result_type=PrUpdate)
        await self._act(
            "set_crop_status", head, "success" if test.ok else "failure", url
        )  # the head's own, current or not
        shown = test.ok and current.head_sha == head and current.state == "open"
        if shown:
            await self._stage("review" if loop else "pr-ready", extra=f"**Snippet PR:** {current.url or f'#{pr.pr}'}\n")
        elif results:  # no stage follows: this head's pass ends here, and the next head starts its own
            await self._flush()
        if test.ok:
            # after the status and the stage: the required status never waits on it
            judged = await self._judged(test)
            if self._judge_grants(judged) and shown and loop:
                await self._pr_approved(test, JUDGE_NAME, workflow.now().isoformat())
            elif judged and shown and loop and self._sends_back(judged):
                await self._send_back(test, judged)
        return current

    async def _merged(self, pr: PrUpdate) -> None:
        """The snippet PR merged. Spec §14: when the merged snippet tree is an approved head's, the crops that
        were approved are published under the merge commit; otherwise the merge commit gets its own set, which
        waits at review like a matched binding's. Pre-§14 histories made the set at the merge commit and
        published it without a review."""
        state, merge_sha = self.state, pr.merge_sha
        if not merge_sha:
            raise RoundFailed(f"snippet PR #{pr.pr} is merged but GitHub reports no merge commit")
        assert state.pins and state.seed  # set at admission; carried in the durable state across rollovers
        tree = state.snippet_name and await self._act("tree_sha", merge_sha, state.snippet_name)
        binding = await self._act("adopt_pr_binding", state.round_id, pr.pr, merge_sha, seconds=600, result_type=dict)
        if binding["errors"]:
            return await self._finish(
                "failed", "; ".join(["the merged example binding was not adopted", *binding["errors"]])
            )
        state.binding, state.binding_hash, state.snippet_name = binding["path"], binding["hash"], binding["name"]
        if workflow.patched("review-loop"):
            state.pins, state.pr = dataclasses.replace(state.pins, snippets_commit=merge_sha), None  # followed no more
            approved = next(
                (t for t in reversed(state.tests) if t.approved_at and t.ok and tree and t.tree_sha == tree), None
            )
            self._found(f"The snippet PR merged as `{merge_sha}`.")
            if approved:
                return await self._publish_set(approved, merge_sha)
            await self._stage(
                "testing", "the merged snippet is not the reviewed head's; making its own crops for review"
            )
            test, failed_jobs, report = await self._test(
                merge_sha, f"{state.seed}-t{merge_sha[:7]}", n_crops=(await self._limits()).per_round
            )
            test.tree_sha = tree
            state.tests.append(test)
            await self._report(report, f"test {state.round_id} {merge_sha}")
            if not test.ok:
                return await self._finish(
                    "failed", "; ".join(["the merge commit's crops failed; nothing is saved", *failed_jobs])
                )
            await self._stage("review")
            await self._refuse_queued(approves=False)
            return await self._from_review()
        # Pre-§14: the set is made at the merge commit and published without a review. Pre-I histories skipped the
        # test when the merged tree was the tested one.
        carried = tree and any(test.ok and test.sha == pr.head_sha and test.tree_sha == tree for test in state.tests)
        if workflow.patched("one-crop-set") or not carried:
            if state.stage != "testing":
                await self._stage("testing")
            test, failed_jobs, report = await self._test(
                merge_sha, f"{state.seed}-t{merge_sha[:7]}", n_crops=(await self._limits()).per_round
            )
            test.tree_sha = await self._act("tree_sha", merge_sha, binding["name"])
            state.tests.append(test)
            await self._report(report, f"test {state.round_id} {merge_sha}")
            if not test.ok:
                return await self._finish(
                    "failed",
                    "; ".join(["the merge commit's crops failed; nothing is saved", *failed_jobs]),
                )
        state.pins = dataclasses.replace(state.pins, snippets_commit=merge_sha)
        await self._publish_set()


@workflow.defn(name="MiaDataPoll")
class MiaDataPoll:
    """Run on a schedule: turn snippet PR changes into signals for the dataset workflows."""

    @workflow.run
    async def run(self) -> list[str]:
        return await workflow.execute_activity(
            "poll",
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
            result_type=list[str],
        )


@workflow.defn(name="MiaMirror")
class MiaMirror:
    """Copy one subject's timeline and stage to its GitHub issue (spec webapp-actions §7). buffer-api starts it,
    or signals `kick` when it is already running, each time the subject is written. One pass posts everything
    that is new; a kick that arrives during a pass asks for one more. The pass runs on the mirror's own task
    queue (`<task queue>-mirror`), where one runs at a time and writes slowly, so it may wait long for its turn
    and may sit out a rate limit. It is retried without limit, so GitHub being away only delays the issue; no
    round waits for this workflow."""

    def __init__(self) -> None:
        self.kicks = 0

    @workflow.signal
    def kick(self) -> None:
        self.kicks += 1

    @workflow.run
    async def run(self, kind: str, number: int) -> None:
        answered = -1
        while answered != self.kicks:
            answered = self.kicks
            await workflow.execute_activity(
                "mirror",
                args=[kind, number],
                task_queue=workflow.info().task_queue + "-mirror",
                start_to_close_timeout=timedelta(minutes=30),  # a pass that sits out a 15-minute rate limit ends
                retry_policy=RetryPolicy(maximum_interval=timedelta(minutes=10)),
                result_type=int,
            )
