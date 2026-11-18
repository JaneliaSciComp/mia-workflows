"""MiaDataDataset on Temporal's time-skipping server with every activity mocked, and the poll activity."""

import asyncio
import contextlib
import dataclasses
import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from mia_helpers import FakeGH, make_cfg
from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker

from mia_data import workflows
from mia_data.activities import Activities
from mia_data.models import (
    AssessmentWritten,
    AutodevLook,
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
    Test,
    VerifyResult,
)
from mia_data.workflows import MiaDataDataset

SHA = "a" * 40
PINS = Pins("rec-1", "c" * 40, SHA, "h" * 40, "sha256:x", root="o/data#7")


@pytest.fixture
async def env():
    """One time-skipping server per test: see the fixture in test_buffers_workflows."""
    e = await WorkflowEnvironment.start_time_skipping()
    yield e
    await e.shutdown()


def ev(kind, eid, at="2026-01-01T00:00:00Z", actor="alice"):
    return LabelEvent(kind, eid, actor, at)


def turn(answers, key, default):
    """The next of a key's queued answers (a list; its last one repeats), the one answer, or `default`."""
    queued = answers.get(key, default)
    if not isinstance(queued, list):
        return queued
    return queued.pop(0) if len(queued) > 1 else queued[0]


def shift(iso, seconds):
    return (datetime.fromisoformat(iso) + timedelta(seconds=seconds)).isoformat()


class Mocks:
    def __init__(
        self,
        outcome="matched",
        host_not_allowed=None,
        states=None,
        test_ok=16,
        set_ok=16,
        n_effective=16,
        await_failures=0,
        gate=None,
        gate_on="",
        test_ok_by=None,
        pr_now=None,
        trees=None,
        revisions=5,
        lock_busy=0,
        slow_stages=(),
        paused=False,
        reservations=None,
        assess_authorized=True,
        submit_error=None,
        revocation=None,
        crop_judge="off",
        judge_result=None,
        submit_gate=None,
        kill_error=None,
        admit_error=None,
        debug_cycles=0,
        debug_result=None,
        look=None,
        max_reworks=1,
        verify_gone=False,
        publish_error=None,
    ):
        self.verify_gone = verify_gone  # read_verify finds nothing readable: a set's crops are gone
        self.publish_error = publish_error  # a message: publish_crops fails with it, non-retryable
        self.max_reworks = max_reworks  # judge.crops.max_reworks the limits activity reports
        self.look = look or AutodevLook(None, True, False, "")  # what autodev_look finds (a list: one per look)
        self.debug_cycles = debug_cycles  # limits.debug_cycles the limits activity reports
        self.debug_result = debug_result or DebugResult("rerun", "the host sent 30 KB/s", "bind the first volume only")
        self.admit_error = admit_error  # a message: admit refuses the record with it, non-retryable
        self.submit_gate = submit_gate  # an asyncio.Event: every submit waits for it, as a slow bsub would
        self.kill_error = kill_error  # a message: kill_job fails with it
        self.slow_stages, self.stage_gate = set(slow_stages), asyncio.Event()  # set_stage waits for the gate there
        self.calls: list[tuple[str, tuple]] = []
        self.outcome, self.states = outcome, states or {}
        self.host_not_allowed = host_not_allowed  # the host a trusted crop job was refused, in its report
        self.test_ok, self.set_ok, self.n_effective = test_ok, set_ok, n_effective
        self.crop_jobs: dict[str, JobArgs] = {}  # crop workspace -> its args (trusted = the dataset's set)
        self.await_failures, self.gate, self.gate_on = await_failures, gate, gate_on
        self.test_ok_by, self.pr_now, self.trees, self.revisions = test_ok_by or {}, pr_now, trees or {}, revisions
        self.lock_busy = lock_busy  # how many take_lock calls find the raw cache held by someone else
        self.paused, self.reservations = paused, reservations or {}  # job kind -> Reservation (default ok)
        self.assess_authorized = assess_authorized  # what the policy answers to an approve
        self.submit_error = submit_error  # a message: every submit raises it, non-retryable
        self.revocation = revocation or Revocation("as-r", ["art1"], ["i3-e4"])  # what revoke_dataset answers
        self.crop_judge = crop_judge  # judge.crops.mode the limits activity reports
        self.judge_result = judge_result or JudgeResult(
            "approve", "approve", 0.9, "fine", "as-j", 0.5, "Crop judge: approve"
        )

    def acts(self):
        rec = lambda name, *a: self.calls.append((name, a))  # noqa: E731

        @activity.defn(name="admit")
        async def admit(issue: int, event_id: str) -> Pins:
            rec("admit", event_id)
            if self.admit_error:
                raise ApplicationError(self.admit_error, type="ValueError", non_retryable=True)
            return PINS

        @activity.defn(name="limits")
        async def limits() -> RoundLimits:
            rec("limits")
            return RoundLimits(
                4,
                16,
                120,
                self.revisions,
                crop_judge=self.crop_judge,
                judge_max_budget_usd=3.5,
                debug_cycles=self.debug_cycles,
                max_reworks=self.max_reworks,
            )

        @activity.defn(name="set_stage")
        async def set_stage(
            issue: int, stage: str, state: DatasetState | None = None, detail: str = "", extra: str = ""
        ) -> None:
            if stage in self.slow_stages:
                rec("stage_waits", stage)
                await self.stage_gate.wait()
            if state and state.pass_stage:  # the write carries the open pass's result (spec job-records §3.6)
                rec("result", state.pass_stage, state.result, stage, list(state.pass_jobs), state.result_binding)
            rec("set_stage", stage, detail, extra)

        @activity.defn(name="stage_result")
        async def stage_result(issue: int, state: DatasetState) -> None:
            rec("result", state.pass_stage, state.result, None, list(state.pass_jobs), state.result_binding)

        @activity.defn(name="comment")
        async def comment(
            issue: int,
            body: str,
            marker: str,
            repo: str = "data",
            binding_path: str | None = None,
            binding_hash: str | None = None,
        ) -> str | None:
            rec("comment", body, marker, repo, binding_path)
            return f"https://github.com/o/{repo}/issues/{issue}#{marker}" if repo == "snippets" else None

        @activity.defn(name="refuse_label")
        async def refuse_label(issue: int, label: str, reason: str, event_id: str) -> None:
            rec("refuse_label", label, reason, event_id)

        @activity.defn(name="prepare_match")
        async def prepare_match(issue: int, round_id: str, pins: Pins, attempt: int = 1) -> str:
            rec("prepare_match", round_id)
            return f"/w/{round_id}/match-{attempt}"

        @activity.defn(name="prepare_debug")
        async def prepare_debug(issue: int, round_id: str, pins: Pins, failure: str, binding: str | None) -> str:
            rec("prepare_debug", round_id, failure, binding)
            return f"/w/{round_id}/debug-1"

        @activity.defn(name="read_debug")
        async def read_debug(issue: int, round_id: str, ws: str) -> DebugResult:
            rec("read_debug", ws)
            return self.debug_result

        @activity.defn(name="debug_go")
        async def debug_go(issue: int, round_id: str, advice: str) -> None:
            rec("debug_go", round_id, advice)  # buffer-api's part, the go as a signal, is the test's to send

        @activity.defn(name="prepare_job")
        async def prepare_job(issue: int, round_id: str, kind: str, attempt: int, binding: str | None) -> str:
            rec("prepare_job", kind, binding)
            return f"/w/{round_id}/{kind}-{attempt}"

        @activity.defn(name="submit")
        async def submit(key: str, kind: str, a: JobArgs) -> str:
            rec("submit", key, kind, a)
            if self.submit_error:
                raise ApplicationError(self.submit_error, non_retryable=True)
            if kind == "crop":
                self.crop_jobs[a.workspace] = a
            if self.submit_gate:
                await self.submit_gate.wait()
            return f"job-{key}"

        @activity.defn(name="read_match")
        async def read_match(ws: str) -> MatchResult:
            rec("read_match")
            return MatchResult(
                self.outcome,
                reason="label class undecidable",
                binding_path="/s/binding-1.yaml",
                binding_hash="bh",
                reasons=["axes from conversion.json"],
            )

        @activity.defn(name="take_lock")
        async def take_lock(kind: str, key: str, owner: str, ttl_s: int) -> LockTaken:
            rec("take_lock", kind, key, owner, ttl_s)
            if self.lock_busy:
                self.lock_busy -= 1
                return LockTaken(False, "mia-buffers b1 stage")
            return LockTaken(True)

        @activity.defn(name="release_lock")
        async def release_lock(kind: str, key: str, owner: str) -> None:
            rec("release_lock", kind, key, owner)

        @activity.defn(name="read_verify")
        async def read_verify(ws: str, crop_ws: str | None = None, record: str | None = None) -> VerifyResult:
            rec("read_verify", ws, crop_ws, *([record] if record else []))
            if self.verify_gone:
                raise ApplicationError("no verify report", type="ValueError", non_retryable=True)
            job = self.crop_jobs.get(crop_ws or "")
            trusted = bool(job and job.trusted)  # a matched binding's or the merge commit's set; else a PR head's
            by_sha = next((v for s, v in self.test_ok_by.items() if s in ws), None)  # by tested SHA
            k = by_sha if by_sha is not None else (self.set_ok if trusted else self.test_ok)
            n = job.n_crops if job else 16
            return VerifyResult(
                k,
                n,
                [] if k == n else ["c9: overlaps c1"],
                [{"crop_id": f"c{i}", "files": {}} for i in range(k)],
                self.n_effective if trusted else n,
                bytes_fetched=1234,
                host_not_allowed=self.host_not_allowed if trusted else None,
            )

        @activity.defn(name="publish_gallery")
        async def publish_gallery(
            issue: int, round_id: str, kind: str, crop_ws: str, verify_ws: str, verified: list[dict]
        ) -> GalleryResult:
            rec("publish_gallery", kind, len(verified))
            return GalleryResult(f"galleries/rec-1/{round_id}/{kind}", {}, f"table {kind}")

        @activity.defn(name="publish_crops")
        async def publish_crops(
            issue: int, round_id: str, crop_ws: str, verified: list[dict], sha: str, binding: str | None = None
        ) -> dict:
            rec("publish_crops", crop_ws, len(verified), sha, binding)
            if self.publish_error:
                raise ApplicationError(self.publish_error, type="ValueError", non_retryable=True)
            return {"published": [c["crop_id"] for c in verified], "already": [], "total": len(verified)}

        @activity.defn(name="open_snippet_issue")
        async def open_snippet_issue(
            issue: int, round_id: str, draft: dict, snippet_issue: int | None = None, binding: str | None = None
        ) -> int:
            rec("open_snippet_issue", round_id, snippet_issue, *([binding, draft] if binding else []))
            return 100

        @activity.defn(name="share_evidence")
        async def share_evidence(issue: int, round_id: str, ws: str, snippet_issue: int, name: str) -> None:
            rec("share_evidence", round_id, ws, snippet_issue, name)

        @activity.defn(name="close_snippet_issue")
        async def close_snippet_issue(snippet_issue: int, pr: int | None, why: str) -> None:
            rec("close_snippet_issue", snippet_issue, pr, why)

        @activity.defn(name="kill_job")
        async def kill_job(job_id: str) -> None:
            rec("kill_job", job_id)
            if self.kill_error:
                raise ApplicationError(self.kill_error, non_retryable=True)

        @activity.defn(name="read_feedback")
        async def read_feedback(issue: int, since: str | None, until: str) -> str:
            rec("read_feedback", issue, since, until)
            return "alice wrote:\nthe masks are shifted by one pixel"

        @activity.defn(name="pr_state")
        async def pr_state(pr: int) -> PrUpdate:
            rec("pr_state", pr)
            return self.pr_now

        @activity.defn(name="autodev_look")
        async def autodev_look(snippet_issue: int, pr: int | None) -> AutodevLook:
            rec("autodev_look", snippet_issue, pr)
            if isinstance(self.look, list):
                return self.look.pop(0) if len(self.look) > 1 else self.look[0]
            return self.look

        @activity.defn(name="set_crop_status")
        async def set_crop_status(sha: str, state: str, url: str | None, description: str = "") -> None:
            rec("set_crop_status", sha, state, url)

        @activity.defn(name="tree_sha")
        async def tree_sha(sha: str, name: str) -> str | None:
            rec("tree_sha", sha, name)
            return self.trees.get(sha, "T1")

        @activity.defn(name="paused")
        async def paused() -> bool:
            rec("paused")
            return turn({"p": self.paused}, "p", False)  # a list: answers in turn, the last repeats

        @activity.defn(name="reserve")
        async def reserve(root: str, reservation_id: str, kind: str, usd: float) -> Reservation:
            rec("reserve", root, reservation_id, kind, usd)
            return turn(self.reservations, kind, Reservation(True))

        @activity.defn(name="settle")
        async def settle(root: str, reservation_id: str, usd: float | None = None, fetched_bytes: int = 0) -> None:
            rec("settle", root, reservation_id, usd, fetched_bytes)

        @activity.defn(name="assess")
        async def assess(
            issue: int,
            round_id: str,
            gate: str,
            subject_kind: str,
            subject_ref: str,
            verdict: str,
            by: str,
            summary: str = "",
            supersedes: str | None = None,
            action_id: str | None = None,
        ) -> AssessmentWritten:
            rec("assess", gate, subject_kind, subject_ref, verdict, by, summary, action_id)
            refused = "rejected by human:bob at t: wrong channel"
            return AssessmentWritten("as-1", self.assess_authorized, "" if self.assess_authorized else refused)

        @activity.defn(name="revoke_dataset")
        async def revoke_dataset(issue: int, round_id: str, actor: str, event_id: str) -> Revocation:
            rec("revoke_dataset", round_id, actor, event_id)
            return self.revocation

        @activity.defn(name="prepare_judge")
        async def prepare_judge(
            issue: int,
            round_id: str,
            gate: str,
            crop_ws: str,
            verify_ws: str,
            verified: list[dict],
            attempt: int = 1,
            tag: str = "",
        ) -> str:
            rec("prepare_judge", gate, crop_ws, verify_ws, len(verified), tag)
            return f"/w/{round_id}/judge-{gate}-{tag}-{attempt}"

        @activity.defn(name="read_judge")
        async def read_judge(
            issue: int, round_id: str, ws: str, gate: str, subject_kind: str, subject_ref: str, mode: str
        ) -> JudgeResult:
            rec("read_judge", ws, gate, subject_kind, subject_ref, mode)
            if isinstance(self.judge_result, Exception):
                raise self.judge_result
            return self.judge_result

        @activity.defn(name="adopt_pr_binding")
        async def adopt_pr_binding(round_id: str, pr: int, sha: str) -> dict:
            rec("adopt_pr_binding", pr, sha)
            return {"path": f"/s/binding-pr-{sha[:7]}.yaml", "hash": "h" + sha[:7], "name": "new-snip", "errors": []}

        return [
            admit,
            limits,
            close_snippet_issue,
            share_evidence,
            set_stage,
            stage_result,
            comment,
            refuse_label,
            prepare_match,
            prepare_job,
            prepare_debug,
            read_debug,
            debug_go,
            submit,
            read_match,
            take_lock,
            release_lock,
            read_verify,
            publish_gallery,
            publish_crops,
            open_snippet_issue,
            read_feedback,
            pr_state,
            autodev_look,
            set_crop_status,
            tree_sha,
            adopt_pr_binding,
            paused,
            reserve,
            settle,
            assess,
            revoke_dataset,
            prepare_judge,
            read_judge,
            kill_job,
        ]

    def await_act(self):
        @activity.defn(name="await_job")
        async def await_job(job_id: str, ws: str, lock: list[str] | None = None) -> JobDone:
            self.calls.append(("await", (job_id, ws, lock)))
            if self.gate and self.gate_on in ws:
                await self.gate.wait()
            if self.await_failures:
                self.await_failures -= 1
                raise ApplicationError("worker died mid-await")
            state, code = turn(self.states, ws.rsplit("/", 1)[1].rsplit("-", 1)[0], ("DONE", 0))  # by kind
            cost = 0.75 if "/match-" in ws else None  # an agent job says what it cost
            return JobDone(job_id, state, code, 4096, cost)  # every job claims a download; only the crop job's counts

        return [await_job]

    def names(self):
        return [c[0] for c in self.calls]

    def of(self, name):
        return [c[1] for c in self.calls if c[0] == name]

    @property
    def stages(self):
        return [a[0] for a in self.of("set_stage")]

    def jobs(self):
        return [a[2] for a in self.of("submit")]


async def until(cond, timeout=10.0):
    for _ in range(int(timeout / 0.02)):
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("timed out")


async def idle(h, m, stage):
    """Wait until set_stage(stage) ran and the workflow is blocked again (its last history event is a completed
    workflow task, not a scheduled activity), so the next signal finds it waiting rather than busy. A wait for
    autodev is timed, so a started timer may follow the task."""
    await until(lambda: m.stages[-1:] == [stage])
    trailing = {
        EventType.EVENT_TYPE_MARKER_RECORDED,
        EventType.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
        EventType.EVENT_TYPE_TIMER_STARTED,
    }
    for _ in range(500):
        events = [e for e in (await h.fetch_history()).events if e.event_type not in trailing]  # a patch marker
        if events[-1].event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("workflow never went idle")


@contextlib.asynccontextmanager
async def started(env, m, first=None, runner=None, signal="go", state=None):
    tq = f"tq-{uuid.uuid4()}"
    kw = {"workflow_runner": runner} if runner else {}
    async with (
        Worker(env.client, task_queue=tq, workflows=[MiaDataDataset], activities=m.acts(), **kw),
        Worker(env.client, task_queue=tq + "-await", activities=m.await_act()),
    ):
        h0 = await env.client.start_workflow(
            MiaDataDataset.run,
            state or DatasetState(7),
            id=f"wf-{uuid.uuid4()}",
            task_queue=tq,
            start_signal=signal,
            start_signal_args=[first or ev("go", "11")],
        )
        h = env.client.get_workflow_handle(h0.id)  # follows continue-as-new
        h.first_run_id = h0.result_run_id  # the run this call started; `run_id` is unset on such a handle
        try:
            yield h
        finally:
            await h.terminate()


async def approve_review(h, m, eid="12", dt=1, kind="approve"):
    await idle(h, m, "review")
    fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
    await h.signal(getattr(MiaDataDataset, kind), ev(kind, eid, shift(fin, dt)))


async def reject_review(h, m, eid="12", dt=1):
    await approve_review(h, m, eid, dt, kind="reject")


async def test_a_round_waits_for_its_stage_write(env):
    """stage-write-awaited: buffer-api checks an action against the registry's stage, so the stage may not lag
    the round: the match job is not submitted until the `matching` write has landed (spec webapp-actions §6)."""
    m = Mocks(outcome="blocked", slow_stages={"matching"})
    async with started(env, m) as h:
        await until(lambda: ("matching",) in m.of("stage_waits"))
        assert "submit" not in m.names() and m.stages == []
        m.stage_gate.set()
        await idle(h, m, "blocked")
    assert m.stages == ["matching", "blocked"] and m.of("submit")[0][1] == "match"


async def test_match_crops_review_approve_publishes_that_set(env):
    """spec §13: one set per round; the crops a human validated are the crops published, no second job."""
    m = Mocks()
    async with started(env, m) as h:
        await idle(h, m, "review")
        assert "publish_crops" not in m.names()  # nothing is published before the approve
        await approve_review(h, m)
        await idle(h, m, "done")
    assert m.stages == ["matching", "testing", "review", "done"]
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "verify"]  # no job between the match and the crops
    (the_set,) = [a for a in m.jobs() if a.n_crops]
    assert (the_set.n_crops, the_set.batch, the_set.trusted, the_set.sha, the_set.existing) == (16, 0, True, SHA, True)
    assert [a[0] for a in m.of("publish_gallery")] == [SHA]
    assert m.of("publish_crops") == [(f"/w/i7-e11/crop-{SHA}-1", 16, SHA, "/s/binding-1.yaml")]
    seq = [n for n in m.names() if n in ("read_verify", "publish_gallery", "publish_crops", "comment")]
    assert seq == ["read_verify", "publish_gallery", "read_verify", "publish_crops"]  # no report as a comment
    # each pass's result is carried by the stage write that ends it, with the jobs the pass ran
    matched, tested, reviewed = m.of("result")
    assert [(r[0], r[2]) for r in m.of("result")] == [
        ("matching", "testing"),
        ("testing", "review"),
        ("review", "done"),
    ]
    assert matched[1].startswith("Matched") and "axes from conversion.json" in matched[1]
    assert matched[3] == ["/w/i7-e11/match-1"] and not matched[4]
    assert "16 of 16 verified" in tested[1] and tested[4]  # the crops report, shown with the binding
    assert tested[3] == [f"/w/i7-e11/crop-{SHA}-1", f"/w/i7-e11/verify-{SHA}-1"]
    assert reviewed[1] == "Approved by alice." and reviewed[3] == []  # `done` and its totals end the review's result
    assert m.of("read_verify")[1] == (
        f"/w/i7-e11/verify-{SHA}-1",
        f"/w/i7-e11/crop-{SHA}-1",
    )  # read back for publishing
    assert "16 crops saved" in m.of("set_stage")[-1][2]
    owners = ["mia-data i7-e11 crop"]
    assert [a[2] for a in m.of("take_lock")] == owners and [a[2] for a in m.of("release_lock")] == owners
    crop_lock = ["raw", "rec-1", owners[0]]
    assert [a[2] for a in m.of("await")] == [None, crop_lock, crop_lock]  # match: none; crop and verify jobs
    report = tested[1]
    assert "table" in report and "axes from conversion.json" in report and "1234 bytes" in report
    done = m.of("set_stage")[-1]
    assert done[0] == "done" and "**Totals:** 16 crops saved" in done[2]  # the gallery table went up at review
    assert not m.of("comment") and not m.of("refuse_label")


async def test_a_round_that_rolled_over_before_it_kept_passes_ends_as_it_began(env):
    """A run that takes over a round at `review` from a state with no pass open (it rolled over before rounds
    kept their passes) writes its stages as before: no result is due, so none is written."""
    tested = "2026-01-01T00:00:00+00:00"
    test = Test(SHA, f"i7-e11-{SHA[:12]}", True, tested, crop_workspace="/w/i7-e11/crop", verify_workspace="/w/v")
    state = DatasetState(7, round_id="i7-e11", seed="1", stage="review", pins=PINS, binding="/s/b.yaml", tests=[test])
    m = Mocks()
    async with started(env, m, first=ev("approve", "12", shift(tested, 5)), signal="approve", state=state) as h:
        await idle(h, m, "done")
    assert m.stages == ["done"] and not m.of("result") and len(m.of("publish_crops")) == 1


async def test_seed_is_recorded(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert st.round_id == "i7-e11" and st.handled == ["11", "12"] and int(st.seed) >= 0
    assert [a.seed for a in m.jobs() if a.n_crops] == [st.seed + "-t"]


async def test_approve_outside_review_is_refused(env):
    m = Mocks(outcome="blocked")
    async with started(env, m) as h:
        await idle(h, m, "blocked")
        await h.signal(MiaDataDataset.approve, ev("approve", "12", "2030-01-01T00:00:00Z"))
        await until(lambda: m.of("refuse_label"))
    assert m.of("refuse_label")[0][0] == "data:approve" and m.of("refuse_label")[0][2] == "12"
    assert "cropping" not in m.stages and not [a for a in m.jobs() if a.n_crops]


async def test_stale_approve_is_refused(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m, "12", dt=-3600)
        await until(lambda: m.of("refuse_label"))
        assert m.stages[-1] == "review"
        await approve_review(h, m, "13")
        await idle(h, m, "done")
    ((label, reason, eid),) = m.of("refuse_label")
    assert (label, eid) == ("data:approve", "12") and "older" in reason


async def test_redelivered_go_is_ignored(env):
    m = Mocks(outcome="blocked")
    async with started(env, m) as h:
        await idle(h, m, "blocked")
        await h.signal(MiaDataDataset.go, ev("go", "11"))
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await until(lambda: len(m.stages) == 4)
        await idle(h, m, "blocked")
    assert m.of("admit") == [("11",), ("12",)] and not m.of("refuse_label")


async def test_go_while_busy_is_rejected_with_comment(env):
    m = Mocks(outcome="blocked", gate=asyncio.Event())
    async with started(env, m) as h:
        await until(lambda: m.of("await"))
        await h.signal(MiaDataDataset.go, ev("go", "12"))  # in history before the job ends
        m.gate.set()
        await until(lambda: m.of("refuse_label"))
        await idle(h, m, "blocked")
        await h.signal(MiaDataDataset.go, ev("go", "13"))  # arrives while idle: a new round
        await until(lambda: len(m.of("admit")) == 2)
        await until(lambda: m.stages.count("blocked") == 2)  # no activity in flight at shutdown: one would hold
        await idle(h, m, "blocked")  # up the test server's time skipping (env.sleep) in later tests
    ((label, reason, eid),) = m.of("refuse_label")
    assert (label, eid) == ("data:go", "12") and "busy" in reason
    assert m.of("admit") == [("11",), ("13",)]


async def test_a_set_with_fewer_than_half_verified_never_reaches_review(env):
    m = Mocks(set_ok=5)
    async with started(env, m) as h:
        await idle(h, m, "failed")
    assert m.stages == ["matching", "testing", "failed"] and "publish_crops" not in m.names()
    stage, report, rests_at, jobs, _ = m.of("result")[-1]  # the failed pass says so itself, with its jobs
    assert (stage, rests_at, len(jobs)) == ("testing", "failed", 2)
    assert "5 of 16 verified (at least 8 needed)" in report and not m.of("comment")


async def test_a_set_with_at_least_half_verified_is_reviewed_and_published(env):
    """harness verify exits 1 whenever a crop fails; the set is judged by its report, not that exit."""
    m = Mocks(states={f"verify-{SHA}": ("EXIT", 1)}, set_ok=12)
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
    assert m.of("publish_crops")[0][1] == 12 and "12 crops saved" in m.of("set_stage")[-1][2]


async def test_an_ineligible_outcome_is_an_unknown_one_now(env):
    m = Mocks(outcome="ineligible")
    async with started(env, m) as h:
        await idle(h, m, "blocked")
    assert m.stages == ["matching", "blocked"] and not [a for a in m.jobs() if a.n_crops]


@pytest.mark.parametrize(
    "mocks, stages, said",
    [
        # the match agent says another run may pass
        ({"outcome": "failed"}, ["matching", "failed"], "match failed: label class undecidable"),
        # a host waits for a person's approval
        (
            {"set_ok": 0, "host_not_allowed": "h.example.org"},
            ["matching", "testing", "blocked"],
            "host not allowed: h.example.org",
        ),
        # admission refuses the record: it will again until someone changes it
        (
            {"admit_error": "rec-1: its dataset folder f belongs to record rec-2"},
            ["blocked"],
            "belongs to record rec-2",
        ),
    ],
)
async def test_a_stop_says_whether_it_failed_or_is_blocked(env, mocks, stages, said):
    """`failed`: running it again may pass. `blocked`: a person has to decide or change something first."""
    m = Mocks(**mocks)
    async with started(env, m) as h:
        await idle(h, m, stages[-1])
    assert m.stages == stages and said in m.of("set_stage")[-1][1]


async def test_blocked_goes_needs_human(env):
    m = Mocks(outcome="blocked")
    async with started(env, m) as h:
        await idle(h, m, "blocked")
    assert m.stages == ["matching", "blocked"] and "label class undecidable" in m.of("set_stage")[-1][1]
    assert [a[1] for a in m.of("submit")] == ["match"]


@pytest.mark.parametrize(
    "states, stages, published",
    [
        ({"match": ("EXIT", 1)}, ["matching", "failed"], []),
        ({f"crop-{SHA}": ("EXIT", 1)}, ["matching", "testing", "failed"], []),
        ({f"verify-{SHA}": ("UNKNOWN", None)}, ["matching", "testing", "failed"], []),
    ],
)
async def test_failed_job_needs_human_no_retry(env, states, stages, published):
    m = Mocks(states=states)
    async with started(env, m) as h:
        await idle(h, m, "failed")
    keys = [a[0] for a in m.of("submit")]
    assert m.stages == stages and len(keys) == len(set(keys))
    assert [a[1] for a in m.of("publish_crops")] == published  # nothing is published before a review
    assert ("read_match" in m.names()) == ("match" not in states)


async def test_rollover_in_review_then_approve(env, monkeypatch):
    from temporalio import workflow

    monkeypatch.setattr(workflows, "_rollover_due", lambda: workflow.info().continued_run_id is None)  # once
    m = Mocks()
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await until(lambda: m.stages[-1:] == ["review"])
        for _ in range(500):
            if (await h.describe()).run_id != h.first_run_id:
                break
            await asyncio.sleep(0.02)
        await approve_review(h, m)
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert (await h.describe()).run_id != h.first_run_id
    assert m.stages == ["matching", "testing", "review", "done"] and len(m.of("admit")) == 1
    assert [a.seed for a in m.jobs() if a.n_crops] == [st.seed + "-t"]
    assert "16 crops saved" in m.of("set_stage")[-1][2]  # the workspaces outlive the rollover


async def test_test_crop_failure_never_produces(env):
    m = Mocks(set_ok=0)
    async with started(env, m) as h:
        await idle(h, m, "failed")
        await h.signal(MiaDataDataset.approve, ev("approve", "12", "2030-01-01T00:00:00Z"))
        await until(lambda: m.of("refuse_label"))
    assert m.stages == ["matching", "testing", "failed"]
    assert [a.batch for a in m.jobs() if a.n_crops] == [0] and "publish_crops" not in m.names()


async def test_worker_death_mid_await_reattaches(env):
    m = Mocks(await_failures=1)
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
    keys = [a[0] for a in m.of("submit")]
    assert len(keys) == len(set(keys)) == 3 and len(m.of("await")) == 4  # one await retried, no job resubmitted


async def test_continue_as_new_keeps_handled(env, monkeypatch):
    monkeypatch.setattr(workflows, "MAX_ROUNDS_PER_EXECUTION", 1)
    m = Mocks(outcome="blocked")
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await idle(h, m, "blocked")
        assert (await h.describe()).run_id != h.first_run_id  # rolled over
        await h.signal(MiaDataDataset.go, ev("go", "11"))  # redelivered into the new run
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await until(lambda: len(m.of("admit")) == 2)
        await idle(h, m, "blocked")
        assert (await h.query(MiaDataDataset.status)).handled == ["11", "12"]
    assert m.of("admit") == [("11",), ("12",)]


async def test_a_restart_starts_its_round_in_a_new_run(env):
    """A dataset's go arrives as `restart`. A run that has carried a round rolls over first, so the round runs in a
    new run (one that takes every code path of today); the round a run starts with stays in it."""
    m = Mocks(outcome="blocked")
    async with started(env, m, signal="restart") as h:
        await idle(h, m, "blocked")
        assert (await h.describe()).run_id == h.first_run_id and m.of("admit") == [("11",)]  # a new run: in place
        await h.signal(MiaDataDataset.restart, ev("go", "12"))
        await until(lambda: m.stages.count("blocked") == 2)
        await idle(h, m, "blocked")
        second = (await h.describe()).run_id
        assert second != h.first_run_id  # the first run had carried a round
        await h.signal(MiaDataDataset.restart, ev("go", "12"))  # redelivered into the new run: handled already
        await h.signal(MiaDataDataset.go, ev("go", "13"))  # a plain go stays in the run it reaches
        await until(lambda: m.stages.count("blocked") == 3)
        await idle(h, m, "blocked")
        st = await h.query(MiaDataDataset.status)
        assert (await h.describe()).run_id == second
    assert m.of("admit") == [("11",), ("12",), ("13",)] and m.stages == ["matching", "blocked"] * 3
    assert (st.handled, st.restart, st.round_id) == (["11", "12", "13"], None, "i7-e13") and not m.of("refuse_label")


async def test_a_restart_at_review_drops_the_set_and_starts_over_in_a_new_run(env):
    m = Mocks()
    async with started(env, m) as h:
        await idle(h, m, "review")
        await h.signal(MiaDataDataset.restart, ev("go", "12"))
        await until(lambda: m.stages.count("review") == 2)
        await idle(h, m, "review")
        assert (await h.describe()).run_id != h.first_run_id
        await approve_review(h, m, eid="13")
        await idle(h, m, "done")
    assert m.stages == ["matching", "testing", "review", "matching", "testing", "review", "done"]
    assert len(m.of("publish_crops")) == 1 and not m.of("refuse_label")  # the first set was never saved


async def test_a_restart_at_review_of_a_pr_heads_set_tests_that_head_again(env):
    """The snippet PR stays the dataset's: the new round, in a new run, tests its head again and asks autodev for
    nothing."""
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await to_review(h, m)
        await h.signal(MiaDataDataset.restart, ev("go", "12"))
        await until(lambda: m.stages.count("review") == 2)
        await idle(h, m, "review")
        assert (await h.describe()).run_id != h.first_run_id
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "developing", "testing", "review", "testing", "review"]
    assert m.of("admit") == [("11",), ("12",)] and m.of("open_snippet_issue") == [("i7-e11", None)]
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "verify", "crop", "verify"]  # no second match
    assert (st.round_id, st.snippet_issue) == ("i7-e12", 100) and not m.of("refuse_label")


async def test_the_crop_job_waits_for_the_raw_lock_and_releases_it_after_a_failed_set(env):
    m = Mocks(set_ok=0, lock_busy=2)
    async with started(env, m) as h:
        for tries in (1, 2):  # a refused take sleeps poll_minutes (2), doubling with each try
            await until(lambda tries=tries: len(m.of("take_lock")) == tries)
            await env.sleep(timedelta(minutes=2 * tries + 1))
        await idle(h, m, "failed")
        timers = [e for e in (await h.fetch_history()).events if e.event_type == EventType.EVENT_TYPE_TIMER_STARTED]
        assert [e.timer_started_event_attributes.start_to_fire_timeout.seconds for e in timers] == [120, 240]
    owner = "mia-data i7-e11 crop"
    assert m.of("take_lock") == [("raw", "rec-1", owner, 900)] * 3
    waiting = [a for a in m.of("comment") if a[1] == "lock i7-e11"]
    assert waiting == [
        ("waiting for the raw cache of `rec-1`: held by mia-buffers b1 stage", "lock i7-e11", "data", None)
    ]
    names = [(name, args[1] if name == "submit" else None) for name, args in m.calls]
    last_take = max(i for i, (name, _) in enumerate(names) if name == "take_lock")
    assert names.index(("submit", "crop")) > last_take > names.index(("comment", None))
    lock = ["raw", "rec-1", owner]
    assert [a[2] for a in m.of("await")] == [None, lock, lock]  # the crop and verify jobs run under it
    assert m.of("read_verify") == [(f"/w/i7-e11/verify-{SHA}-1", f"/w/i7-e11/crop-{SHA}-1", "rec-1")]  # the asset row
    assert m.of("release_lock") == [("raw", "rec-1", owner)]
    assert names.index(("read_verify", None)) < names.index(("release_lock", None))
    assert m.stages == ["matching", "testing", "failed"] and "the crops failed" in m.of("set_stage")[-1][1]


# --- a failed round is debugged and run again -----------------------------------------------

CROP_FAILS = {f"crop-{SHA}": ("EXIT", 1)}


async def debugging(h, m):
    """Wait until the dataset is at `debugging` and its wait for the debugging agent has begun (a timer)."""
    await until(lambda: m.stages[-1:] == ["debugging"])
    for _ in range(500):
        if (await h.fetch_history()).events[-1].event_type == EventType.EVENT_TYPE_TIMER_STARTED:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("the wait before the debugging agent never began")


async def debugged(env, h, m):
    """From `debugging` through the wait and the debugging agent, up to its go being asked for."""
    await debugging(h, m)
    assert "prepare_debug" not in m.names()  # the wait comes first
    await env.sleep(timedelta(minutes=6))
    await until(lambda: m.of("debug_go"))


async def test_a_failed_round_is_debugged_and_run_again(env):
    m = Mocks(debug_cycles=1, states={f"crop-{SHA}": [("EXIT", 1), ("DONE", 0)]})
    async with started(env, m, signal="restart") as h:
        await debugged(env, h, m)
        await h.signal(MiaDataDataset.restart, ev("go", "21", actor="debugger"))  # what buffer-api makes of its go
        await idle(h, m, "review")
        st = await h.query(MiaDataDataset.status)
        assert (await h.describe()).run_id != h.first_run_id  # the re-run is a round of its own, in a new run
    assert m.stages == ["matching", "testing", "debugging", "matching", "testing", "review"]
    assert "A debugging agent looks into this in 5 minutes" in m.of("set_stage")[2][1]
    stage, failed, then, jobs, _ = m.of("result")[1]  # testing's own result carries the failure and its jobs
    assert (stage, then, len(jobs)) == ("testing", "debugging", 2)
    assert "**failed**: the crops failed" in failed and "1 of" not in m.of("set_stage")[2][1]
    ((round_id, failure, binding),) = m.of("prepare_debug")
    assert (round_id, binding) == ("i7-e11", "/s/binding-1.yaml") and "the crops failed" in failure
    assert "debugging agent" not in failure  # the debugger reads why the round failed, not what happens next
    assert [a[1] for a in m.of("submit")][:4] == ["match", "crop", "verify", "debug"]
    assert [a[3] for a in m.of("reserve") if a[2] == "debug"] == [5.0]  # an agent run, reserved like the match
    assert m.of("debug_go") == [("i7-e11", "bind the first volume only")]
    stage, found, then, jobs, _ = next(r for r in m.of("result") if r[0] == "debugging")
    assert found.startswith("**Debugger:** the host sent 30 KB/s") and then is None  # written by stage_result
    assert jobs == ["/w/i7-e11/debug-1"] and not m.of("comment")
    assert (st.round_id, st.debugs) == ("i7-e21", 1) and m.of("admit") == [("11",), ("21",)]


async def test_a_re_run_that_fails_stops_and_a_persons_go_gets_its_own_cycle(env):
    m = Mocks(debug_cycles=1, states=CROP_FAILS)
    async with started(env, m, signal="restart") as h:
        await debugged(env, h, m)
        await h.signal(MiaDataDataset.restart, ev("go", "21", actor="debugger"))
        await idle(h, m, "failed")
        assert m.stages == ["matching", "testing", "debugging", "matching", "testing", "failed"]
        assert m.of("set_stage")[-1][1].endswith("nothing more is tried automatically.")
        assert len(m.of("prepare_debug")) == 1
        await h.signal(MiaDataDataset.restart, ev("go", "31"))  # a person: Run again
        await until(lambda: m.stages.count("debugging") == 2)  # their round has a cycle of its own


@pytest.mark.parametrize(
    "mocks, stage, said",
    [
        (  # no round can pass until a person changes something
            {"debug_result": DebugResult("blocked", "a 65 MB plane is read per volume", "raise job_walltime_min")},
            "blocked",
            ["the crops failed", "**Debugger:** a 65 MB plane", "**What it needs:** raise job_walltime_min"],
        ),
        (  # the debugging agent's own job failed
            {"states": {**CROP_FAILS, "debug": ("EXIT", 1)}, "debug_result": DebugResult("failed", "no outcome")},
            "failed",
            ["the crops failed", "The debugging agent did not finish: debug job job-mia-7-i7-e11-debug-1 ended EXIT"],
        ),
        (  # its job ran, and left nothing readable
            {"debug_result": DebugResult("failed", "the debugging agent left no readable outcome.json")},
            "failed",
            ["The debugging agent did not finish: the debugging agent left no readable outcome.json"],
        ),
        (  # the operator's pause refuses its job, as it would refuse a new round's
            {"reservations": {"debug": Reservation(False, "max_usd reached")}},
            "blocked",
            ["the crops failed", "The debugging agent did not run: root budget: max_usd reached"],
        ),
    ],
)
async def test_a_debugger_that_finds_no_way_on_stops_the_round_with_both_reasons(env, mocks, stage, said):
    m = Mocks(debug_cycles=1, **{"states": CROP_FAILS, **mocks})
    async with started(env, m) as h:
        await debugging(h, m)
        await env.sleep(timedelta(minutes=6))
        await idle(h, m, stage)
    assert m.stages == ["matching", "testing", "debugging", stage] and not m.of("debug_go")
    assert all(text in m.of("set_stage")[-1][1] for text in said), m.of("set_stage")[-1][1]


async def test_run_again_at_debugging_starts_a_persons_round_without_the_debugger(env):
    m = Mocks(debug_cycles=1, states={f"crop-{SHA}": [("EXIT", 1), ("DONE", 0)]})
    async with started(env, m, signal="restart") as h:
        await debugging(h, m)
        await h.signal(MiaDataDataset.restart, ev("go", "12"))
        await idle(h, m, "review")
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "testing", "debugging", "matching", "testing", "review"]
    assert "prepare_debug" not in m.names() and not m.of("refuse_label") and (st.round_id, st.debugs) == ("i7-e12", 0)


async def test_a_cancel_at_debugging_ends_it(env):
    m = Mocks(debug_cycles=1, states=CROP_FAILS)
    async with started(env, m) as h:
        await debugging(h, m)
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await idle(h, m, "cancelled")
    assert m.stages == ["matching", "testing", "debugging", "cancelled"] and "prepare_debug" not in m.names()


@pytest.mark.parametrize(
    "mocks, stages",
    [
        ({"outcome": "blocked"}, ["matching", "blocked"]),  # a person has to act: no round can pass
        ({"admit_error": "rec-1: no such record"}, ["blocked"]),
        ({"debug_cycles": 0, "states": CROP_FAILS}, ["matching", "testing", "failed"]),  # switched off
    ],
)
async def test_only_a_failed_round_with_a_cycle_left_is_debugged(env, mocks, stages):
    m = Mocks(**{"debug_cycles": 1, **mocks})
    async with started(env, m) as h:
        await idle(h, m, stages[-1])
    assert m.stages == stages


# --- the development path (§7.3) ------------------------------------------------------------

H1, H2, H3, MC = "1" * 40, "2" * 40, "3" * 40, "e" * 40
PR_URL = "https://github.com/o/snippets/pull/5"


def head(sha, state="open", merge=None, needs_human=False, issue=100):
    return PrUpdate(5, sha, state, merge, issue, needs_human, PR_URL)


def statuses(m):
    return [(sha, state) for sha, state, _ in m.of("set_crop_status")]


async def wake(h, m, now):
    """The PR is now `now`; the poller's signal is a wake-up only (its fields are deliberately stale)."""
    m.pr_now = now
    await h.signal(MiaDataDataset.pr_update, head("0" * 40))


async def to_review(h, m):
    """needs_snippet → snippets issue → h1's set made and shown → review."""
    await idle(h, m, "developing")
    await wake(h, m, head(H1))
    await idle(h, m, "review")


def pr_comments(m):
    return [a[0] for a in m.of("comment") if a[2] == "snippets"]


async def happy(h, m):
    """h1's set approved → `/approve h1` → autodev merges → h1's crops are published under the merge commit."""
    await to_review(h, m)
    await approve_review(h, m)
    await idle(h, m, "merging")
    await wake(h, m, head(H1, "merged", MC))
    await idle(h, m, "done")


async def test_develop_happy_path(env):
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await happy(h, m)
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "developing", "testing", "review", "merging", "done"]
    assert statuses(m) == [(H1, "pending"), (H1, "success")]
    assert m.of("set_crop_status")[1][2] == f"https://github.com/o/snippets/issues/5#test i7-e11 {H1}"  # the PR comment
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "verify"]  # no smoke: tested; no second set
    assert not m.of("take_lock")  # the PR head's crop job writes a private cache, not the record's
    (the_set,) = [a for a in m.jobs() if a.n_crops]
    assert (the_set.sha, the_set.trusted, the_set.n_crops, the_set.batch) == (H1, False, 16, 0)  # the dataset's set
    assert the_set.existing  # dedups against a published manifest like any set
    assert m.of("adopt_pr_binding") == [(5, H1), (5, MC)]  # the merge commit's binding is what is published
    assert pr_comments(m)[-1] == f"/approve {H1}"  # autodev merges on the worker's word
    assert m.of("publish_crops")[0] == (f"/w/i7-e11/crop-{H1}-1", 16, MC, f"/s/binding-pr-{MC[:7]}.yaml")
    assert m.of("publish_gallery") == [(H1, 16)]
    assert st.snippet_issue == 100 and st.pr is None and st.pins.snippets_commit == MC and st.snippet_name == "new-snip"
    assert [(t.sha, t.ok, t.tree_sha, bool(t.approved_at)) for t in st.tests] == [(H1, True, "T", True)]
    assert [a[2] for a in m.of("set_stage") if a[0] == "review"] == [f"**Snippet PR:** {PR_URL}\n"]
    assert "16 crops saved" in m.of("set_stage")[-1][2]


async def test_revise_new_head_retests(env):
    """A head whose set fails goes back to autodev as `/revise` with the report; the stage leaves review."""
    m = Mocks(outcome="needs_snippet", test_ok_by={H2: 0})
    async with started(env, m) as h:
        await to_review(h, m)
        await wake(h, m, head(H2))
        await until(lambda: (H2, "failure") in statuses(m))
        await idle(h, m, "testing")
    assert m.stages[-3:] == ["testing", "review", "testing"]  # h1's success does not make h2 ready
    assert statuses(m) == [(H1, "pending"), (H1, "success"), (H2, "pending"), (H2, "failure")]
    assert [a.sha for a in m.jobs() if a.n_crops] == [H1, H2] and "cropping" not in m.stages
    h1_report, h2_report = pr_comments(m)
    assert h1_report.startswith("Crops at") and h2_report.startswith(f"/revise the crops at `{H2}` failed")
    assert "0 of 16 verified (at least 8 needed)" in h2_report


async def test_reject_in_develop_review_sends_the_comments_to_the_pr(env):
    """data:reject at a PR head's review: the reviewer's comments since the set's report go to the PR as
    `/revise`; the next head's set comes back to review, and its approve merges (spec §14)."""
    m = Mocks(outcome="needs_snippet", trees={H2: "T2", MC: "T2"})
    async with started(env, m) as h:
        await to_review(h, m)
        await reject_review(h, m, eid="12")
        await idle(h, m, "developing")
        fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
        await wake(h, m, head(H2))
        await approve_review(h, m, eid="13")
        await idle(h, m, "merging")
        await wake(h, m, head(H2, "merged", MC))
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert m.stages == [
        "matching",
        "developing",
        "testing",
        "review",
        "developing",
        "testing",
        "review",
        "merging",
        "done",
    ]
    assert m.of("read_feedback") == [(7, fin, shift(fin, 1))]  # the comments between the report and the reject
    assert pr_comments(m)[1] == "/revise alice wrote:\nthe masks are shifted by one pixel"
    assert pr_comments(m)[-1] == f"/approve {H2}" and not m.of("refuse_label")
    assert [(t.sha, bool(t.approved_at)) for t in st.tests] == [(H1, False), (H2, True)]
    assert m.of("publish_crops")[0][0] == f"/w/i7-e11/crop-{H2}-1"


async def test_reject_in_matched_review_asks_for_a_superseding_snippet(env):
    """data:reject on a matched binding's crops: a snippets issue asks autodev to supersede the binding's
    snippet, with the comments; the round goes on as a develop round and ends like one (spec §14)."""
    m = Mocks(trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await reject_review(h, m)
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await approve_review(h, m, eid="13")
        await idle(h, m, "merging")
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "testing", "review", "developing", "testing", "review", "merging", "done"]
    (asked,) = m.of("open_snippet_issue")
    assert asked[:3] == ("i7-e11", None, "/s/binding-1.yaml")
    # the comments, under a request the developer can answer with "the crops are right as they are"
    assert asked[3]["what_fails"].splitlines()[0].startswith("Request: i7-e11:")
    assert "alice wrote:\nthe masks are shifted by one pixel" in asked[3]["what_fails"]
    assert m.of("open_snippet_issue") == [("i7-e11", None, "/s/binding-1.yaml", {"what_fails": asked[3]["what_fails"]})]
    assert "superseding" in m.of("set_stage")[3][1]
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "verify", "crop", "verify"]
    assert [(a.sha, a.trusted) for a in m.jobs() if a.n_crops] == [(PINS.snippets_commit, True), (H1, False)]
    assert m.of("publish_crops")[0][2] == MC and st.pins.snippets_commit == MC and st.snippet_issue == 100


async def test_stale_and_misplaced_rejects_are_refused(env):
    m = Mocks(outcome="needs_snippet")
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await h.signal(MiaDataDataset.reject, ev("reject", "12"))  # not in review
        await until(lambda: m.of("refuse_label"))
        await wake(h, m, head(H1))
        await idle(h, m, "review")
        await h.signal(MiaDataDataset.reject, ev("reject", "13"))  # older than the set
        await until(lambda: len(m.of("refuse_label")) == 2)
        await idle(h, m, "review")
    assert [(a[0], a[2]) for a in m.of("refuse_label")] == [("data:reject", "12"), ("data:reject", "13")]
    assert "at `review`" in m.of("refuse_label")[0][1] and "older" in m.of("refuse_label")[1][1]
    assert not m.of("read_feedback") and m.stages[-1] == "review"


async def test_rollover_at_a_pr_heads_review_keeps_following_the_pr(env, monkeypatch):
    from temporalio import workflow

    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"})
    m.roll = False  # the rollover is due once h1's set is at review, and once
    monkeypatch.setattr(workflows, "_rollover_due", lambda: m.roll and workflow.info().continued_run_id is None)
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await idle(h, m, "developing")
        m.roll = True
        await wake(h, m, head(H1))
        await until(lambda: m.stages[-1:] == ["review"])
        for _ in range(500):
            if (await h.describe()).run_id != h.first_run_id:
                break
            await asyncio.sleep(0.02)
        await approve_review(h, m)
        await idle(h, m, "merging")
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
    assert (await h.describe()).run_id != h.first_run_id
    assert m.stages == ["matching", "developing", "testing", "review", "merging", "done"]
    assert pr_comments(m)[-1] == f"/approve {H1}"


async def test_stale_result_ignored(env):
    m = Mocks(outcome="needs_snippet", gate=asyncio.Event(), gate_on=H1, test_ok_by={H2: 0})
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await until(lambda: any(H1 in a[1] for a in m.of("await")))
        await wake(h, m, head(H2))  # h1 is still testing (and will pass)
        m.gate.set()
        await until(lambda: (H2, "failure") in statuses(m))
        await idle(h, m, "testing")
        st = await h.query(MiaDataDataset.status)
    assert "review" not in m.stages and [(t.sha, t.ok) for t in st.tests] == [(H1, True), (H2, False)]
    assert statuses(m)[-2:] == [(H2, "pending"), (H2, "failure")]  # h1's late success is h1's, not h2's


@pytest.mark.parametrize("merge_ok", [True, False])
async def test_merge_with_different_tree_makes_the_merge_commits_set_for_review(env, merge_ok):
    """The merged snippet is not the approved head's (someone pushed more): its own set, reviewed again."""
    m = Mocks(outcome="needs_snippet", trees={H1: "T1", MC: "T2"}, test_ok_by={} if merge_ok else {MC: 0})
    async with started(env, m) as h:
        await to_review(h, m)
        await approve_review(h, m)
        await idle(h, m, "merging")
        await wake(h, m, head(H1, "merged", MC))
        if merge_ok:
            await approve_review(h, m, eid="13")
        await idle(h, m, "done" if merge_ok else "failed")
        st = await h.query(MiaDataDataset.status)
    tests = [a for a in m.jobs() if a.n_crops and not a.batch]
    assert [(a.sha, a.trusted) for a in tests] == [(H1, False), (MC, True)]  # the merge commit: a trusted job
    assert statuses(m) == [(H1, "pending"), (H1, "success")]  # no status on the merge commit
    assert bool(m.of("publish_crops")) == merge_ok and (m.stages[-1] == "done") == merge_ok
    assert m.stages[3:6] == ["review", "merging", "testing"] and "not the reviewed head" in m.of("set_stage")[5][1]
    if merge_ok:
        assert m.of("publish_crops")[0][0] == f"/w/i7-e11/crop-{MC}-1" and st.tests[-1].approved_at


async def test_head_test_that_cannot_run_posts_error(env):
    m = Mocks(outcome="needs_snippet", states={f"crop-{H1}": ("UNKNOWN", None)})
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await idle(h, m, "failed")
    assert statuses(m) == [(H1, "pending"), (H1, "error")] and "UNKNOWN" in m.of("set_stage")[-1][1]


@pytest.mark.parametrize("now, why", [(head(H1, "closed"), "closed"), (head(H1, needs_human=True), "needs-human")])
async def test_closed_pr_needs_human(env, now, why):
    """The round ends `blocked`, naming the PR and quoting what autodev last said, so the entry says why."""
    said = "Approved, but merging failed: Pull Request has merge conflicts"
    m = Mocks(outcome="needs_snippet", look=AutodevLook(5, False, True, said))
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await h.signal(MiaDataDataset.go, ev("go", "12"))  # busy: a round is developing
        await until(lambda: m.of("refuse_label"))
        await wake(h, m, now)
        await idle(h, m, "blocked")
    assert m.of("refuse_label")[0][0] == "data:go" and "busy" in m.of("refuse_label")[0][1]
    detail = m.of("set_stage")[-1][1]
    assert why in detail and PR_URL in detail and not [a for a in m.jobs() if a.n_crops]
    assert f'Its last comment: "{said}"' in detail and m.of("autodev_look") == [(100, 5)]


LOOK = timedelta(minutes=16)  # past one look of a round that waits for autodev


async def test_a_waiting_round_ends_when_autodev_stopped_before_it_opened_a_pr(env):
    """autodev's first run failed: there is no PR for the poller to see, and the round's own look finds the issue."""
    m = Mocks(outcome="needs_snippet", look=AutodevLook(None, False, True, "snippets: stopped. agent job 1 ended EXIT"))
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await env.sleep(LOOK)
        await idle(h, m, "blocked")
    detail = m.of("set_stage")[-1][1]
    assert "mia-snippets issue #100: autodev stopped before it opened a pull request" in detail
    assert "agent job 1 ended EXIT" in detail and m.of("autodev_look") == [(100, None)]


async def test_a_waiting_round_ends_after_the_quiet_limit_unless_autodev_is_at_work(env):
    quiet, at_work = AutodevLook(None, False, False, "Round 1 started"), AutodevLook(None, True, False, "")
    m = Mocks(outcome="needs_snippet", look=[at_work] * 12 + [quiet])
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await env.sleep(timedelta(hours=3, minutes=5))  # autodev is at work at every look: the limit starts over
        assert m.stages[-1] == "developing" and len(m.of("autodev_look")) == 12
        await env.sleep(timedelta(hours=1))
        assert m.stages[-1] == "developing"  # quiet, but not for two hours yet
        await env.sleep(timedelta(hours=1, minutes=16))
        await idle(h, m, "blocked")
    detail = m.of("set_stage")[-1][1]
    assert "mia-snippets issue #100: nothing from autodev for 2 h" in detail and "Round 1 started" in detail


async def test_a_look_acts_on_a_pr_the_round_was_not_told_of_and_review_is_not_timed(env):
    m = Mocks(outcome="needs_snippet", look=AutodevLook(5, False, False, ""), pr_now=head(H1))
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await env.sleep(LOOK)
        await idle(h, m, "review")  # the head's crops were made without a pr_update
        await env.sleep(timedelta(hours=5))  # a person is awaited at review, not autodev
        assert m.stages[-1] == "review" and m.of("autodev_look") == [(100, None)]


async def test_a_revision_autodev_answers_without_a_new_head_comes_back_to_review_with_the_answer(env):
    """Incident 2026-11-17 (empiar-10017): a reviewer asked whether the repeated rows of a crop are in the data.
    They are, so no new head could come: the round has to show autodev's answer and ask for the verdict again."""
    said = "The rows repeat in the source file.\nNothing to change."
    quiet = AutodevLook(5, False, False, "")
    m = Mocks(outcome="needs_snippet", look=quiet, pr_now=head(H1))
    async with started(env, m) as h:
        await to_review(h, m)
        await reject_review(h, m, eid="12")
        await idle(h, m, "developing")
        await env.sleep(LOOK)
        assert m.stages[-1] == "developing"  # autodev has not answered yet
        m.look = AutodevLook(5, False, False, "", said, "2999-01-01T00:00:00Z")
        await env.sleep(LOOK)
        await idle(h, m, "review")
        answered = [r[1] for r in m.of("result") if r[0] == "developing"][-1]
        assert "autodev changed nothing" in answered and "> The rows repeat in the source file.\n> Nothing" in answered
        assert len([a for a in m.jobs() if a.n_crops]) == 1  # the same crops: no new set was made
        await env.sleep(LOOK)
        assert m.stages[-1] == "review"  # the answer is shown once
        await approve_review(h, m, eid="13", dt=3)
        await idle(h, m, "merging")
    assert pr_comments(m)[-1] == f"/approve {H1}"


async def test_a_merge_that_does_not_come_ends_the_round(env):
    said = "Approved, but merging failed: Pull Request has merge conflicts"
    m = Mocks(outcome="needs_snippet", look=AutodevLook(5, False, False, said), pr_now=head(H1))
    async with started(env, m) as h:
        await to_review(h, m)
        await approve_review(h, m, eid="13")
        await idle(h, m, "merging")
        await env.sleep(timedelta(hours=2, minutes=16))
        await idle(h, m, "blocked")
    detail = m.of("set_stage")[-1][1]
    assert "nothing from autodev for 2 h" in detail and "merge conflicts" in detail and "PR #5" in detail


async def test_revision_limit(env):
    m = Mocks(outcome="needs_snippet", revisions=2, test_ok=0)
    async with started(env, m) as h:
        await idle(h, m, "developing")
        for sha in (H1, H2):
            await wake(h, m, head(sha))
            await until(lambda sha=sha: (sha, "failure") in statuses(m))
            await idle(h, m, "testing")
        await wake(h, m, head(H3))
        await idle(h, m, "failed")
    assert "revision limit" in m.of("set_stage")[-1][1] and [a.sha for a in m.jobs() if a.n_crops] == [H1, H2]
    # every head tested is a pass of its own: a start, and a result written where no stage write follows
    assert m.stages == ["matching", "developing", "testing", "testing", "failed"]
    first, second = (r for r in m.of("result") if r[2] is None)
    assert (first[0], second[0]) == ("testing", "testing") and H1 in first[1] and H2 in second[1]
    assert f"head [`{H2[:7]}`]({PR_URL}/commits/{H2})" in m.of("set_stage")[3][1]  # the start links the commit
    assert first[3] == [f"/w/i7-e11/crop-{H1}-1", f"/w/i7-e11/verify-{H1}-1"] and first[4]
    assert [r[2] for r in m.of("result")][-1] is None  # no pass is open at the limit: `failed` stands alone


async def test_merge_signal_before_test_finishes(env):
    """A human merged the PR by hand while h1's set was still being made: nobody approved h1's crops, so the
    merge commit's set is made and reviewed like a matched binding's."""
    m = Mocks(outcome="needs_snippet", gate=asyncio.Event(), gate_on=H1, trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await until(lambda: any(H1 in a[1] for a in m.of("await")))
        await wake(h, m, head(H1, "merged", MC))
        m.gate.set()
        await approve_review(h, m)
        await idle(h, m, "done")
    assert m.stages == ["matching", "developing", "testing", "testing", "review", "done"]
    assert statuses(m) == [(H1, "pending"), (H1, "success")]
    assert [(a.sha, a.trusted) for a in m.jobs() if a.n_crops] == [(H1, False), (MC, True)]
    assert m.of("publish_crops")[0][0] == f"/w/i7-e11/crop-{MC}-1"


async def to_needs_human_in_develop(h, m):
    """needs_snippet → snippets issue → h1's crop job is lost (an infrastructure failure) → needs-human."""
    await idle(h, m, "developing")
    await wake(h, m, head(H1))
    await idle(h, m, "failed")


async def test_go_after_needs_human_resumes_the_snippet_pr(env):
    """A develop round that failed for an infrastructure reason: `go` re-tests the same PR under a new round and
    pins, and opens no second snippets issue (spec §7.4, comment and re-trigger)."""
    m = Mocks(outcome="needs_snippet", states={f"crop-{H1}": ("UNKNOWN", None)}, trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await to_needs_human_in_develop(h, m)
        m.states = {}  # the infrastructure is fixed
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await approve_review(h, m, eid="13")
        await idle(h, m, "merging")
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "developing", "testing", "failed", "testing", "review", "merging", "done"]
    assert m.of("admit") == [("11",), ("12",)] and m.of("open_snippet_issue") == [("i7-e11", None)]
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "crop", "verify"]  # no second match
    keys = [a[0] for a in m.of("submit")]
    assert len(keys) == len(set(keys)) and keys[2].startswith("mia-7-i7-e12-crop-")  # the retest is the new round's
    assert statuses(m) == [(H1, "pending"), (H1, "error"), (H1, "pending"), (H1, "success")]
    resumed = m.of("set_stage")[4]
    assert resumed[0] == "testing" and "resuming" in resumed[1] and PR_URL in resumed[1]
    assert (st.round_id, st.snippet_issue, st.pr) == ("i7-e12", 100, None) and not m.of("refuse_label")
    assert [t.test_id for t in st.tests] == [f"i7-e12-{H1[:12]}"]


async def debugged_in_develop(env, h, m):
    """needs_snippet → snippets issue → h1's crop job is lost → debugging → the debugger asks for its go."""
    await idle(h, m, "developing")
    await wake(h, m, head(H1))
    await debugged(env, h, m)


@pytest.mark.parametrize("adviser", ["debugger", "cleaner-advice"])
async def test_a_go_with_advice_matches_again_while_the_snippet_pr_is_open(env, adviser):
    """A develop round that failed and was debugged: a go that carries advice (the debugger's, the cleaner's
    after its diagnosis) starts from the match, whose agent reads the advice, and a reader on main that fits
    ends the round at review. Any other go resumes the PR."""
    m = Mocks(debug_cycles=1, outcome="needs_snippet", states={f"crop-{H1}": ("UNKNOWN", None)})
    async with started(env, m, signal="restart") as h:
        await debugged_in_develop(env, h, m)
        m.outcome = "matched"  # the advice names a reader that is on main
        await h.signal(MiaDataDataset.restart, ev("go", "21", actor=adviser))
        await idle(h, m, "review")
    assert m.stages == ["matching", "developing", "testing", "debugging", "matching", "testing", "review"]
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "debug", "match", "crop", "verify"]
    assert not any("resuming" in stage[1] for stage in m.of("set_stage"))
    assert m.of("open_snippet_issue") == [("i7-e11", None)]


async def test_the_debuggers_go_resumes_the_snippet_pr_when_its_match_asks_for_a_reader_again(env):
    """The advised match finds no reader on main either: the open PR is that reader, and its head is tested."""
    m = Mocks(debug_cycles=1, outcome="needs_snippet", states={f"crop-{H1}": ("UNKNOWN", None)})
    async with started(env, m, signal="restart") as h:
        await debugged_in_develop(env, h, m)
        m.states = {}  # the infrastructure is fixed
        await h.signal(MiaDataDataset.restart, ev("go", "21", actor="debugger"))
        await idle(h, m, "review")
    assert m.stages == ["matching", "developing", "testing", "debugging", "matching", "testing", "review"]
    assert "resuming" in m.of("set_stage")[-2][1] and m.of("open_snippet_issue") == [("i7-e11", None)]


@pytest.mark.parametrize("now", [head(H1, "closed"), head(H1, "merged", MC)], ids=["closed", "merged"])
async def test_go_after_needs_human_with_a_finished_pr_matches_again(env, now):
    """The PR is gone by the time `go` arrives: a new match, and a reader it asks for gets a new snippets issue."""
    m = Mocks(outcome="needs_snippet", states={f"crop-{H1}": ("UNKNOWN", None)})
    async with started(env, m) as h:
        await to_needs_human_in_develop(h, m)
        m.pr_now = now
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await idle(h, m, "developing")
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "developing", "testing", "failed", "matching", "developing"]
    assert m.of("pr_state") == [(5,), (5,)] and [a[1] for a in m.of("submit")] == ["match", "crop", "match"]
    assert m.of("open_snippet_issue") == [("i7-e11", None), ("i7-e12", None)]
    assert (st.round_id, st.snippet_issue, st.pr) == ("i7-e12", 100, None) and not m.of("refuse_label")


# --- the poll activity -------------------------------------------------------------------------


class PollGH(FakeGH):
    def __init__(self, repo, issues=None, events=None, perms=None, prs=None, changed=None, status=None):
        super().__init__(repo, issues=issues)
        self.events, self.perms, self.prs = events or {}, perms or {}, prs or []
        self.changed, self.status, self.set = changed or {}, status or {}, []

    async def list_issues(self, labels=None, state=None, **_):
        return [i for i in self.issues.values() if not labels or labels in {x["name"] for x in i["labels"]}]

    async def label_event(self, n, label):
        return self.events[(n, label)]

    async def permission(self, login):
        return self.perms.get(login, "none")

    async def list_prs(self, state="all", per_page=50):
        return self.prs

    async def pr_files(self, n):
        return self.changed[n]

    async def statuses(self, sha):
        return self.status.get(sha, [])

    async def set_status(self, sha, context, state, description, target_url):
        self.set.append((sha, context, state, description))


class FakeClient:
    def __init__(self):
        self.started, self.signals = [], []

    async def start_workflow(self, wf, arg, **kw):
        self.started.append((wf, arg, kw))

    def get_workflow_handle(self, wid):
        signals = self.signals

        class H:
            async def signal(self, name, arg):
                signals.append((wid, name, arg))

        return H()


def issue(n, *labels, body="", user="bot"):
    return {"number": n, "body": body, "labels": [{"name": x} for x in labels], "user": {"login": user}}


def pr(n, ref, sha, state="open", repo="o/snippets"):
    return {
        "number": n,
        "state": state,
        "updated_at": "2099-01-01T00:00:00Z",
        "merged_at": None,
        "merge_commit_sha": None,
        "head": {"ref": ref, "sha": sha, "repo": {"full_name": repo}},
    }


def _poll(tmp_path, data, snip):
    fc = FakeClient()
    cfg = make_cfg(tmp_path, service={"url": "http://svc.example.org:8765"})
    return Activities(cfg, data, snip, None, None, bot="bot", client=fc), fc


async def test_poll_sets_status_for_non_snippet_pr(tmp_path):
    h = ["1" * 40, "2" * 40, "3" * 40, "4" * 40]
    snip = PollGH(
        "o/snippets",
        issues={
            100: issue(100, body="<!-- mia-data: issue=1 round=i1-e99 -->\n\nNew snippet"),
            101: issue(101, body="<!-- mia-data: issue=2 round=i2-e9 -->", user="mallory"),
        },
        prs=[
            pr(5, "docs", h[0]),
            pr(6, "feature", h[1]),
            pr(7, "docs2", h[2]),
            pr(8, "snippets/issue-100", h[3]),
            pr(9, "snippets/issue-101", h[3]),
        ],
        changed={
            5: ["README.md"],
            6: ["snippets/x/snippet.py"],
            7: ["SPEC.md"],
            8: ["snippets/x/snippet.py"],
            9: ["snippets/y/snippet.py"],
        },
        status={h[2]: [{"context": "mia-data/crops", "state": "success"}]},
    )
    a, fc = _poll(tmp_path, PollGH("o/data"), snip)
    await a.poll()
    assert snip.set == [(h[0], "mia-data/crops", "success", "no snippet in this PR")]
    assert fc.signals == [("mia-data/o/data/issue-1", "pr_update", PrUpdate(8, h[3], "open", None, 100, False))]


async def test_poll_signals_a_pr_only_when_it_changes(tmp_path):
    k = issue(100, body="<!-- mia-data: issue=1 round=i1-e99 -->")
    snip = PollGH(
        "o/snippets",
        issues={100: k},
        prs=[pr(8, "snippets/issue-100", H1)],
        changed={8: ["snippets/x/a.py"]},
        status={H1: [{"context": "mia-data/crops", "state": "pending"}]},
    )
    a, fc = _poll(tmp_path, PollGH("o/data"), snip)
    await a.poll()
    await a.poll()  # nothing changed: no signal
    snip.prs[0]["head"]["sha"] = H2
    snip.status[H2] = snip.status[H1]
    await a.poll()
    k["labels"] = [{"name": "snippets:needs-human"}]
    await a.poll()
    assert [(u.head_sha, u.needs_human) for _, _, u in fc.signals] == [(H1, False), (H2, False), (H2, True)]


HISTORY = Path(__file__).parent / "fixtures" / "mia" / "dataset_history.json"
DEVELOP_HISTORY = HISTORY.with_name("develop_history.json")
LOCK_HISTORY = HISTORY.with_name("dataset_history_c.json")
CANCEL_HISTORY = HISTORY.with_name("dataset_history_cancel.json")
AWAITED_HISTORY = HISTORY.with_name("dataset_history_b.json")
RESULTS_HISTORY = HISTORY.with_name("dataset_history_r.json")
VERDICT_HISTORY = HISTORY.with_name("dataset_history_v.json")
DECIDES_HISTORY = HISTORY.with_name("dataset_history_j.json")


def _scrub(obj):
    """Drop worker/client identities (pid@hostname) from a recorded history."""
    if isinstance(obj, dict):
        return {k: "test" if k == "identity" else _scrub(v) for k, v in obj.items()}
    return [_scrub(v) for v in obj] if isinstance(obj, list) else obj


async def test_replay_dataset_history(env):
    """A recorded match/test/review/approve/crop run must replay on today's MiaDataDataset. This fails when a change
    to workflow code alters the commands it issues, which breaks in-flight runs; guard an intended change with
    workflow.patched() and re-record: MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay"""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks()
        async with started(env, m) as h:
            await approve_review(h, m)
            await idle(h, m, "done")
            HISTORY.parent.mkdir(exist_ok=True)
            HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-recorded", HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_replay_develop_history(env):
    """The develop path's recorded run (test_develop_happy_path), as test_replay_dataset_history; same switch."""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"})
        async with started(env, m) as h:
            await happy(h, m)
            DEVELOP_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-develop", DEVELOP_HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_replay_dataset_history_c(env):
    """The match → done run under the `c-raw-lock` patch (smoke waits once for the raw lock), as
    test_replay_dataset_history. Re-record this one alone, so the older fixtures keep the unpatched path:
    MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay_dataset_history_c"""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks(lock_busy=1)
        async with started(env, m) as h:
            await until(lambda: m.of("take_lock"))
            await env.sleep(timedelta(minutes=3))
            await approve_review(h, m)
            await idle(h, m, "done")
            LOCK_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-raw-lock", LOCK_HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_replay_dataset_history_j(env):
    """A run under the `judge-decides` patch: the crop judge sends a matched set back, a person accepts that set
    while its rework runs, and it is saved; then they reject the saved set, which is revoked and reworked.
    Re-record this one alone, so the older fixtures keep their paths:
    MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay_dataset_history_j"""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks(crop_judge="active", judge_result=DOUBTED, look=AutodevLook(None, True, False, ""))
        async with started(env, m) as h:
            await idle(h, m, "developing")
            held = (await h.query(MiaDataDataset.status)).held
            await h.signal(MiaDataDataset.approve, accept(held))
            await idle(h, m, "done")
            saved = (await h.query(MiaDataDataset.status)).saved
            rejected = LabelEvent("reject", "31", "alice", "2999-01-01T00:00:00Z", saved.set_id)
            await h.signal(MiaDataDataset.reject, rejected)
            await idle(h, m, "developing")
            DECIDES_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-judge-decides", DECIDES_HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_replay_dataset_history_b(env):
    """The match → done run under the `stage-write-awaited` patch (every stage write is awaited), as
    test_replay_dataset_history. Re-record this one alone, so the older fixtures keep their paths:
    MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay_dataset_history_b"""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks()
        async with started(env, m) as h:
            await approve_review(h, m)
            await idle(h, m, "done")
            AWAITED_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-stage-awaited", AWAITED_HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_replay_dataset_history_r(env):
    """The match → done run under the `stage-results` patch (each stage pass writes a start and a result), as
    test_replay_dataset_history. Re-record this one alone, so the older fixtures keep their paths:
    MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay_dataset_history_r"""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks()
        async with started(env, m) as h:
            await approve_review(h, m)
            await idle(h, m, "done")
            RESULTS_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-stage-results", RESULTS_HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_replay_dataset_history_v(env):
    """The match → done run under the `verdict-first` patch (an approve that comes while the shadow judge looks is
    acted on, and the judge ends beside the round), as test_replay_dataset_history. Re-record this one alone:
    MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay_dataset_history_v"""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks(crop_judge="shadow", gate=asyncio.Event(), gate_on="judge")
        async with started(env, m) as h:
            await until(lambda: any("judge" in call[1] for call in m.of("await")))
            fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
            await h.signal(MiaDataDataset.approve, ev("approve", "12", shift(fin, 1)))
            await idle(h, m, "done")
            m.gate.set()
            await until(lambda: [a for a in m.of("comment") if a[1].startswith("judge crops")])
            await idle(h, m, "done")
            VERDICT_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-verdict-first", VERDICT_HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def cancelled_mid_match(h, m):
    """Cancel the round while its match job runs, then let the abandoned wait end."""
    await until(lambda: m.of("await"))
    await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
    await idle(h, m, "cancelled")
    m.gate.set()


async def test_cancel_ends_the_round_kills_its_job_and_a_new_go_starts_over(env):
    """spec webapp-actions §5: a round in progress is cancelled before anything else is asked of its dataset. The
    round ends now: its job is killed by id, its reservation settled, and the dataset is `cancelled`."""
    m = Mocks(gate=asyncio.Event(), gate_on="match")
    async with started(env, m) as h:
        await cancelled_mid_match(h, m)
        job = "job-mia-7-i7-e11-match-1"
        assert m.of("kill_job") == [(job,)] and m.of("await")[0][0] == job
        assert [a[1] for a in m.of("settle")] == ["mia-7-i7-e11-match-1"]  # the reservation does not stay open
        assert m.of("set_stage")[-1][:2] == ("cancelled", "cancelled by alice") and "read_match" not in m.names()
        # the cancelled pass writes its own result: the write that sets `cancelled` carries it, with the killed job
        assert m.of("result") == [("matching", "", "cancelled", ["/w/i7-e11/match-1"], False)]
        await h.signal(MiaDataDataset.go, ev("go", "13"))
        await idle(h, m, "review")
    assert m.stages == ["matching", "cancelled", "matching", "testing", "review"]
    assert not m.of("refuse_label")


async def test_cancel_at_review_ends_the_round_and_releases_nothing_twice(env):
    m = Mocks()
    async with started(env, m) as h:
        await idle(h, m, "review")
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await idle(h, m, "cancelled")
        assert m.of("kill_job") == [] and "publish_crops" not in m.names()  # no job was running
        fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
        await h.signal(MiaDataDataset.approve, ev("approve", "13", shift(fin, 5)))  # the set is no longer under review
        await until(lambda: m.of("refuse_label"))
    assert m.stages[-1] == "cancelled" and m.of("refuse_label")[0][0] == "data:approve"


async def test_a_cancel_with_no_round_running_is_refused(env):
    m = Mocks(outcome="blocked")
    async with started(env, m) as h:
        await idle(h, m, "blocked")
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await until(lambda: m.of("refuse_label"))
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))  # a redelivery: answered once
        await h.signal(MiaDataDataset.go, ev("go", "13"))
        await idle(h, m, "blocked")
    assert [a[0] for a in m.of("refuse_label")] == ["data:cancel"] and "no round" in m.of("refuse_label")[0][1]
    assert m.stages == ["matching", "blocked", "matching", "blocked"] and m.of("kill_job") == []


async def test_cancel_while_the_shadow_judge_job_runs_ends_the_round_and_publishes_nothing(env):
    """The stage is `review` and the crop judge's job still runs: a cancel landing in that wait must not be caught
    by the judge's best-effort handler, or the round would go on and a later approve would publish a set that
    then reads `cancelled`."""
    m = Mocks(crop_judge="shadow", gate=asyncio.Event(), gate_on="judge")
    async with started(env, m) as h:
        await until(lambda: any("judge" in call[1] for call in m.of("await")))
        assert m.stages[-1] == "review"
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await idle(h, m, "cancelled")
        m.gate.set()
        judge_job = next(call[0] for call in m.of("await") if "judge" in call[1])
        assert m.of("kill_job") == [(judge_job,)] and "publish_crops" not in m.names()
        assert not any("did not run to the end" in call[0] for call in m.of("comment"))  # not mistaken for a failure
        fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
        await h.signal(MiaDataDataset.approve, ev("approve", "13", shift(fin, 5)))
        await until(lambda: m.of("refuse_label"))
    assert m.stages[-1] == "cancelled" and "publish_crops" not in m.names()


async def test_cancel_while_a_job_is_being_submitted_kills_the_job_that_was_submitted(env):
    """A submit in flight is not interrupted by a cancel: the cluster gets the job. The round must wait for the
    submit to answer, then kill that job, or it runs on with nobody waiting on it."""
    m = Mocks(submit_gate=asyncio.Event(), gate=asyncio.Event(), gate_on="match")
    async with started(env, m) as h:
        await until(lambda: m.of("submit"))
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await asyncio.sleep(0.5)
        assert m.stages == ["matching"]  # not cancelled while the job's fate is unknown
        m.submit_gate.set()  # the cluster has the job now
        await idle(h, m, "cancelled")
        m.gate.set()
    assert m.of("kill_job") == [("job-mia-7-i7-e11-match-1",)]
    assert [a[1] for a in m.of("settle")] == ["mia-7-i7-e11-match-1"]


async def test_a_job_that_cannot_be_killed_is_said_and_the_dataset_needs_a_human(env):
    m = Mocks(gate=asyncio.Event(), gate_on="match", kill_error="the broker is away")
    async with started(env, m) as h:
        await until(lambda: m.of("await"))
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await idle(h, m, "failed")
        m.gate.set()
    detail = m.of("set_stage")[-1][1]
    assert "job-mia-7-i7-e11-match-1" in detail and "may still be running" in detail and "the broker is away" in detail
    assert "cancelled" not in m.stages


async def test_cancel_kills_the_job_before_the_raw_lock_is_released(env):
    """One writer per raw cache: the lock a job ran under is given up only once the job is dead."""
    m = Mocks(gate=asyncio.Event(), gate_on="crop")
    async with started(env, m) as h:
        await until(lambda: any("crop" in call[1] for call in m.of("await")))
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await idle(h, m, "cancelled")
        m.gate.set()
    names = m.names()
    assert names.index("kill_job") < names.index("release_lock") and m.of("release_lock")


async def test_a_cancel_that_arrives_when_a_rollover_is_due_is_acted_on_not_lost(env, monkeypatch):
    due = {"on": False}
    monkeypatch.setattr(workflows, "_rollover_due", lambda: due["on"] and workflow.info().continued_run_id is None)
    m = Mocks()
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await idle(h, m, "review")
        due["on"] = True  # the history has grown past the server's suggestion: the next activation sees it
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await idle(h, m, "cancelled")


async def test_run_again_after_cancelling_a_develop_round_reuses_its_snippet_issue(env):
    """A cancel while autodev writes the snippet leaves its issue and PR open; the next round must not ask
    autodev for the same snippet a second time."""
    m = Mocks(outcome="needs_snippet")
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await idle(h, m, "cancelled")
        await h.signal(MiaDataDataset.go, ev("go", "13"))
        await until(
            lambda: len(m.of("open_snippet_issue")) == 2 or m.stages[-1:] == ["developing"] and len(m.stages) > 3
        )
        await until(lambda: m.stages.count("developing") == 2 or m.stages.count("testing") == 2)
    remembered = [call[1] for call in m.of("open_snippet_issue")]
    assert remembered[0] is None and (
        len(remembered) == 1 or remembered[1] == 100
    )  # resumed, or reopened by its number


async def test_run_again_after_its_snippet_pr_was_closed_asks_for_a_new_snippet(env):
    """A person closed the round's snippet PR so that another reader gets written. autodev has nothing left to do
    in that PR's issue, so the next round opens a new one and does not read the closed PR as its own stop."""
    m = Mocks(outcome="needs_snippet")
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1, "closed"))
        await idle(h, m, "blocked")
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await until(lambda: m.stages.count("developing") == 2)
    assert m.of("open_snippet_issue") == [("i7-e11", None), ("i7-e12", None)]


async def test_replay_dataset_history_cancel(env):
    """A round cancelled while its match job ran, as test_replay_dataset_history. Re-record this one alone:
    MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay_dataset_history_cancel"""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks(gate=asyncio.Event(), gate_on="match")
        async with started(env, m) as h:
            await cancelled_mid_match(h, m)
            CANCEL_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-cancelled", CANCEL_HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_every_job_is_reserved_against_the_root_and_settled(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
    assert [a[2] for a in m.of("reserve")] == ["match", "crop", "verify"]
    assert [a[1] for a in m.of("reserve")] == [a[0] for a in m.of("submit")]  # the reservation id is the job key
    assert [a[0] for a in m.of("reserve")] == [PINS.root] * 3 and m.of("reserve")[0][3] == 5.0  # the agent's cap
    assert [a[1] for a in m.of("settle")] == [a[1] for a in m.of("reserve")]  # one settle per reservation
    assert [a[3] for a in m.of("settle")] == [0, 4096, 0]  # the crop job's download is charged to the root
    assert [a[2] for a in m.of("settle")] == [0.75, None, None]  # and the agent's cost, where a job had an agent
    assert m.names().index("reserve") < m.names().index("submit")


async def test_a_refused_reservation_ends_the_round_naming_the_root(env):
    reason = "root o/data#7: usd 99.00 reserved or spent + 5.00 would exceed max_usd 100"
    m = Mocks(reservations={"match": Reservation(False, reason)})
    async with started(env, m) as h:
        await idle(h, m, "blocked")
    assert "submit" not in m.names()
    assert m.of("set_stage")[-1][1] == f"root budget: {reason}"


async def test_a_paused_operator_stops_the_round_before_its_first_job(env):
    m = Mocks(paused=True)
    async with started(env, m) as h:
        await idle(h, m, "blocked")
    assert m.of("set_stage")[-1][1].startswith("paused by the operator") and "submit" not in m.names()


# --- human verdicts are assessments -------------------------------------------------


async def test_an_approve_is_written_as_an_assessment_before_the_crops_are_published(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m, eid="12")
        await idle(h, m, "done")
    (assessed,) = m.of("assess")  # the subject is <round id>:<test id>
    assert assessed[:2] == ("crops", "crop_set") and assessed[2].startswith("i7-e11:")
    assert assessed[3:5] == ("approve", "human:alice") and assessed[6] == "12"  # the approve action's own id
    assert m.names().index("assess") < m.names().index("publish_crops")


async def test_an_approve_the_policy_refuses_publishes_nothing(env):
    m = Mocks(assess_authorized=False)
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "blocked")
    assert "publish_crops" not in m.names()
    assert m.of("set_stage")[-1][1] == (
        "approve recorded as as-1, but saving the crops is not authorized: rejected by human:bob at t: wrong channel"
    )


async def test_a_reject_is_written_as_an_assessment(env):
    m = Mocks()
    async with started(env, m) as h:
        await reject_review(h, m)
        await until(lambda: m.of("open_snippet_issue"))
    assert [a[3:5] for a in m.of("assess")] == [("reject", "human:alice")]


async def test_pr_head_verdicts_are_assessed_on_the_head_sha(env):
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await happy(h, m)
    assert m.of("assess")[-1][:5] == ("crops", "pr_head", H1, "approve", "human:alice")
    assert m.names().index("assess") < m.names().index("comment", m.names().index("assess"))  # before `/approve`


# --- revoking a published dataset ----------------------------------------------------

REVOKE_AT = "2030-01-01T00:00:00Z"


async def test_revoke_after_done_ends_in_needs_human_and_lists_the_blast_radius(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
        await h.signal(MiaDataDataset.revoke, ev("revoke", "77", REVOKE_AT))
        await idle(h, m, "blocked")
    assert m.of("revoke_dataset") == [("i7-e11", "alice", "77")]
    detail, extra = m.of("set_stage")[-1][1:3]
    assert (
        detail == "revoked by alice: the dataset row is `revoked`, 1 artifact quarantined; running it again repairs it"
    )
    assert "art1" in extra and "i3-e4" in extra


async def test_revoke_during_a_later_review_is_taken_and_ends_the_round(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
        await h.signal(MiaDataDataset.go, ev("go", "13"))
        await idle(h, m, "review")
        await h.signal(MiaDataDataset.revoke, ev("revoke", "77", REVOKE_AT))
        await idle(h, m, "blocked")
    assert m.of("revoke_dataset") == [("i7-e13", "alice", "77")] and len(m.of("publish_crops")) == 1


async def test_revoke_with_nothing_published_is_refused(env):
    m = Mocks(outcome="blocked", revocation=Revocation(""))  # the activity finds no manifest
    async with started(env, m) as h:
        await idle(h, m, "blocked")
        await h.signal(MiaDataDataset.revoke, ev("revoke", "77", REVOKE_AT))
        await until(lambda: m.of("refuse_label"))
    assert m.of("refuse_label")[-1][:2] == ("data:revoke", "nothing to revoke: this dataset has no saved crops")
    assert len(m.of("revoke_dataset")) == 1


async def test_a_refused_revoke_during_review_leaves_the_round_under_review(env):
    m = Mocks(revocation=Revocation(""))
    async with started(env, m) as h:
        await idle(h, m, "review")
        await h.signal(MiaDataDataset.revoke, ev("revoke", "77", REVOKE_AT))
        await until(lambda: m.of("refuse_label"))
        await approve_review(h, m)
        await idle(h, m, "done")
    assert m.of("refuse_label")[-1][0] == "data:revoke" and len(m.of("publish_crops")) == 1


async def test_a_failed_submit_settles_its_reservation(env):
    m = Mocks(submit_error="csub refused the job")
    async with started(env, m) as h:
        await idle(h, m, "failed")
    assert [a[1] for a in m.of("settle")] == [a[1] for a in m.of("reserve")] == [m.of("submit")[0][0]]


async def test_a_pause_mid_round_stops_at_the_next_job(env):
    m = Mocks(paused=[False, True])
    async with started(env, m) as h:
        await idle(h, m, "blocked")
    assert m.of("set_stage")[-1][1].startswith("paused by the operator") and "submit" not in m.names()
    assert m.of("prepare_match")  # the round got past admission and stopped at its first job


async def test_a_concurrency_refusal_waits_and_tries_again(env):
    busy = Reservation(False, "root o/data#7: 20 jobs running, max_concurrent_jobs 20", wait=True)
    m = Mocks(reservations={"match": [busy, Reservation(True)]})
    async with started(env, m) as h:
        await until(lambda: m.of("reserve"))
        await env.sleep(timedelta(minutes=3))  # past the first wait (poll_minutes)
        await idle(h, m, "review")
    assert [a[2] for a in m.of("reserve")][:2] == ["match", "match"] and len(m.of("submit")) == 3


# --- the crop judge in shadow (spec autonomous-gates §9) -------------------------------------------------


async def test_shadow_judge_runs_after_the_test_and_the_human_still_decides(env):
    m = Mocks(crop_judge="shadow")
    async with started(env, m) as h:
        await idle(h, m, "review")
        names = m.names()
        review = [at for at, name in enumerate(names) if name == "set_stage"][2]  # it carries the crops report
        assert m.stages[2] == "review" and "verified" in m.of("result")[1][1]
        assert review < names.index("prepare_judge") < names.index("read_judge")  # the gate opens first
        (prepared,) = m.of("prepare_judge")
        assert (
            prepared[0] == "crops" and prepared[3] == 16 and prepared[4] == SHA[:12]
        )  # every verified crop is evidence
        judged = [a for a in m.of("submit") if a[1] == "judge"]
        assert len(judged) == 1 and judged[0][2].workspace == f"/w/i7-e11/judge-crops-{SHA[:12]}-1"
        (read,) = m.of("read_judge")
        assert read[1:4] == ("crops", "crop_set", f"i7-e11:i7-e11-{SHA[:12]}") and read[4] == "shadow"
        judge_comments = [a for a in m.of("comment") if a[1].startswith("judge crops i7-e11")]
        assert judge_comments == [("Crop judge: approve", f"judge crops i7-e11 {SHA}", "data", None)]
        assert "publish_crops" not in names  # shadow: the verdict decided nothing
        await approve_review(h, m)
        await idle(h, m, "done")
    assert "publish_crops" in m.names()
    # the judge's own entry links its job: review's result, which reports the person's verdict, does not
    assert next(r for r in m.of("result") if r[0] == "review")[3] == []


async def test_a_verdict_given_while_the_crop_judge_looks_is_acted_on_at_once(env):
    """Dataset 27657349, 2026-11-17: an approve clicked 78 s into `review` waited 45 s, until the judge's job ended.
    The round takes a person's verdict when it comes; the judge's reading ends beside it, on record only."""
    m = Mocks(crop_judge="active", gate=asyncio.Event(), gate_on="judge")
    async with started(env, m) as h:
        await until(lambda: any("judge" in call[1] for call in m.of("await")))
        fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
        await h.signal(MiaDataDataset.approve, ev("approve", "12", shift(fin, 1)))
        await idle(h, m, "done")  # the judge's job still runs
        assert "publish_crops" in m.names() and "read_judge" not in m.names() and m.of("kill_job") == []
        await h.signal(MiaDataDataset.go, ev("go", "13"))  # the next round waits for the judge to end
        m.gate.set()
        await idle(h, m, "review")
    (read,) = [call for call in m.of("read_judge") if call[3] == f"i7-e11:i7-e11-{SHA[:12]}"]
    assert read[4] == "shadow"  # a person decided: an active judge's approve grants nothing after that
    assert [a for a in m.of("comment") if a[1] == f"judge crops i7-e11 {SHA}"]  # its entry is written all the same
    assert [a[1] for a in m.of("settle") if "i7-e11-judge" in a[1]]  # and its reservation settled
    assert m.stages == ["matching", "testing", "review", "done", "matching", "testing", "review"]
    assert not m.of("refuse_label")


async def test_a_cancel_after_a_verdict_stops_the_crop_judge_that_still_looks(env):
    """A reject sends the round on to autodev while the judge's job runs; a cancel there kills that job too."""
    m = Mocks(crop_judge="shadow", gate=asyncio.Event(), gate_on="judge")
    async with started(env, m) as h:
        await until(lambda: any("judge" in call[1] for call in m.of("await")))
        fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
        await h.signal(MiaDataDataset.reject, ev("reject", "12", shift(fin, 1)))
        await idle(h, m, "developing")
        assert "read_judge" not in m.names()
        await h.signal(MiaDataDataset.cancel, ev("cancel", "13"))
        await idle(h, m, "cancelled")
        m.gate.set()
    judge_job = next(call[0] for call in m.of("await") if "judge" in call[1])
    assert m.of("kill_job") == [(judge_job,)] and "read_judge" not in m.names()
    assert [a[1] for a in m.of("settle") if "judge" in a[1]]  # the reservation does not stay open


async def test_a_cancel_while_the_crop_judges_job_is_being_submitted_kills_that_job(env):
    """The judge's task runs beside the round's: its submit in flight is waited for like the round's own, so the
    job the cluster got is killed, never left running with nobody waiting on it."""
    m = Mocks(crop_judge="shadow", submit_gate=asyncio.Event(), gate=asyncio.Event(), gate_on="verify")
    m.submit_gate.set()
    async with started(env, m) as h:
        await until(lambda: any("verify" in call[1] for call in m.of("await")))
        m.submit_gate.clear()  # the next submit is the judge's: it hangs in the cluster's hands
        m.gate.set()
        await until(lambda: [a for a in m.of("submit") if a[1] == "judge"])
        await h.signal(MiaDataDataset.cancel, ev("cancel", "12"))
        await asyncio.sleep(0.5)
        assert m.stages[-1] == "review"  # not cancelled while the job's fate is unknown
        m.submit_gate.set()
        await idle(h, m, "cancelled")
    (judge_job,) = [f"job-{a[0]}" for a in m.of("submit") if a[1] == "judge"]
    assert m.of("kill_job") == [(judge_job,)] and "read_judge" not in m.names()


async def test_a_judge_that_leaves_no_entry_of_its_own_is_linked_by_the_reviews_result(env):
    m = Mocks(crop_judge="shadow", judge_result=ApplicationError("the registry is away", non_retryable=True))
    async with started(env, m) as h:
        await idle(h, m, "review")
        await until(lambda: any("did not run to the end" in call[0] for call in m.of("comment")))
        await approve_review(h, m)
        await idle(h, m, "done")
    assert next(r for r in m.of("result") if r[0] == "review")[3] == [f"/w/i7-e11/judge-crops-{SHA[:12]}-1"]


async def test_shadow_judge_failure_is_a_comment_not_a_stop(env):
    m = Mocks(crop_judge="shadow", states={f"judge-crops-{SHA[:12]}": ("FAILED", 1)})
    async with started(env, m) as h:
        await idle(h, m, "review")
        await until(lambda: [a for a in m.of("comment") if a[1].startswith("judge crops")])
    assert "read_judge" in m.names()  # the failure is an escalate on record
    (failed,) = [a for a in m.of("comment") if a[1].startswith("judge crops")]
    assert failed[0].startswith("The crop judge's judge job job-mia-7-i7-e11-judge-")
    assert "ended FAILED (exit 1); its verdict is recorded as an escalation." in failed[0]
    assert m.stages == ["matching", "testing", "review"]


async def test_a_refused_reservation_skips_the_shadow_judge_and_the_round_goes_on(env):
    m = Mocks(crop_judge="shadow", reservations={"judge": [Reservation(False, "would exceed max_usd 1")]})
    async with started(env, m) as h:
        await idle(h, m, "review")
        await until(lambda: [a for a in m.of("comment") if a[1].startswith("judge crops")])
        (skipped,) = [a for a in m.of("comment") if a[1].startswith("judge crops")]
        assert (
            skipped[0] == "The crop judge was skipped: root budget: would exceed max_usd 1. A person's verdict decides."
        )
        assert "prepare_judge" in m.names() and all(a[1] != "judge" for a in m.of("submit"))
        await approve_review(h, m)
        await idle(h, m, "done")
    assert "publish_crops" in m.names()


async def test_judge_off_runs_nothing(env):
    m = Mocks()
    async with started(env, m) as h:
        await idle(h, m, "review")
    assert "prepare_judge" not in m.names() and all(a[1] != "judge" for a in m.of("submit"))


async def test_judge_is_budgeted_and_settled(env):
    m = Mocks(crop_judge="shadow")
    async with started(env, m) as h:
        await idle(h, m, "review")
    (reserved,) = [a for a in m.of("reserve") if a[2] == "judge"]
    assert reserved[3] == 3.5 and reserved[1].startswith("mia-7-i7-e11-judge-")  # the limits' value, not a default
    assert any(a[1] == reserved[1] for a in m.of("settle"))


async def test_a_pr_heads_set_is_judged_too(env):
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"}, crop_judge="shadow")
    async with started(env, m) as h:
        await to_review(h, m)
    (read,) = m.of("read_judge")
    assert read[3] == f"i7-e11:i7-e11-{H1[:12]}"
    names = m.names()
    status_at = max(i for i, name in enumerate(names) if name == "set_crop_status")
    assert m.of("set_crop_status")[-1][1] == "success" and status_at < names.index("prepare_judge")  # status first


# --- the crop judge with authority (judge.crops.mode: active) ---------------------------------------------

GRANTED = JudgeResult("approve", "approve", 0.9, "fine", "as-j", 0.5, "Crop judge: approve", authorized=True)


async def test_an_active_judges_authorized_approve_saves_the_set_with_no_person(env):
    m = Mocks(crop_judge="active", judge_result=GRANTED)
    async with started(env, m) as h:
        await idle(h, m, "done")
    names = m.names()
    assert names.index("read_judge") < names.index("publish_crops") and "assess" not in names  # no person's verdict
    assert m.of("read_judge")[0][4] == "active"
    assert "Approved by the crop judge." in next(r for r in m.of("result") if r[0] == "review")[1]


async def test_the_saved_set_is_kept_with_the_dataset_across_rounds(env):
    """spec judge-decides §5.4: a set holds together what a later verdict needs, and outlives its round."""
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
        state = await h.query(MiaDataDataset.status)
        saved = state.saved
        assert saved.set_id == f"i7-e11:{state.tests[-1].test_id}" and saved.round_id == "i7-e11"
        assert (saved.binding, saved.pins.record, saved.test.sha) == ("/s/binding-1.yaml", state.pins.record, SHA)
        assert saved.test.approved_by == "alice" and state.held is None
        await h.signal(MiaDataDataset.go, ev("go", "13"))
        await idle(h, m, "review")
        state = await h.query(MiaDataDataset.status)
        # the next round leaves the saved set alone; the set takes its own round's pass numbers with it
        kept = dataclasses.replace(saved, passes={"matching": 1, "testing": 1, "review": 1})
        assert state.round_id == "i7-e13" and state.saved == kept
        assert state.tests[-1].test_id != saved.test.test_id or state.round_id != saved.round_id


async def test_a_set_the_judge_approved_says_so(env):
    m = Mocks(crop_judge="active", judge_result=GRANTED)
    async with started(env, m) as h:
        await idle(h, m, "done")
        assert (await h.query(MiaDataDataset.status)).saved.test.approved_by == "judge"


@pytest.mark.parametrize(
    "judged",
    [
        JudgeResult("approve", "approve", 0.9, "fine", "as-j", 0.5, "Crop judge: approve"),  # not authorized
        JudgeResult("escalate", "escalate", 0.3, "unsure", "as-j", 0.5, "Crop judge: escalate"),
    ],
)
async def test_an_active_judge_that_does_not_grant_leaves_the_set_to_a_person(env, judged):
    m = Mocks(crop_judge="active", judge_result=judged)
    async with started(env, m) as h:
        await idle(h, m, "review")
        assert "publish_crops" not in m.names()
        await approve_review(h, m)
        await idle(h, m, "done")
    assert "publish_crops" in m.names() and m.of("assess")[0][3] == "approve"


DOUBTED = JudgeResult(
    "approve",
    "reject",
    0.9,
    "one doubt",
    "as-j",
    0.5,
    "Crop judge: reject",
    findings=[
        {"item": "c0", "severity": "ok", "note": "dim"},
        {"item": "c1", "severity": "suspect", "note": "labels look shifted"},
    ],
    worst="suspect",
)


def revisions(m):
    return [body for body in pr_comments(m) if body.startswith("/revise")]


async def test_an_active_judge_that_finds_a_suspect_crop_sends_a_matched_set_to_the_developer(env):
    """spec judge-decides §3, §4.1: the findings go to the developer as a person's reject does."""
    m = Mocks(crop_judge="active", judge_result=DOUBTED)
    async with started(env, m) as h:
        await idle(h, m, "developing")
        state = await h.query(MiaDataDataset.status)
    (asked,) = m.of("open_snippet_issue")
    binding, draft = asked[2], asked[3]
    assert binding == "/s/binding-1.yaml" and draft["what_fails"].startswith(f"Request: {state.held.request}\n")
    assert "- c1 [suspect]: labels look shifted" in draft["what_fails"] and "c0" not in draft["what_fails"]
    held = state.held
    assert (held.sent_back_by, held.worst, held.request) == ("judge", "suspect", f"{held.set_id}#1")
    assert held.set_id == f"i7-e11:{held.test.test_id}" and held.binding == "/s/binding-1.yaml"
    assert held.findings == [{"item": "c1", "severity": "suspect", "note": "labels look shifted"}]
    assert (held.opened_issue, held.snippet_issue, state.reworks) == (True, 100, 1)
    # the developer is shown what the judge saw: the judge's workspace, under the snippets issue it answers
    (shared,) = m.of("share_evidence")
    assert shared[0] == "i7-e11" and "judge" in shared[1] and shared[2:] == (100, held.set_id)
    assert m.names().index("open_snippet_issue") < m.names().index("share_evidence")
    assert "assess" not in m.names() and "publish_crops" not in m.names()  # no person, and nothing saved
    assert "Sent back by the crop judge" in next(r for r in m.of("result") if r[0] == "review")[1]


async def test_an_active_judge_sends_a_pr_heads_set_back_once_and_then_leaves_it_to_a_person(env):
    """spec judge-decides §4.3: one automatic rework by default; the next doubted set waits at review."""
    m = Mocks(outcome="needs_snippet", crop_judge="active", judge_result=DOUBTED)
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await until(lambda: revisions(m))
        await idle(h, m, "developing")
        assert m.stages[-3:] == ["testing", "review", "developing"]
        state = await h.query(MiaDataDataset.status)
        assert revisions(m) == [f"/revise Request: {state.held.request}\n" + revisions(m)[0].split("\n", 1)[1]]
        assert (state.held.test.sha, state.held.pr, state.held.opened_issue, state.reworks) == (H1, 5, False, 1)
        assert m.of("share_evidence")[0][2:] == (100, state.held.set_id)  # before the request goes to the PR
        assert (
            m.names().index("share_evidence")
            < [i for i, c in enumerate(m.calls) if c[0] == "comment" and c[1][0].startswith("/revise")][0]
        )
        await wake(h, m, head(H2))
        await idle(h, m, "review")
        state = await h.query(MiaDataDataset.status)
        assert len(revisions(m)) == 1 and state.reworks == 1 and state.held.test.sha == H1  # h1's set stays held
        waits = [a[0] for a in m.of("comment") if "waits for your verdict" in a[0]]
        assert len(waits) == 1 and "max_reworks" in waits[0]
        # a person's verdict starts the count over: their reject goes to the developer, and so does the judge's next
        await reject_review(h, m)
        await until(lambda: len(revisions(m)) == 2)
        await idle(h, m, "developing")
        assert (await h.query(MiaDataDataset.status)).reworks == 0
        await wake(h, m, head(H3))
        await until(lambda: len(revisions(m)) == 3)
        assert (await h.query(MiaDataDataset.status)).reworks == 1


def test_the_rework_request_names_itself_and_lists_each_finding_on_a_line():
    from mia_data.crop_test import rework_request

    findings = [
        {"item": "c1", "severity": "suspect", "note": "labels look shifted"},
        {"item": "c2", "severity": "bad", "note": ""},
    ]
    text = rework_request(findings, "i7-e11:t1#1")
    assert text.splitlines()[0] == "Request: i7-e11:t1#1"
    assert [line for line in text.splitlines() if line.startswith("- ")] == [
        "- c1 [suspect]: labels look shifted",
        "- c2 [bad]:",
    ]
    assert "no_change.json" in text and "make no commits" in text


DOUBTED_BAD = dataclasses.replace(
    DOUBTED, findings=[{"item": "c1", "severity": "bad", "note": "header read as pixels"}], worst="bad"
)
SAID = "**Nothing needs changing.** The answer to each finding:\n\n- c1: the label's own shift"


def answered(pr, request):
    """What a look finds once autodev validated a "nothing needs changing" for `request`."""
    return AutodevLook(pr, False, False, "", SAID, "2999-01-01T00:00:00Z", no_change=request)


async def sent_back_pr(env, h, m):
    """The judge sent h1's set back as a revision request and autodev has not answered yet; the held set."""
    await idle(h, m, "developing")
    await wake(h, m, head(H1))
    await until(lambda: revisions(m))
    await idle(h, m, "developing")
    await env.sleep(LOOK)
    assert m.stages[-1] == "developing"
    return (await h.query(MiaDataDataset.status)).held


async def test_the_developers_nothing_to_change_approves_a_suspect_only_set(env):
    """spec judge-decides §4.2: the judge doubted, the developer looked and found the crops right: saved."""
    quiet = AutodevLook(5, False, False, "")
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"}, crop_judge="active", judge_result=DOUBTED, look=quiet)
    async with started(env, m) as h:
        held = await sent_back_pr(env, h, m)
        m.look = answered(5, held.request)
        await env.sleep(LOOK)
        await idle(h, m, "merging")
        assert pr_comments(m)[-1] == f"/approve {H1}"
        (assessed,) = m.of("assess")
        assert assessed[1:5] == ("pr_head", H1, "approve", "policy:developer")
        said = [r[1] for r in m.of("result") if r[0] == "developing"][-1]
        assert "changed nothing" in said and "> - c1: the label's own shift" in said
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
        assert (await h.query(MiaDataDataset.status)).saved.test.approved_by == "policy:developer"


async def test_the_developers_nothing_to_change_on_a_bad_set_waits_for_a_person(env):
    quiet = AutodevLook(5, False, False, "")
    m = Mocks(outcome="needs_snippet", crop_judge="active", judge_result=DOUBTED_BAD, look=quiet)
    async with started(env, m) as h:
        held = await sent_back_pr(env, h, m)
        m.look = answered(5, held.request)
        await env.sleep(LOOK)
        await idle(h, m, "review")
        assert not m.of("assess") and "/approve" not in " ".join(pr_comments(m))
        detail = m.of("set_stage")[-1][1]
        assert "the crop judge found a bad crop" in detail and "waits for your verdict" in detail
        await approve_review(h, m, eid="13", dt=3)
        await idle(h, m, "merging")
    assert pr_comments(m)[-1] == f"/approve {H1}"


@pytest.mark.parametrize("no_change", ["", "i7-e11:another#1"])
async def test_a_no_commit_round_without_a_validated_answer_approves_nothing(env, no_change):
    """spec judge-decides §4.2: a round with no commits is not "right as they are"."""
    quiet = AutodevLook(5, False, False, "")
    m = Mocks(outcome="needs_snippet", crop_judge="active", judge_result=DOUBTED, look=quiet)
    async with started(env, m) as h:
        await sent_back_pr(env, h, m)
        m.look = AutodevLook(
            5, False, False, "", "could not reach the host", "2999-01-01T00:00:00Z", no_change=no_change
        )
        await env.sleep(LOOK)
        assert m.stages[-1] == "developing" and not m.of("assess")
        assert "/approve" not in " ".join(pr_comments(m))


async def test_the_developers_answer_on_an_issue_with_no_pull_request_saves_a_suspect_only_matched_set(env):
    m = Mocks(crop_judge="active", judge_result=DOUBTED, look=AutodevLook(None, False, False, ""))
    async with started(env, m) as h:
        await idle(h, m, "developing")
        held = (await h.query(MiaDataDataset.status)).held
        m.look = answered(None, held.request)
        await env.sleep(LOOK)
        await idle(h, m, "done")
        state = await h.query(MiaDataDataset.status)
    assert m.of("assess")[0][1:5] == ("crop_set", held.set_id, "approve", "policy:developer")
    assert m.of("publish_crops") == [(f"/w/i7-e11/crop-{SHA}-1", 16, SHA, "/s/binding-1.yaml")]
    assert state.saved.set_id == held.set_id and state.held is None


async def test_an_answer_on_an_issue_with_no_pull_request_brings_a_persons_rejected_set_back_to_review(env):
    """spec judge-decides §4.2: a developer's answer to a person's reject never approves; the person decides."""
    m = Mocks(look=AutodevLook(None, False, False, ""))
    async with started(env, m) as h:
        await reject_review(h, m)
        await idle(h, m, "developing")
        held = (await h.query(MiaDataDataset.status)).held
        draft = m.of("open_snippet_issue")[0][3]
        assert (held.sent_back_by, held.request) == ("alice", f"{held.set_id}#12")
        assert draft["what_fails"].startswith(f"Request: {held.request}\n")
        assert "the masks are shifted by one pixel" in draft["what_fails"]
        await env.sleep(LOOK)
        assert m.stages[-1] == "developing"
        m.look = answered(None, held.request)
        await env.sleep(LOOK)
        await idle(h, m, "review")
        assert [a[3] for a in m.of("assess")] == ["reject"] and "publish_crops" not in m.names()
        assert (await h.query(MiaDataDataset.status)).snippet_issue is None  # that issue is answered and done with
        await approve_review(h, m, eid="13", dt=3)
        await idle(h, m, "done")
    assert m.of("publish_crops") == [(f"/w/i7-e11/crop-{SHA}-1", 16, SHA, "/s/binding-1.yaml")]


def accept(held, eid="21", actor="alice"):
    """A person's approve that names the set that was sent back."""
    return LabelEvent("approve", eid, actor, "2999-01-01T00:00:00Z", held.set_id)


async def test_accepting_a_matched_set_under_rework_cancels_it_and_saves_that_set(env):
    """spec judge-decides §5.1: "cancel the new work and merge the old set", in one action."""
    m = Mocks(crop_judge="active", judge_result=DOUBTED, look=AutodevLook(None, True, False, ""))
    async with started(env, m) as h:
        await idle(h, m, "developing")
        held = (await h.query(MiaDataDataset.status)).held
        await h.signal(MiaDataDataset.approve, accept(held))
        await idle(h, m, "done")
        state = await h.query(MiaDataDataset.status)
    assert "cancelled" not in m.stages and m.stages[-2:] == ["developing", "done"]
    assert m.of("assess")[0][1:5] == ("crop_set", held.set_id, "approve", "human:alice")
    assert m.of("close_snippet_issue")[0][:2] == (100, None)  # the reader the send-back asked for is not wanted
    assert m.of("publish_crops") == [(f"/w/i7-e11/crop-{SHA}-1", 16, SHA, "/s/binding-1.yaml")]
    assert state.saved.set_id == held.set_id and state.saved.test.approved_by == "alice"
    assert state.held is None and state.accepting is None
    assert any("Accepted" in a[0] and "alice" in a[0] for a in m.of("comment"))


async def test_accepting_a_pr_heads_set_asks_autodev_to_merge_that_head_and_ignores_newer_heads(env):
    quiet = AutodevLook(5, True, False, "")
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"}, crop_judge="active", judge_result=DOUBTED, look=quiet)
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await until(lambda: revisions(m))
        await idle(h, m, "developing")
        await wake(h, m, head(H2))  # the developer's fix: doubted too, and the judge may not send back twice
        await idle(h, m, "review")
        held = (await h.query(MiaDataDataset.status)).held
        assert held.test.sha == H1
        await h.signal(MiaDataDataset.approve, accept(held))
        await idle(h, m, "merging")
        assert pr_comments(m)[-1] == f"/accept {H1}" and (await h.query(MiaDataDataset.status)).accepting == H1
        assert "its running job" in m.of("set_stage")[-1][1]
        tested = len([a for a in m.jobs() if a.n_crops])
        await wake(h, m, head(H3))  # a head autodev pushed before it read the accept: not tested, not judged
        await h.signal(MiaDataDataset.approve, accept(held, eid="22"))  # a second click: nothing merges twice
        await until(lambda: [a for a in m.of("refuse_label") if a[2] == "22"])
        assert len([a for a in m.jobs() if a.n_crops]) == tested and m.stages[-1] == "merging"
        assert [c for c in pr_comments(m) if c.startswith("/accept")] == [f"/accept {H1}"]
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
        state = await h.query(MiaDataDataset.status)
    assert m.of("publish_crops")[0][0].endswith(f"crop-{H1}-1") and m.of("publish_crops")[0][2] == MC
    assert state.saved.test.sha == H1 and state.held is None and state.accepting is None
    assert [a[1:5] for a in m.of("assess")] == [("pr_head", H1, "approve", "human:alice")]


async def test_accept_is_taken_after_the_rework_stopped(env):
    """spec judge-decides §5.1: the set stays acceptable when the rework ended blocked, failed or cancelled."""
    m = Mocks(crop_judge="active", judge_result=DOUBTED, look=AutodevLook(None, False, True, "I cannot do this"))
    async with started(env, m) as h:
        await idle(h, m, "developing")
        held = (await h.query(MiaDataDataset.status)).held
        await env.sleep(LOOK)
        await idle(h, m, "blocked")
        await h.signal(MiaDataDataset.approve, accept(held))
        await idle(h, m, "done")
    assert m.of("publish_crops") == [(f"/w/i7-e11/crop-{SHA}-1", 16, SHA, "/s/binding-1.yaml")]


async def test_an_accept_whose_crops_are_gone_is_refused_and_the_rework_goes_on(env):
    m = Mocks(crop_judge="active", judge_result=DOUBTED, look=AutodevLook(None, True, False, ""))
    async with started(env, m) as h:
        await idle(h, m, "developing")
        held = (await h.query(MiaDataDataset.status)).held
        m.verify_gone = True
        await h.signal(MiaDataDataset.approve, accept(held))
        await until(lambda: m.of("refuse_label"))
        assert "can no longer be accepted" in m.of("refuse_label")[0][1]
        await env.sleep(LOOK)  # the rework was not stopped for it: the round still waits for autodev
        assert m.stages[-1] == "developing" and "publish_crops" not in m.names() and not m.of("kill_job")
        assert (await h.query(MiaDataDataset.status)).held == held


async def test_a_save_that_fails_after_an_accept_rests_at_blocked_and_keeps_the_approval(env):
    m = Mocks(crop_judge="active", judge_result=DOUBTED, look=AutodevLook(None, True, False, ""))
    m.publish_error = "the dataset folder is another record's"
    async with started(env, m) as h:
        await idle(h, m, "developing")
        held = (await h.query(MiaDataDataset.status)).held
        await h.signal(MiaDataDataset.approve, accept(held))
        await idle(h, m, "blocked")
        state = await h.query(MiaDataDataset.status)
        detail = m.of("set_stage")[-1][1]
        assert f"The set {held.set_id} was accepted by alice" in detail and "Accept can be sent again" in detail
        assert "another record's" in detail and m.of("assess")[0][3] == "approve"  # the approval stands
        assert state.held.set_id == held.set_id and state.saved is None and state.accepting is None
        m.publish_error = None
        await h.signal(MiaDataDataset.approve, accept(held, eid="22"))  # and it can be sent again
        await idle(h, m, "done")


async def saved_then_rejected(h, m):
    """A matched set a person approved and that was saved, then their reject of the saved set; (saved, held)."""
    await approve_review(h, m)
    await idle(h, m, "done")
    saved = (await h.query(MiaDataDataset.status)).saved
    await h.signal(MiaDataDataset.reject, LabelEvent("reject", "31", "alice", "2999-01-01T00:00:00Z", saved.set_id))
    await idle(h, m, "developing")
    return saved, (await h.query(MiaDataDataset.status)).held


async def test_rejecting_a_saved_set_revokes_it_and_starts_the_rework(env):
    """spec judge-decides §5.2: "revoke and start the rework"."""
    m = Mocks(look=AutodevLook(None, True, False, ""))
    async with started(env, m) as h:
        saved, held = await saved_then_rejected(h, m)
        state = await h.query(MiaDataDataset.status)
    names = m.names()
    revoked = names.index("revoke_dataset")
    assert revoked < names.index("admit", revoked) < names.index("open_snippet_issue")
    assert m.of("revoke_dataset") == [("i7-e11", "alice", "31")]  # the saved set's own round
    (asked,) = m.of("open_snippet_issue")
    assert asked[:3] == ("i7-e31", None, "/s/binding-1.yaml")  # a round of its own, the rejected binding
    assert asked[3]["what_fails"].startswith(f"Request: {saved.set_id}#31\n")
    assert "the masks are shifted by one pixel" in asked[3]["what_fails"]
    assert state.round_id == "i7-e31" and state.saved is None
    assert (held.set_id, held.sent_back_by, held.round_id) == (saved.set_id, "alice", "i7-e11")
    assert any("revoked" in a[0] and "alice" in a[0] for a in m.of("comment"))


async def test_a_reject_that_names_another_set_than_the_saved_one_is_refused(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
        await h.signal(MiaDataDataset.reject, LabelEvent("reject", "31", "alice", "2999-01-01T00:00:00Z", "i7-e0:zz"))
        await until(lambda: m.of("refuse_label"))
        assert m.stages[-1] == "done" and "revoke_dataset" not in m.names()
        assert (await h.query(MiaDataDataset.status)).saved is not None


async def test_accepting_the_revoked_set_again_saves_it_under_its_own_round(env):
    """spec judge-decides §4.2, §5.5: the same set, accepted again, is saved again (which repairs by recipe)."""
    m = Mocks(look=AutodevLook(None, True, False, ""))
    async with started(env, m) as h:
        saved, held = await saved_then_rejected(h, m)
        await h.signal(MiaDataDataset.approve, accept(held, eid="41"))
        await idle(h, m, "done")
        state = await h.query(MiaDataDataset.status)
    the_set = (f"/w/i7-e11/crop-{SHA}-1", 16, SHA, "/s/binding-1.yaml")
    assert m.of("publish_crops") == [the_set, the_set]
    assert state.round_id == "i7-e11" and state.saved.set_id == saved.set_id and state.held is None
    assert m.of("close_snippet_issue")[0][:2] == (100, None)


async def test_the_developers_answer_to_a_reject_of_a_saved_set_brings_that_set_back_to_review(env):
    m = Mocks(look=AutodevLook(None, False, False, ""))
    async with started(env, m) as h:
        saved, held = await saved_then_rejected(h, m)
        m.look = answered(None, held.request)
        await env.sleep(LOOK)
        await idle(h, m, "review")
        state = await h.query(MiaDataDataset.status)
        assert state.round_id == "i7-e11" and state.tests[-1].test_id == saved.test.test_id
        assert len(m.of("publish_crops")) == 1  # an answer to a person's reject never approves
        await h.signal(
            MiaDataDataset.approve, LabelEvent("approve", "51", "alice", "2999-02-01T00:00:00Z", saved.set_id)
        )
        await idle(h, m, "done")
    assert len(m.of("publish_crops")) == 2 and m.of("publish_crops")[1][0] == f"/w/i7-e11/crop-{SHA}-1"


async def test_a_rollover_while_the_developer_works_on_a_set_with_no_pull_request_keeps_the_way_back(env, monkeypatch):
    """Review finding: after a continue-as-new at `developing`, the developer's answer brought the set back to
    review with nothing waiting for the verdict."""
    from temporalio import workflow

    m = Mocks(look=AutodevLook(None, False, False, ""))
    m.roll = False
    monkeypatch.setattr(workflows, "_rollover_due", lambda: m.roll and workflow.info().continued_run_id is None)
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await reject_review(h, m)
        await idle(h, m, "developing")
        held = (await h.query(MiaDataDataset.status)).held
        m.roll = True
        await env.sleep(LOOK)
        for _ in range(500):
            if (await h.describe()).run_id != h.first_run_id:
                break
            await asyncio.sleep(0.02)
        assert (await h.describe()).run_id != h.first_run_id
        m.roll = False
        m.look = answered(None, held.request)
        await env.sleep(LOOK)
        await idle(h, m, "review")
        await approve_review(h, m, eid="13", dt=3)
        await idle(h, m, "done")
    assert not m.of("refuse_label") and len(m.of("publish_crops")) == 1


async def test_a_set_brought_back_from_an_earlier_round_goes_on_counting_that_rounds_passes(env):
    """Review finding: with the round's pass counters reset, the entries of the restored round reused their
    markers and the ledger dropped them: the timeline was silent about the second review and its approval."""
    m = Mocks(look=AutodevLook(None, False, False, ""))
    async with started(env, m) as h:
        saved, held = await saved_then_rejected(h, m)
        assert held.passes == {"matching": 1, "testing": 1, "review": 1}  # what its round had begun
        m.look = answered(None, held.request)
        await env.sleep(LOOK)
        await idle(h, m, "review")
        state = await h.query(MiaDataDataset.status)
        assert state.round_id == "i7-e11" and state.passes["review"] == 2  # a second pass at review, not the first


async def test_a_reject_of_the_saved_set_that_cannot_start_its_rework_leaves_the_set_acceptable(env):
    """Review finding: the pause was on (or admission refused) after the revoke: the dataset was revoked with no
    held set, so the reject could not be undone."""
    m = Mocks(look=AutodevLook(None, True, False, ""))
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
        saved = (await h.query(MiaDataDataset.status)).saved
        m.paused = True
        await h.signal(MiaDataDataset.reject, LabelEvent("reject", "31", "alice", "2999-01-01T00:00:00Z", saved.set_id))
        await idle(h, m, "blocked")
        state = await h.query(MiaDataDataset.status)
        assert state.held.set_id == saved.set_id and state.held.sent_back_by == "alice" and state.saved is None
        assert "open_snippet_issue" not in m.names() and state.debugs == 0
        m.paused = False
        await h.signal(MiaDataDataset.approve, accept(state.held, eid="41"))
        await idle(h, m, "done")
    assert len(m.of("publish_crops")) == 2


async def test_a_set_keeps_what_its_merge_needs_and_an_accept_from_a_later_round_saves_it(env):
    """Review finding: `_restore` did not bring back the snippet's name and the seed, so an accepted pull-request
    set of an earlier round was merged but another set was made instead of saving it."""
    quiet = AutodevLook(5, True, False, "")
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"}, crop_judge="active", judge_result=DOUBTED, look=quiet)
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await until(lambda: revisions(m))
        await idle(h, m, "developing")
        state = await h.query(MiaDataDataset.status)
        held = state.held
        assert (held.snippet_name, held.seed) == (state.snippet_name, state.seed) and held.snippet_name
        await h.signal(MiaDataDataset.cancel, ev("cancel", "14"))
        await idle(h, m, "cancelled")
        m.pr_now = head(H2)
        m.judge_result = JudgeResult("escalate", "escalate", 0.3, "unsure", "as-j", 0.5, "Crop judge: escalate")
        await h.signal(MiaDataDataset.restart, ev("go", "15"))  # a later round picks the pull request up at h2
        await idle(h, m, "review")
        assert (await h.query(MiaDataDataset.status)).round_id == "i7-e15"
        await h.signal(MiaDataDataset.approve, accept(held, eid="41"))
        await idle(h, m, "merging")
        m.pr_now = None
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
        state = await h.query(MiaDataDataset.status)
    assert state.round_id == "i7-e11" and state.saved.set_id == held.set_id
    assert m.of("publish_crops")[-1][0] == f"/w/i7-e11/crop-{H1}-1" and m.of("publish_crops")[-1][2] == MC


async def test_accepting_a_matched_set_closes_the_pull_request_its_rework_opened(env):
    """Review finding (spec §5.1 step 1): the reader the send-back asked for is not wanted any more."""
    m = Mocks(crop_judge="active", judge_result=DOUBTED, look=AutodevLook(None, True, False, ""))
    async with started(env, m) as h:
        await idle(h, m, "developing")
        held = (await h.query(MiaDataDataset.status)).held
        m.judge_result = JudgeResult("escalate", "escalate", 0.3, "unsure", "as-j", 0.5, "Crop judge: escalate")
        await wake(h, m, head(H1))  # the developer's pull request: its head's set waits at review
        await idle(h, m, "review")
        await h.signal(MiaDataDataset.approve, accept(held))
        await idle(h, m, "done")
    assert m.of("close_snippet_issue")[0][:2] == (100, 5)
    assert m.of("publish_crops") == [(f"/w/i7-e11/crop-{SHA}-1", 16, SHA, "/s/binding-1.yaml")]


async def test_an_old_answer_never_approves_a_set_held_from_an_earlier_round(env):
    """Review finding: a later round that follows the same snippets issue found the old "nothing needs changing"
    marker and approved the earlier round's set. A request is answered within its own round, or not at all."""
    m = Mocks(crop_judge="active", judge_result=DOUBTED, look=AutodevLook(None, True, False, ""))
    async with started(env, m) as h:
        await idle(h, m, "developing")
        held = (await h.query(MiaDataDataset.status)).held
        await h.signal(MiaDataDataset.cancel, ev("cancel", "14"))
        await idle(h, m, "cancelled")
        m.outcome, m.look = "needs_snippet", answered(None, held.request)  # the old marker is still on the issue
        await h.signal(MiaDataDataset.restart, ev("go", "15"))
        await idle(h, m, "developing")
        await env.sleep(LOOK)
        state = await h.query(MiaDataDataset.status)
    assert "publish_crops" not in m.names() and not m.of("assess")
    assert state.held.set_id == held.set_id and state.held.request == ""  # still acceptable by a person


def test_the_entries_of_a_send_back_an_accept_and_a_rework_name_their_set():
    """Review finding (spec §6): each entry names the set and who acted."""
    import inspect

    source = inspect.getsource(MiaDataDataset)
    for line in ("Sent back by the crop judge", "Accepted by {event.actor}", "the saved crops are revoked"):
        start = source.index(line)
        assert "set_id}" in source[start - 200 : start + 400], line


async def test_max_reworks_zero_never_sends_back(env):
    m = Mocks(crop_judge="active", judge_result=DOUBTED, max_reworks=0)
    async with started(env, m) as h:
        await idle(h, m, "review")
        assert not m.of("open_snippet_issue") and (await h.query(MiaDataDataset.status)).held is None
        await approve_review(h, m)
        await idle(h, m, "done")
    assert "publish_crops" in m.names()


async def test_a_shadow_judge_that_doubts_a_set_sends_nothing_back(env):
    m = Mocks(crop_judge="shadow", judge_result=DOUBTED)
    async with started(env, m) as h:
        await idle(h, m, "review")
        assert not m.of("open_snippet_issue") and (await h.query(MiaDataDataset.status)).held is None


async def test_an_active_judges_approve_of_a_pr_head_asks_autodev_to_merge_it(env):
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"}, crop_judge="active", judge_result=GRANTED)
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await idle(h, m, "merging")
        assert f"/approve {H1}" in pr_comments(m) and "assess" not in m.names()
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
    assert "publish_crops" in m.names()
