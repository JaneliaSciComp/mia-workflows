"""The mirror: a subject's timeline and stage label copied to its GitHub issue, once per entry, whenever GitHub
answers (spec webapp-actions §7). The activity against a GitHub fake and a real service; the workflow with the
activity mocked."""

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from mia_buffers.api import BufferService
from mia_buffers.ledger import Ledger
from mia_data.config import STAGES
from mia_data.mirror import Mirror
from mia_data.workflows import MiaMirror
from tests.mia_helpers import FakeGH
from tests.test_buffers_adopt import registry_for


def data_label(stage):
    return f"data:{stage}"


class Setup:
    """A dataset subject #1 with two entries waiting, its issue with a stale stage label, and the mirror."""

    def __init__(self, tmp_path):
        self.ledger = Ledger(tmp_path / "l.sqlite")
        self.registry = registry_for(BufferService(self.ledger))
        labels = [{"name": "data:new"}, {"name": "has-labels"}]
        self.gh = FakeGH("o/data", issues={1: {"number": 1, "body": "", "labels": labels}})
        self.mirror = Mirror(self.gh, self.registry, "bot", {"dataset": (data_label, STAGES)})
        self.registry.adopt_subject("dataset", 1, "rec-a")
        self.registry.append_entry("dataset", 1, body="matching\n<!-- m1 -->", marker="<!-- m1 -->", stage="matching")
        self.registry.append_entry("dataset", 1, body="a report", marker="<!-- m2 -->")

    def bodies(self):
        return [comment["body"] for comment in self.gh.comments.get(1, [])]

    def labels(self):
        return [label["name"] for label in self.gh.issues[1]["labels"]]


async def test_entries_are_posted_in_order_once_and_the_label_follows_the_stage(tmp_path):
    t = Setup(tmp_path)
    assert await t.mirror.mirror("dataset", 1) == 2
    assert t.bodies() == ["matching\n<!-- m1 -->", "a report\n\n<!-- m2 -->"]  # a body gets its marker if it has none
    assert t.labels() == ["has-labels", "data:matching"]
    entries = t.ledger.timeline("dataset", 1)
    assert all(entry["mirrored_at"] for entry in entries) and entries[0]["comment_url"].endswith("issues/1#c0")
    assert await t.mirror.mirror("dataset", 1) == 0 and len(t.bodies()) == 2  # nothing new: nothing posted
    t.registry.append_entry("dataset", 1, stage="review")  # a stage with nothing to say still moves the label
    assert await t.mirror.mirror("dataset", 1) == 0 and t.labels() == ["has-labels", "data:review"]


async def test_a_comment_posted_before_a_crash_is_found_by_its_marker_not_posted_again(tmp_path):
    t = Setup(tmp_path)
    await t.gh.comment(1, "matching\n<!-- m1 -->")  # posted by an earlier pass that died before its write-back
    await t.gh.comment(1, "quoting <!-- m2 -->", login="mallory")  # an outsider's copy of a marker does not count
    assert await t.mirror.mirror("dataset", 1) == 1
    assert t.bodies() == ["matching\n<!-- m1 -->", "quoting <!-- m2 -->", "a report\n\n<!-- m2 -->"]
    assert all(entry["mirrored_at"] for entry in t.ledger.timeline("dataset", 1))


async def test_github_away_fails_the_pass_and_a_later_pass_completes_the_issue(tmp_path):
    t = Setup(tmp_path)
    working = t.gh.comment

    async def away(number, body, login="bot"):
        raise RuntimeError("GitHub is away")

    t.gh.comment = away
    with pytest.raises(RuntimeError, match="away"):
        await t.mirror.mirror("dataset", 1)
    assert not any(entry["mirrored_at"] for entry in t.ledger.timeline("dataset", 1))
    t.gh.comment = working
    assert await t.mirror.mirror("dataset", 1) == 2 and t.labels() == ["has-labels", "data:matching"]


async def test_a_subject_that_does_not_exist_is_not_retried(tmp_path):
    t = Setup(tmp_path)
    with pytest.raises(ApplicationError) as refused:
        await t.mirror.mirror("dataset", 99)
    assert refused.value.non_retryable


async def test_a_kick_during_a_pass_runs_another_pass():
    passes, first_started, release = [], asyncio.Event(), asyncio.Event()

    @activity.defn(name="mirror")
    async def mirror(kind: str, number: int) -> int:
        passes.append((kind, number))
        first_started.set()
        await release.wait()
        return 0

    env = await WorkflowEnvironment.start_time_skipping()
    try:
        queue = f"tq-{uuid.uuid4()}"
        async with Worker(env.client, task_queue=queue, workflows=[MiaMirror], activities=[mirror]):
            handle = await env.client.start_workflow(
                "MiaMirror", args=["dataset", 7], id="mirror/dataset/7", task_queue=queue, start_signal="kick"
            )
            await first_started.wait()
            await handle.signal("kick")  # an entry appended while the first pass is posting
            release.set()
            await handle.result()
    finally:
        await env.shutdown()
    assert passes == [("dataset", 7)] * 2
