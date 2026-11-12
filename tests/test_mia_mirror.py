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


# --- a subject started from the page has no issue: the mirror opens one (spec webapp-actions §7 step 1) ---


async def describe_rec_a(subject):
    assert subject["key"] == "rec-a"
    return "rec-a: a dataset", "<!-- mia-data:record=rec-a -->\nA read-only log.\n\n- Record: x\n", ["data:new"]


async def test_the_mirror_opens_the_issue_of_a_subject_that_has_none_and_writes_its_number_back(tmp_path):
    t = Setup(tmp_path)
    t.mirror = Mirror(t.gh, t.registry, "bot", {"dataset": (data_label, STAGES)}, {"dataset": describe_rec_a})
    t.ledger._db.execute("DELETE FROM subjects WHERE number = 1")  # rec-a was never adopted from an issue:
    number = t.ledger.create_subject("dataset", "rec-a")["number"]  # it was started from the page
    t.registry.append_entry("dataset", number, body="**Action:** `go` by `page`", marker="<!-- a1 -->")
    t.registry.append_entry("dataset", number, body="matching\n<!-- m1 -->", marker="<!-- m1 -->", stage="matching")
    assert await t.mirror.mirror("dataset", number) == 2
    (title, body, labels) = t.gh.created[0]
    assert title == "rec-a: a dataset" and body.startswith("<!-- mia-data:record=rec-a -->") and labels == ["data:new"]
    opened = t.ledger.subject("dataset", "rec-a")["issue"]
    assert opened == 101 and [comment["body"] for comment in t.gh.comments[opened]] == [
        "**Action:** `go` by `page`\n\n<!-- a1 -->",
        "matching\n<!-- m1 -->",
    ]
    assert [label["name"] for label in t.gh.issues[opened]["labels"]] == ["data:matching"]
    assert await t.mirror.mirror("dataset", number) == 0 and len(t.gh.created) == 1


async def test_an_issue_opened_before_a_crash_is_found_by_its_marker_and_not_opened_twice(tmp_path):
    t = Setup(tmp_path)
    t.mirror = Mirror(t.gh, t.registry, "bot", {"dataset": (data_label, STAGES)}, {"dataset": describe_rec_a})
    t.ledger._db.execute("UPDATE subjects SET issue = NULL WHERE number = 1")  # the write-back never happened
    t.gh.issues[50] = {"number": 50, "body": "<!-- mia-data:record=rec-a -->", "labels": [], "user": {"login": "eve"}}
    body = "<!-- mia-data:record=rec-a -->\nA read-only log.\n"
    ours = (await t.gh.create_issue("rec-a: a dataset", body, ["data:new"]))["number"]
    t.gh.created.clear()
    assert await t.mirror.mirror("dataset", 1) == 2
    assert t.gh.created == [] and t.ledger.subject("dataset", "rec-a")["issue"] == ours  # ours, not eve's copy


async def test_a_kind_this_worker_cannot_describe_is_not_retried(tmp_path):
    t = Setup(tmp_path)
    t.ledger._db.execute("UPDATE subjects SET issue = NULL WHERE number = 1")
    with pytest.raises(ApplicationError) as refused:
        await t.mirror.mirror("dataset", 1)  # this mirror was given no way to open a dataset's issue
    assert refused.value.non_retryable


async def test_a_dataset_issue_is_the_record_facts_under_a_line_that_sends_people_to_the_page(tmp_path):
    from mia_core.config import ServiceCfg
    from mia_data.mirror import dataset_issue
    from tests.test_mia_activities import _setup

    t = _setup(tmp_path)
    t.cfg.service = ServiceCfg(url="http://svc.example.org:8765")
    title, body, labels = await dataset_issue(t.cfg, t.cat)({"kind": "dataset", "key": "rec-1", "number": 28000000})
    lines = body.splitlines()
    assert lines[0] == "<!-- mia-data:record=rec-1 -->" and title.startswith("rec-1: ")
    assert "read-only log" in lines[1] and "http://svc.example.org:8765/#datasets/rec-1" in lines[1]
    assert any(line.startswith("- Record: [main](https://github.com/o/catalog/blob/main/") for line in lines)
    assert "data:new" in labels
    with pytest.raises(ApplicationError, match="no record"):
        await dataset_issue(t.cfg, t.cat)({"kind": "dataset", "key": "nope", "number": 1})


async def test_an_adopted_issue_is_told_once_that_it_became_a_read_only_log(tmp_path):
    """People's habit on an existing issue is to add a label: the first thing the mirror writes there says that
    labels do nothing now and where to act."""
    t = Setup(tmp_path)
    t.mirror = Mirror(
        t.gh, t.registry, "bot", {"dataset": (data_label, STAGES)}, pages={"dataset": lambda subject: "http://svc/#x"}
    )
    assert await t.mirror.mirror("dataset", 1) == 2
    bodies = t.bodies()
    assert len(bodies) == 3 and "read-only log" in bodies[0] and "http://svc/#x" in bodies[0]
    t.registry.append_entry("dataset", 1, body="more", marker="<!-- m3 -->")
    assert await t.mirror.mirror("dataset", 1) == 1 and len(t.bodies()) == 4  # said once
