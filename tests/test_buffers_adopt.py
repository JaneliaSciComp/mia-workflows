"""adopt-issues: every dataset, buffer and request issue becomes a subject once; duplicates are reported."""

import asyncio

import httpx

from mia_buffers.adopt import adopt_issues
from mia_buffers.api import BufferService
from mia_buffers.ledger import Ledger
from mia_buffers.spec import load_spec_text
from mia_core.registry import Registry
from mia_data.seed import MARKER
from tests.mia_helpers import FakeGH
from tests.test_buffers_plan import FIXTURES

SPEC = (FIXTURES / "spec.yaml").read_text()
NAME = load_spec_text(SPEC).name


def issue(number, body, *labels):
    return {"number": number, "body": body, "labels": [{"name": label} for label in labels]}


def registry_for(service):
    """A registry client whose requests go straight to `service.handle`."""

    def handler(request):
        status, payload, _ = service.handle(
            request.method, request.url.raw_path.decode().split("?")[0], dict(request.url.params), request.content
        )
        return httpx.Response(status, json=payload)

    return Registry("http://svc", None, transport=httpx.MockTransport(handler))


def repos():
    data = FakeGH(
        "o/data",
        issues={
            1: issue(1, MARKER.format("rec-a") + "\n", "data:review"),
            2: issue(2, MARKER.format("rec-b") + "\n", "data:done", "has-labels"),
            3: issue(3, MARKER.format("rec-a") + "\n", "data:testing"),
            4: issue(4, None),
            5: issue(5, "see " + MARKER.format("rec-c"), "data:new"),
        },
    )
    fence = f"```yaml\n{SPEC}```\n"
    buffers = FakeGH(
        "o/buffers",
        issues={
            8: issue(8, fence, "buffer:ready"),
            9: issue(9, "again\n" + fence, "buffer:needs-human", "buffer:go"),
            10: issue(10, "no spec here", "buffer:new"),
            11: issue(11, "```yaml\nrecords: [rec-a]\n```\n", "prep:requested"),
            12: issue(12, fence),
        },
    )
    return data, buffers


def test_adopts_every_issue_once_and_reports_the_rest(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_dataset("rec-a", {"issue": 3})  # the row names the issue that is the dataset's
    registry = registry_for(BufferService(ledger))
    lines = asyncio.run(adopt_issues(*repos(), registry))
    assert lines == [
        "dataset rec-a #3 testing",
        "duplicate: #1 also names rec-a (adopted #3); it is at review",
        "dataset rec-b #2 done",
        f"buffer {NAME} #8 ready",
        f"buffer {NAME}-9 #9 needs-human",
        "buffer issue-10 #10 new",
        "prep prep-11 #11 requested",
        "skipped: #12 has no buffer or prep label",
    ]
    assert [(row["kind"], row["number"]) for row in ledger.find_subjects()] == [
        ("buffer", 8),
        ("buffer", 9),
        ("buffer", 10),
        ("dataset", 2),
        ("dataset", 3),
        ("prep", 11),
    ]
    assert ledger.subject("buffer", NAME)["data"] == {"spec": SPEC}
    assert ledger.subject("buffer", "issue-10")["data"] == {}
    assert ledger.subject("prep", "prep-11")["data"] == {"spec": "records: [rec-a]\n"}
    assert asyncio.run(adopt_issues(*repos(), registry)) == lines  # again: the same report, nothing new
    assert len(ledger.find_subjects()) == 6


def test_without_a_row_the_lowest_issue_is_the_dataset_and_a_dry_run_writes_nothing(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    lines = asyncio.run(adopt_issues(*repos(), registry_for(BufferService(ledger)), dry_run=True))
    assert lines[:2] == [
        "dataset rec-a #1 review",
        "duplicate: #3 also names rec-a (adopted #1); it is at testing",
    ]
    assert ledger.find_subjects() == []
