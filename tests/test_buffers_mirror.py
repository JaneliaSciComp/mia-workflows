"""The issues the mirror opens for a buffer spec and a preparation request created on the page."""

import yaml

from mia_buffers.api import BufferService
from mia_buffers.config import PREP_STAGES, STAGES
from mia_buffers.ledger import Ledger
from mia_buffers.mirror import buffer_issue, prep_issue
from mia_data.mirror import Mirror
from tests.mia_helpers import FakeGH
from tests.test_buffers_adopt import registry_for
from tests.test_buffers_config import make_cfg

PREP_TEXT = yaml.safe_dump(
    {"schema_version": 1, "kind": "preparation", "catalog": {"repo": "o/c", "ref": "c" * 40}, "records": ["a", "b"]}
)


async def test_a_spec_and_a_request_created_on_the_page_get_their_issue(tmp_path):
    cfg = make_cfg(tmp_path)
    ledger = Ledger(tmp_path / "l.sqlite")
    registry, gh = registry_for(BufferService(ledger)), FakeGH("o/buffers")
    labels = {"buffer": (cfg.label, STAGES), "prep": (cfg.prep_label, PREP_STAGES)}
    mirror = Mirror(gh, registry, "bot", labels, {"buffer": buffer_issue(cfg), "prep": prep_issue(cfg)})
    spec = ledger.create_subject("buffer", "EM pool / v2", data={"spec": "name: EM pool / v2\n"})
    request = ledger.create_subject("prep", None, data={"spec": PREP_TEXT})
    registry.append_entry("buffer", spec["number"], body="planning\n<!-- p -->", marker="<!-- p -->", stage="planning")
    registry.append_entry(
        "prep", request["number"], body="requested\n<!-- r -->", marker="<!-- r -->", stage="requested"
    )
    assert await mirror.mirror("buffer", spec["number"]) == 1 and await mirror.mirror("prep", request["number"]) == 1
    (title, body, _), (prep_title, prep_body, _) = gh.created
    lines = body.splitlines()
    assert title == "EM pool / v2" and lines[0] == f"<!-- mia-buffers: subject buffer {spec['number']} -->"
    assert "read-only log" in lines[1] and "http://127.0.0.1:8765/#specs/EM%20pool%20%2F%20v2" in lines[1]
    assert prep_title == f"Prepare 2 records ({request['key']})"
    assert prep_body.startswith(f"<!-- mia-buffers: subject prep {request['number']} -->")
    assert f"/#requests/{request['key']}" in prep_body and f"```yaml\n{PREP_TEXT}```" in prep_body
    opened = {row["kind"]: row["issue"] for row in ledger.find_subjects()}
    assert sorted(opened.values()) == sorted(gh.issues)  # each number written back
    assert [label["name"] for label in gh.issues[opened["buffer"]]["labels"]] == ["buffer:planning"]
    assert [label["name"] for label in gh.issues[opened["prep"]]["labels"]] == ["prep:requested"]
    assert await mirror.mirror("buffer", spec["number"]) == 0 and len(gh.created) == 2  # found again, not reopened
