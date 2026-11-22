"""The peek: one crop as the tensorizer returns it, for the page, which the buffer cannot tell from no peek."""

from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

import pytest

from mia_buffers.api import BufferService
from mia_buffers.ledger import Ledger
from mia_buffers.peek import Peeker, PeekFailed
from tests.test_buffers_api import CFG, CONSUMER, approve, body, call

PICTURES = {"img": [b"p0", b"p1", b"p2"], "labels": {"gt": [b"l0", b"l1", b"l2"]}}
RECORD = {
    "id": "rec",
    "title": "A dataset",
    "imaging": {"modality": ["fluorescence"], "dimensionality": "2D", "organism": ["mouse"]},
    "annotations": {"present": True, "types": ["instance"]},
}


class StandIn:
    """A peeker that draws nothing: it answers `PICTURES` (or raises `error`) and keeps what it was asked."""

    def __init__(self, error: Exception | None = None):
        self.error, self.asked = error, []

    def render(self, pyramid, descriptor, tensorizer, params, labels):
        self.asked.append((pyramid, descriptor, tensorizer, params, labels))
        if self.error:
            raise self.error
        return PICTURES


def serving(tmp_path, name="l", peeker=None, volume=True):
    """A service whose buffer `b` is approved and, with `volume`, holds one volume with a label layer."""
    service = BufferService(Ledger(tmp_path / f"{name}.sqlite"), peeker=peeker)
    assert call(service, "PUT", "/buffers/b", payload=CFG)[0] == 200
    approve(service)
    if volume:
        assert call(service, "PUT", "/buffers/b/volumes/a", payload={**body(), "labels": {"gt": "instance"}})[0] == 200
    return service


def test_a_peek_answers_the_tensorizers_pictures_with_the_datasets_facts(tmp_path):
    peeker = StandIn()
    service = serving(tmp_path, peeker=peeker)
    call(service, "PUT", f"/records/{'a' * 40}", payload={"records": [{"path": "rec.yaml", "record": RECORD}]})
    status, out, headers = call(service, "GET", "/buffers/b/peek")
    assert status == 200 and out["split"] == "train" and headers["Cache-Control"] == "no-store"
    assert [level["factor"] for level in out["levels"]] == [1, 2, 4]
    assert [level["pixel_nm"] for level in out["levels"]] == [10.0, 20.0, 40.0]
    assert [base64.b64decode(level["img"]) for level in out["levels"]] == PICTURES["img"]
    assert out["labels"] == [{"key": "gt", "levels": [base64.b64encode(p).decode() for p in PICTURES["labels"]["gt"]]}]
    assert out["descriptor"]["path"] == "/staged/a/v0.zarr" and "seq" not in out["descriptor"]
    assert out["samples"] is None  # a volume staged whole has no list of candidates
    assert out["dataset"] == {
        "id": "rec",
        "title": "A dataset",
        "modality": "fluorescence",
        "dims": "2D",
        "organisms": ["mouse"],
        "annotation_types": ["instance"],
    }
    pyramid, descriptor, tensorizer, params, labels = peeker.asked[0]
    assert (pyramid, tensorizer, labels) == ("/staged/a/v0.zarr", "muvit-2d", ["gt"])
    assert descriptor == out["descriptor"] and params == {"channels": {"op": "channel", "index": 0}, "stats": None}
    assert call(service, "GET", "/buffers/b/peek", {"split": "val"})[1]["descriptor"]["plane"] == {"z": 4}


def test_a_dataset_in_no_catalog_is_its_id_alone(tmp_path):
    assert call(serving(tmp_path, peeker=StandIn()), "GET", "/buffers/b/peek")[1]["dataset"] == {"id": "rec"}


def test_a_buffer_cannot_tell_a_peek_from_no_peek(tmp_path):
    peeked, untouched = serving(tmp_path, "peeked", StandIn()), serving(tmp_path, "untouched", StandIn())
    for split in ("train", "val", "train", "val", "train"):
        assert call(peeked, "GET", "/buffers/b/peek", {"split": split})[0] == 200
    status = call(peeked, "GET", "/buffers/b/status")[1]
    assert status["draws"] == {} and status["consumers"] == []
    assert peeked.ledger.frozen("b", "val") == [] and peeked.ledger.max_seq("b") == 0
    assert call(peeked, "GET", "/usage/b")[1] == call(untouched, "GET", "/usage/b")[1]
    answers = []
    for service in (peeked, untouched):
        consumer = call(service, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
        drawn = call(service, "GET", "/buffers/b/next", {"n": "5", "consumer": consumer})[1]["descriptors"]
        listed = call(service, "GET", "/buffers/b/split/val", {"from": "0", "n": "2", "consumer": consumer})[1]
        answers.append((drawn, listed))
    assert answers[0] == answers[1]


def test_a_peek_of_nothing_servable_is_a_204_that_is_not_counted(tmp_path):
    peeker = StandIn()
    service = serving(tmp_path, peeker=peeker, volume=False)
    status, out, headers = call(service, "GET", "/buffers/b/peek")
    assert (status, out) == (204, None) and "Retry-After" not in headers and not peeker.asked
    usage = call(service, "GET", "/usage/b")[1]
    assert usage["last_hour"]["empty"] == 0 and not any(day["empty"] for day in usage["days"])


def test_a_peek_refuses_what_a_consumer_is_refused(tmp_path):
    service = BufferService(Ledger(tmp_path / "l.sqlite"), peeker=StandIn())
    assert call(service, "GET", "/buffers/b/peek")[0] == 404
    call(service, "PUT", "/buffers/b", payload=CFG)
    assert call(service, "GET", "/buffers/b/peek")[:2] == (409, {"error": "buffer has no approved event"})
    service = serving(tmp_path, "other", StandIn())
    status, out, _ = call(service, "GET", "/buffers/b/peek", {"split": "other"})
    assert status == 422 and "train, val or test" in out["error"]
    assert call(serving(tmp_path, "bare"), "GET", "/buffers/b/peek")[:2] == (503, {"error": "no harness configured"})
    failing = serving(
        tmp_path, "failing", StandIn(PeekFailed("harness tensorize exited 2: unrecognized arguments: --png"))
    )
    status, out, _ = call(failing, "GET", "/buffers/b/peek")
    assert status == 502 and out["error"].endswith("unrecognized arguments: --png")


def harness(tmp_path: Path, script: str) -> Peeker:
    """A peeker whose "python" is `script` (python source run with what the harness would be run with)."""
    stand_in = tmp_path / "python"
    stand_in.write_text(f"#!{sys.executable}\nimport json, os, sys\nfrom pathlib import Path\n{script}\n")
    stand_in.chmod(0o755)
    return Peeker(stand_in, tmp_path / "snippets")


WRITES = """
out = Path(sys.argv[sys.argv.index("--png") + 1])
out.mkdir()
for name in {names!r}:
    (out / name).write_bytes(name.encode())
Path({seen!r}).write_text(json.dumps([sys.argv[1:], os.environ.get("OMP_NUM_THREADS")]))
"""


def test_the_peeker_runs_the_harness_and_reads_its_pictures(tmp_path):
    seen = tmp_path / "seen.json"
    names = ["img-0.png", "img-1.png", "label-gt-0.png"]  # the label layer lacks level 1
    peeker = harness(tmp_path, WRITES.format(names=names, seen=str(seen)))
    descriptor = {"levels": [1, 2], "patch": 64}
    out = peeker.render("/staged/a/v0.zarr", descriptor, "muvit-2d", {"clip": True}, ["gt", "other"])
    assert out == {"img": [b"img-0.png", b"img-1.png"], "labels": {}}
    arguments, threads = json.loads(seen.read_text())
    assert threads == "1" and arguments[:2] == ["-P", "-c"] and arguments[3:5] == ["tensorize", "/staged/a/v0.zarr"]
    named = dict(zip(arguments[5::2], arguments[6::2], strict=True))
    assert named["--tensorizers"] == str(tmp_path / "snippets") and named["--name"] == "muvit-2d"
    assert json.loads(named["--descriptor"]) == descriptor and json.loads(named["--params"]) == {"clip": True}
    names.append("label-gt-1.png")
    whole = harness(tmp_path, WRITES.format(names=names, seen=str(seen)))
    assert whole.render("/staged/a/v0.zarr", descriptor, "muvit-2d", {}, ["gt"])["labels"] == {
        "gt": [b"label-gt-0.png", b"label-gt-1.png"]
    }


@pytest.mark.parametrize(
    ("script", "said"),
    [
        ('sys.stderr.write("no such option: --png"); sys.exit(2)', "exited 2: no such option: --png"),
        ("pass", "wrote no picture"),
    ],
)
def test_the_peeker_says_why_the_harness_failed(tmp_path, script, said):
    with pytest.raises(PeekFailed, match=said):
        harness(tmp_path, script).render("/staged/a/v0.zarr", {"levels": [1], "patch": 64}, "muvit-2d", {}, [])


def test_a_harness_that_cannot_be_run_fails_the_peek(tmp_path):
    with pytest.raises(PeekFailed, match="cannot run the harness"):
        Peeker(tmp_path / "absent", tmp_path).render("/staged/a/v0.zarr", {"levels": [1]}, "muvit-2d", {}, [])
