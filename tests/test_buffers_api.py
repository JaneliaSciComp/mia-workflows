"""buffer-api: spec v0.5 §5 routes over the ledger, through handle() and once over a real socket."""

import gzip
import json
import re
import shutil
import threading
from urllib.parse import quote

import httpx
import pytest
import yaml

from mia_buffers import api as api_module
from mia_buffers.api import BufferService, ledger_path, make_server, open_ledger, prune_backups
from mia_buffers.dispatch import DispatchError
from mia_buffers.hosts import HostCheck
from mia_buffers.ledger import Ledger
from mia_buffers.plan import load_records
from mia_core.hosts import generate_policy
from mia_core.locks import LockDir
from mia_data.config import NetworkCfg
from mia_data.models import LabelEvent
from tests.test_buffers_config import make_cfg
from tests.test_buffers_plan import CATALOG as FIXTURE_CATALOG
from tests.test_buffers_plan import FIXTURES, ONE_CH, U937
from tests.test_buffers_sync import write_prepared
from tests.test_core_hosts import SHORTHAND_IPS


def _spec_2d_text() -> str:
    """The fixture spec restricted to 2D, as the `spec_2d` fixture builds it, as YAML text."""
    data = yaml.safe_load((FIXTURES / "spec.yaml").read_text())
    data["select"]["where"]["dims"] = {"in": ["2D"]}
    data["select"]["where"]["annotations"]["source"] = "any"
    data["select"]["exclude"].update(ids=[ONE_CH], reasons={ONE_CH: "one channel"})
    return yaml.safe_dump(data)


SPEC_2D_TEXT = _spec_2d_text()

CFG = {
    "spec_name": "s",
    "spec_hash": "h",
    "levels": [1, 2, 4],
    "patch": 256,
    "channels": 1,
    "splits": {"train": 0.8, "val": 0.2},
    "split_sizes": {"val": 4},
}
CONSUMER = {"name": "n", "host": "h", "pid": 1, "split": "train"}


def body(vid="a", extent=2048, planes=5, split_planes=None):
    return {
        "path": f"/staged/{vid}/v0.zarr",
        "record": "rec",
        "axes": "czyx",
        "shape": [1, planes, extent, extent],
        "levels": [
            {
                "factor": f,
                "shape": [1, planes, extent // f, extent // f],
                "scale_nm": {"z": 1.0, "y": 10.0 * f, "x": 10.0 * f},
                "translation_nm": {"z": 0.0, "y": 5.0 * (f - 1), "x": 5.0 * (f - 1)},
            }
            for f in (1, 2, 4)
        ],
        "split_planes": split_planes or {"train": [0, 1, 2, 3], "val": [4]},
        "weight": 1.0,
        "channels_stored": 1,
        "recipe": {"op": "channel", "index": 0},
        "voxel_size_nm": {"y": 10.0, "x": 10.0},
    }


def svc(tmp_path):
    return BufferService(Ledger(tmp_path / "l.sqlite"))


def call(s, method, path, query=None, payload=None):
    return s.handle(method, path, query or {}, json.dumps(payload).encode() if payload is not None else b"")


def approve(s, buffer="b"):
    """Authorize the buffer to serve: consumers are refused until its latest event is approved, ready or idle."""
    return worker(s, "POST", f"/buffers/{buffer}/events", {"kind": "approved"})


def test_register_and_draw(tmp_path):
    s = svc(tmp_path)
    assert call(s, "PUT", "/buffers/b", payload=CFG)[0] == 200
    approve(s)
    assert call(s, "PUT", "/buffers/b/volumes/a", payload=body())[0] == 200
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/small", payload=body("small", extent=1000))
    assert status == 422 and out["error"] == "footprint 1024 exceeds 1000×1000"
    assert call(s, "PUT", "/buffers/zz/volumes/a", payload=body())[0] == 404
    status, out, _ = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)
    assert status == 201
    cid = out["consumer_id"]
    status, out, _ = call(s, "GET", "/buffers/b/next", {"n": "3", "consumer": cid})
    assert status == 200 and [d["seq"] for d in out["descriptors"]] == [1, 2, 3]
    assert all(d["plane"]["z"] in (0, 1, 2, 3) and d["path"] == "/staged/a/v0.zarr" for d in out["descriptors"])
    assert call(s, "GET", "/buffers/b/next", {"n": "1", "consumer": "nope"})[0] == 404
    st = call(s, "GET", "/buffers/b/status")[1]
    assert st["draws"] == {"a": 3} and st["servable"] == {"train": 1, "val": 1} and len(st["consumers"]) == 1
    assert st["volumes"] == 1 and st["config"]["levels"] == [1, 2, 4]


def test_frozen_split_is_stable_and_bounded(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    first = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1]
    again = call(s, "GET", "/buffers/b/split/val", {"from": "2", "n": "10", "consumer": cid})[1]
    assert first["size"] == 4 and len(first["descriptors"]) == 4
    assert all(d["plane"]["z"] == 4 for d in first["descriptors"])
    assert again["descriptors"] == first["descriptors"][2:]
    assert call(s, "GET", "/buffers/b/split/val", {"from": "4", "n": "1", "consumer": cid})[0] == 416
    # a new service over the same ledger serves the same list
    second = BufferService(Ledger(tmp_path / "l.sqlite"))
    assert call(second, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1] == first
    assert call(s, "GET", "/buffers/b/split/train", {"from": "0", "n": "1", "consumer": cid})[1]["size"] == 256


CANDS = [
    {"plane": {"z": 4}, "centre_nm": [10240.0 + i, 10240.0], "levels": [1, 2, 4], "patch": 256, "starts": {}}
    for i in range(4)
]


def test_frozen_list_stops_when_candidates_run_out(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)  # val list of 4
    approve(s)
    call(s, "PUT", "/buffers/b/volumes/a", payload={**body(), "samples": {"val": CANDS[:2]}})
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    head = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "1", "consumer": cid})[1]
    assert head["size"] == 2 and head["exhausted"] is True  # what a client sizes its dataset from
    out = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1]
    assert len(out["descriptors"]) == 2 and out["size"] == 2 and out["exhausted"] is True
    assert {d["candidate"] for d in out["descriptors"]} == {0, 1} and out["descriptors"][:1] == head["descriptors"]
    assert [d["seq"] for d in out["descriptors"]] == [0, 1]
    assert call(s, "GET", "/buffers/b/split/val", {"from": "2", "n": "1", "consumer": cid})[0] == 416


def test_frozen_rows_stay_readable_when_a_volume_goes_missing(tmp_path):
    staged = tmp_path / "staged"
    s = BufferService(Ledger(tmp_path / "l.sqlite"), staged_root=staged)
    call(s, "PUT", "/buffers/b", payload=CFG)  # val list of 4
    approve(s)
    for vid, candidates in (("a", CANDS[:2]), ("c", CANDS[2:])):
        (staged / vid / "v0.zarr").mkdir(parents=True)
        path = str(staged / vid / "v0.zarr")
        call(s, "PUT", f"/buffers/b/volumes/{vid}", payload={**body(vid), "path": path, "samples": {"val": candidates}})
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    whole = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1]
    assert len(whole["descriptors"]) == 4 and whole["size"] == 4
    shutil.rmtree(staged / "c")
    assert call(s, "GET", "/buffers/b/status")[1]["missing"] == ["c"]  # its 2 candidates leave `capacity`
    status, tail, _ = call(s, "GET", "/buffers/b/split/val", {"from": "2", "n": "2", "consumer": cid})
    assert status == 200 and tail["descriptors"] == whole["descriptors"][2:] and tail["size"] == 4
    assert call(s, "GET", "/buffers/b/split/val", {"from": "4", "n": "1", "consumer": cid})[0] == 416


def test_zero_weight_candidates_end_the_list_without_an_error(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    call(s, "PUT", "/buffers/b/volumes/a", payload={**body("a"), "samples": {"val": CANDS[:1]}})
    call(s, "PUT", "/buffers/b/volumes/z", payload={**body("z"), "weight": 0.0, "samples": {"val": CANDS[1:3]}})
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    for _ in range(2):  # a later read replays the same prefix
        status, out, _ = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})
        assert status == 200 and out["size"] == 1 and out["exhausted"] is True
        assert [(d["volume"], d["candidate"]) for d in out["descriptors"]] == [("a", 0)]


def test_re_put_replaces_candidates_and_keeps_frozen(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    planned = {**body(), "samples": {"val": CANDS}}
    call(s, "PUT", "/buffers/b/volumes/a", payload=planned)
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    first = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "2", "consumer": cid})[1]
    assert first["size"] == 4 and first["exhausted"] is False
    assert call(s, "PUT", "/buffers/b/volumes/a", payload=planned)[0] == 200
    again = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "2", "consumer": cid})[1]
    assert again == first and call(s, "GET", "/buffers/b/status")[1]["volumes"] == 1
    whole = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1]
    assert whole["descriptors"][:2] == first["descriptors"] and {d["candidate"] for d in whole["descriptors"]} == {
        0,
        1,
        2,
        3,
    }


def test_re_put_under_a_new_artifact_clears_frozen(tmp_path):
    """A rebuild changes the candidates a frozen row indexes, so the lists start over; a same-artifact re-put keeps
    them (test_re_put_replaces_candidates_and_keeps_frozen)."""
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    planned = {**body(), "samples": {"val": CANDS}, "artifact": "aaaa"}
    assert call(s, "PUT", "/buffers/b/volumes/a", payload=planned)[1]["frozen_cleared"] == 0
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    first = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1]
    assert len(first["descriptors"]) == 4
    assert call(s, "PUT", "/buffers/b/volumes/a", payload=planned)[1]["frozen_cleared"] == 0
    rebuilt = {**planned, "artifact": "bbbb", "samples": {"val": CANDS[:2]}}
    assert call(s, "PUT", "/buffers/b/volumes/a", payload=rebuilt)[1]["frozen_cleared"] == 4
    again = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1]
    assert again["size"] == 2 and {d["candidate"] for d in again["descriptors"]} == {0, 1}


def test_empty_split_is_204_and_bad_bodies_are_422(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body(split_planes={"train": [0, 1, 2, 3, 4]}))
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    status, out, headers = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})
    assert status == 204 and out is None and headers["Retry-After"] == "30"
    assert call(s, "GET", "/buffers/b/next", {"n": "1", "consumer": cid})[0] == 204
    status, out, _ = call(s, "PUT", "/buffers/b", payload={**CFG, "bogus": 1})
    assert status == 422 and "bogus" in out["error"]
    assert call(s, "PUT", "/buffers/b", payload=None)[0] == 422
    assert call(s, "GET", "/nothing")[0] == 404
    assert call(s, "GET", "/buffers/zz/status")[0] == 404
    assert call(s, "GET", "/buffers/b/next", {"n": "x", "consumer": cid})[0] == 422


def test_concurrent_next_gives_unique_seqs(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cids = [call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "pid": i})[1]["consumer_id"] for i in range(3)]
    seqs: list[int] = []

    def pull(cid):
        for _ in range(10):
            out = call(s, "GET", "/buffers/b/next", {"n": "4", "consumer": cid})[1]
            seqs.extend(d["seq"] for d in out["descriptors"])

    threads = [threading.Thread(target=pull, args=(c,)) for c in cids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(seqs) == list(range(1, 121))


def test_over_http(tmp_path):
    server = make_server(BufferService(Ledger(tmp_path / "l.sqlite"), token="t"), "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        auth = {"Authorization": "Bearer t"}
        assert httpx.put(f"{base}/buffers/b", json=CFG).status_code == 401
        assert httpx.put(f"{base}/buffers/b", json=CFG, headers=auth).status_code == 200
        assert httpx.post(f"{base}/buffers/b/events", json={"kind": "approved"}, headers=auth).status_code == 200
        assert httpx.put(f"{base}/buffers/b/volumes/a", json=body(), headers=auth).status_code == 200
        cid = httpx.post(f"{base}/buffers/b/consumers", json=CONSUMER).json()["consumer_id"]
        r = httpx.get(f"{base}/buffers/b/next", params={"n": 2, "consumer": cid})
        assert r.status_code == 200 and len(r.json()["descriptors"]) == 2
        assert httpx.get(f"{base}/buffers/b/status").json()["draws"] == {"a": 2}
        empty = httpx.get(f"{base}/buffers/b/split/val", params={"from": 0, "n": 1, "consumer": cid})
        assert empty.status_code == 200
        assert httpx.get(f"{base}/nothing").status_code == 404
        assert httpx.put(f"{base}/buffers/b/volumes/rec%2Fv0.zarr", json=body("rec"), headers=auth).status_code == 200
        assert httpx.get(f"{base}/buffers/b/status").json()["volumes"] == 2
    finally:
        server.shutdown()


def test_worker_routes_need_the_token(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="secret")
    assert call(s, "PUT", "/buffers/b", payload=CFG)[:2] == (401, {"error": "unauthorized"})
    for bad in ({}, {"Authorization": "Bearer secre"}, {"Authorization": "Bearer "}, {"authorization": "secret"}):
        assert s.handle("PUT", "/buffers/b", {}, json.dumps(CFG).encode(), bad)[0] == 401
    ok = {"Authorization": "Bearer secret"}
    assert s.handle("PUT", "/buffers/b", {}, json.dumps(CFG).encode(), ok)[0] == 200
    assert s.handle("PUT", "/assets/raw/r", {}, b"{}", {"authorization": "Bearer secret"})[0] == 200  # any case
    assert s.handle("PUT", "/buffers/b", {}, b"not json", {})[0] == 401  # refused before parsing


def test_consumer_and_read_routes_are_open(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="secret")
    ok = {"Authorization": "Bearer secret"}
    s.handle("PUT", "/buffers/b", {}, json.dumps(CFG).encode(), ok)
    s.handle("POST", "/buffers/b/events", {}, json.dumps({"kind": "approved"}).encode(), ok)
    assert call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[0] == 201
    assert call(s, "GET", "/buffers/b/status")[0] == 200
    assert call(s, "GET", "/assets/raw")[0] == 200


def test_next_n_is_bounded_and_from_is_non_negative(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    assert call(s, "GET", "/buffers/b/next", {"n": "0", "consumer": cid})[0] == 422
    assert call(s, "GET", "/buffers/b/next", {"n": "100000", "consumer": cid})[0] == 422
    assert call(s, "GET", "/buffers/b/next", {"n": "1024", "consumer": cid})[0] == 200
    assert call(s, "GET", "/buffers/b/split/val", {"from": "-1", "n": "1", "consumer": cid})[0] == 422


def test_volume_levels_are_validated_and_errors_never_drop_the_connection(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    broken = body()
    del broken["levels"][0]["shape"]
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/a", payload=broken)
    assert status == 422 and "shape" in out["error"]
    no_yx = body()
    no_yx["levels"][0]["scale_nm"] = {"z": 1.0}
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/a", payload=no_yx)
    assert status == 422 and "scale_nm" in out["error"]
    short = body()
    short["levels"] = short["levels"][:2]
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/a", payload=short)
    assert status == 422 and "levels" in out["error"] and "4" in out["error"]
    s.ledger.put_volume = lambda *a, **k: 1 / 0  # type: ignore[method-assign]
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    assert status == 500 and out["error"].startswith("ZeroDivisionError")


def test_restart_does_not_replay_the_train_stream(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    first = [
        {k: v for k, v in d.items() if k != "seq"}
        for d in call(s, "GET", "/buffers/b/next", {"n": "5", "consumer": cid})[1]["descriptors"]
    ]
    restarted = BufferService(Ledger(tmp_path / "l.sqlite"))
    again = [
        {k: v for k, v in d.items() if k != "seq"}
        for d in call(restarted, "GET", "/buffers/b/next", {"n": "5", "consumer": cid})[1]["descriptors"]
    ]
    assert again != first


def test_consumer_lookup_is_by_id(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    cid = led.add_consumer("b", {**CONSUMER, "rank": 0, "mode": "local"})
    assert led.consumer("b", cid)["split"] == "train" and led.consumer("b", "nope") is None


def test_descriptor_carries_normalize(tmp_path):
    s = svc(tmp_path)
    assert call(s, "PUT", "/buffers/b", payload=CFG)[0] == 200
    approve(s)
    bounds = {"lo": [1], "hi": [200]}
    assert call(s, "PUT", "/buffers/b/volumes/a", payload={**body(), "normalize": bounds})[0] == 200
    assert call(s, "PUT", "/buffers/b/volumes/c", payload=body("c", split_planes={"test": [0]}))[0] == 200
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    descriptors = call(s, "GET", "/buffers/b/next", {"n": "2", "consumer": cid})[1]["descriptors"]
    assert all(d["normalize"] == bounds for d in descriptors)  # only a has train planes
    test = call(s, "GET", "/buffers/b/split/test", {"from": "0", "n": "1", "consumer": cid})[1]["descriptors"]
    assert test and test[0]["volume"] == "c" and test[0]["normalize"] is None  # c registered without bounds


def test_assets_registry_and_refs_from_registered_volumes(tmp_path):
    """`PUT/GET assets/<kind>/<key>` keep what exists on disk; registering a volume references its staged
    directory and its record's raw cache for the buffer, so a retire can count what is still in use."""
    from urllib.parse import quote

    s = svc(tmp_path)
    key = quote("/staged/a", safe="")
    assert call(s, "GET", f"/assets/staged/{key}")[0] == 404
    assert call(s, "PUT", f"/assets/staged/{key}", payload=[1])[0] == 422  # a JSON object, nothing else
    assert call(s, "PUT", f"/assets/staged/{key}", payload={"record": "rec", "ok": True})[0] == 200
    status, out, _ = call(s, "GET", f"/assets/staged/{key}")
    assert status == 200 and out == {
        "kind": "staged",
        "key": "/staged/a",
        "data": {"record": "rec", "ok": True},
        "refs": [],
    }
    assert call(s, "PUT", "/buffers/b", payload=CFG)[0] == 200
    assert call(s, "PUT", "/buffers/b/volumes/a", payload=body())[0] == 200  # path /staged/a/v0.zarr, record rec
    assert call(s, "GET", f"/assets/staged/{key}")[1]["refs"] == ["b"]
    assert call(s, "GET", "/assets/raw/rec")[1] == {"kind": "raw", "key": "rec", "data": None, "refs": ["b"]}
    assert call(s, "GET", "/assets/staged")[1] == {
        "kind": "staged",
        "assets": [{"key": "/staged/a", "data": {"record": "rec", "ok": True}, "refs": ["b"]}],
        "bytes": 0,
    }
    assert call(s, "GET", "/assets/nope/x")[0] == 404


def test_records_datasets_and_plan(tmp_path):
    s = svc(tmp_path)
    ref = "a" * 40
    records = [{"path": p, "record": r} for p, r in load_records(FIXTURE_CATALOG)]
    assert call(s, "PUT", f"/records/{ref}", payload={"records": records})[1]["count"] == len(records)
    assert call(s, "PUT", "/records/main", payload={"records": []})[0] == 422
    assert call(s, "PUT", f"/records/{ref}", payload={"records": [{"path": "p"}]})[0] == 422
    assert call(s, "GET", "/catalogs")[1]["catalogs"][0]["ref"] == ref
    text = re.sub(r"ref: [0-9a-f]{40}", f"ref: {ref}", SPEC_2D_TEXT)  # the 2D fixture spec's text, at the synced ref
    status, out, _ = call(s, "POST", "/plan", payload={"spec": text})
    assert status == 200 and all(row["prepared"] == "unprepared" for row in out["rows"])
    assert all(row["rule"] == "not prepared" for row in out["rows"] if row["fit"] and row["fit"]["ok"])
    assert call(s, "PUT", f"/datasets/{U937}", payload={"state": "prepared", "crops": 16})[1]["crops"] == 16
    assert call(s, "PUT", f"/datasets/{U937}", payload={"state": "nope"})[0] == 422
    assert call(s, "PUT", f"/datasets/{U937}", payload=["state"])[0] == 422
    assert call(s, "GET", f"/datasets/{U937}")[1]["state"] == "prepared"
    assert call(s, "GET", "/datasets/absent")[0] == 404
    assert [d["id"] for d in call(s, "GET", "/datasets", {"state": "prepared"})[1]["datasets"]] == [U937]
    out = call(s, "POST", "/plan", payload={"spec": text})[1]
    by_id = {row["id"]: row for row in out["rows"]}
    assert by_id[U937]["prepared"] == "prepared" and by_id[U937]["admitted"] and by_id[U937]["weight"] > 0
    status, out, _ = call(s, "POST", "/plan", payload={"spec": SPEC_2D_TEXT})  # the fixture's own ref is not synced
    assert status == 409 and "mia-buffers catalog sync --ref ab6a013d5da39c80651ddc845915d034b2eae82a" in out["error"]
    assert call(s, "POST", "/plan", payload={"spec": "schema_version: 2"})[0] == 422


def test_sync_datasets_route(tmp_path):
    root = tmp_path / "crops"
    write_prepared(root / "confocal-human-u937", U937)
    write_prepared(root / "tem-mouse-stale", "stale", match=False)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), storage_root=root)
    s.ledger.put_dataset("busy", {"state": "in-progress"})
    assert call(s, "POST", "/sync/datasets")[1] == {"prepared": 1, "demoted": 0, "revoked_kept": 0}
    assert call(s, "GET", "/datasets/stale")[0] == 404 and call(s, "GET", "/datasets/busy")[1]["state"] == "in-progress"
    assert call(s, "GET", f"/datasets/{U937}")[1]["folder"] == "confocal-human-u937"
    assert call(s, "POST", "/sync/datasets")[0] == 200
    assert BufferService(Ledger(tmp_path / "l2.sqlite")).handle("POST", "/sync/datasets", {}, b"")[0] == 503
    worker = BufferService(Ledger(tmp_path / "l3.sqlite"), token="t", storage_root=root)
    assert worker.handle("POST", "/sync/datasets", {}, b"")[0] == 401


def test_gallery_png_route_serves_a_rounds_crops_from_its_workspace(tmp_path):
    """A round at review has published nothing: its crops are in its crop job's workspace, their overlays in its
    verify job's, and the newest attempt that has the file is the one served."""
    sha, round_id = "c" * 40, "i7-e99"
    round_dir = tmp_path / "work" / "issue-7" / round_id
    for attempt, raw in ((1, b"old"), (2, b"new")):
        (round_dir / f"crop-{sha}-{attempt}" / "crops" / "c1").mkdir(parents=True)
        (round_dir / f"crop-{sha}-{attempt}" / "crops" / "c1" / "raw.png").write_bytes(raw)
    (round_dir / f"verify-{sha}-1" / "preview" / "c1").mkdir(parents=True)
    (round_dir / f"verify-{sha}-1" / "preview" / "c1" / "overlay.png").write_bytes(b"overlay")
    (round_dir / f"verify-{sha}-2" / "preview").mkdir(parents=True)  # the second attempt drew nothing for c1
    (round_dir / f"crop-{sha}-2" / "crops" / "c2").symlink_to(round_dir / f"crop-{sha}-1" / "crops" / "c1")
    (round_dir / f"crop-{sha}-3").symlink_to(round_dir / f"crop-{sha}-1")  # no workspace of the round's
    s = BufferService(Ledger(tmp_path / "l.sqlite"), work_root=tmp_path / "work")
    status, body, headers = s.handle("GET", f"/gallery/{round_id}/{sha}/c1/raw.png", {}, b"")
    assert (status, body, headers["Content-Type"]) == (200, b"new", "image/png")
    assert s.handle("GET", f"/gallery/{round_id}/{sha}/c1/overlay.png", {}, b"")[1] == b"overlay"
    for missing in (
        f"/gallery/{round_id}/{sha}/c2/raw.png",  # a symlink
        f"/gallery/{round_id}/{'d' * 40}/c1/raw.png",  # another snippets commit
        f"/gallery/i8-e99/{sha}/c1/raw.png",  # another dataset
        f"/gallery/{round_id}/{sha}/..%2F..%2Fc1/raw.png",
        f"/gallery/{round_id}/{sha}/.hidden/raw.png",
        f"/gallery/{round_id}/{sha}/c1/crop.json",
        f"/gallery/..%2Fissue-7/{sha}/c1/raw.png",
    ):
        assert s.handle("GET", missing, {}, b"")[0] == 404, missing
    without = BufferService(Ledger(tmp_path / "l2.sqlite"))
    assert without.handle("GET", f"/gallery/{round_id}/{sha}/c1/raw.png", {}, b"")[0] == 503


def test_crop_png_route_serves_storage_root_only(tmp_path):
    root = tmp_path / "crops"
    (root / "confocal-human-u937" / "c1").mkdir(parents=True)
    (root / "confocal-human-u937" / "c1" / "overlay.png").write_bytes(b"png")
    (root / "confocal-human-u937" / "c1" / "raw.png").symlink_to(root / "confocal-human-u937" / "c1" / "overlay.png")
    s = BufferService(Ledger(tmp_path / "l.sqlite"), storage_root=root)
    status, body, headers = s.handle("GET", "/crops/confocal-human-u937/c1/overlay.png", {}, b"")
    assert (status, body, headers["Content-Type"]) == (200, b"png", "image/png")
    assert s.handle("GET", "/crops/confocal-human-u937/c1/raw.png", {}, b"")[0] == 404  # a symlink
    assert s.handle("GET", "/crops/confocal-human-u937/c2/overlay.png", {}, b"")[0] == 404
    assert s.handle("GET", "/crops/..%2F..%2Fetc/c1/overlay.png", {}, b"")[0] == 404
    assert s.handle("GET", "/crops/.hidden/c1/overlay.png", {}, b"")[0] == 404
    assert s.handle("GET", "/crops/confocal-human-u937/c1/labels.png", {}, b"")[0] == 404
    assert BufferService(Ledger(tmp_path / "l2.sqlite")).handle("GET", "/crops/a/b/raw.png", {}, b"")[0] == 503


def test_sync_keeps_a_row_prepared_during_the_scan(tmp_path, monkeypatch):
    root = tmp_path / "crops"
    write_prepared(root / "u937", U937)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), storage_root=root)
    s.ledger.put_dataset("old", {"state": "prepared"})

    def scan_while_set_stage_runs(path):
        s.ledger.put_dataset("late", {"state": "prepared"})  # mia-data's set_stage("done"), after the scan passed
        return {}

    monkeypatch.setattr("mia_buffers.api.scan_storage", scan_while_set_stage_runs)
    assert call(s, "POST", "/sync/datasets")[1] == {"prepared": 0, "demoted": 1, "revoked_kept": 0}
    assert s.ledger.dataset("late")["state"] == "prepared" and s.ledger.dataset("old") is None


def test_records_with_a_shared_or_integer_id_all_land(tmp_path):
    s = svc(tmp_path)
    ref = "a" * 40
    records = [
        {"path": "datasets/a.yaml", "record": {"id": "same"}},
        {"path": "datasets/b.yaml", "record": {"id": "same"}},
        {"path": "datasets/c.yaml", "record": {"id": 2024}},
    ]
    assert call(s, "PUT", f"/records/{ref}", payload={"records": records}) == (200, {"ref": ref, "count": 3}, {})
    assert [path for path, _ in s.ledger.records(ref)] == ["datasets/a.yaml", "datasets/b.yaml", "datasets/c.yaml"]


def test_sync_refuses_a_missing_or_empty_storage_root(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_dataset("rec", {"state": "prepared", "folder": "rec"})
    for root in (tmp_path / "unmounted", tmp_path / "empty"):
        if root.name == "empty":
            root.mkdir()
        status, out, _ = call(BufferService(led, storage_root=root), "POST", "/sync/datasets")
        assert status == 503 and out == {"error": f"storage_root {root} is missing or empty; not syncing"}
    assert led.dataset("rec")["state"] == "prepared"


def test_sync_409_while_running(tmp_path):
    root = tmp_path / "crops"
    write_prepared(root / "u937", U937)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), storage_root=root)
    with s._sync_lock:
        status, out, _ = call(s, "POST", "/sync/datasets")
    assert status == 409 and out == {"error": "a datasets sync is running"}
    assert s.ledger.datasets() == []


def buffers_cfg(tmp_path, local_root):
    return make_cfg(tmp_path, paths={"local_root": str(local_root) if local_root else None})


def test_open_ledger_refuses_a_missing_ledger_when_backups_exist(tmp_path):
    cfg = buffers_cfg(tmp_path, tmp_path / "local")
    backups = cfg.paths.state_dir / "backups"
    for stamp in ("20261001T000000Z", "20261024T120000Z"):
        Ledger(backups / f"buffers-{stamp}.sqlite").put_buffer("b", {"x": 1})
    with pytest.raises(RuntimeError, match="buffers-20261024T120000Z.sqlite") as err:
        open_ledger(cfg)
    assert str(ledger_path(cfg)) in str(err.value) and not ledger_path(cfg).exists()


def test_open_ledger_first_install_starts_empty(tmp_path):
    led = open_ledger(buffers_cfg(tmp_path, tmp_path / "local"))
    assert led.catalogs() == [] and led.datasets() == []


def test_prune_backups_keeps_the_newest(tmp_path):
    for i in range(50):
        (tmp_path / f"buffers-2026010100{i:02d}00Z.sqlite").write_text("")
    prune_backups(tmp_path, keep=48)
    assert len(list(tmp_path.glob("buffers-*.sqlite"))) == 48
    assert not (tmp_path / "buffers-20260101000000Z.sqlite").exists()


def test_serve_refuses_without_local_root(tmp_path):
    with pytest.raises(RuntimeError, match="local_root"):
        ledger_path(buffers_cfg(tmp_path, None))


def test_serve_refuses_a_network_root(tmp_path, monkeypatch):
    monkeypatch.setattr("mia_core.fs.mount_type", lambda path, mounts=None: "nfs4")
    with pytest.raises(RuntimeError, match="network filesystem"):
        open_ledger(buffers_cfg(tmp_path, tmp_path / "local"))


def test_volume_refs_are_keyed_by_artifact(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload={**body(), "artifact": "art1"})
    assert call(s, "GET", "/assets/staged/art1")[1]["refs"] == ["b"]
    call(s, "PUT", "/buffers/b/volumes/c", payload=body("c"))  # no artifact: the directory
    assert call(s, "GET", "/assets/staged/%2Fstaged%2Fc")[1]["refs"] == ["b"]


def test_assets_merge_filter_and_totals(tmp_path):
    s = svc(tmp_path)
    assert call(s, "PUT", "/assets/staged/a1", payload={"state": "staging", "record": "rec", "job": "7"})[0] == 200
    status, out, _ = call(s, "PUT", "/assets/staged/a1", payload={"state": "ready", "bytes": 100})
    assert status == 200 and out == {
        "kind": "staged",
        "key": "a1",
        "data": {"state": "ready", "record": "rec", "job": "7", "bytes": 100},
    }
    call(s, "PUT", "/assets/staged/a2", payload={"state": "evicted", "record": "rec", "bytes": 50})
    status, out, _ = call(s, "PUT", "/assets/staged/a1", payload={"state": "gone"})
    assert status == 422 and "state must be one of" in out["error"]
    assert call(s, "PUT", "/assets/staged/a1", payload=[1])[0] == 422
    assert s.handle("PUT", "/assets/staged/a1", {}, b"not json")[0] == 422
    status, out, _ = call(s, "GET", "/assets/staged", {"state": "ready"})
    assert status == 200 and [a["key"] for a in out["assets"]] == ["a1"] and out["bytes"] == 100
    assert [a["key"] for a in call(s, "GET", "/assets/staged", {"record": "rec"})[1]["assets"]] == ["a1", "a2"]


def test_locks_route_lists_holders(tmp_path):
    locks = LockDir(tmp_path / "locks")
    assert locks.take("raw", "rec", "job-1", 900)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t", locks=locks)
    status, out, _ = call(s, "GET", "/locks")  # a read route: no token
    assert status == 200 and [(h["kind"], h["key"], h["owner"], h["stale"]) for h in out["locks"]] == [
        ("raw", "rec", "job-1", False)
    ]
    assert call(svc(tmp_path / "other"), "GET", "/locks")[1] == {"locks": []}


def test_sync_assets_skips_a_missing_or_empty_root(tmp_path):
    raw = tmp_path / "raw"
    (raw / "rec").mkdir(parents=True)
    (raw / "rec" / "k").write_bytes(b"x" * 8)
    led = Ledger(tmp_path / "l.sqlite")
    led.put_asset("staged", "art", {"state": "ready", "bytes": 5})
    led.put_asset("raw", "gone", {"state": "ready", "bytes": 5})
    s = BufferService(led, raw_root=raw, staged_root=tmp_path / "unmounted")
    status, out, _ = call(s, "POST", "/sync/assets")
    assert status == 200 and out == {
        "scanned": 1,
        "new": 1,
        "missing": 1,
        "dropped": 0,
        "building": [],
        "empty": [],
        "skipped": ["staged"],
    }
    assert led.asset("staged", "art")["state"] == "ready" and led.asset("raw", "gone")["state"] == "missing"
    assert led.asset("raw", "rec")["state"] == "ready" and led.asset("raw", "rec")["bytes"] == 8
    assert BufferService(led).handle("POST", "/sync/assets", {}, b"")[0] == 503
    worker = BufferService(led, token="t", raw_root=raw)
    assert worker.handle("POST", "/sync/assets", {}, b"")[0] == 401


def test_sync_assets_is_exclusive(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), raw_root=tmp_path / "raw", staged_root=tmp_path / "staged")
    with s._sync_lock:
        status, out, _ = call(s, "POST", "/sync/assets")
    assert status == 409 and out == {"error": "a sync is running"}


AUTH = {"Authorization": "Bearer t"}


def worker(s, method, path, payload=None):
    return s.handle(method, path, {}, json.dumps(payload).encode() if payload is not None else b"", AUTH)


def test_records_route_defaults_to_the_newest_ref(tmp_path):
    s = svc(tmp_path)
    assert call(s, "GET", "/records")[:2] == (404, {"error": "no catalog synced"})
    old, new = "a" * 40, "b" * 40
    call(s, "PUT", f"/records/{old}", payload={"records": [{"path": "x.yaml", "record": {"id": "x"}}]})
    call(s, "PUT", f"/records/{new}", payload={"records": [{"path": "y.yaml", "record": {"id": "y"}}]})
    status, out, _ = call(s, "GET", "/records")
    assert status == 200 and out == {"ref": new, "records": [{"path": "y.yaml", "record": {"id": "y"}}]}
    assert call(s, "GET", "/records", {"ref": old})[1]["records"][0]["path"] == "x.yaml"
    assert call(s, "GET", "/records", {"ref": "c" * 40})[0] == 404


def test_status_board_routes(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    worker(s, "PUT", "/buffers/b", CFG)
    assert call(s, "PUT", "/buffers/b/datasets/rec", payload={"status": "fetching"})[0] == 401
    status, out, _ = worker(s, "PUT", "/buffers/b/datasets/rec", {"status": "fetching", "job": "j1"})
    assert status == 200 and out["status"] == "fetching" and out["job"] == "j1"
    status, out, _ = worker(s, "PUT", "/buffers/b/datasets/rec", {"status": "done"})
    assert status == 422 and "status must be one of" in out["error"]
    assert worker(s, "PUT", "/buffers/b/datasets/rec", {"status": "failed", "nope": 1})[0] == 422
    assert worker(s, "PUT", "/buffers/zz/datasets/rec", {"status": "failed"})[0] == 404
    worker(s, "PUT", "/buffers/b/datasets/rec", {"status": "failed", "reason": "boom"})
    rows = call(s, "GET", "/buffers/b/datasets")[1]["datasets"]
    assert [(row["record"], row["status"], row["reason"], row["job"]) for row in rows] == [
        ("rec", "failed", "boom", None)
    ]
    assert call(s, "POST", "/buffers/b/events", payload={"kind": "created"})[0] == 401
    status, out, _ = worker(s, "POST", "/buffers/b/events", {"kind": "created", "issue": 3, "event_id": "e1"})
    assert status == 200 and out["seq"] == 1 and out["detail"] == "" and out["issue"] == 3
    worker(s, "POST", "/buffers/b/events", {"kind": "needs-human", "detail": "why"})
    status, out, _ = worker(s, "POST", "/buffers/b/events", {"kind": "exploded"})
    assert status == 422 and "kind must be one of" in out["error"]
    events = call(s, "GET", "/buffers/b/events")[1]["events"]
    assert [(event["seq"], event["kind"]) for event in events] == [(1, "created"), (2, "needs-human")]
    assert call(s, "GET", "/buffers")[1]["buffers"][0]["latest_event"]["kind"] == "needs-human"
    # a round's `created` and `planned` come before its buffer config: the events routes need none
    assert worker(s, "POST", "/buffers/early/events", {"kind": "created"})[0] == 200
    assert [event["kind"] for event in call(s, "GET", "/buffers/early/events")[1]["events"]] == ["created"]


def test_retire_route_archives_and_frees_refs(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t", archive_dir=tmp_path / "archive")
    worker(s, "PUT", "/buffers/b", CFG)
    approve(s)
    worker(s, "PUT", "/buffers/b/volumes/a", {**body(), "artifact": "art1"})
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    call(s, "GET", "/buffers/b/next", {"n": "3", "consumer": cid})
    assert call(s, "DELETE", "/buffers/b/refs")[0] == 401
    status, out, _ = worker(s, "DELETE", "/buffers/b/refs")
    archive = tmp_path / "archive" / "b.draws.jsonl.gz"
    assert status == 200 and out == {"refs": 2, "draws": 3, "archive": str(archive)}
    with gzip.open(archive, "rt") as fh:
        assert [json.loads(line)["seq"] for line in fh] == [1, 2, 3]
    assert call(s, "GET", "/assets/staged/art1")[0] == 404  # no row and no reference left
    assert call(s, "GET", "/buffers/b/status")[1]["volumes"] == 1  # the page still shows the retired buffer
    assert worker(s, "DELETE", "/buffers/zz/refs")[0] == 404


def test_events_accept_sourcing_and_staged(tmp_path):
    """The staged gate's lifecycle: `sourcing` when sourcing starts, `staged` at the gate (spec §15)."""
    s = BufferService(Ledger(tmp_path / "l.sqlite"))
    call(s, "PUT", "/buffers/b", payload=CFG)
    assert worker(s, "POST", "/buffers/b/events", {"kind": "sourcing"})[0] == 200
    assert worker(s, "POST", "/buffers/b/events", {"kind": "staged"})[0] == 200
    assert [e["kind"] for e in call(s, "GET", "/buffers/b/events")[1]["events"]] == ["sourcing", "staged"]


def test_only_an_approved_buffer_serves(tmp_path):
    """Serving needs an affirmative authorization (spec autonomous-gates §3): the latest event must be
    `approved`, `ready` or `idle`. Anything else is 409 with a reason; `retired` stays 410; no events is 409."""
    s = BufferService(Ledger(tmp_path / "l.sqlite"))
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    no_events = (409, {"error": "buffer has no approved event"})
    assert call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[:2] == no_events
    for kind in ("created", "planned", "sourcing", "staged"):
        worker(s, "POST", "/buffers/b/events", {"kind": kind})
        waiting = (409, {"error": "buffer not approved"})
        assert call(s, "GET", "/buffers/b/next", {"consumer": "x"})[:2] == waiting, kind
        assert call(s, "GET", "/buffers/b/split/val", {"consumer": "x"})[:2] == waiting, kind
        assert call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[:2] == waiting, kind
    worker(s, "POST", "/buffers/b/events", {"kind": "needs-human", "detail": "why"})
    needs = (409, {"error": "buffer needs a human"})
    assert call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[:2] == needs
    assert call(s, "GET", "/buffers/b/status")[0] == 200 and call(s, "GET", "/buffers/b/events")[0] == 200
    worker(s, "POST", "/buffers/b/events", {"kind": "approved"})
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    worker(s, "POST", "/buffers/b/events", {"kind": "ready"})
    assert call(s, "GET", "/buffers/b/next", {"consumer": cid})[0] == 200
    worker(s, "POST", "/buffers/b/events", {"kind": "idle"})  # a returning consumer still draws
    assert call(s, "GET", "/buffers/b/next", {"consumer": cid})[0] == 200
    worker(s, "POST", "/buffers/b/events", {"kind": "needs-human", "detail": "degraded"})
    assert call(s, "GET", "/buffers/b/next", {"consumer": cid})[:2] == needs
    worker(s, "POST", "/buffers/b/events", {"kind": "retired"})
    assert call(s, "GET", "/buffers/b/next", {"consumer": cid})[:2] == (410, {"error": "buffer retired"})


def test_a_retired_buffer_serves_no_consumer(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"))
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    call(s, "POST", "/buffers/b/events", payload={"kind": "approved"})
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    call(s, "POST", "/buffers/b/events", payload={"kind": "retired"})
    gone = (410, {"error": "buffer retired"})
    assert call(s, "GET", "/buffers/b/next", {"consumer": cid})[:2] == gone
    assert call(s, "GET", "/buffers/b/split/val", {"consumer": cid})[:2] == gone
    assert call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[:2] == gone
    assert call(s, "GET", "/buffers/b/status")[0] == 200  # the page still reads it


def test_status_stats_pyramids_and_next_skips_missing(tmp_path, monkeypatch):
    staged = tmp_path / "staged"
    s = BufferService(Ledger(tmp_path / "l.sqlite"), staged_root=staged)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    for vid in ("a", "c"):
        (staged / vid / "v0.zarr").mkdir(parents=True)
        call(s, "PUT", f"/buffers/b/volumes/{vid}", payload={**body(vid), "path": str(staged / vid / "v0.zarr")})
    out = call(s, "GET", "/buffers/b/status")[1]
    assert out["missing"] == [] and out["servable"] == {"train": 2, "val": 2}
    shutil.rmtree(staged / "c")
    seen = []
    real_exists = __import__("os").path.exists
    monkeypatch.setattr("mia_buffers.api.os.path.exists", lambda path: seen.append(path) or real_exists(path))
    out = call(s, "GET", "/buffers/b/status")[1]
    assert out["missing"] == ["c"] and out["servable"] == {"train": 1, "val": 1} and out["volumes"] == 2
    assert len(seen) == 2  # each volume's path once per call
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    descriptors = call(s, "GET", "/buffers/b/next", {"n": "50", "consumer": cid})[1]["descriptors"]
    assert {d["volume"] for d in descriptors} == {"a"}
    (staged / "c" / "v0.zarr").mkdir(parents=True)  # back on disk: the next status serves it again
    assert call(s, "GET", "/buffers/b/status")[1]["missing"] == []
    assert call(s, "GET", "/buffers/b/status")[1]["servable"] == {"train": 2, "val": 2}


def test_status_without_a_staged_root_does_not_stat(tmp_path, monkeypatch):
    s = svc(tmp_path)  # a dev service has no disk view: every registered volume stays servable
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())

    def boom(path):
        raise AssertionError(f"stat {path}")

    monkeypatch.setattr("mia_buffers.api.os.path.exists", boom)
    out = call(s, "GET", "/buffers/b/status")[1]
    assert out["missing"] == [] and out["servable"] == {"train": 1, "val": 1}


def test_buffers_list_does_not_stat(tmp_path, monkeypatch):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), staged_root=tmp_path / "staged")
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())  # its path does not exist
    call(s, "GET", "/buffers/b/status")  # marks it missing

    def boom(path):
        raise AssertionError(f"stat {path}")

    monkeypatch.setattr("mia_buffers.api.os.path.exists", boom)
    monkeypatch.setattr("os.path.exists", boom)
    status, out, _ = call(s, "GET", "/buffers")
    assert status == 200 and [row["id"] for row in out["buffers"]] == ["b"]
    assert out["buffers"][0]["missing"] == 1 and out["buffers"][0]["servable"] == {"train": 0, "val": 0}


def test_buffer_config_takes_the_lifecycle_fields(tmp_path):
    s = svc(tmp_path)
    recipe = {"tensorizer": "muvit-2d", "params": {}, "harness_commit": "c", "env_digest": "d"}
    lifecycle = {"recipe": recipe, "plan_hash": "p", "idle_hours": 1.5, "retire_after_days": 2, "auto_approve": True}
    assert call(s, "PUT", "/buffers/b", payload={**CFG, **lifecycle})[0] == 200
    config = call(s, "GET", "/buffers/b/status")[1]["config"]
    assert {key: config[key] for key in lifecycle} == {**lifecycle, "retire_after_days": 2.0}
    assert call(s, "PUT", "/buffers/d", payload=CFG)[0] == 200
    defaults = call(s, "GET", "/buffers/d/status")[1]["config"]
    assert (defaults["idle_hours"], defaults["retire_after_days"], defaults["auto_approve"]) == (72, 14, False)


# --- the page's routes: static files, meta, usage, the keeper's last pass ----------------------------------


def test_root_serves_the_page_and_ui_files(tmp_path):
    ui = tmp_path / "ui"
    ui.mkdir()
    (ui / "index.html").write_text("<!doctype html><title>t</title>")
    (ui / "app.js").write_text("let a = 1;")
    (ui / "notes.txt").write_text("not served")
    s = BufferService(Ledger(tmp_path / "l.sqlite"), ui_dir=ui)
    assert call(s, "GET", "/") == (
        200,
        b"<!doctype html><title>t</title>",
        {"Content-Type": "text/html; charset=utf-8"},
    )
    assert call(s, "GET", "/ui/app.js") == (200, b"let a = 1;", {"Content-Type": "text/javascript"})
    assert call(s, "GET", "/ui/notes.txt")[0] == 404  # not one of the served suffixes
    assert call(s, "GET", "/ui/missing.css")[0] == 404
    assert call(svc(tmp_path), "GET", "/")[0] == 404  # no ui_dir: no page

    server = make_server(s, "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        page = httpx.get(f"{base}/")
        assert page.status_code == 200 and page.headers["content-type"] == "text/html; charset=utf-8"
        assert page.content == b"<!doctype html><title>t</title>"
        meta = httpx.get(f"{base}/meta")  # a dict still goes out as JSON
        assert meta.headers["content-type"] == "application/json" and meta.json()["service_url"] is None
        assert httpx.get(f"{base}/keeper").json() is None  # JSON null, not an empty body
    finally:
        server.shutdown()


def test_ui_route_refuses_traversal(tmp_path):
    ui = tmp_path / "ui"
    ui.mkdir()
    (tmp_path / "secret.html").write_text("no")
    (ui / ".hidden.js").write_text("no")
    s = BufferService(Ledger(tmp_path / "l.sqlite"), ui_dir=ui)
    for path in (
        "/ui/..%2Fsecret.html",
        "/ui/%2E%2E%2Fsecret.html",
        "/ui/../secret.html",
        "/ui/.hidden.js",
        "/ui/..",
        "/ui/a%00.js",
        "/ui/sub/x.js",
        "/ui/",
    ):
        assert call(s, "GET", path)[0] == 404, path


def test_meta_route(tmp_path):
    empty = call(svc(tmp_path), "GET", "/meta")[1]
    assert empty == {
        "buffers_repo": None,
        "datasets_repo": None,
        "catalog_repo": None,
        "service_url": None,
        "datasets_workflow_url": None,
        "gallery_branch": None,
        "roots": {},
        "quota": {},
        "keeper": {},
        "lines": {
            "dataset": ["new", "matching", "developing", "testing", "review", "merging", "done"],
            "buffer": ["new", "planning", "plan-review", "sourcing", "staged", "ready", "idle", "retired"],
            "prep": ["new", "requested", "done"],
        },
    }
    from mia_buffers.api import page_meta

    cfg = make_cfg(tmp_path, paths={"staged_root": {"path": str(tmp_path / "staged"), "url": "https://fg/staged"}})
    meta = page_meta(cfg)
    assert set(meta) == set(empty) and meta["buffers_repo"] == cfg.github.buffers_repo
    assert (meta["datasets_repo"], meta["catalog_repo"]) == (cfg.github.datasets_repo, cfg.github.catalog_repo)
    assert meta["service_url"] == cfg.service.url
    assert meta["datasets_workflow_url"] is None
    cfg.temporal.ui_url = "https://temporal.example.org:8443/"
    repo = quote(cfg.github.datasets_repo, safe="")
    assert page_meta(cfg)["datasets_workflow_url"] == (
        f"https://temporal.example.org:8443/namespaces/mia-data/workflows/mia-data%2F{repo}%2Fissue-"
    )
    assert meta["roots"] == {
        "work_root": None,
        "storage_root": None,
        "raw_root": None,
        "staged_root": "https://fg/staged",
    }
    assert meta["quota"] == {"raw_bytes": 0, "staged_bytes": 0, "headroom_bytes": 200e9, "hold_hours": 48}
    assert meta["keeper"] == {"minutes": 30, "catalog_sync_hours": 24}
    assert call(BufferService(Ledger(tmp_path / "m.sqlite"), meta=meta), "GET", "/meta")[1] == meta


def test_usage_live_and_materialised(tmp_path, monkeypatch):
    import time as time_module
    from datetime import UTC, datetime

    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t", archive_dir=tmp_path / "archive")
    worker(s, "PUT", "/buffers/b", CFG)
    approve(s)
    assert call(s, "GET", "/usage/zz")[0] == 404
    one, two = (call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"] for _ in range(2))
    assert call(s, "GET", "/buffers/b/next", {"consumer": one})[0] == 204  # no volume yet: two empty answers
    assert call(s, "GET", "/buffers/b/split/val", {"consumer": one})[0] == 204
    worker(s, "PUT", "/buffers/b/volumes/a", body())
    yesterday = time_module.time() - 86400
    with monkeypatch.context() as patched:
        patched.setattr("mia_buffers.ledger.time.time", lambda: yesterday)
        call(s, "GET", "/buffers/b/next", {"n": "2", "consumer": one})
    call(s, "GET", "/buffers/b/next", {"n": "3", "consumer": one})
    call(s, "GET", "/buffers/b/next", {"n": "1", "consumer": two})
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    before = datetime.fromtimestamp(yesterday, UTC).strftime("%Y-%m-%d")
    expected = [
        {"day": before, "draws": 2, "consumers": 1, "empty": 0},
        {"day": today, "draws": 4, "consumers": 2, "empty": 2},
    ]
    status, out, _ = call(s, "GET", "/usage/b")
    assert status == 200 and out == {
        "buffer": "b",
        "days": expected,
        "last_hour": {"draws": 4, "requests": 2, "empty": 2},  # requests and empty answers share a unit
        "max_empty_fraction": 0.05,
    }
    assert worker(s, "DELETE", "/buffers/b/refs")[1]["draws"] == 6
    assert s.ledger.max_seq("b") == 0  # the draws are gone, the days stay
    assert call(s, "GET", "/usage/b")[1]["days"] == expected


def test_keeper_kv_routes(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    assert call(s, "GET", "/keeper")[:2] == (200, b"null")
    assert call(s, "PUT", "/keeper", payload={"evicted": []})[0] == 401
    assert worker(s, "PUT", "/keeper", [1, 2])[0] == 422
    status, stored, _ = worker(s, "PUT", "/keeper", {"evicted": ["staged a"], "dry_run": False})
    assert status == 200 and stored["evicted"] == ["staged a"] and isinstance(stored["at"], float)
    assert call(s, "GET", "/keeper")[1] == stored
    assert worker(s, "PUT", "/keeper", {"at": 5.0})[1] == {"at": 5.0}  # replaced whole; a given `at` kept


# --- assessments, budgets, volumes routes, revoked datasets -------------------------


def test_assessment_routes(tmp_path):
    from mia_core.judge.policy import new_assessment

    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    row = new_assessment(
        gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s", verdict="approve", by="human:a", at=10.0
    )
    assert call(s, "PUT", f"/assessments/{row['id']}", payload=row)[0] == 401
    assert worker(s, "PUT", f"/assessments/{row['id']}", row)[:2] == (200, row)
    assert worker(s, "PUT", f"/assessments/{row['id']}", {**row, "gate": "vibes"})[0] == 422
    assert worker(s, "PUT", "/assessments/other-id", row)[0] == 422  # the path and the body must agree
    assert call(s, "GET", f"/assessments/{row['id']}")[:2] == (200, row) and call(s, "GET", "/assessments/x")[0] == 404
    listed = call(s, "GET", "/assessments", {"subject_kind": "crop_set", "subject_ref": "i7-e11:s"})[1]
    assert listed == {"assessments": [row]}
    assert call(s, "GET", "/assessments", {"gate": "staged"})[1] == {"assessments": []}
    judged = new_assessment(
        gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s", verdict="approve", by="judge", at=11.0
    )
    worker(s, "PUT", f"/assessments/{judged['id']}", judged)
    assert call(s, "GET", "/assessments", {"by": "judge"})[1] == {"assessments": [judged]}
    assert call(s, "GET", "/assessments", {"before": judged["id"]})[1] == {"assessments": [row]}
    assert call(s, "GET", "/assessments", {"limit": "0"})[0] == 422


def test_decisions_route_is_open_and_returns_agreement(tmp_path):
    from mia_core.judge.policy import GATES, new_assessment

    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    human = new_assessment(
        gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s", verdict="approve", by="human:a", at=10.0
    )
    judge = new_assessment(
        gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s", verdict="approve", by="judge", at=11.0
    )
    for row in (human, judge):
        worker(s, "PUT", f"/assessments/{row['id']}", row)
    status, out, _ = call(s, "GET", "/decisions")
    assert status == 200 and out["assessments"] == [judge, human] and set(out["agreement"]) == set(GATES)
    assert out["agreement"]["crops"]["agree"] == 1 and out["agreement"]["review"]["judged"] == 0
    only = call(s, "GET", "/decisions", {"gate": "crops", "limit": "1"})[1]
    assert only["assessments"] == [judge] and list(only["agreement"]) == ["crops"]
    assert call(s, "GET", "/decisions", {"offset": "1"})[1]["assessments"] == [human]  # older ones: the next page
    assert call(s, "GET", "/decisions", {"gate": "vibes"})[0] == 422


def test_a_persons_verdict_on_a_pr_heads_crops_names_its_dataset_and_meets_the_judges(tmp_path):
    """A person's verdict on crops made from a snippet PR head is recorded on the head, the crop judge's on the crop
    set: the table names the dataset on both, and the agreement pairs them."""
    from mia_core.judge.policy import new_assessment

    head = "16fe7340f499cbc4d8d70e8af069520fe7711d7e"
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    worker(s, "PUT", "/subjects/dataset/7", {"key": "rec-a", "stage": "review"})
    crop_set = f"i7-e11:i7-e11-{head[:12]}"
    rows = [
        new_assessment(
            gate="crops", subject_kind="crop_set", subject_ref=crop_set, verdict="approve", by="judge", at=10.0
        ),
        new_assessment(gate="review", subject_kind="pr_head", subject_ref=head, verdict="approve", by="judge", at=11.0),
        new_assessment(
            gate="crops", subject_kind="pr_head", subject_ref=head, verdict="approve", by="human:page", at=12.0
        ),
    ]
    for row in rows:
        worker(s, "PUT", f"/assessments/{row['id']}", row)
    out = call(s, "GET", "/decisions")[1]
    assert [(row["by"], row["record"]) for row in out["assessments"]] == [
        ("human:page", "rec-a"),
        ("judge", "rec-a"),
        ("judge", "rec-a"),
    ]
    crops = out["agreement"]["crops"]
    assert (crops["judged"], crops["agree"], crops["pending"]) == (1, 1, 0) and crops["pairs"][0]["human"] == "approve"
    only = call(s, "GET", "/decisions", {"gate": "review"})[1]["assessments"]  # one gate's rows still name the dataset
    assert [row["record"] for row in only] == ["rec-a"]
    mine = call(s, "GET", "/decisions", {"record": "rec-a"})[1]["assessments"]  # the dataset page's own list
    assert len(mine) == 3
    # a head no crop set was made from, with nothing to say whose it is, stays unnamed
    other = new_assessment(
        gate="crops", subject_kind="pr_head", subject_ref="ab" * 20, verdict="reject", by="human:page", at=13.0
    )
    worker(s, "PUT", f"/assessments/{other['id']}", other)
    assert "record" not in call(s, "GET", "/decisions")[1]["assessments"][0]


def test_budget_routes(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    root = quote("o/buffers#7", safe="")
    body = {"reservation_id": "job-1", "kind": "match", "usd": 6.0, "caps": {"max_usd": 10}}
    assert call(s, "POST", f"/budgets/{root}/reserve", payload=body)[0] == 401
    status, out, _ = worker(s, "POST", f"/budgets/{root}/reserve", body)
    assert status == 200 and out["ok"] and out["usage"]["root"] == "o/buffers#7"
    refused = worker(s, "POST", f"/budgets/{root}/reserve", {**body, "reservation_id": "job-2"})[1]
    assert not refused["ok"] and "would exceed max_usd 10" in refused["reason"]
    settled = worker(s, "POST", f"/budgets/{root}/settle", {"reservation_id": "job-1", "usd": 1.5})[1]
    assert settled["spent_usd"] == 1.5
    assert call(s, "GET", f"/budgets/{root}")[1]["spent_usd"] == 1.5 and call(s, "GET", "/budgets/nobody")[0] == 404
    assert [row["root"] for row in call(s, "GET", "/budgets")[1]["budgets"]] == ["o/buffers#7"]
    assert worker(s, "POST", f"/budgets/{root}/reserve", {"kind": "x"})[0] == 422


def test_volumes_routes_and_quarantined_status(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    worker(s, "PUT", "/buffers/b", CFG)
    worker(s, "PUT", "/buffers/b/volumes/rec%2Fv0.zarr", {**body("rec/v0.zarr"), "artifact": "art1"})
    worker(s, "PUT", "/buffers/b/volumes/other%2Fv0.zarr", {**body("other/v0.zarr"), "artifact": "art2"})
    worker(s, "PUT", "/assets/staged/art1", {"quarantined": True, "quarantined_at": 1.0, "quarantined_by": "human:a"})
    assert call(s, "GET", "/buffers/b/status")[1]["quarantined"] == ["rec/v0.zarr"]
    rows = call(s, "GET", "/buffers/b/volumes")[1]["volumes"]
    assert [(row["id"], row["artifact"], row["record"]) for row in rows] == [
        ("other/v0.zarr", "art2", "other"),
        ("rec/v0.zarr", "art1", "rec"),
    ]
    assert call(s, "DELETE", "/buffers/b/volumes/rec%2Fv0.zarr")[0] == 401
    assert worker(s, "DELETE", "/buffers/b/volumes/rec%2Fv0.zarr")[:2] == (200, {"deleted": "rec/v0.zarr"})
    assert worker(s, "DELETE", "/buffers/b/volumes/rec%2Fv0.zarr")[0] == 404
    assert call(s, "GET", "/buffers/b/status")[1]["quarantined"] == []
    assert call(s, "GET", "/assets/staged/art1")[1]["refs"] == []
    assert call(s, "GET", "/buffers/zz/volumes")[0] == 404


def test_datasets_accept_the_revoked_state(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"))
    assert call(s, "PUT", "/datasets/rec", payload={"state": "revoked"})[1]["state"] == "revoked"
    assert call(s, "GET", "/datasets", {"state": "revoked"})[1]["datasets"][0]["id"] == "rec"


def test_a_quarantined_volume_is_never_served_nor_counted(tmp_path):
    """The deterministic backstop of a revocation: buffer-api itself stops serving the volume, before any keeper
    pass or workflow reacts (spec autonomous-gates §6)."""
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    approve(s)
    call(s, "PUT", "/buffers/b/volumes/rec%2Fv0.zarr", payload={**body("rec/v0.zarr"), "artifact": "art1"})
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    assert call(s, "GET", "/buffers/b/next", {"n": "2", "consumer": cid})[0] == 200
    assert call(s, "GET", "/buffers/b/status")[1]["servable"] == {"train": 1, "val": 1}
    call(s, "PUT", "/assets/staged/art1", payload={"quarantined": True, "quarantined_at": 1.0})
    assert call(s, "GET", "/buffers/b/next", {"n": "2", "consumer": cid})[0] == 204
    assert call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "1", "consumer": cid})[0] == 204
    status = call(s, "GET", "/buffers/b/status")[1]
    assert status["servable"] == {"train": 0, "val": 0} and status["quarantined"] == ["rec/v0.zarr"]
    assert call(s, "GET", "/buffers")[1]["buffers"][0]["servable"] == {"train": 0, "val": 0}
    call(s, "PUT", "/assets/staged/art1", payload={"quarantined": False})  # a repair released it
    assert call(s, "GET", "/buffers/b/next", {"n": "2", "consumer": cid})[0] == 200


# --- subjects: what the page acts on (spec webapp-actions §4.1, §8) ---


def _subjects(tmp_path):
    return BufferService(Ledger(tmp_path / "l.sqlite"), meta={"datasets_repo": "o/data", "buffers_repo": "o/buffers"})


def test_adopt_then_read_a_subject(tmp_path):
    s = _subjects(tmp_path)
    status, row, _ = call(s, "PUT", "/subjects/dataset/234", payload={"key": "rec-a", "stage": "review"})
    assert status == 200 and row["issue"] == 234
    status, row, _ = call(s, "GET", "/subjects/dataset/rec-a")
    assert (status, row["status"], row["actions"]) == (200, "waiting", ["approve", "cancel", "go", "reject"])
    assert call(s, "GET", "/subjects/dataset/nope")[0] == 404
    assert call(s, "GET", "/subjects/widget/x")[0] == 404
    assert call(s, "PUT", "/subjects/dataset/235", payload={"key": "rec-b", "stage": "bogus"})[0] == 422
    assert call(s, "PUT", "/subjects/dataset/235", payload={"key": "rec-a"})[0] == 409


def test_a_subject_key_with_spaces_a_slash_and_an_accent_round_trips(tmp_path):
    s, key = _subjects(tmp_path), "EM pool / v2 é"
    call(s, "PUT", "/subjects/buffer/8", payload={"key": key, "stage": "ready", "data": {"spec": "name: x"}})
    status, row, _ = call(s, "GET", "/subjects/buffer/" + quote(key, safe=""))
    assert (status, row["key"], row["data"]) == (200, key, {"spec": "name: x"})


def test_needs_lists_what_waits_on_a_person(tmp_path):
    s = _subjects(tmp_path)
    call(s, "PUT", "/subjects/dataset/1", payload={"key": "rec-a", "stage": "review"})
    call(s, "PUT", "/subjects/dataset/2", payload={"key": "rec-b", "stage": "testing"})
    call(s, "PUT", "/subjects/buffer/8", payload={"key": "pool", "stage": "needs-human"})
    call(s, "PUT", "/subjects/prep/9", payload={"key": "prep-9", "stage": "done"})
    needs = call(s, "GET", "/needs")[1]["needs"]
    assert sorted((row["kind"], row["key"], row["status"]) for row in needs) == [
        ("buffer", "pool", "stuck"),
        ("dataset", "rec-a", "waiting"),
    ]
    # one kind at a time, with every kind's count for the page's toggle buttons
    call(s, "PUT", "/subjects/dataset/3", payload={"key": "rec-c", "stage": "failed"})
    only = call(s, "GET", "/needs", {"kind": "dataset", "limit": "1"})[1]
    assert len(only["needs"]) == 1 and only["needs"][0]["kind"] == "dataset" and only["total"] == 2
    assert sorted(row["key"] for row in call(s, "GET", "/needs", {"kind": "dataset"})[1]["needs"]) == ["rec-a", "rec-c"]
    assert only["kinds"] == {"dataset": 2, "buffer": 1, "prep": 0}
    assert call(s, "GET", "/needs", {"kind": "prep"})[1]["needs"] == []
    assert call(s, "GET", "/needs", {"kind": "issue"})[0] == 422


def test_lookup_by_number_url_and_key(tmp_path):
    s = _subjects(tmp_path)
    call(s, "PUT", "/subjects/dataset/234", payload={"key": "rec-a"})
    call(s, "PUT", "/subjects/buffer/234", payload={"key": "pool"})
    call(s, "PUT", "/subjects/prep/9", payload={"key": "prep-9"})

    def found(text):
        return sorted((row["kind"], row["key"]) for row in call(s, "GET", "/lookup", {"q": text})[1]["subjects"])

    assert found("#234") == found(" 234 ") == [("buffer", "pool"), ("dataset", "rec-a")]
    assert found("https://github.com/o/data/issues/234") == [("dataset", "rec-a")]
    assert found("https://github.com/o/buffers/issues/234#issuecomment-1") == [("buffer", "pool")]
    assert found("https://github.com/o/buffers/issues/9") == [("prep", "prep-9")]
    assert found("rec-a") == [("dataset", "rec-a")]
    assert found("https://github.com/o/other/issues/234") == [] and found("nothing") == []
    assert call(s, "GET", "/lookup")[0] == 422


def test_adopting_needs_the_token_and_reading_does_not(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    assert call(s, "PUT", "/subjects/dataset/1", payload={"key": "rec-a"})[0] == 401
    assert call(s, "GET", "/needs")[0] == 200 and call(s, "GET", "/lookup", {"q": "1"})[0] == 200


def test_stream_over_http_and_a_reader_that_leaves(tmp_path):
    server = make_server(BufferService(Ledger(tmp_path / "l.sqlite")), "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with httpx.stream("GET", url + "/stream", timeout=5) as response:
            assert response.headers["content-type"] == "text/event-stream"
            assert httpx.put(url + "/subjects/dataset/234", json={"key": "rec-a"}).status_code == 200
            assert next(response.iter_lines()) == 'data: {"kind": "dataset", "number": 234}'
        # the reader is gone: the next change must not take the service down
        assert httpx.put(url + "/subjects/dataset/235", json={"key": "rec-b"}).status_code == 200
        assert httpx.get(url + "/subjects/dataset/rec-b").json()["number"] == 235
    finally:
        server.shutdown()


# --- the timeline: what a round writes instead of an issue comment (spec webapp-actions §4.3, §7) ---


class Kicks:
    """Stands in for the Temporal gateway: records which subjects' mirrors were kicked."""

    def __init__(self):
        self.kicked = []

    def kick_mirror(self, kind, number):
        self.kicked.append((kind, number))


def test_a_stage_and_an_entry_are_written_read_and_mirrored_back(tmp_path):
    kicks = Kicks()
    s = BufferService(Ledger(tmp_path / "l.sqlite"), dispatcher=kicks)
    call(s, "PUT", "/subjects/dataset/234", payload={"key": "rec-a"})
    entry = {"body": "testing\n<!-- m -->", "marker": "<!-- m -->", "round": "i234-e5", "stage": "testing"}
    status, out, _ = call(s, "POST", "/timeline/dataset/234", payload=entry)
    assert status == 200 and out["subject"]["status"] == "working" and out["entry"]["seq"] == 1
    assert call(s, "POST", "/timeline/dataset/234", payload=entry)[1]["entry"]["seq"] == 1  # a retry appends nothing
    status, out, _ = call(s, "POST", "/timeline/dataset/234", payload={"stage": "review"})  # a stage with no text
    assert (out["subject"]["stage"], out["entry"]) == ("review", None)
    assert kicks.kicked == [("dataset", 234)] * 3  # every write: the label follows the stage
    status, out, _ = call(s, "GET", "/timeline/dataset/234")
    assert status == 200 and out["subject"]["key"] == "rec-a" and [row["seq"] for row in out["entries"]] == [1]
    assert call(s, "PUT", "/timeline/dataset/234/1", payload={"comment_url": "https://x/c"})[0] == 200
    assert call(s, "GET", "/timeline/dataset/234")[1]["entries"][0]["comment_url"] == "https://x/c"
    assert call(s, "PUT", "/timeline/dataset/234/9", payload={"comment_url": None})[0] == 404


def test_timeline_writes_are_checked(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/subjects/dataset/1", payload={"key": "rec-a"})
    assert call(s, "POST", "/timeline/dataset/2", payload={"stage": "testing"})[0] == 404  # no such subject
    assert call(s, "POST", "/timeline/widget/1", payload={"stage": "testing"})[0] == 404
    assert call(s, "POST", "/timeline/dataset/1", payload={"stage": "bogus"})[0] == 422
    assert call(s, "POST", "/timeline/dataset/1", payload={})[0] == 422  # nothing to write
    assert call(s, "POST", "/timeline/dataset/1", payload={"body": "text"})[0] == 422  # an entry needs its marker
    assert call(s, "GET", "/timeline/dataset/2")[0] == 404
    guarded = BufferService(Ledger(tmp_path / "g.sqlite"), token="t")
    assert call(guarded, "POST", "/timeline/dataset/1", payload={"stage": "testing"})[0] == 401
    assert call(guarded, "PUT", "/timeline/dataset/1/1", payload={"comment_url": None})[0] == 401


def test_a_start_kicks_every_subject_with_something_to_mirror(tmp_path):
    kicks = Kicks()
    s = BufferService(Ledger(tmp_path / "l.sqlite"), dispatcher=kicks)
    call(s, "PUT", "/subjects/dataset/1", payload={"key": "rec-a"})
    call(s, "PUT", "/subjects/buffer/8", payload={"key": "pool"})
    call(s, "POST", "/timeline/dataset/1", payload={"body": "a", "marker": "a"})
    call(s, "PUT", "/timeline/dataset/1/1", payload={"comment_url": None})
    call(s, "POST", "/timeline/buffer/8", payload={"body": "b", "marker": "b"})
    kicks.kicked.clear()
    s.kick_unmirrored()
    assert kicks.kicked == [("buffer", 8)]


# --- actions: what the page's buttons call (spec webapp-actions §4.2, §5, §10) ---

JSON = {"Content-Type": "application/json", "Host": "svc:8765"}
REF_A = "a" * 40


def act(s, path, payload, headers=None):
    return s.handle("POST", path, {}, json.dumps(payload).encode(), {**JSON, **(headers or {})})


class Gateway(Kicks):
    """Stands in for the Temporal gateway: records what was sent; `fail` makes Temporal unreachable."""

    def __init__(self, running=True):
        super().__init__()
        self.sent, self.fail, self.running, self.terminated = [], None, running, []

    def terminate(self, kind, number, reason):
        self.terminated.append((kind, number))
        return self.running

    def send(self, kind, number, signal, event):
        if self.fail:
            raise DispatchError(self.fail)
        self.sent.append((kind, number, signal, event))

    def signal_running(self, kind, number, signal, event):
        if self.fail:
            raise DispatchError(self.fail)
        if self.running:
            self.sent.append((kind, number, signal, event))
        return self.running

    def health(self):
        return {"temporal": True, "queues": {"mia-data": True, "mia-buffers": False}}

    def run_status(self, kind, number):
        return "RUNNING" if self.running else None


def _acting(tmp_path, **gateway_kwargs):
    """A service with a worker token, a gateway stand-in and a synced catalog holding rec-a and rec-b."""
    gateway = Gateway(**gateway_kwargs)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t", dispatcher=gateway)
    records = [
        {
            "path": f"{name}.yaml",
            "record": {"id": name, "data": {"download_url": f"https://data.example.org/{name}.zip"}},
        }
        for name in ("rec-a", "rec-b")
    ]
    assert worker(s, "PUT", f"/records/{REF_A}", {"records": records})[0] == 200
    return s, gateway


def test_a_dataset_whose_record_names_no_data_url_cannot_be_started(tmp_path):
    """The record is read from the newest synced catalog on every request: while it names no data URL the dataset
    has no Start and its go is refused, and a sync that brings a record with one makes it startable again."""
    s, gateway = _acting(tmp_path)

    def sync(ref, record):
        assert worker(s, "PUT", f"/records/{ref}", {"records": [{"path": "rec-a.yaml", "record": record}]})[0] == 200

    assert act(s, "/datasets/rec-a/actions", {"action": "go", "key": "click-1"})[0] == 202
    assert call(s, "GET", "/subjects/dataset/rec-a")[1]["gaps"] == []
    sync("b" * 40, {"id": "rec-a", "landing_url": "https://data.example.org/rec-a"})
    status, refused, _ = act(s, "/datasets/rec-a/actions", {"action": "go", "key": "click-2"})
    assert status == 409 and "the record cannot be run: it names no data URL" in refused["error"]
    subject = call(s, "GET", "/subjects/dataset/rec-a")[1]
    assert "go" not in subject["actions"] and subject["gaps"] and len(gateway.sent) == 1
    assert call(s, "GET", "/catalog/datasets/rec-a")[1]["row"]["gaps"] == subject["gaps"]
    sync("c" * 40, {"id": "rec-a", "data": {"download_url": "https://data.example.org/rec-a.zip"}})
    subject = call(s, "GET", "/subjects/dataset/rec-a")[1]
    assert "go" in subject["actions"] and subject["gaps"] == []
    assert call(s, "GET", "/catalog/datasets/rec-a")[1]["row"]["gaps"] == []


def test_a_record_with_a_gap_gets_no_subject(tmp_path):
    s, gateway = _acting(tmp_path)
    bare = [{"path": "rec-c.yaml", "record": {"id": "rec-c"}}]
    assert worker(s, "PUT", f"/records/{'b' * 40}", {"records": bare})[0] == 200
    assert act(s, "/datasets/rec-c/actions", {"action": "go", "key": "click-1"})[0] == 409
    assert call(s, "GET", "/subjects/dataset/rec-c")[0] == 404 and not gateway.sent


def test_a_first_go_creates_the_dataset_subject_and_starts_its_workflow(tmp_path):
    s, gateway = _acting(tmp_path)
    status, action, _ = act(s, "/datasets/rec-a/actions", {"action": "go", "key": "click-1"})
    assert status == 202 and (action["state"], action["actor"], action["action"]) == ("sent", "page", "go")
    subject = call(s, "GET", "/subjects/dataset/rec-a")[1]
    assert subject["issue"] is None and subject["number"] == action["number"] > 20_000_000  # from the clock
    (sent,) = gateway.sent
    event = LabelEvent("go", str(action["id"]), "page", action["at"])
    assert sent == ("dataset", subject["number"], "restart", event)  # a dataset's go: its round starts in a new run
    assert event.kind == "go"
    timeline = call(s, "GET", f"/timeline/dataset/{subject['number']}")[1]
    assert [entry["body"] for entry in timeline["entries"]] == ["**Action:** `go` by `page`"]
    assert timeline["entries"][0]["marker"] == f"<!-- mia: action {action['id']} -->"
    assert [row["id"] for row in timeline["actions"]] == [action["id"]]
    assert gateway.kicked == [("dataset", subject["number"])]  # the mirror opens the issue
    # a double click, or a retry after a lost answer: the same key is the same action and starts nothing
    assert act(s, "/datasets/rec-a/actions", {"action": "go", "key": "click-1"})[1]["id"] == action["id"]
    assert len(gateway.sent) == 1


def test_a_go_for_a_dataset_with_a_known_issue_keeps_that_issues_number(tmp_path):
    s, gateway = _acting(tmp_path)
    worker(s, "PUT", "/datasets/rec-b", {"state": "needs-human", "issue": 234})  # labelled once, never adopted
    assert act(s, "/datasets/rec-b/actions", {"action": "go"})[0] == 202
    subject = call(s, "GET", "/subjects/dataset/rec-b")[1]
    assert (subject["number"], subject["issue"]) == (234, 234) and gateway.sent[0][1] == 234


def test_an_action_for_something_that_is_not_there_is_refused(tmp_path):
    s, gateway = _acting(tmp_path)
    assert act(s, "/datasets/nope/actions", {"action": "go"})[0] == 404  # not a record of the synced catalog
    assert act(s, "/datasets/rec-a/actions", {"action": "approve", "revision": 0})[0] == 404  # nothing to approve
    assert act(s, "/datasets/rec-a/actions", {"action": "dance"})[0] == 422
    assert gateway.sent == [] and call(s, "GET", "/subjects/dataset/rec-a")[0] == 404


def test_only_the_actions_the_stage_accepts_are_taken(tmp_path):
    s, gateway = _acting(tmp_path)
    worker(s, "PUT", "/subjects/dataset/7", {"key": "rec-a", "stage": "testing"})
    status, out, _ = act(s, "/datasets/rec-a/actions", {"action": "go"})
    assert status == 409 and "`testing`" in out["error"] and "cancel" in out["error"]  # says what it would take
    assert act(s, "/datasets/rec-a/actions", {"action": "revoke"})[0] == 409
    assert gateway.sent == [] and call(s, "GET", "/timeline/dataset/7")[1]["actions"] == []  # nothing recorded
    assert act(s, "/datasets/rec-a/actions", {"action": "cancel"})[0] == 202 and gateway.sent[0][2] == "cancel"


def test_decisions_show_what_the_person_wrote_with_their_verdict(tmp_path):
    """An assessment names the action it came from, and the note is read from that action, whatever else the
    person did before or after."""
    from mia_core.judge.policy import new_assessment

    s, _ = _acting(tmp_path)
    worker(s, "PUT", "/subjects/dataset/7", {"key": "rec-a"})
    worker(s, "POST", "/timeline/dataset/7", {"stage": "review"})
    for note in ("the masks are empty", "and the axes are swapped"):  # two rejects by one person, moments apart
        assert act(s, "/datasets/rec-a/actions", {"action": "reject", "revision": 1, "note": note})[0] == 202
    first, second = s.ledger.actions("dataset", 7)

    def verdict(by, at, action=None):
        row = new_assessment(
            gate="crops",
            subject_kind="crop_set",
            subject_ref="i7-e11:i7-e11-abc",
            verdict="reject",
            by=by,
            at=at,
            action=action,
        )
        worker(s, "PUT", f"/assessments/{row['id']}", row)

    verdict("judge", 10.0)
    verdict("human:page", 11.0)  # one that names no action shows no note
    verdict("human:page", 12.0, first["id"])  # written after both actions, and still the first one's
    rows = call(s, "GET", "/decisions", {"record": "rec-a"})[1]["assessments"]
    assert [(row["by"], row.get("note")) for row in rows] == [
        ("human:page", "the masks are empty"),
        ("human:page", None),
        ("judge", None),
    ]
    assert second["payload"]["note"] == "and the axes are swapped"


def test_a_verdict_names_the_revision_the_person_saw(tmp_path):
    s, gateway = _acting(tmp_path)
    worker(s, "PUT", "/subjects/dataset/7", {"key": "rec-a"})
    worker(s, "POST", "/timeline/dataset/7", {"stage": "review"})  # revision 1: the set the page shows
    assert act(s, "/datasets/rec-a/actions", {"action": "approve"})[0] == 422  # a verdict must say what it saw
    worker(s, "POST", "/timeline/dataset/7", {"stage": "testing"})
    worker(s, "POST", "/timeline/dataset/7", {"stage": "review"})  # revision 3: another set since
    status, out, _ = act(s, "/datasets/rec-a/actions", {"action": "approve", "revision": 1})
    assert status == 409 and "changed" in out["error"] and gateway.sent == []
    note = "the labels are shifted <!-- mia-data: x --> by one plane"
    status, action, _ = act(s, "/datasets/rec-a/actions", {"action": "reject", "revision": 3, "note": note})
    assert status == 202 and action["payload"] == {"note": note} and gateway.sent[0][2] == "reject"
    body = call(s, "GET", "/timeline/dataset/7")[1]["entries"][-1]["body"]
    assert body.startswith("**Action:** `reject` by `page`") and "shifted" in body and "<!-- mia-data" not in body


def test_temporal_away_fails_the_action_and_a_retry_sends_the_same_one(tmp_path):
    s, gateway = _acting(tmp_path)
    gateway.fail = "Temporal did not answer within 10 seconds"
    status, out, _ = act(s, "/datasets/rec-a/actions", {"action": "go", "key": "click-1"})
    assert status == 503 and out["action"]["state"] == "failed" and "did not answer" in out["action"]["detail"]
    number = out["action"]["number"]
    assert call(s, "GET", f"/timeline/dataset/{number}")[1]["entries"] == []  # nothing is claimed to have happened
    gateway.fail = None
    worker(s, "POST", f"/timeline/dataset/{number}", {"stage": "matching"})  # the first try did arrive after all
    status, again, _ = act(s, "/datasets/rec-a/actions", {"action": "go", "key": "click-1"})  # Retry: the same key
    assert status == 202 and (again["id"], again["state"]) == (out["action"]["id"], "sent")
    assert [sent[3].event_id for sent in gateway.sent] == [str(again["id"])]  # the workflow drops a repeat by this id


def test_a_cancel_with_no_workflow_running_settles_the_subject_itself(tmp_path):
    s, gateway = _acting(tmp_path, running=False)
    worker(s, "PUT", "/subjects/dataset/7", {"key": "rec-a", "stage": "matching"})  # its run was terminated by hand
    assert act(s, "/datasets/rec-a/actions", {"action": "cancel"})[0] == 202 and gateway.sent == []
    subject = call(s, "GET", "/subjects/dataset/rec-a")[1]
    assert (subject["stage"], subject["actions"]) == ("cancelled", ["go", "revoke"])
    assert "no workflow was running" in call(s, "GET", "/timeline/dataset/7")[1]["entries"][-1]["body"]


def test_action_routes_take_json_from_this_origin_only_and_a_named_actor_needs_the_token(tmp_path):
    s, gateway = _acting(tmp_path)
    go = json.dumps({"action": "go"}).encode()
    form = {"Content-Type": "application/x-www-form-urlencoded", "Host": "svc:8765"}
    assert s.handle("POST", "/datasets/rec-a/actions", {}, go, form)[0] == 415
    assert act(s, "/datasets/rec-a/actions", {"action": "go"}, {"Origin": "http://evil.example"})[0] == 403
    assert gateway.sent == []
    assert act(s, "/datasets/rec-a/actions", {"action": "go"}, {"Origin": "http://svc:8765"})[0] == 202
    named = {"action": "go", "actor": "prep:9", "root": "o/buffers#9"}
    assert act(s, "/datasets/rec-b/actions", named)[0] == 401  # anyone may click; only a worker may be someone
    status, action, _ = act(s, "/datasets/rec-b/actions", named, AUTH)
    assert status == 202 and action["actor"] == "prep:9" and action["payload"] == {"root": "o/buffers#9"}
    assert gateway.sent[-1][3].actor == "prep:9"


def test_a_sent_action_is_asked_until_its_workflow_answers(tmp_path):
    """Between a click and its workflow's first write the stage is still the old one. The subject says what was
    asked, so the page does not offer the old stage's buttons as if nothing had been clicked."""
    s, _ = _acting(tmp_path)
    action = act(s, "/datasets/rec-a/actions", {"action": "go"})[1]
    number = action["number"]
    view = call(s, "GET", "/subjects/dataset/rec-a")[1]
    assert (view["stage"], view["actions"]) == ("new", ["go"])  # the stage table is not bent
    assert (view["asked"]["id"], view["asked"]["action"]) == (action["id"], "go") and 0 < view["asked"]["wait_s"] <= 60
    assert call(s, "GET", f"/timeline/dataset/{number}")[1]["subject"]["asked"]["id"] == action["id"]
    assert call(s, "GET", "/subjects/dataset", {"limit": "5"})[1]["subjects"][0]["asked"]["id"] == action["id"]
    # the mirror's writes are not the workflow's answer
    worker(s, "PUT", f"/subjects/dataset/{number}/issue", {"issue": 240})
    worker(s, "PUT", f"/timeline/dataset/{number}/1", {"comment_url": None})
    assert call(s, "GET", "/subjects/dataset/rec-a")[1]["asked"]["id"] == action["id"]
    # a stage the workflow writes is
    worker(s, "POST", f"/timeline/dataset/{number}", {"stage": "matching"})
    view = call(s, "GET", "/subjects/dataset/rec-a")[1]
    assert view["asked"] is None and view["actions"] == ["cancel"]


def test_what_ends_an_asked_action(tmp_path, monkeypatch):
    s, gateway = _acting(tmp_path)
    worker(s, "PUT", "/subjects/dataset/7", {"key": "rec-a", "stage": "needs-human"})
    asked = lambda: call(s, "GET", "/subjects/dataset/rec-a")[1]["asked"]  # noqa: E731
    assert asked() is None  # nothing was clicked
    # an entry of the workflow's, with no stage
    act(s, "/datasets/rec-a/actions", {"action": "go"})
    assert asked()["action"] == "go"
    worker(s, "POST", "/timeline/dataset/7", {"body": "a round is running", "marker": "m1"})
    assert asked() is None
    # a refusal
    action = act(s, "/datasets/rec-a/actions", {"action": "go"})[1]
    worker(s, "PUT", f"/actions/{action['id']}", {"state": "refused", "detail": "a round is running"})
    assert asked() is None
    # an action that never reached Temporal was not asked of any workflow
    gateway.fail = "temporal is away"
    assert act(s, "/datasets/rec-a/actions", {"action": "go"})[0] == 503
    assert asked() is None
    gateway.fail = None
    # the service's own answer: a cancel with no workflow running, and a force stop
    worker(s, "POST", "/timeline/dataset/7", {"stage": "testing"})
    gateway.running = False
    assert act(s, "/datasets/rec-a/actions", {"action": "cancel"})[0] == 202
    view = call(s, "GET", "/subjects/dataset/rec-a")[1]
    assert (view["stage"], view["asked"]) == ("cancelled", None)
    gateway.running = True
    # a workflow that never answers does not hold the buttons for ever
    act(s, "/datasets/rec-a/actions", {"action": "go"})
    assert asked()["action"] == "go"
    monkeypatch.setattr(api_module, "ASKED_WAIT_S", 0)
    assert asked() is None


def test_a_worker_marks_an_action_refused(tmp_path):
    s, _ = _acting(tmp_path)
    action = act(s, "/datasets/rec-a/actions", {"action": "go"})[1]
    path = f"/actions/{action['id']}"
    assert call(s, "PUT", path, payload={"state": "refused", "detail": "a round is running"})[0] == 401
    status, out, _ = worker(s, "PUT", path, {"state": "refused", "detail": "a round is running"})
    assert status == 200 and (out["state"], out["detail"]) == ("refused", "a round is running")
    assert call(s, "GET", path)[1]["state"] == "refused" and call(s, "GET", "/actions/1")[0] == 404
    assert worker(s, "PUT", "/actions/1", {"state": "refused", "detail": "x"})[0] == 404
    assert worker(s, "PUT", path, {"state": "sent", "detail": ""})[0] == 422  # only a refusal comes from a worker


def test_health_run_status_and_the_issue_write_back(tmp_path):
    s, _ = _acting(tmp_path)
    assert call(s, "GET", "/health")[1] == {"temporal": True, "queues": {"mia-data": True, "mia-buffers": False}}
    number = act(s, "/datasets/rec-a/actions", {"action": "go"})[1]["number"]
    assert call(s, "GET", f"/run/dataset/{number}")[1] == {"status": "RUNNING"}
    assert call(s, "PUT", f"/subjects/dataset/{number}/issue", payload={"issue": 240})[0] == 401
    assert worker(s, "PUT", f"/subjects/dataset/{number}/issue", {"issue": 240})[1]["issue"] == 240
    assert [row["key"] for row in call(s, "GET", "/lookup", {"q": "#240"})[1]["subjects"]] == ["rec-a"]
    bare = svc(tmp_path / "bare")  # no gateway: a dev service says so instead of pretending
    assert call(bare, "GET", "/health")[1] == {"temporal": False, "queues": {}}
    worker(bare, "PUT", "/subjects/dataset/7", {"key": "rec-a"})
    assert act(bare, "/datasets/rec-a/actions", {"action": "go"})[0] == 503


# --- buffer specs and preparation requests from the page (spec webapp-actions §4.4, §5) ---

SPEC_TEXT = (FIXTURES / "spec.yaml").read_text()
SPEC_NAME = yaml.safe_load(SPEC_TEXT)["name"]
SPEC_ACTIONS = f"/specs/{quote(SPEC_NAME, safe='')}/actions"
PREP_TEXT = yaml.safe_dump(
    {"schema_version": 1, "kind": "preparation", "catalog": {"repo": "o/catalog", "ref": REF_A}, "records": ["rec-a"]}
)


def _stage(s, kind, number, stage):
    """Write a stage as a worker would; the subject's new revision."""
    return worker(s, "POST", f"/timeline/{kind}/{number}", {"stage": stage})[1]["subject"]["revision"]


def test_a_spec_is_created_run_and_edited_under_its_name(tmp_path):
    s, gateway = _acting(tmp_path)
    status, out, _ = act(s, "/specs", {"spec": SPEC_TEXT})
    assert status == 200 and (out["subject"]["key"], out["subject"]["stage"]) == (SPEC_NAME, "new")
    assert out["subject"]["data"] == {"spec": SPEC_TEXT} and gateway.sent == []  # saved, not run
    number = out["subject"]["number"]
    status, action, _ = act(s, "/specs", {"spec": SPEC_TEXT, "action": "go", "plan_first": True, "key": "c1"})
    assert status == 202 and action["payload"] == {"spec": SPEC_TEXT, "plan_first": True}
    assert gateway.sent[0][:3] == ("buffer", number, "plan")  # "show me the plan first" is the plan signal
    entry = call(s, "GET", f"/timeline/buffer/{number}")[1]["entries"][0]["body"]
    assert entry.startswith("**Action:** `go` by `page` (plan first)") and f"```yaml\n{SPEC_TEXT}```" in entry
    edited = SPEC_TEXT + "# tuned\n"
    again = act(s, "/specs", {"spec": edited})[1]["subject"]  # the name exists: an edit of that subject
    assert (again["number"], again["data"]) == (number, {"spec": edited})
    assert len(s.ledger.find_subjects(kind="buffer")) == 1
    status, out, _ = act(s, "/specs", {"spec": "name: [not a spec"})
    assert status == 422 and "error" in out
    assert act(s, "/specs", {"spec": PREP_TEXT})[0] == 422  # a preparation request is not a buffer spec


def test_buffer_actions_follow_the_stage_table(tmp_path):
    s, gateway = _acting(tmp_path)
    number = act(s, "/specs", {"spec": SPEC_TEXT})[1]["subject"]["number"]
    status, action, _ = act(s, SPEC_ACTIONS, {"action": "go"})  # runs the stored spec
    assert (
        status == 202 and action["payload"] == {"spec": SPEC_TEXT} and gateway.sent[-1][:3] == ("buffer", number, "go")
    )
    _stage(s, "buffer", number, "sourcing")
    for refused in ("go", "retire", "approve", "revoke", "keep"):
        assert act(s, SPEC_ACTIONS, {"action": refused, "revision": 1, "records": ["rec-a"]})[0] == 409, refused
    revision = _stage(s, "buffer", number, "staged")
    assert act(s, SPEC_ACTIONS, {"action": "approve", "revision": revision - 1})[0] == 409  # an older board
    assert act(s, SPEC_ACTIONS, {"action": "approve", "revision": revision})[0] == 202
    _stage(s, "buffer", number, "ready")
    assert act(s, SPEC_ACTIONS, {"action": "cancel"})[0] == 409  # a serving buffer is not a round in progress
    assert act(s, SPEC_ACTIONS, {"action": "revoke"})[0] == 422  # which records?
    status, action, _ = act(s, SPEC_ACTIONS, {"action": "revoke", "records": ["rec-a", "rec-b"]})
    assert status == 202 and action["payload"] == {"records": ["rec-a", "rec-b"]}
    _stage(s, "buffer", number, "idle")
    assert act(s, SPEC_ACTIONS, {"action": "keep"})[0] == 202 and act(s, SPEC_ACTIONS, {"action": "retire"})[0] == 202
    assert [sent[2] for sent in gateway.sent] == ["go", "approve", "revoke", "keep", "retire"]
    edited = SPEC_TEXT + "# tuned\n"
    status, action, _ = act(s, SPEC_ACTIONS, {"action": "go", "spec": edited})  # edit and run again
    assert status == 202 and action["payload"] == {"spec": edited}
    assert call(s, "GET", f"/subjects/buffer/{quote(SPEC_NAME, safe='')}")[1]["data"] == {"spec": edited}
    assert act(s, "/specs/nope/actions", {"action": "go"})[0] == 404
    renamed = edited.replace(f"name: {SPEC_NAME}", "name: another-name")
    assert act(s, SPEC_ACTIONS, {"action": "go", "spec": renamed})[0] == 422  # a spec's name is its subject


def test_files_are_in_place_is_refused_while_a_copy_is_still_writing(tmp_path):
    import os
    import time

    gateway = Gateway()
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t", raw_root=tmp_path / "raw", dispatcher=gateway)
    number = act(s, "/specs", {"spec": SPEC_TEXT})[1]["subject"]["number"]
    _stage(s, "buffer", number, "sourcing")
    dropped = {"action": "dropped", "record": "rec-a"}
    status, out, _ = act(s, SPEC_ACTIONS, dropped)
    assert status == 409 and "no file" in out["error"]
    incoming = tmp_path / "raw" / "rec-a" / "incoming"
    incoming.mkdir(parents=True)
    (incoming / "a.tif").write_bytes(b"x")
    status, out, _ = act(s, SPEC_ACTIONS, dropped)
    assert status == 409 and "still being written" in out["error"] and gateway.sent == []
    old = time.time() - 300
    os.utime(incoming / "a.tif", (old, old))
    assert act(s, SPEC_ACTIONS, dropped)[0] == 202 and gateway.sent == [("buffer", number, "dropped", "rec-a")]
    assert act(s, SPEC_ACTIONS, {"action": "dropped"})[0] == 422  # which record?
    assert act(s, SPEC_ACTIONS, {"action": "dropped", "record": "../x"})[0] == 422


def test_stop_terminates_the_run_retires_the_subjects_buffers_and_says_what_it_leaves(tmp_path):
    s, gateway = _acting(tmp_path)
    number = act(s, "/specs", {"spec": SPEC_TEXT})[1]["subject"]["number"]
    mine, other = f"i{number}-e5", "i999-e1"
    for buffer in (mine, other):
        worker(s, "PUT", f"/buffers/{buffer}", CFG)
        worker(s, "POST", f"/buffers/{buffer}/events", {"kind": "approved"})
    assert act(s, SPEC_ACTIONS, {"action": "stop"})[0] == 409  # nothing live at `new`
    _stage(s, "buffer", number, "ready")
    assert act(s, SPEC_ACTIONS, {"action": "stop"})[0] == 202
    assert gateway.terminated == [("buffer", number)]
    assert call(s, "GET", f"/subjects/buffer/{quote(SPEC_NAME, safe='')}")[1]["stage"] == "retired"
    latest = {row["id"]: (row["latest_event"] or {}).get("kind") for row in call(s, "GET", "/buffers")[1]["buffers"]}
    assert latest == {mine: "retired", other: "approved"}
    said = call(s, "GET", f"/timeline/buffer/{number}")[1]["entries"][-1]["body"]
    assert "workflow stopped" in said and f"`{mine}`" in said and "cluster job" in said


def test_a_preparation_request_is_filed_and_run_in_one_post(tmp_path):
    s, gateway = _acting(tmp_path)
    status, action, _ = act(s, "/prep", {"spec": PREP_TEXT, "key": "selection-1"})
    assert status == 202 and action["payload"] == {"spec": PREP_TEXT}
    number = action["number"]
    subject = call(s, "GET", f"/subjects/prep/prep-{number}")[1]
    assert subject["data"] == {"spec": PREP_TEXT} and gateway.sent[-1][:3] == ("prep", number, "go")
    assert act(s, "/prep", {"spec": PREP_TEXT, "key": "selection-1"})[1]["id"] == action["id"]  # a double click
    assert len(s.ledger.find_subjects(kind="prep")) == 1 and len(gateway.sent) == 1
    assert act(s, "/prep", {"spec": SPEC_TEXT})[0] == 422  # a buffer spec is not a preparation request
    _stage(s, "prep", number, "requested")
    actions = f"/prep/{number}/actions"
    assert act(s, actions, {"action": "go"})[0] == 409  # a request in progress is cancelled first
    assert act(s, actions, {"action": "cancel"})[0] == 202 and gateway.sent[-1][2] == "cancel"
    assert act(s, actions, {"action": "stop"})[0] == 202 and gateway.terminated == [("prep", number)]
    assert call(s, "GET", f"/subjects/prep/prep-{number}")[1]["stage"] == "done"
    assert "dataset rounds" in call(s, "GET", f"/timeline/prep/{number}")[1]["entries"][-1]["body"]
    assert act(s, actions, {"action": "go"})[0] == 202 and gateway.sent[-1][3].kind == "go"  # run again
    assert act(s, "/prep/1/actions", {"action": "go"})[0] == 404


def test_needs_names_the_actions_that_were_not_delivered(tmp_path):
    s, gateway = _acting(tmp_path)
    gateway.fail = "Temporal is away"
    failed = act(s, "/datasets/rec-a/actions", {"action": "go", "key": "k"})[1]["action"]
    needs = call(s, "GET", "/needs")[1]
    assert [(row["id"], row["subject"]["key"]) for row in needs["failed"]] == [(failed["id"], "rec-a")]
    gateway.fail = None
    act(s, "/datasets/rec-a/actions", {"action": "go", "key": "k"})  # retried: delivered
    assert call(s, "GET", "/needs")[1]["failed"] == []


def test_a_kinds_subjects_are_listed_and_a_number_finds_its_subject(tmp_path):
    s, _ = _acting(tmp_path)
    number = act(s, "/prep", {"spec": PREP_TEXT})[1]["number"]  # a request filed on the page: no issue yet
    worker(s, "PUT", "/subjects/prep/9", {"key": "prep-9", "stage": "done"})
    worker(s, "PUT", "/subjects/buffer/8", {"key": "pool"})
    listed = call(s, "GET", "/subjects/prep")[1]["subjects"]
    assert [(row["key"], row["status"]) for row in listed] == [("prep-9", "done"), (f"prep-{number}", "not started")]
    assert call(s, "GET", "/subjects/widget")[0] == 404
    found = call(s, "GET", "/lookup", {"q": str(number)})[1]["subjects"]  # a round id shows the number, not an issue
    assert [row["key"] for row in found] == [f"prep-{number}"]


# --- found by the whole-branch review ---


def test_a_retry_long_after_the_failure_is_checked_against_the_stage_again(tmp_path, monkeypatch):
    """A Retry sends the same key so an action that did arrive is not run twice; it must not carry an old click
    past the stage table for ever. A revoke that failed, retried after a new round began, is refused."""
    from mia_buffers import ledger as ledger_module

    now = [1_800_000_000.0]
    monkeypatch.setattr(ledger_module.time, "time", lambda: now[0])
    monkeypatch.setattr("mia_buffers.api.time.time", lambda: now[0])
    s, gateway = _acting(tmp_path)
    worker(s, "PUT", "/subjects/dataset/7", {"key": "rec-a", "stage": "done"})
    gateway.fail = "Temporal is away"
    assert act(s, "/datasets/rec-a/actions", {"action": "revoke", "key": "r1"})[0] == 503
    gateway.fail = None
    now[0] += 30  # soon after: the same click, sent again (the first may have arrived after all)
    worker(s, "POST", "/timeline/dataset/7", {"stage": "matching"})
    assert act(s, "/datasets/rec-a/actions", {"action": "revoke", "key": "r1"})[0] == 202
    gateway.fail = "Temporal is away"
    worker(s, "POST", "/timeline/dataset/7", {"stage": "done"})
    assert act(s, "/datasets/rec-a/actions", {"action": "revoke", "key": "r2"})[0] == 503
    gateway.fail = None
    now[0] += 3600  # an hour later a new round is running: the stale notice's Retry is a new decision
    worker(s, "POST", "/timeline/dataset/7", {"stage": "matching"})
    status, out, _ = act(s, "/datasets/rec-a/actions", {"action": "revoke", "key": "r2"})
    assert status == 409 and "`matching`" in out["error"] and len(gateway.sent) == 1


def test_a_note_cannot_mention_anyone_from_the_bots_account(tmp_path):
    s, _ = _acting(tmp_path)
    worker(s, "PUT", "/subjects/dataset/7", {"key": "rec-a"})
    revision = _stage(s, "dataset", 7, "review")
    note = "ask @octocat or @org/team about the shift"
    assert act(s, "/datasets/rec-a/actions", {"action": "reject", "revision": revision, "note": note})[0] == 202
    body = call(s, "GET", "/timeline/dataset/7")[1]["entries"][-1]["body"]
    assert "@octocat" not in body and "@org/team" not in body and "octocat" in body  # mirrored text pings nobody
    assert s.ledger.actions("dataset", 7)[-1]["payload"]["note"] == note  # the agent still reads it as written


def test_needs_lists_a_buffer_whose_dataset_waits_for_its_files(tmp_path):
    s, _ = _acting(tmp_path)
    go = act(s, "/specs", {"spec": SPEC_TEXT, "action": "go"})[1]
    number, buffer = go["number"], f"i{go['number']}-e{go['id']}"
    _stage(s, "buffer", number, "sourcing")
    assert call(s, "GET", "/needs")[1]["needs"] == []  # working, and nothing waits on a person
    worker(s, "PUT", f"/buffers/{buffer}", CFG)
    worker(s, "PUT", f"/buffers/{buffer}/datasets/rec-a", {"status": "waiting-drop", "reason": "", "job": None})
    worker(s, "PUT", f"/buffers/{buffer}/datasets/rec-b", {"status": "staging", "reason": "", "job": None})
    (waiting,) = call(s, "GET", "/needs")[1]["needs"]
    assert (waiting["key"], waiting["status"], waiting["drops"]) == (SPEC_NAME, "working", ["rec-a"])
    _stage(s, "buffer", number, "ready")
    assert call(s, "GET", "/needs")[1]["needs"] == []  # the round is over: a waiting row of it asks for nothing


# --- one page of a list (docs/plans/2026-11-12-ui-domain-flow.md): with `limit`, a route answers a page and a total


def test_assets_paged_name_their_dataset_and_sort_by_it(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/datasets/rec-b", payload={"state": "prepared", "issue": 12})
    call(
        s, "PUT", "/assets/staged/a1", payload={"state": "ready", "record": "rec-b", "bytes": 100, "levels_key": "p256"}
    )
    call(s, "PUT", "/assets/staged/a2", payload={"state": "evicted", "record": "rec-a", "bytes": 50})
    call(s, "PUT", "/assets/staged/a3", payload={"state": "ready", "record": "rec-c"})
    whole = call(s, "GET", "/assets/staged")[1]
    assert [a["key"] for a in whole["assets"]] == ["a1", "a2", "a3"] and "total" not in whole
    assert set(whole["assets"][0]) == {"key", "data", "refs"}  # a worker's answer is what it was
    status, out, _ = call(s, "GET", "/assets/staged", {"limit": "2", "sort": "dataset"})
    assert status == 200 and out["total"] == 3 and out["bytes"] == 100
    assert [(a["record"], a["issue"], a["key"]) for a in out["assets"]] == [("rec-a", None, "a2"), ("rec-b", 12, "a1")]
    out = call(s, "GET", "/assets/staged", {"limit": "2", "offset": "2", "sort": "dataset"})[1]
    assert [a["record"] for a in out["assets"]] == ["rec-c"] and out["total"] == 3
    out = call(s, "GET", "/assets/staged", {"limit": "5", "sort": "bytes", "dir": "desc"})[1]
    assert [a["key"] for a in out["assets"]] == ["a1", "a2", "a3"]  # no bytes sorts last
    out = call(s, "GET", "/assets/staged", {"limit": "5", "q": "P256"})[1]
    assert [a["key"] for a in out["assets"]] == ["a1"] and out["total"] == 1
    out = call(s, "GET", "/assets/staged", {"limit": "5", "record": "rec-a"})[1]
    assert [a["key"] for a in out["assets"]] == ["a2"]
    status, out, _ = call(s, "GET", "/assets/staged", {"limit": "5", "sort": "nope"})
    assert status == 422 and "sort must be one of" in out["error"]
    assert call(s, "GET", "/assets/staged", {"limit": "500"})[0] == 422


def test_a_raw_asset_is_its_dataset(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/datasets/rec-a", payload={"state": "prepared", "issue": 7})
    call(s, "PUT", "/assets/raw/rec-a", payload={"state": "ready", "bytes": 10})
    out = call(s, "GET", "/assets/raw", {"limit": "5"})[1]
    assert [(a["record"], a["issue"]) for a in out["assets"]] == [("rec-a", 7)] and out["total"] == 1


def test_locks_paged(tmp_path):
    locks = LockDir(tmp_path / "locks")
    for record in ("rec-b", "rec-a", "rec-c"):
        assert locks.take("raw", record, f"job-{record}", 900)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), locks=locks)
    out = call(s, "GET", "/locks", {"limit": "2", "sort": "key"})[1]
    assert [h["key"] for h in out["locks"]] == ["rec-a", "rec-b"] and out["total"] == 3
    assert call(svc(tmp_path / "other"), "GET", "/locks", {"limit": "2"})[1] == {"locks": [], "total": 0}


def catalog_service(tmp_path):
    """A service with the fixture catalog synced at REF_A."""
    s = svc(tmp_path)
    records = [{"path": p, "record": r} for p, r in load_records(FIXTURE_CATALOG)]
    assert call(s, "PUT", f"/records/{REF_A}", payload={"records": records})[0] == 200
    return s


HEMIBRAIN = "janelia-hemibrain-v1-2-fib-sem-connectome"


def test_catalog_datasets_joins_each_record_with_what_the_registry_knows(tmp_path):
    assert call(svc(tmp_path / "empty"), "GET", "/catalog/datasets")[:2] == (404, {"error": "no catalog synced"})
    s = catalog_service(tmp_path)
    call(s, "PUT", f"/datasets/{U937}", payload={"state": "prepared", "issue": 9, "data_stage": "done", "crops": 16})
    call(s, "PUT", f"/assets/raw/{U937}", payload={"state": "ready", "bytes": 10})
    call(s, "PUT", "/assets/staged/p256/a1", payload={"state": "ready", "record": U937, "bytes": 5})
    call(s, "PUT", "/assets/staged/p256/a2", payload={"state": "evicted", "record": U937})
    status, out, _ = call(s, "GET", "/catalog/datasets", {"sort": "dataset"})
    assert status == 200 and out["total"] == 6 and out["ref"] == REF_A
    assert [row["id"] for row in out["rows"]][:2] == [
        "empiar-11037-cem-mitolab-mitochondria",
        "github-bcss-breast-cancer-semantic-segmentation",
    ]
    row = next(row for row in out["rows"] if row["id"] == U937)
    assert (row["state"], row["stage"], row["issue"], row["crops"]) == ("prepared", "done", 9, 16)
    assert (row["dims"], row["modality"], row["licence"], row["bytes"]) == ("2D", "confocal", "CC-BY-4.0", 1153470226)
    assert row["title"].startswith("Dataset of confocal images") and row["in_catalog"] is True
    assert row["raw"] == {"state": "ready", "bytes": 10} and row["staged"] == 1 and row["quarantined"] is False
    other = next(row for row in out["rows"] if row["id"] == HEMIBRAIN)
    assert (other["state"], other["raw"], other["staged"], other["issue"]) == ("unprepared", None, 0, None)
    assert out["facets"]["state"] == {"prepared": 1, "unprepared": 5} and out["facets"]["dims"] == {"2D": 5, "3D": 1}
    assert out["facets"]["modality"]["TEM"] == 2 and out["facets"]["licence"]["CC0-1.0"] == 2

    def found(**query):
        return [row["id"] for row in call(s, "GET", "/catalog/datasets", {"sort": "dataset", **query})[1]["rows"]]

    assert found(state="prepared") == [U937] and found(dims="3D") == [HEMIBRAIN]
    assert found(modality="TEM") == [ONE_CH, "synthetic-em-2d-dup"] and len(found(licence="CC0-1.0")) == 2
    assert found(q="HEMIBRAIN") == [HEMIBRAIN] and found(q="confocal") == [U937]  # the id, the title, the modality
    assert found(sort="bytes", dir="desc")[0] == HEMIBRAIN and found(limit="2", offset="4") == found()[4:]
    assert found(state="prepared", q="nothing") == []
    assert call(s, "GET", "/catalog/datasets", {"sort": "nope"})[0] == 422


def test_catalog_datasets_lists_a_dataset_whose_record_left_the_catalog(tmp_path):
    s = catalog_service(tmp_path)
    call(s, "PUT", "/datasets/rec-gone", payload={"state": "needs-human", "issue": 3})
    out = call(s, "GET", "/catalog/datasets", {"q": "rec-gone"})[1]
    assert [(row["id"], row["state"], row["in_catalog"], row["title"]) for row in out["rows"]] == [
        ("rec-gone", "needs-human", False, "")
    ]
    assert call(s, "GET", "/catalog/datasets")[1]["total"] == 7


def test_catalog_datasets_of_a_request(tmp_path):
    s = catalog_service(tmp_path)
    text = yaml.safe_dump(
        {
            "schema_version": 1,
            "kind": "preparation",
            "catalog": {"repo": "o/c", "ref": REF_A},
            "records": [U937, ONE_CH],
        }
    )
    number = s.ledger.create_subject("prep", None, data={"spec": text})["number"]
    out = call(s, "GET", "/catalog/datasets", {"request": str(number), "sort": "dataset"})[1]
    assert [row["id"] for row in out["rows"]] == [ONE_CH, U937] and out["total"] == 2
    assert call(s, "GET", "/catalog/datasets", {"request": "5"})[0] == 404
    assert call(s, "GET", "/catalog/datasets", {"request": "x"})[0] == 422


def test_one_dataset_with_everything_known_about_it(tmp_path):
    s = catalog_service(tmp_path)
    path = f"/catalog/datasets/{U937}"
    status, out, _ = call(s, "GET", path)
    assert status == 200 and out["row"]["state"] == "unprepared" and out["record"]["id"] == U937
    assert out["path"].endswith(".yaml") and out["ref"] == REF_A
    assert (out["dataset"], out["raw_asset"], out["staged_assets"], out["requests"], out["subject"]) == (
        None, None, [], [], None
    )  # fmt: skip
    call(s, "PUT", f"/datasets/{U937}", payload={"state": "prepared", "issue": 9, "binding_hash": "b" * 64})
    call(s, "PUT", f"/assets/raw/{U937}", payload={"state": "ready", "bytes": 10})
    call(s, "PUT", "/assets/staged/a1", payload={"state": "ready", "record": U937})
    call(s, "PUT", "/buffers/i5-e6", payload=CFG)
    call(s, "PUT", f"/buffers/i5-e6/datasets/{U937}", payload={"status": "servable"})
    text = yaml.safe_dump(
        {"schema_version": 1, "kind": "preparation", "catalog": {"repo": "o/c", "ref": REF_A}, "records": [U937]}
    )
    request = s.ledger.create_subject("prep", None, data={"spec": text})
    call(s, "PUT", "/subjects/dataset/9", payload={"key": U937, "stage": "review"})
    out = call(s, "GET", path)[1]
    assert out["dataset"]["binding_hash"] == "b" * 64 and out["raw_asset"]["data"]["bytes"] == 10
    assert [asset["key"] for asset in out["staged_assets"]] == ["a1"]
    held = call(s, "GET", "/buffers", {"limit": "5", "record": U937})[1]["buffers"]  # the buffers whose pool holds it
    assert [(b["id"], b["spec"], b["board"]["status"]) for b in held] == [("i5-e6", None, "servable")]
    assert [(r["key"], r["number"]) for r in out["requests"]] == [(request["key"], request["number"])]
    assert out["subject"]["stage"] == "review" and "approve" in out["subject"]["actions"]
    assert call(s, "GET", "/catalog/datasets/no-such-record")[0] == 404
    call(s, "PUT", "/datasets/rec-gone", payload={"state": "needs-human"})
    gone = call(s, "GET", "/catalog/datasets/rec-gone")[1]
    assert gone["record"] is None and gone["row"]["in_catalog"] is False and gone["dataset"]["state"] == "needs-human"


def test_decisions_are_paged_and_name_their_dataset(tmp_path):
    from mia_core.judge.policy import new_assessment

    s = catalog_service(tmp_path)
    call(s, "PUT", f"/datasets/{U937}", payload={"state": "prepared", "issue": 7})
    call(s, "PUT", "/subjects/dataset/31", payload={"key": ONE_CH})
    call(s, "PUT", "/assets/staged/p256/abc123", payload={"state": "ready", "record": HEMIBRAIN})
    rows = [
        new_assessment(
            gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s", verdict="approve", by="judge", at=10.0
        ),
        new_assessment(
            gate="crops", subject_kind="crop_set", subject_ref="i31-e2:s", verdict="reject", by="human:a", at=11.0
        ),
        new_assessment(
            gate="staged", subject_kind="artifact", subject_ref="abc123", verdict="reject", by="human:a", at=12.0
        ),
        new_assessment(
            gate="code", subject_kind="pr_head", subject_ref="f" * 40, verdict="approve", by="human:a", at=13.0
        ),
    ]
    for row in rows:
        s.ledger.put_assessment(row)
    status, out, _ = call(s, "GET", "/decisions")
    assert status == 200 and out["total"] == 4
    assert [row.get("record") for row in out["assessments"]] == [None, HEMIBRAIN, ONE_CH, U937]  # newest first
    later = new_assessment(
        gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s", verdict="reject", by="human:a", at=14.0
    )
    s.ledger.put_assessment(later)
    marked = call(s, "GET", "/decisions", {"record": U937})[1]["assessments"]
    assert [(row["verdict"], row.get("overruled")) for row in marked] == [("reject", None), ("approve", True)]
    with s.ledger._transaction() as db:
        db.execute("DELETE FROM assessments WHERE id = ?", (later["id"],))
    out = call(s, "GET", "/decisions", {"limit": "2", "offset": "2"})[1]
    assert [row["subject"]["ref"] for row in out["assessments"]] == ["i31-e2:s", "i7-e11:s"] and out["total"] == 4
    out = call(s, "GET", "/decisions", {"record": U937})[1]
    assert [row["subject"]["ref"] for row in out["assessments"]] == ["i7-e11:s"] and out["total"] == 1
    assert call(s, "GET", "/decisions", {"record": HEMIBRAIN, "gate": "crops"})[1]["total"] == 0
    assert call(s, "GET", "/decisions", {"limit": "500"})[0] == 422


def test_plan_paged_answers_a_page_and_the_numbers_over_the_whole_plan(tmp_path):
    s = catalog_service(tmp_path)
    call(s, "PUT", f"/datasets/{U937}", payload={"state": "prepared", "issue": 9})
    call(s, "PUT", f"/assets/raw/{U937}", payload={"state": "ready", "bytes": 10})
    text = re.sub(r"ref: [0-9a-f]{40}", f"ref: {REF_A}", SPEC_2D_TEXT)
    whole = call(s, "POST", "/plan", payload={"spec": text})[1]
    assert "summary" not in whole and "total" not in whole  # without `page`: the plan as it always was
    status, out, _ = call(s, "POST", "/plan", payload={"spec": text, "page": {"limit": 2, "sort": "dataset"}})
    assert status == 200 and out["total"] == len(whole["rows"]) and len(out["rows"]) == 2
    assert [row["id"] for row in out["rows"]] == sorted(row["id"] for row in whole["rows"])[:2]
    assert (
        out["n_seen"] == whole["n_seen"] and out["excluded"] == whole["excluded"] and out["errors"] == whole["errors"]
    )
    fitting = [row for row in whole["rows"] if row["fit"] and row["fit"]["ok"]]
    pool = [row for row in fitting if row["admitted"]]
    summary = out["summary"]
    assert (summary["fitting"], summary["pool"]) == (len(fitting), len(pool)) and summary["pool"] == 1
    assert summary["bytes_fitting"] == sum(row["bytes"] or 0 for row in fitting)
    assert summary["to_prepare"] == sorted(row["id"] for row in fitting if row["prepared"] != "prepared")
    assert sum(step["value"] for step in summary["funnel"]) == whole["n_seen"]  # every record leaves or is in the pool
    row = next(r for r in call(s, "POST", "/plan", payload={"spec": text, "page": {"q": "u937"}})[1]["rows"])
    assert (row["id"], row["issue"], row["raw"], row["in_pool"]) == (U937, 9, {"state": "ready", "bytes": 10}, True)
    assert row["title"].startswith("Dataset of confocal") and row["licence"] == "CC-BY-4.0"
    only = call(s, "POST", "/plan", payload={"spec": text, "page": {"prepared_only": True}})[1]
    assert [r["id"] for r in only["rows"]] == [U937] and only["summary"]["fitting"] == 1
    assert call(s, "POST", "/plan", payload={"spec": text, "page": {"sort": "nope"}})[0] == 422
    assert call(s, "POST", "/plan", payload={"spec": text, "page": {"limit": 500}})[0] == 422
    assert call(s, "POST", "/plan", payload={"spec": text, "page": {"oops": 1}})[0] == 422


def test_a_kept_plan_is_rebuilt_when_a_dataset_changes_state(tmp_path):
    s = catalog_service(tmp_path)
    text = re.sub(r"ref: [0-9a-f]{40}", f"ref: {REF_A}", SPEC_2D_TEXT)
    first = call(s, "POST", "/plan", payload={"spec": text, "page": {}})[1]["summary"]
    assert first["pool"] == 0 and first["to_prepare"]
    again = call(s, "POST", "/plan", payload={"spec": text, "page": {"offset": 1}})[1]["summary"]
    assert again == first  # the same plan, another page
    call(s, "PUT", f"/datasets/{first['to_prepare'][0]}", payload={"state": "prepared"})
    after = call(s, "POST", "/plan", payload={"spec": text, "page": {}})[1]["summary"]
    assert after["pool"] == 1 and first["to_prepare"][0] not in after["to_prepare"]
    assert call(s, "POST", "/plan", payload={"spec": text.replace(REF_A, "c" * 40), "page": {}})[0] == 409  # not synced


def test_subjects_and_needs_paged(tmp_path):
    s = _subjects(tmp_path)
    for number, key, stage in ((1, "rec-b", "review"), (2, "rec-a", "testing"), (3, "rec-c", "needs-human")):
        call(s, "PUT", f"/subjects/dataset/{number}", payload={"key": key, "stage": stage})
    whole = call(s, "GET", "/subjects/dataset")[1]
    assert len(whole["subjects"]) == 3 and "total" not in whole
    out = call(s, "GET", "/subjects/dataset", {"limit": "2", "sort": "key"})[1]
    assert [row["key"] for row in out["subjects"]] == ["rec-a", "rec-b"] and out["total"] == 3
    assert [row["key"] for row in call(s, "GET", "/subjects/dataset", {"limit": "5", "q": "stuck"})[1]["subjects"]] == [
        "rec-c"
    ]
    out = call(s, "GET", "/needs", {"limit": "1", "sort": "key"})[1]
    assert [row["key"] for row in out["needs"]] == ["rec-b"] and out["total"] == 2 and out["failed"] == []
    assert "total" not in call(s, "GET", "/needs")[1]


def test_buffers_paged_name_their_spec_and_which_round_is_newest(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/subjects/buffer/5", payload={"key": "alpha"})
    for buffer in ("i5-e6", "i5-e9", "i7-e1"):
        call(s, "PUT", f"/buffers/{buffer}", payload=CFG)
    call(s, "PUT", "/buffers/i5-e6/datasets/rec-a", payload={"status": "servable"})
    whole = call(s, "GET", "/buffers")[1]
    assert "total" not in whole and "spec" not in whole["buffers"][0]
    out = call(s, "GET", "/buffers", {"limit": "5"})[1]
    assert out["total"] == 3  # the newest spec first, its newest round first
    assert [(b["id"], b["spec"], b["superseded_by"]) for b in out["buffers"]] == [
        ("i7-e1", None, None), ("i5-e9", "alpha", None), ("i5-e6", "alpha", "i5-e9")
    ]  # fmt: skip
    assert out["buffers"][0]["config"]["spec_name"] == "s"  # no spec page to link: the name it ran under is text
    assert [b["id"] for b in call(s, "GET", "/buffers", {"limit": "5", "spec": "5"})[1]["buffers"]] == [
        "i5-e9",
        "i5-e6",
    ]
    held = call(s, "GET", "/buffers", {"limit": "5", "record": "rec-a"})[1]
    assert [(b["id"], b["board"]["status"]) for b in held["buffers"]] == [("i5-e6", "servable")] and held["total"] == 1
    assert [b["id"] for b in call(s, "GET", "/buffers", {"limit": "5", "sort": "buffer"})[1]["buffers"]][0] == "i5-e6"
    assert call(s, "GET", "/buffers", {"limit": "5", "spec": "x"})[0] == 422


def test_a_buffers_events_pool_volumes_and_consumers_paged(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    for kind in ("created", "sourcing", "approved"):
        call(s, "POST", "/buffers/b/events", payload={"kind": kind})
    assert [e["kind"] for e in call(s, "GET", "/buffers/b/events")[1]["events"]] == ["created", "sourcing", "approved"]
    out = call(s, "GET", "/buffers/b/events", {"limit": "2"})[1]
    assert [e["kind"] for e in out["events"]] == ["approved", "sourcing"] and out["total"] == 3  # newest first
    call(s, "PUT", "/datasets/rec", payload={"state": "prepared", "issue": 4, "folder": "f", "previews": ["c0"]})
    call(s, "PUT", "/buffers/b/datasets/rec", payload={"status": "servable"})
    call(s, "PUT", "/buffers/b/datasets/other", payload={"status": "failed", "reason": "no data"})
    assert set(call(s, "GET", "/buffers/b/datasets")[1]["datasets"][0]) == {
        "record",
        "status",
        "reason",
        "job",
        "updated_at",
    }
    out = call(s, "GET", "/buffers/b/datasets", {"limit": "5", "sort": "dataset"})[1]
    assert [(row["record"], row["issue"], row["folder"]) for row in out["datasets"]] == [
        ("other", None, None),
        ("rec", 4, "f"),
    ]
    assert out["total"] == 2 and out["datasets"][1]["previews"] == ["c0"]
    assert [
        row["record"] for row in call(s, "GET", "/buffers/b/datasets", {"limit": "5", "q": "no data"})[1]["datasets"]
    ] == ["other"]
    assert call(s, "PUT", "/buffers/b/volumes/va", payload=body("va"))[0] == 200
    assert call(s, "PUT", "/buffers/b/volumes/vb", payload=body("vb"))[0] == 200
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    call(s, "GET", "/buffers/b/next", {"n": "3", "consumer": cid})
    out = call(s, "GET", "/buffers/b/volumes", {"limit": "1", "sort": "draws", "dir": "desc"})[1]
    assert out["total"] == 2 and out["volumes"][0]["draws"] >= 2 and out["volumes"][0]["missing"] is False
    assert "draws" not in call(s, "GET", "/buffers/b/volumes")[1]["volumes"][0]
    out = call(s, "GET", "/buffers/b/consumers", {"limit": "5"})[1]
    assert [(c["name"], c["split"], c["stale"]) for c in out["consumers"]] == [("n", "train", False)] and out[
        "total"
    ] == 1
    assert call(s, "GET", "/buffers/zz/consumers")[0] == 404


def test_usage_days_paged(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    with s.ledger._transaction() as db:
        db.executemany(
            "INSERT INTO usage VALUES ('b', ?, ?, 1, 0)", [(f"2026-01-0{day}", day * 10) for day in (1, 2, 3)]
        )
    whole = call(s, "GET", "/usage/b")[1]
    assert [day["day"] for day in whole["days"]] == ["2026-01-01", "2026-01-02", "2026-01-03"] and "total" not in whole
    out = call(s, "GET", "/usage/b", {"limit": "2"})[1]
    assert [day["day"] for day in out["days"]] == ["2026-01-03", "2026-01-02"] and out["total"] == 3  # newest first
    assert [d["draws"] for d in call(s, "GET", "/usage/b", {"limit": "1", "sort": "draws"})[1]["days"]] == [10]


def test_a_dataset_started_from_the_page_names_the_issue_the_mirror_opened(tmp_path):
    """The worker writes the workflow's number into the dataset row's `issue`; for a dataset started from the page
    that number is the subject's, not a GitHub issue's. The GitHub issue is the subject's."""
    s = catalog_service(tmp_path)
    number = s.ledger.create_subject("dataset", U937)["number"]
    call(s, "PUT", f"/datasets/{U937}", payload={"state": "in-progress", "issue": number})
    call(s, "PUT", f"/assets/raw/{U937}", payload={"state": "ready", "bytes": 10})
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", f"/buffers/b/datasets/{U937}", payload={"status": "fetching"})
    text = re.sub(r"ref: [0-9a-f]{40}", f"ref: {REF_A}", SPEC_2D_TEXT)

    def issues():
        return (
            call(s, "GET", "/catalog/datasets", {"q": U937})[1]["rows"][0]["issue"],
            call(s, "GET", f"/catalog/datasets/{U937}")[1]["row"]["issue"],
            call(s, "GET", "/assets/raw", {"limit": "5"})[1]["assets"][0]["issue"],
            call(s, "GET", "/buffers/b/datasets", {"limit": "5"})[1]["datasets"][0]["issue"],
            call(s, "POST", "/plan", payload={"spec": text, "page": {"q": U937}})[1]["rows"][0]["issue"],
        )

    assert issues() == (None, None, None, None, None)  # the mirror has opened none yet: no link, never the number
    s.ledger.set_subject_issue("dataset", number, 301)
    assert issues() == (301, 301, 301, 301, 301)
    call(
        s, "PUT", f"/datasets/{ONE_CH}", payload={"state": "prepared", "issue": 12}
    )  # its row names an issue; it has no subject
    assert call(s, "GET", "/catalog/datasets", {"q": ONE_CH})[1]["rows"][0]["issue"] == 12


# --- POST /hosts/missing: which hosts a run needs and the csub policy lacks ---


@pytest.fixture
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))


def _checking(tmp_path):
    """A service whose policy allows zenodo.org, with a catalog holding rec-a (data.example.org) and rec-b."""
    base = tmp_path / "base" / "broker.toml"
    base.parent.mkdir()
    base.write_text('[broker]\nallowed_hosts = ["zenodo.org"]\n')
    policy_dir = tmp_path / ".mia" / "policy"
    generate_policy(base, policy_dir)
    check = HostCheck(
        base_policy=base,
        policy_dir=policy_dir,
        network=NetworkCfg(package_hosts=[]),
        storage_root=tmp_path / "storage",
        rounds_dir=tmp_path / "rounds",
        own_hosts=("buffer-api.internal.example", "temporal.internal.example"),
    )
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t", hosts=check)
    urls = {"rec-a": "https://data.example.org/a.zip", "rec-b": "https://zenodo.org/b.zip"}
    records = [{"path": f"{n}.yaml", "record": {"id": n, "data": {"download_url": u}}} for n, u in urls.items()]
    assert worker(s, "PUT", f"/records/{REF_A}", {"records": records})[0] == 200
    return s


def _missing(s, payload):
    return act(s, "/hosts/missing", payload)


def test_hosts_missing_lists_what_the_named_records_need_and_the_policy_lacks(tmp_path, _home):
    s = _checking(tmp_path)
    status, out, _ = _missing(s, {"records": ["rec-a", "rec-b"]})
    assert (status, out) == (200, {"hosts": [{"host": "data.example.org", "records": ["rec-a"]}]})
    assert _missing(s, {"records": ["rec-b"]})[:2] == (200, {"hosts": []})
    assert _missing(s, {"records": ["nope"]})[0] == 404
    assert _missing(s, {})[0] == 422
    assert _missing(s, {"records": ["rec-a"], "prep": 1})[0] == 422


def test_hosts_missing_for_a_request_reads_its_records(tmp_path, _home):
    s = _checking(tmp_path)
    number = s.ledger.create_subject("prep", None, data={"spec": PREP_TEXT})["number"]
    status, out, _ = _missing(s, {"prep": number})
    assert (status, out) == (200, {"hosts": [{"host": "data.example.org", "records": ["rec-a"]}]})
    assert _missing(s, {"prep": 5})[0] == 404


def test_hosts_missing_for_a_buffer_reads_the_rounds_plan_and_its_bindings(tmp_path, _home):
    s = _checking(tmp_path)
    number = s.ledger.create_subject("buffer", "spec-x", data={"spec": SPEC_TEXT})["number"]
    assert _missing(s, {"spec": "spec-x"})[:2] == (409, {"error": "buffer 'spec-x' has no plan to read"})
    s.ledger.append_entry("buffer", number, body="plan", marker="m1", round_id="i7-e1", stage="plan-review")
    plan = tmp_path / "rounds" / "i7-e1" / "plan.json"
    plan.parent.mkdir(parents=True)
    plan.write_text(json.dumps({"rows": [{"id": "rec-a", "admitted": True, "fit": {"ok": True}}]}))
    s.ledger.put_dataset("rec-a", {"folder": "rec-a"})
    folder = tmp_path / "storage" / "rec-a"
    folder.mkdir(parents=True)
    (folder / "binding.yaml").write_text(yaml.safe_dump({"raw": [{"urls": ["https://data.example.org/x"]}]}))
    status, out, _ = _missing(s, {"spec": "spec-x"})
    assert (status, out) == (200, {"hosts": [{"host": "data.example.org", "records": ["rec-a"]}]})
    plan.unlink()
    assert _missing(s, {"spec": "spec-x"})[0] == 409
    assert _missing(s, {"spec": "other"})[0] == 404


def test_hosts_missing_for_a_request_skips_a_record_no_synced_catalog_holds(tmp_path, _home):
    s = _checking(tmp_path)
    text = PREP_TEXT.replace("- rec-a", "- rec-a\n- rec-gone")
    number = s.ledger.create_subject("prep", None, data={"spec": text})["number"]
    status, out, _ = _missing(s, {"prep": number})
    assert (status, out) == (200, {"hosts": [{"host": "data.example.org", "records": ["rec-a"]}]})
    assert _missing(s, {"records": ["rec-a", "rec-gone"]})[0] == 404
    s.dispatcher = Gateway()
    go = {"action": "go", "approve_hosts": ["data.example.org"]}
    assert act(s, f"/prep/{number}/actions", go)[0] == 202
    assert act(s, "/prep", {"spec": text, "approve_hosts": ["data.example.org"]})[0] == 202


def test_hosts_missing_for_a_buffer_reads_the_newest_plan_review_round(tmp_path, _home):
    s = _checking(tmp_path)
    number, plan = _at_plan_review(s, tmp_path)
    s.ledger.append_entry("buffer", number, body="plan", marker="m2", round_id="i7-e2", stage="plan-review")
    newer = tmp_path / "rounds" / "i7-e2" / "plan.json"
    newer.parent.mkdir(parents=True)
    newer.write_text(json.dumps({"rows": [{"id": "rec-b", "admitted": True, "fit": {"ok": True}}]}))
    s.ledger.put_dataset("rec-b", {"folder": "rec-b"})
    (tmp_path / "storage" / "rec-b").mkdir()
    binding = {"raw": [{"urls": ["https://zenodo.org/b.zip"]}]}
    (tmp_path / "storage" / "rec-b" / "binding.yaml").write_text(yaml.safe_dump(binding))
    s.hosts.base_policy.write_text("[broker]\nallowed_hosts = []\n")
    generate_policy(s.hosts.base_policy, s.hosts.policy_dir)
    status, out, _ = _missing(s, {"spec": SPEC_NAME})
    assert (status, out) == (200, {"hosts": [{"host": "zenodo.org", "records": ["rec-b"]}]})


@pytest.mark.parametrize("policy", [None, "not = = toml"])
def test_hosts_missing_with_an_unreadable_generated_policy_is_503(tmp_path, _home, policy):
    s = _checking(tmp_path)
    generated = s.hosts.policy_dir / "broker.toml"
    generated.unlink()
    if policy is not None:
        generated.write_text(policy)
    status, out, _ = _missing(s, {"records": ["rec-a"]})
    assert status == 503 and "mia up" in out["error"] and str(s.hosts.policy_dir) not in out["error"]


def test_hosts_missing_without_a_policy_is_503(tmp_path):
    s, _ = _acting(tmp_path)
    assert _missing(s, {"records": ["rec-a"]})[0] == 503


def test_hosts_missing_writes_nothing(tmp_path, _home):
    s = _checking(tmp_path)
    _missing(s, {"records": ["rec-a"]})
    assert not (s.hosts.policy_dir / "approved-hosts.jsonl").exists()


def test_serve_refuses_a_policy_dir_it_must_not_write(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    token = tmp_path / "token"
    token.write_text("t\n")
    token.chmod(0o600)
    inside = tmp_path / "storage" / "csub"
    cfg = make_cfg(tmp_path, service={"token_file": str(token)}, csub={"policy_dir": str(inside)})
    with pytest.raises(SystemExit, match=re.escape(str(inside))):
        api_module.serve(cfg)
    assert not inside.exists()


# --- approve_hosts: the hosts a person ticked on an action that starts jobs ---


def _starting(tmp_path):
    """`_checking`'s service with a gateway stand-in to send to."""
    s = _checking(tmp_path)
    gateway = Gateway()
    s.dispatcher = gateway
    return s, gateway


def _approvals(s):
    path = s.hosts.policy_dir / "approved-hosts.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _bodies(s, kind, number):
    return [entry["body"] for entry in s.ledger.timeline(kind, number)]


def test_a_go_with_approve_hosts_approves_them_then_starts_the_round(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    status, action, _ = act(s, "/datasets/rec-a/actions", {"action": "go", "approve_hosts": ["data.example.org"]})
    assert status == 202
    number = action["number"]
    assert _bodies(s, "dataset", number) == [
        "**Hosts approved:** `data.example.org` by `page`",
        "**Action:** `go` by `page`",
    ]
    (line,) = _approvals(s)
    assert (line["host"], line["by"], line["kind"], line["number"]) == ("data.example.org", "page", "dataset", number)
    assert _missing(s, {"records": ["rec-a"]})[:2] == (200, {"hosts": []})
    assert len(gateway.sent) == 1


@pytest.mark.parametrize(
    "host", ["Data.4tu.nl", "data.4tu.nl.", "*.4tu.nl", "data.4tu.nl:443", "github.com", "other.example.org"]
)
def test_a_host_the_run_does_not_name_is_refused_and_nothing_happens(tmp_path, _home, host):
    s, gateway = _starting(tmp_path)
    status, out, _ = act(s, "/datasets/rec-a/actions", {"action": "go", "approve_hosts": [host]})
    assert status == 422 and host in out["error"]
    assert _approvals(s) == [] and s.ledger.subject("dataset", "rec-a") is None and gateway.sent == []


def test_approve_hosts_on_an_action_that_starts_nothing_is_422(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    subject = s.ledger.create_subject("dataset", "rec-a")
    _stage(s, "dataset", subject["number"], "testing")
    status, out, _ = act(s, "/datasets/rec-a/actions", {"action": "cancel", "approve_hosts": ["data.example.org"]})
    assert (status, out["error"]) == (422, "approve_hosts goes with an action that starts jobs")
    assert _approvals(s) == [] and gateway.sent == []


def test_a_refused_action_approves_nothing(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    subject = s.ledger.create_subject("dataset", "rec-a")
    _stage(s, "dataset", subject["number"], "testing")
    assert act(s, "/datasets/rec-a/actions", {"action": "go", "approve_hosts": ["data.example.org"]})[0] == 409
    assert _approvals(s) == [] and gateway.sent == []


def test_a_policy_that_cannot_be_generated_is_503_and_nothing_is_sent(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    before = (s.hosts.policy_dir / "broker.toml").read_bytes()
    s.hosts.base_policy.write_text("not toml")
    status, out, _ = act(s, "/datasets/rec-a/actions", {"action": "go", "approve_hosts": ["data.example.org"]})
    assert status == 503 and out["error"]
    assert (s.hosts.policy_dir / "broker.toml").read_bytes() == before and gateway.sent == []
    assert _approvals(s) == []


def test_a_repeat_of_the_key_adds_no_line_and_no_entry(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    body = {"action": "go", "key": "k1", "approve_hosts": ["data.example.org"]}
    first = act(s, "/datasets/rec-a/actions", body)[1]
    assert act(s, "/datasets/rec-a/actions", body)[1]["id"] == first["id"]
    assert len(_approvals(s)) == 1 and len(gateway.sent) == 1
    assert len(_bodies(s, "dataset", first["number"])) == 2


def test_filing_a_request_approves_its_hosts(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    status, action, _ = act(s, "/prep", {"spec": PREP_TEXT, "approve_hosts": ["data.example.org"]})
    assert status == 202
    assert _bodies(s, "prep", action["number"])[0] == "**Hosts approved:** `data.example.org` by `page`"
    assert _approvals(s)[0]["kind"] == "prep" and len(gateway.sent) == 1
    status, out, _ = act(s, "/prep", {"spec": PREP_TEXT, "approve_hosts": ["other.example.org"]})
    assert status == 422 and "other.example.org" in out["error"]
    assert len(s.ledger.find_subjects(kind="prep")) == 1


def _at_plan_review(s, tmp_path):
    """A buffer subject at plan-review whose plan pools rec-a, bound to data.example.org: (number, plan path)."""
    number = s.ledger.create_subject("buffer", SPEC_NAME, data={"spec": SPEC_TEXT})["number"]
    s.ledger.append_entry("buffer", number, body="plan", marker="m1", round_id="i7-e1", stage="plan-review")
    plan = tmp_path / "rounds" / "i7-e1" / "plan.json"
    plan.parent.mkdir(parents=True)
    plan.write_text(json.dumps({"rows": [{"id": "rec-a", "admitted": True, "fit": {"ok": True}}]}))
    s.ledger.put_dataset("rec-a", {"folder": "rec-a"})
    folder = tmp_path / "storage" / "rec-a"
    folder.mkdir(parents=True)
    (folder / "binding.yaml").write_text(yaml.safe_dump({"raw": [{"urls": ["https://data.example.org/x"]}]}))
    return number, plan


def test_running_a_buffers_plan_approves_its_hosts(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    number, _ = _at_plan_review(s, tmp_path)
    revision = s.ledger.subject("buffer", SPEC_NAME)["revision"]
    body = {"action": "approve", "revision": revision, "approve_hosts": ["data.example.org"]}
    assert act(s, SPEC_ACTIONS, {**body, "approve_hosts": ["other.example.org"]})[0] == 422
    assert act(s, SPEC_ACTIONS, body)[0] == 202
    assert _bodies(s, "buffer", number)[1] == "**Hosts approved:** `data.example.org` by `page`"
    assert _approvals(s)[0]["kind"] == "buffer" and len(gateway.sent) == 1


def test_a_repeat_of_a_sent_approve_returns_the_first_action_though_the_plan_is_gone(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    number, plan = _at_plan_review(s, tmp_path)
    revision = s.ledger.subject("buffer", SPEC_NAME)["revision"]
    body = {"action": "approve", "revision": revision, "key": "k1", "approve_hosts": ["data.example.org"]}
    first = act(s, SPEC_ACTIONS, body)[1]
    plan.unlink()
    status, again, _ = act(s, SPEC_ACTIONS, body)
    assert (status, again["id"]) == (202, first["id"])
    assert len(_approvals(s)) == 1 and len(_bodies(s, "buffer", number)) == 3 and len(gateway.sent) == 1


def test_a_repeat_of_a_sent_approve_after_the_stage_moved_returns_the_first_action(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    number, _ = _at_plan_review(s, tmp_path)
    revision = s.ledger.subject("buffer", SPEC_NAME)["revision"]
    body = {"action": "approve", "revision": revision, "key": "k1", "approve_hosts": ["data.example.org"]}
    first = act(s, SPEC_ACTIONS, body)[1]
    _stage(s, "buffer", number, "sourcing")
    entries = len(_bodies(s, "buffer", number))
    status, again, _ = act(s, SPEC_ACTIONS, body)
    assert (status, again["id"]) == (202, first["id"])
    assert len(_approvals(s)) == 1 and len(_bodies(s, "buffer", number)) == entries and len(gateway.sent) == 1


def test_an_edited_spec_sent_with_approve_hosts_is_refused_and_not_stored(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    s.ledger.create_subject("buffer", SPEC_NAME, data={"spec": SPEC_TEXT})
    status, out, _ = act(
        s, SPEC_ACTIONS, {"action": "go", "spec": SPEC_TEXT + "\n# edited\n", "approve_hosts": ["data.example.org"]}
    )
    assert status == 422 and "starts jobs" in out["error"]
    assert s.ledger.subject("buffer", SPEC_NAME)["data"]["spec"] == SPEC_TEXT and gateway.sent == []


def test_a_reused_key_approves_nothing(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    assert act(s, "/datasets/rec-b/actions", {"action": "go", "key": "k1"})[0] == 202
    sent = len(gateway.sent)
    status, out, _ = act(
        s, "/datasets/rec-a/actions", {"action": "go", "key": "k1", "approve_hosts": ["data.example.org"]}
    )
    assert status == 409 and "k1" in out["error"]
    assert _approvals(s) == [] and s.ledger.subject("dataset", "rec-a") is None and len(gateway.sent) == sent
    subject = s.ledger.create_subject("dataset", "rec-a")  # and with a subject there: no approval entry either
    again = act(s, "/datasets/rec-a/actions", {"action": "go", "key": "k1", "approve_hosts": ["data.example.org"]})
    assert again[0] == 409 and _approvals(s) == [] and _bodies(s, "dataset", subject["number"]) == []


def test_a_service_with_no_way_to_temporal_approves_nothing(tmp_path, _home):
    s, _ = _starting(tmp_path)
    s.dispatcher = None
    subject = s.ledger.create_subject("dataset", "rec-a")
    status, _, _ = act(s, "/datasets/rec-a/actions", {"action": "go", "approve_hosts": ["data.example.org"]})
    assert status == 503 and _approvals(s) == [] and _bodies(s, "dataset", subject["number"]) == []


def test_a_buffers_go_with_approve_hosts_is_refused_before_the_spec_is_saved(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    status, out, _ = act(s, "/specs", {"spec": SPEC_TEXT, "action": "go", "approve_hosts": ["data.example.org"]})
    assert status == 422 and "starts jobs" in out["error"]
    assert s.ledger.subject("buffer", SPEC_NAME) is None and _approvals(s) == [] and gateway.sent == []


def test_a_spec_saved_with_approve_hosts_is_refused_and_not_stored(tmp_path, _home):
    s, _ = _starting(tmp_path)
    status, out, _ = act(s, "/specs", {"spec": SPEC_TEXT, "approve_hosts": ["data.example.org"]})
    assert status == 422 and "starts jobs" in out["error"]
    assert s.ledger.subject("buffer", SPEC_NAME) is None and _approvals(s) == []


def test_a_requests_go_approves_its_hosts(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    number = s.ledger.create_subject("prep", None, data={"spec": PREP_TEXT})["number"]
    status, _, _ = act(s, f"/prep/{number}/actions", {"action": "go", "approve_hosts": ["data.example.org"]})
    assert status == 202
    assert _bodies(s, "prep", number)[0] == "**Hosts approved:** `data.example.org` by `page`"
    assert _approvals(s)[0]["kind"] == "prep" and len(gateway.sent) == 1


def test_a_buffers_hosts_are_those_its_bindings_name_that_its_catalog_records_also_name(tmp_path, _home):
    s, gateway = _starting(tmp_path)
    number, plan = _at_plan_review(s, tmp_path)
    rows = [{"id": record, "admitted": True, "fit": {"ok": True}} for record in ("rec-a", "rec-z")]
    plan.write_text(json.dumps({"rows": rows}))
    binding = {"raw": [{"urls": ["https://data.example.org/x", "https://exfil.example.net/y"]}]}
    (tmp_path / "storage" / "rec-a" / "binding.yaml").write_text(yaml.safe_dump(binding))  # rewritten by a job
    s.ledger.put_dataset("rec-z", {"folder": "rec-z"})  # in no synced catalog
    (tmp_path / "storage" / "rec-z").mkdir()
    (tmp_path / "storage" / "rec-z" / "binding.yaml").write_text(yaml.safe_dump(binding))
    assert _missing(s, {"spec": SPEC_NAME})[:2] == (
        200,
        {"hosts": [{"host": "data.example.org", "records": ["rec-a"]}]},
    )
    revision = s.ledger.subject("buffer", SPEC_NAME)["revision"]
    status, out, _ = act(
        s, SPEC_ACTIONS, {"action": "approve", "revision": revision, "approve_hosts": ["exfil.example.net"]}
    )
    assert status == 422 and "exfil.example.net" in out["error"]
    assert _approvals(s) == [] and gateway.sent == [] and len(_bodies(s, "buffer", number)) == 1
    assert (
        act(s, SPEC_ACTIONS, {"action": "approve", "revision": revision, "approve_hosts": ["data.example.org"]})[0]
        == 202
    )


@pytest.mark.parametrize(
    "url, host",
    [
        ("https://buffer-api.internal.example:8443/a.zip", "buffer-api.internal.example"),
        ("http://temporal.internal.example:7233/a.zip", "temporal.internal.example"),
        ("http://10.1.2.3/a.zip", "10.1.2.3"),
        ("https://127.0.0.1/a.zip", "127.0.0.1"),
    ],
)
def test_an_ip_literal_and_the_deployments_own_hosts_are_never_offered_or_approved(tmp_path, _home, url, host):
    s, gateway = _starting(tmp_path)
    record = {"id": "rec-x", "data": {"download_url": [url, "https://x.example.org/a.zip"]}}
    assert worker(s, "PUT", f"/records/{REF_A}", {"records": [{"path": "rec-x.yaml", "record": record}]})[0] == 200
    assert _missing(s, {"records": ["rec-x"]})[:2] == (
        200,
        {"hosts": [{"host": "x.example.org", "records": ["rec-x"]}]},
    )
    status, out, _ = act(s, "/datasets/rec-x/actions", {"action": "go", "approve_hosts": [host]})
    assert status == 422 and host in out["error"]
    assert _approvals(s) == [] and s.ledger.subject("dataset", "rec-x") is None and gateway.sent == []


@pytest.mark.parametrize("host", [*SHORTHAND_IPS, "1.2.3.4.", "2130706433", "buffer-api.internal.example."])
def test_a_host_that_cannot_be_approved_is_never_offered(tmp_path, _home, host):
    s = _checking(tmp_path)
    record = {"id": "rec-x", "data": {"download_url": [f"http://{host}/a.zip", "https://x.example.org/a.zip"]}}
    assert worker(s, "PUT", f"/records/{REF_A}", {"records": [{"path": "rec-x.yaml", "record": record}]})[0] == 200
    assert _missing(s, {"records": ["rec-x"]})[:2] == (
        200,
        {"hosts": [{"host": "x.example.org", "records": ["rec-x"]}]},
    )


def test_two_approvals_at_once_both_land(tmp_path, _home):
    s, _ = _starting(tmp_path)
    s.hosts.base_policy.write_text('[broker]\nallowed_hosts = ["zenodo.org"]\n')
    worker(
        s,
        "PUT",
        f"/records/{REF_A}",
        {
            "records": [
                {"path": f"{n}.yaml", "record": {"id": n, "data": {"download_url": f"https://{n}.example.org/a.zip"}}}
                for n in ("rec-c", "rec-d")
            ]
        },
    )
    results = {}

    def go(name):
        results[name] = act(s, f"/datasets/{name}/actions", {"action": "go", "approve_hosts": [f"{name}.example.org"]})

    threads = [threading.Thread(target=go, args=(name,)) for name in ("rec-c", "rec-d")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert [results[name][0] for name in results] == [202, 202]
    policy = (s.hosts.policy_dir / "broker.toml").read_text()
    assert "rec-c.example.org" in policy and "rec-d.example.org" in policy
