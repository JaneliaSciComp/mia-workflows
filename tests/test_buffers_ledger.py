"""The SQLite ledger: buffers, volumes, consumers, draws and frozen lists."""

import dataclasses
import gzip
import json
import sqlite3
import threading
import time

import pytest

from mia_buffers.ledger import Ledger
from tests.test_buffers_sampler import vol

INFO = {"name": "n", "host": "h", "pid": 1, "rank": 0, "split": "train", "mode": "local"}


def test_buffer_volume_roundtrip_and_upsert(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    assert led.buffer("b") is None
    led.put_buffer("b", {"levels": [1, 2, 4]})
    led.put_volume("b", vol("a"))
    led.put_volume("b", vol("a", weight=0.5))
    assert led.buffer("b") == {"levels": [1, 2, 4]} and [v.weight for v in led.volumes("b")] == [0.5]
    assert led.volumes("b")[0] == vol("a", weight=0.5)
    assert led.volumes("other") == []


def test_volumes_are_parsed_once_per_write(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_volume("b", vol("a"))
    statements: list[str] = []
    led._db.set_trace_callback(statements.append)
    first = led.volumes("b")
    second = led.volumes("b")
    assert first == second == [vol("a")] and first is not second  # a copy: a caller may replace entries
    assert sum("FROM volumes" in statement for statement in statements) == 1  # the second read never hit SQLite
    led.put_volume("b", vol("c"))
    assert [v.id for v in led.volumes("b")] == ["a", "c"]
    led.set_volume_missing("b", "c", True, vol("c").path)
    assert [v.missing for v in led.volumes("b")] == [False, True]
    assert sum("FROM volumes" in statement for statement in statements) == 3


def test_consumers_and_staleness(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    cid = led.add_consumer("b", INFO)
    assert led.touch("b", cid) and not led.touch("b", "nope")
    seen = led.consumers("b", stale_s=300)
    assert seen[0]["stale"] is False and seen[0]["name"] == "n" and seen[0]["consumer_id"] == cid
    assert led.consumers("b", stale_s=-1)[0]["stale"] is True


def test_draws_are_sequential_under_threads(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    cid = led.add_consumer("b", INFO)
    out: list[int] = []

    def work():
        for _ in range(20):
            out.extend(d["seq"] for d in led.record_draws("b", cid, [{"volume": "a"}, {"volume": "a"}]))

    threads = [threading.Thread(target=work) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(out) == list(range(1, 161)) and led.draws_per_volume("b") == {"a": 160}


def test_frozen_lists(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    assert led.frozen("b", "val") == []
    led.extend_frozen("b", "val", [{"volume": "a", "seq": 0}, {"volume": "a", "seq": 1}])
    assert [d["seq"] for d in led.frozen("b", "val")] == [0, 1]


def test_assets_and_refs(tmp_path):
    """The registry: what exists on disk (by kind and key) and which buffers reference it."""
    led = Ledger(tmp_path / "l.sqlite")
    assert led.asset("staged", "/s/p256/u937") is None and led.assets("staged") == []
    led.put_asset("staged", "/s/p256/u937", {"record": "u937", "binding_hash": "b1"})
    led.put_asset("staged", "/s/p256/u937", {"record": "u937", "binding_hash": "b2"})  # upsert
    assert led.asset("staged", "/s/p256/u937") == {"record": "u937", "binding_hash": "b2"}
    for buffer in ("buf-1", "buf-1", "buf-2"):
        led.add_ref(buffer, "staged", "/s/p256/u937")
    led.add_ref("buf-2", "raw", "u937")  # a reference may precede the asset's own row
    assert led.refs("staged", "/s/p256/u937") == ["buf-1", "buf-2"] and led.refs("raw", "u937") == ["buf-2"]
    assert led.assets("staged") == [
        {"key": "/s/p256/u937", "data": {"record": "u937", "binding_hash": "b2"}, "refs": ["buf-1", "buf-2"]}
    ]
    assert led.assets("raw") == [{"key": "u937", "data": None, "refs": ["buf-2"]}]


def test_records_replace_per_ref_and_catalogs_list(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    assert led.put_records("a" * 40, [("datasets/x.yaml", {"id": "x"}), ("datasets/y.yaml", {"id": "y"})]) == 2
    assert led.put_records("a" * 40, [("datasets/z.yaml", {"id": "z", "k": 1})]) == 1
    assert led.records("a" * 40) == [("datasets/z.yaml", {"id": "z", "k": 1})] and led.records("b" * 40) == []
    led.put_records("b" * 40, [])
    assert [c["ref"] for c in led.catalogs()] == ["b" * 40, "a" * 40] and led.latest_catalog() == "b" * 40
    assert led.catalogs()[1]["count"] == 1


def test_dataset_rows_merge(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    assert led.dataset("r") is None
    led.put_dataset("r", {"state": "in-progress", "data_stage": "matching", "issue": 7})
    row = led.put_dataset("r", {"data_stage": "review"})
    assert row["state"] == "in-progress" and row["data_stage"] == "review" and row["issue"] == 7 and row["id"] == "r"
    assert [d["id"] for d in led.datasets("in-progress")] == ["r"] and led.datasets("prepared") == []


def test_sync_datasets_keeps_in_progress_rows(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_dataset("busy", {"state": "in-progress", "issue": 1})
    led.put_dataset("stuck", {"state": "needs-human", "issue": 2})
    led.put_dataset("old", {"state": "prepared", "folder": "f-old"})
    out = led.sync_datasets({"new": {"folder": "f-new", "crops": 3}})
    assert out == {"prepared": 1, "demoted": 1, "revoked_kept": 0}
    assert led.dataset("old") is None and led.dataset("busy")["state"] == "in-progress"
    assert led.dataset("stuck")["state"] == "needs-human" and led.dataset("new")["crops"] == 3


def test_sync_datasets_marks_prepared_only_what_no_later_round_left_open(tmp_path):
    """A folder with saved crops makes its record prepared unless a round after that save is open or stopped."""
    led = Ledger(tmp_path / "l.sqlite")
    for record, state, stage in (
        ("rerun", "in-progress", "matching"),
        ("review", "needs-human", "review"),
        ("stopped", "needs-human", "needs-human"),
        ("unsaved", "needs-human", "done"),  # its round ended done before the folder held a matching manifest
    ):
        led.put_dataset(record, {"state": state, "data_stage": stage, "reason": "why" if record == "unsaved" else None})
    folders = {record: {"state": "prepared", "folder": f"f-{record}", "crops": 4} for record in ("rerun", "review")}
    led.sync_datasets({**folders, "stopped": {"crops": 4}, "unsaved": {"crops": 4}, "found": {"crops": 4}})
    states = {row["id"]: (row["state"], row.get("crops")) for row in led.datasets()}
    assert states == {
        "rerun": ("in-progress", 4),
        "review": ("needs-human", 4),
        "stopped": ("needs-human", 4),
        "unsaved": ("prepared", 4),
        "found": ("prepared", 4),
    }
    assert led.dataset("unsaved")["reason"] is None


def test_put_volume_with_refs_is_atomic(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    led.put_volume("b", vol("a"), refs=[("staged", "art1"), ("raw", "rec")])
    assert led.refs("staged", "art1") == ["b"] and led.refs("raw", "rec") == ["b"]
    with pytest.raises((sqlite3.InterfaceError, sqlite3.ProgrammingError)):
        led.put_volume("b", vol("c"), refs=[("staged", object())])  # unbindable on purpose
    assert [v.id for v in led.volumes("b")] == ["a"]


def test_backup_is_a_consistent_copy(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {"x": 1})
    led.backup(tmp_path / "bk" / "copy.sqlite")
    assert Ledger(tmp_path / "bk" / "copy.sqlite").buffer("b") == {"x": 1}
    assert not (tmp_path / "bk" / "copy.tmp").exists()


def test_put_asset_merges_and_filters(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_asset("raw", "rec", {"state": "fetching", "job": "1", "path": "/netfs/lab/raw/rec"})
    merged = ledger.put_asset("raw", "rec", {"bytes": 10, "scanned_at": 5.0})
    assert merged["state"] == "fetching" and merged["job"] == "1" and merged["bytes"] == 10
    ledger.put_asset("staged", "art1", {"state": "ready", "record": "rec", "bytes": 100})
    ledger.put_asset("staged", "art2", {"state": "evicted", "record": "rec", "bytes": 0})
    ledger.add_ref("b", "staged", "ref-only")
    assert [a["key"] for a in ledger.assets("staged", state="ready")] == ["art1"]
    assert [a["key"] for a in ledger.assets("staged", record="rec")] == ["art1", "art2"]
    assert [a["key"] for a in ledger.assets("staged")] == ["art1", "art2", "ref-only"]
    assert ledger.asset_bytes("staged") == 100 and ledger.asset_bytes("raw") == 10


def test_sync_assets_marks_missing_but_keeps_building_rows(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_asset("staged", "gone", {"state": "ready", "bytes": 5})
    ledger.put_asset("staged", "building", {"state": "staging", "job": "7"})
    ledger.put_asset("staged", "back", {"state": "missing"})
    ledger.put_asset("raw", "rec", {"state": "ready"})
    found = {
        ("staged", "back"): {"path": "/netfs/lab/s/p/rec/back", "bytes": 3, "scanned_at": 9.0},
        ("staged", "new"): {"path": "/netfs/lab/s/p/rec/new", "bytes": 4, "scanned_at": 9.0, "record": "rec"},
    }
    counts = ledger.sync_assets(found, older_than=time.time() + 1, kinds=("staged",))
    by_key = {a["key"]: a["data"] for a in ledger.assets("staged")}
    assert by_key["gone"]["state"] == "missing" and by_key["building"]["state"] == "staging"
    assert by_key["back"]["state"] == "ready" and by_key["new"]["state"] == "ready" and by_key["new"]["record"] == "rec"
    assert counts == {"scanned": 2, "new": 1, "missing": 1, "dropped": 0}
    assert by_key["new"]["last_used"] == 9.0  # first seen: used then
    assert by_key["back"]["last_used"] == 9.0  # known, but without last_used (from before it): as first seen
    ledger.sync_assets({("staged", "new"): {"bytes": 4, "scanned_at": 99.0}}, older_than=time.time() + 1, kinds=())
    assert ledger.asset("staged", "new")["last_used"] == 9.0  # a later scan is not a use
    assert ledger.asset("raw", "rec")["state"] == "ready"  # a kind not scanned is never demoted
    # a row written after the scan started is left alone
    ledger.put_asset("staged", "late", {"state": "ready"})
    ledger.sync_assets({}, older_than=time.time() - 60)
    assert {a["key"]: a["data"]["state"] for a in ledger.assets("staged")}["late"] == "ready"


def test_sync_assets_readies_stateless_and_evicted_rows_and_skips_rows_written_during_the_scan(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_asset("staged", "old", {"binding_hash": "b", "bytes": 1})  # a milestone-B row: no state
    ledger.put_asset("staged", "evicted", {"state": "evicted", "bytes": 0})
    started = time.time() + 1
    found = {("staged", key): {"bytes": 7, "scanned_at": 9.0} for key in ("old", "evicted")}
    assert ledger.sync_assets(found, older_than=started) == {"scanned": 2, "new": 0, "missing": 0, "dropped": 0}
    assert ledger.asset("staged", "old") == {
        "binding_hash": "b",
        "bytes": 7,
        "scanned_at": 9.0,
        "state": "ready",
        "last_used": 9.0,  # a pre-C row is not evictable at once: the hold rule counts from its first scan
    }
    assert ledger.asset("staged", "evicted")["state"] == "ready"
    # evicted by a worker after the scan saw the directory: the scan's row is not merged back
    ledger.put_asset("staged", "evicted", {"state": "evicted", "bytes": 0})
    ledger.sync_assets({("staged", "evicted"): {"bytes": 7, "scanned_at": 10.0}}, older_than=time.time() - 60)
    assert ledger.asset("staged", "evicted") == {"state": "evicted", "bytes": 0, "scanned_at": 9.0, "last_used": 9.0}


def test_staged_last_used_comes_from_draws(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer("b", {})
    ledger.put_volume("b", dataclasses.replace(vol("a"), artifact="art1"))
    ledger.put_volume("b", vol("plain"))  # no artifact: never in the map
    ledger.put_asset("staged", "art1", {"state": "ready", "last_used": 1.0})
    assert ledger.staged_last_used() == {}
    before = time.time()
    ledger.record_draws("b", "c", [{"volume": "a"}, {"volume": "plain"}])
    after = time.time()
    used = ledger.staged_last_used()
    assert list(used) == ["art1"] and before <= used["art1"] <= after
    ledger.sync_assets({("staged", "art1"): {"bytes": 1, "scanned_at": after}})
    assert ledger.asset("staged", "art1")["last_used"] == used["art1"]
    ledger.put_asset("staged", "art1", {"last_used": after + 100})  # newer than every draw: kept
    ledger.sync_assets({("staged", "art1"): {"bytes": 1, "scanned_at": after}})
    assert ledger.asset("staged", "art1")["last_used"] == after + 100


def test_status_board_and_events(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer_dataset("b1", "rec", "fetching", job="j1")
    row = ledger.put_buffer_dataset("b1", "rec", "servable")
    assert row["status"] == "servable" and row["job"] is None and ledger.buffer_datasets("b1")[0]["record"] == "rec"
    assert ledger.buffer_datasets("b2") == [] and ledger.latest_event("b1") is None
    first = ledger.add_event("b1", "created", issue=2, event_id="e1", detail="")
    ledger.add_event("b1", "ready", issue=2, event_id=None, detail="2 of 2")
    assert first["seq"] == 1 and ledger.latest_event("b1")["kind"] == "ready" and len(ledger.events("b1")) == 2
    assert ledger.add_event("b2", "created", issue=None, event_id=None, detail="")["seq"] == 1  # per buffer


def test_retire_drops_only_this_buffers_refs(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer("b1", {})
    ledger.put_buffer("b2", {})
    ledger.put_volume("b1", vol("a"), refs=[("staged", "artA"), ("raw", "rec")])
    ledger.put_volume("b2", vol("b"), refs=[("staged", "artB"), ("raw", "rec")])
    cid = ledger.add_consumer("b1", INFO)
    ledger.record_draws("b1", cid, [{"volume": "a"}, {"volume": "a"}])
    ledger.record_draws("b2", cid, [{"volume": "b"}])
    out = ledger.retire("b1", tmp_path / "archive")
    assert out["refs"] == 2 and out["draws"] == 2 and (tmp_path / "archive" / "b1.draws.jsonl.gz").is_file()
    assert ledger.refs("staged", "artA") == [] and ledger.refs("raw", "rec") == ["b2"]
    assert ledger.max_seq("b1") == 0 and len(ledger.volumes("b1")) == 1  # volumes stay for the page
    assert ledger.max_seq("b2") == 1 and ledger.buffer("b1") == {}
    with gzip.open(tmp_path / "archive" / "b1.draws.jsonl.gz", "rt") as fh:
        lines = [json.loads(line) for line in fh]
    assert [line["seq"] for line in lines] == [1, 2] and set(lines[0]) == {"seq", "consumer", "ts", "descriptor"}
    assert lines[0]["consumer"] == cid and lines[0]["descriptor"]["volume"] == "a"
    again = ledger.retire("b1", tmp_path / "archive")  # a retried retire keeps the archive it wrote
    assert again["refs"] == 0 and again["draws"] == 0
    with gzip.open(tmp_path / "archive" / "b1.draws.jsonl.gz", "rt") as fh:
        assert len(fh.readlines()) == 2
    ledger.record_draws("b1", cid, [{"volume": "a"}])
    ledger.retire("b1", tmp_path / "archive")  # a later retire appends, never overwrites
    with gzip.open(tmp_path / "archive" / "b1.draws.jsonl.gz", "rt") as fh:
        assert len(fh.readlines()) == 3
    kept = ledger.retire("b2", None)  # a dev service keeps the draws
    assert kept == {"refs": 2, "draws": 0, "archive": None} and ledger.max_seq("b2") == 1


def test_buffers_summary_and_missing_flag(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer("b1", {"spec_name": "s"})
    ledger.put_buffer("b2", {})
    ledger.put_volume("b1", vol("a"))
    ledger.put_volume("b1", vol("c"))
    ledger.set_volume_missing("b1", "c", True, "/elsewhere")  # a stat of an old path changes nothing
    assert not ledger.volumes("b1")[1].missing
    ledger.set_volume_missing("b1", "c", True, "/staged/c")
    assert [volume.missing for volume in ledger.volumes("b1")] == [False, True]
    ledger.add_event("b1", "ready", issue=1, event_id=None, detail="")
    cid = ledger.add_consumer("b1", INFO)
    ledger.record_draws("b1", cid, [{"volume": "a"}, {"volume": "a"}])
    with ledger._lock:  # one draw from yesterday
        ledger._db.execute("UPDATE draws SET ts = ts - 86400 WHERE seq = 1")
    one, two = ledger.buffers()
    assert one["id"] == "b1" and one["config"] == {"spec_name": "s"} and one["latest_event"]["kind"] == "ready"
    assert one["volumes"] == 2 and one["missing"] == 1 and one["servable"] == {"train": 1}
    assert one["draws_today"] == 1 and one["consumers_seen"] is not None
    assert two == {
        "id": "b2",
        "config": {},
        "latest_event": None,
        "volumes": 0,
        "missing": 0,
        "servable": {},
        "consumers_seen": None,
        "draws_today": 0,
    }
    ledger.set_volume_missing("b1", "c", False, "/staged/c")
    assert ledger.buffers()[0]["servable"] == {"train": 2}
    ledger.add_event("b0", "planned", issue=3, event_id=None, detail="")  # planned, never sourced: no config yet
    ledger.add_event("b1", "idle", issue=1, event_id=None, detail="")  # a configured buffer is listed once
    planned, *rest = ledger.buffers()
    assert [row["id"] for row in rest] == ["b1", "b2"]
    assert planned["id"] == "b0" and planned["config"] is None and planned["latest_event"]["kind"] == "planned"
    assert (planned["volumes"], planned["draws_today"], planned["servable"]) == (0, 0, {})


def test_retire_keeps_a_draw_recorded_while_the_archive_is_written(tmp_path, monkeypatch):
    """The archive is read outside the lock: a draw recorded meanwhile has a newer seq and stays in the table."""
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer("b1", {})
    cid = ledger.add_consumer("b1", INFO)
    ledger.record_draws("b1", cid, [{"volume": "a"}, {"volume": "a"}])
    archive_draws = ledger._archive_draws

    def draw_meanwhile(buffer, archive, last):
        archive_draws(buffer, archive, last)
        ledger.record_draws("b1", cid, [{"volume": "late"}])  # would deadlock if the archive held the lock

    monkeypatch.setattr(ledger, "_archive_draws", draw_meanwhile)
    out = ledger.retire("b1", tmp_path / "archive")
    assert out["draws"] == 2 and ledger.max_seq("b1") == 3
    with gzip.open(tmp_path / "archive" / "b1.draws.jsonl.gz", "rt") as fh:
        assert [json.loads(line)["seq"] for line in fh] == [1, 2]


def test_usage_counts_empty_answers_and_merges_live_draws(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    day = time.strftime("%Y-%m-%d", time.gmtime())
    led.count_empty("b", day)
    led.count_empty("b", day)
    led.count_empty("b", "2026-01-01")
    led.count_empty("other", day)
    assert led.usage("b") == [
        {"day": "2026-01-01", "draws": 0, "consumers": 0, "empty": 1},
        {"day": day, "draws": 0, "consumers": 0, "empty": 2},
    ]
    led.record_draws("b", "c1", [{"volume": "a"}, {"volume": "a"}])
    led.record_draws("b", "c2", [{"volume": "a"}])
    assert led.usage("b")[1] == {"day": day, "draws": 3, "consumers": 2, "empty": 2}
    assert led.last_hour("b") == {"draws": 3, "requests": 2}  # two requests, three crops
    assert led.last_hour("other") == {"draws": 0, "requests": 0}
    assert led.retire("b", tmp_path / "archive")["draws"] == 3
    assert led.usage("b")[1] == {"day": day, "draws": 3, "consumers": 2, "empty": 2}  # materialised, empty kept
    led.record_draws("b", "c3", [{"volume": "a"}])  # a draw after the retire adds to the materialised day
    assert led.usage("b")[1] == {"day": day, "draws": 4, "consumers": 2, "empty": 2}


def test_kv(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    assert led.kv("keeper") is None
    led.put_kv("keeper", {"a": 1})
    led.put_kv("keeper", {"b": 2})
    assert led.kv("keeper") == {"b": 2}


def test_sync_assets_drops_a_raw_row_with_nothing_cached_that_the_scan_skipped(tmp_path):
    """The scan skips an empty raw directory; its row (registered by an earlier scan) goes rather than turning
    `missing`. A row with content is still marked missing, and a fetch in progress is untouched."""
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_asset("raw", "phantom", {"state": "ready", "bytes": 0, "files": {}})
    ledger.put_asset("raw", "wiped", {"state": "ready", "bytes": 12, "files": {}})
    ledger.put_asset("raw", "fetching", {"state": "fetching", "bytes": 0, "files": {}, "job": "1"})
    counts = ledger.sync_assets({}, older_than=time.time() + 1, kinds=("raw",))
    by_key = {a["key"]: a["data"] for a in ledger.assets("raw")}
    assert "phantom" not in by_key
    assert by_key["wiped"]["state"] == "missing" and by_key["fetching"]["state"] == "fetching"
    assert counts == {"scanned": 0, "new": 0, "missing": 1, "dropped": 1}


# --- assessments, budgets, revoked rows, quarantined volumes ------------------------


def test_assessments_are_append_only_and_filterable(tmp_path):
    from mia_core.judge.policy import new_assessment

    led = Ledger(tmp_path / "l.sqlite")
    first = new_assessment(
        gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s", verdict="approve", by="human:a", at=10.0
    )
    second = new_assessment(
        gate="staged", subject_kind="artifact", subject_ref="art1", verdict="reject", by="human:b", at=20.0
    )
    assert led.put_assessment(first) == first and led.put_assessment({**first, "verdict": "reject"}) == first
    led.put_assessment(second)
    assert [row["id"] for row in led.assessments()] == [second["id"], first["id"]]  # newest first
    assert led.assessments(subject_kind="artifact", subject_ref="art1") == [second]
    assert led.assessments(gate="crops") == [first] and led.assessment("nope") is None
    assert led.assessments(limit=1) == [second]


def test_reserve_enforces_the_caps_atomically_and_settles_once(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    caps = {"max_usd": 10, "max_fetch_bytes": 100, "max_jobs": 3, "max_concurrent_jobs": 2, "max_days": 14}
    root = "o/buffers#7"
    first = led.reserve(root, "job-1", "match", 6.0, caps, now=1000.0)
    assert first["ok"] and first["usage"]["reserved_usd"] == 6.0 and first["usage"]["running"] == 1
    assert led.reserve(root, "job-1", "match", 6.0, caps, now=1001.0)["usage"]["reserved_usd"] == 6.0  # a retry
    over = led.reserve(root, "job-2", "match", 5.0, caps, now=1002.0)
    assert not over["ok"]
    assert over["reason"] == "root o/buffers#7: usd 6.00 reserved or spent + 5.00 would exceed max_usd 10"
    assert led.reserve(root, "job-2", "stage", 0.0, caps, now=1002.0)["ok"]  # jobs: 2 running of 2
    busy = led.reserve(root, "job-3", "stage", 0.0, caps, now=1003.0)
    assert busy["reason"] == "root o/buffers#7: 2 jobs running, max_concurrent_jobs 2"
    settled = led.settle(root, "job-1", usd=2.5)
    assert (settled["reserved_usd"], settled["spent_usd"], settled["running"], settled["jobs"]) == (0.0, 2.5, 1, 2)
    assert led.settle(root, "job-1", usd=2.5) == settled  # settling twice changes nothing
    assert led.settle(root, "never", fetched_bytes=5) == settled  # an unknown reservation: a no-op
    assert led.reserve(root, "job-3", "stage", 0.0, caps, now=1004.0)["ok"]
    assert led.settle(root, "job-2", fetched_bytes=60)["fetched_bytes"] == 60
    launched = led.reserve(root, "job-4", "fetch", 0.0, caps, now=1005.0)
    assert launched["reason"] == "root o/buffers#7: 3 jobs launched, max_jobs 3"
    assert led.settle(root, "job-3", usd=None)["spent_usd"] == 2.5  # a job reserved at 0 without a cost: nothing added
    assert led.settle(root, "job-3", fetched_bytes=50) == led.budget(root)  # closed already: a no-op
    roomy = {**caps, "max_jobs": 99}
    assert led.reserve(root, "job-5", "fetch", 0.0, roomy, now=1006.0)["ok"]
    assert led.settle(root, "job-5", fetched_bytes=50)["fetched_bytes"] == 110
    bytes_over = led.reserve(root, "job-6", "fetch", 0.0, roomy, now=1007.0)
    assert bytes_over["reason"] == "root o/buffers#7: 110 bytes fetched, over max_fetch_bytes 100"
    old = led.reserve(root, "job-9", "stage", 0.0, {**roomy, "max_fetch_bytes": 1e9}, now=1000.0 + 15 * 86400)
    assert old["ok"] and old["usage"]["epochs"] == 2 and old["usage"]["jobs"] == 1  # idle past max_days: a fresh epoch
    assert led.budget("nobody") is None and [row["root"] for row in led.budgets()] == [root]


def test_sync_datasets_never_touches_a_revoked_row(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_dataset("bad", {"state": "revoked", "revoked_by": "human:a"})
    out = led.sync_datasets({"bad": {"folder": "f-bad"}, "good": {"folder": "f-good"}})
    assert out == {"prepared": 1, "demoted": 0, "revoked_kept": 1}
    assert led.dataset("bad")["state"] == "revoked" and "folder" not in led.dataset("bad")
    assert led.dataset("good")["state"] == "prepared"


def test_delete_volume_drops_its_frozen_rows_and_an_orphaned_ref(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    led.put_volume("b", dataclasses.replace(vol("rec/a"), artifact="art1"), refs=[("staged", "art1"), ("raw", "rec")])
    led.put_volume("b", dataclasses.replace(vol("rec/b"), artifact="art1"), refs=[("staged", "art1")])
    led.extend_frozen("b", "val", [{"volume": "rec/a", "i": 0}, {"volume": "rec/b", "i": 1}])
    assert led.delete_volume("b", "rec/a") and not led.delete_volume("b", "rec/a")
    assert [v.id for v in led.volumes("b")] == ["rec/b"] and led.frozen("b", "val") == [{"volume": "rec/b", "i": 1}]
    assert led.refs("staged", "art1") == ["b"]  # rec/b still lives in art1
    assert led.delete_volume("b", "rec/b") and led.refs("staged", "art1") == []
    assert led.volume_rows("b") == []


def test_quarantined_volumes_are_those_whose_artifact_is_quarantined(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_volume("b", dataclasses.replace(vol("rec/a"), artifact="art1"))
    led.put_volume("b", dataclasses.replace(vol("rec/b"), artifact="art2"))
    led.put_asset("staged", "art1", {"quarantined": True, "quarantined_at": 1.0})
    assert led.quarantined_volumes("b") == ["rec/a"]
    assert led.volume_rows("b")[0] == {"id": "rec/a", "artifact": "art1", "record": "rec", "missing": False}


def test_an_unknown_cost_keeps_the_reservation_as_spent(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    caps = {"max_usd": 10}
    assert led.reserve("r", "job-1", "match", 5.0, caps, now=1.0)["ok"]
    settled = led.settle("r", "job-1", usd=None)  # the report was unreadable: the reservation is what it cost
    assert (settled["reserved_usd"], settled["spent_usd"]) == (0.0, 5.0)
    assert led.reserve("r", "job-2", "match", 5.0, caps, now=2.0)["ok"]
    assert not led.reserve("r", "job-3", "match", 1.0, caps, now=3.0)["ok"]  # 5 spent + 5 reserved: full


def test_a_concurrency_refusal_asks_the_caller_to_wait(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    caps = {"max_concurrent_jobs": 1, "max_jobs": 1}
    assert led.reserve("r", "job-1", "stage", 0.0, caps, now=1.0)["ok"]
    busy = led.reserve("r", "job-2", "stage", 0.0, caps, now=2.0)
    assert not busy["ok"] and busy["wait"] is True and "max_concurrent_jobs" in busy["reason"]
    led.settle("r", "job-1")
    full = led.reserve("r", "job-2", "stage", 0.0, caps, now=3.0)
    assert not full["ok"] and full["wait"] is False and "max_jobs" in full["reason"]


def test_an_idle_root_past_max_days_starts_a_new_epoch(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    caps = {"max_days": 14, "max_jobs": 2}
    assert led.reserve("r", "job-1", "match", 1.0, caps, now=0.0)["ok"]
    assert led.reserve("r", "job-2", "match", 1.0, caps, now=1.0)["ok"]
    assert "max_jobs" in led.reserve("r", "job-3", "match", 1.0, caps, now=2.0)["reason"]
    led.settle("r", "job-1", usd=1.0)
    late = led.reserve("r", "job-3", "match", 1.0, caps, now=15 * 86400.0)
    assert "15.0 days old" in late["reason"]  # job-2 still runs: the request is a runaway, not an old one
    led.settle("r", "job-2", usd=1.0)
    fresh = led.reserve("r", "job-3", "match", 1.0, caps, now=15 * 86400.0)
    assert fresh["ok"] and fresh["usage"]["since"] == 15 * 86400.0 and fresh["usage"]["jobs"] == 1
    assert fresh["usage"]["epochs"] == 2 and fresh["usage"]["spent_usd"] == 0.0


def test_assessments_before_cursor_and_by_filter(tmp_path):
    from mia_core.judge.policy import new_assessment

    led = Ledger(tmp_path / "l.sqlite")
    rows = [
        new_assessment(gate="crops", subject_kind="crop_set", subject_ref=f"s{i}", verdict="approve", by=by, at=at)
        for i, (by, at) in enumerate([("human:a", 10.0), ("judge", 20.0), ("human:b", 30.0)])
    ]
    for row in rows:
        led.put_assessment(row)
    assert [r["by"] for r in led.assessments()] == ["human:b", "judge", "human:a"]
    assert [r["by"] for r in led.assessments(before=rows[2]["id"])] == ["judge", "human:a"]
    assert led.assessments(before=rows[2]["id"], limit=1) == [rows[1]]
    assert led.assessments(by="judge") == [rows[1]] and led.assessments(before=rows[0]["id"]) == []


def test_agreement_pairs_judge_with_newest_human(tmp_path):
    from mia_core.judge.policy import new_assessment

    led = Ledger(tmp_path / "l.sqlite")

    def put(ref, verdict, by, at):
        led.put_assessment(
            new_assessment(gate="crops", subject_kind="crop_set", subject_ref=ref, verdict=verdict, by=by, at=at)
        )

    put("s1", "reject", "human:a", 10.0)  # relabelled later: the newest human verdict counts
    put("s1", "approve", "judge", 11.0)
    put("s1", "approve", "human:a", 12.0)
    put("s2", "approve", "judge", 20.0)  # no human yet
    put("s3", "reject", "judge", 30.0)
    put("s3", "approve", "human:b", 31.0)
    put("s4", "approve", "human:c", 40.0)  # never judged: not a pair
    put("s5", "escalate", "judge", 45.0)  # abstained: neither agreement nor disagreement
    put("s5", "approve", "human:c", 46.0)
    put("art", "approve", "judge", 50.0)  # another gate's row must not count here
    led.put_assessment(
        new_assessment(
            gate="staged", subject_kind="artifact", subject_ref="art", verdict="approve", by="judge", at=51.0
        )
    )
    out = led.agreement("crops")
    assert (out["judged"], out["agree"], out["disagree"], out["abstain"], out["pending"]) == (5, 1, 1, 1, 2)
    assert {pair["subject"]: pair["agree"] for pair in out["pairs"]}["crop_set:s5"] is None
    by_subject = {pair["subject"]: pair for pair in out["pairs"]}
    assert by_subject["crop_set:s1"]["agree"] is True and by_subject["crop_set:s1"]["human"] == "approve"
    assert by_subject["crop_set:s3"] == {
        "subject": "crop_set:s3",
        "judge": "reject",
        "human": "approve",
        "agree": False,
        "at": by_subject["crop_set:s3"]["at"],
    }
    assert by_subject["crop_set:s2"]["agree"] is None and "crop_set:s4" not in by_subject
    assert led.agreement("staged")["judged"] == 1 and led.agreement("review") == {
        "judged": 0,
        "agree": 0,
        "disagree": 0,
        "abstain": 0,
        "pending": 0,
        "pairs": [],
    }


def test_adopt_subject_is_idempotent_and_keeps_a_written_stage(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    row = ledger.adopt_subject("dataset", 234, "rec-a", stage="review", data={"note": 1})
    assert {name: row[name] for name in ("kind", "number", "key", "stage", "revision", "issue", "data")} == {
        "kind": "dataset",
        "number": 234,
        "key": "rec-a",
        "stage": "review",
        "revision": 0,
        "issue": 234,
        "data": {"note": 1},
    }
    # until a workflow writes a stage, the label is the truth: a second adoption refreshes stage and data
    assert ledger.adopt_subject("dataset", 234, "rec-a", stage="done")["stage"] == "done"
    assert ledger.set_subject_stage("dataset", 234, "testing")["revision"] == 1
    again = ledger.adopt_subject("dataset", 234, "rec-a", stage="new", data={"note": 2})
    assert (again["stage"], again["revision"], again["data"]) == ("testing", 1, {})


def test_subject_keys_are_unique_within_a_kind(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.adopt_subject("buffer", 8, "em pool")
    with pytest.raises(ValueError, match="em pool"):
        ledger.adopt_subject("buffer", 9, "em pool")
    assert ledger.adopt_subject("prep", 9, "em pool")["number"] == 9  # another kind may hold the same key


def test_find_subjects_by_issue_and_key(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.adopt_subject("dataset", 234, "rec-a")
    ledger.adopt_subject("buffer", 234, "pool")
    assert [row["kind"] for row in ledger.find_subjects(issue=234)] == ["buffer", "dataset"]
    assert [row["number"] for row in ledger.find_subjects(key="pool")] == [234]
    assert len(ledger.find_subjects()) == 2
    assert ledger.subject("dataset", "rec-a")["number"] == 234 and ledger.subject("dataset", "nope") is None
    assert ledger.set_subject_stage("dataset", 999, "new") is None


def test_timeline_entries_are_once_per_marker_and_a_stage_always_moves(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    assert ledger.append_entry("dataset", 1, body="x", marker="m") is None  # no such subject
    ledger.adopt_subject("dataset", 1, "rec-a")
    subject, entry = ledger.append_entry(
        "dataset", 1, body="testing\nm1", marker="m1", round_id="i1-e2", stage="testing"
    )
    assert (subject["stage"], subject["revision"]) == ("testing", 1)
    assert {name: entry[name] for name in ("seq", "round", "stage", "body", "marker", "mirrored_at")} == {
        "seq": 1,
        "round": "i1-e2",
        "stage": "testing",
        "body": "testing\nm1",
        "marker": "m1",
        "mirrored_at": None,
    }
    ledger.append_entry("dataset", 1, body="review", marker="m2", stage="review")
    # the round enters testing again: nothing is appended, the stage and the revision still move
    subject, again = ledger.append_entry("dataset", 1, body="other text", marker="m1", stage="testing")
    assert (subject["stage"], subject["revision"], again["seq"], again["body"]) == ("testing", 3, 1, "testing\nm1")
    subject, none = ledger.append_entry("dataset", 1, stage="done")  # a stage with nothing to say
    assert (subject["stage"], none) == ("done", None)
    assert [row["seq"] for row in ledger.timeline("dataset", 1)] == [1, 2]


def test_mark_mirrored_and_the_subjects_still_to_mirror(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.adopt_subject("dataset", 1, "rec-a")
    ledger.adopt_subject("buffer", 8, "pool")
    ledger.append_entry("dataset", 1, body="a", marker="a")
    ledger.append_entry("buffer", 8, body="b", marker="b")
    assert ledger.unmirrored_subjects() == [("buffer", 8), ("dataset", 1)]
    assert ledger.mark_mirrored("buffer", 8, 1, "https://x/c1") and not ledger.mark_mirrored("buffer", 8, 9, None)
    assert ledger.unmirrored_subjects() == [("dataset", 1)]
    row = ledger.timeline("buffer", 8)[0]
    assert row["comment_url"] == "https://x/c1" and row["mirrored_at"]
    assert [row["number"] for row in ledger.find_subjects(kind="buffer", number=8)] == [8]


# --- actions and the ids the registry issues (spec webapp-actions §4.1, §4.2) ---


def test_new_subjects_get_numbers_from_the_clock_that_never_repeat(tmp_path, monkeypatch):
    from mia_buffers import ledger as ledger_module

    now = [ledger_module.NUMBER_EPOCH + 1000.5]
    monkeypatch.setattr(ledger_module.time, "time", lambda: now[0])
    ledger = Ledger(tmp_path / "l.sqlite")
    first = ledger.create_subject("dataset", "rec-a", data={"x": 1})
    assert (first["number"], first["issue"], first["stage"], first["data"]) == (1000, None, "new", {"x": 1})
    assert ledger.create_subject("dataset", "rec-a")["number"] == 1000  # the key exists: the same subject
    assert ledger.create_subject("buffer", "pool")["number"] == 1001  # one sequence for every kind, bumped
    ledger.backup(tmp_path / "backup.sqlite")
    assert ledger.create_subject("prep", "prep-x")["number"] == 1002
    now[0] += 60  # the backup is restored a minute later: its counter is old, the clock is not
    restored = Ledger(tmp_path / "backup.sqlite")
    assert restored.create_subject("prep", "prep-y")["number"] == 1060
    adopted = ledger.create_subject("dataset", "rec-b", number=234)  # a dataset whose GitHub issue is known
    assert (adopted["number"], adopted["issue"]) == (234, 234)


def test_an_action_is_recorded_once_per_key_and_settled_with_its_entry(tmp_path, monkeypatch):
    from mia_buffers import ledger as ledger_module

    now = [1_800_000_000.25]
    monkeypatch.setattr(ledger_module.time, "time", lambda: now[0])
    ledger = Ledger(tmp_path / "l.sqlite")
    number = ledger.create_subject("dataset", "rec-a")["number"]
    action, created = ledger.add_action("dataset", number, "go", "page", key="k1", payload={"root": "o/b#8"})
    assert created and action["id"] == 1_800_000_000_250 and action["state"] == "pending"
    assert (action["kind"], action["number"], action["action"], action["actor"]) == ("dataset", number, "go", "page")
    assert action["payload"] == {"root": "o/b#8"} and action["at"].startswith("2027-01-15T08:00:00")
    again, created = ledger.add_action("dataset", number, "go", "page", key="k1")
    assert not created and again["id"] == action["id"]  # a repeat of the key is the same action
    with pytest.raises(ValueError, match="k1"):
        ledger.add_action("dataset", number, "approve", "page", key="k1")  # the key names another request
    second, _ = ledger.add_action("dataset", number, "cancel", "page")  # the same millisecond: bumped
    assert second["id"] == action["id"] + 1 and second["key"] is None
    sent = ledger.settle_action(action["id"], "sent", entry=("**Action:** `go`", "<!-- action -->"))
    assert sent["state"] == "sent" and [row["body"] for row in ledger.timeline("dataset", number)] == [
        "**Action:** `go`"
    ]
    failed = ledger.settle_action(second["id"], "failed", "Temporal is away")
    assert (failed["state"], failed["detail"]) == ("failed", "Temporal is away")
    assert [row["id"] for row in ledger.actions("dataset", number)] == [action["id"], second["id"]]
    assert ledger.action(action["id"])["state"] == "sent" and ledger.action(1) is None
    assert ledger.settle_action(1, "sent") is None


def test_subject_data_and_issue_can_be_written(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    number = ledger.create_subject("buffer", "pool", data={"spec": "a"})["number"]
    assert ledger.set_subject_data("buffer", number, {"spec": "b"})["data"] == {"spec": "b"}
    assert ledger.set_subject_issue("buffer", number, 77)["issue"] == 77
    assert ledger.find_subjects(issue=77)[0]["key"] == "pool" and ledger.set_subject_issue("buffer", 1, 2) is None
