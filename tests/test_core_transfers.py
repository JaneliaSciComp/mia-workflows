"""Reading the transfer files a job wrote: bounded, typed, summed."""

from __future__ import annotations

import json

from mia_core import transfers
from mia_core.transfers import collect

NOW = 6_000_000.0  # minute 100000
OK = {
    "minute": 100000,
    "host": "a.org",
    "requests": 2,
    "connection_seconds": 3.5,
    "peak": 1,
    "bytes": 10,
    "answers": {"206": 2},
    "recovered": {},
    "final": {},
    "asked_wait_seconds": 0,
    "backoff_seconds": 0,
}


def write(workspace, name, *lines):
    (workspace / "transfers").mkdir(parents=True, exist_ok=True)
    text = "\n".join(line if isinstance(line, str) else json.dumps(line) for line in lines)
    (workspace / "transfers" / name).write_text(text)


def test_rows_of_several_files_are_added_up(tmp_path):
    write(tmp_path, "5-1.jsonl", OK, {**OK, "minute": 99999})
    write(tmp_path, "6-2.jsonl", {**OK, "answers": {"206": 1, "503": 1}, "recovered": {"503": 1}})
    out = collect(tmp_path, ["a.org"], NOW)
    assert out["transfers_dropped"] == 0 and [row["minute"] for row in out["transfers"]] == [99999, 100000]
    assert out["transfers"][1] == {
        **OK,
        "requests": 4,
        "connection_seconds": 7.0,
        "peak": 2,
        "bytes": 20,
        "answers": {"206": 3, "503": 1},
        "recovered": {"503": 1},
    }


def test_a_line_that_is_not_a_row_is_dropped_and_counted(tmp_path):
    bad = [
        "not json",
        '{"minute": 100000, "host": "a.org", "bytes": NaN}',
        {**OK, "requests": 1_000_001},
        {**OK, "bytes": -1},
        {**OK, "peak": True},
        {**OK, "backoff_seconds": 86_401},
        {**OK, "answers": {str(100 + i): 1 for i in range(33)}},
        {**OK, "answers": {"refused": 1}},
        {**OK, "host": "b.org"},  # not a host the job was allowed
        {**OK, "minute": 100001},  # after the collection
        {**OK, "minute": 100000 - 7 * 1440 - 1},  # over 7 days old
        '{"minute": 100000, "host": "a.o',  # the line a killed process was writing
        '{"minute": 100000, "host": "a.org", "connection_seconds": ' + "9" * 400 + "}",  # past float range
    ]
    write(tmp_path, "5-1.jsonl", {**OK, "host": "A.ORG"}, *bad)
    out = collect(tmp_path, ["A.org"], NOW)
    assert out == {"transfers": [OK], "transfers_dropped": len(bad)}


def test_a_job_over_its_bounds_keeps_no_rows(tmp_path, monkeypatch):
    many, big, rows = tmp_path / "many", tmp_path / "big", tmp_path / "rows"
    for i in range(257):
        write(many, f"{i}-1.jsonl", OK)
    write(big, "5-1.jsonl", OK, " " * (8 << 20))
    write(rows, "5-1.jsonl", OK, {**OK, "minute": 99999}, {**OK, "minute": 99998})
    monkeypatch.setattr(transfers, "MAX_ROWS", 2)
    for workspace in (many, big, rows):
        out = collect(workspace, ["a.org"], NOW)
        assert set(out) == {"transfers_refused"} and out["transfers_refused"]


def test_a_record_without_its_hosts_keeps_no_rows_and_no_files_is_nothing(tmp_path):
    assert collect(tmp_path, ["a.org"], NOW) == {}
    write(tmp_path, "5-1.jsonl", OK)
    assert set(collect(tmp_path, None, NOW)) == {"transfers_refused"}


def test_nothing_is_read_through_a_symlink(tmp_path):
    outside, linked_dir, linked_file = tmp_path / "outside", tmp_path / "w1", tmp_path / "w2"
    write(outside, "5-1.jsonl", OK)
    linked_dir.mkdir()
    (linked_dir / "transfers").symlink_to(outside / "transfers")
    (linked_file / "transfers").mkdir(parents=True)
    (linked_file / "transfers" / "5-1.jsonl").symlink_to(outside / "transfers" / "5-1.jsonl")
    assert collect(linked_dir, ["a.org"], NOW) == {}
    assert collect(linked_file, ["a.org"], NOW).get("transfers", []) == []


def test_a_sum_is_cut_to_its_limit(tmp_path):
    write(tmp_path, "5-1.jsonl", {**OK, "bytes": 1 << 50})
    write(tmp_path, "6-1.jsonl", {**OK, "bytes": 1 << 50})
    assert collect(tmp_path, ["a.org"], NOW)["transfers"][0]["bytes"] == 1 << 50
