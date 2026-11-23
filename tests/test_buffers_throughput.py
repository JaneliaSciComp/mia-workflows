"""The arithmetic behind the host routes: minutes added over jobs, then read by level."""

import pytest

from mia_buffers.throughput import by_level, by_minute, failed, level, summary


def row(minute, job, connection_seconds, bytes_, answers, host="a.org", **more):
    return {
        "host": host,
        "minute": minute,
        "job": job,
        "service": "buffers",
        "requests": sum(answers.values()),
        "connection_seconds": connection_seconds,
        "peak": 1,
        "bytes": bytes_,
        "answers": answers,
        "recovered": {},
        "final": {},
        "asked_wait_seconds": 0.0,
        "backoff_seconds": 0.0,
        **more,
    }


ROWS = [
    row(100, "j1", 120, 6000, {"206": 9, "503": 1}, recovered={"503": 1}),
    row(100, "j2", 60, 3000, {"206": 5}),  # minute 100: a mean of 3 open
    row(101, "j1", 240, 1200, {"206": 2, "403": 2}, final={"403": 2}, asked_wait_seconds=7.0, backoff_seconds=16.0),
    row(102, "j1", 20, 600, {"200": 1}),  # under half a connection: level 0
]


def test_minutes_are_added_over_jobs():
    first = by_minute(ROWS)[0]
    assert (first["minute"], first["jobs"], first["connection_seconds"], first["bytes"], first["peak"]) == (
        100,
        2,
        180,
        9000,
        2,
    )
    assert first["answers"] == {"206": 14, "503": 1} and first["recovered"] == {"503": 1}


def test_a_host_is_read_by_its_mean_connections_open():
    zero, three, four = by_level(by_minute(ROWS))
    assert [(r["level"], r["minutes"], r["jobs_per_minute"]) for r in (zero, three, four)] == [
        (0, 1, 1.0),
        (3, 1, 2.0),
        (4, 1, 1.0),
    ]
    assert (three["bytes_per_second"], three["bytes_per_connection_second"], three["requests"]) == (150.0, 50.0, 15)
    assert three["failed_share"] == pytest.approx(1 / 15) and four["failed_share"] == 0.5
    assert (four["bytes_per_second"], four["final"], four["asked_wait_seconds"], four["backoff_seconds"]) == (
        20.0,
        {"403": 2},
        7.0,
        16.0,
    )
    assert zero["failed_share"] == 0.0 and zero["bytes_per_connection_second"] == 30.0


def test_a_hosts_summary():
    out = summary(by_minute(ROWS))
    assert (out["minutes"], out["bytes"], out["bytes_per_second"], out["requests"], out["last_minute"]) == (
        3,
        10800,
        60.0,
        20,
        102,
    )
    assert out["bytes_per_connection_second"] == pytest.approx(10800 / 440)


def test_levels_round_to_the_nearest_and_nothing_divides_by_zero():
    assert [level(seconds) for seconds in (0, 29, 30, 89, 90, 240)] == [0, 0, 1, 1, 2, 4]
    idle = by_level(by_minute([row(100, "j1", 0, 0, {}, asked_wait_seconds=30.0)]))[0]
    assert idle["failed_share"] is None and idle["bytes_per_connection_second"] is None
    assert failed("403") and failed("timeout") and not failed("206") and not failed("302")
