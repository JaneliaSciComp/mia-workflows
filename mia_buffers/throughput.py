"""What each host gave: stored per-job minute rows added over jobs, then read per host and by load level.

Pure arithmetic over the ledger's transfer rows: no ledger, no clock. It labels nothing; the share of failed
attempts is counted from the outcome keys of `answers`.
"""

from __future__ import annotations

from collections import defaultdict

_NUMBERS = ("requests", "connection_seconds", "peak", "bytes", "asked_wait_seconds", "backoff_seconds")
_MAPS = ("answers", "recovered", "final")


def failed(outcome: str) -> bool:
    """Whether an outcome (a status text or an error name) is anything but a 2xx or 3xx status."""
    return not (len(outcome) == 3 and outcome[0] in "23" and outcome.isdigit())


def level(connection_seconds: float) -> int:
    """A minute's mean connections open, rounded to the nearest whole number."""
    return int(connection_seconds / 60 + 0.5)


def _added(maps: list[dict[str, int]]) -> dict[str, int]:
    """The maps added key by key."""
    total: dict[str, int] = defaultdict(int)
    for one in maps:
        for key, count in one.items():
            total[key] += count
    return dict(total)


def by_minute(rows: list[dict]) -> list[dict]:
    """The stored rows added over jobs: one row per host and minute (ordered), with `jobs` the rows added."""
    groups: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for stored in rows:
        groups[stored["host"], stored["minute"]].append(stored)
    return [
        {
            "host": host,
            "minute": minute,
            "jobs": len(group),
            **{name: sum(stored[name] for stored in group) for name in _NUMBERS},
            **{name: _added([stored[name] for stored in group]) for name in _MAPS},
        }
        for (host, minute), group in sorted(groups.items())
    ]


def _total(minutes: list[dict]) -> dict:
    """One row for a set of a host's minutes: what they moved, asked and were answered."""
    seconds = 60 * len(minutes)
    moved = sum(minute["bytes"] for minute in minutes)
    open_seconds = sum(minute["connection_seconds"] for minute in minutes)
    return {
        "minutes": len(minutes),
        "bytes": moved,
        "bytes_per_second": moved / seconds if seconds else 0.0,
        "bytes_per_connection_second": moved / open_seconds if open_seconds else None,
        "requests": sum(minute["requests"] for minute in minutes),
        **{name: _added([minute[name] for minute in minutes]) for name in _MAPS},
        "asked_wait_seconds": sum(minute["asked_wait_seconds"] for minute in minutes),
        "backoff_seconds": sum(minute["backoff_seconds"] for minute in minutes),
    }


def summary(minutes: list[dict]) -> dict:
    """One host's minutes as one row, with the last minute seen."""
    return {**_total(minutes), "last_minute": max((minute["minute"] for minute in minutes), default=None)}


def by_level(minutes: list[dict]) -> list[dict]:
    """One host's minutes grouped by level (ascending), each group read as `summary` does, plus the mean jobs per
    minute and the share of attempts that ended failed (None when none ended)."""
    groups: dict[int, list[dict]] = defaultdict(list)
    for minute in minutes:
        groups[level(minute["connection_seconds"])].append(minute)
    rows = []
    for at, group in sorted(groups.items()):
        row = _total(group)
        attempts = sum(row["answers"].values())
        misses = sum(count for outcome, count in row["answers"].items() if failed(outcome))
        rows.append(
            {
                "level": at,
                **row,
                "jobs_per_minute": sum(minute["jobs"] for minute in group) / len(group),
                "failed_share": misses / attempts if attempts else None,
            }
        )
    return rows
