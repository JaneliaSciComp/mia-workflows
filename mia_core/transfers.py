"""Reads the transfer files a job wrote (one row per host and minute) as bounded, typed rows. Job output is untrusted."""

from __future__ import annotations

import itertools
import os
import re
from pathlib import Path
from typing import Any

from mia_core.untrusted import loads, read_untrusted

TRANSFERS_DIR = "transfers"
TRANSFERS_ENV = "MIA_HARNESS_TRANSFERS_DIR"
MAX_FILES, MAX_BYTES, MAX_ROWS = 256, 8 << 20, 20_000
MAX_COUNT, MAX_BODY_BYTES, MAX_SECONDS, MAX_KEYS = 1_000_000, 1 << 50, 86_400, 32
KINDS = frozenset({"timeout", "connect", "dropped", "truncated"})
WINDOW_S = 7 * 86_400

_COUNTS = {"requests": MAX_COUNT, "peak": MAX_COUNT, "bytes": MAX_BODY_BYTES}
_SECONDS = ("connection_seconds", "asked_wait_seconds", "backoff_seconds")
_MAPS = ("answers", "recovered", "final")
_FILE_NAME = re.compile(r"\d+-\d+\.jsonl")
_KEY = re.compile(r"\d{3}")


def _is_int(value: Any, high: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= high


def reduce_row(value: Any) -> dict | None:
    """The typed row a job's line says, or None when the line is not one. Checks nothing about host list or time."""
    if not isinstance(value, dict):
        return None
    minute, host = value.get("minute"), value.get("host")
    if not _is_int(minute, 1 << 62) or not isinstance(host, str) or not 0 < len(host) <= 253:
        return None
    row: dict = {"minute": minute, "host": host.lower()}
    for name, high in _COUNTS.items():
        field = value.get(name, 0)
        if not _is_int(field, high):
            return None
        row[name] = field
    for name in _SECONDS:
        field = value.get(name, 0)
        if isinstance(field, bool) or not isinstance(field, int | float) or not 0 <= field <= MAX_SECONDS:
            return None
        row[name] = field
    for name in _MAPS:
        field = value.get(name, {})
        if not isinstance(field, dict) or len(field) > MAX_KEYS:
            return None
        if not all(isinstance(key, str) and (_KEY.fullmatch(key) or key in KINDS) for key in field):
            return None
        if not all(_is_int(count, MAX_COUNT) for count in field.values()):
            return None
        row[name] = dict(field)
    return row


def _add(total: dict, row: dict) -> None:
    """Add `row` into `total` (same host and minute); every sum is cut to its field's limit."""
    for name, high in _COUNTS.items():
        total[name] = min(total[name] + row[name], high)
    for name in _SECONDS:
        total[name] = min(total[name] + row[name], MAX_SECONDS)
    for name in _MAPS:
        merged = dict(total[name])
        for key, count in row[name].items():
            merged[key] = min(merged.get(key, 0) + count, MAX_COUNT)
        total[name] = {key: merged[key] for key in sorted(merged)[:MAX_KEYS]}


def collect(workspace: Path, allow_hosts: list[str] | None, now: float) -> dict:
    """The rows of `<workspace>/transfers`, or `{}` when there is nothing to read, or `{"transfers_refused": why}`."""
    root = Path(workspace).resolve()
    directory = root / TRANSFERS_DIR
    try:
        if directory.is_symlink() or not directory.is_dir() or not directory.resolve().is_relative_to(root):
            return {}
        with os.scandir(directory) as listing:
            entries = [entry.name for entry in itertools.islice(listing, MAX_FILES + 1)]
    except OSError:
        return {}
    if len(entries) > MAX_FILES:
        return {"transfers_refused": f"the transfers directory holds more than {MAX_FILES} entries"}
    names = sorted(name for name in entries if _FILE_NAME.fullmatch(name))
    if not names:
        return {}
    if allow_hosts is None:
        return {"transfers_refused": "the job's record names no allowed hosts"}
    allowed = {host.lower() for host in allow_hosts}
    newest, oldest = int(now // 60), int((now - WINDOW_S) // 60)
    sums: dict[tuple[int, str], dict] = {}
    dropped, size = 0, 0
    for name in names:
        data = read_untrusted(root, f"{TRANSFERS_DIR}/{name}", MAX_BYTES - size + 1, binary=True)
        if data is None:
            continue
        size += len(data)
        if size > MAX_BYTES:
            return {"transfers_refused": f"the transfer files hold more than {MAX_BYTES} bytes"}
        for line in data.decode(errors="replace").splitlines():
            if not line.strip():
                continue
            row = reduce_row(loads(line))
            if row is None or row["host"] not in allowed or not oldest <= row["minute"] <= newest:
                dropped += 1
                continue
            key = (row["minute"], row["host"])
            if key in sums:
                _add(sums[key], row)
            else:
                sums[key] = row
    if len(sums) > MAX_ROWS:
        return {"transfers_refused": f"the transfer files hold more than {MAX_ROWS} rows"}
    return {"transfers": [sums[key] for key in sorted(sums)], "transfers_dropped": dropped}
