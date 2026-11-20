"""Small helpers for raw-cache asset rows: the states, the harness's files.json and the row both workers post."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from pathlib import Path

from mia_core.fs import dir_bytes
from mia_core.untrusted import loads, read_untrusted, text

ASSET_STATES = ("fetching", "staging", "waiting-drop", "ready", "partial", "missing", "evicted")
MANIFEST = "files.json"


def cache_key(url: str, validators: dict) -> str:
    """The name of a url's entry in a raw cache: the harness's `mia_harness.io._key`, repeated here."""
    return hashlib.sha256(f"{url}\n{json.dumps(validators, sort_keys=True)}".encode()).hexdigest()


def is_entry(path: Path) -> bool:
    """Whether the path is a cache entry's own file: a regular file, never a link."""
    return path.is_file() and not path.is_symlink()


def read_files_json(cache_dir: Path) -> dict:
    """The harness's files.json (url -> {validators, source}); {} when missing or unreadable."""
    data = loads(read_untrusted(cache_dir, MANIFEST, 1 << 20))
    return data if isinstance(data, dict) else {}


def _file_entry(entry: dict) -> dict:
    """The size and source of one files.json entry, reduced to bounded typed values."""
    validators = entry.get("validators")
    size = validators.get("size") if isinstance(validators, dict) else None
    return {
        "size": size if isinstance(size, int) and not isinstance(size, bool) else None,
        "source": text(entry.get("source"), 50) or None,
    }


def file_entries(manifest: dict) -> dict:
    """A files.json mapping reduced to bounded urls and typed entries, as raw rows carry it."""
    return {text(url, 500): _file_entry(entry) for url, entry in manifest.items() if isinstance(entry, dict)}


def _names_members(url: str) -> bool:
    """Whether the url is `<zip or directory>::<member or glob>`: a `::` in its path, not in an IPv6 host."""
    path_start = url.find("/", url.find("://") + 3)
    return path_start >= 0 and "::" in url[path_start:]


def _cached(cache_dir: Path, manifest: dict, url: str) -> bool:
    """Whether the cache holds what `url` names. The harness lists a whole file at its first HEAD, before it
    keeps a byte (a read by range keeps none), so a file counts only with its entry on disk. A `::` url counts
    when listed: `harness fetch` lists it once its members are cached, while the container's own entry says no
    more than that a HEAD recorded its validators."""
    entry = manifest.get(url)
    if not isinstance(entry, dict):
        return False
    validators = entry.get("validators")
    return _names_members(url) or (isinstance(validators, dict) and is_entry(cache_dir / cache_key(url, validators)))


def cache_missing(cache_dir: Path, manifest: dict, urls: Iterable[str]) -> tuple[bool, list[str]]:
    """(ready, missing): the urls of `urls` the cache does not hold (`_cached`). Ready when none is missing; with
    no urls at all, when files.json lists anything."""
    wanted = dict.fromkeys(urls)
    missing = [url for url in wanted if not _cached(cache_dir, manifest, url)]
    return (not missing if wanted else bool(manifest)), missing


def raw_asset(cache_dir: Path) -> dict:
    """The raw-cache asset row: path, bytes, files.json entries, and the state `ready`, which for a raw cache says
    that it is at rest on disk, whatever jobs left in it: nothing completes one, so none is partial."""
    return {
        "path": str(cache_dir),
        "bytes": dir_bytes(cache_dir),
        "files": file_entries(read_files_json(cache_dir)),
        "state": "ready",
    }
