"""The manual drop (spec §3): which file name a human gives each URL, the raw cache's key for a dropped file,
and the `files.json` manifest the harness reads instead of a HEAD request.

A dropped file is named by `mia_core.assets.cache_key`, so it lands where the harness's `io.fetch(url)` looks.
`cached_files` goes the other way: the files of a record's raw cache under the names they had upstream.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

from mia_core.assets import cache_key, is_entry, read_files_json
from mia_core.untrusted import loads, read_untrusted

MANIFEST = "files.json"
_SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._()-]{0,120}")


def _file_url(url: str) -> str:
    """The URL without a `::member` suffix (the harness's zip-member syntax)."""
    path_start = url.find("/", url.find("://") + 3)
    member = url.find("::", path_start) if path_start >= 0 else -1
    return url if member < 0 else url[:member]


def drop_names(urls: list[str]) -> list[tuple[str, str]]:
    """(file URL, name under incoming/) per distinct file: the URL's last path segment when it looks like a plain
    unique file name (an extension, no query string), else `file-<index>`."""
    files = list(dict.fromkeys(_file_url(url) for url in urls))
    wanted = []
    for url in files:
        parts = urlsplit(url)
        name = unquote(parts.path.rsplit("/", 1)[-1]) if not parts.query else ""
        wanted.append(name if "." in name.strip(".") and _SAFE_NAME.fullmatch(name) else "")
    counts = {name: wanted.count(name) for name in wanted}
    return [
        (url, name if name and counts[name] == 1 else f"file-{i}")
        for i, (url, name) in enumerate(zip(files, wanted, strict=True))
    ]


def _member_name(member: object) -> bool:
    """Whether a zip directory's member name is a plain relative path: it becomes a path under a person's folder."""
    return (
        isinstance(member, str)
        and member.isprintable()
        and not member.startswith("/")
        and bool(PurePosixPath(member).parts)
        and ".." not in PurePosixPath(member).parts
    )


def cached_files(cache_dir: Path) -> list[tuple[str, Path]]:
    """(upstream name, cache entry) of every file a record's raw cache holds whole. An entry is the file's own
    bytes (a zip member inflated and CRC-checked) under a hash of its URL, so this names it again: a `files.json`
    URL by its drop name, a zip's member as `<zip name>/<member path>`, found through the directory the harness
    caches beside it. A zip the cache holds whole is listed as itself, without its members."""
    cache_dir = Path(cache_dir)
    manifest = {url: entry for url, entry in read_files_json(cache_dir).items() if isinstance(entry, dict)}
    names = dict(drop_names(list(manifest)))
    found = []
    for url, entry in manifest.items():
        validators = entry.get("validators")
        if not isinstance(validators, dict) or url not in names:
            continue
        if is_entry(whole := cache_dir / cache_key(url, validators)):
            found.append((names[url], whole))
            continue
        # ponytail: a zip directory over 64 MiB of JSON (about a million members) lists nothing; raise the limit then
        members = loads(read_untrusted(cache_dir, cache_key(f"{url}::", validators), 64 << 20))
        for member in members if isinstance(members, dict) else ():
            if _member_name(member) and is_entry(path := cache_dir / cache_key(f"{url}::{member}", validators)):
                found.append((f"{names[url]}/{member}", path))
    return found


def read_manifest(cache_dir: Path) -> dict:
    path = Path(cache_dir) / MANIFEST
    return json.loads(path.read_text()) if path.is_file() else {}


def merge_manifest(cache_dir: Path, entries: dict[str, dict]) -> None:
    """Add `entries` (url -> {validators, source, at}) to files.json, written atomically."""
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    merged = {**read_manifest(cache_dir), **entries}
    tmp = cache_dir / f".{MANIFEST}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(merged, indent=1, sort_keys=True) + "\n")
    os.replace(tmp, cache_dir / MANIFEST)
