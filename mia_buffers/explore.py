"""What the page's lists are made of (docs/plans/2026-11-12-ui-domain-flow.md): one page of a list, the row that
joins a catalog record with everything the registry knows about its dataset, and the summary of a plan.

A list route called with `limit` answers one page (`page_query`, `paged`); without it the route answers its whole
list as before, which is what the workers call. Nothing here writes."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .specmap import facts

PAGE_LIMIT = 200  # the most rows one page may ask for


@dataclass(frozen=True)
class Page:
    """Which rows of a list the page asks for: `text` is the search, lower-cased; `sort` None keeps the list's order."""

    limit: int
    offset: int
    sort: str | None
    descending: bool
    text: str


def _whole(query: Mapping[str, str], name: str, default: int, low: int, high: int) -> int:
    try:
        value = int(query.get(name, default))
    except ValueError:
        raise ValueError(f"query {name} must be an integer") from None
    if not low <= value <= high:
        raise ValueError(f"query {name} must be between {low} and {high}, got {value}")
    return value


def page_query(query: Mapping[str, str], sorts: Collection[str]) -> Page | None:
    """The page a query asks for, or None when it names no `limit` (the whole list). ValueError says what is wrong."""
    if "limit" not in query:
        return None
    sort, direction = query.get("sort") or None, query.get("dir", "asc")
    if sort is not None and sort not in sorts:
        raise ValueError(f"query sort must be one of {sorted(sorts)}, got {sort!r}")
    if direction not in ("asc", "desc"):
        raise ValueError(f"query dir must be asc or desc, got {direction!r}")
    return Page(
        limit=_whole(query, "limit", 50, 1, PAGE_LIMIT),
        offset=_whole(query, "offset", 0, 0, 10**9),
        sort=sort,
        descending=direction == "desc",
        text=query.get("q", "").strip().lower(),
    )


def _rank(value: Any) -> tuple[int, Any]:
    """A sort key that never compares a number with text: numbers, then text (case-folded)."""
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, int | float):
        return (0, value)
    return (1, str(value).lower())


def paged(
    rows: Iterable[dict],
    page: Page,
    keys: Mapping[str, Callable[[dict], Any]],
    haystack: Callable[[dict], Iterable[Any]] | None = None,
) -> tuple[list[dict], int]:
    """One page of `rows` and how many rows matched the search. `keys` maps a sort name to the row's value for it
    (None sorts last in either direction); `haystack` gives the values the search looks in."""
    # ponytail: sorts and cuts the whole list in Python; SQL LIMIT/OFFSET when a list reaches tens of thousands
    found = list(rows)
    if page.text and haystack is not None:
        found = [row for row in found if any(page.text in str(value).lower() for value in haystack(row) if value)]
    if page.sort is not None:
        value_of = keys[page.sort]
        present = [row for row in found if value_of(row) is not None]
        present.sort(key=lambda row: _rank(value_of(row)), reverse=page.descending)
        found = present + [row for row in found if value_of(row) is None]
    return found[page.offset : page.offset + page.limit], len(found)


# --- a dataset as the page lists it: its catalog record joined with what the registry knows -----------------

UNUSABLE = ("evicted", "missing")  # a staged artifact in one of these states is not on disk


def record_row(path: str | None, record: dict) -> dict:
    """What a dataset's row takes from its catalog record; it never changes for a catalog ref. `path` None: a
    dataset the registry knows whose record is in no synced catalog."""
    row: dict[str, Any] = {
        "id": str(record.get("id") or (path or "").rsplit("/", 1)[-1].removesuffix(".yaml")),
        "path": path,
        "in_catalog": path is not None,
        "title": str(record.get("title") or ""),
        "url": record.get("landing_url"),
        "dims": None,
        "modality": None,
        "licence": None,
        "bytes": None,
    }
    if path is not None:
        try:
            found = facts(path, record)
        except (TypeError, ValueError, AttributeError):  # an unreadable record is still a dataset with a name
            return row
        row.update(dims=found.dims, modality=found.modality_group, licence=found.license_spdx, bytes=found.size_bytes)
    return row


def dataset_row(static: dict, dataset: dict | None, raw: dict | None, staged: list[dict]) -> dict:
    """One dataset: `static` (record_row) with its preparation row, its raw download and its staged artifacts
    (asset rows: {key, data, refs})."""
    dataset = dataset or {}
    usable = [asset for asset in staged if (asset["data"] or {}).get("state") not in UNUSABLE]
    refs = {buffer for asset in ([raw] if raw else []) + staged for buffer in asset.get("refs") or []}
    return {
        **static,
        "state": dataset.get("state", "unprepared"),
        "stage": dataset.get("data_stage"),
        "reason": dataset.get("reason"),
        "issue": dataset.get("issue"),
        "updated_at": dataset.get("updated_at"),
        "folder": dataset.get("folder"),
        "previews": dataset.get("previews") or [],
        "crops": dataset.get("crops"),
        "raw": {"state": raw["data"].get("state"), "bytes": raw["data"].get("bytes")} if raw and raw["data"] else None,
        "staged": len(usable),
        "quarantined": any((asset["data"] or {}).get("quarantined") for asset in staged),
        "buffers": sorted(refs),
    }


DATASET_FILTERS = ("state", "modality", "dims", "licence")  # a query names one value of the row's field

DATASET_SORTS: dict[str, Callable[[dict], Any]] = {
    "dataset": lambda row: row["id"],
    "title": lambda row: row["title"] or None,
    "state": lambda row: row["state"],
    "stage": lambda row: row["stage"],
    "dims": lambda row: row["dims"],
    "modality": lambda row: row["modality"],
    "licence": lambda row: row["licence"],
    "bytes": lambda row: row["bytes"],
    "raw": lambda row: (row["raw"] or {}).get("bytes"),
    "staged": lambda row: row["staged"] or None,
    "buffers": lambda row: len(row["buffers"]) or None,
    "crops": lambda row: row["crops"],
    "updated": lambda row: row["updated_at"],
}


def dataset_haystack(row: dict) -> tuple:
    return (row["id"], row["title"], row["modality"], row["licence"], row["dims"], row["state"], row["stage"])


def facets(rows: Iterable[dict]) -> dict[str, dict[str, int]]:
    """How many datasets have each value of each filter: what the page's filter boxes offer."""
    counts: dict[str, dict[str, int]] = {name: {} for name in DATASET_FILTERS}
    for row in rows:
        for name in DATASET_FILTERS:
            if (value := row[name]) is not None:
                counts[name][value] = counts[name].get(value, 0) + 1
    return {name: dict(sorted(values.items())) for name, values in counts.items()}
