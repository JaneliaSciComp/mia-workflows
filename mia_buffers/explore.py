"""What the page's lists are made of (docs/plans/2026-11-12-ui-domain-flow.md): one page of a list, the row that
joins a catalog record with everything the registry knows about its dataset, and the summary of a plan.

A list route called with `limit` answers one page (`page_query`, `paged`); without it the route answers its whole
list as before, which is what the workers call. Nothing here writes."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

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
