"""What the page's lists are made of (docs/plans/2026-11-12-ui-domain-flow.md): one page of a list, the row that
joins a catalog record with everything the registry knows about its dataset, and the summary of a plan.

A list route called with `limit` answers one page (`page_query`, `paged`); without it the route answers its whole
list, which is what the workers call. Nothing here writes."""

from __future__ import annotations

import re
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .plan import NOT_PREPARED
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


def matches(text: str, values: Iterable[Any]) -> bool:
    """Whether a search (lower-cased) is in any of a row's values."""
    return any(text in str(value).lower() for value in values if value)


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
        found = [row for row in found if matches(page.text, haystack(row))]
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
        "levels_keys": sorted({key for asset in usable if (key := (asset["data"] or {}).get("levels_key"))}),
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


# --- a plan as the buffer designer shows it: one page of its rows, and the numbers over all of them ---------

# The planner's reason strings, grouped and in its order: the funnel's steps and the decision column.
RULE_GROUPS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(pattern), label)
    for pattern, label in (
        (r"^path not in", "path not selected"),
        (r"^excluded by path", "excluded by path"),
        (r"^excluded by id", "excluded by id"),
        (r"^dims ", "dims not selected"),
        (r"^modality ", "modality"),
        (r"^organism ", "organism"),
        (r"^license .*unknown", "licence unknown"),
        (r"^license ", "licence not allowed"),
        (r"^annotations required", "annotations required"),
        (r"^annotation types", "annotation types"),
        (r"^annotation source", "annotation source"),
        (r"^largest raw array under", "array below minimum"),
        (r"^no dataset folder", "no mia-data folder"),
        (r"^dedup", "duplicate DOI"),
        (r"^record unreadable", "record unreadable"),
        (r"^no patch for dims", "no patch for dims"),
        (r"^no array shape", "no array shape"),
        (r"^footprint", "footprint too large"),
        (r"^patch z", "too few z slices"),
        (r"^voxel size unknown", "voxel size unknown"),
        (rf"^{re.escape(NOT_PREPARED)}$", NOT_PREPARED),
    )
]
POOL = "pool"  # the decision group of a row the buffer would draw from


def rule_group(rule: str | None) -> str:
    """The group a planner reason belongs to; a reason no group knows is its own group."""
    return next((label for pattern, label in RULE_GROUPS if pattern.search(rule or "")), rule or "unknown")


def rule_rank(group: str) -> int:
    """Where a group comes in the planner's order (the pool first, an unknown group last)."""
    if group == POOL:
        return -1
    return next((index for index, (_, label) in enumerate(RULE_GROUPS) if label == group), len(RULE_GROUPS))


def _fits(row: dict) -> bool:
    return bool(row.get("fit") and row["fit"]["ok"])


def decision_of(row: dict) -> dict:
    """Why a plan row is in the pool or not: {group, reason}, the reason in the planner's own words."""
    if row["admitted"] and _fits(row):
        return {"group": POOL, "reason": row["fit"]["reason"]}
    if row["rule"] == NOT_PREPARED:
        return {"group": NOT_PREPARED, "reason": f"fits; dataset is {row.get('prepared') or 'unprepared'}"}
    reason = row["fit"]["reason"] if row["admitted"] and row.get("fit") else row["rule"]
    return {"group": rule_group(reason), "reason": reason}


PLAN_SORTS: dict[str, Callable[[dict], Any]] = {
    "dataset": lambda row: row["id"],
    "decision": lambda row: rule_rank(row["decision"]["group"]),
    "dims": lambda row: row.get("dims"),
    "modality": lambda row: row.get("modality_group"),
    "licence": lambda row: row["licence"],
    "state": lambda row: row.get("prepared"),
    "raw": lambda row: (row["raw"] or {}).get("bytes"),
    "buffers": lambda row: len(row["buffers"]) or None,
    "bytes": lambda row: row.get("bytes"),
    "weight": lambda row: row.get("weight") or None,
}


def _plan_haystack(row: dict) -> tuple:
    return (
        row["id"],
        row["title"],
        row.get("modality_group"),
        row["licence"],
        row["decision"]["group"],
        row.get("prepared"),
    )


def _shares(rows: Iterable[dict], value_of: Callable[[dict], float]) -> list[dict]:
    """Rows summed per modality, largest first, at most twelve: a bar chart's bars."""
    sums: dict[str, float] = {}
    for row in rows:
        label = row.get("modality_group") or "none"
        sums[label] = sums.get(label, 0) + value_of(row)
    ordered = sorted(sums.items(), key=lambda item: (-item[1], item[0]))[:12]
    return [{"label": label, "value": value} for label, value in ordered]


def plan_view(
    plan: dict,
    datasets: Mapping[str, dict],
    levels_key: str,
    page: Page,
    *,
    fit_only: bool = False,
    prepared_only: bool = False,
    raw_cached: bool = False,
    staged_geom: bool = False,
) -> dict:
    """{rows, total, summary} of a plan (`Plan.to_dict` with states). `datasets` maps a record to its dataset_row.
    The three registry toggles narrow the rows and the summary's numbers; the funnel is the plan's own and
    ignores them. `fit_only` and the page's search narrow only the rows and, the search, the datasets to prepare:
    a request takes a bounded number, and the search is how a person prepares a large plan in parts."""
    rows = []
    for row in sorted(plan["rows"], key=lambda row: row["path"]):
        known = datasets.get(row["id"]) or {}
        joined = {
            **row,
            **{name: known.get(name) for name in ("issue", "folder", "crops", "raw", "url")},
            "title": known.get("title") or "",
            "licence": known.get("licence"),
            "previews": known.get("previews") or [],
            "buffers": known.get("buffers") or [],
            "quarantined": bool(known.get("quarantined")),
            "staged_here": levels_key in (known.get("levels_keys") or []),
            "fits": _fits(row),
            "in_pool": bool(row["admitted"] and _fits(row)),
            "decision": decision_of(row),
        }
        if prepared_only and row.get("prepared") != "prepared":
            continue
        if raw_cached and (joined["raw"] or {}).get("state") != "ready":
            continue
        if staged_geom and not joined["staged_here"]:
            continue
        rows.append(joined)
    fitting = [row for row in rows if row["fits"]]
    pool = [row for row in rows if row["in_pool"]]

    def total(some: list[dict], name: str) -> int:
        return sum(row.get(name) or 0 for row in some)

    def count(state: str) -> int:
        return sum(1 for row in fitting if row.get("prepared") == state)

    groups: dict[str, int] = {}
    for rule, n in (plan.get("excluded") or {}).items():
        groups[rule_group(rule)] = groups.get(rule_group(rule), 0) + n
    for row in plan["rows"]:  # the plan's own rows: the toggles do not move the funnel
        if (group := decision_of(row)["group"]) != POOL:
            groups[group] = groups.get(group, 0) + 1
    remaining = plan.get("n_seen") or 0
    funnel: list[dict] = []
    for label, value in sorted(groups.items(), key=lambda item: rule_rank(item[0])):
        remaining -= value
        funnel.append({"label": label, "value": value, "remaining": remaining})
    funnel.append({"label": "in the pool", "value": sum(1 for row in plan["rows"] if row["admitted"] and _fits(row))})
    heaviest = sorted(pool, key=lambda row: -(row.get("weight") or 0))[:8]
    summary = {
        "fitting": len(fitting),
        "pool": len(pool),
        "prepared": count("prepared"),
        "in_progress": count("in-progress"),
        "needs_human": count("needs-human"),
        "unprepared": len(fitting) - count("prepared") - count("in-progress"),
        "not_prepared": sum(1 for row in plan["rows"] if row["rule"] == NOT_PREPARED),
        "bytes_fitting": total(fitting, "bytes"),
        "bytes_pool": total(pool, "bytes"),
        "bytes_to_fetch": total([row for row in fitting if (row["raw"] or {}).get("state") != "ready"], "bytes"),
        "dims": sorted({row["dims"] for row in fitting if row.get("dims")}),
        "capacity": total(fitting, "positions"),
        "top_share": max((row.get("weight") or 0 for row in pool), default=0),
        "funnel": funnel,
        "by_modality": _shares(fitting, lambda row: 1),
        "weight_by_modality": _shares(pool, lambda row: row.get("weight") or 0),
        "heaviest": [
            {name: row.get(name) for name in ("id", "modality_group", "positions", "channels", "weight")}
            for row in heaviest
        ],
        "to_prepare": sorted(
            row["id"]
            for row in fitting
            if row.get("prepared") not in ("prepared", "in-progress")
            and (not page.text or matches(page.text, _plan_haystack(row)))
        ),
    }
    shown = [row for row in rows if row["fits"]] if fit_only else rows
    found, matched = paged(shown, page, PLAN_SORTS, _plan_haystack)
    return {"rows": found, "total": matched, "summary": summary}
