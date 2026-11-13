"""What the page's lists are made of: one page of a list, the dataset join, the plan's summary."""

import pytest

from mia_buffers.explore import Page, page_query, paged

ROWS = [
    {"id": "b", "bytes": 5, "state": "ready"},
    {"id": "a", "bytes": None, "state": "evicted"},
    {"id": "c", "bytes": 9, "state": "ready"},
    {"id": "d", "state": "fetching"},
]
KEYS = {"id": lambda row: row["id"], "bytes": lambda row: row.get("bytes")}


def test_no_limit_means_the_whole_list_as_before() -> None:
    assert page_query({}, KEYS) is None
    assert page_query({"sort": "id"}, KEYS) is None  # a worker's call never names a limit


def test_page_query_reads_and_checks_its_fields() -> None:
    page = page_query({"limit": "2", "offset": "4", "sort": "bytes", "dir": "desc", "q": " Ready "}, KEYS)
    assert page == Page(limit=2, offset=4, sort="bytes", descending=True, text="ready")
    assert page_query({"limit": "50"}, KEYS) == Page(limit=50, offset=0, sort=None, descending=False, text="")
    for bad in ({"limit": "0"}, {"limit": "201"}, {"limit": "x"}, {"limit": "5", "offset": "-1"}):
        with pytest.raises(ValueError):
            page_query(bad, KEYS)
    with pytest.raises(ValueError, match="sort must be one of"):
        page_query({"limit": "5", "sort": "nope"}, KEYS)
    with pytest.raises(ValueError, match="dir must be asc or desc"):
        page_query({"limit": "5", "dir": "up"}, KEYS)


def ids(rows: list[dict]) -> str:
    return "".join(row["id"] for row in rows)


def test_a_page_is_sorted_cut_and_counted() -> None:
    rows, total = paged(ROWS, Page(2, 0, "id", False, ""), KEYS)
    assert (ids(rows), total) == ("ab", 4)
    rows, total = paged(ROWS, Page(2, 2, "id", False, ""), KEYS)
    assert (ids(rows), total) == ("cd", 4)
    assert paged(ROWS, Page(2, 9, "id", False, ""), KEYS) == ([], 4)  # past the end: empty, the total still true
    assert ids(paged(ROWS, Page(9, 0, None, False, ""), KEYS)[0]) == "bacd"  # no sort: the list's own order


def test_a_missing_value_sorts_last_in_either_direction() -> None:
    assert ids(paged(ROWS, Page(9, 0, "bytes", False, ""), KEYS)[0]) == "bcad"
    assert ids(paged(ROWS, Page(9, 0, "bytes", True, ""), KEYS)[0]) == "cbad"


def test_search_matches_any_value_of_the_haystack_without_case() -> None:
    haystack = lambda row: (row["id"], row.get("state"))  # noqa: E731
    rows, total = paged(ROWS, Page(9, 0, "id", False, "read"), KEYS, haystack)
    assert (ids(rows), total) == ("bc", 2)
    assert paged(ROWS, Page(9, 0, "id", False, "zzz"), KEYS, haystack) == ([], 0)


def test_mixed_types_in_a_column_do_not_break_the_sort() -> None:
    mixed = [{"id": "a", "bytes": "12"}, {"id": "b", "bytes": 3}]
    assert ids(paged(mixed, Page(9, 0, "bytes", False, ""), KEYS)[0]) == "ba"  # numbers before text


# --- a plan as the designer shows it: one page of its rows and the numbers over all of them

from mia_buffers.explore import decision_of, plan_view  # noqa: E402


def plan_row(record: str, **fields) -> dict:
    row = {
        "id": record,
        "path": f"p/{record}",
        "dims": "2D",
        "modality_group": "TEM",
        "admitted": True,
        "rule": "selected",
    }
    row |= {"fit": {"ok": True, "reason": "fits"}, "positions": 0, "weight": 0.0, "bytes": None, "channels": 1}
    return row | {"prepared": "prepared", "splits": {}} | fields


PLAN = {
    "rows": [
        plan_row("c", admitted=False, rule="not prepared", prepared="unprepared", bytes=30, positions=7),
        plan_row("a", positions=10, weight=0.75, bytes=100),
        plan_row("e", admitted=False, rule="record unreadable: boom", fit=None, prepared="unprepared"),
        plan_row("b", positions=5, weight=0.25, bytes=50, modality_group="confocal"),
        plan_row(
            "d", fit={"ok": False, "reason": "footprint 4096 px exceeds the 1024 px array"}, prepared="needs-human"
        ),
    ],
    "excluded": {"dims 3D not in ['2D']": 4, "license unknown and allow_unknown is false": 2},
    "n_seen": 11,
}
DATASETS = {
    "a": {
        "title": "Alpha",
        "licence": "CC0-1.0",
        "issue": 7,
        "raw": {"state": "ready", "bytes": 9},
        "levels_keys": ["p256-l1-2"],
    },
    "b": {"title": "Beta", "licence": "MIT", "raw": {"state": "fetching", "bytes": None}, "levels_keys": []},
}
ALL = Page(50, 0, None, False, "")


def test_a_row_decision_is_its_first_reason_in_the_planners_words() -> None:
    by_id = {row["id"]: decision_of(row) for row in PLAN["rows"]}
    assert by_id["a"] == {"group": "pool", "reason": "fits"}
    assert by_id["c"] == {"group": "not prepared", "reason": "fits; dataset is unprepared"}
    assert by_id["d"]["group"] == "footprint too large" and by_id["e"]["group"] == "record unreadable"
    assert decision_of(plan_row("x", admitted=False, rule="something new", fit=None))["group"] == "something new"


def test_the_plan_summary_counts_over_every_row() -> None:
    summary = plan_view(PLAN, DATASETS, "p256-l1-2", ALL)["summary"]
    assert (summary["fitting"], summary["pool"], summary["prepared"], summary["in_progress"]) == (3, 2, 2, 0)
    assert (summary["unprepared"], summary["needs_human"], summary["not_prepared"]) == (1, 0, 1)
    assert (summary["bytes_fitting"], summary["bytes_pool"], summary["bytes_to_fetch"]) == (180, 150, 80)
    assert (summary["dims"], summary["capacity"], summary["top_share"]) == (["2D"], 22, 0.75)
    assert [(step["label"], step["value"], step.get("remaining")) for step in summary["funnel"]] == [
        ("dims not selected", 4, 7),
        ("licence unknown", 2, 5),
        ("record unreadable", 1, 4),
        ("footprint too large", 1, 3),
        ("not prepared", 1, 2),
        ("in the pool", 2, None),
    ]
    assert summary["by_modality"] == [{"label": "TEM", "value": 2}, {"label": "confocal", "value": 1}]
    assert summary["weight_by_modality"] == [{"label": "TEM", "value": 0.75}, {"label": "confocal", "value": 0.25}]
    assert [(row["id"], row["weight"]) for row in summary["heaviest"]] == [("a", 0.75), ("b", 0.25)]
    assert summary["to_prepare"] == ["c"]
    # what a request could start is read from the stage: a dataset in a round or at review is not offered, whatever
    # its state says; one that stopped is
    for stage, offered in (("review", []), ("testing", []), ("needs-human", ["c"]), ("cancelled", ["c"])):
        waiting = plan_view(PLAN, {**DATASETS, "c": {"stage": stage}}, "p256-l1-2", ALL)["summary"]
        assert waiting["to_prepare"] == offered, stage


def test_the_plan_rows_are_joined_sorted_and_cut() -> None:
    view = plan_view(PLAN, DATASETS, "p256-l1-2", ALL)
    assert [row["id"] for row in view["rows"]] == ["a", "b", "c", "d", "e"] and view["total"] == 5  # the plan's order
    by_decision = plan_view(PLAN, DATASETS, "p256-l1-2", Page(50, 0, "decision", False, ""))
    assert [row["id"] for row in by_decision["rows"]] == ["a", "b", "e", "d", "c"]  # the pool, then the planner's rules
    first = view["rows"][0]
    assert (first["title"], first["licence"], first["issue"], first["staged_here"]) == ("Alpha", "CC0-1.0", 7, True)
    assert first["fits"] and first["in_pool"] and first["decision"]["group"] == "pool"
    assert view["rows"][2]["title"] == "" and view["rows"][2]["raw"] is None  # a record the registry knows nothing of
    page = plan_view(PLAN, DATASETS, "p256-l1-2", Page(2, 2, "dataset", False, ""))
    assert [row["id"] for row in page["rows"]] == ["c", "d"] and page["total"] == 5
    assert [row["id"] for row in plan_view(PLAN, DATASETS, "p256-l1-2", Page(50, 0, None, False, "beta"))["rows"]] == [
        "b"
    ]
    fit = plan_view(PLAN, DATASETS, "p256-l1-2", ALL, fit_only=True)
    assert [row["id"] for row in fit["rows"]] == ["a", "b", "c"] and fit["summary"]["fitting"] == 3


def test_the_registry_toggles_narrow_the_rows_and_the_numbers_but_not_the_funnel() -> None:
    prepared = plan_view(PLAN, DATASETS, "p256-l1-2", ALL, prepared_only=True)
    assert [row["id"] for row in prepared["rows"]] == ["a", "b"] and prepared["summary"]["fitting"] == 2
    assert prepared["summary"]["funnel"] == plan_view(PLAN, DATASETS, "p256-l1-2", ALL)["summary"]["funnel"]
    assert [row["id"] for row in plan_view(PLAN, DATASETS, "p256-l1-2", ALL, raw_cached=True)["rows"]] == ["a"]
    assert [row["id"] for row in plan_view(PLAN, DATASETS, "p256-l1-2", ALL, staged_geom=True)["rows"]] == ["a"]
    assert plan_view(PLAN, DATASETS, "p512-l1", ALL, staged_geom=True)["rows"] == []


def test_the_datasets_to_prepare_follow_the_search() -> None:
    """A request takes a bounded number of datasets: the search box is how a person prepares a large plan in parts."""
    assert plan_view(PLAN, DATASETS, "p256-l1-2", Page(50, 0, None, False, "zzz"))["summary"]["to_prepare"] == []
    assert plan_view(PLAN, DATASETS, "p256-l1-2", Page(50, 0, None, False, "not prepared"))["summary"][
        "to_prepare"
    ] == ["c"]
    assert (
        plan_view(PLAN, DATASETS, "p256-l1-2", Page(50, 0, None, False, "zzz"))["summary"]["fitting"] == 3
    )  # numbers stay
