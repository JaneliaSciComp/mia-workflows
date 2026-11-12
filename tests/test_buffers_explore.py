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
