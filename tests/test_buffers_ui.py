"""The page only requests routes that exist and take no token (read, consumer, or an action), holds no token
itself, and calls no stray host."""

import re
from importlib import resources

from mia_buffers.api import ROUTES

PAGE = (resources.files("mia_buffers") / "ui" / "index.html").read_text()


def resolve(method: str, path: str) -> str | None:
    """The capability of the route that serves `method path`, or None when no route does."""
    segments = path.split("?")[0].split("/")
    filled = [index for index, segment in enumerate(segments) if segment == "x"]
    for numbers in range(1 << len(filled)):  # a `${…}` stands for a name or for a number: try both in each place
        candidate = list(segments)
        for bit, index in enumerate(filled):
            if numbers >> bit & 1:
                candidate[index] = "1"
        for route_method, pattern, _name, capability in ROUTES:
            if route_method == method and pattern.match("/".join(candidate)):
                return capability
    return None


def requested_paths() -> set[tuple[str, str]]:
    """Every path the page passes to api()/load(), as (method, path) with `${…}` replaced by x."""
    found = set()
    for call in re.finditer(r"\b(?:api|load)\(\s*([\"'`])(/[^\"'`]*)\1", PAGE):
        path = re.sub(r"\$\{[^}]*\}", "x", call.group(2))
        options = re.match(r"\s*,\s*\{\s*method:\s*\"(\w+)\"", PAGE[call.end() :])
        method = options.group(1) if options else "GET"
        found.add((method, path))
    return found


def test_resolver_rejects_a_bogus_path() -> None:
    assert resolve("GET", "/no/such/route") is None
    assert resolve("GET", "/buffers") == "read"
    assert resolve("PUT", "/keeper") == "worker"


def test_every_path_the_page_requests_is_an_open_route() -> None:
    requested = requested_paths()
    assert {path for _, path in requested} >= {
        "/plan",
        "/meta",
        "/catalogs",
        "/datasets",
        "/buffers",
        "/keeper",
        "/locks",
        "/decisions",
    }
    assert ("POST", "/plan") in requested
    assert resolve("GET", "/crops/folder/crop/overlay.png") == "read"  # the thumbnails' <img src>, not an api() call
    for method, path in requested:
        assert resolve(method, path) in {"read", "consumer", "action"}, (method, path)


def test_the_pages_buttons_call_action_routes_and_it_follows_the_stream() -> None:
    """spec webapp-actions §8: a button is a POST to an action route; the page is told what changed."""
    requested = requested_paths()
    assert ("POST", "/datasets/x/actions") in requested and resolve("POST", "/datasets/x/actions") == "action"
    for path in ("/subjects/x/x", "/timeline/x/x", "/run/x/x", "/lookup?q=x", "/health"):
        assert ("GET", path) in requested, path
    assert 'new EventSource("/stream")' in PAGE and resolve("GET", "/stream") == "read"
    # a click is one key (Retry sends it again) and a verdict names the revision the page is showing
    assert "key: newKey()" in PAGE and "revision: subject.revision" in PAGE and "key: a.key" in PAGE
    assert '"Content-Type": "application/json"' in PAGE  # the action routes take nothing else
    assert "innerHTML" not in PAGE  # timeline text is inserted as text, never parsed


def test_page_holds_no_token_and_calls_only_known_hosts() -> None:
    assert not re.search(r"authorization|bearer", PAGE, re.I)
    assert not re.search(r"https?://\$\{", PAGE), "a host built from a template could be any host"
    assert len(re.findall(r"\bfetch\(", PAGE)) == 1, "every request goes through api()"
    hosts = set(re.findall(r"https?://[^/\"'`$\s)]+/?", PAGE))
    allowed = {"https://github.com/", "https://fonts.googleapis.com/", "http://www.w3.org/"}
    assert {h if h.endswith("/") else h + "/" for h in hosts} <= allowed


def test_page_names_the_new_states_and_labels() -> None:
    for needle in ('"revoked":', '"quarantined":', "buffer:revoke", "data:revoke", "paused by the operator"):
        assert needle in PAGE, needle


def test_page_has_a_decisions_tab() -> None:
    assert 'data-tab="decisions"' in PAGE and 'data-view="decisions"' in PAGE
    assert "decisions: viewDecisions" in PAGE
    for needle in ("Shadow", "agree", "awaiting a human", "load older"):
        assert needle in PAGE, needle
