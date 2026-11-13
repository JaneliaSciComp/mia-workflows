"""The page only requests routes that exist and take no token (read, consumer, or an action), holds no token
itself, and calls no stray host."""

import json
import re
import shutil
import subprocess
from importlib import resources

import pytest

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
    """Every path the page passes to api()/load()/pageOf(), as (method, path) with `${…}` replaced by x."""
    found = set()
    for call in re.finditer(r"\b(?:api|load|pageOf)\(\s*([\"'`])(/[^\"'`]*)\1", PAGE):
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
        "/catalog/datasets",
        "/subjects/buffer",
        "/subjects/prep",
        "/assets/raw",
        "/assets/staged",
        "/needs",
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
    for path in ("/datasets/x/actions", "/specs", "/specs/x/actions", "/prep", "/prep/x/actions"):
        assert ("POST", path) in requested and resolve("POST", path) == "action", path
    for path in ("/needs", "/subjects/buffer", "/subjects/prep"):
        assert ("GET", path) in requested, path
    assert "issues/new" not in PAGE  # nothing is started by opening a GitHub issue
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
    allowed = {
        "https://github.com/",
        "https://fonts.googleapis.com/",
        "http://www.w3.org/",
        "https://doi.org/",
    }  # doi: a link
    assert {h if h.endswith("/") else h + "/" for h in hosts} <= allowed


def test_page_names_the_revoked_states_and_the_pause() -> None:
    for needle in ('"revoked":', '"quarantined":', "Revoke the ticked", "paused by the operator"):
        assert needle in PAGE, needle


def test_the_docs_describe_the_system_as_it_is() -> None:
    """The Docs tab names buttons, never label commands, and explains nothing by what it replaced."""
    docs = PAGE[PAGE.index('<section data-view="docs"') : PAGE.index("</main>")]
    assert not re.search(r"\b(buffer|data|prep|autodev):[a-z]", docs)
    assert not re.search(r"\b(no longer|any more|anymore|used to|as before|previously|read-only log)\b", docs, re.I)
    for needle in ("Needs you", "Cancel the round", "Files are in place", "MiaMirror", "/stream"):
        assert needle in docs, needle


def test_page_has_a_decisions_tab() -> None:
    assert 'data-tab="decisions"' in PAGE and 'data-view="decisions"' in PAGE
    assert "decisions: viewDecisions" in PAGE
    for needle in ("Shadow", "agree", "awaiting a human", "overruled", 'pageOf("/decisions"'):
        assert needle in PAGE, needle


FAKE_DOM = """
class FakeNode {
  constructor(tag) { this.tag = tag; this.attrs = {}; this.kids = []; }
  setAttribute(name, value) { this.attrs[name] = value; }
  set className(value) { this.attrs.class = value; }
  addEventListener() {}
  append(...kids) { this.kids.push(...kids); }
}
const document = { createElement: tag => new FakeNode(tag), createElementNS: (space, tag) => new FakeNode(tag) };
const escaped = text => text.replace(/&/g, "&amp;").replace(/</g, "&lt;");
const html = node => typeof node === "string" ? escaped(node)
  : `<${node.tag}${Object.entries(node.attrs).map(([k, v]) => ` ${k}="${v}"`).join("")}>${node.kids.map(html).join("")}</${node.tag}>`;
"""


def rendered(*bodies: str) -> list[str]:
    """What the page's `markdown()` builds for each timeline body, as HTML text (a string child is a text node)."""
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    names = ("el", "svgEl", "GH_MARK", "ghMark", "link")
    helpers = "\n".join(
        line for line in PAGE.splitlines() if line.startswith(tuple(f"const {name} = " for name in names))
    )
    renderer = PAGE[PAGE.index("// ---- a timeline entry's markdown") : PAGE.index("function actionBar(")]
    script = (
        f"{FAKE_DOM}\n{helpers}\n{renderer}\n"
        'const bodies = JSON.parse(require("fs").readFileSync(0, "utf8"));\n'
        'console.log(JSON.stringify(bodies.map(body => markdown(body).map(html).join(""))));\n'
    )
    done = subprocess.run([node, "-e", script], input=json.dumps(bodies), capture_output=True, text=True, check=True)
    return json.loads(done.stdout)


def test_a_timeline_entry_is_rendered_as_markdown() -> None:
    stage, spec, plan, notes = rendered(
        "**Stage:** `buffer:planning`\n\n**Folders:** [Workspace](https://files.example/w) · [Staged](https://files.example/s)\n\n"
        "<!-- mia-buffers: stage i1-e2 planning -->",
        "**Action:** `go` by `page`\n\n````yaml\nname: x\nselect:\n  paths: [datasets/**]\n```\n**not bold**\n````\n",
        "1 in the pool.\n\n| id | weight |\n|---|---|\n| `rec-a` | 1.000 |\n| `rec-b` |  |\n\nafter",
        "## Why\nfirst line\nsecond *line*\n- one\n- two https://example.org/x\n\n> the masks\n> are shifted",
    )
    assert stage == (
        "<p><strong>Stage:</strong> <code>buffer:planning</code></p>"
        '<p><strong>Folders:</strong> <a href="https://files.example/w" target="_blank" rel="noopener">Workspace</a>'
        ' · <a href="https://files.example/s" target="_blank" rel="noopener">Staged</a></p>'
    )  # the marker comment is not shown
    assert spec == (
        "<p><strong>Action:</strong> <code>go</code> by <code>page</code></p>"
        "<pre><code>name: x\nselect:\n  paths: [datasets/**]\n```\n**not bold**</code></pre>"
    )  # a fence closes only on one as long as it opened; nothing inside it is markdown
    assert plan == (
        '<p>1 in the pool.</p><div class="tablewrap"><table class="plain">'
        "<thead><tr><th>id</th><th>weight</th></tr></thead>"
        "<tbody><tr><td><code>rec-a</code></td><td>1.000</td></tr><tr><td><code>rec-b</code></td><td></td></tr></tbody>"
        "</table></div><p>after</p>"
    )
    assert notes == (
        "<h4>Why</h4><p>first line<br></br>second <em>line</em></p>"
        '<ul><li>one</li><li>two <a href="https://example.org/x" target="_blank" rel="noopener">https://example.org/x</a></li></ul>'
        "<blockquote><p>the masks<br></br>are shifted</p></blockquote>"
    )


def test_a_link_to_github_carries_the_github_mark() -> None:
    (body,) = rendered(
        "see [the PR](https://github.com/o/r/pull/3) and https://github.com/o/r/issues/4 "
        "but not https://github.com.evil.example/x"
    )
    assert body.count("<svg") == 2
    assert '<a href="https://github.com/o/r/pull/3" target="_blank" rel="noopener"><svg' in body
    assert "</svg> the PR</a>" in body
    assert "</svg> https://github.com/o/r/issues/4</a>" in body
    assert '<a href="https://github.com.evil.example/x" target="_blank" rel="noopener">https://github.com.evil' in body


def test_a_github_issue_is_the_mark_with_a_tooltip() -> None:
    """No link to GitHub is spelled out in words ("log on GitHub"): it is the mark, and the tooltip says what it is."""
    assert "log on GitHub" not in PAGE and "ghIssue" not in PAGE
    assert "a read-only copy of the timeline" in PAGE


def test_markdown_never_becomes_html_or_a_script_link() -> None:
    """A timeline body can carry text from a sandboxed job: tags stay text, and only http(s) links are links."""
    (unsafe,) = rendered("<img src=x onerror=alert(1)> [click](javascript:alert(1)) <script>alert(2)</script>")
    assert (
        unsafe == "<p>&lt;img src=x onerror=alert(1)> [click](javascript:alert(1)) &lt;script>alert(2)&lt;/script></p>"
    )


def lifecycle(*calls: list) -> list[str]:
    """What the page's `lifecycle()` builds for each (line, stage, history), as HTML text."""
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    helpers = "\n".join(line for line in PAGE.splitlines() if line.startswith(("const el = ", "const BAD = ")))
    drawer = PAGE[PAGE.index("// ---- a lifecycle as boxes and arrows") : PAGE.index("// ---- buffers")]
    script = (
        f"{FAKE_DOM}\n{helpers}\n{drawer}\n"
        'const calls = JSON.parse(require("fs").readFileSync(0, "utf8"));\n'
        "console.log(JSON.stringify(calls.map(call => html(lifecycle(...call)))));\n"
    )
    done = subprocess.run([node, "-e", script], input=json.dumps(calls), capture_output=True, text=True, check=True)
    return json.loads(done.stdout)


def test_a_lifecycle_marks_the_current_stage_on_its_line() -> None:
    """The steps before the current one are past, it alone is current; a stage off the line hangs under the step
    its round stopped at, and stands apart at the end when the history names no step."""
    line = ["new", "testing", "review", "done"]
    on, stopped, unknown = lifecycle(
        [line, "review", []], [line, "needs-human", ["needs-human", "testing", "new"]], [line, "cancelled", [None]]
    )
    step = '<span class="step">{}</span>'.format
    assert on == (
        '<ol class="flow" aria-label="lifecycle">'
        f'<li class="past">{step("new")}</li><li class="past">{step("testing")}</li>'
        f'<li class="now" aria-current="step">{step("review")}</li><li>{step("done")}</li></ol>'
    )
    assert stopped == (
        '<ol class="flow" aria-label="lifecycle">'
        f'<li class="past">{step("new")}</li>'
        f'<li class="past">{step("testing")}<span class="step off stuck" aria-current="step">needs-human</span></li>'
        f"<li>{step('review')}</li><li>{step('done')}</li></ol>"
    )
    assert unknown == (
        '<ol class="flow" aria-label="lifecycle">'
        + "".join(f"<li>{step(name)}</li>" for name in line)
        + '<li class="apart"><span class="step off" aria-current="step">cancelled</span></li></ol>'
    )
    assert PAGE.count("lifecycle(") == 4  # the function, a subject's page (twice: one never asked of) and a buffer's


def test_a_buffer_is_always_planned_first() -> None:
    """The page has no tick for the plan: every go it sends for a buffer stops at the plan, and Run continues it."""
    assert "Show me the plan first" not in PAGE and "first.checked" not in PAGE
    assert PAGE.count("plan_first: true") == 2  # New buffer, and a spec's own page
    assert 'subject.stage === "plan-review" ? "Run"' in PAGE


def test_an_asked_action_holds_the_buttons_until_its_workflow_answers() -> None:
    """A click is not shown as the old stage's buttons again: while the subject says an action is `asked`, every
    button that would send an action is disabled, and a reload that started earlier never paints over a later one."""
    assert PAGE.count("disabled: !!subject.asked") == 4 and "disabled: !!x.asked" in PAGE
    assert "waiting for its workflow to answer" in PAGE
    assert "turn !== subjectTurn" in PAGE


def test_reject_asks_for_its_feedback_in_a_dialog() -> None:
    """Reject has more to enter: its button ends in an ellipsis and opens a dialog for the feedback. No feedback box
    sits beside the buttons, and a rejection with nothing typed cannot be sent."""
    assert 'reject: "Reject…"' in PAGE and "showModal()" in PAGE
    assert "bar.append(note)" not in PAGE
    assert "Send rejection" in PAGE and "sendButton.disabled = !note.value.trim()" in PAGE


def test_a_datasets_page_leads_with_its_links_then_its_preparation_workflow() -> None:
    """Under the name come the dataset's links, then the lifecycle labelled as the preparation workflow, then the
    buttons; the button that starts the workflow says Start, and no state pill stands between them."""
    assert 'dataset: "Preparation workflow:"' in PAGE and "flowRow(kind, " in PAGE
    assert '(subject.again ? "Start again" : "Start")' in PAGE and '"Prepare again"' not in PAGE
    assert "statePill(detail ? detail.row.state" not in PAGE
    assert "const [links = null, ...cards] = " in PAGE
