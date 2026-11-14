"""The page only requests routes that exist and take no token (read, consumer, or an action), holds no token
itself, and calls no stray host."""

import json
import os
import re
import shutil
import subprocess
import sys
from importlib import resources
from pathlib import Path
from typing import Any

from mia_buffers.api import ROUTES

# every file the browser is given, by name, and all of them as one text: what the page is, for a rule about it
UI_DIR = Path(str(resources.files("mia_buffers") / "ui")).resolve()
FILES = {
    entry.name: entry.read_text() for entry in sorted(UI_DIR.iterdir()) if entry.suffix in (".html", ".js", ".css")
}
PAGE = "\n".join(FILES.values())


def squeezed(text: str) -> str:
    """`text` as it reads whatever its layout: without whitespace, and without the comma a formatter puts before a
    closing bracket. Source is compared this way, so a rule about the page's code does not break when it is laid
    out differently."""
    return re.sub(r",(?=[)\]}])", "", re.sub(r"\s+", "", text))


SQUEEZED = squeezed(PAGE)


def has(code: str, text: str = SQUEEZED) -> bool:
    """Whether the page's source (or `text`, already squeezed) holds `code`, whatever the layout of either."""
    return squeezed(code) in text


def count(code: str, text: str = SQUEEZED) -> int:
    """How many times the page's source (or `text`, already squeezed) holds `code`, whatever the layout."""
    return text.count(squeezed(code))


# node of the environment that runs the tests (pixi provides it), before any other on the PATH
NODE = shutil.which("node", path=os.pathsep.join([str(Path(sys.executable).parent), os.environ.get("PATH", "")]))


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
    assert resolve("GET", f"/gallery/i1-e2/{'a' * 40}/crop/raw.png") == "read"  # a round's crops in its timeline
    for method, path in requested:
        assert resolve(method, path) in {"read", "consumer", "action"}, (method, path)


def test_the_pages_buttons_call_action_routes_and_it_follows_the_stream() -> None:
    """spec webapp-actions §8: a button is a POST to an action route; the page is told what changed."""
    requested = requested_paths()
    for path in ("/datasets/x/actions", "/specs", "/specs/x/actions", "/prep", "/prep/x/actions"):
        assert ("POST", path) in requested and resolve("POST", path) == "action", path
    for path in ("/needs", "/subjects/buffer", "/subjects/prep"):
        assert ("GET", path) in requested, path
    assert not has("issues/new")  # nothing is started by opening a GitHub issue
    for path in ("/subjects/x/x", "/timeline/x/x", "/run/x/x", "/lookup?q=x", "/health"):
        assert ("GET", path) in requested, path
    assert has('new EventSource("/stream")') and resolve("GET", "/stream") == "read"
    # a click is one key (Retry sends it again) and a verdict names the revision the page is showing
    assert has("key: newKey()") and has("revision: subject.revision") and has("key: a.key")
    assert has('"Content-Type": "application/json"')  # the action routes take nothing else
    assert not has("innerHTML")  # timeline text is inserted as text, never parsed


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
        assert has(needle), needle


def test_the_docs_describe_the_system_as_it_is() -> None:
    """The Docs tab names buttons, never label commands, and explains nothing by what it replaced."""
    docs = FILES["guide.html"] + FILES["architecture.html"]
    assert has("send(`/ui/${doc}.html`)") and resolve("GET", "/ui/guide.html") == "read"
    assert not re.search(r"\b(buffer|data|prep|autodev):[a-z]", docs)
    assert not re.search(r"\b(no longer|any more|anymore|used to|as before|previously|read-only log)\b", docs, re.I)
    for needle in ("Needs you", "Cancel the round", "Files are in place", "MiaMirror", "/stream"):
        assert needle in docs, needle


def test_page_has_a_decisions_tab() -> None:
    assert has('data-tab="decisions"') and has('data-view="decisions"')
    assert has("decisions: viewDecisions")
    for needle in ("Shadow", "agree", "awaiting a human", "overruled", 'pageOf("/decisions"'):
        assert has(needle), needle


# A DOM small enough to read. The page's modules build nodes with `el()`, so a test sees what they built: `html`
# writes a node as text, `walk`, `textOf` and `byText` look into one, and `fire` and `close` stand for a person.
FAKE_DOM = """
class FakeNode {
  constructor(tag) { this.tag = tag; this.attrs = {}; this.kids = []; this.listeners = {}; this.dataset = {}; this.checked = false; this.disabled = false; }
  setAttribute(name, value) { this.attrs[name] = value; if (name.startsWith("data-")) this.dataset[name.slice(5)] = value; }
  set className(value) { this.attrs.class = value; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  fire(type) { (this.listeners[type] || []).forEach(fn => fn({ target: this })); }
  append(...kids) { for (const kid of kids) { if (kid instanceof FakeNode) kid.parent = this; this.kids.push(kid); } }
  prepend(...kids) { const rest = this.kids; this.kids = []; this.append(...kids, ...rest); }
  replaceChildren(...kids) { this.kids = []; this.append(...kids); }
  remove() { if (this.parent) this.parent.kids = this.parent.kids.filter(kid => kid !== this); }
  get children() { return this.kids.filter(kid => kid instanceof FakeNode); }
  get firstChild() { return this.kids[0]; }
  querySelector(selector) { return walk(this).slice(1).find(node => (node.attrs.class || "").split(" ").includes(selector.slice(1))) || null; }
  showModal() {}
  close(value) { this.returnValue = value; this.fire("close"); }
}
const nodes = {};
globalThis.document = {
  body: new FakeNode("body"), createElement: tag => new FakeNode(tag), createElementNS: (space, tag) => new FakeNode(tag),
  getElementById: id => (nodes[id] ||= new FakeNode("#" + id)),
};
const walk = (node, found = []) => { if (typeof node === "string") return found; found.push(node); node.kids.forEach(kid => walk(kid, found)); return found; };
const textOf = node => typeof node === "string" ? node : node.kids.map(textOf).join("");
const byText = (root, text) => walk(root).find(node => node.tag === "button" && textOf(node) === text);
const escaped = text => text.replace(/&/g, "&amp;").replace(/</g, "&lt;");
const html = node => typeof node === "string" ? escaped(node)
  : `<${node.tag}${Object.entries(node.attrs).map(([k, v]) => ` ${k}="${v}"`).join("")}>${node.kids.map(html).join("")}</${node.tag}>`;
"""


def run_js(body: str, imports: dict[str, str], payload: object = None) -> Any:
    """Run `body` under node as the browser would run it: the page's own modules, imported by file name (`imports`
    maps a file to the names taken from it), over the fake DOM. The body reads `input` (the payload) and calls
    `print(value)` once; that value is returned."""
    assert NODE, "node is a test dependency: pixi provides it (pyproject.toml)"
    loads = "".join(
        f'const {{ {names} }} = await import("{(UI_DIR / file).as_uri()}");\n' for file, names in imports.items()
    )
    script = (
        'import { readFileSync } from "node:fs";\n'
        f"{FAKE_DOM}\n"
        'const input = JSON.parse(readFileSync(0, "utf8"));\n'
        "const print = value => console.log(JSON.stringify(value));\n"
        f"{loads}{body}\n"
        "process.exit(0);\n"  # a module may leave a timer running (a banner fades after 20 s)
    )
    done = subprocess.run(
        [NODE, "--input-type=module", "-e", script], input=json.dumps(payload), capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def test_every_file_the_page_loads_is_one_of_its_own() -> None:
    """index.html links app.css and boots app.js; a module imports only modules beside it, and never in a circle:
    a module can be read, and imported by a test, without the modules above it."""
    index = FILES["index.html"]
    assert '<link rel="stylesheet" href="/ui/app.css">' in index and 'import { boot } from "/ui/app.js"' in index
    assert index.count("<script") == 1 and "<style" not in index
    modules = {name: text for name, text in FILES.items() if name.endswith(".js")}
    needs = {
        name: set(re.findall(r'^(?:import|\} from) .*"\./([\w.]+)";$', text, re.M)) for name, text in modules.items()
    }
    assert not re.search(r'\bimport\(|from "(?!\./)', "\n".join(modules.values())), "an import from somewhere else"
    for name, wanted in needs.items():
        assert wanted <= set(modules), (name, wanted - set(modules))
        assert resolve("GET", f"/ui/{name}") == "read"
    ordered: list[str] = []
    while len(ordered) < len(modules):  # each pass places the modules whose imports are all placed
        ready = [name for name in modules if name not in ordered and needs[name] <= set(ordered)]
        assert ready, f"modules import each other in a circle: {sorted(set(modules) - set(ordered))}"
        ordered += ready
    assert ordered[0] in ("dom.js", "spec.js") and ordered[-1] == "app.js"


def rendered(*bodies: str) -> list[str]:
    """What the page's `markdown()` builds for each timeline body, as HTML text (a string child is a text node)."""
    return run_js("print(input.map(body => markdown(body).map(html).join('')));", {"markdown.js": "markdown"}, bodies)


def test_the_page_says_which_workers_are_away_until_they_are_back() -> None:
    healths = [
        {"temporal": True, "queues": {"mia-data": True, "snippets": False, "autodev": False}},
        {"temporal": True, "queues": {"mia-data": True, "snippets": False}},
        {"temporal": True, "queues": {"mia-data": True}},
        {"temporal": False, "queues": {}},
    ]
    both, one, none, no_temporal = run_js("print(input.map(downLines));", {"app.js": "downLines"}, healths)
    assert len(both) == 1 and both[0].startswith(
        "No worker is running for snippets, autodev: nothing that waits on them"
    )
    assert both[0].endswith("pixi run mia restart snippets autodev") and "waits on it is" in one[0]
    assert none == [] and len(no_temporal) == 1 and no_temporal[0].startswith("Temporal does not answer")
    # the lines are replaced by each check, never faded, and the check repeats
    assert has('$("down").replaceChildren(') and has("setInterval(checkHealth, HEALTH_MS)")


def test_what_can_be_prepared_is_read_from_the_stage_not_the_state() -> None:
    from mia_data.config import STAGES, round_open

    stages = run_js("print([...ROUND_OPEN]);", {"datasets.js": "ROUND_OPEN"})
    assert stages == [stage for stage in STAGES if round_open(stage)]
    assert has('row.state !== "prepared" && !ROUND_OPEN.has(row.stage)')


def test_needs_you_is_narrowed_by_toggle_buttons_not_a_drop_down() -> None:
    view = squeezed(FILES["subjects.js"][FILES["subjects.js"].index("function viewNeeds(") :])
    assert (
        has('"aria-pressed": String(kind === needsKind)', view)
        and not has("<select", view)
        and not has('"select"', view)
    )
    assert has(
        'pageOf("/needs", "needs", () => ({ ...(needsKind ? { kind: needsKind } : {}),'
        " ...(needsShelved ? { shelved: 1 } : {}) }))",
        view,
    )
    assert has('["", "All"], ["dataset", "Datasets"], ["buffer", "Buffer specs"], ["prep", "Requests"]')
    assert has('.toggles .btn[aria-pressed="true"]')


def test_a_dataset_at_review_offers_a_restart_beside_its_verdicts() -> None:
    from mia_data.config import STAGE_ACTIONS

    assert "go" in STAGE_ACTIONS["review"][1]
    assert has('action === "go" && subject.kind === "dataset" && subject.stage === "review" ? "Restart"')
    order = run_js("print(ACTION_ORDER);", {"actions.js": "ACTION_ORDER"})
    assert order.index("approve") < order.index("reject") < order.index("go") < order.index("cancel")


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


def test_a_rounds_crops_are_pictures_served_by_this_origin() -> None:
    """A round's crops table names images in the gallery repo, which a browser cannot read: each one is shown from
    buffer-api's own copy. Any other image is a link, never a picture loaded from somewhere else."""
    sha = "c" * 40
    gallery = f"https://github.com/o/data/blob/galleries/galleries/rec-1/i7-e99/{sha}/i7-e99-b0-000"
    table, other = rendered(
        "<details><summary>2 crops</summary>\n\n| raw | overlay | caption |\n|---|---|---|\n"
        f"| ![raw]({gallery}/raw.png?raw=true) | ![overlay]({gallery}/overlay.png?raw=true) | i7-e99-b0-000 · z=3 |\n"
        "\n</details>\n\nafter the fold",
        "![logo](https://tracker.example/pixel.png) and ![raw](https://github.com/o/data/blob/galleries/x/raw.png?raw=true)",
    )
    local = f"/gallery/i7-e99/{sha}/i7-e99-b0-000"
    assert table.startswith("<details><summary>2 crops</summary><div") and "<p>after the fold</p>" in table
    assert "&lt;" not in table and "!" not in table and "github.com" not in table
    for name in ("raw", "overlay"):
        assert f'<a href="{local}/{name}.png" target="_blank" rel="noopener"' in table
        assert f'<img class="crop" src="{local}/{name}.png" loading="lazy" alt="{name} of crop i7-e99-b0-000">' in table
    assert "<img" not in other and "!" not in other
    assert '<a href="https://tracker.example/pixel.png" target="_blank" rel="noopener">logo</a>' in other


def test_a_judges_findings_show_the_crops_they_name_behind_a_fold() -> None:
    """A findings table names crops by id: the timeline gives `markdown` where each crop's pictures are, read from
    the crops tables before it, and the findings are folded like a crops table. Only the newest fold starts open."""
    sha = "c" * 40
    gallery = f"https://github.com/o/data/blob/galleries/galleries/rec-1/i7-e99/{sha}/i7-e99-b0-000"
    crops = (
        "| raw | overlay | caption |\n|---|---|---|\n"
        f"| ![raw]({gallery}/raw.png?raw=true) | ![overlay]({gallery}/overlay.png?raw=true) | i7-e99-b0-000 |\n"
    )
    judge = (
        "**Crop judge:** `approve`\n\n| item | severity | note |\n|---|---|---|\n"
        "| i7-e99-b0-000 | ok | sharp membranes |\n| binding | suspect | no crop of this name |\n"
        "| i7-e99-b0-000 | bad | labels shifted |\n"
    )
    pictured, plain = run_js(
        "const [crops, judge] = input, show = nodes => nodes.map(html).join('');\n"
        "print([show(markdown(judge, cropsOf(crops, {}))), show(markdown(judge))]);",
        {"markdown.js": "markdown, cropsOf"},
        [crops, judge],
    )
    local = f"/gallery/i7-e99/{sha}/i7-e99-b0-000"
    assert pictured.startswith(
        "<p><strong>Crop judge:</strong> <code>approve</code></p><details><summary>3 findings</summary>"
    )
    assert "<th>overlay</th><th>item</th><th>severity</th><th>note</th>" in pictured and "raw.png" not in pictured
    # `ok` is plain; `suspect` and `bad` colour the word, and the picture's border with it
    picture = f'src="{local}/overlay.png" loading="lazy" alt="overlay of crop i7-e99-b0-000">'
    assert f'<img class="crop" {picture}' in pictured and "<td>ok</td>" in pictured
    assert f'<img class="crop sev-bad" {picture}' in pictured
    assert '<td><span class="sev-bad">bad</span></td>' in pictured
    # a finding about something that is no crop has no picture, and a timeline with no crops table adds no column
    assert '<tr><td></td><td>binding</td><td><span class="sev-suspect">suspect</span></td>' in pictured
    assert "<img" not in plain and "<details><summary>3 findings</summary>" in plain and "<th>overlay</th>" not in plain
    assert has("markdown(e.body, cropsOf(e.body, crops))") and has("log.append(...shown.reverse())")
    assert has('log.querySelectorAll("details").forEach((fold, n) => { fold.open = n === 0; });')


def test_a_github_issue_is_the_mark_with_a_tooltip() -> None:
    """No link to GitHub is spelled out in words ("log on GitHub"): it is the mark, and the tooltip says what it is."""
    assert not has("log on GitHub") and not has("ghIssue")
    assert has("a read-only copy of the timeline")


def test_markdown_never_becomes_html_or_a_script_link() -> None:
    """A timeline body can carry text from a sandboxed job: tags stay text, and only http(s) links are links."""
    (unsafe,) = rendered("<img src=x onerror=alert(1)> [click](javascript:alert(1)) <script>alert(2)</script>")
    assert (
        unsafe == "<p>&lt;img src=x onerror=alert(1)> [click](javascript:alert(1)) &lt;script>alert(2)&lt;/script></p>"
    )


def lifecycle(*calls: list) -> list[str]:
    """What the page's `lifecycle()` builds for each (line, stage, history), as HTML text."""
    return run_js("print(input.map(call => html(lifecycle(...call))));", {"dom.js": "lifecycle"}, calls)


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
    assert count("lifecycle(") == 4  # the function, a subject's page (twice: one never asked of) and a buffer's


def test_a_buffer_is_always_planned_first() -> None:
    """The page has no tick for the plan: every go it sends for a buffer stops at the plan, and Run continues it."""
    assert not has("Show me the plan first") and not has("first.checked")
    assert count("plan_first: true") == 2  # New buffer, and a spec's own page
    assert has('subject.stage === "plan-review" ? "Run"')


def test_an_asked_action_holds_the_buttons_until_its_workflow_answers() -> None:
    """A click is not shown as the old stage's buttons again: while the subject says an action is `asked`, every
    button that would send an action is disabled, and a reload that started earlier never paints over a later one."""
    assert count("disabled: !!subject.asked") == 4 and has("disabled: !!x.asked")
    assert has("waiting for its workflow to answer")
    assert has("turn !== subjectTurn")


def test_what_is_typed_while_a_subjects_page_loads_is_kept() -> None:
    """A redraw the stream asked for leaves a view someone is typing into alone. So does a load that ends after
    someone typed into the view: it is not painted over them, one notice offers the change, and taking the offer
    draws the page."""
    kept, shown = run_js(
        """
        const root = document.getElementById("subjectBody"), was = el("p", {}, "what was there");
        root.append(was);
        let answer;  // the service answers when the test says so: no request named k
        globalThis.fetch = () => new Promise(settle => { answer = () => settle({ ok: false, status: 404, statusText: "Not Found", text: async () => "" }); });
        globalThis.location = { hash: "#requests/k" };
        const typing = [];
        for (let turn = 0; turn < 2; turn++) {  // typed during the load, then before a later stream redraw
            const drawn = viewSubject("prep", "k", "stream");
            root.dataset.dirty = "1";
            if (answer) answer();
            answer = null;
            await drawn;
            typing.push(root.kids.map(kid => kid.attrs.class || textOf(kid)));
        }
        const offered = viewSubject("prep", "k", true);  // Show the change
        answer();
        await offered;
        print([typing, root.kids.map(textOf)]);
        """,
        {"dom.js": "el", "subjects.js": "viewSubject"},
    )
    assert kept == [["notice stale", "what was there"], ["notice stale", "what was there"]]
    assert shown == ["k", "No such request."]


def test_the_page_asks_which_hosts_a_run_lacks_before_it_starts() -> None:
    """A start action first asks the service which hosts the run needs that the policy lacks."""
    assert ("POST", "/hosts/missing") in requested_paths() and resolve("POST", "/hosts/missing") == "read"
    assert count("askHosts(") >= 3
    for body in (
        "{ records: [subject.key] }",
        "{ prep: subject.number }",
        "{ spec: subject.key }",
        "{ records: todo }",
    ):
        assert has(body)


def test_the_hosts_dialog_offers_approve_start_anyway_and_cancel() -> None:
    """The dialog lists the hosts with Select all; Approve and start waits for a ticked host."""
    for text in (
        "Approve and start",
        "Start without approving",
        "Select all",
        "Hosts to approve",
        "approveButton.disabled = ",
    ):
        assert has(text)
    for key in ("approveHosts:", "startUnapproved:", "cancelHosts:"):
        assert has(key)


def test_an_approval_travels_with_the_start_action() -> None:
    """The approved hosts ride on the start action itself; the page has no route of its own to approve them."""
    assert has("approve_hosts") and not has('"/hosts/approve"')


HOSTS_DOM = """
class FakeNode {
  constructor(tag) { this.tag = tag; this.attrs = {}; this.kids = []; this.listeners = {}; this.checked = false; this.disabled = false; }
  setAttribute(name, value) { this.attrs[name] = value; }
  set className(value) { this.attrs.class = value; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  fire(type) { (this.listeners[type] || []).forEach(fn => fn({})); }
  append(...kids) { this.kids.push(...kids); }
  remove() { body.kids = body.kids.filter(kid => kid !== this); }
  showModal() {}
  close(value) { this.returnValue = value; this.fire("close"); }
}
const body = new FakeNode("body");
const document = { createElement: tag => new FakeNode(tag), body };
const walk = (node, found = []) => { if (typeof node === "string") return found; found.push(node); node.kids.forEach(kid => walk(kid, found)); return found; };
const textOf = node => typeof node === "string" ? node : node.kids.map(textOf).join("");
const byText = (root, text) => walk(root).find(node => node.tag === "button" && textOf(node) === text);
"""


def hosts_dialog(scenarios: list[dict]) -> list[dict]:
    """Run the page's `askHosts` over the fake DOM and a fake service: for each scenario (`hosts`, or
    `fail: [status, message]`, then `steps` on the dialog) report what `send` got, the banner, the dialog and settling."""
    return run_js(
        """
        let fake;
        globalThis.fetch = async () => fake.fail
            ? { ok: false, status: fake.fail[0], statusText: "", text: async () => JSON.stringify({ error: fake.fail[1] }) }
            : { ok: true, status: 200, text: async () => JSON.stringify({ hosts: fake.hosts || [] }) };
        const body = document.body, banner = document.getElementById("banner"), out = [];
        for (fake of input) {
            banner.kids = []; body.kids = [];
            const sent = []; let settled = false;
            const done = askHosts({ records: [] }, hosts => sent.push(hosts)).then(() => { settled = true; });
            await new Promise(r => setImmediate(r));
            const box = body.kids[0], seen = { shown: !!box };
            if (box) {
                const approve = byText(box, "Approve and start"), ticks = walk(box).filter(n => n.attrs.type === "checkbox");
                seen.rows = ticks.slice(1).length; seen.disabledAtStart = approve.disabled;
                for (const step of fake.steps) {
                    if (step === "all") { ticks[0].checked = true; ticks[0].fire("change"); }
                    else if (typeof step === "number") { ticks[step + 1].checked = true; ticks[step + 1].fire("change"); }
                    else box.close(step);
                }
                seen.enabledAfterTicks = !approve.disabled; seen.allChecked = ticks[0].checked; seen.text = textOf(box);
            }
            await done;
            out.push({ ...seen, sent, banners: banner.kids.map(line => textOf(line.kids[0])), settled, removed: body.kids.length === 0 });
        }
        print(out);
        """,
        {"actions.js": "askHosts"},
        scenarios,
    )


def test_the_hosts_dialog_works_as_it_runs() -> None:
    both = [{"host": "a.example.org", "records": ["rec-a"]}, {"host": "b.example.org", "records": ["rec-b", "rec-c"]}]
    nothing, approved, one, anyway, cancelled, escaped, failed = hosts_dialog(
        [
            {"hosts": []},
            {"hosts": both, "steps": ["all", "approve"]},
            {"hosts": both, "steps": [1, "approve"]},
            {"hosts": both, "steps": ["start"]},
            {"hosts": both, "steps": ["cancel"]},
            {"hosts": both, "steps": [""]},
            {"fail": [503, "no broker"]},
        ]
    )
    assert nothing["sent"] == [[]] and not nothing["shown"] and nothing["settled"]
    assert approved["rows"] == 2 and approved["disabledAtStart"] is True
    assert (
        "a.example.org" in approved["text"] and "b.example.org" in approved["text"] and "Select all" in approved["text"]
    )
    assert approved["enabledAfterTicks"] and approved["allChecked"]
    assert approved["sent"] == [["a.example.org", "b.example.org"]] and approved["removed"] and approved["settled"]
    assert one["sent"] == [["b.example.org"]] and one["enabledAfterTicks"] and not one["allChecked"]
    assert anyway["sent"] == [[]] and anyway["removed"] and anyway["settled"]
    for closed in (cancelled, escaped):
        assert closed["sent"] == [] and closed["removed"] and closed["settled"]
    assert failed["sent"] == [] and failed["settled"] and not failed["shown"]
    assert failed["banners"] == ["Hosts not checked (503): no broker. Nothing was started."]


def test_a_second_click_during_the_host_check_starts_nothing() -> None:
    """Both start buttons hold a flag from the click until `askHosts` settles."""
    assert count("if (checking) return;") == 2 and count("checking = true;") == 2
    assert count("checking = false; })") == 2


def test_reject_asks_for_its_feedback_in_a_dialog() -> None:
    """Reject has more to enter: its button ends in an ellipsis and opens a dialog for the feedback. No feedback box
    sits beside the buttons, and a rejection with nothing typed cannot be sent."""
    assert has('reject: "Reject…"') and has("showModal()")
    assert not has("bar.append(note)")
    assert has("Send rejection") and has("sendButton.disabled = !note.value.trim()")


def test_shelving_is_offered_for_every_kind_and_asks_for_its_reason() -> None:
    """Shelve is a button of the action bar like any other the service offers; it opens a dialog for the reason,
    which cannot be sent empty. A shelved subject says who, when and why, and Needs you has a toggle for them."""
    covered, order = run_js(
        "print([Object.entries(ACTION_HELP).map(([kind, help]) => [kind, !!help.shelve && !!help.unshelve]),"
        " ACTION_ORDER]);",
        {"actions.js": "ACTION_HELP, ACTION_ORDER"},
    )
    assert covered == [["dataset", True], ["buffer", True], ["prep", True]]
    assert {"shelve", "unshelve"} <= set(order)
    assert has('shelve: "Shelve…"') and has("if (dialog) return askNote(subject, dialog, note => send({ note }))")
    assert has("if (subject.shelved) parts.push(shelvedNotice(subject.shelved))")
    assert has('"aria-pressed": String(needsShelved)') and has('"shelved": "Set aside by a person')


def test_shelve_sends_its_reason_once_one_is_typed() -> None:
    calls, words, blocked = run_js(
        """
        const calls = [];
        globalThis.fetch = async (url, init) => {
            calls.push([url, JSON.parse(init.body)]);
            return { ok: true, status: 202, text: async () => "{}" };
        };
        const subject = { kind: "dataset", key: "rec-a", number: 7, stage: "failed", status: "stuck", revision: 3,
                          actions: ["go", "revoke", "shelve"], asked: null };
        const bar = actionBar(subject, () => {});
        bar.querySelectorAll = () => walk(bar).filter(node => node.tag === "button");  // the fake DOM has none
        const words = walk(bar).filter(node => node.tag === "button").map(textOf);
        const typed = text => {
            byText(bar, "Shelve…").fire("click");
            const box = document.body.kids.at(-1), note = walk(box).find(node => node.tag === "textarea");
            note.value = text; note.fire("input");
            const blocked = byText(box, "Shelve").disabled;
            box.close("send");
            return blocked;
        };
        const blocked = [typed("  "), typed(" it needs a reader of its own ")];
        await new Promise(r => setImmediate(r));
        print([calls.map(([url, body]) => [url, body.action, body.note, "revision" in body]), words, blocked]);
        """,
        {"actions.js": "actionBar"},
    )
    assert words == ["Run again", "Revoke", "Shelve…"] and blocked == [True, False]
    assert calls == [
        ["/datasets/rec-a/actions", "shelve", "it needs a reader of its own", False]
    ]  # the empty one: none


def test_a_datasets_page_leads_with_its_links_then_its_preparation_workflow() -> None:
    """Under the name come the dataset's links, then the lifecycle labelled as the preparation workflow, then the
    buttons; the button that starts the workflow says Start, and no state pill stands between them."""
    assert has('dataset: "Preparation workflow:"') and has("flowRow(kind, ")
    assert has('subject.again ? "Start again" : "Start"') and not has('"Prepare again"')
    assert not has("statePill(detail ? detail.row.state")
    assert has("const [links = null, ...cards] = ")


def test_a_dataset_whose_record_has_a_gap_offers_no_start_and_says_why() -> None:
    """The page reads `gaps` from the dataset's subject or row: a never-started one stands blocked at `new` with no
    Start, a notice says what the record lacks, and the Datasets list marks it."""
    assert has("const gaps = (subject || detail?.row)?.gaps || [];")
    assert has('actions: kind === "dataset" && !gaps.length ? ["go"] : []')
    assert has('gaps.length ? "blocked" : detail?.row.stage || "new", ["new"]')
    assert count("gapsNotice(gaps)") == 2 and has('"cannot start":')
    assert has('pill("cannot start", "warn", x.gaps.map(t => ({ t })))')


def test_a_subjects_page_says_where_it_stands_with_its_lifecycle_alone() -> None:
    """No status pill, no "waiting on you", no "workflow running" under the line: every link of a subject stands
    in the row under its name, and a table names a subject's stage, not a status word."""
    assert not has("subjectFacts") and not has("waiting on you") and not has('"workflow " +')
    assert not has("statePill(subject.status)") and not has('statePill(x.status), " ", x.stage')
    assert count("stagePill(x.stage)") == 3  # the specs, the requests and Needs you
    assert has("subjectLinks(subject, entries)")


def test_a_lifecycle_marks_the_step_a_round_is_working_at() -> None:
    """While a round is in progress the current step says so; a stage that waits, or one off the line, does not."""
    line = ["new", "testing", "review", "done"]
    working, waiting, stopped = lifecycle(
        [line, "testing", [], True], [line, "review", [], False], [line, "needs-human", ["testing"], True]
    )
    assert '<li class="now busy" aria-current="step" title="a round is in progress">' in working
    assert "busy" not in waiting and "busy" not in stopped


def test_every_button_that_asks_for_something_says_what_it_does() -> None:
    """A button's tooltip says what pressing it does. Every action a stage table offers has one (a spec's go,
    revoke and dropped have controls of their own, which carry theirs), and so has every other such button."""
    from mia_buffers.config import PREP_STAGE_ACTIONS
    from mia_buffers.config import STAGE_ACTIONS as BUFFER_ACTIONS
    from mia_data.config import STAGE_ACTIONS as DATASET_ACTIONS

    covered, keys = run_js(
        "print([Object.fromEntries(Object.entries(ACTION_HELP).map(([kind, help]) => [kind, Object.keys(help)])),"
        " Object.keys(BUTTON_HELP)]);",
        {"actions.js": "ACTION_HELP, BUTTON_HELP"},
    )
    own_control = {"buffer": {"go", "revoke", "dropped"}}
    for kind, table in (("dataset", DATASET_ACTIONS), ("buffer", BUFFER_ACTIONS), ("prep", PREP_STAGE_ACTIONS)):
        offered = set().union(*(actions for _, actions in table.values())) - own_control.get(kind, set())
        assert offered <= set(covered[kind]), (kind, offered - set(covered[kind]))
    assert has("title: actionHelp(subject, action)")
    assert {"plan", "dropped", "revokeTicked", "retry", "prepare", "sendReject"} <= set(keys)
    for key in keys:
        assert has(f"title: BUTTON_HELP.{key}"), key


def test_a_buffers_page_follows_a_datasets_name_links_then_workflow() -> None:
    """A buffer's page reads like a dataset's: its id, a row of links (its spec, the spec's GitHub issue with its
    number, the round's workspace and its staged pyramids), then the labelled workflow. No pill beside the id, no
    row of bare root folders."""
    body = squeezed(FILES["buffers.js"][FILES["buffers.js"].index("async function viewBuffer(root, id, refresh) {") :])
    name, links, flow = (
        body.index(squeezed(code)) for code in ('el("h2", {}, id)', 'class: "links"', 'flowRow("buffer", ')
    )
    assert name < links < flow
    assert has("ghIcon(meta.buffers_repo, spec.issue, true)", body) and has("SUBJECT_HASH.buffer(spec.key)", body)
    assert not has("Object.entries(roots)", body) and not has("all buffers", body)
    assert has('buffer: "Buffer workflow:"')


def test_any_dataset_can_be_ticked_and_the_ticked_prepared_or_made_a_spec() -> None:
    """Every row of the Datasets list has a tick, prepared or not. The ticked that still need it can be prepared
    in one request, and all of them can open New buffer as a spec that selects exactly them: each by a glob on
    its record's file name, which no other record's path matches."""
    from mia_buffers.select import _glob

    assert has("select: { chosen: CHOSEN, id: row => row.id, onchange: ") and not has("can: PREPARABLE")
    assert has("function specFromDatasets(ids)") and has("ids.map(id => `*/${id}.yaml`)")
    assert has("title: BUTTON_HELP.specFrom")
    patterns = ["*/u937.yaml", "*/bcss.yaml"]
    assert _glob(patterns, "datasets/2D/confocal/u937.yaml") and _glob(patterns, "datasets/bcss.yaml")
    assert not _glob(patterns, "datasets/2D/confocal/not-u937.yaml") and not _glob(patterns, "datasets/u937.yaml.bak")


def test_the_datasets_selection_shows_its_actions_only_when_something_is_ticked() -> None:
    """With nothing ticked the list offers Select all and Clear selection and says nothing more. With a tick, a
    box of its own colour says how many datasets are selected and offers Prepare and Create buffer."""
    assert has('"Select all")') and not has("Select all matching")
    assert not has("tick the datasets to prepare first")
    assert has('class: "chosen", hidden: true') and has(".chosen[hidden]{display:none}")
    assert has("box.hidden = !CHOSEN.size") and has('"Create buffer")')
    # Select all takes the rows of the page that is showing, and asks the server for nothing
    assert not has("datasets match; the first") and not has('ids: "1"')


# strings a person can type into a spec's name or paths, or that a record's id can be: each reads as something
# else, or as nothing, when YAML is handed it bare
AWKWARD = [
    "plain", "with space", "a: b", "#hash", "- dash", "-", "? q", "*star", "&anchor", "!tag", "|", ">", "@at", "`tick",
    "%pct", "it's", 'say "hi"', "a,b", "[x]", "{y}", "é ü", "", " lead", "trail ", "tab\there", "two\nlines",
    "yes", "No", "ON", "true", "null", "~", "12", "+12", "-1.5", ".5", "1e3", "1_000", "017", "0x1f", "0b11", "0o17",
    ".inf", "-.INF", ".NaN", "1:30", "2026-11-14", "2026-11-14 10:00:00", "<<", "=",
]  # fmt: skip


def test_the_yaml_the_page_writes_is_what_the_service_reads() -> None:
    """The page writes a buffer spec and a preparation request as YAML by hand (spec.js). The service's own parsers
    read them back: the controls land in the fields they name, and every string arrives as it was typed."""
    from mia_buffers.spec import load_prep_text, load_spec_text

    ref, controls = "a" * 40, {
        "name": "em pool", "paths": "datasets/2D/** */u937.yaml", "dims": ["2D", "3D"], "lic": ["CC0-1.0", "MIT"],
        "allowUnknown": True, "annRequired": True, "sources": ["manual", "proofread"], "minVox": 1000000,
        "patch": 256, "level": 4, "patchZ": 32, "dedup": True, "T": 0.25,
    }  # fmt: skip
    spec_text, named, prep_text = run_js(
        "const filled = c => ({ ...c, dims: new Set(c.dims), lic: new Set(c.lic), sources: new Set(c.sources) });\n"
        "print([toSpecYaml(filled(input.controls), input.ref, 'o/catalog'),\n"
        "  input.awkward.map(text => toSpecYaml(filled({ ...input.controls, name: 'n ' + text + ' n', paths: 'p' }), input.ref, text)),\n"
        "  toPrepYaml(input.awkward.filter(Boolean), input.ref, 'o/catalog')]);",
        {"spec.js": "toSpecYaml, toPrepYaml"},
        {"controls": controls, "ref": ref, "awkward": AWKWARD},
    )
    spec = load_spec_text(spec_text)
    assert (spec.name, spec.catalog.repo, spec.catalog.ref) == ("em pool", "o/catalog", ref)
    assert spec.select.paths == ["datasets/2D/**", "*/u937.yaml"] and spec.select.dedup.by == ["doi"]
    where = spec.select.where.model_dump(by_alias=True)
    assert where["dims"] == {"in": ["2D", "3D"]} and where["min_size_voxels"] == 1000000
    assert where["license"] == {"in": ["CC0-1.0", "MIT"], "allow_unknown": True}
    assert (where["annotations"]["required"], where["annotations"]["source"]) == (True, {"in": ["manual", "proofread"]})
    patch = spec.sampling.patch.model_dump(by_alias=True, exclude_none=True)
    assert patch == {"2D": {"y": 256, "x": 256}, "3D": {"z": 32, "y": 256, "x": 256}, "c": 1}
    assert spec.sampling.levels == [1, 2, 4] and spec.sampling.weighting.temperature == 0.25
    # a catalog repo is any string, and a name is trimmed: what was typed between the n's comes back
    for text, written in zip(AWKWARD, named, strict=True):
        read = load_spec_text(written)
        assert (read.catalog.repo, read.name) == (text, f"n {text} n"), text
    assert load_prep_text(prep_text).records == [text for text in AWKWARD if text]


def test_a_datasets_page_lists_every_address_of_its_catalog_record_in_its_links_card() -> None:
    """Under the name stand only the links to what the workflows made (the issue, the saved crops). The Links card
    has every address of the catalog record: the dataset's own page, its DOI, each paper, the download, the code,
    the record and its pull request in the catalog repository, then the longer lists, each address once. A record
    is untrusted: only an http(s) address is a link, and any other is shown as text."""
    record = {
        "landing_url": "https://zenodo.org/records/1",
        "doi": "10.5281/zenodo.1",
        "publications": [
            {"title": "First <b>paper</b>", "doi": "10.1/a", "url": "https://journal.example/a"},
            {"title": None, "doi": "10.1/b", "url": None},
            {"title": "No address"},
        ],
        "data": {"download_url": "s3://bucket/data", "download_method": "aws-cli", "access": "open"},
        "ml": {"baseline_code_url": "javascript:alert(1)"},
        "provenance": {"evidence_urls": ["https://zenodo.org/records/1", "https://zenodo.org/api/records/1"]},
        "technical": {
            "sample": {"urls": ["https://zenodo.org/api/records/1/files/a.tif/content"]},
            "inspected_urls": ["https://zenodo.org/api/records/1", "ftp://old.example/x"],
        },
    }
    detail = {
        "row": {"id": "rec 1", "in_catalog": True, "state": "prepared", "issue": 9},
        "record": record,
        "dataset": {"folder": "rec 1"},
        "path": "datasets/2D/confocal/rec 1.yaml",
        "ref": "a" * 40,
        "requests": [],
    }
    row, links, bare = run_js(
        """
        globalThis.fetch = async () => ({ ok: true, status: 200, text: async () => "{}" });
        S.meta = { catalog_repo: "o/catalog", datasets_repo: "o/data", roots: { storage_root: "https://files.example/s" } };
        const titled = cards => cards.find(card => card.kids[0] && textOf(card.kids[0]) === "Links");
        const [links, ...cards] = datasetCards(input.detail);
        const more = titled(cards).kids.at(-1).kids;  // the card's dl: dt, dd, dt, dd, …
        const listed = more.map(node => node.tag === "dt" ? textOf(node) : node.kids[0].kids.map(line => line.kids.map(html).join("")));
        const unlinked = datasetCards({ row: { id: "x" }, record: { title: "x" }, path: null, requests: [] });
        print([html(links), listed, [unlinked[0].kids.length, !!titled(unlinked.slice(1))]]);
        """,
        {"api.js": "S", "datasets.js": "datasetCards"},
        {"detail": detail},
    )
    # the row: the issue and the saved crops, and no address of the record
    assert "https://github.com/o/data/issues/9" in row and "saved crops" in row
    assert "zenodo" not in row and "o/catalog" not in row

    def link_to(href: str, text: str = "", title: str = "") -> str:
        mark = "" if "github.com" not in href else row[row.index("<svg") : row.index("</svg>") + 6] + " "
        said = f' title="{title}"' if title else ""
        return f'<a href="{href}" target="_blank" rel="noopener"{said}>{mark}{text or href}</a>'

    catalog = "https://github.com/o/catalog"
    assert links == [
        "dataset page", [link_to("https://zenodo.org/records/1")],
        "doi", [link_to("https://doi.org/10.5281/zenodo.1", "10.5281/zenodo.1")],
        "paper", [
            link_to("https://journal.example/a", "First &lt;b>paper&lt;/b>"),  # a title is text
            link_to("https://doi.org/10.1/b"),
        ],
        "download", ["s3://bucket/data · aws-cli · open"],  # no address a browser opens: text
        "code", ["javascript:alert(1)"],
        "catalog record", [
            link_to(f"{catalog}/blob/{'a' * 40}/datasets/2D/confocal/rec%201.yaml", "datasets/2D/confocal/rec 1.yaml")
        ],
        "catalog PR", [
            link_to(
                f"{catalog}/pulls?q=is%3Apr%20head%3Aclaude%2Fdataset%2Frec%201",
                "pull requests from claude/dataset/rec 1",
                "the pull request that brought this record into the catalog: the ones opened from its branch",
            )
        ],
        "sample files", [link_to("https://zenodo.org/api/records/1/files/a.tif/content")],
        "evidence", [link_to("https://zenodo.org/api/records/1")],  # the dataset's page is not listed again
        "inspected", ["ftp://old.example/x"],
    ]  # fmt: skip
    assert 'href="javascript:' not in json.dumps(links)
    assert bare == [0, False]  # a record with no address and no place in the catalog: an empty row, and no card
