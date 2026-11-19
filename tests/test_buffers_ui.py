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
        "/subjects",
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
    assert ("POST", "/specs/parse") in requested and resolve("POST", "/specs/parse") == "read"  # it writes nothing
    for path in ("/subjects", "/subjects/buffer", "/subjects/prep"):
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
    for needle in ('"revoked":', '"quarantined":', "Revoke the checked", "paused by the operator"):
        assert has(needle), needle


def test_the_docs_describe_the_system_as_it_is() -> None:
    """The Docs tab names buttons, never label commands, and explains nothing by what it replaced."""
    docs = FILES["guide.html"] + FILES["architecture.html"]
    assert has("send(`/ui/${doc}.html`)") and resolve("GET", "/ui/guide.html") == "read"
    assert not re.search(r"\b(buffer|data|prep|autodev):[a-z]", docs)
    assert not re.search(r"\b(no longer|any more|anymore|used to|as before|previously|read-only log)\b", docs, re.I)
    for needle in ("Needs you", "Cancel the round", "Files are in place", "MiaMirror", "/stream"):
        assert needle in docs, needle


def test_page_has_a_cleaner_view_and_the_banner_says_its_findings() -> None:
    assert has('data-tab="cleaner"') and has('data-view="cleaner"') and has("cleaner: viewCleaner")
    assert resolve("GET", "/cleaner") == "read" and resolve("GET", "/cases") == "read"
    findings = [
        {"id": "worker snippets", "text": "no worker polls `snippets`"},  # the health check says this itself
        {"id": "restart mia-data", "text": "the `mia-data` worker runs commit `aaaaaaa`: a restart is owed"},
    ]
    health = {"temporal": True, "queues": {"mia-data": True}}
    lines = run_js(
        "print(downLines(input.health, input.findings));",
        {"app.js": "downLines"},
        {"health": health, "findings": findings},
    )
    assert lines == ["the mia-data worker runs commit aaaaaaa: a restart is owed"]
    cases = [
        {
            "kind": "dataset",
            "key": "rec-a",
            "rule": "no run",
            "state": "gave-up",
            "updated": 5.0,
            "opened": 4.0,
            "data": {
                "seen": "no open run",
                "steps": [
                    {"do": "cancel", "result": "action 1", "at": 4.5},
                    {"do": "report_defect", "title": "a defect", "evidence": "line 3", "result": "kept on the case"},
                    {"do": "needs", "text": "look", "result": "left to a person"},
                ],
                "plan": [{"do": "go"}],
                "needs": "look",
                "diagnosis": {"cause": "defect", "diagnosis": "the worker dropped it", "needs": "look"},
            },
            "jobs": ["issue-224/i224-e1/match-1", "o__s/issue-49/01a1/round-1", "cleaner/case-1/diagnose-0"],
        },
        {
            "kind": "job",
            "key": "issue-9/i9-e1/crops-1",
            "rule": "left-over job",
            "state": "open",
            "updated": 6.0,
            "data": {"plan": [{"do": "kill_job"}]},
        },
    ]
    stuck, job = run_js("print(caseRows(input));", {"operations.js": "caseRows"}, cases)
    # the row is short: the subject, the problem in a few words, a status and how far the cleaner got
    assert (stuck["href"], stuck["about"], stuck["problem"]) == ("#datasets/rec-a", "rec-a", "No workflow run")
    assert (stuck["status"], stuck["progress"]) == ("gave up", "2 done, 1 to go")
    assert (job["href"], job["problem"]) == ("#job/issue-9/i9-e1/crops-1", "Left-over cluster job")
    assert (job["status"], job["progress"]) == ("queued", "1 to go")
    # the long words are the drill-down's: what the problem is, what was found, each step with its result, what is
    # left, what a person must do (once: the `needs` step is not listed as done) and the diagnosis
    assert "Temporal has no open run" in stuck["meaning"] and stuck["found"] == "no open run"
    assert stuck["did"] == [
        {"what": "cancel the round", "result": "action 1", "at": 4.5},
        {"what": "report the defect", "result": "kept on the case", "title": "a defect", "evidence": "line 3"},
    ]
    assert (stuck["next"], stuck["needs"]) == (["run it again"], "look")
    assert stuck["diagnosis"] == {"cause": "defect", "text": "the worker dropped it"}
    assert (job["did"], job["next"], job["needs"], job["diagnosis"]) == ([], ["kill the job"], None, None)
    # each job record a case is about is linked, under the job's name
    assert stuck["jobs"] == [
        {"href": "#job/issue-224/i224-e1/match-1", "name": "match-1"},
        {"href": "#job/o__s/issue-49/01a1/round-1", "name": "snippets #49 round-1"},
        {"href": "#job/cleaner/case-1/diagnose-0", "name": "diagnose-0"},
    ]
    assert job["jobs"] == []


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


def test_the_catalog_in_use_is_named_on_the_datasets_view_with_a_link_to_that_commit() -> None:
    """Which catalog the datasets come from is said where they are listed, top right of the Datasets view: the
    commit links the catalog repo at that commit. The top bar does not carry it."""
    assert "catref" not in FILES["index.html"] and "catref" not in FILES["app.js"]
    catalog = {"ref": "2c55a495" + "0" * 32, "count": 2243, "synced_at": "2026-11-15T12:00:00+00:00"}
    linked, bare = run_js(
        "print(input.map(([catalog, repo]) => { const node = catalogLine(catalog, repo);"
        " return [textOf(node), walk(node).filter(n => n.tag === 'a').map(a => a.attrs.href), node.attrs.class]; }));",
        {"datasets.js": "catalogLine"},
        [[catalog, "o/catalog"], [catalog, None]],
    )
    words = [" ".join(text.split()) for text in (linked[0], bare[0])]  # a GitHub link carries the mark and a space
    assert words[0].startswith("Sourced from catalog: 2c55a495 · 2,243 datasets · synced ") and linked[2] == "catalog"
    assert linked[1] == [f"https://github.com/o/catalog/tree/{catalog['ref']}"]
    assert words[1] == words[0] and bare[1] == []  # no catalog repo configured: the commit is plain text
    assert has('el("div", { class: "viewhead" }, el("h2", {}, "Datasets"), catalogLine(')


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


def test_a_bare_url_ends_before_the_punctuation_that_follows_it() -> None:
    (said,) = rendered(
        "snippet PR https://github.com/o/r/pull/44: closed. See https://a.org/x?y=1, then (https://b.org)."
    )
    assert 'href="https://github.com/o/r/pull/44"' in said and "</a>: closed" in said
    assert 'href="https://a.org/x?y=1"' in said and 'href="https://b.org"' in said


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


def test_needs_you_is_narrowed_by_toggle_buttons_not_a_drop_down() -> None:
    view = squeezed(FILES["subjects.js"][FILES["subjects.js"].index("function viewNeeds(") :])
    assert (
        has('"aria-pressed": String(kind === needsKind)', view)
        and not has("<select", view)
        and not has('"select"', view)
    )
    assert has(
        'pageOf("/subjects", "subjects", () => ({ ...(needsKind ? { kind: needsKind } : {}),'
        ' ...(needsShelved ? { status: "shelved" } : {}) }))',
        view,
    )
    assert has('["", "All"], ["dataset", "Datasets"], ["buffer", "Buffers"], ["prep", "Requests"]')
    assert has('.toggles .btn[aria-pressed="true"]')


def test_a_dataset_at_review_offers_a_restart_beside_its_verdicts() -> None:
    from mia_data.config import STAGE_ACTIONS

    assert "go" in STAGE_ACTIONS["review"][1]
    order, word = run_js(
        'print([ACTION_ORDER, actionWord({ kind: "dataset", stage: "review" }, "go")]);',
        {"actions.js": "ACTION_ORDER, actionWord"},
    )
    assert word == "Restart"
    assert order.index("approve") < order.index("reject") < order.index("resume") < order.index("go")
    assert order.index("go") < order.index("cancel")


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
    assert '<td><span class="sev-bad">bad</span></td><td class="wrap">labels shifted</td>' in pictured  # a note wraps
    # a finding about something that is no crop has no picture, and a timeline with no crops table adds no column
    assert '<tr><td></td><td>binding</td><td><span class="sev-suspect">suspect</span></td>' in pictured
    assert "<img" not in plain and "<details><summary>3 findings</summary>" in plain and "<th>overlay</th>" not in plain
    assert has("markdown(e.body, cropsOf(e.body, crops))") and has("log.append(...shown.reverse())")
    assert has('log.querySelectorAll("details").forEach((fold, n) => { fold.open = n === 0; });')


def test_a_github_issue_is_the_mark_with_a_tooltip() -> None:
    """No link to GitHub is spelled out in words ("log on GitHub"): it is the mark, and the tooltip says what it is."""
    assert not has("log on GitHub") and not has("ghIssue")
    assert has("a read-only copy of the timeline")


def test_the_gist_of_an_entry_is_its_sentence_cut_between_words() -> None:
    """The line under the lifecycle says what the newest entry says: the sentence under a stage's or a result's
    heading (the diagram shows the stage), the heading too for any other entry, never the folders or a table. A
    long one is cut between two words and never inside a link or a code span."""
    long_url = "https://github.com/o/r/pull/44"
    bodies = [
        "**Stage:** `data:developing`\n\nautodev is writing the snippet in issue #84\n\n**Folders:** [W](https://x.org/w)\n\n<!-- m -->",
        "**Stage:** `data:review`\n\n**Snippet PR:** https://x.org/pull/1\n\n**Folders:** [W](https://x.org/w)",
        "**Crop judge:** `reject`\n\nThe labels are empty.\n\n| item | severity | note |\n|---|---|---|\n| a | bad | b |",
        "**Action:** `reject` by `page`\n\n> the masks are empty",
        "**Result:** `data:matching`\n\nMatched: a reader fits. The agent's reasons:\n- raw: the sample\n\n**Jobs:** [m](https://x.org/#job/m)",
        f"**Result:** `data:developing`\n\n**blocked**: one two three snippet PR {long_url}: closed",
        "Refused `data:go`: busy, ignored: a round is already running.",
        "",
    ]
    stage, bare, judge, action, listed, cut, plain, empty = run_js(
        "print(input.map(body => gist(body, 40)));", {"markdown.js": "gist"}, bodies
    )
    assert stage == {"text": "autodev is writing the snippet in issue…", "cut": True}
    assert bare == {"text": "**Stage:** `data:review`", "cut": False}
    assert judge == {"text": "**Crop judge:** `reject` · The labels…", "cut": True}
    assert action == {"text": "**Action:** `reject` by `page` · the…", "cut": True}
    assert listed == {"text": "Matched: a reader fits. The agent's…", "cut": True}
    assert cut == {"text": "**blocked**: one two three snippet PR…", "cut": True}  # the link is not cut in two
    assert plain["text"].startswith("Refused `data:go`: busy,") and empty == {"text": "", "cut": False}
    (whole,) = run_js("print(input.map(body => gist(body)));", {"markdown.js": "gist"}, bodies[:1])
    assert whole == {"text": "autodev is writing the snippet in issue #84", "cut": False}
    # the subject's page shows it under the lifecycle, and a cut one links down to the timeline
    assert has("const said = gist(entries.at(-1)?.body") and has("log.scrollIntoView(")


def test_markdown_never_becomes_html_or_a_script_link() -> None:
    """A timeline body can carry text from a sandboxed job: tags stay text, and only http(s) links are links."""
    (unsafe,) = rendered("<img src=x onerror=alert(1)> [click](javascript:alert(1)) <script>alert(2)</script>")
    assert (
        unsafe == "<p>&lt;img src=x onerror=alert(1)> [click](javascript:alert(1)) &lt;script>alert(2)&lt;/script></p>"
    )


def test_a_job_link_in_a_timeline_opens_in_this_page() -> None:
    """A result entry links its jobs' records as `<service url>/#job/<address>`: on the page that address is this
    page's own, so the link opens here; a link to anywhere else opens a new tab."""
    here, away = run_js(
        "globalThis.location = { origin: 'https://page.example', hash: '' };\n"
        "print(input.map(text => html(markdown(text)[0])));",
        {"markdown.js": "markdown"},
        ["**Jobs:** [match-1](https://page.example/#job/issue-1/i1-e2/match-1)", "[x](https://other.example/#job/y)"],
    )
    assert '<a href="https://page.example/#job/issue-1/i1-e2/match-1">match-1</a>' in here
    assert 'target="_blank"' not in here and 'target="_blank"' in away


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
    """The page has no check for the plan: every go it sends for a buffer stops at the plan, and Run continues it."""
    assert not has("Show me the plan first") and not has("first.checked")
    assert has('subject.stage === "plan-review" ? "Run"')


def test_an_asked_action_holds_the_buttons_until_its_workflow_answers() -> None:
    """A click is not shown as the old stage's buttons again: while the subject says an action is `asked`, every
    button that would send an action is disabled, and a reload that started earlier never paints over a later one."""
    # the bar, a drop, a revoke, a new plan; the forward and back buttons of a step add the stage to theirs
    # ... and the accept of a set that was sent back, beside a newer one under review
    assert count("disabled: !!subject.asked") == 5 and has("disabled: !!x.asked")
    assert count("|| !!subject.asked") == 4  # the plan of step 3, Run, Approve and serve, the way back
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
    """The dialog lists the hosts with Select all; Approve and start waits for a checked host."""
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
    """Every start button (a subject's bar, Prepare, a buffer's Run) holds a flag from the click until `askHosts`
    settles."""
    assert count("if (checking) return;") == 3 and count("checking = true;") == 3
    assert count("checking = false; })") == 3


def test_a_datasets_verdict_names_the_set_it_is_about() -> None:
    """spec judge-decides §5.4: the page sends the set with the click; which set depends on the stage."""
    sets = {
        "review": "i7-e2:t2",
        "held": {"set": "i7-e1:t1", "by": "judge", "worst": "suspect", "findings": [], "gallery": None},
        "saved": {"set": "i7-e0:t0", "approved_by": "judge"},
    }
    cases = [
        ({"kind": "dataset", "stage": "review", "revision": 4, "data": {"sets": sets}}, "approve"),
        ({"kind": "dataset", "stage": "review", "revision": 4, "data": {"sets": sets}}, "reject"),
        ({"kind": "dataset", "stage": "developing", "revision": 5, "data": {"sets": sets}}, "approve"),
        ({"kind": "dataset", "stage": "blocked", "revision": 6, "data": {"sets": sets}}, "approve"),
        ({"kind": "dataset", "stage": "done", "revision": 7, "data": {"sets": sets}}, "reject"),
        ({"kind": "dataset", "stage": "review", "revision": 8, "data": {}}, "approve"),  # a round from before sets
        ({"kind": "buffer", "stage": "plan-review", "revision": 9, "data": {}}, "approve"),
        ({"kind": "dataset", "stage": "done", "revision": 7, "data": {"sets": sets}}, "go"),  # no verdict
    ]
    got = run_js(
        "print(input.map(([subject, action]) => verdictOf(subject, action)));", {"actions.js": "verdictOf"}, cases
    )
    assert got == [
        {"revision": 4, "set": "i7-e2:t2"},
        {"revision": 4, "set": "i7-e2:t2"},
        {"revision": 5, "set": "i7-e1:t1"},
        {"revision": 6, "set": "i7-e1:t1"},
        {"revision": 7, "set": "i7-e0:t0"},
        {"revision": 8},
        {"revision": 9},
        {},
    ]
    assert has("...verdictOf(subject, action)")


def test_a_set_that_was_sent_back_is_shown_with_its_findings_and_an_accept() -> None:
    """spec judge-decides §5.1, §6: who sent the set back, the findings as text, and what Accept does."""
    words = run_js(
        "print(input.map(([subject, action]) => actionWord(subject, action)));",
        {"actions.js": "actionWord"},
        [
            [{"kind": "dataset", "stage": "developing"}, "approve"],
            [{"kind": "dataset", "stage": "review"}, "approve"],
            [{"kind": "dataset", "stage": "done"}, "reject"],
        ],
    )
    assert words == ["Accept these crops", "Approve", "Reject…"]
    assert has("export function heldPanel(") and has("heldPanel(subject, redraw)")
    assert has("title: BUTTON_HELP.acceptHeld")
    assert has("Accept these crops instead")  # at review, beside the verdict on the newer set
    assert has("Cancel the rework and save this set as it is")
    assert has("Revoke the saved crops and send them back with your feedback")
    # a finding is text a job wrote: inserted as text, never as markup
    assert has("`${finding.item} [${finding.severity}]: ${finding.note}`")


def test_a_stopped_dataset_offers_resume_first_and_restart_beside_it() -> None:
    """spec resume-restart §10: one word on each button; what sets the two apart is in their tooltips."""
    buttons, words, hosts = run_js(
        """
        const subject = { kind: "dataset", key: "rec-a", number: 7, stage: "blocked", status: "stuck", revision: 3,
                          actions: ["go", "resume", "revoke"], asked: null };
        const bar = actionBar(subject, () => {});
        const buttons = walk(bar).filter(node => node.tag === "button")
            .map(node => [textOf(node), node.attrs.class, node.attrs.title]);
        const words = input.map(([subject, action]) => actionWord(subject, action));
        print([buttons, words, [hostsRun(subject, "resume"), hostsRun({ ...subject, kind: "buffer" }, "resume")]]);
        """,
        {"actions.js": "actionBar, actionWord, hostsRun"},
        [
            [{"kind": "dataset", "stage": "failed"}, "resume"],
            [{"kind": "dataset", "stage": "failed"}, "go"],
            [{"kind": "dataset", "stage": "done"}, "go"],
            [{"kind": "dataset", "stage": "review"}, "go"],
            [{"kind": "dataset", "stage": "debugging"}, "go"],
            [{"kind": "dataset", "stage": "new"}, "go"],
        ],
    )
    assert [(word, style) for word, style, _ in buttons] == [
        ("Resume", "btn primary"),
        ("Restart", "btn"),
        ("Revoke", "btn danger"),
    ]
    resume, restart = buttons[0][2], buttons[1][2]
    assert resume.startswith("Run what is left. Every step that finished is kept") and "use Restart" in resume
    assert restart.startswith("Start over from scratch on today's catalog, readers and harness.")
    assert words == ["Resume", "Restart", "Restart", "Restart", "Restart now", "Start"]
    assert hosts == [{"records": ["rec-a"]}, None]  # a resume starts jobs: it asks for the hosts they lack
    for file in ("dom.js", "subjects.js", "operations.js"):  # what a dataset's page says of its buttons
        assert "Run again" not in (UI_DIR / file).read_text(), file


def test_a_reject_of_the_saved_set_asks_for_missing_hosts_like_a_start() -> None:
    """spec judge-decides §5.2: the rework's jobs need the dataset's hosts, so the reject carries their approval."""
    got = run_js(
        "print(input.map(([subject, action]) => hostsRun(subject, action)));",
        {"actions.js": "hostsRun"},
        [
            [{"kind": "dataset", "key": "rec-a", "stage": "done"}, "reject"],
            [{"kind": "dataset", "key": "rec-a", "stage": "review"}, "reject"],
            [{"kind": "dataset", "key": "rec-a", "stage": "new"}, "go"],
        ],
    )
    assert got == [{"records": ["rec-a"]}, None, {"records": ["rec-a"]}]


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
    assert has('shelve: "Shelve…"') and has("if (dialog) return askNote(subject, dialog, note => start({ note }))")
    assert has("if (subject.shelved) parts.push(shelvedNotice(subject.shelved))")
    assert has('"aria-pressed": String(needsShelved)') and has('"shelved": "Set aside by a person')
    assert has('dialog.cancels && subject.actions.includes("cancel")')  # a round at review is said to be cancelled


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
    assert words == ["Restart", "Revoke", "Shelve…"] and blocked == [True, False]
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
    assert count("stagePill(x.stage)") == 3  # the requests (all, and a buffer's own) and Needs you
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
    """A button's tooltip says what pressing it does. Every action a stage table offers has one (a buffer's go,
    edit, revoke and dropped have controls of their own, which carry theirs), and so has every other such button."""
    from mia_buffers.config import PREP_STAGE_ACTIONS
    from mia_buffers.config import STAGE_ACTIONS as BUFFER_ACTIONS
    from mia_data.config import STAGE_ACTIONS as DATASET_ACTIONS

    covered, keys = run_js(
        "print([Object.fromEntries(Object.entries(ACTION_HELP).map(([kind, help]) => [kind, Object.keys(help)])),"
        " Object.keys(BUTTON_HELP)]);",
        {"actions.js": "ACTION_HELP, BUTTON_HELP"},
    )
    own_control = {"buffer": {"go", "revoke", "dropped", "edit"}}
    for kind, table in (("dataset", DATASET_ACTIONS), ("buffer", BUFFER_ACTIONS), ("prep", PREP_STAGE_ACTIONS)):
        offered = set().union(*(actions for _, actions in table.values())) - own_control.get(kind, set())
        assert offered <= set(covered[kind]), (kind, offered - set(covered[kind]))
    assert has("title: actionHelp(subject, action)")
    assert {"save", "saveChanges", "plan", "dropped", "revokeTicked", "retry", "prepare", "sendReject"} <= set(keys)
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


def test_a_request_filed_from_either_list_resumes_its_datasets_or_says_restart() -> None:
    """The Datasets list and a buffer's Prepare step file the same two requests (spec resume-restart §5, §10):
    Resume's says nothing of how it runs, Restart's says `restart`, so a checked dataset that is prepared or at
    review starts over too. The buffer's request also names its buffer."""
    sent = run_js(
        "const { S } = await import(input.api);\n"
        "S.ref = 'a'.repeat(40); S.meta = { catalog_repo: 'o/catalog' };\n"
        "const posts = [];\n"
        "globalThis.fetch = async (path, options) => {\n"
        "  if (path === '/prep') posts.push(JSON.parse(options.body));\n"
        "  const answer = path === '/prep' ? { number: 5 } : { hosts: [] };\n"
        "  return { ok: true, status: 200, text: async () => JSON.stringify(answer) };\n"
        "};\n"
        "const titles = [];\n"
        "for (const owner of [undefined, 7]) for (const restart of [false, true]) {\n"
        "  const box = document.createElement('span');\n"
        "  prepareButton(box, ['u937', 'bcss'], null, null, restart ? 'Restart' : 'Resume', owner, restart);\n"
        "  const button = walk(box).find(node => node.tag === 'button');\n"
        "  titles.push(button.attrs.title);\n"
        "  button.fire('click');\n"
        "  await new Promise(done => setTimeout(done, 20));\n"
        "}\n"
        "print([posts.map(post => [post.spec.includes('\\nrestart: true\\n'), post.spec.includes('again'),"
        " post.buffer ?? null]), titles]);",
        {"actions.js": "prepareButton"},
        {"api": (UI_DIR / "api.js").as_uri()},
    )
    posts, titles = sent
    assert posts == [[False, False, None], [True, False, None], [False, False, 7], [True, False, 7]]
    assert titles[0].startswith("Prepare the selected datasets: one that never ran starts")
    assert titles[1].startswith("Prepare every selected dataset from scratch")


def test_any_dataset_can_be_ticked_and_the_ticked_prepared_or_made_a_spec() -> None:
    """Every row of the Datasets list has a check, prepared or not. The checked that still need it can be prepared
    in one request, and all of them can open New buffer as a spec that selects exactly them: each by a glob on
    its record's file name, which no other record's path matches."""
    from mia_buffers.select import _glob

    assert has("select: { chosen: CHOSEN, id: row => row.id, onchange: ") and not has("can: PREPARABLE")
    assert has("function specFromDatasets(ids)") and has("ids.map(id => `*/${id}.yaml`)")
    assert has("title: BUTTON_HELP.specFrom")
    patterns = ["*/u937.yaml", "*/bcss.yaml"]
    assert _glob(patterns, "datasets/2D/confocal/u937.yaml") and _glob(patterns, "datasets/bcss.yaml")
    assert not _glob(patterns, "datasets/2D/confocal/not-u937.yaml") and not _glob(patterns, "datasets/u937.yaml.bak")


def test_the_datasets_list_flags_and_filters_what_a_record_lacks() -> None:
    """A dataset whose record lacks something every buffer needs (an array shape, a voxel size, a data URL) says so
    in its row, and the list filters by one of them or by any. An address can name the filter: a buffer's step 2
    links there for the datasets its plan could not place."""
    datasets = squeezed(FILES["datasets.js"])
    assert has('const DS_FILTER = { state: "", modality: "", dims: "", licence: "", lacks: "" };', datasets)
    assert has('(x.lacks || []).map(name => pill(`no ${name}`, "warn"))', datasets)  # every lack is marked
    assert has('name === "lacks" ? "lacks…"', datasets)
    assert ("GET", "/catalog/datasets") in requested_paths()
    # step 2 of a buffer counts them and sends the person to Datasets; it never lists them
    assert has('href: "#datasets?lacks=anything"', squeezed(FILES["designer.js"]))
    assert has("m.record_lacks", squeezed(FILES["designer.js"]))


def test_a_list_of_datasets_shows_its_actions_only_when_something_is_ticked() -> None:
    """With nothing checked a list of datasets says nothing: the header's check takes the page, and no button
    repeats it. With a check, a box of its own colour says how many datasets are selected and offers Resume and
    Restart; the Datasets list also offers Create buffer, a buffer's Prepare step files the request for its buffer. Both lists
    have the same columns, and none links a folder."""
    empty, lists, labels = run_js(
        "const { S } = await import(input.api);\n"
        "S.meta = { datasets_repo: 'o/data' };\n"
        "const chosen = new Set();\n"
        "const { box, show } = chosenBox(chosen);\n"
        "show();\n"
        "const empty = box.hidden;\n"
        "const shown = owner => { const made = chosenBox(new Set(['u937', 'bcss']), null, owner); made.show();\n"
        "  return [made.box.hidden, walk(made.box).find(node => node.tag === 'b').textContent, textOf(made.box)]; };\n"
        "print([empty, [shown(undefined), shown(7)], datasetCols().map(col => col.label)]);",
        {"datasets.js": "chosenBox, datasetCols"},
        {"api": (UI_DIR / "api.js").as_uri()},
    )
    assert empty is True
    assert [(hidden, count) for hidden, count, _ in lists] == [(False, "2 datasets selected")] * 2
    assert [text for _, _, text in lists] == ["ResumeRestartCreate buffer", "ResumeRestart"]
    assert labels[:5] == ["dataset", "issue", "preview", "state", "stage"] and "files" not in labels
    datasets, flow = squeezed(FILES["datasets.js"]), squeezed(FILES["flow.js"])
    assert has("cols: datasetCols()", datasets) and has("cols: datasetCols()", flow)
    assert has("chosenBox(CHOSEN)", datasets) and has("chosenBox(chosen, redraw, subject.number)", flow)
    assert not has('"Select all")', datasets) and not has("Clear selection") and has(".chosen[hidden]{display:none}")


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
        "cropsTrain": 5000, "cropsVal": 300, "cropsTest": 200, "maxCrops": 64, "holdout": "u937, a: b\nbcss",
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
    assert spec.sampling.crops == {"train": 5000, "val": 300, "test": 200}
    assert spec.sampling.max_crops_per_dataset == 64 and spec.splits.holdout_ids == ["u937", "a:", "b", "bcss"]
    assert spec.plan.auto_approve is False  # a person approves, unless the spec's author edits the line
    # a catalog repo is any string, and a name is trimmed: what was typed between the n's comes back
    for text, written in zip(AWKWARD, named, strict=True):
        read = load_spec_text(written)
        assert (read.catalog.repo, read.name) == (text, f"n {text} n"), text
    assert load_prep_text(prep_text).records == [text for text in AWKWARD if text]


def test_the_controls_are_filled_from_a_spec_and_say_what_they_cannot_hold() -> None:
    """A definition opened or pasted as YAML: the controls take their values from the spec as the service parsed
    it (fromSpec), and a spec the controls write comes back from them unchanged. For a spec written by hand,
    specDiff names what the controls cannot say, so the page keeps its text."""
    from mia_buffers.spec import load_spec_text

    def parsed(text: str) -> dict:
        return load_spec_text(text).model_dump(mode="json", by_alias=True, exclude={"warnings"})

    ref, controls = "a" * 40, {
        "name": "em pool", "paths": "datasets/2D/** */u937.yaml", "dims": ["2D", "3D"], "lic": ["CC0-1.0", "MIT"],
        "allowUnknown": True, "annRequired": True, "sources": ["manual", "proofread"], "minVox": 1000000,
        "patch": 128, "level": 8, "patchZ": 16, "dedup": False, "T": 0.25,
        "cropsTrain": 5000, "cropsVal": 300, "cropsTest": 200, "maxCrops": 64, "holdout": "u937 bcss",
        "exclude": ["bcss", "u937"],
    }  # fmt: skip
    sets = (
        "const filled = c => ({ ...c, dims: new Set(c.dims), lic: new Set(c.lic), sources: new Set(c.sources),"
        " exclude: new Set(c.exclude) });\n"
    )
    lists = (
        "const listed = c => ({ ...c, dims: [...c.dims], lic: [...c.lic], sources: [...c.sources],"
        " exclude: [...c.exclude].sort() });\n"
    )
    written = run_js(
        sets + "print(toSpecYaml(filled(input.controls), input.ref, 'o/catalog'));",
        {"spec.js": "toSpecYaml"},
        {"controls": controls, "ref": ref},
    )
    by_hand = (Path(__file__).parent / "fixtures" / "buffers" / "spec.yaml").read_text()
    back, same, lost = run_js(
        sets + lists + "const again = fromSpec(input.written), hand = fromSpec(input.hand);\n"
        "print([listed(again), specDiff(input.written, input.rewritten),\n"
        "  [listed(hand), toSpecYaml(hand, input.hand.catalog.ref, input.hand.catalog.repo)]]);",
        {"spec.js": "toSpecYaml, fromSpec, specDiff"},
        {"written": parsed(written), "rewritten": parsed(written), "hand": parsed(by_hand)},
    )
    assert back == controls and same == []
    hand_controls, hand_text = lost
    assert hand_controls["name"] == "microscopy-foundation-pretrain" and hand_controls["level"] == 4
    assert load_spec_text(written).select.exclude.ids == ["bcss", "u937"]  # an unchecked dataset is an exclusion
    differs = run_js(
        "print(specDiff(input.hand, input.rewritten));",
        {"spec.js": "specDiff"},
        {"hand": parsed(by_hand), "rewritten": parsed(hand_text)},
    )
    assert "sampling.patch.c" in differs and "name" not in differs and "select.where.license.in" not in differs


def test_a_buffer_is_one_page_and_the_tabs_name_no_spec_and_no_preparation() -> None:
    """The Buffers tab lists buffers, each opening its own page of six steps; there is no list of specs, no list
    of every round's buffer and no Preparation tab. The requests stay reachable from Datasets."""
    index, flow = FILES["index.html"], squeezed(FILES["flow.js"])
    assert 'data-tab="preparation"' not in index and 'data-view="designer"' not in index
    assert all(f'data-step="{step}"' in index for step in ("define", "datasets", "prepare", "plan", "source", "serve"))
    assert has('el("a", { href: "#preparation"', squeezed(FILES["datasets.js"]))
    assert count('pageOf("/subjects/buffer", "subjects")') == 1 and not has('source: pageOf("/buffers", "buffers"),')
    assert has('el("a", { class: "btn primary", href: "#buffers/new" }, "New buffer")')
    assert not has("Buffer specs") and not has("specControls")
    # an old address of a buffer's page still opens it
    assert not has('"#specs/"') and has('if (name === "specs" && rest.length)')
    assert has('buffer: key => "#buffer/" + encodeURIComponent(key)')
    assert "/#buffer/" in (UI_DIR.parent / "mirror.py").read_text()
    # the strip, and what is not a step
    assert has("stripOf(subject?.stage, step)", flow) and has("stepToShow(subject.stage, asked)", flow)
    assert has('el("summary", { class: "btn" }, "Actions…")', flow) and has("actionBar(subject, redraw, ", flow)
    assert has("bar.prepend(", flow) and has('document.querySelectorAll("details.more[open]")')  # Clone first; closes
    assert has("timelineCard(entries)", flow) and has("shelvedNotice(subject.shelved)", flow)
    # the stream redraws the buffer's page, not a subject's
    assert has("if (onScreen.redraw) onScreen.redraw();") and has("onScreen.redraw = null;")


def test_the_definition_is_controls_and_yaml_over_one_text() -> None:
    """Steps 1 and 2. The definition is one text with two views: the Controls tab writes it, the YAML tab is read
    back into the controls by the service. Nothing is saved before step 2's button, and a saved buffer's change is
    an `edit`."""
    index, designer, flow = FILES["index.html"], squeezed(FILES["designer.js"]), squeezed(FILES["flow.js"])
    assert all(f'id="{name}"' in index for name in ("tabControls", "tabYaml", "specText", "controlsLock", "kpisFit"))
    assert 'id="filterBox"' not in index and not has("Create buffer", designer) and not has("designFrom")
    # one text: what the YAML tab holds while that is not what the controls write, else what they write
    assert has("export const definition = () => TEXT ?? toSpecYaml(C, S.ref, S.meta.catalog_repo);", designer)
    assert has('api("/specs/parse"', designer) and has("Object.assign(C, fromSpec(parsed));", designer)
    assert has("spec: definition(), page", designer)  # the plan is of the definition, not of the controls alone
    # a control was changed: the controls are the definition again
    assert has("function touched() { TEXT = null; INVALID = null;", designer)
    # YAML that does not parse: the controls keep their values, nothing is planned or sent, the way on is closed
    assert has("INVALID = error.message.split", designer) and has("if (INVALID) return", designer)
    assert has("export const problem = () =>", designer) and has("disabled: !!problem()", flow)
    # YAML that holds what no control shows: the controls are read-only, and say what they cannot show
    assert has('$("controlsLock").disabled = LOCKED || LOST.length > 0;', designer)
    assert has("the controls do not show", designer)
    # a redraw the stream asked for leaves a definition someone is changing alone
    assert has("if (changed()) return;", designer)
    # the four numbers of step 1, and the check of step 2 that takes a dataset out
    assert has('$("kpisFit").replaceChildren(tile("Datasets that fit"', designer)
    assert has("C.exclude.add(x.id);", designer) and has("C.exclude.delete(id);", designer)
    # saving: a new subject and no action; a saved buffer's change is an edit that names the revision it was made at
    assert has("body: JSON.stringify({ spec: definition() })", flow) and has('api("/specs", {', flow)
    assert has('{ action: "edit", spec: definition(), revision: baseRevision(), key: newKey() }', flow)


def test_step_three_prepares_for_the_buffer_and_goes_on_with_what_is_prepared() -> None:
    """Prepare lists the datasets that fit, each with a check as on the Datasets list, and the requests this
    buffer filed. A request filed there names the buffer and leaves the person on the step. The way forward plans
    over whatever is prepared now, and always stops at the plan."""
    flow, actions = squeezed(FILES["flow.js"]), squeezed(FILES["actions.js"])
    assert has("fit_only: true", flow) and not has("to_prepare: true", flow)
    assert has('pageOf("/subjects/prep", "subjects", () => ({ buffer: subject.number }))', flow)
    assert has("...(owner == null ? {} : { buffer: owner })", actions)
    assert has('if (owner == null) location.hash = "#find/" + action.number;', actions)
    # any row can be checked, prepared or at review too
    assert has("select: { chosen, id: x => x.id, onchange: show }", flow)
    # both lists file the same requests: Restart's says so, Resume's says nothing
    assert has('(restart ? "restart: true\\n" : "") +', actions) and not has("again: true", actions)
    assert has('{ action: "go", plan_first: true, revision: subject.revision, key: newKey() }', flow)
    assert count("plan_first: true") == 1 and has("title: BUTTON_HELP.plan", flow)  # every plan stops for its Run
    assert has("disabled: !!blocker || !!subject.asked", flow)


def test_steps_four_to_six_are_the_rounds_own_buttons_and_back_is_a_cancel() -> None:
    """Plan, Source and Serve are the round's stages. Forward is the approve of that stage (the first after the
    host check); back from Plan and Source asks, then cancels the round; Serve names the version by its id."""
    flow = squeezed(FILES["flow.js"])
    assert has("askHosts({ spec: subject.key }, hosts =>", flow)
    assert has("...(hosts.length ? { approve_hosts: hosts } : {})", flow)
    assert has('{ action: "approve", revision: subject.revision, key: newKey() }', flow)
    assert has('disabled: subject.stage !== "staged" || !!subject.asked', flow)  # Approve and serve
    assert has('sendAction(subject, { action: "cancel", key: newKey() }, redraw)', flow) and has("if (!confirm(", flow)
    assert has("droppedNotices(subject, board, redraw)", flow) and has("poolTable(version.id", flow)
    # the plan is shown as the round wrote it, as text
    assert has("/^# Plan for /.test(entry.body)", flow) and has("markdown(plan.body", flow)
    # step 6: the id a training run names, never the buffer's name
    assert has('buffer="${version.id}"', flow) and has("versionsCard(subject)", flow)
    assert count("plan_first: true") == 1  # one place builds a plan's go: step 3 and "Plan a new version" share it


DESIGNER_STUB = """
// the service, as the designer reaches it: /specs/parse answers the parsed spec of a known text (of any other, the
// fallback's: the text the controls write for it), /plan an empty plan
const { S } = await import(input.api);
S.ref = "a".repeat(40);
globalThis.fetch = async (path, options) => {
  const body = JSON.parse(options.body);
  const answer = path === "/plan" ? { rows: [], total: 0, summary: {} } : { spec: input.parsed[body.spec] || input.fallback };
  return { ok: true, status: 200, text: async () => JSON.stringify(answer) };
};
const later = ms => new Promise(done => setTimeout(done, ms));
"""


def designer(body: str, texts: dict[str, str]) -> Any:
    """Run `body` over designer.js with its controls bound and the service stubbed; `texts` name the definitions
    the stub can parse (`input.texts.<name>`)."""
    from mia_buffers.spec import load_spec_text

    def parsed(text: str) -> dict:
        return load_spec_text(text).model_dump(mode="json", by_alias=True, exclude={"warnings"})

    return run_js(
        DESIGNER_STUB + "bindControls();\n" + body,
        {"designer.js": "bindControls, ensureDefinition, definition, changed, baseRevision, showDesigner"},
        {
            "api": (UI_DIR / "api.js").as_uri(),
            "texts": texts,
            "parsed": {text: parsed(text) for text in texts.values()},
            "fallback": parsed(next(iter(texts.values()))),
        },
    )


def _definitions() -> dict[str, str]:
    by_hand = (Path(__file__).parent / "fixtures" / "buffers" / "spec.yaml").read_text()
    return {"first": by_hand, "second": by_hand.replace("name: ", "name: other-", 1)}


def test_a_definition_changed_elsewhere_is_not_overwritten_by_an_edit_made_before_it() -> None:
    """Two tabs. While a person changes a buffer's definition, a redraw brings a newer stored text (someone
    else's edit). What they typed is kept, and it keeps the revision it was made at: the service refuses their
    save. A definition nobody is changing follows the stored one, with its revision."""
    kept, moved, followed = designer(
        "const at = (revision, text) => ({ key: 'a', revision, actions: ['edit'], data: { spec: text } });\n"
        "await ensureDefinition(at(1, input.texts.first));\n"
        "const name = document.getElementById('specName');\n"
        "name.value = 'typed here'; name.fire('change');\n"  # the person changes a control
        "await ensureDefinition(at(2, input.texts.second));\n"  # the other tab's edit arrives
        "const kept = [changed(), definition().includes('typed here'), baseRevision()];\n"
        "await ensureDefinition(at(3, definition()));\n"  # this page's own text is the stored one now
        "const moved = [changed(), baseRevision()];\n"
        "await ensureDefinition(at(4, input.texts.second));\n"  # nobody is typing: the stored text is shown
        "print([kept, moved, [changed(), definition() === input.texts.second, baseRevision()]]);",
        _definitions(),
    )
    assert kept == [True, True, 1] and moved == [False, 3] and followed == [False, True, 4]
    assert has("revision: baseRevision()", squeezed(FILES["flow.js"]))


def test_a_redraw_leaves_a_number_being_typed_alone() -> None:
    """A crop count is taken when its field is left. A redraw of the step (the stream, the poll) before that does
    not put the old number back into the field."""
    typed = designer(
        "const field = document.getElementById('cropsTrain');\n"
        "field.value = '25000';\n"  # typed, the field not left yet
        "showDesigner();\n"
        "print(field.value);",
        _definitions(),
    )
    assert typed == "25000"


def test_step_two_lists_the_datasets_that_fit_and_the_others_only_when_asked() -> None:
    """Step 1 counts the datasets that fit, and step 2's table is those datasets. A dataset the patch does not fit,
    or whose record has no array shape, is counted where datasets leave and is listed only once the person asks."""
    asked, shown = designer(
        "const pages = [];\n"
        "const service = globalThis.fetch;\n"
        "globalThis.fetch = async (path, options) => {\n"
        "  if (path === '/plan') pages.push(JSON.parse(options.body).page);\n"
        "  return service(path, options);\n"
        "};\n"
        "showDesigner(); await later(50);\n"
        "const others = walk(document.getElementById('planTable')).find(node => node.tag === 'input' && node.attrs.type === 'checkbox');\n"
        "others.checked = true; others.fire('change'); await later(50);\n"
        "print([pages.map(page => page.fit_only), textOf(document.getElementById('planTable'))]);",
        _definitions(),
    )
    assert asked == [True, False]
    assert "also list the datasets that do not fit" in shown
    assert "The datasets that fit this definition" in FILES["index.html"]


def test_yaml_typed_into_one_buffer_never_lands_in_the_next_one_opened() -> None:
    """The YAML tab is read after a pause in typing. Opening another buffer within that pause drops the read: the
    buffer opened shows its own definition, unchanged."""
    shown = designer(
        "const at = (key, text) => ({ key, revision: 1, actions: ['edit'], data: { spec: text } });\n"
        "await ensureDefinition(at('a', input.texts.first));\n"
        "const box = document.getElementById('specText');\n"
        "box.value = input.texts.second; box.fire('input');\n"  # typed into a's YAML tab
        "await ensureDefinition(at('b', input.texts.first));\n"  # b is opened before the pause ends
        "await later(700);\n"
        "print([definition() === input.texts.first, changed()]);",
        _definitions(),
    )
    assert shown == [True, False]


def test_prepare_plans_what_is_stored_and_the_versions_are_under_every_step() -> None:
    """Step 3's numbers and its way forward are of the stored definition, which is what the round will run, not of
    a change that was made on steps 1 and 2 and not saved. A buffer's versions are listed under every step: after a
    round that failed or was cancelled the buffer rests at Prepare, and its versions are still one click away."""
    flow = squeezed(FILES["flow.js"])
    prepare = flow[flow.index("functionprepareStep(") : flow.index("//----step4")]
    assert has("JSON.stringify({ spec: subject.data.spec, page })", prepare) and "definition()" not in prepare
    assert "ensureDefinition" not in prepare
    assert has("[versionsCard(subject), timelineCard(entries)]", flow) and count("versionsCard(subject)", flow) == 1


def test_the_page_says_buffer_and_definition_and_explains_nothing_by_its_past() -> None:
    """What a person reads names a buffer and its definition, a dataset is selected, in a version or on disk, and
    no comment says how the page once was."""
    assert not re.search(r"in the buffer\b", FILES["index.html"], re.I)
    assert not has("the definition's buffers") and not re.search(r"before it was|used to be", PAGE)
    service = (UI_DIR.parent / "api.py").read_text()
    for said in ('f"no buffer spec', 'f"not a buffer spec', 'f"buffer spec {key!r}'):
        assert said not in service, said


def test_a_spec_that_says_why_it_excludes_is_more_than_the_ticks_can_hold() -> None:
    """Unchecking a dataset writes its id to `select.exclude.ids`. A reason beside it, or an excluded path, is
    something no control shows: specDiff names it, so the page keeps the text."""
    from mia_buffers.spec import load_spec_text

    def parsed(text: str) -> dict:
        return load_spec_text(text).model_dump(mode="json", by_alias=True, exclude={"warnings"})

    by_hand = (Path(__file__).parent / "fixtures" / "buffers" / "spec.yaml").read_text()
    plain = parsed(by_hand)
    reasoned = {
        **plain,
        "select": {**plain["select"], "exclude": {"paths": [], "ids": ["x"], "reasons": {"x": "blurry"}}},
    }
    checked = {**plain, "select": {**plain["select"], "exclude": {"paths": [], "ids": ["x"], "reasons": {}}}}
    ids, differs = run_js(
        "print([[...fromSpec(input.reasoned).exclude], specDiff(input.reasoned, input.checked)]);",
        {"spec.js": "fromSpec, specDiff"},
        {"reasoned": reasoned, "checked": checked},
    )
    assert ids == ["x"] and differs == ["select.exclude.reasons.x"]


def test_a_stage_opens_at_its_step() -> None:
    """A buffer is six steps. A stage a round works or waits at is the step of that work; every other stage is
    Prepare, where a saved buffer rests. A hash may ask for a step already passed, never one ahead."""
    from mia_buffers.config import STAGE_ACTIONS

    steps, at, shown, words, can, strips = run_js(
        "print([STEPS, Object.fromEntries(input.stages.map(stage => [stage, stepOf(stage)])),\n"
        "  input.asks.map(([stage, asked]) => stepToShow(stage, asked)),\n"
        "  input.subjects.map(statusWord), input.subjects.map(editable).concat(editable(null)),\n"
        "  input.strips.map(([stage, shown]) => stripOf(stage, shown).map(item => item.state).join(' '))]);",
        {"steps.js": "STEPS, stepOf, stepToShow, statusWord, editable, stripOf"},
        {
            "stages": list(STAGE_ACTIONS),
            "asks": [
                ["plan-review", "serve"],
                ["plan-review", "define"],
                ["ready", "bogus"],
                ["new", None],
                [None, "prepare"],
                [None, "datasets"],
                [None, None],
            ],  # fmt: skip
            "strips": [["plan-review", "plan"], ["plan-review", "define"], [None, "define"], ["ready", "serve"]],
            "subjects": [
                {"stage": "new", "status": "not started", "actions": ["edit", "go"]},
                {"stage": "plan-review", "status": "needs human", "actions": ["approve"]},
                {"stage": "sourcing", "status": "working", "actions": ["cancel"]},
                {"stage": "ready", "status": "done", "actions": ["go"]},
                {"stage": "idle", "status": "needs human", "actions": ["keep"]},
                {"stage": "needs-human", "status": "in repair", "actions": ["edit", "go"]},
                {"stage": "retired", "status": "done", "actions": ["edit", "go"]},
                {"stage": "cancelled", "status": "shelved", "actions": ["unshelve"], "shelved": {"reason": "later"}},
            ],
        },
    )
    assert steps == ["define", "datasets", "prepare", "plan", "source", "serve"]
    assert at == {
        "new": "prepare", "planning": "plan", "plan-review": "plan", "sourcing": "source", "staged": "source",
        "ready": "serve", "idle": "serve", "retired": "prepare", "needs-human": "prepare", "cancelled": "prepare",
    }  # fmt: skip
    for stage, (status, _actions) in STAGE_ACTIONS.items():  # a round in progress is never shown as resting
        assert (at[stage] == "prepare") == (status not in ("working", "waiting") and stage not in ("ready",)), stage
    # a buffer that is not saved (no stage) has its first two steps and opens at the first
    assert shown == ["plan", "define", "serve", "prepare", "define", "datasets", "define"]
    assert strips == [
        "done done done here ahead ahead", "here done done done ahead ahead", "here done ahead ahead ahead ahead",
        "done done done done done here",
    ]  # fmt: skip
    assert words == [
        "saved, not started", "needs human", "working", "serving", "serving", "in repair", "retired", "shelved",
    ]  # fmt: skip
    assert can == [True, False, False, False, False, True, True, False, True]  # an unsaved buffer is editable


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


PLANTED = "<script>alert(1)</script>"
JOB_ADDRESS = "issue-12/i12-e345/match-1"
JOB_RECORD = {
    "schema": 1,
    "job_id": "777",
    "workspace": JOB_ADDRESS,
    "submitted_at": "2026-11-14T10:00:00+00:00",
    "finished_at": "2026-11-14T10:05:00+00:00",
    "request": {"name": "match", "command": "claude -p … > claude.jsonl", "cpus": 2, "env": ["ANTHROPIC_API_KEY"]},
    "submit": {"queue": "local", "estimated_max_cost_usd": 0.1, "job_group": None},
    "outcome": {"state": "EXIT", "exit_code": 1, "exec_host": "node-1", "queue": "local"},
    "usage": {"max_mem": "2 Gbytes", "run_time": "61 second(s)"},
    "logs": {"stdout": "out-tail", "stderr": ""},
    "agent": {
        "prompt": "Match this dataset.",
        "model": "claude-opus",
        "cost_usd": 0.25,
        "num_turns": 3,
        "duration_ms": 1200,
        "turns": [
            {"role": "thinking", "text": "look at the record first"},
            {"role": "tool", "name": "Read", "input": {"file_path": "record.yaml"}, "error": False},
            {"role": "tool", "name": "Write", "input": {"content": PLANTED + "x" * 200}, "error": True},
            {"role": "assistant", "text": f"done\n{PLANTED}", "sub": True},
            "not a turn",
            {"role": "unheard-of"},
        ],
    },
}
# a record's page as a test reads it: its HTML text, the tags it is built of, its text, and each fold
JOB_SHOWN = (
    "const shownOf = root => ({ html: html(root), tags: [...new Set(walk(root).map(node => node.tag))],"
    " text: textOf(root), folds: walk(root).filter(node => node.tag === 'details').map(fold =>"
    " ({ open: 'open' in fold.attrs, cls: fold.attrs.class, summary: textOf(fold.children[0]),"
    " body: textOf(fold.children[1]) })) });\n"
)


def test_a_jobs_record_is_shown_as_text_never_as_markup() -> None:
    """What a job printed stays text: a tag in a turn or in a tool's input makes no element. An agent's prompt, its
    thinking and each tool call are closed folds; a failed call says so and a sub-agent's turn is indented."""
    full, bare = run_js(
        JOB_SHOWN
        + "print(input.map(record => shownOf(el('div', {}, ...jobView(record, 'issue-12/i12-e345/match-1')))));",
        {"dom.js": "el", "job.js": "jobView"},
        [JOB_RECORD, {"schema": 1, "request": {"command": "sleep 1"}, "submit": {"queue": "local"}}],
    )
    assert "script" not in full["tags"] and "<script" not in full["html"] and full["text"].count(PLANTED) == 3
    assert f'<div class="turn said subagent">done\n{PLANTED.replace("<", "&lt;")}</div>' in full["html"]
    assert full["html"].startswith(f'<div><h2>match</h2><div class="small mono">{JOB_ADDRESS}</div><dl class="kv">')
    assert '<dt>state</dt><dd><span class="pill warn">EXIT</span></dd><dt>exit code</dt><dd>1</dd>' in full["html"]
    for shown in ("<dd>local</dd>", "<dd>node-1</dd>", "<dd>777</dd>", "<dd>2026-11-14T10:00:00+00:00 (", "<dd>3</dd>"):
        assert shown in full["html"], shown
    for shown in ("<dt>peak memory</dt><dd>2 Gbytes</dd>", "<dt>run time</dt><dd>61 second(s)</dd>", "<dd>$0.25</dd>"):
        assert shown in full["html"], shown
    assert "average memory" not in full["text"] and "still running" not in full["text"]  # only what the record has
    prompt, thinking, read, write = full["folds"]
    assert not any(fold["open"] for fold in full["folds"])
    assert (prompt["summary"], prompt["body"]) == ("Prompt", "Match this dataset.")
    assert (thinking["summary"], thinking["body"]) == ("Thinking", "look at the record first")
    assert (read["summary"], read["body"]) == ('Read {"file_path":"record.yaml"}', '{\n  "file_path": "record.yaml"\n}')
    whole = json.dumps({"content": PLANTED + "x" * 200}, separators=(",", ":"))
    assert write["summary"] == f"Write {whole[:120]}… failed" and write["body"].count("x") == 200
    assert "unheard-of" not in full["text"] and "not a turn" not in full["text"]
    assert "<h2>Script</h2><pre>claude -p … > claude.jsonl</pre>" in full["html"]
    assert '<dt>cpus</dt><dd>2</dd><dt>env</dt><dd>["ANTHROPIC_API_KEY"]</dd>' in full["html"]
    assert "<dt>estimated_max_cost_usd</dt><dd>0.1</dd>" in full["html"] and "job_group" not in full["text"]
    assert '<div class="sub">stdout</div><pre>out-tail</pre>' in full["html"] and "stderr" not in full["text"]
    # a record with its first half alone: the job's name is its workspace's, and nothing is said about an agent
    assert bare["html"].startswith("<div><h2>match-1</h2>")
    assert "This job is still running, or its end was not recorded." in bare["text"]
    assert "Conversation" not in bare["text"] and "Output" not in bare["text"] and bare["folds"] == []
    assert '"./markdown.js"' not in FILES["job.js"]  # nothing of a record is read as markdown


def test_a_jobs_record_is_read_from_its_address_and_a_job_without_one_says_so() -> None:
    """`#job/<address>` is the whole remainder after `#job/`, slashes and all, and the page reads `/jobs/<address>`
    (a read route); a 404 is a job that left no record. A round's job links the subject its round is of: the
    number in its address, looked up, in the kind its service and round say."""
    assert (
        has("job: viewJob")
        and has('const arg = decoded(rest.join("/"))')
        and has('data-view="job"')
        and has('id="jobBody"')
    )
    assert ("GET", "/jobs/x") in requested_paths() and resolve("GET", f"/jobs/{JOB_ADDRESS}") == "read"
    found = run_js(
        "globalThis.location = { hash: '#job/issue-12/i12-e345/match-1' };\n"
        "const asked = [], out = [], body = $('jobBody');\n"
        "for (const answer of input.answers) {\n"
        "  const lookup = { ok: true, status: 200, body: JSON.stringify(input.lookup) };\n"
        "  globalThis.fetch = async path => { asked.push(path);\n"
        "    const sent = path.startsWith('/lookup') ? lookup : answer; return { ...sent, text: async () => sent.body }; };\n"
        "  await viewJob('issue-12/i12-e345/match-1');\n"
        "  out.push({ asked: asked.splice(0), text: textOf(body), html: html(body) });\n"
        "}\n"
        "print([...out, subjectOf({}, 'issue-12/p12-e3/plan-1'), subjectOf({}, 'cleaner/case-1/diagnose-0')]);",
        {"job.js": "viewJob, subjectOf", "dom.js": "$"},
        {
            "answers": [
                {"ok": True, "status": 200, "body": json.dumps({"service": "mia-data", "request": {"name": "match"}})},
                {"ok": False, "status": 404, "statusText": "Not Found", "body": '{"error": "no record for this job"}'},
            ],
            # issue numbers repeat across the kinds: the job's own kind is the one linked
            "lookup": {
                "subjects": [
                    {"kind": "buffer", "number": 12, "key": "spec-a"},
                    {"kind": "dataset", "number": 12, "key": "ds a"},
                ]
            },
        },
    )
    found, missing, request, cleaner = found
    assert found["asked"] == [f"/jobs/{JOB_ADDRESS}", "/lookup?q=12"] and missing["asked"] == [f"/jobs/{JOB_ADDRESS}"]
    assert found["text"].startswith("match" + JOB_ADDRESS + "dataset ds aThis job is still running")
    assert '<a href="#datasets/ds%20a">ds a</a>' in found["html"] and "spec-a" not in found["text"]
    assert missing["text"] == "match-1No record for this job."
    assert (request, cleaner) == ({"kind": "prep", "number": 12}, None)
