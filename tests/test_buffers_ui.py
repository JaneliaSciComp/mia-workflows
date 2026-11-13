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
    assert table.startswith('<details open=""><summary>2 crops</summary><div') and "<p>after the fold</p>" in table
    assert "&lt;" not in table and "!" not in table and "github.com" not in table
    for name in ("raw", "overlay"):
        assert f'<a href="{local}/{name}.png" target="_blank" rel="noopener"' in table
        assert f'<img class="crop" src="{local}/{name}.png" loading="lazy" alt="{name} of crop i7-e99-b0-000">' in table
    assert "<img" not in other and "!" not in other
    assert '<a href="https://tracker.example/pixel.png" target="_blank" rel="noopener">logo</a>' in other


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


def test_the_page_asks_which_hosts_a_run_lacks_before_it_starts() -> None:
    """A start action first asks the service which hosts the run needs that the policy lacks."""
    assert ("POST", "/hosts/missing") in requested_paths() and resolve("POST", "/hosts/missing") == "read"
    assert PAGE.count("askHosts(") >= 3
    for body in (
        "{ records: [subject.key] }",
        "{ prep: subject.number }",
        "{ spec: subject.key }",
        "{ records: todo }",
    ):
        assert body in PAGE


def test_the_hosts_dialog_offers_approve_start_anyway_and_cancel() -> None:
    """The dialog lists the hosts with Select all; Approve and start waits for a ticked host."""
    for text in (
        "Approve and start",
        "Start without approving",
        "Select all",
        "Hosts to approve",
        "approveButton.disabled = ",
    ):
        assert text in PAGE
    for key in ("approveHosts:", "startUnapproved:", "cancelHosts:"):
        assert key in PAGE


def test_an_approval_travels_with_the_start_action() -> None:
    """The approved hosts ride on the start action itself; the page has no route of its own to approve them."""
    assert "approve_hosts" in PAGE and '"/hosts/approve"' not in PAGE


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
    """Run the page's `askHosts` under node with a fake DOM, `api` and `banner`: for each scenario (`hosts`, or
    `fail: [status, message]`, then `steps` on the dialog) report what `send` got, the banner, the dialog and settling."""
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    helpers = "\n".join(line for line in PAGE.splitlines() if line.startswith("const el = "))
    start = PAGE.index("async function askHosts(")
    asking = PAGE[start : PAGE.index("// what the service is asked about", start)]
    help_texts = PAGE[PAGE.index("const BUTTON_HELP = {") : PAGE.index("const ACTION_HELP = {")]
    script = (
        f"{HOSTS_DOM}\n{helpers}\n{help_texts}\nconst JSON_HEADERS = {{}};\nlet fake, banners = [];\n"
        "const api = async () => { if (fake.fail) { const e = new Error(fake.fail[1]); e.status = fake.fail[0]; throw e; } return { hosts: fake.hosts || [] }; };\n"
        "const banner = message => banners.push(message);\n"
        f"{asking}\n"
        'const scenarios = JSON.parse(require("fs").readFileSync(0, "utf8"));\n'
        "(async () => { const out = [];\n"
        "for (fake of scenarios) { banners = []; body.kids = []; const sent = []; let settled = false;\n"
        "  const done = askHosts({ records: [] }, hosts => sent.push(hosts)).then(() => { settled = true; });\n"
        "  await new Promise(r => setImmediate(r));\n"
        "  const box = body.kids[0], seen = { shown: !!box };\n"
        "  if (box) { const approve = byText(box, 'Approve and start'), ticks = walk(box).filter(n => n.attrs.type === 'checkbox');\n"
        "    seen.rows = ticks.slice(1).length; seen.disabledAtStart = approve.disabled;\n"
        "    for (const step of fake.steps) { if (step === 'all') { ticks[0].checked = true; ticks[0].fire('change'); }\n"
        "      else if (typeof step === 'number') { ticks[step + 1].checked = true; ticks[step + 1].fire('change'); } else box.close(step); }\n"
        "    seen.enabledAfterTicks = !approve.disabled; seen.allChecked = ticks[0].checked; seen.text = textOf(box); }\n"
        "  await done;\n"
        "  out.push({ ...seen, sent, banners, settled, removed: body.kids.length === 0 }); }\n"
        "console.log(JSON.stringify(out)); })();\n"
    )
    done = subprocess.run([node, "-e", script], input=json.dumps(scenarios), capture_output=True, text=True, check=True)
    return json.loads(done.stdout)


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
    assert PAGE.count("if (checking) return;") == 2 and PAGE.count("checking = true;") == 2
    assert PAGE.count("checking = false; })") == 2


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


def test_a_subjects_page_says_where_it_stands_with_its_lifecycle_alone() -> None:
    """No status pill, no "waiting on you", no "workflow running" under the line: every link of a subject stands
    in the row under its name, and a table names a subject's stage, not a status word."""
    assert "subjectFacts" not in PAGE and "waiting on you" not in PAGE and '"workflow " +' not in PAGE
    assert "statePill(subject.status)" not in PAGE and 'statePill(x.status), " ", x.stage' not in PAGE
    assert PAGE.count("stagePill(x.stage)") == 3  # the specs, the requests and Needs you
    assert "subjectLinks(subject, entries)" in PAGE


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

    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    block = PAGE[PAGE.index("const ACTION_HELP = {") : PAGE.index("const actionHelp = ")]
    names = "Object.fromEntries(Object.entries(ACTION_HELP).map(([kind, help]) => [kind, Object.keys(help)]))"
    done = subprocess.run(
        [node, "-e", f"{block}\nconsole.log(JSON.stringify({names}));"], capture_output=True, text=True, check=True
    )
    covered = json.loads(done.stdout)
    own_control = {"buffer": {"go", "revoke", "dropped"}}
    for kind, table in (("dataset", DATASET_ACTIONS), ("buffer", BUFFER_ACTIONS), ("prep", PREP_STAGE_ACTIONS)):
        offered = set().union(*(actions for _, actions in table.values())) - own_control.get(kind, set())
        assert offered <= set(covered[kind]), (kind, offered - set(covered[kind]))
    assert "title: actionHelp(subject, action)" in PAGE
    keys = re.findall(
        r"^  (\w+): \"", PAGE[PAGE.index("const BUTTON_HELP = {") : PAGE.index("const ACTION_HELP = {")], re.M
    )
    assert {"plan", "dropped", "revokeTicked", "retry", "prepare", "sendReject"} <= set(keys)
    for key in keys:
        assert f"title: BUTTON_HELP.{key}" in PAGE, key


def test_a_buffers_page_follows_a_datasets_name_links_then_workflow() -> None:
    """A buffer's page reads like a dataset's: its id, a row of links (its spec, the spec's GitHub issue with its
    number, the round's workspace and its staged pyramids), then the labelled workflow. No pill beside the id, no
    row of bare root folders."""
    body = PAGE[PAGE.index("async function viewBuffer(root, id, refresh) {") : PAGE.index("// ---- cache")]
    name, links, flow = body.index('el("h2", {}, id)'), body.index('class: "links"'), body.index('flowRow("buffer", ')
    assert name < links < flow
    assert "ghIcon(meta.buffers_repo, spec.issue, true)" in body and "SUBJECT_HASH.buffer(spec.key)" in body
    assert "Object.entries(roots)" not in body and "all buffers" not in body
    assert 'buffer: "Buffer workflow:"' in PAGE


def test_any_dataset_can_be_ticked_and_the_ticked_prepared_or_made_a_spec() -> None:
    """Every row of the Datasets list has a tick, prepared or not. The ticked that still need it can be prepared
    in one request, and all of them can open New buffer as a spec that selects exactly them: each by a glob on
    its record's file name, which no other record's path matches."""
    from mia_buffers.select import _glob

    assert "select: { chosen: CHOSEN, id: row => row.id, onchange: " in PAGE and "can: PREPARABLE" not in PAGE
    assert "function specFromDatasets(ids)" in PAGE and "ids.map(id => `*/${id}.yaml`)" in PAGE
    assert "title: BUTTON_HELP.specFrom" in PAGE
    patterns = ["*/u937.yaml", "*/bcss.yaml"]
    assert _glob(patterns, "datasets/2D/confocal/u937.yaml") and _glob(patterns, "datasets/bcss.yaml")
    assert not _glob(patterns, "datasets/2D/confocal/not-u937.yaml") and not _glob(patterns, "datasets/u937.yaml.bak")


def test_the_datasets_selection_shows_its_actions_only_when_something_is_ticked() -> None:
    """With nothing ticked the list offers Select all and Clear selection and says nothing more. With a tick, a
    box of its own colour says how many datasets are selected and offers Prepare and Create buffer."""
    assert '"Select all")' in PAGE and "Select all matching" not in PAGE
    assert "tick the datasets to prepare first" not in PAGE
    assert 'class: "chosen", hidden: true' in PAGE and ".chosen[hidden]{display:none}" in PAGE
    assert "box.hidden = !CHOSEN.size" in PAGE and '"Create buffer")' in PAGE
    # Select all takes the rows of the page that is showing, and asks the server for nothing
    assert "datasets match; the first" not in PAGE and 'ids: "1"' not in PAGE
