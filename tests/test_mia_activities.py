import hashlib
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml
from autodev_helpers import GIT_ID, run_git
from mia_helpers import FakeGH, make_cfg, registry_for
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from test_autodev_agent import FakeCsub
from test_buffers_register import token_server  # noqa: F401  (fixture)

from autodev.jobs import CsubJobs
from mia_buffers.api import BufferService
from mia_buffers.ledger import Ledger
from mia_core.registry import Registry, RegistryError
from mia_data.activities import Activities
from mia_data.binding import record_gaps
from mia_data.jobspec import lock_dir
from mia_data.models import (
    DatasetState,
    DebugResult,
    JobArgs,
    JobDone,
    LockTaken,
    Reservation,
    Revocation,
    job_key,
)

REC_SHA, SNIP_SHA = "c" * 40, "a" * 40
RECORD = {
    "data": {"download_url": "https://data.example.org/a.zip", "formats": ["tiff"]},
    "imaging": {"modality": ["confocal"], "organism": ["Homo sapiens"]},
}
BINDING = {
    "record": "rec-1",
    "record_commit": REC_SHA,
    "snippets_commit": SNIP_SHA,
    "reasons": ["r"],
    "raw": [{"snippet": "tiff-zip-member", "urls": ["https://data.example.org/a.zip::m.tif"], "params": {}}],
}
ROUND = "i1-e99"


def _setup(tmp_path, probe=None, csub=None):
    cfg = make_cfg(tmp_path, network={"redirect_hosts": {}}, service={"url": "http://svc.example.org:8765"})
    h = cfg.harness.dir
    (h / "harness").mkdir()
    (h / "harness" / "pixi.lock").write_text("lock v1\n")
    (h / "harness" / "src" / "mia_harness").mkdir(parents=True)
    (h / "harness" / "src" / "mia_harness" / "cli.py").write_text("def main(): return 0\n")
    run_git("init", "-q", "-b", "main", cwd=h)
    run_git("add", "-A", cwd=h)
    run_git(*GIT_ID, "commit", "-qm", "harness", cwd=h)
    dl = tmp_path / "dl"
    dl.mkdir()
    (dl / "conversion.json").write_text('{"axes": "zyx"}')
    downloads = {"downloaded": {"rec-1": {"at": "2026-10-01", "record_commit": REC_SHA, "dataset_dir": str(dl)}}}
    cat = FakeGH(
        "o/catalog",
        files={
            ("state/downloads.json", "state"): json.dumps(downloads).encode(),
            ("datasets/rec-1.yaml", REC_SHA): yaml.safe_dump(RECORD).encode(),
        },
    )
    data = FakeGH(
        "o/data", issues={1: {"number": 1, "body": "<!-- mia-data:record=rec-1 -->\n\n- Record: x", "labels": []}}
    )
    snip = FakeGH("o/snippets", refs={"main": SNIP_SHA})
    snap = cfg.paths.work_root / "_snapshots" / SNIP_SHA  # as snapshot() leaves it
    for name in ("tiff-zip-member", "old-tiff"):
        (snap / "snippets" / name).mkdir(parents=True)
        (snap / "snippets" / name / "snippet.yaml").write_text(f"name: {name}\n")
    (snap / "WITHDRAWN.yaml").write_text("- {name: old-tiff, reason: transposes yx}\n")
    (snap / "SPEC.md").write_text("spec\n")
    csub = csub or FakeCsub()
    jobs = CsubJobs("mia-data", cfg.paths.state_dir / "jobs", client_factory=lambda: csub)
    a = Activities(
        cfg, data, snip, cat, jobs, bot="bot", probe=probe or (lambda: ["*.example.org", *cfg.network.package_hosts])
    )
    ledger = Ledger(tmp_path / "ledger.sqlite")  # buffer-api in this process: dataset 1 is rec-1
    a.registry = registry_for(BufferService(ledger))
    ledger.adopt_subject("dataset", 1, "rec-1")
    t = SimpleNamespace(a=a, cfg=cfg, data=data, snip=snip, cat=cat, csub=csub, ledger=ledger)
    for event_id in (99, 98, 11):  # the rounds these tests admit are named by their go's id
        put_action(t, event_id)
    return t


def put_action(t, action_id: int, number: int = 1, action: str = "go", payload: dict | None = None) -> None:
    """An action with a chosen id in the fixture's ledger."""
    row = (action_id, "dataset", number, action, "page", "2026-11-01T00:00:00+00:00", None, json.dumps(payload or {}))
    with t.ledger._transaction() as db:
        db.execute("INSERT OR REPLACE INTO actions VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'sent', '')", row)


async def test_admit_is_idempotent(tmp_path):
    t = _setup(tmp_path)
    p1 = await t.a.admit(1, "99")
    assert (p1.record, p1.record_commit, p1.snippets_commit) == ("rec-1", REC_SHA, SNIP_SHA)
    rd = t.cfg.paths.state_dir / "rounds" / ROUND
    assert yaml.safe_load((rd / "record.yaml").read_text()) == RECORD
    t.cat.files[("datasets/rec-1.yaml", REC_SHA)] = b"changed: true\n"
    t.snip.refs["main"] = "b" * 40  # main moved: a retried admission keeps its pins
    assert await t.a.admit(1, "99") == p1
    assert yaml.safe_load((rd / "record.yaml").read_text()) == RECORD
    assert [g for g in t.cat.gets if g[0].startswith("datasets/")] == [("datasets/rec-1.yaml", REC_SHA)]


async def test_admit_charges_the_root_its_go_carries_or_the_dataset_itself(tmp_path):
    t = _setup(tmp_path)
    put_action(t, 98, payload={"root": "o/buffers#9"})
    assert (await t.a.admit(1, "98")).root == "o/buffers#9"
    assert (await t.a.admit(1, "11")).root == f"{t.cfg.github.data_repo}#1"
    with pytest.raises(ApplicationError, match="event 12 is not an action") as error:
        await t.a.admit(1, "12")
    assert error.value.non_retryable and str(error.value).count("ValueError") == 1  # the type is named once


async def _match_ws(t, outcome, binding=None):
    pins = await t.a.admit(1, "99")
    ws = Path(await t.a.prepare_match(1, ROUND, pins, 1))
    assert sorted(p.name for p in (ws / "catalog").iterdir()) == ["tiff-zip-member.yaml"]  # withdrawn hidden
    assert (ws / "conversion.json").read_text() == '{"axes": "zyx"}' and "rec-1" in (ws / "prompt.md").read_text()
    if outcome is not None:
        (ws / "outcome.json").write_text(json.dumps(outcome))
    if binding is not None:
        (ws / "binding.yaml").write_text(yaml.safe_dump(binding))
    return pins, ws


async def test_prepare_debug_gives_the_agent_what_the_rounds_jobs_left(tmp_path):
    t = _setup(tmp_path)
    pins, ws = await _match_ws(t, {"outcome": "matched"}, BINDING)
    match = await t.a.read_match(str(ws))
    crop = Path(await t.a.prepare_job(1, ROUND, "crop-abc", 1, match.binding_path))
    (crop / "crops").mkdir()
    (crop / "crops" / "report.json").write_text('{"errors": ["ReadTimeout"]}')
    (crop / "exit_code").write_text("1\n")
    (crop / ".csub" / "jobs" / "155").mkdir(parents=True)
    (crop / ".csub" / "jobs" / "155" / "stderr").write_text("x" * 100_000 + "the end")
    (crop / "notes.txt").write_text("not one of a job's reports")
    (tmp_path / "elsewhere.json").write_text("{}")
    (crop / "verify.json").symlink_to(tmp_path / "elsewhere.json")  # a job could plant a link
    failure = "crop job 155 ended EXIT (exit 1): ReadTimeout"
    debug = Path(await t.a.prepare_debug(1, ROUND, pins, failure, match.binding_path))
    assert debug.name == "debug-1" and sorted(p.name for p in (debug / "round").iterdir()) == ["crop-abc-1", "match-1"]
    left = debug / "round" / "crop-abc-1"
    assert sorted(p.name for p in left.iterdir()) == ["binding.yaml", "crops", "exit_code", "stderr"]
    assert json.loads((left / "crops" / "report.json").read_text()) == {"errors": ["ReadTimeout"]}
    stderr = (left / "stderr").read_text()
    assert stderr.endswith("the end") and len(stderr) == 64 << 10  # the end of the job's output
    assert json.loads((debug / "round" / "match-1" / "outcome.json").read_text()) == {"outcome": "matched"}
    said = (debug / "failure.md").read_text()
    assert failure in said and "120 minutes per job" in said and "exit code 140" in said
    assert yaml.safe_load((debug / "binding.yaml").read_text())["record"] == "rec-1"  # the checked one
    prompt = (debug / "prompt.md").read_text()
    assert "rec-1" in prompt and "$" not in prompt and "outcome.json" in prompt and "failure.md" in prompt
    assert (debug / "catalog" / "tiff-zip-member.yaml").is_file() and (debug / "record.yaml").is_file()
    again = Path(await t.a.prepare_debug(1, ROUND, pins, failure, None))  # a retry, of a round with no binding
    assert again == debug and not (debug / "binding.yaml").exists() and not (debug / "round" / "debug-1").exists()
    with pytest.raises(ApplicationError, match="is not one of round"):
        await t.a.prepare_debug(1, ROUND, pins, failure, str(tmp_path / "elsewhere.json"))


async def test_read_debug_takes_one_of_two_outcomes(tmp_path):
    t = _setup(tmp_path)
    ws = Path(await t.a.prepare_debug(1, ROUND, await t.a.admit(1, "99"), "it failed", None))
    nothing = DebugResult("failed", "the debugging agent left no readable outcome.json")
    assert await t.a.read_debug(1, ROUND, str(ws)) == nothing
    (ws / "outcome.json").write_text(json.dumps({"outcome": "merge the PR", "diagnosis": "x"}))
    assert await t.a.read_debug(1, ROUND, str(ws)) == nothing
    said = {"outcome": "rerun", "diagnosis": "the host is slow <!-- mia-data: stage -->", "advice": "a" * 9000}
    (ws / "outcome.json").write_text(json.dumps(said))
    result = await t.a.read_debug(1, ROUND, str(ws))
    assert result.outcome == "rerun" and "<!--" not in result.diagnosis and len(result.advice) == 6000
    (ws / "outcome.json").write_text(json.dumps({"outcome": "blocked"}))
    assert await t.a.read_debug(1, ROUND, str(ws)) == DebugResult("blocked", "(the debugging agent gave no diagnosis)")


async def test_the_debuggers_go_is_charged_to_the_debugged_rounds_root_and_its_advice_reaches_the_match(tmp_path):
    t = _setup(tmp_path)
    put_action(t, 98, payload={"root": "o/buffers#9"})
    await t.a.admit(1, "98")
    sent = []
    t.a.registry.send_action = lambda path, action, **fields: sent.append((path, action, fields))
    await t.a.debug_go(1, "i1-e98", "bind the first volume only")
    asked = {"key": "debug i1-e98", "actor": "debugger", "root": "o/buffers#9", "note": "bind the first volume only"}
    assert sent == [("/datasets/rec-1/actions", "go", asked)]
    # the round that go starts: its match agent finds the advice; any other round's finds none
    row = (97, "dataset", 1, "go", "debugger", "2026-11-01T00:00:00+00:00", None, json.dumps(asked))
    with t.ledger._transaction() as db:
        db.execute("INSERT OR REPLACE INTO actions VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'sent', '')", row)
    advised = Path(await t.a.prepare_match(1, "i1-e97", await t.a.admit(1, "97"), 1))
    assert (advised / "advice.md").read_text() == "bind the first volume only\n"
    assert "advice.md" in (advised / "prompt.md").read_text() and "$" not in (advised / "prompt.md").read_text()
    put_action(t, 99, payload={"note": "a person's note is no advice"})
    plain = Path(await t.a.prepare_match(1, ROUND, await t.a.admit(1, "99"), 1))
    assert not (plain / "advice.md").exists() and "advice.md" not in (plain / "prompt.md").read_text()


async def test_read_match_matched_copies_binding(tmp_path):
    t = _setup(tmp_path)
    _, ws = await _match_ws(t, {"outcome": "matched"}, BINDING)
    r = await t.a.read_match(str(ws))
    p = Path(r.binding_path)
    assert r.outcome == "matched" and p == t.cfg.paths.state_dir / "rounds" / ROUND / "binding-1.yaml"
    assert r.binding_hash == hashlib.sha256(p.read_bytes()).hexdigest() and yaml.safe_load(p.read_text()) == BINDING


async def test_read_match_stores_the_pinned_snippets_commit(tmp_path):
    t = _setup(tmp_path)
    _, ws = await _match_ws(t, {"outcome": "matched"}, {**BINDING, "snippets_commit": "f" * 40})
    r = await t.a.read_match(str(ws))
    assert r.outcome == "matched" and yaml.safe_load(Path(r.binding_path).read_text()) == BINDING


async def test_read_match_symlinked_outcome_is_missing(tmp_path):
    t = _setup(tmp_path)
    _, ws = await _match_ws(t, None, BINDING)
    real = tmp_path / "elsewhere.json"
    real.write_text(json.dumps({"outcome": "matched"}))
    (ws / "outcome.json").symlink_to(real)
    r = await t.a.read_match(str(ws))
    assert r.outcome == "failed" and "outcome.json" in r.reason and r.binding_path is None  # no word from the agent


async def test_read_match_foreign_host_fails_the_match(tmp_path):
    t = _setup(tmp_path)
    bad = {**BINDING, "raw": [{**BINDING["raw"][0], "urls": ["https://evil.example.com/a.tif"]}]}
    _, ws = await _match_ws(t, {"outcome": "matched"}, bad)
    r = await t.a.read_match(str(ws))
    assert r.outcome == "failed" and "evil.example.com" in r.reason and r.binding_path is None
    assert not list((t.cfg.paths.state_dir / "rounds" / ROUND).glob("binding-*"))


async def test_read_match_takes_the_agents_word_for_why_it_stopped(tmp_path):
    """The agent says whether a person has to act first (`blocked`) or another run may pass (`failed`)."""
    t = _setup(tmp_path)
    for outcome, reason in (("blocked", "the label classes are not named"), ("failed", "the host timed out")):
        _, ws = await _match_ws(t, {"outcome": outcome, "reason": reason}, BINDING)
        r = await t.a.read_match(str(ws))
        assert (r.outcome, r.reason, r.binding_path) == (outcome, reason, None)
    _, ws = await _match_ws(t, {"outcome": "failed"}, BINDING)
    assert (await t.a.read_match(str(ws))).reason == "the match agent reported failed, with no reason"
    _, ws = await _match_ws(t, {"outcome": "gave up"}, BINDING)
    assert (await t.a.read_match(str(ws))).outcome == "failed"  # a word that is neither: nothing to act on


async def test_prepare_match_malformed_snippet_yaml_is_permanent(tmp_path):
    t = _setup(tmp_path)
    pins = await t.a.admit(1, "99")
    (t.cfg.paths.work_root / "_snapshots" / SNIP_SHA / "snippets" / "tiff-zip-member" / "snippet.yaml").write_text(
        "a: [\n"
    )
    with pytest.raises(ApplicationError) as e:
        await t.a.prepare_match(1, ROUND, pins, 1)
    assert e.value.non_retryable and e.value.type in ("ParserError", "ScannerError")


async def _submit_match(t):
    pins, ws = await _match_ws(t, None)
    return await t.a.submit(job_key(1, ROUND, "match", 1), "match", JobArgs(str(ws), ROUND, pins))


async def test_submit_refuses_moved_harness(tmp_path):
    t = _setup(tmp_path)
    pins, ws = await _match_ws(t, None)
    (t.cfg.harness.dir / "harness" / "pixi.lock").write_text("lock v2\n")
    with pytest.raises(ApplicationError) as e:
        await t.a.submit(job_key(1, ROUND, "match", 1), "match", JobArgs(str(ws), ROUND, pins))
    assert e.value.non_retryable and "changed" in str(e.value) and t.csub.submitted == []


async def test_submit_refuses_host_missing_from_probe(tmp_path):
    t = _setup(
        tmp_path,
        probe=lambda: ["*.pypi.org", "pypi.org", "files.pythonhosted.org", "conda.anaconda.org", "repo.prefix.dev"],
    )
    with pytest.raises(ApplicationError) as e:
        await _submit_match(t)
    assert e.value.non_retryable and "data.example.org" in str(e.value) and t.csub.submitted == []


async def test_submit_match_once(tmp_path):
    t = _setup(tmp_path)
    assert await _submit_match(t) == "777" == await _submit_match(t)
    s = t.csub.submitted
    assert len(s) == 1 and s[0]["agent"] == "claude" and "data.example.org" in s[0]["allow_hosts"]
    assert not any(m["path"] == str(tmp_path / "dl") for m in s[0]["mounts"])  # its files are copied into the ws


async def test_set_stage_appends_one_entry_per_stage_and_leaves_the_issue_alone(tmp_path):
    t = _setup(tmp_path)
    t.cfg.paths.work_root_url, t.cfg.paths.storage_root_url = "https://fg.example/work", "https://fg.example/crops"
    t.data.issues[1]["labels"] = [{"name": "data:new"}, {"name": "has-labels"}]
    s = DatasetState(1, round_id=ROUND, pins=await t.a.admit(1, "99"))
    await t.a.set_stage(1, "matching", s)
    await t.a.set_stage(1, "matching", s)  # a retry adds nothing
    await t.a.set_stage(1, "done", s, "", "<details>16 crops</details>\n\n**Totals:** 16 new")
    await t.a.set_stage(1, "needs-human", s, "host <!-- x --> missing")
    assert t.ledger.subject("dataset", "rec-1")["stage"] == "needs-human"
    bodies = [entry["body"] for entry in t.ledger.timeline("dataset", 1)]
    assert len(bodies) == 3 and all(b.startswith("**Stage:** `data:") for b in bodies)  # nothing edited in place
    assert "<!-- x" not in bodies[2] and "host" in bodies[2]
    assert f"[Workspace](https://fg.example/work/issue-1/{ROUND})" in bodies[2]
    assert "**Totals:** 16 new" in bodies[1] and "[Crops](https://fg.example/crops/lm-human-rec-1)" in bodies[1]
    assert "[Gallery](https://github.com/o/data/tree/galleries/galleries/rec-1/" + ROUND + ")" in bodies[1]
    assert [x["name"] for x in t.data.issues[1]["labels"]] == ["data:new", "has-labels"]  # the label is the mirror's
    assert t.data.comments == {}


def _registry_seen(t, handler=None):
    """Give the activities a registry on a mock transport; the (path, body) of each write lands in the list."""
    seen = []

    def record(request):
        if request.method == "GET":  # a lookup: this registry knows no action and no subject
            return httpx.Response(404, json={"error": "not found"})
        seen.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={})

    t.a.registry = Registry("http://svc", "t", transport=httpx.MockTransport(handler or record))
    return seen


async def test_set_stage_reports_the_dataset_state(tmp_path):
    t = _setup(tmp_path)
    state = DatasetState(1, round_id=ROUND, pins=await t.a.admit(1, "99"))
    seen = _registry_seen(t)
    await t.a.set_stage(1, "matching", state)
    working = {"state": "in-progress", "reason": None, "data_stage": "matching", "issue": 1}
    assert seen[-2] == ("/datasets/rec-1", working)
    assert seen[-1][0] == "/timeline/dataset/1" and seen[-1][1]["stage"] == "matching"  # then the stage itself
    for stage in ("needs-human", "review", "cancelled"):  # a person has to act
        await t.a.set_stage(1, stage, state)
        assert seen[-2][1]["state"] == "needs-human" and seen[-2][1]["reason"] is None
    await t.a.set_stage(1, "done", state)  # done, and the folder holds nothing: not prepared, and the row says why
    unsaved = seen[-2][1]
    assert unsaved["state"] == "needs-human" and "its crops are not saved" in unsaved["reason"]
    assert "`lm-human-rec-1` holds no binding.yaml with a matching manifest.json" in unsaved["reason"]
    dataset_dir = t.cfg.paths.storage_root / "lm-human-rec-1"  # binding.yaml + a manifest that matches it
    dataset_dir.mkdir(parents=True)
    binding_bytes = yaml.safe_dump(BINDING).encode()
    (dataset_dir / "binding.yaml").write_bytes(binding_bytes)
    manifest = {"count": 2, "binding": {"sha256": hashlib.sha256(binding_bytes).hexdigest()}}
    (dataset_dir / "manifest.json").write_text(json.dumps(manifest))
    await t.a.set_stage(1, "done", state)
    row = seen[-2][1]
    assert row["state"] == "prepared" and row["crops"] == 2 and row["binding_hash"] and row["folder"]
    assert "record" not in row and row["issue"] == 1 and row["reason"] is None
    # a new round on the prepared dataset: its state follows the stage again, the folder's facts stay on the row
    for stage, word in (("matching", "in-progress"), ("review", "needs-human"), ("needs-human", "needs-human")):
        await t.a.set_stage(1, stage, state)
        assert seen[-2][1]["state"] == word and seen[-2][1]["crops"] == 2
    written = [path for path, _ in seen].count("/datasets/rec-1")
    await t.a.set_stage(1, "matching", None)  # no round: no dataset row, the stage alone
    assert [path for path, _ in seen].count("/datasets/rec-1") == written
    assert seen[-1] == ("/timeline/dataset/1", {"body": "", "marker": "", "round": None, "stage": "matching"})
    assert t.data.comments == {} and t.data.issues[1]["labels"] == []  # nothing goes to the issue from a round


async def test_set_stage_fails_while_the_registry_is_down_and_nothing_reaches_github(tmp_path, caplog):
    def down(request):
        raise httpx.ConnectError("down")

    t = _setup(tmp_path)
    t.data.issues[1]["labels"] = [{"name": "data:new"}]
    state = DatasetState(1, round_id=ROUND, pins=await t.a.admit(1, "99"))
    _registry_seen(t, down)
    with caplog.at_level(logging.WARNING), pytest.raises(RegistryError):  # Temporal retries the activity
        await t.a.set_stage(1, "matching", state)
    assert "registry write skipped" in caplog.text  # the dataset's row stays best effort; the stage does not
    assert [x["name"] for x in t.data.issues[1]["labels"]] == ["data:new"] and t.data.comments == {}


async def test_set_stage_needs_human_with_an_unclear_record_still_comments(tmp_path):
    t = _setup(tmp_path)
    state = DatasetState(1, round_id=ROUND, pins=await t.a.admit(1, "99"))
    seen = _registry_seen(t)
    record_file = t.cfg.paths.state_dir / "rounds" / ROUND / "record.yaml"
    record_file.write_text(yaml.safe_dump({"imaging": {"modality": [], "organism": []}}))  # no folder
    await t.a.set_stage(1, "needs-human", state, "why")
    assert seen[0] == (
        "/datasets/rec-1",
        {"state": "needs-human", "reason": None, "data_stage": "needs-human", "issue": 1},
    )
    (path, entry) = seen[1]
    assert path == "/timeline/dataset/1" and entry["stage"] == "needs-human" and "why" in entry["body"]


async def test_set_stage_with_a_malformed_record_still_comments(tmp_path, caplog):
    t = _setup(tmp_path)
    t.data.issues[1]["labels"] = [{"name": "data:new"}]
    state = DatasetState(1, round_id=ROUND, pins=await t.a.admit(1, "99"))
    seen = _registry_seen(t)
    record_file = t.cfg.paths.state_dir / "rounds" / ROUND / "record.yaml"
    record_file.write_text(yaml.safe_dump({"imaging": "x"}))  # untrusted catalog data of the wrong shape
    await t.a.set_stage(1, "needs-human", state, "why")
    assert "rec-1: registry write skipped" in caplog.text  # no dataset row; the stage is still written
    assert [path for path, _ in seen] == ["/timeline/dataset/1"] and "why" in seen[0][1]["body"]


async def test_comment_can_carry_the_binding(tmp_path):
    t = _setup(tmp_path)
    b = tmp_path / "b.yaml"
    b.write_text("record: rec-1\n")
    await t.a.comment(1, "Test crops: **passed**", "test r 1", "data", str(b), "cd" * 32)
    body = t.ledger.timeline("dataset", 1)[0]["body"]
    assert body.startswith(
        "Test crops: **passed**\n\n**Binding** (sha256 `" + "cd" * 32 + "`):\n```yaml\nrecord: rec-1\n```"
    )


async def test_open_snippet_issue_twice_gives_one_issue(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    await t.snip.create_issue("copy", "<!-- mia-data: issue=1 round=i1-e99 -->", [])
    t.snip.issues[100]["user"]["login"], t.snip.created = "mallory", []  # an outsider's copy of the marker
    draft = {"draft_binding": BINDING, "what_fails": "no reader for <!-- x --> files", "tried": []}
    n = await t.a.open_snippet_issue(1, ROUND, draft)
    assert await t.a.open_snippet_issue(1, ROUND, draft) == n and len(t.snip.created) == 1
    title, body, labels = t.snip.created[0]
    assert labels == ["snippets:go"] and title == "New snippet for tiff/zip: rec-1"
    assert (
        body.startswith(
            "<!-- mia-data: issue=1 round=i1-e99 -->\n\nFor [rec-1](http://svc.example.org:8765/#datasets/rec-1): "
            "dataset `rec-1` has no snippet that reads it.\n\n"
        )
        and "<!-- x" not in body
    )


async def test_open_snippet_issue_with_a_binding_asks_to_supersede_its_snippets(tmp_path):
    """A reviewer rejected a matched binding's crops (spec §14): the issue names the binding's snippets, carries
    the comments and the binding, and asks for a superseding snippet, never an edit."""
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    binding = t.a._round_dir(ROUND) / "binding-1.yaml"
    binding.write_text(
        yaml.safe_dump({**BINDING, "labels": [{"snippet": "hdf5-zip-member", "urls": ["https://x/y.h5"]}]})
    )
    n = await t.a.open_snippet_issue(
        1, ROUND, {"what_fails": "alice wrote:\nthe masks are <!-- shifted"}, None, str(binding)
    )
    title, body, labels = t.snip.created[0]
    assert n == 100 and labels == ["snippets:go"]
    assert title == "Fix the crops of rec-1: supersede hdf5-zip-member, tiff-zip-member"
    assert body.startswith(
        "<!-- mia-data: issue=1 round=i1-e99 -->\n\nFor [rec-1](http://svc.example.org:8765/#datasets/rec-1): "
    )
    assert "**The reviewer's comments:**" in body and "the masks are &lt;!-- shifted" in body
    assert "**supersedes**" in body and "**The rejected binding:**" in body and "tiff-zip-member" in body
    with pytest.raises(ApplicationError, match="not one of round"):
        await t.a.open_snippet_issue(1, ROUND, {}, None, str(tmp_path / "elsewhere.yaml"))


async def test_open_snippet_issue_reuses_the_remembered_issue_in_a_new_round(tmp_path):
    t = _setup(tmp_path)
    for event_id in ("99", "100", "101"):
        put_action(t, int(event_id))
        await t.a.admit(1, event_id)
    draft = {"draft_binding": BINDING, "what_fails": "no reader", "tried": []}
    n = await t.a.open_snippet_issue(1, ROUND, draft)
    assert await t.a.open_snippet_issue(1, "i1-e100", draft, n) == n and len(t.snip.created) == 1  # a re-trigger
    t.snip.issues[n]["state"] = "closed"  # autodev is done with it: the new round gets a new one
    m = await t.a.open_snippet_issue(1, "i1-e100", draft, n)
    assert m != n and t.snip.created[1][1].startswith("<!-- mia-data: issue=1 round=i1-e100 -->")
    other = (await t.snip.create_issue("other", "<!-- mia-data: issue=2 round=i2-e5 -->", []))["number"]
    assert await t.a.open_snippet_issue(1, "i1-e101", draft, other) not in (n, m, other)  # another dataset's
    t.snip.issues[m]["user"]["login"] = "mallory"
    assert await t.a.open_snippet_issue(1, "i1-e101", draft, m) not in (n, m, other)  # not ours


def _pr_binding(t, b, files=("snippets/new-snip/snippet.py", "snippets/new-snip/examples/rec-1.yaml")):
    d = t.cfg.paths.work_root / "_snapshots" / SNIP_SHA / "snippets" / "new-snip"  # the PR head's snapshot
    d.mkdir()
    (d / "snippet.yaml").write_text("name: new-snip\n")
    t.snip.files[("snippets/new-snip/examples/rec-1.yaml", SNIP_SHA)] = yaml.safe_dump(b).encode()

    async def pr_files(n):
        return list(files)

    async def dir_sha(path, ref):
        return "tree1" if (path, ref) == ("snippets/new-snip", SNIP_SHA) else None

    t.snip.pr_files, t.snip.dir_sha = pr_files, dir_sha


NEW = {**BINDING, "snippets_commit": "f" * 40, "raw": [{**BINDING["raw"][0], "snippet": "new-snip"}]}


async def test_adopt_pr_binding_pins_it_to_the_sha(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    _pr_binding(t, NEW)
    r = await t.a.adopt_pr_binding(ROUND, 5, SNIP_SHA)
    p = Path(r["path"])
    assert r["errors"] == [] and r["name"] == "new-snip" and p.name == f"binding-pr-{SNIP_SHA[:7]}.yaml"
    assert yaml.safe_load(p.read_text()) == {**NEW, "snippets_commit": SNIP_SHA}
    assert r["hash"] == hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    "b, files, why",
    [
        ({**NEW, "record": "rec-2"}, None, "rec-2"),
        ({**NEW, "raw": [{**NEW["raw"][0], "urls": ["https://evil.example.com/a.tif"]}]}, None, "evil.example.com"),
        (BINDING, None, "new-snip"),  # the example doesn't use the PR's snippet
        (NEW, ["snippets/new-snip/a.py", "snippets/other/b.py"], "exactly one"),
    ],
)
async def test_adopt_pr_binding_rejects(tmp_path, b, files, why):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    _pr_binding(t, b, *([files] if files else []))
    r = await t.a.adopt_pr_binding(ROUND, 5, SNIP_SHA)
    assert "path" not in r and why in " ".join(r["errors"])
    assert not list((t.cfg.paths.state_dir / "rounds" / ROUND).glob("binding-*"))


async def test_comment_returns_its_url_once(tmp_path):
    t = _setup(tmp_path)
    u = await t.a.comment(5, "crops passed", "test i1-e99 x", repo="snippets")
    assert u and await t.a.comment(5, "again", "test i1-e99 x", repo="snippets") == u and len(t.snip.comments[5]) == 1


def _crop(root, cid, n):
    d = root / "crops" / cid
    d.mkdir(parents=True)
    files = {
        "raw.png": b"raw" + cid.encode(),
        "crop.json": json.dumps(
            {"crop_id": cid, "plane": {"z": n, "c": 0}, "box": {"y": 10 * n, "x": 5, "h": 256, "w": 256}}
        ).encode(),
    }
    for rel, b in files.items():
        (d / rel).write_bytes(b)
    return {"crop_id": cid, "files": {r: hashlib.sha256(b).hexdigest() for r, b in files.items()}}


async def test_publish_gallery_verified_only(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    cw = Path(await t.a.prepare_job(1, ROUND, "crop", 1, None))
    vw = Path(await t.a.prepare_job(1, ROUND, "verify", 1, None))
    verified = [_crop(cw, c, i) for i, c in enumerate(["c1", "c2", "c3"])][:2]
    for c in ("c1", "c2", "c3"):
        (vw / "preview" / c).mkdir(parents=True)
        (vw / "preview" / c / "overlay.png").write_bytes(b"ov" + c.encode())
    (vw / "binding.yaml").write_text("record: rec-1\n")
    (vw / "verify.json").write_text("{}")
    pre = f"galleries/rec-1/{ROUND}/{SNIP_SHA}"
    t.data.files[f"{pre}/c1/raw.png"] = b"already there"
    g = await t.a.publish_gallery(1, ROUND, SNIP_SHA, str(cw), str(vw), verified)
    assert g.prefix == pre
    assert sorted(t.data.puts) == [
        f"{pre}/{r}" for r in ("binding.yaml", "c1/overlay.png", "c2/overlay.png", "c2/raw.png", "verify.json")
    ]
    rows = [ln for ln in g.table.splitlines() if ln.startswith("| ![raw]")]
    assert len(rows) == 2 and all(ln.count("?raw=true") == 2 for ln in rows) and "c3" not in g.table
    assert "c2 · z=1 c=0 · (10, 5)" in rows[1]


@pytest.mark.parametrize("extra, ok", [({}, 1), ({"errors": ["manifest unreadable"]}, 0), ({"n": 0}, 0)])
async def test_read_verify(tmp_path, extra, ok):
    t = _setup(tmp_path)
    ws = tmp_path / "vws"
    ws.mkdir()
    good = {"crop_id": "c1", "ok": True, "errors": [], "files": {"crop.json": "0" * 64, "raw.png": "1" * 64}}
    bad = {"crop_id": "c2", "ok": False, "errors": ["overlaps c1"], "files": {}}
    (ws / "verify.json").write_text(json.dumps({"crops": [good, bad], "ok_count": 2, "n": 2, "errors": [], **extra}))
    r = await t.a.read_verify(str(ws))
    assert r.ok_count == ok and [c["crop_id"] for c in r.crops] == ["c1"][:ok]
    assert any("overlaps c1" in e for e in r.errors)
    if not ok:
        assert r.errors[0] in ("manifest unreadable", "verify checked no crops")


async def test_publish_crops_into_dataset_folder(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    cw = Path(await t.a.prepare_job(1, ROUND, "crop", 1, None))
    v = _crop(cw, "c1", 0)
    r = await t.a.publish_crops(1, ROUND, str(cw), [v], SNIP_SHA)
    m = json.loads((t.cfg.paths.storage_root / "lm-human-rec-1" / "manifest.json").read_text())
    assert r == {"published": ["c1"], "already": [], "total": 1} and m["crops"][0]["files"] == v["files"]
    with pytest.raises(ApplicationError):  # not this round's workspace
        await t.a.publish_crops(1, "i1-e100", str(cw), [v], SNIP_SHA)


async def test_read_verify_reports_the_lowered_n(tmp_path):
    t = _setup(tmp_path)
    ws, cw = tmp_path / "vws", tmp_path / "cws"
    (cw / "crops").mkdir(parents=True)
    ws.mkdir()
    (ws / "verify.json").write_text(json.dumps({"crops": [], "n": 0, "errors": []}))
    assert (await t.a.read_verify(str(ws), str(cw))).n_effective is None  # no report: the caller's n stands
    (cw / "crops" / "report.json").write_text(json.dumps({"requested_n": 16, "n": 3, "errors": ["ReadTimeout: x"]}))
    assert (await t.a.read_verify(str(ws), str(cw))).n_effective == 3
    assert (await t.a.read_verify(str(ws), str(cw))).crop_errors == ["ReadTimeout: x"]  # why the crop job failed
    assert (await t.a.read_verify(str(ws), str(cw))).host_not_allowed is None
    refused = {"host": "evil.invalid", "chain": ["https://a.example.org/x", "http://evil.invalid/x"]}
    (cw / "crops" / "report.json").write_text(json.dumps({"n": 3, "errors": ["x"], "host_not_allowed": refused}))
    assert "evil.invalid" in ((await t.a.read_verify(str(ws), str(cw))).host_not_allowed or "")
    assert (await t.a.read_verify(str(ws))).n_effective is None


async def test_admit_refuses_a_record_that_names_no_data_url(tmp_path):
    """A record with only file names and a landing page can never be bound: the round stops before any job."""
    t = _setup(tmp_path)
    bare = {**RECORD, "data": {"download_url": None}, "landing_url": "https://data.example.org/landing"}
    bare["technical"] = {"arrays": [{"role": "raw", "path_pattern": "stack_*.tif"}]}
    t.cat.files[("datasets/rec-1.yaml", REC_SHA)] = yaml.safe_dump(bare).encode()
    with pytest.raises(ApplicationError, match="rec-1: the record cannot be run: it names no data URL") as refused:
        await t.a.admit(1, "99")
    assert refused.value.non_retryable  # the workflow ends such a round `blocked`
    assert not (t.cfg.paths.state_dir / "rounds" / ROUND / "pins.json").exists()
    assert record_gaps({"technical": {"arrays": "not a list of arrays"}})  # an unreadable record lacks them too
    assert record_gaps({"technical": {"arrays": [{"path_pattern": "s3://bucket/a/*.tif"}]}}) == []


async def test_admit_without_downloads_entry_pins_main(tmp_path):
    t = _setup(tmp_path)
    main_sha = "d" * 40
    t.cat.files[("state/downloads.json", "state")] = b'{"downloaded": {}}'
    t.cat.files[("datasets/rec-1.yaml", main_sha)] = yaml.safe_dump(RECORD).encode()
    t.cat.refs["main"] = main_sha
    pins = await t.a.admit(1, "99")
    assert pins.record_commit == main_sha
    downloads = json.loads((t.cfg.paths.state_dir / "rounds" / ROUND / "downloads.json").read_text())
    assert downloads == {"record_commit": main_sha, "downloaders": []}


async def test_a_folder_another_record_prepared_is_refused(tmp_path):
    """Two catalog records can share a folder name; the one prepared first owns the folder."""
    t = _setup(tmp_path)
    state = DatasetState(1, round_id=ROUND, pins=await t.a.admit(1, "99"))
    cw = Path(await t.a.prepare_job(1, ROUND, "crop", 1, None))
    dataset_dir = t.cfg.paths.storage_root / "lm-human-rec-1"
    dataset_dir.mkdir(parents=True)
    binding_bytes = yaml.safe_dump({**BINDING, "record": "zenodo-1-rec-1"}).encode()
    (dataset_dir / "binding.yaml").write_bytes(binding_bytes)
    manifest = {"count": 2, "binding": {"sha256": hashlib.sha256(binding_bytes).hexdigest()}}
    (dataset_dir / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ApplicationError, match="lm-human-rec-1 belongs to record zenodo-1-rec-1") as error:
        await t.a.admit(1, "11")  # a new round stops at admission
    assert error.value.non_retryable
    with pytest.raises(ApplicationError, match="belongs to record zenodo-1-rec-1"):  # a round under way: at publish
        await t.a.publish_crops(1, ROUND, str(cw), [_crop(cw, "c1", 0)], SNIP_SHA)
    assert (dataset_dir / "binding.yaml").read_bytes() == binding_bytes and not (dataset_dir / "c1").exists()
    seen = _registry_seen(t)
    await t.a.set_stage(1, "needs-human", state)  # the other record's publication is not this one's
    assert seen[0] == (
        "/datasets/rec-1",
        {"state": "needs-human", "reason": None, "data_stage": "needs-human", "issue": 1},
    )


async def test_publish_crops_publishes_the_binding(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    binding = t.cfg.paths.state_dir / "rounds" / ROUND / "binding-1.yaml"
    binding.write_text("record: rec-1\n")
    cw = Path(await t.a.prepare_job(1, ROUND, "crop", 1, None))
    v = _crop(cw, "c1", 0)
    await t.a.publish_crops(1, ROUND, str(cw), [v], SNIP_SHA, str(binding))
    ds = t.cfg.paths.storage_root / "lm-human-rec-1"
    assert (ds / "binding.yaml").read_text() == "record: rec-1\n"
    digest = hashlib.sha256(b"record: rec-1\n").hexdigest()
    assert json.loads((ds / "manifest.json").read_text())["binding"] == {"sha256": digest}


async def test_publish_crops_refuses_a_binding_outside_the_round(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    cw = Path(await t.a.prepare_job(1, ROUND, "crop", 1, None))
    v = _crop(cw, "c1", 0)
    (tmp_path / "elsewhere.yaml").write_text("record: rec-1\n")
    with pytest.raises(ApplicationError, match="not one of round"):
        await t.a.publish_crops(1, ROUND, str(cw), [v], SNIP_SHA, str(tmp_path / "elsewhere.yaml"))


# --- the raw-cache lock and the raw asset row ---------------------------------------------------


async def test_take_lock_names_the_holder(tmp_path):
    t = _setup(tmp_path)
    assert (await t.a.limits()).poll_minutes == t.cfg.temporal.poll_minutes  # the wait between tries
    assert await t.a.take_lock("raw", "rec-1", "mia-data i1-e99 smoke", 900) == LockTaken(True)
    assert await t.a.take_lock("raw", "rec-1", "mia-data i1-e98 crop", 900) == LockTaken(False, "mia-data i1-e99 smoke")
    await t.a.release_lock("raw", "rec-1", "mia-data i1-e98 crop")  # not the holder: the lock stays
    assert not (await t.a.take_lock("raw", "rec-1", "mia-data i1-e98 crop", 900)).ok
    await t.a.release_lock("raw", "rec-1", "mia-data i1-e99 smoke")
    assert (await t.a.take_lock("raw", "rec-1", "mia-data i1-e98 crop", 900)).ok


async def test_await_job_heartbeats_the_lock_it_is_given(tmp_path):
    t = _setup(tmp_path)
    locks = lock_dir(t.cfg)
    assert locks.take("raw", "rec-1", "me", 900)
    before = locks.holder("raw", "rec-1").heartbeat_at

    async def wait(job_id, workspace, heartbeat):
        heartbeat("PEND")
        heartbeat("RUN")
        return SimpleNamespace(job_id=job_id, state="DONE", exit_code=0)

    t.a.jobs = SimpleNamespace(wait=wait)
    env, beats = ActivityEnvironment(), []
    env.on_heartbeat = lambda *details: beats.append(details)
    assert await env.run(t.a.await_job, "j1", "/w", ["raw", "rec-1", "me"]) == JobDone("j1", "DONE", 0)
    assert beats == [("PEND",), ("RUN",)] and locks.holder("raw", "rec-1").heartbeat_at > before
    assert await env.run(t.a.await_job, "j2", "/w") == JobDone("j2", "DONE", 0)  # no lock: two arguments still work
    locks.release("raw", "rec-1", "me")
    with pytest.raises(ApplicationError) as info:  # the lock is gone: the wait fails, never retried
        await env.run(t.a.await_job, "j3", "/w", ["raw", "rec-1", "me"])
    assert info.value.non_retryable and info.value.type == "LockLost" and "lost the lock raw/rec-1" in str(info.value)


def _cache_with_files_json(t):
    cache = t.cfg.paths.work_root / "_cache" / "rec-1"
    cache.mkdir(parents=True)
    url = "https://data.example.org/a.zip"
    (cache / "files.json").write_text(
        json.dumps(
            {url: {"validators": {"size": 3}}, f"{url}::m.tif": {"validators": {"size": 3}, "source": "members"}}
        )
    )
    (cache / "a.zip").write_bytes(b"zip")
    return cache


async def test_read_smoke_puts_the_raw_asset(tmp_path):
    t = _setup(tmp_path)
    seen = _registry_seen(t)
    cache = _cache_with_files_json(t)
    ws = tmp_path / "sws"
    ws.mkdir()
    (ws / "binding.yaml").write_text(yaml.safe_dump(BINDING))
    (ws / "smoke.json").write_text(json.dumps({"ok": True, "eligible": True}))
    assert (await t.a.read_smoke(str(ws))).ok and seen == []  # no record: the caller holds no lock, no row
    assert (await t.a.read_smoke(str(ws), "rec-1")).ok
    ((path, row),) = seen
    assert path == "/assets/raw/rec-1" and row["state"] == "ready" and row["last_used"] > 0
    assert row["path"] == str(cache) and row["bytes"] > 0 and "https://data.example.org/a.zip" in row["files"]


async def test_read_verify_puts_the_raw_asset_and_survives_a_down_registry(tmp_path, caplog):
    def down(request):
        raise httpx.ConnectError("down")

    t = _setup(tmp_path)
    seen = _registry_seen(t)
    _cache_with_files_json(t)
    cw, vw = tmp_path / "cws", tmp_path / "vws"
    cw.mkdir()
    vw.mkdir()
    (cw / "binding.yaml").write_text(
        yaml.safe_dump({**BINDING, "labels": [{"urls": ["https://data.example.org/b.zip"]}]})
    )
    (vw / "verify.json").write_text(json.dumps({"n": 0}))
    await t.a.read_verify(str(vw), str(cw), "rec-1")
    assert [(path, row["state"]) for path, row in seen] == [("/assets/raw/rec-1", "partial")]  # b.zip not cached
    _registry_seen(t, down)
    with caplog.at_level(logging.WARNING):
        assert (await t.a.read_verify(str(vw), str(cw), "rec-1")).errors == ["verify checked no crops"]
    assert "raw asset rec-1: registry write skipped" in caplog.text


async def test_reserve_and_settle_charge_the_root(tmp_path, token_server):  # noqa: F811
    t = _setup(tmp_path)
    t.a.registry = Registry(token_server, "t")
    t.cfg.autonomy.root.max_usd = 7
    assert await t.a.reserve("o/data#7", "mia-7-i7-e11-match-1", "match", 5.0) == Reservation(True, "")
    refused = await t.a.reserve("o/data#7", "mia-7-i7-e11-match-2", "match", 5.0)
    assert not refused.ok and "would exceed max_usd 7" in refused.reason
    await t.a.settle("o/data#7", "mia-7-i7-e11-match-1", usd=1.0)
    assert t.a.registry.budget("o/data#7")["spent_usd"] == 1.0
    assert (await t.a.reserve("", "k", "match", 5.0)) == Reservation(True, "no budget")


async def test_paused_reads_the_manifest_fresh(tmp_path, monkeypatch):
    from mia_core.autonomy import set_pause

    t = _setup(tmp_path)
    manifest = tmp_path / "mia.yaml"
    manifest.write_text("services: {}\n")
    monkeypatch.setenv("MIA_CONFIG", str(manifest))
    assert await t.a.paused() is False
    set_pause(manifest, True)
    assert await t.a.paused() is True


async def test_assess_writes_the_human_verdict_and_asks_the_policy(tmp_path, token_server):  # noqa: F811
    t = _setup(tmp_path)
    await t.a.admit(1, "11")  # pins rec-1
    t.a.registry = Registry(token_server, "t")
    first = await t.a.assess(1, "i1-e11", "crops", "crop_set", "i1-e11:s-t", "approve", "human:alice", "data:approve")
    assert first.authorized and first.id and "human:alice" in first.reason
    rows = t.a.registry.assessments(subject_kind="crop_set", subject_ref="i1-e11:s-t")
    assert [r["verdict"] for r in rows] == ["approve"] and rows[0]["by"] == "human:alice" and rows[0]["gate"] == "crops"
    assert t.a.registry.dataset("rec-1")["assessment"] == first.id
    second = await t.a.assess(
        1, "i1-e11", "crops", "crop_set", "i1-e11:s-t", "reject", "human:bob", "data:reject", None, "1794621340330"
    )
    assert not second.authorized and second.reason.startswith("rejected by human:bob")
    newest, oldest = t.a.registry.assessments(subject_kind="crop_set", subject_ref="i1-e11:s-t")
    assert (newest["action"], oldest["action"]) == (1794621340330, None)  # the action the verdict came from


async def test_revoke_dataset_rejects_quarantines_and_finds_the_buffers(tmp_path, token_server):  # noqa: F811
    from test_buffers_api import CFG, body
    from test_buffers_sync import write_prepared

    t = _setup(tmp_path)
    await t.a.admit(1, "11")
    registry = t.a.registry = Registry(token_server, "t")
    folder = t.a._dataset_dir("i1-e11", t.a._pins("i1-e11"))
    write_prepared(folder, "rec-1")
    approve = await t.a.assess(1, "i1-e11", "crops", "crop_set", "i1-e11:s-t", "approve", "human:alice")
    registry.put_asset("staged", "art1", {"record": "rec-1", "state": "ready", "path": "/x", "bytes": 1})
    registry.put_asset("staged", "art2", {"record": "rec-1", "state": "ready", "path": "/y", "bytes": 1})
    registry.put_asset("staged", "other", {"record": "rec-2", "state": "ready"})
    registry.request("PUT", "/buffers/i3-e4", json=CFG)
    volume = {**body("rec-1/v0.zarr"), "artifact": "art1"}
    registry.request("PUT", "/buffers/i3-e4/volumes/rec-1%2Fv0.zarr", json=volume)
    out = await t.a.revoke_dataset(1, "i1-e11", "alice", "77")
    assert out.artifacts == ["art1", "art2"] and out.buffers == ["i3-e4"]
    row = registry.dataset("rec-1")
    assert (row["state"], row["revoked_by"], row["assessment"]) == ("revoked", "human:alice", out.assessment)
    crop_set = registry.assessments(subject_kind="crop_set", subject_ref="i1-e11:s-t")
    assert crop_set[0]["verdict"] == "reject" and crop_set[0]["supersedes"] == approve.id
    assert crop_set[0]["id"] == out.assessment and crop_set[0]["action"] == 77
    assert registry.assessments(subject_kind="artifact", subject_ref="art1")[0]["verdict"] == "reject"
    art1 = registry.asset("staged", "art1")
    assert art1["quarantined"] is True and art1["quarantined_by"] == "human:alice"
    assert "quarantined" not in registry.asset("staged", "other")
    assert json.loads((folder / "revoked.json").read_text())["by"] == "human:alice"
    assert t.a.dataset_fields(1, "i1-e11", "done")["state"] == "revoked"  # the row stays revoked whatever the stage
    assert await t.a.revoke_dataset(1, "i1-e11", "alice", "77") == out  # a retry: the same assessment, nothing doubled
    assert len(registry.assessments(subject_kind="artifact", subject_ref="art1")) == 1


async def test_revoke_dataset_with_nothing_published_records_nothing(tmp_path, token_server):  # noqa: F811
    t = _setup(tmp_path)
    await t.a.admit(1, "11")
    registry = t.a.registry = Registry(token_server, "t")
    assert await t.a.revoke_dataset(1, "i1-e11", "alice", "77") == Revocation("")
    folder = t.a._dataset_dir("i1-e11", t.a._pins("i1-e11"))
    assert not (folder / "revoked.json").exists() and registry.assessments() == [] and registry.dataset("rec-1") is None


async def test_a_publication_after_a_revoke_releases_the_artifacts(tmp_path, token_server):  # noqa: F811
    from test_buffers_sync import write_prepared

    t = _setup(tmp_path)
    await t.a.admit(1, "11")
    registry = t.a.registry = Registry(token_server, "t")
    folder = t.a._dataset_dir("i1-e11", t.a._pins("i1-e11"))
    write_prepared(folder, "rec-1")
    registry.put_asset("staged", "art1", {"record": "rec-1", "state": "ready", "path": "/x", "bytes": 1})
    await t.a.revoke_dataset(1, "i1-e11", "alice", "77")
    assert registry.asset("staged", "art1")["quarantined"] is True
    binding = t.cfg.paths.state_dir / "rounds" / "i1-e11" / "binding-1.yaml"
    binding.write_text("record: rec-1\n")
    cw = Path(await t.a.prepare_job(1, "i1-e11", "crop", 1, None))
    await t.a.publish_crops(1, "i1-e11", str(cw), [_crop(cw, "c1", 0)], SNIP_SHA, str(binding))
    assert not (folder / "revoked.json").exists()
    art1 = registry.asset("staged", "art1")
    assert art1["quarantined"] is False and art1["repaired_by"] == "i1-e11"
    newest = registry.assessments(subject_kind="artifact", subject_ref="art1")[0]
    assert newest["verdict"] == "approve" and newest["by"] == "policy:repair"
    assert t.a.dataset_fields(1, "i1-e11", "done")["state"] == "prepared"


# --- the judge: evidence in, an assessment out ----------------------------------------------------------


def _judged_round(t, verified_n=2):
    """A round with a crop and a verify workspace holding `verified_n` verified crops; returns their paths."""
    cw = Path(t.a._make_workspace(1, ROUND, "crop-" + SNIP_SHA, 1))
    vw = Path(t.a._make_workspace(1, ROUND, "verify-" + SNIP_SHA, 1))
    verified = [_crop(cw, f"c{i}", i) for i in range(verified_n)]
    for crop in verified:
        (vw / "preview" / crop["crop_id"]).mkdir(parents=True)
        (vw / "preview" / crop["crop_id"] / "overlay.png").write_bytes(b"ov")
    (vw / "binding.yaml").write_text("record: rec-1\nraw: [{snippet: s, urls: [u]}]\nreasons: [guessed the axes]\n")
    (vw / "verify.json").write_text('{"n": 2}')
    return cw, vw, verified


async def test_prepare_judge_builds_evidence_without_reasons(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    cw, vw, verified = _judged_round(t)
    (t.a._round_workspace(1, ROUND) / "match-1").mkdir(parents=True)
    ws = Path(await t.a.prepare_judge(1, ROUND, "crops", str(cw), str(vw), verified))
    assert ws == t.a._round_workspace(1, ROUND) / "judge-crops-1"
    ev = ws / "evidence"
    assert (ev / "c0" / "raw.png").read_bytes() == b"rawc0" and (ev / "c1" / "overlay.png").read_bytes() == b"ov"
    binding = yaml.safe_load((ev / "binding.yaml").read_text())
    assert "reasons" not in binding and binding["record"] == "rec-1"
    assert yaml.safe_load((ev / "record.yaml").read_text())["imaging"]["modality"] == ["confocal"]
    question = (ev / "question.md").read_text()
    assert "serious issues" in question.lower() and "- `c0`" in question and "- `c1`" in question
    listed = json.loads((ev / "evidence.json").read_text())["files"]
    assert {"question.md", "record.yaml", "binding.yaml", "verify.json", "c0/raw.png", "c1/overlay.png"} <= set(listed)
    with pytest.raises(ApplicationError, match="crops gate"):
        await t.a.prepare_judge(1, ROUND, "review", str(cw), str(vw), verified)


def _claude_json(ws: Path, verdict: dict, cost=0.31):
    (ws / "claude.json").write_text(
        json.dumps({"result": json.dumps(verdict), "total_cost_usd": cost, "modelUsage": {"claude-opus-5-5": {}}})
    )


async def test_read_judge_records_assessment_and_comment(tmp_path, token_server):  # noqa: F811
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    t.a.registry = Registry(token_server, "t")
    cw, vw, verified = _judged_round(t)
    ws = Path(await t.a.prepare_judge(1, ROUND, "crops", str(cw), str(vw), verified))
    _claude_json(
        ws,
        {
            "verdict": "approve",
            "confidence": 0.9,
            "findings": [{"item": "c0", "severity": "ok", "note": "cells | nuclei <!-- x -->"}],
            "summary": "One dataset, read right.",
            "schema_version": 1,
        },
    )
    out = await t.a.read_judge(1, ROUND, str(ws), "crops", "crop_set", f"{ROUND}:t1", "shadow")
    assert (out.verdict, out.effective, out.confidence, out.cost_usd) == ("approve", "approve", 0.9, 0.31)
    assert out.assessment and out.findings[0]["note"] == "cells | nuclei &lt;!-- x -->"
    (row,) = t.a.registry.assessments(subject_kind="crop_set", subject_ref=f"{ROUND}:t1")
    assert row["id"] == out.assessment and row["by"] == "judge" and row["verdict"] == "approve"
    assert row["model"] == "claude-opus-5-5" and len(row["prompt_version"]) == 12 and len(row["evidence_digest"]) == 64
    assert row["checks"]["verify"] == t.a._pins(ROUND).harness_commit and row["confidence"] == 0.9
    verdict = json.loads((ws / "verdict.json").read_text())
    assert verdict["assessment"]["id"] == out.assessment and verdict["cost_usd"] == 0.31
    assert "**Crop judge:** `approve`" in out.comment and "Shadow mode" in out.comment
    assert "| c0 | ok | cells \\| nuclei &lt;!-- x --> |" in out.comment and "$0.31" in out.comment
    assert "model `claude-opus-5-5`" in out.comment


async def test_read_judge_garbage_is_escalate(tmp_path, token_server):  # noqa: F811
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    t.a.registry = Registry(token_server, "t")
    cw, vw, verified = _judged_round(t)
    ws = Path(await t.a.prepare_judge(1, ROUND, "crops", str(cw), str(vw), verified))
    out = await t.a.read_judge(1, ROUND, str(ws), "crops", "crop_set", f"{ROUND}:t1", "shadow")
    assert out.effective == "escalate" and out.summary.startswith("unreadable verdict") and out.cost_usd is None
    (row,) = t.a.registry.assessments(subject_kind="crop_set", subject_ref=f"{ROUND}:t1")
    assert row["verdict"] == "escalate" and row["model"] == "opus"
    (ws / "claude.json").write_text(
        json.dumps({"result": "I looked at the crops and they seem fine.", "total_cost_usd": 1})
    )
    out = await t.a.read_judge(1, ROUND, str(ws), "crops", "crop_set", f"{ROUND}:t1", "shadow")
    assert out.effective == "escalate" and "`escalate`" in out.comment


async def test_read_judge_reject_without_bad_is_approve(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    cw, vw, verified = _judged_round(t)
    ws = Path(await t.a.prepare_judge(1, ROUND, "crops", str(cw), str(vw), verified))
    _claude_json(
        ws,
        {
            "verdict": "reject",
            "confidence": 0.8,
            "findings": [{"item": "c1", "severity": "suspect", "note": "dim"}],
            "summary": "nits",
        },
    )
    out = await t.a.read_judge(1, ROUND, str(ws), "crops", "crop_set", f"{ROUND}:t1", "shadow")
    assert (out.verdict, out.effective) == ("reject", "approve") and out.assessment
    assert "`approve` (the judge said `reject`: reject without a bad finding" in out.comment
    _claude_json(ws, {"verdict": "approve", "confidence": 0.3, "findings": [], "summary": "unsure"})
    out = await t.a.read_judge(1, ROUND, str(ws), "crops", "crop_set", f"{ROUND}:t1", "shadow")
    assert out.effective == "escalate" and "under the 0.6 floor" in out.comment


async def test_limits_carry_the_judge_config(tmp_path):
    t = _setup(tmp_path)
    t.a.cfg = t.cfg.model_copy(
        update={
            "judge": t.cfg.judge.model_copy(
                update={"max_budget_usd": 3.5, "crops": t.cfg.judge.crops.model_copy(update={"mode": "shadow"})}
            )
        }
    )
    limits = await t.a.limits()
    assert (limits.crop_judge, limits.judge_max_budget_usd) == ("shadow", 3.5)


async def test_read_judge_applies_the_suspect_cap(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    cw, vw, verified = _judged_round(t)
    ws = Path(await t.a.prepare_judge(1, ROUND, "crops", str(cw), str(vw), verified, 1, "abc"))
    assert ws.name == "judge-crops-abc-1"
    findings = [{"item": f"c{i}", "severity": "suspect", "note": "dim"} for i in range(3)]
    _claude_json(ws, {"verdict": "approve", "confidence": 0.9, "findings": findings, "summary": "meh"})
    out = await t.a.read_judge(1, ROUND, str(ws), "crops", "crop_set", f"{ROUND}:t1", "shadow")
    assert out.effective == "reject" and "more than the cap of 2" in out.comment


# --- with buffer-api: a round writes the timeline, the mirror writes the issue (spec webapp-actions §6, §7) ---


def _with_service(t, tmp_path):
    """The ledger of the fixture's in-process service."""
    return t.ledger


async def test_a_round_completes_its_writes_with_every_issue_call_failing_and_the_mirror_catches_up(tmp_path):
    from mia_data.config import STAGES
    from mia_data.mirror import Mirror

    t = _setup(tmp_path)
    ledger = _with_service(t, tmp_path)
    state = DatasetState(1, round_id=ROUND, pins=await t.a.admit(1, "99"))
    t.a.registry.adopt_subject("dataset", 1, "rec-1")
    working = {name: getattr(t.data, name) for name in ("get_issue", "issue_comments", "comment", "add_labels")}

    async def away(*args, **kwargs):
        raise RuntimeError("GitHub is away")

    for name in working:
        setattr(t.data, name, away)
    await t.a.set_stage(1, "matching", state)
    await t.a.set_stage(1, "matching", state)  # a retry adds nothing
    assert await t.a.comment(1, "Test crops: **passed**", "test r 1") is None
    await t.a.set_stage(1, "needs-human", state, "host <!-- x --> missing")
    subject = ledger.subject("dataset", "rec-1")
    assert (subject["stage"], subject["revision"]) == ("needs-human", 3)
    entries = ledger.timeline("dataset", 1)
    assert [entry["stage"] for entry in entries] == ["matching", None, "needs-human"]
    assert entries[0]["round"] == ROUND and "<!-- x" not in entries[2]["body"] and "host" in entries[2]["body"]
    for name, method in working.items():  # GitHub is back: the issue reads as it always did
        setattr(t.data, name, method)
    mirror = Mirror(t.data, t.a.registry, "bot", {"dataset": (t.cfg.label, STAGES)})
    assert await mirror.mirror("dataset", 1) == 3
    bodies = [comment["body"] for comment in t.data.comments[1]]
    assert bodies[0].startswith("**Stage:** `data:matching`") and bodies[1].startswith("Test crops: **passed**")
    assert [label["name"] for label in t.data.issues[1]["labels"]] == ["data:needs-human"]


async def test_a_write_for_a_dataset_the_registry_does_not_know_fails(tmp_path):
    t = _setup(tmp_path)
    with pytest.raises(RegistryError) as error:
        await t.a.set_stage(2, "new")
    assert error.value.status == 404


async def test_a_refusal_is_a_timeline_entry(tmp_path):
    t = _setup(tmp_path)
    ledger = _with_service(t, tmp_path)
    t.a.registry.adopt_subject("dataset", 1, "rec-1")
    t.data.issues[1]["labels"] = [{"name": "data:go"}]
    await t.a.refuse_label(1, "data:go", "a round is running", "42")  # an event that is no action
    assert t.data.removed == [] and t.data.comments == {}  # the issue is the mirror's
    (entry,) = ledger.timeline("dataset", 1)
    assert entry["body"].startswith("Ignoring `data:go`: a round is running") and entry["stage"] is None


async def test_kill_job_kills_by_id_and_a_cancelled_dataset_reads_as_stopped(tmp_path):
    t = _setup(tmp_path)
    await t.a.kill_job("777")
    assert t.csub.killed == ["777"]
    await t.a.admit(1, "99")
    assert t.a.dataset_fields(1, ROUND, "cancelled")["state"] == "needs-human"  # what follows it stops waiting
    await t.a.refuse_label(1, "data:cancel", "there is no round in progress to cancel", "42")  # an action's name
    assert "Ignoring `data:cancel`" in t.ledger.timeline("dataset", 1)[0]["body"]


async def test_admit_for_an_action_reads_the_record_and_the_root_from_the_registry(tmp_path):
    t = _setup(tmp_path)
    ledger = _with_service(t, tmp_path)
    number = ledger.create_subject("dataset", "rec-1")["number"]  # started from the page: it has no issue
    go, _ = ledger.add_action("dataset", number, "go", "prep:9", payload={"root": "o/buffers#9"})

    async def no_issue(issue_number):
        raise AssertionError("an action's round must not read the issue")

    t.data.get_issue = no_issue
    pins = await t.a.admit(number, str(go["id"]))
    assert (pins.record, pins.root) == ("rec-1", "o/buffers#9")  # charged to whoever asked
    plain, _ = ledger.add_action("dataset", number, "go", "page")
    assert (await t.a.admit(number, str(plain["id"]))).root == f"o/data#{number}"  # nobody else: itself
    other = ledger.create_subject("dataset", "rec-2")["number"]
    with pytest.raises(ApplicationError, match="is not an action on dataset"):
        await t.a.admit(other, str(go["id"]))


def test_every_activity_is_registered_on_a_worker(tmp_path):
    """An activity the workflow calls but no worker registers is retried for hours, with the round stuck."""
    acts = _setup(tmp_path).a
    defined = {name for name, member in vars(type(acts)).items() if hasattr(member, "__temporal_activity_definition")}
    assert defined and defined == {method.__name__ for method in [*acts.all(), acts.await_job]}


async def test_read_feedback_is_the_note_of_the_reject_action_in_the_window(tmp_path, monkeypatch):
    from mia_buffers import ledger as ledger_module

    t = _setup(tmp_path)
    ledger = _with_service(t, tmp_path)
    ledger.adopt_subject("dataset", 1, "rec-1")
    now = [1_800_000_000.0]
    monkeypatch.setattr(ledger_module.time, "time", lambda: now[0])
    ledger.add_action("dataset", 1, "reject", "page", payload={"note": "an earlier set's note"})
    now[0] += 600
    reject, _ = ledger.add_action("dataset", 1, "reject", "page", payload={"note": "the masks are shifted <!-- by one"})
    text = await t.a.read_feedback(1, "2027-01-15T08:05:00Z", reject["at"])  # since the set's report, up to the reject
    assert text == "page wrote:\nthe masks are shifted &lt;!-- by one"
    now[0] += 600
    bare, _ = ledger.add_action("dataset", 1, "reject", "page")
    assert await t.a.read_feedback(1, reject["at"], bare["at"]) == "(no details)"  # a reject without a note
    assert await t.a.read_feedback(1, "2026-10-27T10:05:00Z", "2026-10-27T10:12:00Z") == "(no details)"  # none then


async def test_refusing_an_action_marks_it_refused_and_touches_no_label(tmp_path):
    t = _setup(tmp_path)
    ledger = _with_service(t, tmp_path)
    ledger.adopt_subject("dataset", 1, "rec-1")
    go, _ = ledger.add_action("dataset", 1, "go", "page")
    await t.a.refuse_label(1, "data:go", "busy <!-- x -->", str(go["id"]))
    refused = ledger.action(go["id"])
    assert (refused["state"], refused["detail"]) == ("refused", "busy &lt;!-- x -->") and t.data.removed == []
    assert ledger.timeline("dataset", 1)[0]["body"].startswith("Refused `go`: busy")  # the action, not a label
