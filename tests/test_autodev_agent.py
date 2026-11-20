import json
import os
import subprocess
import threading
from types import SimpleNamespace

import pytest
from autodev_helpers import K, make_cfg, make_origin, run_git
from csub.protocol import CsubError, JobSpec, ProtocolError
from csub.transport.base import TransportError
from temporalio.exceptions import ApplicationError

from autodev.activities import Activities
from autodev.agent import (
    AgentJobs,
    csub_client,
    cut_off,
    hosts_marker,
    issue_hosts,
    job_script,
    log_tail,
    read_no_change,
    read_result,
    render_prompt,
)
from autodev.gitops import Mirror
from autodev.models import SubmitInput, Workspace

ISSUE = {"number": 7, "title": "Add feature", "body": "Please add feature.txt"}


def test_prompt_has_issue_and_human_discussion_only(tmp_path):
    cfg = make_cfg(tmp_path)
    comments = [
        {"user": {"login": "alice"}, "body": "use lowercase"},
        {"user": {"login": "bot"}, "body": "Started <!-- autodev: started k-1 -->"},
    ]
    p = render_prompt(cfg, ISSUE, comments, "autodev/issue-7", "abc", None, None)
    assert "#7 in o/r" in p and "Add feature" in p and "Please add feature.txt" in p
    assert "use lowercase" in p and "Started" not in p and "Requested changes" not in p
    p2 = render_prompt(cfg, ISSUE, [], "autodev/issue-7", "abc", 5, "rename it")
    assert "Pull request #5" in p2 and "rename it" in p2


def test_job_script_runs_claude_and_bundles(tmp_path):
    """Run the real script with a fake `claude` on PATH, against a real clone."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "claude"
    fake.write_text(
        '#!/bin/sh\necho x > feature.txt\ngit add feature.txt\ngit commit -qm "add feature"\n'
        'echo \'{"type":"result","result":"Added feature.txt","total_cost_usd":0.12}\'\n'
    )
    fake.chmod(0o755)
    cfg = make_cfg(tmp_path, agent={"path_prepend": [str(bin_dir)], "model": "it's-quoted"})
    m = Mirror(tmp_path / "mirror.git", str(make_origin(tmp_path)))
    m.sync()
    d = tmp_path / "round-1"
    base = m.workspace(d, "main", "autodev/issue-7", "https://example.invalid/r.git")
    (d / "prompt.md").write_text("do it")
    script = job_script(cfg, base)
    r = subprocess.run(["bash", "-c", script], cwd=d)
    assert r.returncode == 0
    assert (d / "exit_code").read_text().strip() == "0"
    assert read_result(d) == ("Added feature.txt", 0.12)
    assert run_git("bundle", "list-heads", str(d / "result.bundle"), cwd=d)


def test_job_script_budget_flag(tmp_path):
    assert "--max-budget-usd 5.0" in job_script(make_cfg(tmp_path, agent={"max_budget_usd": 5}), "abc")
    assert "--max-budget-usd" not in job_script(make_cfg(tmp_path), "abc")


def test_read_result_tolerates_garbage(tmp_path):
    assert read_result(tmp_path)[0].startswith("(the agent left no readable result.json")
    (tmp_path / "result.json").write_text("[1, 2]")
    assert read_result(tmp_path)[1] is None
    (tmp_path / "result.json").write_text(json.dumps({"result": "x" * 50000, "total_cost_usd": "free"}))
    text, cost = read_result(tmp_path)
    assert len(text) < 20100 and text.endswith("(truncated)") and cost is None


def test_cut_off_is_claude_codes_own_error_and_nothing_else(tmp_path):
    for result, said in (
        ({"is_error": True, "result": "Not logged in · Please run /login"}, "Not logged in · Please run /login"),
        ({"is_error": True, "result": "the tests fail"}, ""),  # the agent's own failure
        ({"is_error": False, "result": "I could not log in to the FTP server; Please run /login"}, ""),
        (["is_error"], ""),
    ):
        (tmp_path / "result.json").write_text(json.dumps(result))
        assert cut_off(tmp_path) == said
    (tmp_path / "result.json").unlink()
    assert cut_off(tmp_path) == ""


def test_log_tail_reads_claude_and_job_stderr(tmp_path):
    (tmp_path / "claude.err").write_text("\n".join(f"line {i}" for i in range(100)))
    (tmp_path / ".csub" / "jobs" / "9").mkdir(parents=True)
    (tmp_path / ".csub" / "jobs" / "9" / "stderr").write_text("bwrap: oops\n")
    t = log_tail(tmp_path, lines=5)
    assert "line 99" in t and "line 90" not in t and "bwrap: oops" in t


class FakeCsub:
    def __init__(self, states=("DONE",), existing=(), error=None, policy_hosts=()):
        self.policy_hosts, self.probes, self.probe_error = list(policy_hosts), 0, None
        self.submitted, self.states, self.existing, self.error = [], list(states), list(existing), error
        self.killed = []

    def submit(self, spec):
        if self.error:
            raise self.error
        JobSpec.from_dict(spec)
        self.submitted.append(spec)
        return SimpleNamespace(job_id="777", walltime_min=spec["walltime_min"])

    def probe(self):
        self.probes += 1
        if self.probe_error:
            raise self.probe_error
        return SimpleNamespace(allowed_hosts=list(self.policy_hosts))

    def kill(self, job_ids=None):
        self.killed.extend(job_ids or [])
        return SimpleNamespace(killed=list(job_ids or []))

    def status(self, ids=None):
        if ids is None:
            return self.existing
        st = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return [
            SimpleNamespace(
                job_id=ids[0],
                state=st,
                exit_code=0 if st == "DONE" else None,
                name=None,
                terminal=st in ("DONE", "EXIT", "UNKNOWN"),
            )
        ]


def _jobs(tmp_path, fake, **agent):
    sleeps = []

    async def sleep(s):
        sleeps.append(s)

    j = AgentJobs(make_cfg(tmp_path, agent=agent), client_factory=lambda: fake, sleep=sleep)
    j.sleeps = sleeps
    return j


async def test_submit_is_idempotent_and_scoped(tmp_path):
    fake = FakeCsub()
    j = _jobs(tmp_path, fake, extra_mounts=["/tools:ro"], allow_hosts=["pypi.org"])
    ws = Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev/issue-7")
    ref = await j.submit(7, K, 1, ws)
    assert ref.job_id == "777" and await j.submit(7, K, 1, ws) == ref
    assert len(fake.submitted) == 1
    spec = fake.submitted[0]
    assert spec["agent"] == "claude" and spec["shell"] is True and spec["cwd"] == ws.dir
    assert spec["mounts"] == [{"path": ws.dir, "mode": "rw"}, {"path": "/tools", "mode": "ro"}]
    assert spec["allow_hosts"] == ["pypi.org"] and spec["name"] == "autodev-7-r1" and "env" not in spec


async def test_an_agent_job_reads_the_evidence_folder_of_its_own_issue_and_no_other(tmp_path):
    """spec judge-decides §4.1: the folder is found by the issue's number under the configured directory, never
    by a path an issue or a comment names."""
    evidence = tmp_path / "evidence"
    (evidence / "issue-8").mkdir(parents=True)  # another issue's
    ws = Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev/issue-7")
    fake = FakeCsub()
    j = _jobs(tmp_path, fake, evidence_dir=str(evidence))
    await j.submit(7, K, 1, ws)
    assert fake.submitted[0]["mounts"] == [{"path": ws.dir, "mode": "rw"}]  # none for this issue: no mount
    (evidence / "issue-7").mkdir()
    await j.submit(7, K, 2, ws)
    assert fake.submitted[1]["mounts"] == [
        {"path": ws.dir, "mode": "rw"},
        {"path": str(evidence / "issue-7"), "mode": "ro"},
    ]
    (evidence / "issue-9").symlink_to(tmp_path)  # a link is not a folder of evidence
    await j.submit(9, K, 1, ws)
    assert fake.submitted[2]["mounts"] == [{"path": ws.dir, "mode": "rw"}]
    plain = _jobs(tmp_path, FakeCsub())  # no evidence_dir configured: nothing is looked for
    assert plain.cfg.agent.evidence_dir is None


def test_the_prompt_names_the_evidence_folder_when_there_is_one(tmp_path):
    evidence = tmp_path / "evidence"
    cfg = make_cfg(tmp_path, agent={"evidence_dir": str(evidence)})
    issue = {"number": 7, "title": "T", "body": "b"}
    assert "evidence" not in render_prompt(cfg, issue, [], "autodev/issue-7", "abc", None, None).lower()
    (evidence / "issue-7").mkdir(parents=True)
    prompt = render_prompt(cfg, issue, [], "autodev/issue-7", "abc", None, None)
    assert f"`{evidence / 'issue-7'}`" in prompt and "read-only" in prompt


async def test_an_agent_job_names_its_configured_hosts_then_those_its_issue_names(tmp_path):
    """The broker's policy is the ceiling, never the list: a job gets the configured hosts and, of the hosts its
    issue names, those the policy allows."""
    too_long = "a" * 64 + ".example.org"  # csub refuses a job host with a label over 63 characters
    policy = ["pypi.org", "zenodo.org", "*.huggingface.co", "github.com", too_long, "data.4tu.nl"]
    named = ["zenodo.org", "cdn.huggingface.co", "github.com", too_long, "evil.example.org", "pypi.org", "10.0.0.1"]
    fake = FakeCsub(policy_hosts=policy)
    j = _jobs(tmp_path, fake, allow_hosts=["pypi.org"])
    await j.submit(7, K, 1, Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev/issue-7"), named)
    assert fake.submitted[0]["allow_hosts"] == ["pypi.org", "zenodo.org", "cdn.huggingface.co"]
    await j.submit(8, K, 1, Workspace(str(tmp_path / "work" / "r2"), "abc123", "autodev/issue-8"))
    assert fake.submitted[1]["allow_hosts"] == ["pypi.org"]  # an issue that names none: nothing of the policy's


async def test_an_agent_job_gets_raw_github_when_its_issue_names_it_and_no_other_github_host(tmp_path):
    """2026-11-19, RV-PBS: a dataset whose files are on raw.githubusercontent.com alone. The reader's job was
    granted no GitHub host whatever its issue named, so the agent could read nothing and made no commits."""
    raw = "raw.githubusercontent.com"
    named = [raw, "github.com", "gist.githubusercontent.com", "api.github.com"]
    fake = FakeCsub(policy_hosts=[*named, "pypi.org"])
    j = _jobs(tmp_path, fake, allow_hosts=["pypi.org"])
    await j.submit(7, K, 1, Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev/issue-7"), named)
    assert fake.submitted[0]["allow_hosts"] == ["pypi.org", raw]
    await j.submit(8, K, 1, Workspace(str(tmp_path / "work" / "r2"), "abc123", "autodev/issue-8"))
    assert fake.submitted[1]["allow_hosts"] == ["pypi.org"]  # the policy allowing it grants it to no other issue
    unapproved = FakeCsub(policy_hosts=["pypi.org"])
    j = _jobs(tmp_path / "other", unapproved, allow_hosts=["pypi.org"])
    await j.submit(7, K, 1, Workspace(str(tmp_path / "work" / "r3"), "abc123", "autodev/issue-7"), named)
    assert unapproved.submitted[0]["allow_hosts"] == ["pypi.org"]


def test_an_issue_names_its_jobs_hosts_in_a_marker():
    marker = hosts_marker(["zenodo.org", "ftp.ebi.ac.uk"])
    assert marker == "<!-- autodev: hosts zenodo.org ftp.ebi.ac.uk -->"
    assert issue_hosts(f"For a dataset.\n\nIts job may reach `zenodo.org`. {marker}\n\nmore") == [
        "zenodo.org",
        "ftp.ebi.ac.uk",
    ]
    assert issue_hosts("no marker") == [] and issue_hosts("<!-- autodev: hosts -->") == [] and issue_hosts(None) == []
    assert issue_hosts(f"{marker} <!-- autodev: hosts evil.example.org -->") == ["zenodo.org", "ftp.ebi.ac.uk"]


async def test_only_an_issue_a_trusted_bot_opened_names_hosts(tmp_path):
    body = "For a dataset. " + hosts_marker(["zenodo.org"])

    class Issues:
        def __init__(self, author):
            self.author = author

        async def get_issue(self, number):
            return {"number": number, "user": {"login": self.author}, "body": body}

    for author, trusted, expected in (
        ("mia-bot[bot]", ["mia-bot"], ["pypi.org", "zenodo.org"]),
        ("mallory", ["mia-bot"], ["pypi.org"]),
        ("mia-bot[bot]", [], ["pypi.org"]),  # this deployment trusts no bot
    ):
        fake = FakeCsub(policy_hosts=["zenodo.org"])
        cfg = make_cfg(
            tmp_path / author / str(len(trusted)), agent={"allow_hosts": ["pypi.org"]}, github={"trusted_bots": trusted}
        )
        jobs = AgentJobs(cfg, client_factory=lambda fake=fake: fake)
        activities = Activities(cfg, Issues(author), None, jobs, "bot")
        await activities.submit_agent(SubmitInput(7, K, 1, Workspace(str(cfg.paths.work_root / "r1"), "abc", "b")))
        assert fake.submitted[0]["allow_hosts"] == expected, (author, trusted)


async def test_a_retry_does_not_probe_the_broker(tmp_path):
    fake = FakeCsub()
    j = _jobs(tmp_path, fake)
    ws = Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev/issue-7")
    ref = await j.submit(7, K, 1, ws)
    fake.probe_error = OSError("socket down")
    assert await j.submit(7, K, 1, ws) == ref and fake.probes == 1


async def test_a_failed_probe_on_a_first_submission_submits_nothing(tmp_path):
    fake = FakeCsub()
    fake.probe_error = OSError("socket down")
    j = _jobs(tmp_path, fake)
    with pytest.raises(OSError):
        await j.submit(7, K, 1, Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev/issue-7"))
    assert fake.submitted == []


async def test_a_job_that_needs_more_hosts_than_csub_takes_is_not_started(tmp_path):
    """Past the cap nothing is dropped to fit: a job without a host it needs fails later and says less."""

    class Issues:
        def __init__(self, named):
            self.body = hosts_marker(named)

        async def get_issue(self, number):
            return {"number": number, "user": {"login": "mia-bot[bot]"}, "body": self.body}

    configured = ["pypi.org", "files.pythonhosted.org"]
    for count in (62, 63):
        named = [f"h{n}.example.org" for n in range(count)]
        fake = FakeCsub(policy_hosts=["*.example.org"])
        cfg = make_cfg(tmp_path / str(count), agent={"allow_hosts": configured}, github={"trusted_bots": ["mia-bot"]})
        activities = Activities(cfg, Issues(named), None, AgentJobs(cfg, client_factory=lambda fake=fake: fake), "bot")
        request = SubmitInput(7, K, 1, Workspace(str(cfg.paths.work_root / "r1"), "abc", "b"))
        if count == 62:
            await activities.submit_agent(request)
            assert fake.submitted[0]["allow_hosts"] == configured + named
            continue
        with pytest.raises(ApplicationError) as refused:
            await activities.submit_agent(request)
        assert refused.value.non_retryable and fake.submitted == []
        assert "65 hosts" in str(refused.value) and "at most 64" in str(refused.value)


async def test_submit_reuses_job_found_by_name(tmp_path):
    fake = FakeCsub(
        existing=[SimpleNamespace(job_id="555", name="autodev-7-r1", cwd=str(tmp_path / "round"), state="RUN")]
    )
    j = _jobs(tmp_path, fake)
    ref = await j.submit(7, K, 1, Workspace(str(tmp_path / "round"), "abc", "b"))
    assert ref.job_id == "555" and fake.submitted == []


async def test_submit_ignores_same_name_job_from_another_round_dir(tmp_path):
    fake = FakeCsub(existing=[SimpleNamespace(job_id="555", name="autodev-7-r1", cwd="/old/run", state="DONE")])
    ref = await _jobs(tmp_path, fake).submit(7, "c0ffee02", 1, Workspace(str(tmp_path / "round"), "abc", "b"))
    assert ref.job_id == "777" and len(fake.submitted) == 1


async def test_api_key_mode_passes_key_in_job_env(tmp_path, monkeypatch):
    monkeypatch.setenv("SVC_KEY", "sk-1")
    fake = FakeCsub()
    await _jobs(tmp_path, fake, api_key_env="SVC_KEY").submit(7, K, 1, Workspace(str(tmp_path / "round"), "a", "b"))
    assert fake.submitted[0]["env"] == {"ANTHROPIC_API_KEY": "sk-1"}


async def test_wait_heartbeats_until_terminal(tmp_path):
    fake = FakeCsub(states=["PEND", "RUN", "DONE"])
    j = _jobs(tmp_path, fake)
    beats = []
    out = await j.wait("777", Workspace(str(tmp_path), "a", "b"), beats.append)
    assert (out.state, out.exit_code) == ("DONE", 0)
    assert beats == ["PEND", "RUN", "DONE"] and j.sleeps == [60.0, 60.0]


async def test_unknown_state_uses_exit_code_file(tmp_path):
    (tmp_path / "exit_code").write_text("0\n")
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait(
        "777", Workspace(str(tmp_path), "a", "b"), lambda s: None
    )
    assert (out.state, out.exit_code) == ("DONE", 0)


def test_log_tail_ignores_symlinks(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("TOKEN-123")
    d = tmp_path / "round"
    (d / ".csub").mkdir(parents=True)
    (d / "claude.err").symlink_to(secret)
    (d / ".csub" / "jobs").symlink_to(tmp_path / "elsewhere")
    (tmp_path / "elsewhere" / "9").mkdir(parents=True)
    (tmp_path / "elsewhere" / "9" / "stderr").write_text("TOKEN-456")
    assert "TOKEN" not in log_tail(d)


def test_read_result_fifo_does_not_block(tmp_path):
    os.mkfifo(tmp_path / "result.json")
    out = []
    t = threading.Thread(target=lambda: out.append(read_result(tmp_path)), daemon=True)
    t.start()
    t.join(5)
    assert out and out[0][0].startswith("(the agent left no readable result.json")


def test_read_result_deeply_nested_json(tmp_path):
    (tmp_path / "result.json").write_text("[" * 100000)
    assert read_result(tmp_path)[0].startswith("(the agent left no readable")


async def test_exit_code_symlink_and_garbage_ignored(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("0")
    d = tmp_path / "r"
    d.mkdir()
    (d / "exit_code").symlink_to(secret)
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", Workspace(str(d), "a", "b"), lambda s: None)
    assert (out.state, out.exit_code) == ("UNKNOWN", None)
    (d / "exit_code").unlink()
    (d / "exit_code").write_text("\u00b2")
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", Workspace(str(d), "a", "b"), lambda s: None)
    assert out.exit_code is None


def test_csub_client_uses_deployment_session(tmp_path, monkeypatch):
    monkeypatch.setenv("CSUB_TRANSPORT", "local")
    c = csub_client(make_cfg(tmp_path, deployment="mydep"))
    assert c.config.session == "mydep"


async def test_symlink_loops_are_survived(tmp_path):
    d = tmp_path / "r"
    (d / ".csub" / "jobs" / "9").mkdir(parents=True)
    for rel in ("claude.err", "result.json", "exit_code", ".csub/jobs/9/stderr"):
        (d / rel).symlink_to(d / rel)
    assert log_tail(d) == ""
    assert read_result(d)[0].startswith("(the agent left no readable result.json")
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", Workspace(str(d), "a", "b"), lambda s: None)
    assert out.exit_code is None


def test_log_tail_reads_only_newest_job_dirs(tmp_path):
    for i in range(10):
        (tmp_path / ".csub" / "jobs" / str(i)).mkdir(parents=True)
        (tmp_path / ".csub" / "jobs" / str(i) / "stderr").write_text(f"err{i}")
    t = log_tail(tmp_path)
    assert "err9" in t and "err7" in t and "err6" not in t


async def test_submit_reuses_job_when_work_root_is_symlinked(tmp_path):
    """Config canonicalizes work_root, so round dirs (and the csub cwd/mount) match what the broker records."""
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "link").symlink_to(real)
    fake = FakeCsub(
        existing=[
            SimpleNamespace(job_id="555", name="x", cwd=str(real.resolve() / "r1"), state="RUN"),
            SimpleNamespace(job_id="556", name="y", state="RUN"),
        ]
    )
    cfg = make_cfg(tmp_path, paths={"state_dir": str(tmp_path / "state"), "work_root": str(tmp_path / "link")})
    j = AgentJobs(cfg, client_factory=lambda: fake)
    ws = Workspace(str(j.cfg.paths.work_root / "r1"), "abc", "b")
    assert ws.dir == str(real.resolve() / "r1")
    ref = await j.submit(7, K, 1, ws)
    assert ref.job_id == "555" and fake.submitted == []


@pytest.mark.parametrize(
    "err,permanent",
    [
        (CsubError("policy_violation", "mount not under allowed_roots"), True),
        (CsubError("invalid_request", "bad spec"), True),
        (ProtocolError("response: not valid JSON"), True),
        (TransportError("cannot reach broker socket"), False),
        (CsubError("lsf_error", "bsub timed out"), False),
    ],
)
async def test_submit_rejection_is_permanent_transport_error_is_not(tmp_path, err, permanent):
    j = _jobs(tmp_path, FakeCsub(error=err))
    a = Activities(j.cfg, None, None, j, "bot")
    with pytest.raises(Exception) as e:
        await a.submit_agent(SubmitInput(7, K, 1, Workspace(str(tmp_path / "round"), "abc", "b")))
    if permanent:
        assert isinstance(e.value, ApplicationError) and e.value.non_retryable and err.code in str(e.value)
    else:
        assert e.value is err


@pytest.mark.parametrize("key", ["", "../../tmp", "C0FFEE01", "c0ffee0", "c0ffee01/.."])
async def test_submit_refuses_a_key_that_is_not_8_hex(tmp_path, key):
    j = _jobs(tmp_path, FakeCsub())
    with pytest.raises(ApplicationError) as e:
        await Activities(j.cfg, None, None, j, "bot").submit_agent(
            SubmitInput(7, key, 1, Workspace(str(tmp_path / "round"), "a", "b"))
        )
    assert e.value.non_retryable and not (tmp_path / "state" / "jobs").exists()


async def test_job_name_leaves_out_the_deployment(tmp_path):
    # csub rejects job names containing the submitter's username, and personal deployments are autodev-<user>
    fake = FakeCsub()
    j = AgentJobs(make_cfg(tmp_path, deployment="autodev-someuser"), client_factory=lambda: fake)
    await j.submit(7, K, 1, Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev-someuser/issue-7"))
    assert fake.submitted[0]["name"] == "autodev-7-r1" and fake.submitted[0]["session"] == "autodev-someuser"


def test_judge_script_is_read_only(tmp_path):
    from autodev.agent import judge_script

    script = judge_script(make_cfg(tmp_path, agent={"path_prepend": ["/opt/bin"]}, judge={"model": "opus"}))
    assert script.startswith("set -u\nexport PATH=/opt/bin:")
    assert '--allowedTools "Read,Glob,Grep"' in script and '--tools "Read,Glob,Grep"' in script
    assert "--restricted" in script and "--add-dir evidence > claude.json" in script and "--model opus" in script
    assert "> exit_code" in script and "git bundle" not in script and "Edit" not in script and "Bash" not in script


REQUEST = """/revise Request: i7-e1:t1#1

The crop judge looked at the sample crops this reader made and doubts them. Its findings:

- crop-3 [suspect]: labels look shifted
- crop-5 [bad]: empty

Look into every finding before you change anything."""
ANSWERS = [
    {"item": "crop-3", "answer": "the shift is the label's own: the record says so"},
    {"item": "crop-5", "answer": "there are no cells in that field"},
]


def _answered(round_dir, content, result=None):
    """A round dir whose agent left `content` as no_change.json in its checkout, and a result object."""
    (round_dir / "repo").mkdir(exist_ok=True)
    (round_dir / "result.json").write_text(json.dumps(result or {"result": "nothing to change", "is_error": False}))
    if content is not None:
        text = content if isinstance(content, str) else json.dumps(content)
        (round_dir / "repo" / "no_change.json").write_text(text)


def test_a_complete_answer_to_this_request_counts(tmp_path):
    _answered(tmp_path, {"request": "i7-e1:t1#1", "findings": ANSWERS})
    assert read_no_change(tmp_path, REQUEST) == (
        "i7-e1:t1#1",
        {"crop-3": "the shift is the label's own: the record says so", "crop-5": "there are no cells in that field"},
    )


@pytest.mark.parametrize(
    "content",
    [
        None,  # no file
        "not json",
        ["a", "list"],
        {"request": "i7-e0:t9#1", "findings": ANSWERS},  # another request
        {"request": "i7-e1:t1#1", "findings": ANSWERS[:1]},  # one finding left out
        {"request": "i7-e1:t1#1", "findings": [{"item": "crop-3", "answer": " "}, ANSWERS[1]]},  # an empty answer
        {"request": "i7-e1:t1#1", "findings": [*ANSWERS, {"item": "crop-9", "answer": "x"}]},  # one it was not asked
        {"request": "i7-e1:t1#1", "findings": [ANSWERS[0], ANSWERS[0]]},  # one twice, one missing
        {"request": "i7-e1:t1#1", "findings": "all fine"},
        {"request": "i7-e1:t1#1", "findings": [{"item": "crop-3", "answer": 1}, ANSWERS[1]]},
        "x" * (300 * 1024),  # oversized
    ],
)
def test_anything_else_is_not_an_answer(tmp_path, content):
    _answered(tmp_path, content)
    assert read_no_change(tmp_path, REQUEST) == ("", {})


def test_an_answer_counts_only_for_a_request_with_findings_and_a_run_that_ended_by_itself(tmp_path):
    good = {"request": "i7-e1:t1#1", "findings": ANSWERS}
    _answered(tmp_path, good)
    assert read_no_change(tmp_path, "/revise please rename the file") == ("", {})  # nothing was asked that way
    assert read_no_change(tmp_path, "Request: i7-e1:t1#1\n\nno findings listed") == ("", {})
    _answered(tmp_path, good, result={"result": "API Error", "is_error": True})
    assert read_no_change(tmp_path, REQUEST) == ("", {})  # Claude Code ended the session
    (tmp_path / "result.json").write_text("garbage")
    assert read_no_change(tmp_path, REQUEST) == ("", {})
    _answered(tmp_path, good)
    (tmp_path / "repo" / "no_change.json").unlink()
    (tmp_path / "elsewhere.json").write_text(json.dumps(good))
    (tmp_path / "repo" / "no_change.json").symlink_to(tmp_path / "elsewhere.json")
    assert read_no_change(tmp_path, REQUEST) == ("", {})  # a link is not the agent's file
