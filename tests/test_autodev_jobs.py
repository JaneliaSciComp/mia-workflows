import gzip
import json
from types import SimpleNamespace

import pytest
from test_autodev_agent import FakeCsub

from autodev.jobs import CsubJobs, JobSpec
from mia_core import jobrecord


def _jobs(tmp_path, fake):
    return CsubJobs("dep", tmp_path / "ids", client_factory=lambda: fake)


async def test_submit_once_per_key(tmp_path):
    fake = FakeCsub()
    spec = JobSpec("j", "true", str(tmp_path / "ws"), 10)
    assert await _jobs(tmp_path, fake).submit("k1", spec) == "777"
    assert (tmp_path / "ids" / "k1.id").read_text() == "777"
    fresh = CsubJobs("dep", tmp_path / "ids", client_factory=lambda: pytest.fail("client used"))
    assert await fresh.submit("k1", spec) == "777" and len(fake.submitted) == 1


async def test_a_spec_factory_may_be_sync_or_async(tmp_path):
    fake = FakeCsub()

    async def async_spec():
        return JobSpec("j", "true", str(tmp_path / "ws"), 10)

    assert await _jobs(tmp_path, fake).submit("k1", async_spec) == "777"
    assert await _jobs(tmp_path, fake).submit("k2", lambda: JobSpec("j", "true", str(tmp_path / "ws"), 10)) == "777"
    assert len(fake.submitted) == 2


async def test_reattach_by_cwd(tmp_path):
    ws = tmp_path / "ws"
    fake = FakeCsub(
        existing=[SimpleNamespace(job_id="555", cwd="/elsewhere"), SimpleNamespace(job_id="556", cwd=str(ws))]
    )
    assert await _jobs(tmp_path, fake).submit("k1", JobSpec("j", "true", str(ws), 10)) == "556"
    assert fake.submitted == [] and (tmp_path / "ids" / "k1.id").read_text() == "556"


async def test_plain_job_spec(tmp_path):
    fake = FakeCsub()
    ws = str(tmp_path / "ws")
    await _jobs(tmp_path, fake).submit(
        "k1",
        JobSpec(
            "harness",
            "echo hi",
            ws,
            30,
            cpus=2,
            mem_mb=8000,
            allow_hosts=["pypi.org"],
            ro_mounts=["/data", "/tools"],
            env={"A": "1"},
        ),
    )
    s = fake.submitted[0]
    assert s["agent"] is None and s["env"] == {"A": "1"} and s["session"] == "dep" and s["cwd"] == ws
    assert s["mounts"] == [
        {"path": ws, "mode": "rw"},
        {"path": "/data", "mode": "ro"},
        {"path": "/tools", "mode": "ro"},
    ]
    assert (s["name"], s["command"], s["cpus"], s["mem_mb"], s["walltime_min"], s["allow_hosts"]) == (
        "harness",
        "echo hi",
        2,
        8000,
        30,
        ["pypi.org"],
    )


async def test_rw_mounts_follow_the_workspace(tmp_path):
    fake = FakeCsub()
    ws = str(tmp_path / "ws")
    await _jobs(tmp_path, fake).submit("k1", JobSpec("j", "true", ws, 10, ro_mounts=["/ro"], rw_mounts=["/cache"]))
    assert fake.submitted[0]["mounts"] == [
        {"path": ws, "mode": "rw"},
        {"path": "/cache", "mode": "rw"},
        {"path": "/ro", "mode": "ro"},
    ]


@pytest.mark.parametrize("key", ["../x", "", "a/b", "x" * 121])
async def test_bad_key_rejected(tmp_path, key):
    with pytest.raises(ValueError):
        await _jobs(tmp_path, FakeCsub()).submit(key, JobSpec("j", "true", str(tmp_path), 10))
    assert not (tmp_path / "ids").exists()


async def test_wait_reads_exit_code_when_csub_forgot_the_job(tmp_path):
    (tmp_path / "exit_code").write_text("3\n")
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", str(tmp_path), lambda s: None)
    assert (out.state, out.exit_code) == ("EXIT", 3)


async def test_recorded_id_skips_building_the_spec(tmp_path, monkeypatch):
    from autodev_helpers import K, make_cfg

    from autodev.agent import AgentJobs
    from autodev.models import Workspace

    monkeypatch.delenv("UNSET_KEY", raising=False)
    j = AgentJobs(
        make_cfg(tmp_path, agent={"api_key_env": "UNSET_KEY"}), client_factory=lambda: pytest.fail("client used")
    )
    (j.cfg.paths.state_dir / "jobs").mkdir(parents=True)
    (j.cfg.paths.state_dir / "jobs" / f"issue-7-{K}-r1.id").write_text("444")
    assert (await j.submit(7, K, 1, Workspace(str(tmp_path), "a", "b"))).job_id == "444"


async def test_wait_kills_the_job_when_the_heartbeat_loses_the_lock(tmp_path):
    from mia_core.locks import LockLost

    fake = FakeCsub(states=["RUN", "RUN", "DONE"])
    beats = []

    def heartbeat(state):
        beats.append(state)
        if len(beats) == 2:
            raise LockLost("raw", "rec", "me")

    with pytest.raises(LockLost):
        await _jobs(tmp_path, fake).wait("777", str(tmp_path), heartbeat)
    assert fake.killed == ["777"] and beats == ["RUN", "RUN"]


async def test_wait_kills_even_when_the_lock_is_lost_on_the_terminal_poll(tmp_path):
    from mia_core.locks import LockLost

    fake = FakeCsub(states=["DONE"])

    def heartbeat(state):
        raise LockLost("raw", "rec", "me")

    with pytest.raises(LockLost):
        await _jobs(tmp_path, fake).wait("777", str(tmp_path), heartbeat)
    assert fake.killed == ["777"]  # harmless on a finished job; the wait still fails, so nothing is read from it


def _record(root, where):
    return json.loads(gzip.decompress((root / "_jobs" / f"{where}.json.gz").read_bytes()))


async def test_a_job_leaves_its_record_in_two_halves(tmp_path, monkeypatch):
    monkeypatch.setattr(jobrecord, "lsf_usage", lambda job_id: {"max_mem": "1 Gbytes"})
    root, ws = tmp_path / "work", tmp_path / "work" / "issue-1" / "r1" / "match-1"
    ws.mkdir(parents=True)
    (ws / "prompt.md").write_text("the prompt")
    (ws / "claude.jsonl").write_text('{"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}}')
    jobs = CsubJobs("dep", tmp_path / "ids", client_factory=lambda: FakeCsub(), work_root=root)
    spec = JobSpec(
        "j",
        "claude -p …",
        str(ws),
        10,
        env={"KEY": "v-secret"},
        prompt_file="prompt.md",
        transcript_file="claude.jsonl",
    )
    job_id = await jobs.submit("k1", spec)
    first = _record(root, "issue-1/r1/match-1")
    assert (first["service"], first["key"], first["job_id"]) == ("dep", "k1", "777")
    assert first["request"]["command"] == "claude -p …" and first["request"]["env"] == ["KEY"]
    assert first["submit"]["job_id"] == "777" and "outcome" not in first
    done = await jobs.wait(job_id, str(ws), lambda state: None)
    record = _record(root, "issue-1/r1/match-1")
    assert record["outcome"]["state"] == done.state == "DONE" and record["usage"] == {"max_mem": "1 Gbytes"}
    assert record["agent"]["prompt"] == "the prompt" and record["agent"]["turns"] == [
        {"role": "assistant", "text": "hi"}
    ]
    assert b"v-secret" not in gzip.decompress((root / "_jobs" / "issue-1/r1/match-1.json.gz").read_bytes())


async def test_a_job_found_by_its_workspace_records_the_request_alone(tmp_path):
    root, ws = tmp_path / "work", tmp_path / "work" / "ws"
    fake = FakeCsub(existing=[SimpleNamespace(job_id="556", cwd=str(ws))])
    jobs = CsubJobs("dep", tmp_path / "ids", client_factory=lambda: fake, work_root=root)
    assert await jobs.submit("k1", JobSpec("j", "true", str(ws), 10)) == "556"
    record = _record(root, "ws")
    assert record["job_id"] == "556" and record["submit"] is None and record["request"]["command"] == "true"


async def test_a_record_that_cannot_be_written_harms_neither_submit_nor_wait(tmp_path, monkeypatch):
    def broken(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(jobrecord, "write_atomic", broken)
    monkeypatch.setattr(jobrecord, "lsf_usage", lambda job_id: None)
    root, ws = tmp_path / "work", tmp_path / "work" / "ws"
    ws.mkdir(parents=True)
    jobs = CsubJobs("dep", tmp_path / "ids", client_factory=lambda: FakeCsub(), work_root=root)
    job_id = await jobs.submit("k1", JobSpec("j", "true", str(ws), 10))
    assert job_id == "777" and (await jobs.wait(job_id, str(ws), lambda state: None)).state == "DONE"
    assert not (root / "_jobs").exists()


@pytest.mark.parametrize("writable", ["work", "work/_jobs", "work/_jobs/issue-1"])
async def test_no_job_gets_the_records_writable(tmp_path, writable):
    root = tmp_path / "work"
    jobs = CsubJobs("dep", tmp_path / "ids", client_factory=lambda: pytest.fail("client used"), work_root=root)
    with pytest.raises(ValueError, match="holds the job records"):
        await jobs.submit("k1", JobSpec("j", "true", str(root / "ws"), 10, rw_mounts=[str(tmp_path / writable)]))
    with pytest.raises(ValueError, match="holds the job records"):
        await jobs.submit("k2", JobSpec("j", "true", str(tmp_path / writable), 10))
