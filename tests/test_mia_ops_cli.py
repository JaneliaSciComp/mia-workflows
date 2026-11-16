import pytest
from test_mia_ops_services import fake, make_stack, make_stack_v2, tmux_calls  # noqa: F401  (fixture re-export)

from mia_ops import cli, services


@pytest.fixture
def stack(tmp_path, fake, monkeypatch):  # noqa: F811
    s = make_stack(tmp_path)
    monkeypatch.setattr(cli, "load_manifest", lambda _: s.manifest)
    return s


def new_sessions(fake):  # noqa: F811
    return [c[c.index("-s") + 1] for c in tmux_calls(fake, "new-session")]


def killed(fake):  # noqa: F811
    return [c[3].lstrip("=") for c in tmux_calls(fake, "kill-session")]


def test_help_lists_commands(capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(["--help"])
    assert e.value.code == 0
    out = capsys.readouterr().out
    for cmd in ("up", "down", "restart", "ps", "logs", "attach", "exec", "init-token", "pause", "resume"):
        assert cmd in out


def test_up_all_starts_temporal_first(stack, fake):  # noqa: F811
    assert cli.main(["up"]) == 0
    assert new_sessions(fake) == ["temporal", "autodev", "mia-data"]


def test_up_one_service(stack, fake):  # noqa: F811
    assert cli.main(["up", "mia-data"]) == 0
    assert new_sessions(fake) == ["temporal", "mia-data"]


def test_up_temporal_alone(stack, fake):  # noqa: F811
    assert cli.main(["up", "temporal"]) == 0
    assert new_sessions(fake) == ["temporal"]


def test_down_all_stops_temporal_last(stack, fake):  # noqa: F811
    cli.main(["up"])
    assert cli.main(["down"]) == 0
    assert killed(fake) == ["mia-data", "autodev", "temporal"]


def test_down_one_leaves_temporal(stack, fake):  # noqa: F811
    cli.main(["up"])
    assert cli.main(["down", "autodev"]) == 0
    assert killed(fake) == ["autodev"] and fake.running == {"temporal", "mia-data"}


def test_restart_one(stack, fake):  # noqa: F811
    cli.main(["up"])
    assert cli.main(["restart", "autodev"]) == 0
    assert killed(fake) == ["autodev"] and new_sessions(fake) == ["temporal", "autodev", "mia-data", "autodev"]


def test_unknown_service_exits(stack, fake):  # noqa: F811
    with pytest.raises(SystemExit, match="unknown service"):
        cli.main(["up", "nope"])


def test_ps(stack, fake, capsys):  # noqa: F811
    assert cli.main(["ps"]) == 1
    out = capsys.readouterr().out
    assert "temporal: stopped" in out and "autodev: stopped" in out
    cli.main(["up"])
    capsys.readouterr()
    assert cli.main(["ps"]) == 0
    out = capsys.readouterr().out
    assert "temporal: running" in out and "autodev: running" in out and "worker: alive" in out
    fake.windows = "  broker: alive\n  worker: dead\n"
    assert cli.main(["ps"]) == 1  # a service whose worker has exited is not running
    assert "autodev: BROKEN, its worker has exited (mia restart autodev)" in capsys.readouterr().out
    assert cli.main(["up", "autodev"]) == 1


def test_logs(stack, fake, tmp_path):  # noqa: F811
    log = tmp_path / "ad-state/logs/worker.log"
    log.parent.mkdir(parents=True)
    log.write_text("x\n")
    assert cli.main(["logs", "autodev", "-n", "3", "-f"]) == 0
    assert fake.calls[-1][0] == ["tail", "-n", "3", "-F", str(log)]
    with pytest.raises(SystemExit, match="no log"):
        cli.main(["logs", "autodev", "broker"])
    with pytest.raises(SystemExit):  # argparse: `temporal` is not a window name; its log is `mia logs temporal`
        cli.main(["logs", "autodev", "temporal"])
    tlog = tmp_path / "temporal/logs/temporal.log"
    tlog.parent.mkdir(parents=True)
    tlog.write_text("x\n")
    assert cli.main(["logs", "temporal"]) == 0 and fake.calls[-1][0][-1] == str(tlog)


def test_attach(stack, fake):  # noqa: F811
    with pytest.raises(SystemExit, match="not running"):
        cli.main(["attach", "autodev"])
    cli.main(["up", "autodev"])
    assert cli.main(["attach", "autodev"]) == 0
    assert fake.calls[-1][0] == ["tmux", "attach", "-t", "=autodev"]


def test_exec_runs_the_app_cli_with_its_config(stack, fake, tmp_path):  # noqa: F811
    assert cli.main(["exec", "mia-data", "reindex", "rec-1"]) == 0
    argv, kw = fake.calls[-1]
    assert argv == [str(services.PY), "-m", "mia_data.cli", "reindex", "rec-1"]
    assert kw["env"]["MIA_DATA_CONFIG"] == f"{tmp_path}/mia_data.yaml"


def test_exec_passes_the_service_name(tmp_path, fake, monkeypatch):  # noqa: F811
    s = make_stack_v2(tmp_path)
    monkeypatch.setattr(cli, "load_manifest", lambda _: s.manifest)
    assert cli.main(["exec", "mia-buffers", "plan", "--help"]) == 0
    argv, kw = fake.calls[-1]
    assert argv[1:3] == ["-m", "mia_buffers.cli"] and kw["env"]["MIA_SERVICE"] == "mia-buffers"


def test_exec_uses_the_services_broker_when_it_is_running(tmp_path, fake, monkeypatch):  # noqa: F811
    s = make_stack_v2(tmp_path)
    monkeypatch.setattr(cli, "load_manifest", lambda _: s.manifest)
    monkeypatch.delenv("CSUB_TRANSPORT", raising=False)
    monkeypatch.delenv("CSUB_SOCKET", raising=False)

    def exec_env() -> dict:
        assert cli.main(["exec", "mia-data", "status"]) == 0
        return fake.calls[-1][1]["env"]

    env = exec_env()  # not running, no socket: the operator's own csub client config
    assert "CSUB_TRANSPORT" not in env and "CSUB_SOCKET" not in env
    sock = tmp_path / "scratch/me/mia-data/csub.sock"
    sock.parent.mkdir(parents=True)
    sock.touch()
    env = exec_env()  # the socket a killed broker left behind
    assert "CSUB_TRANSPORT" not in env and "CSUB_SOCKET" not in env
    fake.running.add("mia-data")
    env = exec_env()
    assert env["CSUB_TRANSPORT"] == "unix" and env["CSUB_SOCKET"] == str(sock)


def test_init_token_writes_once_with_mode_600(tmp_path, fake, monkeypatch, capsys):  # noqa: F811
    s = make_stack_v2(tmp_path)
    (tmp_path / "token").unlink()
    monkeypatch.setattr(cli, "load_manifest", lambda _: s.manifest)
    assert cli.main(["init-token"]) == 0
    assert oct((tmp_path / "token").stat().st_mode & 0o777) == "0o600" and len((tmp_path / "token").read_text()) == 64
    assert cli.main(["init-token"]) == 0 and "exists" in capsys.readouterr().err


def test_pause_and_resume_edit_the_manifest(stack, tmp_path, monkeypatch):  # noqa: F811
    manifest = tmp_path / "mia.yaml"
    manifest.write_text("services: {}\n")
    monkeypatch.setenv("MIA_CONFIG", str(manifest))
    assert cli.main(["pause"]) == 0 and "pause: true" in manifest.read_text()
    assert cli.main(["resume"]) == 0 and "pause: false" in manifest.read_text()


def test_judge_eval_cli(stack, tmp_path, monkeypatch, capsys):  # noqa: F811
    from mia_core.judge.eval import EvalRow
    from mia_ops import cli as ops_cli

    manifest = tmp_path / "mia.yaml"
    manifest.write_text("judge: {model: opus, min_confidence: 0.5}\nservices: {}\n")
    monkeypatch.setenv("MIA_CONFIG", str(manifest))
    seen = {}

    def evaluate(eval_dir, cfg, *, limit=None, run=None, max_suspect=None):
        seen.update(
            dir=eval_dir, model=cfg.model, min_confidence=cfg.min_confidence, limit=limit, max_suspect=max_suspect
        )
        return [
            EvalRow("a", "confocal/none", "approve", "approve", 0.9, 0.2),
            EvalRow("b", "confocal/none", "reject", "approve", 0.8, None),
        ]

    monkeypatch.setattr(ops_cli.judge_eval, "evaluate", evaluate)
    assert ops_cli.main(["judge", "eval", "crops", str(tmp_path / "set"), "--limit", "2", "--model", "haiku"]) == 0
    assert seen == {"dir": tmp_path / "set", "model": "haiku", "min_confidence": 0.5, "limit": 2, "max_suspect": 2}
    out = capsys.readouterr().out
    assert "a\tconfocal/none\texpected approve\tgot approve\t0.9\t$0.20" in out and "\tgot approve\t0.8\t-" in out
    assert "| confocal/none | 2 | 1 | 1 | 0 | 0 |" in out and "| all | 2 | 1 | 1 | 0 | 0 |" in out
