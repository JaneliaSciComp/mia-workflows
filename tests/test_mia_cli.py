import json
from types import SimpleNamespace

from mia_helpers import make_cfg

from mia_data import cli
from mia_data.cli import DATA_HOSTS, main


def _run(monkeypatch, tmp_path, argv, **cfg_over):
    cfg = make_cfg(tmp_path, **cfg_over)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    return cfg, main(argv)


def _storage(cfg, withdrawn=None):
    for ds, crops in {"ds1": [("a", "tiff-zip"), ("b", "other")], "ds2": [("c", "tiff-zip")]}.items():
        for cid, snip in crops:
            d = cfg.paths.storage_root / ds / cid
            d.mkdir(parents=True)
            (d / "crop.json").write_text(json.dumps({"crop_id": cid, "snippets": [{"name": snip, "commit": "1"}]}))
        (cfg.paths.storage_root / ds / "manifest.json").write_text(
            json.dumps({"crops": [{"crop_id": c, "snippets": [{"name": n}], "withdrawn": False} for c, n in crops]})
        )


def test_crops_by_snippet(monkeypatch, tmp_path, capsys):
    cfg = make_cfg(tmp_path)
    _storage(cfg)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["crops-by-snippet", "tiff-zip"]) == 0
    assert capsys.readouterr().out.split("\n")[:-1] == ["ds1 a false", "ds2 c false"]
    assert main(["crops-by-snippet", "nope"]) == 0 and capsys.readouterr().out == ""


def test_reindex_marks_withdrawn(monkeypatch, tmp_path, capsys):
    cfg = make_cfg(tmp_path)
    _storage(cfg)
    (cfg.harness.dir / "WITHDRAWN.yaml").write_text("- name: tiff-zip\n  reason: bad\n")
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["reindex", "ds2"]) == 0
    assert capsys.readouterr().out == "ds2: 1 crop(s)\n"
    assert json.loads((cfg.paths.storage_root / "ds2/manifest.json").read_text())["crops"][0]["withdrawn"] is True
    assert json.loads((cfg.paths.storage_root / "ds1/manifest.json").read_text())["crops"][0]["withdrawn"] is False
    assert main(["reindex"]) == 0
    assert capsys.readouterr().out == "ds1: 2 crop(s)\nds2: 1 crop(s)\n"


class _GH:
    def __init__(self, repo, *a, **k):
        self.repo = repo

    async def request(self, method, path):
        if self.repo == "o/snippets":
            raise RuntimeError("404")
        return SimpleNamespace(json=lambda: {"full_name": self.repo})

    async def aclose(self):
        pass


def _fake_env(monkeypatch, hosts, env_allow=("MIA_HARNESS_*",), roots=("/",), queues=None):
    async def client(cfg):
        return SimpleNamespace(workflow_service=SimpleNamespace(describe_namespace=_aw))

    async def _aw(req):
        return None

    async def login(cfg):
        return "bot"

    probe = SimpleNamespace(
        agents=["claude"],
        allowed_hosts=hosts,
        user="u",
        env_allow=list(env_allow),
        allowed_roots=list(roots),
        queues=queues or {"short": {}, "local": {}},
    )
    monkeypatch.setattr(cli, "_lsf_queues", lambda: {"short", "local", "gpu_a100"})
    monkeypatch.setattr(cli, "GitHub", _GH)
    monkeypatch.setattr(cli, "_client", client)
    monkeypatch.setattr(cli, "bot_login", login)
    monkeypatch.setattr("autodev.jobs.csub_client", lambda s: SimpleNamespace(probe=lambda: probe))
    monkeypatch.setattr("mia_data.jobspec.harness_pin", lambda cfg: ("c" * 40, "d" * 64))


def _harness_bin(cfg, mode=0o755):
    b = cfg.harness.dir / "harness" / ".pixi" / "envs" / "default" / "bin" / "python"
    b.parent.mkdir(parents=True)
    b.write_text("#!/bin/sh\n")
    b.chmod(mode)


def test_check_reports_every_row_and_fails_on_one(monkeypatch, tmp_path, capsys):
    _fake_env(monkeypatch, ["*"])
    for d in ("state", "work", "storage", "local"):
        (tmp_path / d).mkdir()
    cfg = make_cfg(tmp_path)
    _harness_bin(cfg)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["check"]) == 1
    out = capsys.readouterr().out.split("\n")
    assert [line[:3] for line in out[:-1]] == ["ok ", "BAD", "ok ", "ok ", "ok ", "ok ", "ok ", "ok "]
    assert "github snippets" in out[1] and "RuntimeError" in out[1]


def test_check_flags_unallowed_data_host(monkeypatch, tmp_path, capsys):
    _fake_env(
        monkeypatch,
        [h for h in DATA_HOSTS if h != "zenodo.org"]
        + ["pypi.org", "files.pythonhosted.org", "conda.anaconda.org", "repo.prefix.dev"],
    )
    monkeypatch.setattr(_GH, "request", lambda self, m, p: _ok(self))
    for d in ("state", "work", "storage"):
        (tmp_path / d).mkdir()
    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["check"]) == 1
    assert "host not allowed: zenodo.org" in capsys.readouterr().out


async def _ok(gh):
    return SimpleNamespace(json=lambda: {"full_name": gh.repo})


def test_check_wants_an_installed_harness_env(monkeypatch, tmp_path, capsys):
    _fake_env(monkeypatch, ["*"])
    cfg = make_cfg(tmp_path)
    _harness_bin(cfg, 0o644)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["check"]) == 1
    pin = next(line for line in capsys.readouterr().out.split("\n") if "harness pin" in line)
    assert pin.startswith("BAD") and "cd harness && pixi install" in pin


def test_check_flags_csub_env_roots_and_home(monkeypatch, tmp_path, capsys):
    _fake_env(monkeypatch, ["*"], env_allow=["MIA_HARNESS_COMMIT"], roots=[str(tmp_path / "work")])
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["check"]) == 1
    row = next(line for line in capsys.readouterr().out.split("\n") if "csub broker" in line)
    assert row.startswith("BAD") and "MIA_HARNESS_JOB_BYTES" in row and "MIA_HARNESS_COMMIT," not in row
    assert 'add "MIA_HARNESS_*" to env_allow' in row
    assert f'add "{cfg.harness.dir}", "{cfg.paths.storage_root}" to allowed_roots' in row
    assert f"home directory {tmp_path.resolve()}" in row and str(cfg.harness.dir) in row.split("home directory")[1]


def test_check_flags_policy_queue_lsf_does_not_have(monkeypatch, tmp_path, capsys):
    _fake_env(
        monkeypatch, ["*"], queues={"short": {}, "long": {}, "gpu": {"gpu": True}}
    )  # gpu tables are not LSF names
    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["check"]) == 1
    row = next(line for line in capsys.readouterr().out.split("\n") if "csub broker" in line)
    assert (
        row.startswith("BAD")
        and "not an LSF queue: long" in row
        and "gpu" not in row.split("not an LSF queue:")[1].split("(")[0]
    )


def test_help_lists_app_commands(capsys):
    import pytest

    with pytest.raises(SystemExit) as e:
        main(["--help"])
    assert e.value.code == 0
    out = capsys.readouterr().out
    for cmd in ("check", "worker", "test-pr", "schedule"):
        assert cmd in out
    assert "seed" not in out  # an issue appears when its dataset has something to say: the mirror opens it
    assert "restart" not in out and "attach" not in out


def test_check_adds_local_root_and_token_rows_when_configured(monkeypatch, tmp_path, capsys):
    _fake_env(monkeypatch, ["*"])
    for d in ("state", "work", "storage"):
        (tmp_path / d).mkdir()
    (tmp_path / "tok").write_text("t")
    (tmp_path / "tok").chmod(0o600)
    cfg = make_cfg(
        tmp_path,
        paths={"local_root": str(tmp_path / "local")},
        service={"url": "http://svc:1", "token_file": str(tmp_path / "tok")},
    )
    _harness_bin(cfg)
    monkeypatch.setattr(cli, "load_config", lambda path=None, service=None: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    monkeypatch.setattr(cli, "require_local_dir", lambda p: p.mkdir(parents=True, exist_ok=True))
    main(["check"])
    out = capsys.readouterr().out.rstrip("\n").split("\n")
    assert len(out) == 9 and out[7].startswith("ok   local root") and out[8].startswith("ok   service token")


def test_collect_judge_set(monkeypatch, tmp_path, capsys):
    import hashlib

    import yaml

    from mia_core.judge import policy

    cfg = make_cfg(tmp_path, service={"url": "http://svc:8765", "token_file": None})
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    sha = "a" * 40
    round_ws = cfg.paths.work_root / "issue-7" / "i7-e11"
    crop_ws, verify_ws = round_ws / f"crop-{sha}-1", round_ws / f"verify-{sha}-1"
    raw = b"rawpng"
    (crop_ws / "crops" / "c1").mkdir(parents=True)
    (crop_ws / "crops" / "c1" / "raw.png").write_bytes(raw)
    (verify_ws / "preview" / "c1").mkdir(parents=True)
    (verify_ws / "preview" / "c1" / "overlay.png").write_bytes(b"ov")
    (verify_ws / "binding.yaml").write_text(
        yaml.safe_dump({"record": "rec-1", "labels": [{"segmentation_type": "instance"}], "reasons": ["guessed"]})
    )
    (verify_ws / "verify.json").write_text(
        json.dumps({"crops": [{"crop_id": "c1", "ok": True, "files": {"raw.png": hashlib.sha256(raw).hexdigest()}}]})
    )
    (cfg.paths.state_dir / "rounds" / "i7-e11").mkdir(parents=True)
    (cfg.paths.state_dir / "rounds" / "i7-e11" / "record.yaml").write_text(
        yaml.safe_dump({"imaging": {"modality": ["fluorescence"]}})
    )
    rows = [
        policy.new_assessment(
            gate="crops",
            subject_kind="crop_set",
            subject_ref=f"i7-e11:i7-e11-{sha[:12]}",
            verdict="reject",
            by="human:a",
            at=10.0,
        ),
        policy.new_assessment(
            gate="crops",
            subject_kind="crop_set",
            subject_ref=f"i7-e11:i7-e11-{sha[:12]}",
            verdict="approve",
            by="human:b",
            at=20.0,
        ),
        policy.new_assessment(
            gate="crops",
            subject_kind="crop_set",
            subject_ref=f"i7-e11:i7-e11-{sha[:12]}",
            verdict="approve",
            by="judge",
            at=21.0,
        ),
        policy.new_assessment(
            gate="crops",
            subject_kind="crop_set",
            subject_ref="i8-e12:i8-e12-bbbbbbbbbbbb",
            verdict="approve",
            by="human:a",
            at=30.0,
        ),
    ]

    class FakeRegistry:
        def assessments(self, **kw):
            return sorted(rows, key=lambda r: r["id"], reverse=True)

    monkeypatch.setattr("mia_core.registry.Registry.from_config", classmethod(lambda cls, s: FakeRegistry()))
    assert main(["collect-judge-set", str(tmp_path / "set")]) == 0
    out = capsys.readouterr().out
    item = tmp_path / "set" / f"i7-e11_i7-e11-{sha[:12]}"
    expected = json.loads((item / "expected.json").read_text())
    assert expected["verdict"] == "approve" and expected["by"] == "human:b"  # the newest human verdict
    assert expected["category"] == "fluorescence/instance"
    assert (item / "evidence" / "c1" / "raw.png").read_bytes() == raw and (
        item / "evidence" / "c1" / "overlay.png"
    ).exists()
    assert "reasons" not in yaml.safe_load((item / "evidence" / "binding.yaml").read_text())
    assert "skipped i8-e12:i8-e12-bbbbbbbbbbbb" in out and "1 items in" in out
