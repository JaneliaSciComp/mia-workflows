import copy
from pathlib import Path

import pytest
import yaml

from mia_core.config import MANIFEST_ONLY, csub_config, load_service_config, service_config

SHARED = {
    "sandbox_dir": "../agentic-sandbox/scripts",
    "temporal": {"managed": True, "state_dir": "~/.local/state/temporal", "address": "h:7233", "poll_minutes": 3},
    "github": {"auth": "token", "catalog_repo": "o/c", "snippets_repo": "o/s"},
    "paths": {"state_root": "~/.local/state/mia", "work_root": "/w", "storage_root": "/s"},
    "limits": {"job_bytes": 5e9},
    "services": {
        "mia-data": {"app": "mia-data", "github": {"data_repo": "o/d"}, "agent": {"model": "sonnet"}},
        "snippets": {"app": "autodev", "github": {"repo": "o/snips"}},
    },
}


def test_merge_overlays_a_service_section_one_level_deep():
    cfg = service_config(SHARED, "mia-data")
    assert cfg["github"] == {"auth": "token", "catalog_repo": "o/c", "snippets_repo": "o/s", "data_repo": "o/d"}
    assert cfg["agent"] == {"model": "sonnet"} and cfg["limits"] == {"job_bytes": 5e9}
    assert cfg["deployment"] == "mia-data"
    assert not MANIFEST_ONLY & cfg.keys() and "managed" not in cfg["temporal"] and "state_dir" not in cfg["temporal"]
    assert cfg["temporal"]["address"] == "h:7233"


def test_state_dir_comes_from_state_root_and_the_service_name():
    cfg = service_config(SHARED, "snippets")
    assert cfg["paths"]["state_dir"] == "~/.local/state/mia/snippets" and "state_root" not in cfg["paths"]
    explicit = {**SHARED, "paths": {**SHARED["paths"], "state_dir": "/x"}}
    assert service_config(explicit, "snippets")["paths"]["state_dir"] == "/x"


def test_a_service_does_not_see_another_services_sections():
    assert "agent" not in service_config(SHARED, "snippets")


def test_a_buffers_service_learns_where_the_datasets_pipeline_runs():
    manifest = copy.deepcopy(SHARED)
    manifest["services"]["mia-data"]["temporal"] = {"namespace": "data-ns"}
    manifest["services"]["buffers"] = {"app": "mia-buffers", "github": {"buffers_repo": "o/b"}}
    cfg = service_config(manifest, "buffers")
    assert cfg["datasets_pipeline"] == {"namespace": "data-ns", "task_queue": "mia-data"}  # the queue is the name
    assert "datasets_pipeline" not in service_config(manifest, "mia-data")
    del manifest["services"]["mia-data"]
    assert "datasets_pipeline" not in service_config(manifest, "buffers")  # no such service: the model's default


def test_service_block_keys_are_sections():
    bad = {**SHARED, "services": {"x": {"app": "autodev", "deployment": "y"}}}
    with pytest.raises(ValueError, match=r"services\.x\.deployment"):
        service_config(bad, "x")


@pytest.mark.parametrize("services, message", [({}, "no service 'x'"), ({"x": {"github": {}}}, "needs app")])
def test_missing_service_or_app(services, message):
    with pytest.raises(ValueError, match=message):
        service_config({**SHARED, "services": services}, "x")


@pytest.mark.parametrize(
    "override, message",
    [
        ({"github": "foo"}, "github: the shared section must be a mapping"),
        ({"paths": {**SHARED["paths"], "state_root": None}}, "paths.state_root must be a path string"),
    ],
)
def test_malformed_shared_sections_raise_value_error(override, message):
    with pytest.raises(ValueError, match=message):
        service_config({**SHARED, **override}, "mia-data")


def test_the_input_is_not_mutated():
    before = copy.deepcopy(SHARED)
    service_config(SHARED, "mia-data")
    assert SHARED == before


def test_load_service_config_reads_mia_config_env(tmp_path, monkeypatch):
    (tmp_path / "mia.yaml").write_text(yaml.safe_dump(SHARED))
    monkeypatch.setenv("MIA_CONFIG", str(tmp_path / "mia.yaml"))
    assert load_service_config("mia-data")["deployment"] == "mia-data"
    with pytest.raises(SystemExit, match="no service 'nope'"):
        load_service_config("nope")
    with pytest.raises(SystemExit, match="not found"):
        load_service_config("mia-data", tmp_path / "missing.yaml")


@pytest.mark.parametrize(
    "service, module, attr, expected",
    [
        ("autodev", "autodev.config", "github.repo", "JaneliaSciComp/mia-workflows"),
        ("snippets", "autodev.config", "github.repo", "JaneliaSciComp/mia-snippets"),
        ("mia-data", "mia_data.config", "github.data_repo", "JaneliaSciComp/mia-workflow-data"),
        ("mia-buffers", "mia_buffers.config", "github.buffers_repo", "JaneliaSciComp/mia-buffers"),
    ],
)
def test_example_mia_yaml_loads_every_service(service, module, attr, expected, monkeypatch):
    import importlib

    from mia_core.config import PROJECT_DIR

    for name in ("TEMPORAL_ADDRESS", "AUTODEV_DEPLOYMENT", "MIA_SERVICE"):
        monkeypatch.delenv(name, raising=False)
    cfg = importlib.import_module(module).load_config(PROJECT_DIR / "mia.yaml.example", service=service)
    section, field = attr.split(".")
    assert getattr(getattr(cfg, section), field) == expected
    assert cfg.deployment == service and cfg.paths.state_dir.name == service
    labels = {"autodev": "autodev:go", "snippets": "snippets:go", "mia-data": "data:go", "mia-buffers": "buffer:go"}
    assert cfg.label("go") == labels[service]


def test_service_token_file_is_absolute(tmp_path):
    from tests.mia_helpers import make_cfg

    cfg = make_cfg(tmp_path, service={"url": "http://svc.example.org:8765", "token_file": "~/x"})
    assert cfg.service.token_file.is_absolute()
    # every application reads the token through the shared model: `~` is expanded for all of them (autodev too)
    from mia_core.config import ServiceCfg

    shared = ServiceCfg(url="http://svc.example.org:8765", token_file="~/.config/mia/service-token")
    assert shared.token_file == Path.home() / ".config/mia/service-token"


def test_service_url_needs_an_explicit_port():
    from pydantic import ValidationError

    from mia_core.config import ServiceCfg

    assert ServiceCfg(url="http://svc.example.org:8765").url == "http://svc.example.org:8765"
    with pytest.raises(ValidationError, match="service.url needs an explicit port"):
        ServiceCfg(url="http://svc.example.org")


def test_autonomy_defaults_and_caps():
    from mia_core.config import AutonomyCfg

    cfg = AutonomyCfg()
    assert (cfg.pause, cfg.quarantine_days) == (False, 30)
    assert cfg.caps() == {
        "max_usd": 100,
        "max_fetch_bytes": 500e9,
        "max_jobs": 200,
        "max_concurrent_jobs": 20,
        "max_days": 14,
    }


def test_pause_is_a_text_edit_that_keeps_comments(tmp_path):
    from mia_core.autonomy import paused, set_pause

    manifest = tmp_path / "mia.yaml"
    manifest.write_text("temporal:\n  address: localhost:7233   # keep me\nservices: {}\n")
    assert paused(manifest) is False
    set_pause(manifest, True)
    assert paused(manifest) is True and "# keep me" in manifest.read_text()
    assert manifest.read_text().endswith("autonomy:\n  pause: true\n")
    set_pause(manifest, False)
    assert paused(manifest) is False and manifest.read_text().count("autonomy:") == 1
    manifest.write_text(
        "autonomy:                 # the switches\n  quarantine_days: 30\n  pause: false  # mia pause flips this\n"
        "services: {}\n"
    )
    set_pause(manifest, True)
    text = manifest.read_text()
    assert paused(manifest) is True and "# the switches" in text and "# mia pause flips this" in text
    assert "quarantine_days: 30" in text and text.count("pause:") == 1
    assert paused(tmp_path / "missing.yaml") is False


def test_set_pause_survives_blank_lines_comments_and_a_nested_pause(tmp_path):
    from mia_core.autonomy import paused, set_pause

    manifest = tmp_path / "mia.yaml"
    manifest.write_text("autonomy:\n  quarantine_days: 30\n\n  pause: false\nservices: {}\n")
    set_pause(manifest, True)
    assert paused(manifest) is True and manifest.read_text().count("pause:") == 1
    manifest.write_text("autonomy:\n  root:\n    max_usd: 100\n# operator switch\n  pause: false\nservices: {}\n")
    set_pause(manifest, True)
    assert (
        paused(manifest) is True
        and manifest.read_text().count("pause:") == 1
        and "# operator switch" in manifest.read_text()
    )
    manifest.write_text("autonomy:\n  root:\n    pause: false\nservices: {}\n")  # a nested key is not the switch
    set_pause(manifest, True)
    text = manifest.read_text()
    assert paused(manifest) is True and "    pause: false" in text and "  pause: true" in text


def test_judge_cfg_defaults_and_enabled_gates():
    from mia_core.config import JudgeCfg

    cfg = JudgeCfg()
    assert cfg.model == "opus" and cfg.max_budget_usd == 2.0 and cfg.min_confidence == 0.6
    assert cfg.crops.mode == "off" and cfg.review.mode == "off" and cfg.crops.max_suspect == 2
    assert cfg.enabled_gates() == frozenset()
    assert JudgeCfg(crops={"mode": "shadow"}).on_gates() == frozenset({"crops"})
    assert JudgeCfg(crops={"mode": "shadow"}).enabled_gates() == frozenset()  # shadow authorizes nothing
    with pytest.raises(ValueError, match="milestone N"):  # nothing grants on a judge yet: active would lie
        JudgeCfg(crops={"mode": "active"})


def test_gate_mode_takes_yamls_bare_off():
    from mia_core.config import GateCfg

    assert GateCfg(mode=False).mode == "off"  # `mode: off` in YAML is False


def test_csub_policy_dir_defaults_beside_the_state_root(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    with_root = csub_config({"paths": {"state_root": "/x/state"}})
    assert with_root.policy_dir == Path("/x/state/csub")
    assert with_root.base_policy == tmp_path / ".config/csub/broker.toml"
    assert csub_config({}).policy_dir == tmp_path / ".local/state/csub"
    explicit = csub_config({"paths": {"state_root": "/x/state"}, "csub": {"policy_dir": "~/p", "base_policy": "~/b"}})
    assert explicit.policy_dir == tmp_path / "p" and explicit.base_policy == tmp_path / "b"


def test_every_service_receives_the_csub_block():
    manifest = {**SHARED, "csub": {"policy_dir": "/h/p"}}
    assert service_config(manifest, "mia-data")["csub"] == service_config(manifest, "snippets")["csub"]
    assert service_config(manifest, "snippets")["csub"]["policy_dir"] == "/h/p"
