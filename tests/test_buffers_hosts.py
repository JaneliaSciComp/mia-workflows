import json
import re
import subprocess
import sys

import pytest
import yaml

from mia_buffers.hosts import HostCheck
from mia_core.hosts import generate_policy
from mia_data.config import NetworkCfg
from tests.test_buffers_config import make_cfg


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))


def check(tmp_path) -> HostCheck:
    base = tmp_path / "base" / "broker.toml"
    base.parent.mkdir()
    base.write_text('[broker]\nallowed_hosts = ["zenodo.org", "*.s3.amazonaws.com"]\n')
    policy_dir = tmp_path / ".mia" / "policy"
    generate_policy(base, policy_dir)
    return HostCheck(
        base_policy=base,
        policy_dir=policy_dir,
        network=NetworkCfg(package_hosts=["pypi.org"], redirect_hosts={"data.example.org": ["cdn.example.net"]}),
        storage_root=tmp_path / "storage",
        rounds_dir=tmp_path / "rounds",
        own_hosts=("buffer-api.internal.example", "temporal.internal.example"),
    )


def record(url: str) -> dict:
    return {"data": {"download_url": url}}


def test_a_records_needs_are_its_url_hosts_their_redirect_targets_and_the_package_hosts(tmp_path):
    needed = check(tmp_path).for_records({"rec-a": record("https://data.example.org/f.zip")})
    assert set(needed) == {"data.example.org", "cdn.example.net", "pypi.org"}
    assert needed["pypi.org"] == ["rec-a"]


def test_an_s3_url_needs_its_bucket_host(tmp_path):
    hosts = check(tmp_path)
    needed = hosts.for_records({"rec-a": record("s3://bucket/key")})
    assert "bucket.s3.amazonaws.com" in needed
    assert "bucket.s3.amazonaws.com" not in hosts.missing(needed)


def test_missing_groups_the_records_by_host(tmp_path):
    hosts = check(tmp_path)
    records = {"rec-b": record("https://h.example.org/b"), "rec-a": record("https://h.example.org/a")}
    assert hosts.missing(hosts.for_records(records))["h.example.org"] == ["rec-a", "rec-b"]


def test_a_record_naming_a_github_url_contributes_nothing(tmp_path):
    records = {"rec-a": record("https://github.com/o/r/archive.zip"), "rec-b": record("https://h.example.org/b")}
    needed = check(tmp_path).for_records(records)
    assert needed["h.example.org"] == ["rec-b"]
    assert "rec-a" not in {rec for recs in needed.values() for rec in recs}


def test_a_bindings_needs_skip_manual_bindings_unreadable_bindings_and_folders_outside_the_root(tmp_path):
    hosts = check(tmp_path)

    def write(folder: str, urls: list[str]) -> None:
        (hosts.storage_root / folder).mkdir(parents=True, exist_ok=True)
        (hosts.storage_root / folder / "binding.yaml").write_text(yaml.safe_dump({"raw": [{"urls": urls}]}))

    write("rec-a", ["https://data.example.org/a.tif"])
    write("rec-m", ["https://m.example.org/a.tif", "manual://drop/a"])
    write("../outside", ["https://out.example.org/a.tif"])
    (hosts.storage_root / "rec-b").mkdir()
    (hosts.storage_root / "rec-b" / "binding.yaml").write_text(": : not yaml [")
    folders = {name: name for name in ("rec-a", "rec-m", "rec-b", "rec-c", "../outside")}
    assert hosts.for_bindings(folders) == {
        "cdn.example.net": ["rec-a"],
        "data.example.org": ["rec-a"],
        "pypi.org": ["rec-a"],
    }


NEVER_APPROVED = ["https://buffer-api.internal.example:8443/a", "http://10.1.2.3/a", "https://127.0.0.1/a"]


def test_an_ip_literal_and_the_deployments_own_hosts_are_left_out_and_the_records_other_hosts_kept(tmp_path):
    hosts = check(tmp_path)
    urls = [*NEVER_APPROVED, "http://temporal.internal.example:7233/a", "https://data.example.org/a"]
    expected = {"cdn.example.net": ["rec-a"], "data.example.org": ["rec-a"], "pypi.org": ["rec-a"]}
    assert hosts.for_records({"rec-a": {"data": {"download_url": urls}}}) == expected
    (hosts.storage_root / "rec-a").mkdir(parents=True)
    (hosts.storage_root / "rec-a" / "binding.yaml").write_text(yaml.safe_dump({"raw": [{"urls": urls}]}))
    assert hosts.for_bindings({"rec-a": "rec-a"}) == expected


@pytest.mark.parametrize("host", ["buffer-api.internal.example", "temporal.internal.example", "10.1.2.3", "::1"])
def test_check_refuses_an_ip_literal_and_the_deployments_own_hosts_though_needed(tmp_path, host):
    with pytest.raises(ValueError, match=re.escape(host)):
        check(tmp_path).check([host], {host: ["rec-a"]})


def test_from_config_names_the_deployments_own_hosts(tmp_path):
    cfg = make_cfg(
        tmp_path,
        service={"url": "https://Buffer-API.internal.example:8443"},
        temporal={"address": "temporal.internal.example:7233", "ui_url": "https://temporal-ui.internal.example/"},
    )
    assert HostCheck.from_config(cfg).own_hosts == (
        "buffer-api.internal.example",
        "temporal-ui.internal.example",
        "temporal.internal.example",
    )
    assert HostCheck.from_config(make_cfg(tmp_path)).own_hosts == ("127.0.0.1", "localhost")


def test_the_pool_is_the_plans_in_pool_rows(tmp_path):
    hosts = check(tmp_path)
    rows = [
        {"id": "in", "admitted": True, "fit": {"ok": True}},
        {"id": "unfit", "admitted": True, "fit": {"ok": False}},
        {"id": "out", "admitted": False, "fit": {"ok": True}},
        {"id": "unread", "admitted": True, "fit": None},
    ]
    (hosts.rounds_dir / "i1-e2").mkdir(parents=True)
    (hosts.rounds_dir / "i1-e2" / "plan.json").write_text(json.dumps({"rows": rows}))
    assert hosts.pool("i1-e2") == ["in"]


@pytest.mark.parametrize("plan", [None, "[]", '{"nope": 1}', '{"rows": [1]}', "not json"])
def test_a_plan_that_cannot_be_read_raises_value_error(tmp_path, plan):
    hosts = check(tmp_path)
    (hosts.rounds_dir / "i1-e2").mkdir(parents=True)
    if plan is not None:
        (hosts.rounds_dir / "i1-e2" / "plan.json").write_text(plan)
    with pytest.raises(ValueError, match="i1-e2"):
        hosts.pool("i1-e2")
    with pytest.raises(ValueError):
        hosts.pool("../x")


def test_importing_hosts_does_not_import_the_activities():
    code = "import sys, mia_buffers.hosts; assert 'mia_buffers.activities' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)


@pytest.mark.parametrize(
    "host",
    ["Data.4TU.nl", "data.4tu.nl.", "*.4tu.nl", "data.4tu.nl:443", "github.com", "other.example.org"],
)
def test_check_refuses_a_host_that_is_not_exact_not_needed_or_github(tmp_path, host):
    with pytest.raises(ValueError, match=host.replace("*", r"\*")):
        check(tmp_path).check([host], {"data.4tu.nl": ["rec-a"], "github.com": ["rec-a"]})


def test_check_accepts_a_needed_host(tmp_path):
    check(tmp_path).check(["data.4tu.nl"], {"data.4tu.nl": ["rec-a"]})


def test_approve_adds_the_host_to_the_generated_policy(tmp_path):
    hosts = check(tmp_path)
    needed = {"h.example.org": ["rec-a"]}
    assert hosts.missing(needed) == needed
    assert hosts.approve(["h.example.org"], by="op", kind="data", number=3) == ["h.example.org"]
    assert hosts.missing(needed) == {}
