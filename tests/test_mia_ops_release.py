import subprocess
from pathlib import Path

import pytest

from mia_ops.release import LayoutError, installation_root, release, release_to


def fake_run(calls, fail_on=None, origin="https://example.org/mia.git"):
    """Records every argv; `fail_on(argv)` makes that call fail like a subprocess would."""

    def run(argv, **kw):
        argv = [str(a) for a in argv]
        calls.append((argv, kw))
        if fail_on and fail_on(argv):
            raise subprocess.CalledProcessError(1, argv, stderr="fatal: Remote branch v9 not found\n")
        if argv[:2] == ["git", "-C"] and "get-url" in argv:
            return subprocess.CompletedProcess(argv, 0, stdout=origin + "\n", stderr="")
        if argv[:2] == ["git", "clone"]:
            Path(argv[-1]).mkdir(parents=True)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    return run


def layout(tmp_path, tag="v1"):
    root = tmp_path / "mia-prod"
    released(root, tag)
    (root / "mia.yaml").write_text("services:\n  autodev: {app: autodev}\n  mia-buffers: {app: mia-buffers}\n")
    (root / "current").symlink_to(Path("releases") / tag)
    return root


def released(root, tag):
    """A finished release: its checkout with the mia.yaml link the install writes last."""
    (root / "releases" / tag).mkdir(parents=True)
    (root / "releases" / tag / "mia.yaml").symlink_to(Path("../../mia.yaml"))


def test_installation_root_and_its_refusal(tmp_path):
    root = layout(tmp_path)
    assert installation_root(root / "current") == root
    assert installation_root(root / "releases/v1") == root
    with pytest.raises(LayoutError, match="is not <root>/releases/<tag>"):
        installation_root(tmp_path / "dev/mia-workflows")


def test_release_clones_installs_checks_flips_and_restarts(tmp_path):
    root, calls = layout(tmp_path), []
    assert release("v2", root / "current", run=fake_run(calls)) == 0
    argvs = [c for c, _ in calls]
    assert argvs[0][-2:] == ["get-url", "origin"]
    # the environment is installed where it stays: a pixi env hard-codes its own path
    assert argvs[1][:5] == ["git", "clone", "--branch", "v2", "--depth"] and argvs[1][-1] == str(root / "releases/v2")
    assert argvs[2] == ["pixi", "install", "--locked"] and calls[2][1]["cwd"] == root / "releases/v2"
    assert (root / "releases/v2/mia.yaml").readlink() == Path("../../mia.yaml")
    assert [a[-2:] for a in argvs[3:5]] == [["autodev", "check"], ["mia-buffers", "check"]]
    assert (
        argvs[3][0] == str(root / "releases/v2/.pixi/envs/default/bin/mia")
        and calls[3][1]["cwd"] == root / "releases/v2"
    )
    assert argvs[5] == [str(root / "current/.pixi/envs/default/bin/mia"), "restart"]
    assert (root / "current").readlink() == Path("releases/v2")


def test_release_of_an_existing_tag_only_flips_and_restarts(tmp_path):
    root, calls = layout(tmp_path), []
    released(root, "v0")
    assert release("v0", root / "current", run=fake_run(calls)) == 0
    assert [c[-1] for c, _ in calls] == ["check", "check", "restart"]  # no git, no pixi
    assert (root / "current").readlink() == Path("releases/v0")


def test_release_stops_before_the_flip_when_a_check_fails(tmp_path, capsys):
    root, calls = layout(tmp_path), []
    assert release("v2", root / "current", run=fake_run(calls, fail_on=lambda a: a[-1] == "check")) == 1
    assert (root / "current").readlink() == Path("releases/v1")
    assert not any(c[-1] == "restart" for c, _ in calls)


def test_release_of_a_tag_not_on_origin_leaves_nothing_behind(tmp_path, capsys):
    root, calls = layout(tmp_path), []
    assert release("v9", root / "current", run=fake_run(calls, fail_on=lambda a: a[:2] == ["git", "clone"])) == 1
    assert "Remote branch v9 not found" in capsys.readouterr().err
    assert sorted(p.name for p in (root / "releases").iterdir()) == ["v1"]
    assert (root / "current").readlink() == Path("releases/v1")


def test_release_to_pushes_the_tag_then_releases_over_ssh(tmp_path):
    calls = []
    assert release_to("v2", "prod.example.org:/home/svc/mia-prod", tmp_path, run=fake_run(calls)) == 0
    assert [c for c, _ in calls] == [
        ["git", "-C", str(tmp_path), "push", "origin", "v2"],
        ["ssh", "prod.example.org", "/home/svc/mia-prod/current/.pixi/envs/default/bin/mia release v2"],
    ]


def test_release_to_needs_host_and_root(tmp_path):
    with pytest.raises(LayoutError, match="HOST:ROOT"):
        release_to("v2", "prod.example.org", tmp_path, run=fake_run([]))


def test_release_rebuilds_a_release_whose_install_never_finished(tmp_path):
    root, calls = layout(tmp_path), []
    (root / "releases/v2").mkdir()  # cloned, but no mia.yaml link: the install did not finish
    assert release("v2", root / "current", run=fake_run(calls)) == 0
    assert [c[1] for c, _ in calls if c[0] == "git"] == ["-C", "clone"]
    assert (root / "releases/v2/mia.yaml").readlink() == Path("../../mia.yaml")


def test_release_without_pixi_on_path_is_one_line(tmp_path, capsys):
    root, calls = layout(tmp_path), []

    def run(argv, **kw):
        if str(argv[0]) == "pixi":
            raise FileNotFoundError(2, "No such file or directory", "pixi")
        return fake_run(calls)(argv, **kw)

    assert release("v2", root / "current", run=run) == 1
    assert "pixi" in capsys.readouterr().err
    assert not (root / "releases/v2").exists() and (root / "current").readlink() == Path("releases/v1")


@pytest.mark.parametrize("tag", ["release/2026-11", "-v2", "v 2", ""])
def test_release_refuses_a_tag_that_is_not_a_plain_name(tmp_path, tag):
    root = layout(tmp_path)
    with pytest.raises(LayoutError, match="tag"):
        release(tag, root / "current", run=fake_run([]))
    assert (root / "current").readlink() == Path("releases/v1")
