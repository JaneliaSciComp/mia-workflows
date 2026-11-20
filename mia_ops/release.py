"""`mia release <tag>`: put a tagged checkout live in an installation laid out as `<root>/releases/<tag>` with
`<root>/current -> releases/<tag>` and `<root>/mia.yaml`. Five steps: clone the tag from `origin` and `pixi install`
it (when `releases/<tag>` is not there yet), link `mia.yaml` into it, run every service's `check` from it, flip
`current`, and `mia restart` from the new `current`. Rolling back is the same command with an earlier tag."""

from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

from mia_core.config import PROJECT_DIR, read_manifest

MIA_BIN = Path(".pixi/envs/default/bin/mia")  # relative to a checkout

Runner = Callable[..., subprocess.CompletedProcess]


class LayoutError(Exception):
    """The checkout is not inside an installation of the releases/current layout, or the target is malformed."""


def installation_root(checkout: Path) -> Path:
    """The `<root>` of the installation `checkout` belongs to: its resolved path is `<root>/releases/<tag>`."""
    resolved = checkout.resolve()
    if resolved.parent.name == "releases":
        return resolved.parent.parent
    raise LayoutError(
        f"{checkout} is not <root>/releases/<tag>: mia release runs from an installation laid out as "
        "<root>/releases/<tag> with <root>/current -> releases/<tag> and <root>/mia.yaml"
    )


def _fetch(tag: str, checkout: Path, target: Path, run: Runner) -> None:
    """Clone `tag` from the checkout's origin and install it, under a hidden name until both succeeded."""
    building = target.with_name(f".{tag}-new")
    shutil.rmtree(building, ignore_errors=True)
    try:
        origin = run(
            ["git", "-C", str(checkout), "remote", "get-url", "origin"], capture_output=True, text=True, check=True
        )
        run(["git", "clone", "--branch", tag, "--depth", "1", origin.stdout.strip(), str(building)], check=True)
        run(["pixi", "install", "--locked"], cwd=building, check=True)
    except subprocess.CalledProcessError:
        shutil.rmtree(building, ignore_errors=True)
        raise
    (building / "mia.yaml").symlink_to(Path("../../mia.yaml"))
    building.rename(target)


def release(tag: str, checkout: Path = PROJECT_DIR, run: Runner = subprocess.run) -> int:
    """Put `tag` live in the installation `checkout` belongs to; 1 when a step failed before the flip."""
    root = installation_root(checkout)
    target = root / "releases" / tag
    try:
        if not target.exists():
            _fetch(tag, checkout, target, run)
        for name in read_manifest(root / "mia.yaml").get("services") or {}:
            run([str(target / MIA_BIN), "exec", name, "check"], cwd=target, check=True)
    except subprocess.CalledProcessError as error:
        said = (error.stderr or "").strip().splitlines()
        print(f"mia release {tag}: {' '.join(error.cmd)} failed" + (f": {said[-1]}" if said else ""), file=sys.stderr)
        return 1
    new = root / "current.new"
    new.unlink(missing_ok=True)
    new.symlink_to(Path("releases") / tag)
    new.replace(root / "current")  # atomic: the old release stays bound into its workers until they drain
    return run([str(root / "current" / MIA_BIN), "restart"], cwd=root / "current", check=False).returncode


def release_to(tag: str, target: str, checkout: Path = PROJECT_DIR, run: Runner = subprocess.run) -> int:
    """Push `tag` to origin and run `mia release <tag>` on the installation `HOST:ROOT` over ssh."""
    host, _, root = target.partition(":")
    if not host or not root:
        raise LayoutError("--to takes HOST:ROOT, the installation's host and its root directory")
    run(["git", "-C", str(checkout), "push", "origin", tag], check=True)
    return run(["ssh", host, f"{root}/current/{MIA_BIN} release {tag}"], check=False).returncode
