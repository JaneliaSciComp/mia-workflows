"""`mia release <tag>`: put a tagged checkout live in an installation laid out as `<root>/releases/<tag>` with
`<root>/current -> releases/<tag>` and `<root>/mia.yaml`. Five steps: clone the tag from `origin` and `pixi install`
it (when `releases/<tag>` is not there yet), link `mia.yaml` into it, run every service's `check` from it, flip
`current`, and `mia restart` from the new `current`. Rolling back is the same command with an earlier tag."""

from __future__ import annotations

import re
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


TAG_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")  # one path component, and never a git option


def _fetch(tag: str, checkout: Path, target: Path, run: Runner) -> None:
    """Clone `tag` from the checkout's origin into `target` and install it there (a pixi environment hard-codes its
    own path, so it is built where it stays); the `mia.yaml` link, written last, marks the install finished. Whatever
    is left of a failed attempt is removed."""
    shutil.rmtree(target, ignore_errors=True)
    try:
        origin = run(
            ["git", "-C", str(checkout), "remote", "get-url", "origin"], capture_output=True, text=True, check=True
        )
        run(["git", "clone", "--branch", tag, "--depth", "1", origin.stdout.strip(), str(target)], check=True)
        run(["pixi", "install", "--locked"], cwd=target, check=True)
    except (subprocess.CalledProcessError, OSError):
        shutil.rmtree(target, ignore_errors=True)
        raise
    (target / "mia.yaml").symlink_to(Path("../../mia.yaml"))


def _said(error: Exception) -> str:
    """One line of what a failed step said."""
    if isinstance(error, subprocess.CalledProcessError):
        lines = (error.stderr or "").strip().splitlines()
        return f"{' '.join(error.cmd)} failed" + (f": {lines[-1]}" if lines else "")
    return str(error)


def release(tag: str, checkout: Path = PROJECT_DIR, run: Runner = subprocess.run) -> int:
    """Put `tag` live in the installation `checkout` belongs to; 1 when a step failed before the flip."""
    if not TAG_RE.fullmatch(tag):
        raise LayoutError(f"tag {tag!r}: a release tag is letters, digits, '.', '_' and '-', not starting with '-'")
    root = installation_root(checkout)
    target = root / "releases" / tag
    try:
        if not (target / "mia.yaml").is_symlink():  # not there, or an install that never finished
            _fetch(tag, checkout, target, run)
        for name in read_manifest(root / "mia.yaml").get("services") or {}:
            run([str(target / MIA_BIN), "exec", name, "check"], cwd=target, check=True)
    except (subprocess.CalledProcessError, OSError) as error:
        print(f"mia release {tag}: {_said(error)}", file=sys.stderr)
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
