"""The code of the checkout a process runs from: a worker says what it loaded when it starts, and the cleaner
compares that with the checkout later, since a worker keeps the activities it loaded until it is restarted."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
PACKAGES = ("autodev", "mia_core", "mia_data", "mia_buffers")  # what a worker loads


def _rev_parse(checkout: Path, *names: str) -> list[str] | None:
    """`git rev-parse <names>` in `checkout`: one id per name; None when git is not there, the directory is no
    checkout, a name does not resolve, or git refuses."""
    try:
        done = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", *names], capture_output=True, text=True, timeout=10, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    ids = done.stdout.split()
    return ids if done.returncode == 0 and len(ids) == len(names) else None


def head_commit(checkout: Path = PROJECT_DIR) -> str | None:
    """The commit `checkout` is at."""
    found = _rev_parse(checkout, "HEAD")
    return found[0] if found else None


def code_digest(checkout: Path = PROJECT_DIR) -> str | None:
    """A digest of the packages a worker loads, as committed at HEAD (their git trees): it changes when their code
    does, and not with a commit that touches only docs or tests."""
    trees = _rev_parse(checkout, *(f"HEAD:{package}" for package in PACKAGES))
    return hashlib.sha256(" ".join(trees).encode()).hexdigest()[:16] if trees else None
