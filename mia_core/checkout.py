"""The code of the checkout a process runs from: a worker says what it loaded when it starts, and the cleaner
compares that with the checkout later, since a worker keeps the activities it loaded until it is restarted."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
PACKAGES = ("autodev", "mia_core", "mia_data", "mia_buffers")  # what a worker loads


NOT_LOADED = ("mia_buffers/ui",)  # the page: buffer-api serves its files from disk as they are


def _git(checkout: Path, *arguments: str) -> str | None:
    """What `git <arguments>` prints in `checkout`; None when git is not there, the directory is no checkout or
    git refuses."""
    try:
        done = subprocess.run(
            ["git", "-C", str(checkout), *arguments], capture_output=True, text=True, timeout=10, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout if done.returncode == 0 else None


def head_commit(checkout: Path = PROJECT_DIR) -> str | None:
    """The commit `checkout` is at."""
    found = (_git(checkout, "rev-parse", "HEAD") or "").strip()
    return found or None


def code_digest(checkout: Path = PROJECT_DIR) -> str | None:
    """A digest of the packages a worker loads, as committed at HEAD (the git ids of what each holds): it changes
    when their code does, and not with a commit that touches only docs, tests or the page."""
    listed = _git(checkout, "ls-tree", "HEAD", *(f"{package}/" for package in PACKAGES))
    entries = [line for line in (listed or "").splitlines() if line.split("\t")[-1] not in NOT_LOADED]
    return hashlib.sha256("\n".join(entries).encode()).hexdigest()[:16] if entries else None
