"""The commit of the checkout this code runs from: a worker says it when it starts, and the cleaner compares it
with the checkout's commit later, since a worker keeps the activities it loaded until it is restarted."""

from __future__ import annotations

import subprocess
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent


def head_commit(checkout: Path = PROJECT_DIR) -> str | None:
    """`git rev-parse HEAD` in `checkout`; None when git is not there, the directory is no checkout, or git
    refuses it."""
    try:
        done = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=10, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    commit = done.stdout.strip()
    return commit if done.returncode == 0 and len(commit) == 40 else None
