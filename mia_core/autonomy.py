"""The pause switch: `autonomy.pause` in mia.yaml, re-read by every worker at each job boundary and flipped in
place by `mia pause` / `mia resume` with a text edit that keeps the file's comments."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml

log = logging.getLogger(__name__)
BLOCK_RE = re.compile(r"^autonomy:[^\n]*\n((?:[ \t]+[^\n]*\n?)*)", re.M)  # the top-level block and its children
PAUSE_RE = re.compile(r"^([ \t]+pause:[ \t]*)(\S+)", re.M)


def paused(manifest: Path) -> bool:
    """Whether `autonomy.pause` is true in the manifest now; a missing or unreadable file is not a pause (logged)."""
    try:
        data = yaml.safe_load(manifest.read_text())
    except (OSError, yaml.YAMLError) as error:
        log.warning("paused: cannot read %s (%s); treating as not paused", manifest, error)
        return False
    autonomy = data.get("autonomy") if isinstance(data, dict) else None
    return isinstance(autonomy, dict) and autonomy.get("pause") is True


def set_pause(manifest: Path, value: bool) -> None:
    """Write `autonomy.pause: <value>` into the manifest, editing the existing line in place (its trailing comment
    stays), else adding the key as the block's first child, else appending the block."""
    text = manifest.read_text()
    flag = "true" if value else "false"
    block = BLOCK_RE.search(text)
    if block is None:
        joint = "" if text.endswith("\n") or not text else "\n"
        manifest.write_text(f"{text}{joint}autonomy:\n  pause: {flag}\n")
        return
    body = block.group(1)
    if PAUSE_RE.search(body):
        edited = PAUSE_RE.sub(lambda match: f"{match.group(1)}{flag}", body, count=1)
    else:
        edited = f"  pause: {flag}\n{body}"
    manifest.write_text(text[: block.start(1)] + edited + text[block.end(1) :])
