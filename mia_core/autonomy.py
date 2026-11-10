"""The pause switch: `autonomy.pause` in mia.yaml, re-read by every worker at each job boundary and flipped in
place by `mia pause` / `mia resume` with a text edit that keeps the file's comments."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml

from mia_core.untrusted import write_atomic

log = logging.getLogger(__name__)
PAUSE_LINE_RE = re.compile(r"^(\s*pause:\s*)\S+")


def paused(manifest: Path) -> bool:
    """Whether `autonomy.pause` is true in the manifest now; a missing or unreadable file is not a pause (logged)."""
    try:
        data = yaml.safe_load(manifest.read_text())
    except (OSError, yaml.YAMLError) as error:
        log.warning("paused: cannot read %s (%s); treating as not paused", manifest, error)
        return False
    autonomy = data.get("autonomy") if isinstance(data, dict) else None
    return isinstance(autonomy, dict) and autonomy.get("pause") is True


def _block_end(lines: list[str], start: int) -> int:
    """The index after the last line of the top-level block opened at `start`: blank lines, comments and indented
    lines belong to it; the next column-0 key ends it."""
    end = start + 1
    while end < len(lines) and (
        not lines[end].strip() or lines[end].lstrip().startswith("#") or lines[end][0].isspace()
    ):
        end += 1
    return end


def set_pause(manifest: Path, value: bool) -> None:
    """Write `autonomy.pause: <value>` into the manifest: the block's own `pause` line is edited in place (its trailing
    comment stays), else the key is added as the block's first child, else the block is appended. The result is read
    back; a file the edit did not flip raises instead of reporting a pause that is not there."""
    text = manifest.read_text()
    flag = "true" if value else "false"
    lines = text.split("\n")
    start = next((index for index, line in enumerate(lines) if re.match(r"^autonomy:", line)), None)
    if start is None:
        joint = "" if text.endswith("\n") or not text else "\n"
        out = f"{text}{joint}autonomy:\n  pause: {flag}\n"
    else:
        end = _block_end(lines, start)
        children = [
            index
            for index in range(start + 1, end)
            if lines[index].strip() and not lines[index].lstrip().startswith("#")
        ]
        indent = len(lines[children[0]]) - len(lines[children[0]].lstrip()) if children else 2
        own = [index for index in children if len(lines[index]) - len(lines[index].lstrip()) == indent]
        hit = next((index for index in own if PAUSE_LINE_RE.match(lines[index])), None)
        if hit is not None:
            lines[hit] = PAUSE_LINE_RE.sub(lambda match: f"{match.group(1)}{flag}", lines[hit], count=1)
        else:
            lines.insert(start + 1, f"{' ' * indent}pause: {flag}")
        out = "\n".join(lines)
    write_atomic(manifest, out)
    if paused(manifest) is not value:
        raise RuntimeError(f"{manifest}: autonomy.pause did not become {flag}; edit the file by hand")
