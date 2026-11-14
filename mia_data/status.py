"""Rendering the pieces of mia-data's issue comments.

Every stage change and every result is a new comment, never an edit, so the issue reads as a log. All
text that came from a record or a job goes through `safe_text` or `neutralize` first.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

from mia_core.judge.comment import render_judge_comment as render_judge_comment
from mia_core.untrusted import neutralize


def safe_text(value: Any) -> str:
    """Record or agent text as a single markdown-safe line: no forged markers, no table or line breaks."""
    return " ".join(str(value).split()).replace("<!--", "&lt;!--").replace("|", "\\|")


def render_stage(stage: str, detail: str = "", prefix: str = "data", sentence: str = "") -> str:
    """A stage's entry: the stage line and what the stage is doing: `detail` when the round has something of its
    own to say, else the stage's standing `sentence`."""
    said = neutralize(detail) if detail else sentence
    return f"**Stage:** `{prefix}:{safe_text(stage)}`" + (f"\n\n{said}" if said else "") + "\n"


def render_result(stage: str, text: str = "", prefix: str = "data", rests_at: str = "", noun: str = "dataset") -> str:
    """A stage pass's result entry: the stage it reports and what happened in it (`text`, already neutralized).
    `rests_at` is the stage the subject rests at when this result ended its round."""
    body = f"**Result:** `{prefix}:{safe_text(stage)}`" + (f"\n\n{text}" if text else "") + "\n"
    return body + (f"\nThe {noun} rests at `{prefix}:{safe_text(rests_at)}`.\n" if rests_at else "")


def render_jobs(links: dict[str, str]) -> str:
    """`**Jobs:** [name](url) · …`: the records of the jobs a pass ran, folded when there are more than four;
    empty when there is none."""
    items = [f"[{safe_text(name)}]({quote(url, safe=':/?=&%#')})" for name, url in links.items()]
    if not items:
        return ""
    line = f"**Jobs:** {' · '.join(items)}\n"
    return f"<details><summary>{len(items)} jobs</summary>\n\n{line}\n</details>\n" if len(items) > 4 else line


def stage_entries(
    prefix: str,
    round_id: str,
    stage: str,
    *,
    is_pass: bool,
    start_n: int = 1,
    sentence: str = "",
    detail: str = "",
    extra: str = "",
    links: str = "",
    open_stage: str = "",
    open_n: int = 1,
    result: str = "",
    shown: str = "",
    tag: str = "",
    noun: str = "dataset",
) -> list[tuple[str, str, str | None]]:
    """The timeline entries one stage write makes, oldest first, each as (body, marker text, the stage the entry
    sets or None). A round is a series of stage passes, and a pass writes its start and its result.

    `open_stage` is the pass whose result is due (its `open_n`th of the round), `result` what it found and
    `shown` what goes with it (its binding, its jobs). `stage` is what is written now; `is_pass` says whether a
    round works or waits there, `start_n` which pass of it this is. Every text is already neutralized.

    - an open pass, and `stage` is a pass: the open pass's result, then the new pass's start;
    - an open pass, and `stage` is where the subject rests: one entry, the open pass's result, which says how the
      pass ended (`detail`, `extra`), sets the stage and names it;
    - no open pass: the stage's own entry.

    A first pass's start keeps the marker `stage <round> <stage>`; a later one adds its number."""
    entries: list[tuple[str, str, str | None]] = []
    result_marker = f"{tag}result {round_id} {open_stage} {open_n}"
    if open_stage and not is_pass:
        ended = f"**{safe_text(stage)}**" + (f": {detail}" if detail else "")
        text = "\n\n".join(part for part in (result, ended, extra) if part)
        body = "\n".join(part for part in (render_result(open_stage, text, prefix, stage, noun), shown, links) if part)
        return [(body, result_marker, stage)]
    if open_stage:
        body = "\n".join(part for part in (render_result(open_stage, result, prefix), shown) if part)
        entries.append((body, result_marker, None))
    start_marker = f"{tag}stage {round_id} {stage}" + (f" {start_n}" if start_n > 1 else "")
    start = "\n".join(part for part in (render_stage(stage, detail, prefix, sentence), extra, links) if part)
    return [*entries, (start, start_marker, stage)]


def render_crops_table(rows: list[dict]) -> str:
    """A markdown table of crop thumbnails (raw, overlay, caption), folded into <details> when it is long.

    Each row has crop_id, plane, box, raw_url and overlay_url (see Activities.publish_gallery)."""

    def caption(row: dict) -> str:
        plane, box = row.get("plane") or {}, row.get("box") or {}
        return safe_text(
            neutralize(f"{row['crop_id']} · z={plane.get('z')} c={plane.get('c')} · ({box.get('y')}, {box.get('x')})")
        )

    lines = [
        "| raw | overlay | caption |",
        "|---|---|---|",
        *(
            f"| ![raw]({quote(row['raw_url'], safe=':/?=&%')}) | ![overlay]({quote(row['overlay_url'], safe=':/?=&%')}) "
            f"| {caption(row)} |"
            for row in rows
        ),
    ]
    table = "\n".join(lines) + "\n"
    return f"<details><summary>{len(rows)} crops</summary>\n\n{table}\n</details>\n" if len(rows) > 4 else table


def render_binding(path: str | None, digest: str | None) -> str:
    """The binding YAML in a fence longer than any backtick run inside it."""
    binding_file = Path(path) if path else None
    if not (binding_file and binding_file.is_file()):
        return "**Binding:** (none)\n"
    text = neutralize(binding_file.read_text())
    fence = "`" * max(3, 1 + max(map(len, re.findall(r"`+", text)), default=0))
    return f"**Binding** (sha256 `{safe_text(digest)}`):\n{fence}yaml\n{text.rstrip(chr(10))}\n{fence}\n"


def render_links(links: dict[str, str | None]) -> str:
    """`**Folders:** [name](url) · …` for the links that have a URL; empty when none has."""
    items = [f"[{safe_text(name)}]({quote(url, safe=':/?=&%')})" for name, url in links.items() if url]
    return f"**Folders:** {' · '.join(items)}\n" if items else ""
