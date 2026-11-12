"""What the mirror writes when it opens the GitHub issue of a buffer spec or a preparation request created on
the page (spec webapp-actions §7 step 1). The issue is a read-only log: its first line is a marker naming the
subject, so an issue opened just before a crash is found again; its second says where to act. The mirror itself
is `mia_data.mirror.Mirror`; the buffers worker registers it with these two descriptions."""

from __future__ import annotations

from urllib.parse import quote

from mia_data.mirror import Describe, read_only_line

from .config import Config
from .spec import SpecError, load_prep_text


def buffer_issue(cfg: Config) -> Describe:
    """A buffer spec's issue: titled by the spec's name, as the issues opened by hand were. The spec itself is in
    the comment of each `go`, since every run may carry an edited one."""

    async def describe(subject: dict) -> tuple[str, str, list[str]]:
        name = subject["key"]
        page = f"{cfg.service.url.rstrip('/')}/#specs/{quote(name, safe='')}"
        body = "\n".join(
            [
                cfg.marker(f"subject buffer {subject['number']}"),
                read_only_line(page),
                "",
                f"Buffer spec `{name}`. Each run's comment carries the spec it ran.",
                "",
            ]
        )
        return name, body, [cfg.label("new")]

    return describe


def prep_issue(cfg: Config) -> Describe:
    """A preparation request's issue: titled by how many records it asks for, with the request as it was filed."""

    async def describe(subject: dict) -> tuple[str, str, list[str]]:
        key, text = subject["key"], str(subject["data"].get("spec") or "")
        try:
            spec = load_prep_text(text)
            what = f"{len(spec.records)} records" if spec.records else "the records a selection admits"
        except SpecError:  # stored as it was sent; the round says what is wrong with it
            what = "records"
        page = f"{cfg.service.url.rstrip('/')}/#requests/{quote(key, safe='')}"
        fenced = text.replace("```", "` ` `")
        body = "\n".join(
            [
                cfg.marker(f"subject prep {subject['number']}"),
                read_only_line(page),
                "",
                f"```yaml\n{fenced}{'' if fenced.endswith(chr(10)) else chr(10)}```",
                "",
            ]
        )
        return f"Prepare {what} ({key})", body, []

    return describe
