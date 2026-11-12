"""Adopt the issues that exist as subjects of the registry (spec webapp-actions §11): every dataset issue of the
data repo, every buffer and preparation request issue of the buffers repo. A subject keeps its issue number as its
number, so workflow ids, round ids and budget roots stay what they are. Safe to run again: a subject a workflow has
written a stage to keeps that stage."""

from __future__ import annotations

from autodev.github import GitHub
from mia_core.registry import Registry, RegistryError
from mia_data import config as mia_data_config
from mia_data.seed import MARKER_RE

from .activities import SPEC_FENCE
from .config import PREP_STAGES, STAGES
from .spec import SpecError, load_spec_text


def _stage(issue: dict, prefix: str, stages: dict) -> str | None:
    """The stage the issue's first `<prefix><stage>` label names; None without one (`buffer:go` is not a stage)."""
    names = [label["name"] for label in issue.get("labels", [])]
    return next(
        (name[len(prefix) :] for name in names if name.startswith(prefix) and name[len(prefix) :] in stages), None
    )


def _fences(issue: dict) -> list[str]:
    return SPEC_FENCE.findall((issue.get("body") or "").replace("\r\n", "\n"))  # GitHub saves CRLF


async def adopt_issues(gh_data: GitHub, gh_buffers: GitHub, registry: Registry, *, dry_run: bool = False) -> list[str]:
    """Write a subject per issue and return the report, one line per issue looked at. A line starting `duplicate:`,
    `skipped:` or `refused:` names an issue that got no subject. `dry_run` reports and writes nothing."""
    lines: list[str] = []

    def adopt(kind: str, number: int, key: str, stage: str, data: dict) -> None:
        try:
            if not dry_run:
                registry.adopt_subject(kind, number, key, stage=stage, data=data)
        except RegistryError as error:
            lines.append(f"refused: {kind} #{number} {key}: {error.message}")
        else:
            lines.append(f"{kind} {key} #{number} {stage}")

    by_record: dict[str, list[dict]] = {}
    for issue in sorted(await gh_data.list_issues(state="all"), key=lambda issue: issue["number"]):
        if match := MARKER_RE.match(issue.get("body") or ""):  # the start of the body only: a quoted marker is not one
            by_record.setdefault(match[1], []).append(issue)
    for record, issues in sorted(by_record.items()):
        named = (registry.dataset(record) or {}).get("issue")  # the issue the dataset's row says is its own
        chosen = next((issue for issue in issues if issue["number"] == named), issues[0])
        adopt("dataset", chosen["number"], record, _stage(chosen, "data:", mia_data_config.STAGES) or "new", {})
        for other in issues:
            if other is not chosen:
                stage = _stage(other, "data:", mia_data_config.STAGES) or "new"
                lines.append(
                    f"duplicate: #{other['number']} also names {record} (adopted #{chosen['number']}); it is at {stage}"
                )

    names_taken: set[str] = set()
    for issue in sorted(await gh_buffers.list_issues(state="all"), key=lambda issue: issue["number"]):
        number, fences = issue["number"], _fences(issue)
        if prep_stage := _stage(issue, "prep:", PREP_STAGES):
            adopt("prep", number, f"prep-{number}", prep_stage, {"spec": fences[0]} if len(fences) == 1 else {})
        elif buffer_stage := _stage(issue, "buffer:", STAGES):
            try:
                name = load_spec_text(fences[0]).name if len(fences) == 1 else None
            except SpecError:
                name = None
            if name is None:  # no single valid spec: nothing to name it by
                adopt("buffer", number, f"issue-{number}", buffer_stage, {})
                continue
            key = f"{name}-{number}" if name in names_taken else name  # a later issue with the same spec name
            names_taken.add(name)
            adopt("buffer", number, key, buffer_stage, {"spec": fences[0]})
        else:
            lines.append(f"skipped: #{number} has no buffer or prep label")
    return lines
