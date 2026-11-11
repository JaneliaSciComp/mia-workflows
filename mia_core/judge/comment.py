"""The comment a judge leaves on the issue or PR (spec autonomous-gates §10): what it decided and why policy kept or
changed it, the findings table, the provenance line and what the verdict means in this mode. Shared by every
gate; the text that came from the judge goes through `neutralize` and loses table and line breaks."""

from __future__ import annotations

from typing import Any

from mia_core.untrusted import neutralize

JUDGE_NAMES = {"crops": "Crop judge", "review": "Review judge", "code": "Code judge", "staged": "Staged judge"}
SHADOW_LINE = "Shadow mode: this verdict is recorded and compared with the human label; it decides nothing."


def _cell(value: Any) -> str:
    """Judge text as one markdown table cell: no forged markers, no table or line breaks."""
    return " ".join(str(value).split()).replace("<!--", "&lt;!--").replace("|", "\\|")


def render_judge_comment(
    gate: str,
    verdict: str,
    effective: str,
    why: str,
    summary: str,
    findings: list[dict],
    confidence: float,
    model: str,
    prompt_version: str,
    policy_version: int,
    cost_usd: float | None,
    mode: str,
) -> str:
    head = f"**{JUDGE_NAMES.get(gate, gate)}:** `{effective}`"
    if effective != verdict:
        head += f" (the judge said `{verdict}`: {_cell(why)})"
    lines = [head, "", neutralize(summary) if summary else "(no summary)", ""]
    if findings:
        lines += [
            "| item | severity | note |",
            "|---|---|---|",
            *(
                f"| {_cell(finding.get('item', ''))} | {_cell(finding.get('severity', ''))} | "
                f"{_cell(finding.get('note', ''))} |"
                for finding in findings
            ),
            "",
        ]
    provenance = (
        f"confidence {confidence:g} · model `{_cell(model)}` · prompt `{prompt_version}` · policy v{policy_version}"
    )
    if cost_usd is not None:
        provenance += f" · ${cost_usd:.2f}"
    lines += [provenance, ""]
    if mode == "shadow":
        lines.append(SHADOW_LINE)
    elif effective == "approve":
        lines.append(f"Approved automatically under policy v{policy_version}; a revoke label withdraws it at any time.")
    else:
        lines.append(f"Decided automatically under policy v{policy_version}.")
    return "\n".join(lines) + "\n"
