"""`mia judge eval <gate> <dir>` (spec autonomous-gates §9): run a judge over an evaluation set and score it.

An evaluation set is a directory of items, each `<item>/evidence/` as the worker would build it plus
`<item>/expected.json` ({"verdict": "approve" | "reject", "category": "<modality>/<label kind>", ...}). The judge
runs on this host (the operator's `claude` login), not through csub: the set is the operator's, built from rounds
already judged by a human. The report counts, per category, agreement, false approvals (a human said reject, the
judge approve), false rejections and abstentions."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from mia_core.config import JudgeCfg

from .evidence import EVIDENCE_DIR, command, read_claude_json
from .schema import effective_verdict, parse_verdict

EXPECTED_FILE = "expected.json"
Runner = Callable[..., object]


@dataclass(frozen=True)
class EvalRow:
    """One item's result: what the human said, what the judge said after policy."""

    item: str
    category: str
    expected: str
    got: str
    confidence: float
    cost_usd: float | None


def run_item(item: Path, cfg: JudgeCfg, run: Runner = subprocess.run, max_suspect: int | None = None) -> EvalRow:
    """Run the judge over a copy of the item's evidence in a fresh temporary directory (the answer key stays out
    of its reach) and read its answer; a run that fails, hangs or writes no readable verdict is an `escalate`."""
    # ponytail: on the operator's host the judge is not sandboxed, so Read works anywhere; the copy keeps the set's
    # own expected.json files away from it, nothing more. Run the set through csub if that ceiling matters.
    expected = json.loads((item / EXPECTED_FILE).read_text())
    with tempfile.TemporaryDirectory(prefix="judge-eval-") as scratch:
        workspace = Path(scratch) / "run"
        shutil.copytree(item / EVIDENCE_DIR, workspace / EVIDENCE_DIR)
        try:
            run(
                ["bash", "-c", command(cfg.model, cfg.max_budget_usd)],
                cwd=workspace,
                check=False,
                capture_output=True,
                timeout=cfg.walltime_min * 60,
            )
        except subprocess.TimeoutExpired:
            return EvalRow(
                item.name, str(expected.get("category") or "unknown"), str(expected["verdict"]), "escalate", 0.0, None
            )
        text, cost, _ = read_claude_json(workspace)
    try:
        verdict = parse_verdict(text)
        got, _ = effective_verdict(verdict, cfg.min_confidence, max_suspect)
        confidence = verdict.confidence
    except ValueError:
        got, confidence = "escalate", 0.0
    return EvalRow(
        item.name, str(expected.get("category") or "unknown"), str(expected["verdict"]), got, confidence, cost
    )


def items(eval_dir: Path) -> list[Path]:
    """The set's items, in name order: directories with an evidence dir and an expected.json."""
    return sorted(
        path
        for path in Path(eval_dir).iterdir()
        if path.is_dir() and (path / EVIDENCE_DIR).is_dir() and (path / EXPECTED_FILE).is_file()
    )


def evaluate(
    eval_dir: Path,
    cfg: JudgeCfg,
    *,
    limit: int | None = None,
    run: Runner = subprocess.run,
    max_suspect: int | None = None,
) -> list[EvalRow]:
    return [run_item(item, cfg, run, max_suspect) for item in items(eval_dir)[:limit]]


def summarize(rows: list[EvalRow]) -> dict[str, dict[str, int]]:
    """Per category and for `all`: n, agree, false_approve, false_reject, abstain."""
    out: dict[str, dict[str, int]] = {}
    for row in rows:
        for key in (row.category, "all"):
            bucket = out.setdefault(key, {"n": 0, "agree": 0, "false_approve": 0, "false_reject": 0, "abstain": 0})
            bucket["n"] += 1
            if row.got == "escalate":
                bucket["abstain"] += 1
            elif row.got == row.expected:
                bucket["agree"] += 1
            elif row.got == "approve":
                bucket["false_approve"] += 1
            else:
                bucket["false_reject"] += 1
    return out


def render(summary: dict[str, dict[str, int]]) -> str:
    """The report as a markdown table, `all` last."""
    lines = ["| category | n | agree | false approve | false reject | abstain |", "|---|---|---|---|---|---|"]
    for key in [*sorted(k for k in summary if k != "all"), *(["all"] if "all" in summary else [])]:
        bucket = summary[key]
        lines.append(
            f"| {key} | {bucket['n']} | {bucket['agree']} | {bucket['false_approve']} | {bucket['false_reject']} | "
            f"{bucket['abstain']} |"
        )
    return "\n".join(lines) + "\n"
