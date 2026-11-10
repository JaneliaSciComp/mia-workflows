"""The acceptance policy: the shape of an assessment and the deterministic rule that turns a set of them into an
authorization (spec autonomous-gates §5). Pure functions; the registry stores the records, the activities ask here."""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

POLICY_VERSION: int = 1
GATES = ("review", "crops", "code", "staged")
SUBJECT_KINDS = ("pr_head", "crop_set", "artifact")
VERDICTS = ("approve", "reject", "escalate", "audited")


def assessment_id(at: float) -> str:
    """An id that sorts by time: millisecond epoch, then a random tail."""
    return f"{int(at * 1000):013d}-{uuid.uuid4().hex[:12]}"


def _iso(at: float) -> str:
    return datetime.fromtimestamp(at, UTC).isoformat()


def new_assessment(
    *,
    gate: str,
    subject_kind: str,
    subject_ref: str,
    verdict: str,
    by: str,
    at: float,
    source_digest: str = "",
    summary: str = "",
    findings: Sequence[Mapping[str, Any]] = (),
    supersedes: str | None = None,
    confidence: float | None = None,
    evidence_digest: str = "",
    checks: Mapping[str, Any] | None = None,
    model: str = "",
    prompt_version: str = "",
    coverage: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The spec §5 record. Raises ValueError for a gate, kind, verdict or author outside the policy's vocabulary."""
    if gate not in GATES:
        raise ValueError(f"unknown gate {gate!r}")
    if subject_kind not in SUBJECT_KINDS:
        raise ValueError(f"unknown subject kind {subject_kind!r}")
    if verdict not in VERDICTS:
        raise ValueError(f"unknown verdict {verdict!r}")
    if by != "judge" and not by.startswith("human:") and not by.startswith("policy:"):
        raise ValueError(f"assessment author must be 'judge', 'human:<login>' or 'policy:<name>', got {by!r}")
    return {
        "id": assessment_id(at),
        "gate": gate,
        "subject": {"kind": subject_kind, "ref": subject_ref, "source_digest": source_digest},
        "verdict": verdict,
        "by": by,
        "at": _iso(at),
        "policy_version": POLICY_VERSION,
        "confidence": confidence,
        "findings": [dict(finding) for finding in findings],
        "summary": summary,
        "supersedes": supersedes,
        "evidence_digest": evidence_digest,
        "checks": dict(checks) if checks is not None else None,
        "model": model,
        "prompt_version": prompt_version,
        "coverage": dict(coverage) if coverage is not None else None,
    }


@dataclass(frozen=True)
class Authorization:
    """What the policy answers: whether the subject may proceed, why, and the assessment it rests on."""

    ok: bool
    reason: str
    assessment: str | None = None


def authorized(
    assessments: Sequence[Mapping[str, Any]],
    *,
    policy_version: int = POLICY_VERSION,
    enabled_gates: frozenset[str] = frozenset(),
    checks_green: bool = True,
    budget_ok: bool = True,
) -> Authorization:
    """The newest acceptable approve stands unless a reject follows it (or supersedes it), the deterministic checks
    are red, or the root budget says no. A judge's approve counts only for an enabled gate."""
    ordered = sorted(assessments, key=lambda row: str(row.get("at", "")))
    approves = [
        row
        for row in ordered
        if row.get("verdict") == "approve"
        and row.get("policy_version") == policy_version
        and (str(row.get("by", "")).startswith(("human:", "policy:")) or row.get("gate") in enabled_gates)
    ]
    if not approves:
        return Authorization(False, f"no approve assessment under policy v{policy_version}")
    approve = approves[-1]
    rejects = [
        row
        for row in ordered
        if row.get("verdict") == "reject"
        and (str(row.get("at", "")) >= str(approve.get("at", "")) or row.get("supersedes") == approve.get("id"))
    ]
    if rejects:
        reject = rejects[-1]
        summary = reject.get("summary") or "no summary"
        return Authorization(False, f"rejected by {reject.get('by')} at {reject.get('at')}: {summary}")
    if not checks_green:
        return Authorization(False, "deterministic checks are not green")
    if not budget_ok:
        return Authorization(False, "the root budget does not allow it")
    return Authorization(True, f"approved by {approve.get('by')} at {approve.get('at')}", str(approve.get("id")))


def refused(assessments: Sequence[Mapping[str, Any]]) -> str | None:
    """The newest assessment when it is a reject, as 'by at time: summary'; None otherwise."""
    if not assessments:
        return None
    newest = max(assessments, key=lambda row: str(row.get("at", "")))
    if newest.get("verdict") != "reject":
        return None
    return f"{newest.get('by')} at {newest.get('at')}: {newest.get('summary') or 'no summary'}"
