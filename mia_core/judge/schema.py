"""What a judge answers and how policy reads it (spec autonomous-gates §4): the verdict object the job's final
message must be, its parser over an untrusted text, the acceptance rule (serious issues only, confidence as an
abstain signal) and the prompts, versioned by their file hash."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from importlib.resources import files

SCHEMA_VERSION = 1
SEVERITIES = ("ok", "suspect", "bad")
JUDGE_VERDICTS = ("approve", "reject", "escalate")
PROMPT_GATES = ("crops", "review")
MAX_FINDINGS = 64
_FENCE_RE = re.compile(r"```(?:json)?\s*\n(.*?)\n\s*```", re.DOTALL)


@dataclass(frozen=True)
class Finding:
    """One item the judge looked at: a crop id, a file, a volume; `bad` is the only severity that can reject."""

    item: str
    severity: str
    note: str = ""


@dataclass(frozen=True)
class Verdict:
    """The judge's own answer, before policy."""

    verdict: str
    confidence: float
    findings: tuple[Finding, ...]
    summary: str
    schema_version: int = SCHEMA_VERSION


def _candidates(text: str) -> list[str]:
    """The texts that may hold the verdict object: the whole message, the last fenced block, the outermost braces."""
    out = [text.strip()]
    out.extend(match.group(1) for match in reversed(list(_FENCE_RE.finditer(text))))
    start, end = text.find("{"), text.rfind("}")
    if 0 <= start < end:
        out.append(text[start : end + 1])
    return out


def _finding(raw: object) -> Finding:
    if not isinstance(raw, dict) or not isinstance(raw.get("item"), str) or raw.get("severity") not in SEVERITIES:
        raise ValueError(f"bad finding {str(raw)[:200]!r}")
    note = raw.get("note", "")
    if not isinstance(note, str):
        raise ValueError("a finding's note must be text")
    return Finding(raw["item"][:200], raw["severity"], note[:2000])


def _verdict(data: object) -> Verdict:
    if not isinstance(data, dict):
        raise ValueError("the verdict is not a JSON object")
    if data.get("schema_version", SCHEMA_VERSION) != SCHEMA_VERSION:
        raise ValueError(f"schema_version {data.get('schema_version')!r} is not {SCHEMA_VERSION}")
    if data.get("verdict") not in JUDGE_VERDICTS:
        raise ValueError(f"verdict {data.get('verdict')!r} is not one of {JUDGE_VERDICTS}")
    confidence = data.get("confidence")
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool) or not 0 <= confidence <= 1:
        raise ValueError(f"confidence {confidence!r} is not a number in [0, 1]")
    findings = data.get("findings", [])
    if not isinstance(findings, list) or len(findings) > MAX_FINDINGS:
        raise ValueError(f"findings must be a list of at most {MAX_FINDINGS}")
    summary = data.get("summary", "")
    if not isinstance(summary, str):
        raise ValueError("summary must be text")
    return Verdict(data["verdict"], float(confidence), tuple(_finding(raw) for raw in findings), summary[:4000])


def parse_verdict(text: str) -> Verdict:
    """The one verdict object in an agent's final message. Raises ValueError when there is none, or it does not
    fit the schema: the caller records an `escalate`."""
    errors = []
    for candidate in _candidates(text):
        try:
            return _verdict(json.loads(candidate))
        except (ValueError, RecursionError) as error:  # JSONDecodeError is a ValueError
            errors.append(str(error))
    raise ValueError(errors[-1] if errors else "empty message")


def effective_verdict(verdict: Verdict, min_confidence: float, max_suspect: int | None = None) -> tuple[str, str]:
    """What policy makes of the judge's answer: (approve | reject | escalate, why). Low confidence abstains; any
    `bad` finding rejects whatever the judge wrote (spec §4.2); a reject with no `bad` finding is an approve with
    notes (the user's rule: serious issues only); more `suspect` findings than `max_suspect` reject too."""
    if verdict.verdict == "escalate":
        return "escalate", "escalate"
    if verdict.confidence < min_confidence:
        return "escalate", f"confidence {verdict.confidence:g} is under the {min_confidence:g} floor"
    bad = [finding.item for finding in verdict.findings if finding.severity == "bad"]
    if bad:
        return "reject", f"a bad finding: {', '.join(bad[:5])}"
    if verdict.verdict == "reject":
        return "approve", "reject without a bad finding is treated as approve with notes"
    suspects = sum(1 for finding in verdict.findings if finding.severity == "suspect")
    if max_suspect is not None and suspects > max_suspect:
        return "reject", f"{suspects} suspect findings, more than the cap of {max_suspect}"
    return "approve", "approve"


def prompt_text(gate: str) -> str:
    """The gate's question (prompts/<gate>.md)."""
    if gate not in PROMPT_GATES:
        raise ValueError(f"no prompt for gate {gate!r}")
    return files("mia_core.judge").joinpath(f"prompts/{gate}.md").read_text()


def prompt_version(gate: str) -> str:
    """The prompt's identity in an assessment: the first 12 hex of the sha256 of its text."""
    return hashlib.sha256(prompt_text(gate).encode()).hexdigest()[:12]
