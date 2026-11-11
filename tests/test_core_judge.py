"""The judge mechanism every gate shares: the verdict schema and policy, the prompts, the evidence builder."""

import hashlib
import json

import pytest

from mia_core.judge.schema import (
    SCHEMA_VERSION,
    Finding,
    Verdict,
    effective_verdict,
    parse_verdict,
    prompt_text,
    prompt_version,
)

APPROVE = {
    "verdict": "approve",
    "confidence": 0.9,
    "findings": [{"item": "c1", "severity": "ok", "note": "fine"}],
    "summary": "one dataset",
    "schema_version": 1,
}


def test_parse_verdict_accepts_whole_text_fence_and_trailing_object():
    body = json.dumps(APPROVE)
    expected = Verdict("approve", 0.9, (Finding("c1", "ok", "fine"),), "one dataset", SCHEMA_VERSION)
    assert parse_verdict(body) == expected
    assert parse_verdict(f"Here is my verdict.\n```json\n{body}\n```\nDone.") == expected
    assert parse_verdict(f"Looked at every crop.\n{body}") == expected


@pytest.mark.parametrize(
    "text",
    [
        "no object here",
        "[1, 2]",
        json.dumps({**APPROVE, "verdict": "maybe"}),
        json.dumps({**APPROVE, "confidence": 1.5}),
        json.dumps({**APPROVE, "findings": [{"item": "c1", "severity": "meh"}]}),
        json.dumps({**APPROVE, "schema_version": 2}),
        json.dumps({**APPROVE, "summary": 3}),
    ],
)
def test_parse_verdict_rejects_bad_shapes(text):
    with pytest.raises(ValueError):
        parse_verdict(text)


def test_reject_without_bad_finding_is_approve_with_notes():
    verdict = Verdict("reject", 0.9, (Finding("c1", "suspect", "dim"),), "nits")
    assert effective_verdict(verdict, 0.6) == (
        "approve",
        "reject without a bad finding is treated as approve with notes",
    )
    bad = Verdict("reject", 0.9, (Finding("c1", "bad", "header read as pixels"),), "broken")
    assert effective_verdict(bad, 0.6) == ("reject", "a bad finding: c1")


def test_low_confidence_escalates():
    verdict = Verdict("approve", 0.4, (), "unsure")
    assert effective_verdict(verdict, 0.6) == ("escalate", "confidence 0.4 is under the 0.6 floor")
    assert effective_verdict(Verdict("approve", 0.6, (), "ok"), 0.6) == ("approve", "approve")
    assert effective_verdict(Verdict("escalate", 0.9, (), "cannot tell"), 0.6) == ("escalate", "escalate")


def test_prompts_exist_and_versions_are_file_hashes():
    for gate in ("crops", "review"):
        text = prompt_text(gate)
        assert "serious" in text.lower() and "schema_version" in text
        assert prompt_version(gate) == hashlib.sha256(text.encode()).hexdigest()[:12]
    assert prompt_version("crops") != prompt_version("review")
    with pytest.raises(ValueError):
        prompt_text("nope")
