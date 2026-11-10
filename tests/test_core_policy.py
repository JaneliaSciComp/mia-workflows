"""mia_core.judge.policy: the acceptance rules a gate's authorization rests on (spec autonomous-gates §5)."""

import pytest

from mia_core.judge.policy import POLICY_VERSION, Authorization, assessment_id, authorized, new_assessment, refused


def a(verdict, by="human:alice", at=1000.0, **kw):
    return new_assessment(
        gate="crops", subject_kind="crop_set", subject_ref="i7-e11:s-t", verdict=verdict, by=by, at=at, **kw
    )


def test_new_assessment_has_the_spec_shape_and_validates_its_enums():
    row = a("approve", summary="data:approve")
    assert row["subject"] == {"kind": "crop_set", "ref": "i7-e11:s-t", "source_digest": ""}
    assert row["policy_version"] == POLICY_VERSION and row["at"] == "1970-01-01T00:16:40+00:00"
    assert row["id"].startswith("0000001000000-") and len(row["id"]) == 26
    base = dict(gate="crops", subject_kind="crop_set", subject_ref="r", verdict="approve", by="judge", at=1.0)
    for bad in ({"gate": "vibes"}, {"subject_kind": "file"}, {"verdict": "maybe"}, {"by": "alice"}):
        with pytest.raises(ValueError):
            new_assessment(**{**base, **bad})


def test_ids_sort_by_time():
    assert assessment_id(1000.0) < assessment_id(1000.001) < assessment_id(2000.0)


def test_a_human_approve_authorizes_until_a_later_reject():
    assert authorized([]) == Authorization(False, f"no approve assessment under policy v{POLICY_VERSION}")
    ok = authorized([a("approve", at=10.0)])
    assert ok.ok and ok.assessment and "human:alice" in ok.reason
    out = authorized([a("approve", at=10.0), a("reject", by="human:bob", at=20.0, summary="wrong channel")])
    assert out == Authorization(False, "rejected by human:bob at 1970-01-01T00:00:20+00:00: wrong channel")
    again = authorized([a("approve", at=10.0), a("reject", at=5.0), a("approve", at=30.0)])
    assert again.ok  # a reject older than the newest approve does not count


def test_a_revocation_superseding_the_approve_blocks_whatever_its_time():
    approve = a("approve", at=10.0)
    revoke = a("reject", by="human:bob", at=9.0, supersedes=approve["id"], summary="revoked")
    assert not authorized([approve, revoke]).ok


def test_a_judge_approve_counts_only_for_an_enabled_gate_and_the_current_policy():
    judged = a("approve", by="judge", at=10.0)
    assert not authorized([judged]).ok
    assert authorized([judged], enabled_gates=frozenset({"crops"})).ok
    stale = {**judged, "policy_version": POLICY_VERSION - 1}
    assert authorized([stale], enabled_gates=frozenset({"crops"})).reason == (
        f"no approve assessment under policy v{POLICY_VERSION}"
    )


def test_checks_and_budget_are_inputs_the_policy_cannot_override():
    approve = [a("approve", at=10.0)]
    assert authorized(approve, checks_green=False) == Authorization(False, "deterministic checks are not green")
    assert authorized(approve, budget_ok=False) == Authorization(False, "the root budget does not allow it")


def test_refused_names_the_newest_reject_only():
    assert refused([]) is None and refused([a("approve", at=10.0)]) is None
    assert refused([a("approve", at=10.0), a("reject", by="human:bob", at=20.0, summary="shifted labels")]) == (
        "human:bob at 1970-01-01T00:00:20+00:00: shifted labels"
    )
    assert refused([a("reject", at=5.0), a("approve", at=10.0)]) is None
