import dataclasses

from autodev.decide import CLOSED, DONE, ESCALATE, MERGE, RESET, REVISE, WAIT, decide, escalation
from autodev.models import Feedback, PrSnapshot

F1, F2 = Feedback("review-1", "a"), Feedback("comment-2", "b")


def snap(state="open", approved=False, checks="none", feedback=()):
    return PrSnapshot(5, state, "h", approved, checks, list(feedback))


def test_terminal_states():
    assert decide(snap("merged"), [], 0, 3) == (DONE, None)
    assert decide(snap("closed", approved=True), [], 0, 3) == (CLOSED, None)


def test_unhandled_feedback_wins_over_approval():
    assert decide(snap(approved=True, checks="success", feedback=[F1, F2]), ["review-1"], 1, 3) == (REVISE, F2)


def test_approval_with_passing_or_no_checks_merges():
    assert decide(snap(approved=True, checks="success"), [], 0, 3) == (MERGE, None)
    assert decide(snap(approved=True, checks="none"), [], 0, 3) == (MERGE, None)


def test_stale_approval_waits():
    # PrSnapshot.approved is computed against the current head, so a pre-revision approval arrives as False.
    assert decide(snap(approved=False, checks="success"), [], 1, 3) == (WAIT, None)


def test_approved_but_pending_waits_and_failing_escalates():
    assert decide(snap(approved=True, checks="pending"), [], 0, 3) == (WAIT, None)
    assert decide(snap(approved=True, checks="failure"), [], 0, 3)[0] == ESCALATE


def test_revision_limit_escalates_even_when_approved():
    """A pending human change request blocks a merge regardless of the retry counter (spec autonomous-gates §3);
    `_snapshot` has already dropped requests their author superseded by approving."""
    assert decide(snap(feedback=[F1]), [], 3, 3) == (ESCALATE, "revision limit (3) reached")
    assert decide(snap(approved=True, checks="success", feedback=[F1]), [], 3, 3) == (
        ESCALATE,
        "approved with unaddressed change requests (revision limit 3 reached)",
    )
    assert decide(snap(approved=True, checks="success"), [], 3, 3) == (MERGE, None)  # nothing pending: merges


def test_raised_escalation_waits_until_the_head_changes():
    reason = "revision limit (3) reached"
    raised = escalation(reason, snap())
    assert decide(snap(feedback=[F1]), [], 3, 3, raised) == (WAIT, None)
    assert decide(snap(approved=True, checks="failure"), [], 3, 3, raised)[0] == ESCALATE  # a different reason
    assert decide(PrSnapshot(5, "open", "h2", False, "none", [F1]), [], 3, 3, raised) == (ESCALATE, reason)


def test_an_accept_outranks_pending_feedback_and_resets_before_it_merges():
    """spec judge-decides §5.1: a person accepted the set of an earlier head; later commits are discarded."""
    later = dataclasses.replace(snap(checks="success", feedback=[F1]), head_sha="b" * 40, accepted="a" * 40)
    assert decide(later, [], 0, 3) == (RESET, "a" * 40)  # never a revision once a head was accepted
    there = dataclasses.replace(later, head_sha="a" * 40)
    assert decide(there, [], 0, 3) == (MERGE, None)  # no approval of the head is needed: the accept is one
    assert decide(dataclasses.replace(there, checks="pending"), [], 0, 3) == (WAIT, None)
    assert decide(dataclasses.replace(there, checks="failure"), [], 0, 3)[0] == ESCALATE
    assert decide(dataclasses.replace(later, state="merged"), [], 0, 3) == (DONE, None)
