"""The decision table for an open PR: given what the PR looks like, what should the workflow do next?

This is a pure function so the workflow can call it directly and tests can enumerate every case.
"""

from __future__ import annotations

from .models import Feedback, PrSnapshot

DONE, CLOSED, MERGE, REVISE, ESCALATE, WAIT = "DONE", "CLOSED", "MERGE", "REVISE", "ESCALATE", "WAIT"
RESET = "RESET"  # move the branch back to the head a person accepted


def escalation(reason: str, snapshot: PrSnapshot) -> str:
    """The identity of an escalation: the same reason for the same head is raised with a human only once."""
    return f"{reason} {snapshot.head_sha}"


def decide(
    snapshot: PrSnapshot, handled: list[str], revisions: int, max_revisions: int, escalated: str | None = None
) -> tuple[str, Feedback | str | None]:
    """Return (action, detail): the Feedback to act on for REVISE, the reason for ESCALATE, the head to go back
    to for RESET, else None.

    A head a person accepted (`snapshot.accepted`) outranks everything on an open pull request: the branch is
    reset to it, then it is merged with no further approval, and no change request starts a revision.

    Pending change requests win over an approval while revisions remain; once the limit is hit, a pending
    request escalates to a human instead, approved or not: an approval never outranks a request someone else
    still holds (the activity already dropped requests their own author withdrew by approving, spec
    autonomous-gates §3). An escalation already raised (`escalated`) is a WAIT: the PR is in a human's hands
    until something about it changes."""

    def escalate(reason: str) -> tuple[str, str | None]:
        return (WAIT, None) if escalation(reason, snapshot) == escalated else (ESCALATE, reason)

    if snapshot.state == "merged":
        return DONE, None
    if snapshot.state == "closed":
        return CLOSED, None
    if snapshot.accepted:
        if snapshot.head_sha != snapshot.accepted:
            return RESET, snapshot.accepted
        if snapshot.checks in ("success", "none"):
            return MERGE, None
        return escalate("accepted, but checks are failing") if snapshot.checks == "failure" else (WAIT, None)
    pending = [item for item in snapshot.feedback if item.id not in handled]
    if pending and revisions < max_revisions:
        return REVISE, pending[0]
    if pending and snapshot.approved:
        return escalate(f"approved with unaddressed change requests (revision limit {max_revisions} reached)")
    if snapshot.approved:
        if snapshot.checks in ("success", "none"):
            return MERGE, None
        if snapshot.checks == "failure":
            return escalate("approved, but checks are failing")
    if pending:
        return escalate(f"revision limit ({max_revisions}) reached")
    return WAIT, None
