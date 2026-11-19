"""The cleaner's rules (spec cleaner §4.3 to §4.5): what is a problem of the installation, what counts as stuck,
and what is done about each. Pure: every argument is what a route or a client returned, so each rule has a
fixture. `mia_buffers.sweep` gathers the evidence and carries the steps out."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import datetime

from mia_data.models import RERUNNERS

from .config import CleanerCfg

ALL_KINDS = ("dataset", "buffer", "prep")
SNIPPETS = "snippets"  # not a subject kind: what a dataset round waits on while autodev writes its reader
# every workflow type a worker of this repository registers; an open run of any other type has no worker
WORKFLOW_TYPES = frozenset(
    {
        "IssueWorkflow",
        "PollWorkflow",
        "MiaDataDataset",
        "MiaDataPoll",
        "MiaMirror",
        "MiaBuffer",
        "MiaPrep",
        "MiaCacheKeeper",
        "MiaCleaner",
    }
)
ACTORS = RERUNNERS  # whose actions are not a person's: everyone else's keep the cleaner off
RETRYING_ATTEMPTS = 10  # an activity past this attempt is not going to pass by itself soon
YOUNG_ATTEMPTS = 3  # an activity on one of its first attempts is work in flight
SCHEDULE_MISSES = 3  # a schedule this many intervals late has stopped firing
SERVICE_RE = re.compile(
    r"timed? ?out|timeout|connection|connect error|temporarily|unavailable|rate limit|\b(429|500|502|503|504)\b",
    re.IGNORECASE,
)
NOT_REGISTERED_RE = re.compile(r"not registered", re.IGNORECASE)
ENTRY_NOISE_RE = re.compile(r"(?s:<!--.*?-->)|^\*\*(Stage|Result|Folders|Jobs):\*\*[^\n]*$", re.MULTILINE)
SEEN_CHARS = 600  # how much of a stop's own words a case quotes
# what a stop says, by the remedy it calls for (spec §4.5); the first that matches decides
BUDGET_RE = re.compile(r"root budget: (.*?max_\w+ \S+)")  # the refusal of `Ledger.reserve`, to its cap
HOSTS_RE = re.compile(r"host not allowed: ([^)\n]+)")
RECORD_GAP_RE = re.compile(r"match blocked:")
REVISION_LIMIT_RE = re.compile(r"revision limit", re.IGNORECASE)
CONFLICT_RE = re.compile(r"conflict|not mergeable", re.IGNORECASE)
RUN_FAILED_RE = re.compile(r"agent job \S+ ended|snippets: (stopped|failed)|made no commits", re.IGNORECASE)
TIME_LIMIT_RE = re.compile(r"exit 140|time limit|TERM_RUNLIMIT", re.IGNORECASE)
PASSING_RE = re.compile(
    r"timed? ?out|timeout|connection (reset|aborted|refused)|reset by peer|truncated|incomplete ?read|"
    r"temporarily unavailable|rate limit|job (was )?lost|\b(403|429|500|502|503|504)\b",
    re.IGNORECASE,
)
WORKER_ERROR_RE = re.compile(r"\b[A-Z][A-Za-z]*(Error|Exception)\b")
NEEDS_HUMAN = "snippets:needs-human"  # how a round that stopped on autodev's label says so
SECOND_RETRY = 6  # the second run after a passing cause waits this many times as long as the first
REVISE_TEXT = "/revise Merge `main` into this branch and resolve the conflicts. Change nothing else."

STEP_WORDS = {
    "cancel": "cancel the round",
    "stop": "force-stop the run",
    "kill_jobs": "end the round's cluster jobs",
    "go": "run it again from the beginning",
    "resume": "resume it",
    "recheck": "tell the request to look at its datasets again",
    "terminate": "terminate the run",
    "kill_job": "kill the job",
    "snippets_go": "ask autodev to try again",
    "revise": "ask autodev to merge main into the pull request",
    "await_autodev": "wait until autodev has taken it",
    "close": "close the snippet pull request and issue",
    "wait": "wait",
    "diagnose": "diagnose it",
    "report_defect": "report the defect",
}


def plan_words(plan: list[dict]) -> str:
    """A plan as a person reads it: "cancel the round, then run it again"."""
    return ", then ".join(step.get("text") or STEP_WORDS.get(step["do"], step["do"]) for step in plan) or "nothing"


def _age(seconds: float) -> str:
    return f"{seconds / 3600:.1f} h" if seconds >= 5400 else f"{seconds / 60:.0f} min"


def _at(entry: dict) -> float:
    return datetime.fromisoformat(entry["at"]).timestamp()


# --- the installation (spec §4.3) ------------------------------------------------------------------------------


def service_findings(
    health: dict,
    workers: dict,
    schedules: list[dict],
    checkout: str | None,
    pause: bool,
    queue_kinds: dict[str, list[str]],
    now: float,
) -> list[dict]:
    """What is wrong with the installation, each {id, text, kinds}: `kinds` are the subject kinds (and SNIPPETS)
    the cleaner leaves alone until the finding clears. `queue_kinds` maps a task queue (a service's name) to
    the kinds whose rounds its worker runs; `checkout` is the digest of the checkout's code now, which each
    worker's `code` is compared with."""
    findings = []
    if not health.get("temporal"):
        return [
            {"id": "temporal", "text": "Temporal does not answer: no round can move", "kinds": [*ALL_KINDS, SNIPPETS]}
        ]
    for queue, polled in sorted(health.get("queues", {}).items()):
        if not polled:
            text = f"no worker polls the task queue `{queue}`: the service is down"
            findings.append({"id": f"worker {queue}", "text": text, "kinds": queue_kinds.get(queue, [])})
    for service, row in sorted(workers.items()):
        code = row.get("code")
        if code and checkout and code != checkout and health.get("queues", {}).get(service, True):
            text = (
                f"the `{service}` worker runs the code of commit `{(row.get('commit') or '?')[:7]}`, and the "
                f"checkout's code has changed since: a restart is owed (`mia restart {service}`)"
            )
            findings.append({"id": f"restart {service}", "text": text, "kinds": queue_kinds.get(service, [])})
    for schedule in schedules:
        every, last = schedule.get("every_s"), schedule.get("last")
        if every and last and now - last > SCHEDULE_MISSES * every:
            text = f"the schedule `{schedule['id']}` last fired {_age(now - last)} ago and is due every {_age(every)}"
            findings.append({"id": f"schedule {schedule['id']}", "text": text, "kinds": []})
    if pause:
        text = "`autonomy.pause` is set: every round stops at its next job"
        findings.append({"id": "pause", "text": text, "kinds": [*ALL_KINDS, SNIPPETS]})
    return findings


def evidence_findings(evidence: list[dict], pend_hours: float, now: float, cases: Sequence[dict] = ()) -> list[dict]:
    """The findings that only show across subjects: workflow tasks failing on more than one run of a kind (the
    code and the histories disagree), a service that refuses several rounds at once, and jobs the cluster has
    kept pending for `pend_hours`. `cases`: the cases so far, whose `frozen run`s of the last day count too,
    since runs frozen by one cause come due in different passes."""
    findings = []
    day = now - 86400
    for kind in ALL_KINDS:
        frozen = [item["subject"]["key"] for item in evidence if item["subject"]["kind"] == kind and _frozen(item)]
        frozen += [
            case["key"]
            for case in cases
            if case["kind"] == kind
            and case["rule"] == "frozen run"
            and case["opened"] >= day
            and case["key"] not in frozen
        ]
        if len(frozen) > 1:
            text = (
                f"workflow tasks fail on {len(frozen)} {kind} runs ({', '.join(frozen[:5])}): code and history disagree"
            )
            findings.append({"id": f"tasks {kind}", "text": text, "kinds": [kind]})
    refused = [failure for item in evidence for failure in _retrying(item, SERVICE_RE)]
    if len(refused) > 1:
        text = f"a service refuses {len(refused)} rounds at once; the last answer: {refused[0][:200]}"
        findings.append({"id": "service", "text": text, "kinds": []})
    pending = [
        job["address"]
        for item in evidence
        for job in item.get("jobs", ())
        if job.get("state") == "PENDING" and now - job.get("submitted", now) > pend_hours * 3600
    ]
    if pending:
        text = (
            f"{len(pending)} jobs have waited in the cluster queue for over {pend_hours:g} h ({', '.join(pending[:3])})"
        )
        findings.append({"id": "cluster queue", "text": text, "kinds": []})
    return findings


def _frozen(item: dict) -> bool:
    return bool((item.get("run") or {}).get("task"))


def _retrying(item: dict, pattern: re.Pattern[str] | None = None) -> list[str]:
    """The last failures of the item's activities that have retried past RETRYING_ATTEMPTS (those matching
    `pattern`, when given)."""
    return [
        pending["failure"]
        for pending in (item.get("run") or {}).get("activities", ())
        if pending["attempt"] > RETRYING_ATTEMPTS and (pattern is None or pattern.search(pending["failure"]))
    ]


# --- one subject (spec §4.4) -----------------------------------------------------------------------------------


def stop_entry(entries: list[dict], stage: str) -> dict | None:
    """The entry that moved the subject to the stage it rests at."""
    return next((entry for entry in reversed(entries) if entry.get("stage") == stage), None)


def entry_text(entry: dict) -> str:
    """An entry's own words: without its header lines and markers, on one line, cut to SEEN_CHARS."""
    text = " ".join(ENTRY_NOISE_RE.sub("", entry.get("body") or "").split())
    return text if len(text) <= SEEN_CHARS else text[:SEEN_CHARS].rstrip() + "…"


def last_person(actions: list[dict]) -> float | None:
    """When somebody other than the cleaner or the debugger last acted on the subject."""
    times = [datetime.fromisoformat(action["at"]).timestamp() for action in actions if action["actor"] not in ACTORS]
    return max(times, default=None)


def standing(view: dict) -> str:
    """Where a subject stands, for a decision about it: `shelved` when a person set it aside, else the phase of
    its stage. No decision reads the status a person is shown (spec overview §3.1)."""
    return "shelved" if view.get("shelved") else view["phase"]


def sighting(item: dict, cfg: CleanerCfg, now: float) -> dict | None:
    """Whether the subject is stuck, and how: {what: quiet | stopped, rule, seen}; None when it is not. `item`
    is the subject's evidence: {subject, entries, actions, run, jobs, party}. A rule of None is for the
    diagnosis."""
    subject, entries = item["subject"], item.get("entries") or []
    status, stage = standing(subject), subject["stage"]
    if status == "working":
        since = max((_at(entry) for entry in entries), default=subject.get("updated") or now)
        if now - since < cfg.quiet_minutes * 60:
            return None
        return _quiet(item, f"at `{stage}` with no write for {_age(now - since)}")
    if status == "stopped":
        stop = stop_entry(entries, stage)
        since = _at(stop) if stop else subject.get("updated") or now
        if now - since < cfg.stopped_minutes * 60:
            return None
        said = entry_text(stop) if stop else ""
        if subject["kind"] == "prep" and (item.get("party") or {}).get("settled"):
            seen = f"at `{stage}` for {_age(now - since)} though each of its datasets is prepared or shelved"
            return {"what": "stopped", "rule": "request behind", "seen": seen}
        seen = f"stopped at `{stage}` {_age(now - since)} ago" + (f": {said}" if said else "")
        return {"what": "stopped", "rule": stop_rule(item, said, since), "seen": seen}
    return None  # a person's turn, done, not started, shelved


def _quiet(item: dict, where: str) -> dict | None:
    """A working subject with no write for the quiet time: which state repair it is, None when something is
    still going on for it, a rule of None when nothing is and no rule says why."""
    run, jobs, party = item.get("run"), item.get("jobs") or [], item.get("party") or {}
    if run is None or run.get("status") != "RUNNING":
        return {"what": "quiet", "rule": "no run", "seen": f"{where}, and Temporal has no open run for it"}
    if run.get("task"):
        seen = f"{where}: its workflow task has failed {run['task']} times in a row, so the run cannot move"
        return {"what": "quiet", "rule": "frozen run", "seen": seen}
    if retrying := [pending for pending in run.get("activities", ()) if pending["attempt"] > RETRYING_ATTEMPTS]:
        worst = max(retrying, key=lambda pending: pending["attempt"])
        seen = f"{where}: its activity `{worst['type']}` is on attempt {worst['attempt']}: {worst['failure'][:300]}"
        return {"what": "quiet", "rule": "activity retrying", "seen": seen, "activity": worst}
    if any(job.get("live") for job in jobs):
        return None  # a job of its round is pending or running
    if any(pending["attempt"] <= YOUNG_ATTEMPTS for pending in run.get("activities", ())):
        return None  # an activity in flight
    if party.get("working"):
        return None  # who it waits for is at work
    return {
        "what": "quiet",
        "rule": None,
        "seen": f"{where}: its run is open, and no job, activity or party is at work",
    }


def stop_rule(item: dict, said: str, since: float) -> str | None:
    """Which remedy a stop calls for, from what it said, the hosts its jobs were refused (the subject's `hosts`),
    the snippets issue it waited on (`party`) and when the worker of its rounds started on its present commit
    (`worker`); None when no rule covers it."""
    party, worker = item.get("party") or {}, item.get("worker") or {}
    if item["subject"]["kind"] != "dataset":
        return None
    if party.get("merged") and NEEDS_HUMAN in said:  # the round gave up on a pull request that merged afterwards
        return "merged since"
    if party.get("stopped") and party.get("state") == "open":  # autodev stopped on the snippets issue
        words = f"{said} {party.get('last_word') or ''}"
        if party.get("pr") and (party.get("mergeable") is False or CONFLICT_RE.search(words)):
            return "merge conflict"
        if REVISION_LIMIT_RE.search(words):
            return "revision limit"
        if not party.get("pr") and RUN_FAILED_RE.search(words):
            return "autodev's run failed"
        return None
    if any(host in said for host in item["subject"].get("hosts") or ()):  # a host its jobs were refused, unapproved
        return "host not approved"
    for rule, pattern in (
        ("budget used up", BUDGET_RE),  # first: its numbers read like a passing cause's
        ("host not approved", HOSTS_RE),
        ("record gap", RECORD_GAP_RE),
        ("revision limit", REVISION_LIMIT_RE),
        ("time limit", TIME_LIMIT_RE),
        ("passing cause", PASSING_RE),
    ):
        if pattern.search(said):
            return rule
    if WORKER_ERROR_RE.search(said) and (worker.get("since") or 0) > since:
        return "fixed since"
    return None


def leftover_reason(view: dict | None, labels: set[str], newer: bool, leftover_days: float, now: float) -> str | None:
    """Why an open snippets issue of ours is left over, given its dataset's subject (`view`), the issue's labels
    and whether the dataset has opened a newer one; None when it is not, or autodev is at work on it."""
    if view is None or "snippets:working" in labels:
        return None
    if newer:
        return "its dataset has opened a newer snippets issue"
    if view.get("shelved"):
        return "its dataset is shelved"
    if view["stage"] == "done":
        return "its dataset is prepared through another reader"
    rested = now - (view.get("updated") or now)
    if view["phase"] in ("stopped", "done") and rested >= leftover_days * 86400:
        return f"its dataset has rested at `{view['stage']}` for {rested / 86400:.0f} days"
    return None


# --- what is done (spec §4.5) ----------------------------------------------------------------------------------


def plan_for(found: dict, item: dict) -> list[dict]:
    """The steps for a sighting, in order. A step is {do, ...}; `needs` ends a plan with what a person must do.

    A subject is run again with a `resume`, which keeps what its round has (spec resume-restart §9): a dataset
    and a request take one, a buffer is planned again with a `go`. A `go` stays where the remedy needs more
    than the round's pins hold: a changed record, a pull request the plan closed."""
    rule, kind = found["rule"], item["subject"]["kind"]
    again = {"do": "go" if kind == "buffer" else "resume"}
    if rule == "no run":
        return [{"do": "cancel"}, {"do": "kill_jobs"}, again]
    if rule == "frozen run":
        if kind == "buffer":  # a stop retires every buffer of the spec, also one that is served: a person's call
            text = "its run is frozen: Force stop on its page ends it and retires the spec's buffers, then Plan again"
            return [{"do": "needs", "text": text}]
        return [{"do": "stop"}, {"do": "kill_jobs"}, again]
    if rule == "request behind":
        return [{"do": "recheck"}]
    party = item.get("party") or {}
    earlier = [case for case in item.get("earlier") or () if case["rule"] == rule]
    if rule == "autodev's run failed":
        issue = party["issue"]
        tried = [step for case in earlier for step in case["data"].get("steps", ())]
        if any(step["do"] == "snippets_go" and step.get("issue") == issue for step in tried):  # once per issue
            return [{"do": "diagnose"}]
        return [{"do": "snippets_go", "issue": issue}, {"do": "await_autodev", "issue": issue}, again]
    if rule == "merge conflict":
        issue, pr = party["issue"], party["pr"]
        if REVISION_LIMIT_RE.search(party.get("last_word") or ""):  # no revision left: start over from the match
            why = "the pull request cannot be merged and autodev has no revision left"
            return [{"do": "close", "issue": issue, "pr": pr, "why": why}, {"do": "go"}]
        return [{"do": "revise", "issue": issue, "pr": pr}, {"do": "await_autodev", "issue": issue}, again]
    if rule == "merged since":
        if any((case["data"].get("party") or {}).get("pr") == party.get("pr") for case in earlier):
            return [{"do": "diagnose"}]  # once per pull request
        return [again]  # the round finds the merge: the crops that were approved are saved, or the merge's own made
    if rule == "passing cause":
        if len(earlier) >= 2:  # at most twice
            return [{"do": "diagnose"}]
        return [{"do": "wait", "hours": item["retry_hours"] * (SECOND_RETRY if earlier else 1)}, again]
    if rule == "fixed since":
        commit = (item.get("worker") or {}).get("commit")
        if any(case["data"].get("commit") == commit for case in earlier):  # once per commit
            return [{"do": "diagnose"}]
        return [again]
    if rule == "budget used up":
        refusal = BUDGET_RE.search(found["seen"])
        named = refusal.group(1) if refusal else "see its page"
        text = (
            f"its budget is used up ({named}): raise that cap (`autonomy.root`) and restart the workers, then resume it"
        )
        return [{"do": "needs", "text": text}]
    if rule == "host not approved":
        hosts = HOSTS_RE.search(found["seen"])
        waiting = ", ".join(item["subject"].get("hosts") or ())  # what Resume lists to check
        named = hosts.group(1).strip() if hosts else waiting or "see its page"
        text = f"approve the hosts its jobs need ({named}): press Resume on its page and check them"
        return [{"do": "needs", "text": text}]
    if rule == "record gap":
        text = "its catalog record needs a decision or a correction; the dataset is restarted once the record changes"
        return [{"do": "needs", "text": text}]
    if rule == "input changed":
        return [{"do": "go"}]
    if rule == "activity retrying":
        failure = found["activity"]["failure"]
        if NOT_REGISTERED_RE.search(failure):
            text = f"restart the worker that runs this {kind}'s rounds: its activity `{found['activity']['type']}` is not registered on it"
            return [{"do": "needs", "text": text}]
        if SERVICE_RE.search(failure):  # it may pass by itself: look again in an hour
            return [{"do": "wait", "hours": 1.0}, {"do": "diagnose"}]
    return [{"do": "diagnose"}]
