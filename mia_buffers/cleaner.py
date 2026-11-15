"""The cleaner's rules (spec cleaner §4.3 to §4.5): what is a problem of the installation, what counts as stuck,
and what is done about each. Pure: every argument is what a route or a client returned, so each rule has a
fixture. `mia_buffers.sweep` gathers the evidence and carries the steps out."""

from __future__ import annotations

import re
from datetime import datetime

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
ACTORS = ("cleaner", "debugger")  # whose actions are not a person's: everyone else's keep the cleaner off
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

STEP_WORDS = {
    "cancel": "cancel the round",
    "stop": "force-stop the run",
    "kill_jobs": "end the round's cluster jobs",
    "go": "run it again",
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
    the kinds whose rounds its worker runs."""
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
        commit = row.get("commit")
        if commit and checkout and commit != checkout and health.get("queues", {}).get(service, True):
            text = (
                f"the `{service}` worker runs commit `{commit[:7]}` and the checkout is at `{checkout[:7]}`: "
                f"a restart is owed (`mia restart {service}`)"
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


def evidence_findings(evidence: list[dict], pend_hours: float, now: float) -> list[dict]:
    """The findings that only show across subjects: workflow tasks failing on more than one run of a kind (the
    code and the histories disagree), a service that refuses several rounds at once, and jobs the cluster has
    kept pending for `pend_hours`."""
    findings = []
    for kind in ALL_KINDS:
        frozen = [item["subject"]["key"] for item in evidence if item["subject"]["kind"] == kind and _frozen(item)]
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


def sighting(item: dict, cfg: CleanerCfg, now: float) -> dict | None:
    """Whether the subject is stuck, and how: {what: quiet | stopped, rule, seen}; None when it is not. `item`
    is the subject's evidence: {subject, entries, actions, run, jobs, party}. A rule of None is for the
    diagnosis."""
    subject, entries = item["subject"], item.get("entries") or []
    status, stage = subject["status"], subject["stage"]
    if status == "working":
        since = max((_at(entry) for entry in entries), default=subject.get("updated") or now)
        if now - since < cfg.quiet_minutes * 60:
            return None
        return _quiet(item, f"at `{stage}` with no write for {_age(now - since)}")
    if status == "stuck":
        stop = stop_entry(entries, stage)
        since = _at(stop) if stop else subject.get("updated") or now
        if now - since < cfg.stopped_minutes * 60:
            return None
        said = entry_text(stop) if stop else ""
        if subject["kind"] == "prep" and (item.get("party") or {}).get("settled"):
            seen = f"at `{stage}` for {_age(now - since)} though each of its datasets is prepared or shelved"
            return {"what": "stopped", "rule": "request behind", "seen": seen}
        return {
            "what": "stopped",
            "rule": None,
            "seen": f"stopped at `{stage}` {_age(now - since)} ago" + (f": {said}" if said else ""),
        }
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


# --- what is done (spec §4.5) ----------------------------------------------------------------------------------


def plan_for(found: dict, item: dict) -> list[dict]:
    """The steps for a sighting, in order. A step is {do, ...}; `needs` ends a plan with what a person must do."""
    rule, kind = found["rule"], item["subject"]["kind"]
    if rule == "no run":
        return [{"do": "cancel"}, {"do": "kill_jobs"}, {"do": "go"}]
    if rule == "frozen run":
        return [{"do": "stop"}, {"do": "kill_jobs"}, {"do": "go"}]
    if rule == "request behind":
        return [{"do": "recheck"}]
    if rule == "activity retrying":
        failure = found["activity"]["failure"]
        if NOT_REGISTERED_RE.search(failure):
            text = f"restart the worker that runs this {kind}'s rounds: its activity `{found['activity']['type']}` is not registered on it"
            return [{"do": "needs", "text": text}]
        if SERVICE_RE.search(failure):
            return [{"do": "wait", "hours": 1.0}]
    return [{"do": "diagnose"}]
