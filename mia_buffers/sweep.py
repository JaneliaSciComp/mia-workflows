"""One pass of the cleaner (spec cleaner §4): read the installation, name what is wrong with it, find the
subjects that are stuck, open a case for each and carry every open case as far as it goes.

Runs as the `sweep` activity (`Activities.sweep`) and uses the activity object's clients. The rules are in
`mia_buffers.cleaner`; this module does the reading and the doing. A case is a row of the ledger that holds
its remaining steps, so a pass that fails half way leaves cases the next pass continues: every step that
changes something carries a key, and a step that has to wait returns None and is tried again next time."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from temporalio import activity

from mia_core import autonomy
from mia_core.checkout import code_digest
from mia_core.config import manifest_path
from mia_core.jobrecord import address, job_link, open_records, round_records
from mia_core.judge.evidence import read_claude_json
from mia_core.registry import RegistryError
from mia_core.untrusted import neutralize
from mia_data.models import CLEANER, CLEANER_ADVISED

from .cleaner import (
    ALL_KINDS,
    REVISE_TEXT,
    SNIPPETS,
    WORKFLOW_TYPES,
    evidence_findings,
    last_person,
    leftover_reason,
    plan_for,
    plan_words,
    service_findings,
    sighting,
    standing,
)
from .diagnose import diagnose_job, parse_outcome, reduce_outcome, workspace_of, write_evidence

if TYPE_CHECKING:
    from .activities import Activities

log = logging.getLogger(__name__)

ACTIVE = ("open", "working", "waiting")  # the states of a case that is not over
ACTIONS = ("cancel", "stop", "go", "resume", "recheck")  # steps sent to buffer-api's action routes
RUNS = ("go", "resume")  # the two that run a subject again: from scratch, or what is left of its round
STARTS_WORK = (*RUNS, "snippets_go", "revise")  # steps the weekly limit counts
CASE_DAYS = 30  # how far back a subject's earlier cases are read
STEP_WAIT_S = 2 * 3600  # a step that is still not possible after this long ends its case
ROUND_ENDED_S = 600  # a job still alive this long after its round ended is left over
RECENT_S = 86400  # how long after a round ended its jobs are looked for
ISSUE_RE = re.compile(r"mia-snippets issue #(\d+)")
PULL_RE = re.compile(r"/pull/(\d+)")
BRANCH_RE = re.compile(r"snippets/issue-(\d+)")
OURS_RE = re.compile(r"<!-- mia-data: issue=(\d+) round=")  # the first line of a snippets issue a round opened
LAST_WORD_CHARS = 400
DEVELOP_STAGES = ("developing", "testing", "merging")  # where a dataset round may wait on autodev
ACTION_PATHS = {
    "dataset": "/datasets/{key}/actions",
    "buffer": "/specs/{key}/actions",
    "prep": "/prep/{number}/actions",
}


class Sweep:
    """One pass. `now` is the pass's clock; every decision of the pass uses it."""

    def __init__(self, activities: Activities, now: float | None = None) -> None:
        self.activities, self.cfg, self.rules = activities, activities.cfg, activities.cfg.cleaner
        self.registry, self.now = activities.registry, now or time.time()
        self.report: dict[str, Any] = {"mode": self.rules.mode, "findings": [], "opened": [], "did": [], "errors": []}
        self.cases: list[dict] = []
        self.working = 0  # cases in work, against `max_parallel`
        self.workers: dict = {}  # service -> {commit, at, since}
        self.present: set[tuple[str, str]] = set()  # the left-over things this pass saw: (case kind, key)

    async def _ask(self, call: Any, *args: Any, **fields: Any) -> Any:
        """A registry call, off the event loop (the client is synchronous)."""
        return await asyncio.to_thread(call, *args, **fields)

    # --- the pass ----------------------------------------------------------------------------------------------

    async def run(self) -> dict:
        registry, rules, now = self.registry, self.rules, self.now
        if rules.mode == "off":
            return self.report
        findings = await self._service_findings()
        skipped = {kind for finding in findings for kind in finding["kinds"]}
        self.report["findings"] = findings
        if any(finding["id"] == "temporal" for finding in findings):
            return await self._done()
        self.cases = await self._ask(registry.cases, days=CASE_DAYS)
        self.working = sum(case["state"] == "working" for case in self.cases)
        views = [view for kind in ALL_KINDS for view in await self._ask(registry.subjects, kind)]
        evidence: list[dict] = []
        for view in views:
            if view["kind"] not in skipped and self._due(view):
                await self._try(view["key"], self._gather(view, skipped, evidence))
        late = evidence_findings(evidence, rules.pend_hours, now, self.cases)
        findings.extend(late)
        skipped |= {kind for finding in late for kind in finding["kinds"]}
        for item in evidence:
            if item["subject"]["kind"] not in skipped and (found := sighting(item, rules, now)):
                await self._try(item["subject"]["key"], self._sighted(item, found))
        if not any(finding["id"].startswith(("restart ", "worker ")) for finding in findings):
            await self._try("open runs", self._unrunnable())  # a worker on old code does not know every type
        for view in views:
            await self._try(view["key"], self._left_jobs(view))
        by_subject = {(view["kind"], view["number"]): view for view in views}
        if SNIPPETS not in skipped:
            await self._try("snippet work", self._left_snippets(by_subject))
        for case in [case for case in self.cases if case["state"] == "needs-person" and case["rule"] == "record gap"]:
            await self._try(case["key"], self._record_changed(case, by_subject))
        for case in [case for case in self.cases if case["state"] in ACTIVE]:
            await self._try(case["key"], self._advance(case, by_subject, skipped))
        return await self._done()

    async def _done(self) -> dict:
        self.report["cases"] = sum(case["state"] in ACTIVE for case in self.cases)
        await self._try("report", self._ask(self.registry.put_cleaner, self.report))
        return self.report

    async def _try(self, what: str, work: Any) -> None:
        """One subject's or one case's part of the pass: its failure is the report's, never the pass's."""
        if activity.in_activity():
            activity.heartbeat(what)  # a pass that outlives its timeout is cancelled here, before the next begins
        try:
            await work
        except Exception as error:
            log.warning("cleaner: %s failed: %s", what, error, exc_info=not isinstance(error, RegistryError))
            self.report["errors"].append(f"{what}: {type(error).__name__}: {error}"[:500])

    async def _service_findings(self) -> list[dict]:
        registry, cfg = self.registry, self.cfg
        health = await self._ask(registry.health)
        if not health.get("temporal"):
            return service_findings(health, {}, [], None, False, {}, self.now)
        queue_kinds = {queue: [SNIPPETS] for queue in cfg.workers}
        queue_kinds |= {cfg.datasets_pipeline.task_queue: ["dataset"], cfg.task_queue: ["buffer", "prep"]}
        self.workers = workers = await self._ask(registry.workers)
        schedules = await self._ask(registry.schedules)
        checkout = await asyncio.to_thread(code_digest)
        pause = await asyncio.to_thread(autonomy.paused, manifest_path())
        return service_findings(health, workers, schedules, checkout, pause, queue_kinds, self.now)

    # --- evidence ----------------------------------------------------------------------------------------------

    def _cases_of(self, kind: str, number: int | None, key: str | None = None) -> list[dict]:
        return [
            case
            for case in self.cases
            if case["kind"] == kind and case["number"] == number and (key is None or case["key"] == key)
        ]

    def _due(self, view: dict) -> bool:
        """Whether the subject is worth its evidence: working or stuck for long enough, and not looked at
        already as it stands (a case of its revision exists)."""
        age = self.now - (view.get("updated") or self.now)
        if standing(view) == "working":
            old = age >= self.rules.quiet_minutes * 60
        elif standing(view) == "stopped":
            old = age >= self.rules.stopped_minutes * 60
        else:
            return False
        seen = any(
            case["data"].get("revision") == view["revision"] for case in self._cases_of(view["kind"], view["number"])
        )
        return old and not seen

    async def _gather(self, view: dict, skipped: set[str], evidence: list[dict]) -> None:
        timeline = await self._ask(self.registry.timeline, view["kind"], view["number"])
        if timeline is not None:
            evidence.append(await self._evidence(timeline, skipped))

    async def _evidence(self, timeline: dict, skipped: set[str]) -> dict:
        """The subject's evidence: its timeline and actions, and for a working one its run, the jobs of its
        round and the party it waits for."""
        view = timeline["subject"]
        kind, number = view["kind"], view["number"]
        item = {"subject": timeline["subject"], "entries": timeline["entries"], "actions": timeline["actions"]}
        item["round"] = next((entry["round"] for entry in reversed(item["entries"]) if entry.get("round")), None)
        if standing(view) == "working":
            item["run"] = await self._run(kind, number)
            item["jobs"] = await self._jobs(number, item["round"])
        if standing(view) == "working" or kind in ("prep", "dataset"):
            item["party"] = await self._party(item, skipped)
        queue = self.cfg.datasets_pipeline.task_queue if kind == "dataset" else self.cfg.task_queue
        item |= {"worker": self.workers.get(queue), "retry_hours": self.rules.retry_hours}
        item["earlier"] = self._cases_of(kind, number)
        return item

    async def _run(self, kind: str, number: int) -> dict | None:
        run = await self._ask(self.registry.run, kind, number)
        return run if run.get("status") else None

    async def _jobs(self, number: int, round_id: str | None) -> list[dict]:
        """The jobs of the round whose records have no end, each {address, job_id, live, state, submitted}. A
        job the cluster cannot be asked about counts as alive."""
        if not round_id or self.activities.jobs is None:
            return []
        records = await asyncio.to_thread(open_records, self.cfg.paths.work_root, f"issue-{number}/{round_id}")
        jobs = []
        for record in records:
            job_id, state, live = str(record.get("job_id")), "UNKNOWN", True
            try:
                statuses = await asyncio.to_thread(self.activities.jobs.client.status, [job_id])
                live = bool(statuses) and not statuses[0].terminal
                state = statuses[0].state if statuses else "GONE"
            except Exception as error:
                log.warning("cleaner: csub status of job %s failed: %s", job_id, error)
            submitted = _epoch(record.get("submitted_at"))
            jobs.append(
                {"address": record["address"], "job_id": job_id, "live": live, "state": state, "submitted": submitted}
            )
        return jobs

    async def _party(self, item: dict, skipped: set[str]) -> dict | None:
        """Who the working subject waits for, and whether they are at work: autodev for a dataset whose round
        waits on a snippet, its datasets for a request. None when it waits for nobody."""
        subject = item["subject"]
        if subject["kind"] == "dataset" and (subject["stage"] in DEVELOP_STAGES or standing(subject) == "stopped"):
            issue = await self._snippet_issue(item)
            if issue is None:
                return None
            if SNIPPETS in skipped:
                return {"issue": issue, "working": True}  # autodev's worker is away: not this round's fault
            return await self._snippets(issue, subject["number"])
        if subject["kind"] == "prep":
            datasets = await self._request_datasets(subject)
            return {
                "working": not datasets or any(standing(view) in ("working", "waiting") for view in datasets),
                "settled": bool(datasets)
                and all(standing(view) == "shelved" or view["stage"] == "done" for view in datasets),
            }
        return None

    async def _snippet_issue(self, item: dict) -> int | None:
        """The snippets issue the subject's last round waited on: the one its entries name, else the one behind
        the pull request they link."""
        said = " ".join(entry.get("body") or "" for entry in item["entries"] if entry.get("round") == item["round"])
        if named := ISSUE_RE.findall(said):
            return int(named[-1])
        if pulls := PULL_RE.findall(said):
            pull = await self.activities.gh_snippets.get_pr(int(pulls[-1]))
            branch = BRANCH_RE.fullmatch((pull.get("head") or {}).get("ref") or "")
            return int(branch.group(1)) if branch else None
        return None

    async def _snippets(self, number: int, dataset: int) -> dict | None:
        """The snippets issue as the rules read it: {issue, state, working, stopped, pr, mergeable, merged,
        last_word}. `pr` is its open pull request, or the one autodev merged when the issue is closed.
        `last_word` is autodev's last comment on the issue or its pull request, on one line. None when the issue
        is not the one a round of this dataset opened: its number came from a timeline entry, which quotes what
        jobs and agents wrote."""
        gh, bot = self.activities.gh_snippets, self.activities.bot
        issue = await gh.get_issue(number)
        ours = OURS_RE.match(issue.get("body") or "")
        if not ours or int(ours.group(1)) != dataset or (issue.get("user") or {}).get("login") != bot:
            return None
        labels = {label["name"] for label in issue.get("labels", [])}
        found = await gh.find_pr(f"snippets/issue-{number}")
        if found is None and issue.get("state") == "closed":  # autodev closes the issue when it has merged
            found = await gh.find_pr(f"snippets/issue-{number}", state="closed")
        pull = await gh.get_pr(found["number"]) if found else None
        comments = await gh.issue_comments(number) + (await gh.issue_comments(pull["number"]) if pull else [])
        said = [
            comment
            for comment in comments
            if (comment.get("user") or {}).get("login") == bot and "<!-- mia" not in (comment.get("body") or "")
        ]
        last = max(said, key=lambda comment: comment.get("created_at") or "", default=None)
        return {
            "issue": number,
            "state": issue.get("state"),
            "working": "snippets:working" in labels,
            "stopped": "snippets:needs-human" in labels,
            "pr": pull["number"] if pull else None,
            "mergeable": pull.get("mergeable") if pull else None,
            "merged": bool(pull and pull.get("merged")),
            "last_word": neutralize(" ".join((last.get("body") or "").split()))[:LAST_WORD_CHARS] if last else "",
        }

    async def _request_datasets(self, subject: dict) -> list[dict]:
        """The dataset subjects a request names in its text (none for a request that selects them)."""
        from .spec import SpecError, load_prep_text

        try:
            records = load_prep_text((subject.get("data") or {}).get("spec") or "").records
        except SpecError:
            return []
        found = [await self._ask(self.registry.subject, "dataset", record) for record in records]
        return [view for view in found if view is not None]

    # --- opening cases -----------------------------------------------------------------------------------------

    async def _open(self, kind: str, number: int | None, key: str, rule: str, data: dict) -> dict:
        case = await self._ask(self.registry.open_case, kind=kind, number=number, key=key, rule=rule, data=data)
        self.cases.append(case)
        self.report["opened"].append(f"{key}: {rule}: {plan_words(data['plan'])}")
        return case

    async def _sighted(self, item: dict, found: dict) -> None:
        """A stuck subject: a case, unless a person acted on it lately or one is open already. A subject that is
        stuck again while its case waits for its remedy's outcome was not helped."""
        subject = item["subject"]
        kind, number = subject["kind"], subject["number"]
        if active := [case for case in self._cases_of(kind, number) if case["state"] in ACTIVE]:
            if active[0]["state"] == "waiting":
                await self._not_helped(active[0], found["seen"])
            return
        acted = last_person(item["actions"])
        if acted is not None and self.now - acted < self.rules.hands_off_hours * 3600:
            return
        data = {
            "seen": found["seen"],
            "what": found["what"],
            "revision": subject["revision"],
            "round": item.get("round"),
            "run_id": (item.get("run") or {}).get("run_id"),
            "plan": plan_for(found, item),
            "steps": [],
        }
        if (item.get("party") or {}).get("issue"):  # the snippets issue and pull request a remedy may name
            data["party"] = {"issue": item["party"]["issue"], "pr": item["party"].get("pr")}
        if found["rule"] == "fixed since":
            data["commit"] = (item.get("worker") or {}).get("commit")
        if found["rule"] == "record gap":
            data["record"] = await self._record_digest(subject["key"])
        await self._open(kind, number, subject["key"], found["rule"] or "diagnosis", data)

    async def _record_digest(self, record: str) -> str | None:
        """A digest of the dataset's record as the newest synced catalog has it; None when no catalog holds it."""
        try:
            found = await self._ask(self.registry.request, "GET", f"/catalog/datasets/{quote(record, safe='')}")
        except RegistryError:
            return None
        return hashlib.sha256(json.dumps(found.get("record"), sort_keys=True).encode()).hexdigest()

    async def _record_changed(self, case: dict, views: dict) -> None:
        """A dataset that waits for its record to be corrected (`record gap`) and whose record has changed since:
        its case is closed and a new one runs it again."""
        view = views.get((case["kind"], case["number"]))
        before, now = case["data"].get("record"), await self._record_digest(case["key"])
        if view is None or view["revision"] != case["data"].get("revision") or now in (None, before):
            return
        await self._save(case, "resolved", "its record changed")
        seen = f"{case['data']['seen']}. Its catalog record has changed since"
        data = {"seen": seen, "what": "stopped", "revision": view["revision"], "plan": [{"do": "go"}], "steps": []}
        await self._open(case["kind"], case["number"], case["key"], "input changed", data)

    async def _unrunnable(self) -> None:
        """Open runs of a `Mia…` workflow type no worker registers: each logs an error on every task and never
        ends."""
        for run in await self._ask(self.registry.runs):
            key = f"{run['namespace']}/{run['id']}"
            if run["type"] in WORKFLOW_TYPES or not run["type"].startswith("Mia"):
                continue  # ours by name alone: a namespace may hold Temporal's own runs and other people's
            self.present.add(("run", key))
            if self._cases_of("run", None, key):
                continue
            step = {"do": "terminate", "namespace": run["namespace"], "workflow_id": run["id"], "run_id": run["run_id"]}
            seen = f"an open run of the workflow type `{run['type']}`, which no worker registers"
            await self._open("run", None, key, "unrunnable run", {"seen": seen, "plan": [step], "steps": []})

    async def _left_jobs(self, view: dict) -> None:
        """Jobs still alive after their round ended (a round ended from outside kills none). Only where no job
        of the round can be wanted any more: a dataset that stopped, was cancelled or is prepared, a buffer spec
        that was cancelled or retired. A round at a person's turn and a serving buffer still run jobs."""
        ended = self.now - (view.get("updated") or self.now)
        over = {
            "dataset": standing(view) == "stopped" or view["stage"] in ("cancelled", "done"),
            "buffer": view["stage"] in ("cancelled", "retired"),
        }
        if not over.get(view["kind"]) or not ROUND_ENDED_S <= ended <= RECENT_S:
            return
        if any(case["state"] in ACTIVE for case in self._cases_of(view["kind"], view["number"])):
            return  # the subject's own case ends its round's jobs
        timeline = await self._ask(self.registry.timeline, view["kind"], view["number"])
        round_id = next((entry["round"] for entry in reversed(timeline["entries"]) if entry.get("round")), None)
        for job in await self._jobs(view["number"], round_id):
            if job["live"]:
                self.present.add(("job", job["address"]))
            if job["live"] and not self._cases_of("job", None, job["address"]):
                seen = f"job {job['job_id']} of `{view['key']}` is {job['state']} though its round ended"
                plan = [{"do": "kill_job", "job_id": job["job_id"], "address": job["address"]}]
                await self._open(
                    "job", None, job["address"], "left-over job", {"seen": seen, "plan": plan, "steps": []}
                )

    async def _left_snippets(self, views: dict) -> None:
        """Snippets issues and pull requests of ours that nobody follows any more (spec §4.5, leftovers): an open
        issue whose dataset is prepared another way, is shelved, opened a newer issue or has rested for
        `leftover_days`, while autodev is not at work on it; and an open pull request whose issue is closed."""
        gh, bot = self.activities.gh_snippets, self.activities.bot
        ours: list[tuple[int, int, dict]] = []  # (dataset number, issue number, issue)
        for issue in await gh.list_issues(state="open"):
            marker = OURS_RE.match(issue.get("body") or "")
            if marker and (issue.get("user") or {}).get("login") == bot:
                ours.append((int(marker.group(1)), issue["number"], issue))
        newest = {dataset: max(number for other, number, _ in ours if other == dataset) for dataset, _, _ in ours}
        for dataset, number, issue in ours:
            labels = {label["name"] for label in issue.get("labels", [])}
            view = views.get(("dataset", dataset))
            why = leftover_reason(view, labels, number != newest[dataset], self.rules.leftover_days, self.now)
            if why is None:
                continue
            self.present.add(("snippets", f"issue-{number}"))
            if not self._cases_of("snippets", None, f"issue-{number}"):
                pull = await gh.find_pr(f"snippets/issue-{number}")
                step = {"do": "close", "issue": number, "pr": pull["number"] if pull else None, "why": why}
                seen = f"snippets issue #{number} of `{view['key'] if view else dataset}` is open, and {why}"
                await self._open(
                    "snippets",
                    None,
                    f"issue-{number}",
                    "left-over snippet work",
                    {"seen": seen, "plan": [step], "steps": []},
                )
        for pull in await gh.list_prs(state="open"):
            branch = BRANCH_RE.fullmatch((pull.get("head") or {}).get("ref") or "")
            if not branch or (pull.get("user") or {}).get("login") != bot:
                continue
            issue = await gh.get_issue(int(branch.group(1)))
            if issue.get("state") != "closed" or not OURS_RE.match(issue.get("body") or ""):
                continue
            key = f"pull-{pull['number']}"
            self.present.add(("snippets", key))
            if not self._cases_of("snippets", None, key):
                step = {"do": "close", "issue": None, "pr": pull["number"], "why": "its snippets issue is closed"}
                seen = f"snippet pull request #{pull['number']} is open though its issue #{issue['number']} is closed"
                await self._open(
                    "snippets", None, key, "pull request of a closed issue", {"seen": seen, "plan": [step], "steps": []}
                )

    # --- carrying cases on -------------------------------------------------------------------------------------

    async def _save(self, case: dict, state: str, note: str = "") -> None:
        if note:
            case["data"]["note"] = note
        saved = await self._ask(self.registry.update_case, case["id"], state=state, data=case["data"])
        if state != case["state"]:
            self.report["did"].append(f"{case['key']}: {state}" + (f": {note}" if note else ""))
        case.update(saved)

    async def _advance(self, case: dict, views: dict, skipped: set[str]) -> None:
        """Carry one case as far as it goes in this pass."""
        data, acting = case["data"], self.rules.mode == "act"
        if case["kind"] not in ALL_KINDS:  # a run, a job, an issue or a pull request that was left behind
            if (case["kind"], case["key"]) not in self.present and not _acted(data):
                return await self._save(case, "resolved", "it is gone")
            if acting:
                await self._work(case, None)
            return
        view = views.get((case["kind"], case["number"]))
        if view is None:
            return await self._save(case, "dropped", "the subject is gone")
        if case["kind"] in skipped:
            return  # what is wrong with the installation comes first: nothing is concluded about its subjects
        if case["state"] == "waiting":
            return await self._outcome(case, view)
        moved = view["revision"] != data.get("revision") or standing(view) not in ("working", "stopped")
        if moved and not _acted(data):  # once the case acted, the changes are its own
            return await self._save(case, "resolved", "it moved on by itself")
        if not acting:
            return
        timeline = await self._ask(self.registry.timeline, case["kind"], case["number"])
        acted = last_person(timeline["actions"])
        if acted is not None and acted > case["opened"]:
            return await self._save(case, "dropped", "somebody acted on it")
        if not _acted(data) and timeline["subject"]["revision"] != data.get("revision"):  # since the pass began
            return await self._save(case, "resolved", "it moved on by itself")
        await self._work(case, timeline)

    async def _work(self, case: dict, timeline: dict | None) -> None:
        """Do the case's remaining steps in order until one has to wait, one fails, or none is left."""
        data = case["data"]
        if case["state"] == "open":
            if self.working >= self.rules.max_parallel:
                return
            self.working += 1
        state, told = "working", False  # told: an entry says already what a person must do
        while data["plan"]:
            step, failure = data["plan"][0], None
            try:
                if step["do"] in STARTS_WORK and (tried := self._remedies(case)) >= self.rules.max_remedies:
                    raise Refused(f"the cleaner has started work on this subject {tried} times in seven days")
                if not _acted(data) and step["do"] not in QUIET_STEPS and "diagnosis" not in data:
                    await self._say(case, f"{data['seen']}. Doing: {plan_words(data['plan'])}.", "start")
                result = await self._do(case, step, timeline)
            except Refused as refused:
                data["steps"].append({**step, "at": self.now, "result": f"refused: {refused}"})
                data["needs"] = f"{plan_words([step])} was refused: {refused}"
                state = "needs-person"
                break
            except Exception as error:  # tried again next pass, like a step that is not possible yet
                failure, result = error, None
            if result is None:  # not possible yet, or it failed
                waiting = data.setdefault("waiting", {"step": len(data["steps"]), "since": self.now})
                if waiting["step"] != len(data["steps"]):
                    data["waiting"] = {"step": len(data["steps"]), "since": self.now}
                elif self.now - waiting["since"] > STEP_WAIT_S and (failure or not _patient(case, step)):
                    hours = STEP_WAIT_S // 3600
                    why = f"kept failing for {hours} h: {failure}" if failure else f"was not possible for {hours} h"
                    data["needs"] = f"{plan_words([step])} {why}"
                    state = "needs-person"
                if failure and state != "needs-person":
                    await self._save(case, state)  # what the case did so far is kept
                    raise failure
                break
            data["steps"].append({**step, "at": self.now, "result": result})
            data["plan"].pop(0)
            data.pop("waiting", None)
            if step["do"] == "needs":
                data["needs"], state, told = step["text"], "needs-person", bool(step.get("told"))
                break
            if step["do"] in ACTIONS and timeline is not None:  # the next step decides on the subject as it is now
                timeline = await self._ask(self.registry.timeline, case["kind"], case["number"])
        else:
            state = "waiting" if any(step["do"] in (*STARTS_WORK, "recheck") for step in data["steps"]) else "resolved"
            if data["steps"] and data["steps"][-1]["do"] == "wait":
                data["revision"] = None  # the wait is over: the subject is looked at again as it stands
        if state == "needs-person" and not told:
            await self._say(case, f"needs a person: {data['needs']}", "needs")
        await self._save(case, state)

    async def _outcome(self, case: dict, view: dict) -> None:
        """A case whose remedy is done: resolved when the subject came to rest somewhere good, not helped when
        it stopped again. A subject that has not moved since the remedy's last action has not answered it yet:
        that is "not helped" only after the time a step may take."""
        went = case["data"].get("went")
        if went and view["revision"] == went["revision"]:
            if self.now - went["at"] > STEP_WAIT_S:
                await self._not_helped(case, f"nothing changed in {STEP_WAIT_S // 3600} h")
            return
        if standing(view) == "stopped":
            return await self._not_helped(case, f"stopped again at `{view['stage']}`")
        if standing(view) != "working":
            await self._save(case, "resolved", f"now at `{view['stage']}`")

    async def _not_helped(self, case: dict, seen: str) -> None:
        """The remedy did not help: one diagnosis, with what was tried; after that a person."""
        data = case["data"]
        if data.get("diagnosed") or self.rules.mode != "act":
            data["needs"] = f"the remedy did not help: {seen}"
            await self._say(case, f"needs a person: {data['needs']}", "needs")
            return await self._save(case, "needs-person")
        data["diagnosed"], data["plan"] = True, [{"do": "diagnose"}]
        data["seen"] = f"{data['seen']}. After the remedy: {seen}"
        data["round"] = data["run_id"] = None  # those were the round the remedy ended: the diagnosis reads today's
        await self._save(case, "working", "the remedy did not help")

    async def _say(self, case: dict, text: str, mark: str) -> None:
        """One timeline entry on the case's subject (none for a run or a job)."""
        if case["kind"] not in ALL_KINDS:
            return
        marker = f"<!-- mia: cleaner {case['id']} {mark} -->"
        body = f"**Cleaner:** {neutralize(text)}\n\n{marker}"
        await self._ask(self.registry.append_entry, case["kind"], case["number"], body=body, marker=marker)

    # --- steps -------------------------------------------------------------------------------------------------

    async def _do(self, case: dict, step: dict, timeline: dict | None) -> str | None:
        """Carry one step out: what was done, or None when it is not possible yet. Refused: it never will be."""
        do = step["do"]
        if do in ACTIONS:
            assert timeline is not None
            return await self._act(case, step, timeline)
        if do in ("kill_jobs", "kill_job"):
            if self.activities.jobs is None:
                raise Refused("this worker has no job runner")
            return await self._kill(case, step, self.activities.jobs.client)
        if do == "terminate":
            reason = f"no worker runs this workflow type (cleaner case {case['id']})"
            ended = await self._ask(
                self.registry.terminate_run, step["namespace"], step["workflow_id"], step["run_id"], reason
            )
            return "terminated" if ended else "it was over already"
        if do == "wait":
            step.setdefault("until", self.now + step.get("hours", 1.0) * 3600)
            return "waited" if self.now >= step["until"] else None
        if do in ("snippets_go", "revise", "close", "await_autodev"):
            return await self._github(case, step)
        if do == "needs":
            return "left to a person"
        if do == "diagnose":
            assert timeline is not None  # only a subject's case is diagnosed
            return await self._diagnose(case, timeline)
        if do == "report_defect":
            return await self._report_defect(case, step)
        raise Refused(f"`{do}` is not a step the cleaner knows")

    async def _kill(self, case: dict, step: dict, cluster: Any) -> str | None:
        """End the jobs of the case's round (`kill_jobs`) or one job (`kill_job`): done only once the cluster
        says none is alive, so the kill is sent now and confirmed by a later pass."""
        if step["do"] == "kill_jobs":
            live = [job for job in await self._jobs(case["number"], case["data"].get("round")) if job["live"]]
            for job in live:
                await asyncio.to_thread(cluster.kill, [job["job_id"]])
            return None if live else "no job of the round is alive"
        statuses = await asyncio.to_thread(cluster.status, [step["job_id"]])
        if not statuses or statuses[0].terminal:
            return "the job is gone"
        await asyncio.to_thread(cluster.kill, [step["job_id"]])
        return None

    async def _act(self, case: dict, step: dict, timeline: dict) -> str | None:
        """Send one action to buffer-api as `cleaner`: only one the subject accepts as it stands, with the
        revision just read (buffer-api refuses it if the subject moved meanwhile), a key that makes a second
        send the same action, and for a stop the run it is about."""
        subject, do, data = timeline["subject"], step["do"], case["data"]
        if do == "go" and case["kind"] == "prep":
            do = "resume"  # a request's go starts every dataset it names over: the cleaner only ever resumes one
        if do not in subject["actions"]:
            return None
        if do == "resume" and case["kind"] == "prep" and not await self._request_may_go(subject):
            return None
        actor = CLEANER_ADVISED if do == "go" and step.get("note") else CLEANER  # advice makes the round match first
        fields: dict[str, Any] = {"key": f"cleaner/{case['id']}/{len(data['steps'])}", "actor": actor}
        fields["revision"] = subject["revision"]
        if do == "stop" and data.get("run_id"):
            fields["run"] = data["run_id"]
        if do in RUNS:
            ran = [
                action for action in timeline["actions"] if action["action"] in RUNS and action["payload"].get("root")
            ]
            if ran:
                fields["root"] = ran[-1]["payload"]["root"]  # charged to whoever the last round was charged to
            if do == "go" and step.get("note"):
                fields["note"] = step["note"]
        path = ACTION_PATHS[case["kind"]].format(key=case["key"], number=case["number"])
        try:
            action = await self._ask(self.registry.send_action, path, do, **fields)
        except RegistryError as error:
            if error.status in (409, 422):
                raise Refused(error.message) from None
            raise
        data["went"] = {"revision": subject["revision"], "at": self.now}  # the outcome is judged once it moved
        return f"action {action['id']}"

    async def _diagnose(self, case: dict, timeline: dict) -> str | None:
        """The diagnosis of one case (spec §4.6), over two or more passes: reserve its cost against the day's
        budget, write the evidence and submit the job; once the job is over, read its answer, keep the steps
        that are on the menu and put them in the plan. None while the budget is spent or the job runs."""
        data, jobs = case["data"], self.activities.jobs
        if jobs is None:
            raise Refused("this worker has no job runner")
        job = data.get("diagnosis_job")
        if job is None:
            week = self.now - 7 * 86400
            earlier = [other for other in self._cases_of(case["kind"], case["number"]) if other["updated"] >= week]
            if sum("diagnosis" in other["data"] for other in earlier) >= self.rules.max_remedies:
                raise Refused(f"it was diagnosed {self.rules.max_remedies} times in seven days")
            attempt = len(data["steps"])
            root = f"cleaner-{datetime.fromtimestamp(self.now, UTC).date().isoformat()}"
            reservation = f"case-{case['id']}-{attempt}"
            usd = self.cfg.judge.max_budget_usd * self.cfg.autonomy.price_factor  # what its cap costs here
            caps = {"max_usd": self.rules.max_usd_per_day}
            held = await self._ask(self.registry.reserve, root, reservation, "diagnose", usd, caps)
            if not held.get("ok"):
                return None  # the day's budget is spent: the case waits
            workspace = workspace_of(self.cfg, case["id"], attempt)
            item = await self._evidence(timeline, set())
            # what the diagnosis asks for is about the round and the run it was shown
            data["round"], data["run_id"] = item["round"], (item.get("run") or {}).get("run_id")
            under = f"issue-{case['number']}/{item['round']}"
            records = await asyncio.to_thread(round_records, self.cfg.paths.work_root, under) if item["round"] else {}
            await asyncio.to_thread(write_evidence, workspace, case, item, self.report["findings"], records)
            job_id = await jobs.submit(f"cleaner-case-{case['id']}-{attempt}", diagnose_job(self.cfg, workspace))
            data["diagnosis_job"] = {
                "id": job_id,
                "workspace": str(workspace),
                "root": root,
                "reservation": reservation,
            }
            return None
        statuses = await asyncio.to_thread(jobs.client.status, [job["id"]])
        if statuses and not statuses[0].terminal:
            return None
        await jobs.wait(job["id"], job["workspace"], lambda state: None)  # it is over: this writes its record
        answer, cost, _ = read_claude_json(Path(job["workspace"]))
        paid = None if cost is None else cost * self.cfg.autonomy.price_factor
        await self._ask(self.registry.settle, job["root"], job["reservation"], usd=paid)
        del data["diagnosis_job"]
        outcome = parse_outcome(answer)
        if outcome is None:
            raise Refused("the diagnosis left no answer that can be read")
        kept = reduce_outcome(outcome, case["kind"], data.get("party"))
        plan = kept.pop("plan")
        record = address(self.cfg.paths.work_root, job["workspace"])
        data["diagnosis"], data["diagnosed"] = {**kept, "job": record}, True
        # no remedy: a person's turn, which the diagnosis's own entry says (`told`)
        needs = (
            [] if plan else [{"do": "needs", "text": kept["needs"] or "the diagnosis proposes no remedy", "told": True}]
        )
        if kept["defect"]:
            plan.insert(0, {"do": "report_defect", **kept["defect"]})
        said = f"Diagnosis ({kept['cause']}): {kept['diagnosis']}"
        if plan:
            said += f" Doing: {plan_words(plan)}."
        if needs:
            said += f" Needs a person: {needs[0]['text']}"
        data["plan"][1:] = plan + needs  # after this step: what the diagnosis asks for replaces whatever was left
        link = f" [Its job]({job_link(self.cfg.service.url, record)})." if record else ""
        await self._say(case, said + link, f"diagnosis {len(data['steps'])}")
        return f"{kept['cause']}: {kept['diagnosis'][:300]}"

    async def _report_defect(self, case: dict, step: dict) -> str:
        """File a defect the diagnosis found in this installation's own code: an issue in `cleaner.defects_repo`,
        one per defect (found again by its marker, where this case is added as a comment). With no repository
        configured the defect stays on its case."""
        repo = self.rules.defects_repo
        if not repo:
            return "kept on the case: no `cleaner.defects_repo` is set"
        gh = copy.copy(self.activities.gh_issues)  # ponytail: the issue repo's client pointed at another repo
        gh.repo = repo
        marker = f"<!-- mia: defect {hashlib.sha256(step['title'].lower().encode()).hexdigest()[:12]} -->"
        about = f"Cleaner case {case['id']} (`{case['key']}`): {case['data'].get('seen', '')}"
        for issue in await gh.list_issues(state="open", creator=self.activities.bot):
            if marker in (issue.get("body") or ""):
                await gh.comment(issue["number"], f"Seen again. {neutralize(about)}")
                return f"added to issue #{issue['number']}"
        body = f"{step['evidence']}\n\n{neutralize(about)}\n\n{marker}"
        created = await gh.create_issue(f"cleaner: {step['title']}", body, [])
        return f"filed as issue #{created['number']}"

    async def _request_may_go(self, subject: dict) -> bool:
        """Whether a `resume` of the request would run no dataset round (spec §4.7): every dataset it names is
        in a round, at a person's turn, prepared or shelved. What a request sends its datasets goes as
        `prep:<number>`, past each dataset's own limits, so until then the request waits on its datasets'
        cases. Refused when one of those needs a person. The cleaner sends a request no `go`: that one starts
        every dataset it names over."""
        from .spec import SpecError, load_prep_text

        try:
            spec = load_prep_text((subject.get("data") or {}).get("spec") or "")
        except SpecError:
            raise Refused("its text cannot be read, so what a resume would start is not known") from None
        if spec.select is not None or not spec.records:
            raise Refused("it selects its datasets: a resume would run the rounds of whatever it selects")
        for view in await self._request_datasets(subject):
            if standing(view) in ("working", "waiting", "shelved") or view["stage"] == "done":
                continue
            stuck = [case for case in self._cases_of("dataset", view["number"]) if case["state"] == "needs-person"]
            if stuck:
                raise Refused(f"its dataset `{view['key']}` needs a person: {stuck[0]['data'].get('needs', '')}")
            return False
        return True

    async def _github(self, case: dict, step: dict) -> str | None:
        """A step on the snippets repository. A comment carries the step's marker, so a second try posts none."""
        gh, do = self.activities.gh_snippets, step["do"]
        marker = f"<!-- mia: cleaner {case['id']}/{len(case['data']['steps'])} -->"

        async def say(number: int, text: str) -> None:
            if not any(marker in (comment.get("body") or "") for comment in await gh.issue_comments(number)):
                await gh.comment(number, f"{text}\n\n{marker}")

        if do == "await_autodev":  # a command to autodev is read at its next poll: until then the label stays
            issue = await gh.get_issue(step["issue"])
            if "snippets:needs-human" not in {label["name"] for label in issue.get("labels", [])}:
                return "autodev has taken it"
            step.setdefault("since", self.now)
            if self.now - step["since"] > self.rules.quiet_minutes * 60:
                raise Refused(f"autodev did not take it within {self.rules.quiet_minutes} min")
            return None
        if do == "snippets_go":
            await say(
                step["issue"],
                "The cleaner asks autodev to try this issue again: its last run stopped before a pull request.",
            )
            await gh.add_labels(step["issue"], ["snippets:go"])
            return f"asked on issue #{step['issue']}"
        if do == "revise":
            await say(step["pr"], REVISE_TEXT)
            return f"asked on pull request #{step['pr']}"
        for number in (step.get("pr"), step.get("issue")):  # close: the pull request first, which ends autodev's run
            if number is not None and (await gh.get_issue(number)).get("state") == "open":
                await say(number, f"Closed by the cleaner: {neutralize(step['why'])}.")
                await gh.update_issue(number, state="closed")
        return "closed"

    def _remedies(self, case: dict) -> int:
        """How many steps that start work the cleaner did on the case's subject in the last seven days."""
        week = self.now - 7 * 86400
        return sum(
            step["do"] in STARTS_WORK and step["at"] >= week
            for other in self._cases_of(case["kind"], case["number"])
            for step in other["data"].get("steps", ())
        )


class Refused(Exception):
    """A step that will never be possible as the case stands; the text says why."""


QUIET_STEPS = ("wait", "needs", "diagnose", "await_autodev", "report_defect")  # steps that change no subject


def _acted(data: dict) -> bool:
    """Whether the case has changed anything yet (a wait or a note to a person changes nothing)."""
    return any(step["do"] not in QUIET_STEPS for step in data.get("steps", ()))


def _patient(case: dict, step: dict) -> bool:
    """Whether the step has a wait of its own, so the common limit on a step that is not possible yet is not
    its limit: a wait, autodev's answer, a diagnosis (its job, the day's budget) and a request's go, which
    waits on its datasets' cases."""
    return step["do"] in ("wait", "await_autodev", "diagnose") or (case["kind"] == "prep" and step["do"] in RUNS)


def _epoch(stamp: str | None) -> float:
    try:
        return datetime.fromisoformat(stamp or "").timestamp()
    except ValueError:
        return 0.0


async def run_sweep(activities: Activities, now: float | None = None) -> dict:
    """One pass of the cleaner; its report (also stored for the page)."""
    return await Sweep(activities, now).run()
