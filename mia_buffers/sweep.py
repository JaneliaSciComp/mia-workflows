"""One pass of the cleaner (spec cleaner §4): read the installation, name what is wrong with it, find the
subjects that are stuck, open a case for each and carry every open case as far as it goes.

Runs as the `sweep` activity (`Activities.sweep`) and uses the activity object's clients. The rules are in
`mia_buffers.cleaner`; this module does the reading and the doing. A case is a row of the ledger that holds
its remaining steps, so a pass that fails half way leaves cases the next pass continues: every step that
changes something carries a key, and a step that has to wait returns None and is tried again next time."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import TYPE_CHECKING, Any

from mia_core import autonomy
from mia_core.checkout import head_commit
from mia_core.config import manifest_path
from mia_core.jobrecord import open_records
from mia_core.registry import RegistryError
from mia_core.untrusted import neutralize

from .cleaner import (
    ALL_KINDS,
    SNIPPETS,
    WORKFLOW_TYPES,
    evidence_findings,
    last_person,
    plan_for,
    plan_words,
    service_findings,
    sighting,
)

if TYPE_CHECKING:
    from .activities import Activities

log = logging.getLogger(__name__)

ACTIVE = ("open", "working", "waiting")  # the states of a case that is not over
ACTIONS = ("cancel", "stop", "go", "recheck")  # steps sent to buffer-api's action routes
STARTS_WORK = ("go", "snippets_go", "revise")  # steps the weekly limit counts
CASE_DAYS = 30  # how far back a subject's earlier cases are read
STEP_WAIT_S = 2 * 3600  # a step that is still not possible after this long ends its case
ROUND_ENDED_S = 600  # a job still alive this long after its round ended is left over
RECENT_S = 86400  # how long after a round ended its jobs are looked for
ISSUE_RE = re.compile(r"mia-snippets issue #(\d+)")
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
        late = evidence_findings(evidence, rules.pend_hours, now)
        findings.extend(late)
        skipped |= {kind for finding in late for kind in finding["kinds"]}
        for item in evidence:
            if item["subject"]["kind"] not in skipped and (found := sighting(item, rules, now)):
                await self._try(item["subject"]["key"], self._sighted(item, found))
        await self._try("open runs", self._unrunnable())
        for view in views:
            await self._try(view["key"], self._left_jobs(view))
        by_subject = {(view["kind"], view["number"]): view for view in views}
        for case in [case for case in self.cases if case["state"] in ACTIVE]:
            await self._try(case["key"], self._advance(case, by_subject, skipped))
        return await self._done()

    async def _done(self) -> dict:
        self.report["cases"] = sum(case["state"] in ACTIVE for case in self.cases)
        await self._try("report", self._ask(self.registry.put_cleaner, self.report))
        return self.report

    async def _try(self, what: str, work: Any) -> None:
        """One subject's or one case's part of the pass: its failure is the report's, never the pass's."""
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
        workers, schedules = await self._ask(registry.workers), await self._ask(registry.schedules)
        checkout = await asyncio.to_thread(head_commit)
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
        if view["status"] == "working":
            old = age >= self.rules.quiet_minutes * 60
        elif view["status"] == "stuck":
            old = age >= self.rules.stopped_minutes * 60
        else:
            return False
        seen = any(
            case["data"].get("revision") == view["revision"] for case in self._cases_of(view["kind"], view["number"])
        )
        return old and not seen

    async def _gather(self, view: dict, skipped: set[str], evidence: list[dict]) -> None:
        """The subject's evidence: its timeline and actions, and for a working one its run, the jobs of its
        round and the party it waits for."""
        kind, number = view["kind"], view["number"]
        timeline = await self._ask(self.registry.timeline, kind, number)
        if timeline is None:
            return
        item = {"subject": timeline["subject"], "entries": timeline["entries"], "actions": timeline["actions"]}
        item["round"] = next((entry["round"] for entry in reversed(item["entries"]) if entry.get("round")), None)
        if view["status"] == "working":
            item["run"] = await self._run(kind, number)
            item["jobs"] = await self._jobs(number, item["round"])
        if view["status"] == "working" or kind == "prep":
            item["party"] = await self._party(item, skipped)
        evidence.append(item)

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
        if subject["kind"] == "dataset" and subject["stage"] in DEVELOP_STAGES:
            issues = [int(match) for entry in item["entries"] for match in ISSUE_RE.findall(entry.get("body") or "")]
            if not issues:
                return None
            if SNIPPETS in skipped:
                return {"issue": issues[-1], "working": True}  # autodev's worker is away: not this round's fault
            issue = await self.activities.gh_snippets.get_issue(issues[-1])
            labels = {label["name"] for label in issue.get("labels", [])}
            return {"issue": issues[-1], "state": issue.get("state"), "working": "snippets:working" in labels}
        if subject["kind"] == "prep":
            datasets = await self._request_datasets(subject)
            return {
                "working": not datasets or any(view["status"] in ("working", "waiting") for view in datasets),
                "settled": bool(datasets)
                and all(view["status"] == "shelved" or view["stage"] == "done" for view in datasets),
            }
        return None

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
        await self._open(kind, number, subject["key"], found["rule"] or "diagnosis", data)

    async def _unrunnable(self) -> None:
        """Open runs of a workflow type no worker registers: each logs an error on every task and never ends."""
        for run in await self._ask(self.registry.runs):
            key = f"{run['namespace']}/{run['id']}"
            if run["type"] in WORKFLOW_TYPES or self._cases_of("run", None, key):
                continue
            step = {"do": "terminate", "namespace": run["namespace"], "workflow_id": run["id"], "run_id": run["run_id"]}
            seen = f"an open run of the workflow type `{run['type']}`, which no worker registers"
            await self._open("run", None, key, "unrunnable run", {"seen": seen, "plan": [step], "steps": []})

    async def _left_jobs(self, view: dict) -> None:
        """Jobs still alive after their round ended (a round ended from outside kills none)."""
        ended = self.now - (view.get("updated") or self.now)
        if view["status"] == "working" or view["kind"] == "prep" or not ROUND_ENDED_S <= ended <= RECENT_S:
            return
        if any(case["state"] in ACTIVE for case in self._cases_of(view["kind"], view["number"])):
            return  # the subject's own case ends its round's jobs
        timeline = await self._ask(self.registry.timeline, view["kind"], view["number"])
        round_id = next((entry["round"] for entry in reversed(timeline["entries"]) if entry.get("round")), None)
        for job in await self._jobs(view["number"], round_id):
            if job["live"] and not self._cases_of("job", None, job["address"]):
                seen = f"job {job['job_id']} of `{view['key']}` is {job['state']} though its round ended"
                plan = [{"do": "kill_job", "job_id": job["job_id"], "address": job["address"]}]
                await self._open(
                    "job", None, job["address"], "left-over job", {"seen": seen, "plan": plan, "steps": []}
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
        if case["kind"] not in ALL_KINDS:  # a run or a job that was left behind
            if acting:
                await self._work(case, None)
            return
        view = views.get((case["kind"], case["number"]))
        if view is None:
            return await self._save(case, "dropped", "the subject is gone")
        if case["state"] == "waiting":
            return await self._outcome(case, view)
        moved = view["revision"] != data.get("revision") or view["status"] not in ("working", "stuck")
        if moved and not _acted(data):  # once the case acted, the changes are its own
            return await self._save(case, "resolved", "it moved on by itself")
        if not acting or case["kind"] in skipped:
            return
        timeline = await self._ask(self.registry.timeline, case["kind"], case["number"])
        acted = last_person(timeline["actions"])
        if acted is not None and acted > case["opened"]:
            return await self._save(case, "dropped", "somebody acted on it")
        await self._work(case, timeline)

    async def _work(self, case: dict, timeline: dict | None) -> None:
        """Do the case's remaining steps in order until one has to wait, one fails, or none is left."""
        data = case["data"]
        if case["state"] == "open":
            if self.working >= self.rules.max_parallel:
                return
            self.working += 1
        state = "working"
        while data["plan"]:
            step = data["plan"][0]
            try:
                result = await self._do(case, step, timeline)
            except Refused as refused:
                data["steps"].append({**step, "at": self.now, "result": f"refused: {refused}"})
                data["needs"] = f"{plan_words([step])} was refused: {refused}"
                state = "needs-person"
                break
            if result is None:  # not possible yet
                waiting = data.setdefault("waiting", {"step": len(data["steps"]), "since": self.now})
                if waiting["step"] != len(data["steps"]):
                    data["waiting"] = {"step": len(data["steps"]), "since": self.now}
                elif self.now - waiting["since"] > STEP_WAIT_S and step["do"] not in ("wait", "await_autodev"):
                    data["needs"] = f"{plan_words([step])} was not possible for {STEP_WAIT_S // 3600} h"
                    state = "needs-person"
                break
            data["steps"].append({**step, "at": self.now, "result": result})
            data["plan"].pop(0)
            data.pop("waiting", None)
            if step["do"] == "needs":
                data["needs"], state = step["text"], "needs-person"
                break
            if step["do"] in ACTIONS and timeline is not None:  # the next step decides on the subject as it is now
                timeline = await self._ask(self.registry.timeline, case["kind"], case["number"])
        else:
            state = "waiting" if any(step["do"] in (*STARTS_WORK, "recheck") for step in data["steps"]) else "resolved"
        if state == "needs-person":
            await self._say(case, f"needs a person: {data['needs']}", "needs")
        await self._save(case, state)

    async def _outcome(self, case: dict, view: dict) -> None:
        """A case whose remedy is done: resolved when the subject came to rest somewhere good, not helped when
        it stopped again."""
        if view["status"] == "stuck":
            return await self._not_helped(case, f"stopped again at `{view['stage']}`")
        if view["status"] != "working":
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
        if do == "needs":
            return "left to a person"
        if do == "diagnose":
            raise Refused("no rule covers this and no diagnosis is available")
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
        if do not in subject["actions"]:
            return None
        if do in STARTS_WORK and (tried := self._remedies(case)) >= self.rules.max_remedies:
            raise Refused(f"the cleaner has started work on this subject {tried} times in seven days")
        fields: dict[str, Any] = {"key": f"cleaner/{case['id']}/{len(data['steps'])}", "actor": "cleaner"}
        fields["revision"] = subject["revision"]
        if do == "stop" and data.get("run_id"):
            fields["run"] = data["run_id"]
        if do == "go":
            gos = [
                action for action in timeline["actions"] if action["action"] == "go" and action["payload"].get("root")
            ]
            if gos:
                fields["root"] = gos[-1]["payload"]["root"]  # charged to whoever the last round was charged to
            if step.get("note"):
                fields["note"] = step["note"]
        if not _acted(data):
            await self._say(case, f"{data['seen']}. Doing: {plan_words(data['plan'])}.", "start")
        path = ACTION_PATHS[case["kind"]].format(key=case["key"], number=case["number"])
        try:
            action = await self._ask(self.registry.send_action, path, do, **fields)
        except RegistryError as error:
            if error.status in (409, 422):
                raise Refused(error.message) from None
            raise
        return f"action {action['id']}"

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


def _acted(data: dict) -> bool:
    """Whether the case has changed anything yet (a wait or a note to a person changes nothing)."""
    return any(step["do"] not in ("wait", "needs") for step in data.get("steps", ()))


def _epoch(stamp: str | None) -> float:
    from datetime import datetime

    try:
        return datetime.fromisoformat(stamp or "").timestamp()
    except ValueError:
        return 0.0


async def run_sweep(activities: Activities, now: float | None = None) -> dict:
    """One pass of the cleaner; its report (also stored for the page)."""
    return await Sweep(activities, now).run()
