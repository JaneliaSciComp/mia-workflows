"""One pass of the cleaner against a buffer-api in this process: what it opens a case for, what it sends, and
what it leaves alone (spec cleaner §4)."""

import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from mia_buffers import sweep as sweep_module
from mia_buffers.api import BufferService
from mia_buffers.config import CleanerCfg
from mia_buffers.ledger import Ledger
from mia_buffers.sweep import run_sweep
from mia_core.jobrecord import write_submitted
from mia_core.registry import Registry
from tests.test_buffers_api import PREP_TEXT, REF_A, SHAPE, Gateway, act, call, worker
from tests.test_buffers_config import make_cfg

HOURS = 3600.0
RUN = {"status": "RUNNING", "run_id": "run-1", "task": None, "activities": []}


class Temporal(Gateway):
    """The gateway stand-in with what the cleaner reads: each subject's run (`runs`, by number; none listed means
    no open run), the open runs of every type and each queue's worker."""

    def __init__(self):
        super().__init__(running=False)
        self.runs, self.open, self.queues = {}, [], {"mia-data": True, "mia-buffers": True}

    def health(self):
        return {"temporal": True, "queues": dict(self.queues)}

    def run(self, kind, number):
        return self.runs.get(number)

    def run_status(self, kind, number):
        return "RUNNING" if number in self.runs else None

    def open_runs(self):
        return list(self.open)

    def schedules(self):
        return []

    def signal_running(self, kind, number, signal, *args):
        self.running = number in self.runs
        return super().signal_running(kind, number, signal, *args)

    def terminate(self, kind, number, reason, run_id=None):
        self.running = self.runs.pop(number, None) is not None
        return super().terminate(kind, number, reason, run_id)


class Csub:
    """The cluster: `alive` holds the ids of the jobs that run; a kill is recorded and takes the job away."""

    def __init__(self):
        self.alive, self.killed = set(), []

    def status(self, job_ids):
        return [SimpleNamespace(job_id=job, state="RUN", terminal=False) for job in job_ids if job in self.alive]

    def kill(self, job_ids):
        self.killed += job_ids
        self.alive -= set(job_ids)


class Snippets:
    """The snippets repository: issues and pull requests by number (a pull request is also an issue), each a
    dict as GitHub returns it; comments by number."""

    def __init__(self):
        self.issues, self.pulls, self.comments = {}, {}, {}

    def issue(self, number, dataset, labels=(), state="open", said=()):
        body = f"<!-- mia-data: issue={dataset} round=i{dataset}-e1 -->\nwrite a reader"
        named = [{"name": label} for label in labels]
        self.issues[number] = {
            "number": number,
            "state": state,
            "labels": named,
            "body": body,
            "user": {"login": "bot"},
        }
        self.comments[number] = [
            {"body": text, "user": {"login": "bot"}, "created_at": f"2027-01-0{index + 1}"}
            for index, text in enumerate(said)
        ]

    def pull(self, number, issue, mergeable=True, merged=False):
        head = {"ref": f"snippets/issue-{issue}"}
        state = "closed" if merged else "open"
        self.pulls[number] = {
            "number": number,
            "state": state,
            "head": head,
            "mergeable": mergeable,
            "merged": merged,
            "user": {"login": "bot"},
        }
        self.issues[number] = {"number": number, "state": state, "labels": [], "body": "", "pull_request": {}}
        self.comments.setdefault(number, [])

    def labels(self, number):
        return [label["name"] for label in self.issues[number]["labels"]]

    async def get_issue(self, number):
        return self.issues[number]

    async def get_pr(self, number):
        return self.pulls[number]

    async def find_pr(self, branch, state="open"):
        return next(
            (pull for pull in self.pulls.values() if pull["head"]["ref"] == branch and pull["state"] == state), None
        )

    async def list_issues(self, state, creator=None):
        return [issue for issue in self.issues.values() if issue["state"] == state and "pull_request" not in issue]

    async def create_issue(self, title, body, labels):
        number = max(self.issues, default=0) + 1
        self.issues[number] = {"number": number, "state": "open", "labels": [], "body": body, "title": title}
        return self.issues[number]

    async def list_prs(self, state):
        return [pull for pull in self.pulls.values() if pull["state"] == state]

    async def issue_comments(self, number):
        return self.comments.get(number, [])

    async def comment(self, number, body):
        self.comments.setdefault(number, []).append(
            {"body": body, "user": {"login": "bot"}, "created_at": "2027-02-01"}
        )

    async def add_labels(self, number, labels):
        self.issues[number]["labels"] += [{"name": label} for label in labels]

    async def update_issue(self, number, state):
        self.issues[number]["state"] = state
        if number in self.pulls:
            self.pulls[number]["state"] = state


class World:
    def __init__(self, tmp_path, monkeypatch, **cleaner):
        self.temporal, self.csub, self.snippets = Temporal(), Csub(), Snippets()
        self.service = BufferService(Ledger(tmp_path / "l.sqlite"), token="t", dispatcher=self.temporal)

        def handler(request):
            path, query = request.url.raw_path.decode().split("?")[0], dict(request.url.params)
            status, payload, _ = self.service.handle(
                request.method, path, query, request.content, dict(request.headers)
            )
            return httpx.Response(status, json=payload)

        cfg = make_cfg(tmp_path).model_copy(update={"cleaner": CleanerCfg(**{"mode": "act", **cleaner})})
        self.cfg = cfg
        registry = Registry("http://svc", "t", transport=httpx.MockTransport(handler))
        self.submitted = []  # the diagnosis jobs: (key, JobSpec)

        async def submit(key, spec):
            self.submitted.append((key, spec))
            self.csub.alive.add(f"d{len(self.submitted)}")
            return f"d{len(self.submitted)}"

        async def wait(job_id, workspace, heartbeat):
            return None

        jobs = SimpleNamespace(client=self.csub, submit=submit, wait=wait)
        self.issues = Snippets()  # the worker's issue repository client; a defect is filed through a copy of it
        self.issues.repo = "o/buffers"
        self.activities = SimpleNamespace(
            cfg=cfg, registry=registry, jobs=jobs, gh_snippets=self.snippets, gh_issues=self.issues, bot="bot"
        )
        monkeypatch.setattr(sweep_module, "code_digest", lambda: "b" * 16)
        monkeypatch.setattr(sweep_module.autonomy, "paused", lambda manifest: False)
        records = [
            {
                "path": f"{name}.yaml",
                "record": {"id": name, "data": {"download_url": f"https://data.example.org/{name}.zip"}, **SHAPE},
            }
            for name in ("rec-a", "rec-b")
        ]
        assert worker(self.service, "PUT", f"/records/{REF_A}", {"records": records})[0] == 200

    def sweep(self, hours=3.0):
        """One pass, `hours` after now: everything written so far is that old to it."""
        return asyncio.run(run_sweep(self.activities, time.time() + hours * HOURS))

    def dataset(self, record="rec-a", stage="developing", run=True):
        """A dataset a person started that is now at `stage` in round `i<number>-e1`; its number."""
        number = act(self.service, f"/datasets/{record}/actions", {"action": "go"})[1]["number"]
        self.stage("dataset", number, stage)
        if run:
            self.temporal.runs[number] = dict(RUN)
        return number

    def stage(self, kind, number, stage, said=""):
        body = {
            "stage": stage,
            "round": f"i{number}-e1",
            "body": f"**Stage:** `{stage}`\n\n{said}",
            "marker": f"<!-- {stage} {time.time_ns()} -->",
        }
        assert worker(self.service, "POST", f"/timeline/{kind}/{number}", body)[0] == 200

    def answer(self, outcome, cost=0.5):
        """The running diagnosis job ends with this answer (a dict is its JSON object, a string its whole text)."""
        (job,) = [case["data"]["diagnosis_job"] for case in self.cases() if "diagnosis_job" in case["data"]]
        text = outcome if isinstance(outcome, str) else f"Here it is.\n```json\n{json.dumps(outcome)}\n```"
        Path(job["workspace"], "claude.json").write_text(json.dumps({"result": text, "total_cost_usd": cost}))
        self.csub.alive.discard(job["id"])

    def cases(self, state=None):
        query = {"state": state} if state else {"days": "30"}
        return call(self.service, "GET", "/cases", query)[1]["cases"]

    def timeline(self, kind, number):
        return call(self.service, "GET", f"/timeline/{kind}/{number}")[1]

    def job(self, number, name="crops-1", job_id="901"):
        workspace = self.cfg.paths.work_root / f"issue-{number}" / f"i{number}-e1" / name
        workspace.mkdir(parents=True)
        write_submitted(self.cfg.paths.work_root, workspace, service="mia-data", key=name, job_id=job_id, request={})
        self.csub.alive.add(job_id)


@pytest.fixture
def world(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def test_in_report_mode_a_case_says_what_would_be_done_and_nothing_is_sent(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch, mode="report")
    number = world.dataset(run=False)
    report = world.sweep()
    (case,) = world.cases()
    assert (case["rule"], case["state"], case["number"]) == ("no run", "open", number)
    assert [step["do"] for step in case["data"]["plan"]] == ["cancel", "kill_jobs", "resume"]
    assert report["opened"] == ["rec-a: no run: cancel the round, then end the round's cluster jobs, then resume it"]
    assert [action["actor"] for action in world.timeline("dataset", number)["actions"]] == ["page"]
    assert call(world.service, "GET", "/cleaner")[1]["opened"] == report["opened"]
    world.sweep()
    assert len(world.cases()) == 1  # the same sighting is the same case
    world.stage("dataset", number, "review")  # the round moved by itself
    world.sweep()
    assert [(case["state"], case["data"]["note"]) for case in world.cases()] == [("resolved", "it moved on by itself")]


def test_with_mode_off_nothing_is_read_or_written(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch, mode="off")
    world.dataset(run=False)
    assert world.sweep()["mode"] == "off" and world.cases() == []
    assert call(world.service, "GET", "/cleaner")[:2] == (200, b"null")


def test_a_round_with_no_run_is_cancelled_and_run_again(world):
    number = world.dataset(run=False)
    world.sweep()
    out = world.timeline("dataset", number)
    cleaner = [(action["action"], action["key"]) for action in out["actions"] if action["actor"] == "cleaner"]
    (case,) = world.cases()
    assert cleaner == [("cancel", f"cleaner/{case['id']}/0"), ("resume", f"cleaner/{case['id']}/2")]
    assert world.temporal.sent[-1][:3] == ("dataset", number, "restart") and world.temporal.sent[-1][3].kind == "resume"
    assert case["state"] == "waiting" and [step["do"] for step in case["data"]["steps"]] == [
        "cancel",
        "kill_jobs",
        "resume",
    ]
    said = [entry["body"] for entry in out["entries"] if entry["body"].startswith("**Cleaner:**")]
    assert len(said) == 1 and "no open run" in said[0] and "Doing: cancel the round" in said[0]
    world.temporal.runs[number] = dict(RUN)
    world.stage("dataset", number, "matching")
    world.sweep(0.2)
    assert world.cases()[0]["state"] == "waiting"  # its round is at work
    world.stage("dataset", number, "review")
    world.sweep(0.2)
    assert [(case["state"], case["data"]["note"]) for case in world.cases()] == [("resolved", "now at `review`")]


def test_a_go_is_not_sent_while_a_job_of_the_ended_round_lives(world):
    number = world.dataset(run=False)
    world.job(number)
    world.sweep()
    (case,) = world.cases()
    assert [step["do"] for step in case["data"]["steps"]] == ["cancel"] and case["state"] == "working"
    asked = [
        action["action"] for action in world.timeline("dataset", number)["actions"] if action["actor"] == "cleaner"
    ]
    assert world.csub.killed == ["901"] and asked == ["cancel"]  # killed, not yet seen dead: no go
    world.sweep()
    (case,) = world.cases()
    assert [step["do"] for step in case["data"]["steps"]] == ["cancel", "kill_jobs", "resume"] and case[
        "state"
    ] == "waiting"


def test_a_job_that_cannot_be_seen_dead_ends_the_case_at_a_person(world):
    number = world.dataset(run=False)
    world.job(number)
    world.csub.kill = lambda job_ids: None  # the kill does nothing
    world.sweep(3.0)
    world.sweep(6.0)
    (case,) = world.cases()
    assert case["state"] == "gave-up" and "end the round's cluster jobs was not possible" in case["data"]["needs"]
    assert "could not repair this" in world.timeline("dataset", number)["entries"][-1]["body"]
    view = call(world.service, "GET", "/subjects/dataset/rec-a")[1]
    assert case["data"]["ended_revision"] == view["revision"]  # where the subject stood when the case ended
    assert all(sent[3].actor == "page" for sent in world.temporal.sent if sent[2] == "restart")  # no go of its own


def test_a_frozen_run_is_stopped_by_its_run_id(world):
    number = world.dataset()
    world.temporal.runs[number] = {**RUN, "task": 40}
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "frozen run" and world.temporal.terminated == [("dataset", number, "run-1")]
    assert [step["do"] for step in case["data"]["steps"]] == ["stop", "kill_jobs", "resume"]


def test_a_person_who_acts_on_the_subject_takes_the_case_over(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch, mode="report")
    number = world.dataset()
    world.temporal.runs[number] = {**RUN, "task": 40}
    world.sweep()
    assert act(world.service, "/datasets/rec-a/actions", {"action": "cancel"})[0] == 202  # a person, on the page
    world.activities.cfg = world.cfg.model_copy(update={"cleaner": CleanerCfg(mode="act")})
    world.sweep(6.0)
    (case,) = world.cases()
    assert (case["state"], case["data"]["note"]) == ("dropped", "somebody acted on it")
    assert world.temporal.terminated == []


def test_a_subject_a_person_just_acted_on_gets_no_case(world):
    world.dataset(run=False)
    world.sweep(1.0)  # quiet for an hour, but the go is a person's and an hour old
    assert world.cases() == []


def test_a_worker_that_is_down_keeps_the_cleaner_off_its_subjects(world):
    world.dataset(run=False)
    world.temporal.queues["mia-data"] = False
    report = world.sweep()
    assert [finding["id"] for finding in report["findings"]] == ["worker mia-data"] and world.cases() == []


def test_runs_frozen_across_a_kind_are_a_finding_not_cases(world):
    for record in ("rec-a", "rec-b"):
        world.temporal.runs[world.dataset(record)] = {**RUN, "task": 12}
    report = world.sweep()
    assert [finding["id"] for finding in report["findings"]] == ["tasks dataset"]
    assert world.cases() == [] and world.temporal.terminated == []


def test_a_remedy_that_did_not_help_is_diagnosed_once_and_then_ends_at_a_person(world):
    number = world.dataset(run=False)
    world.sweep()  # cancel, go
    world.stage("dataset", number, "failed", "the crops do not match the annotation")
    world.sweep()
    (case,) = world.cases()
    assert case["state"] == "working" and "After the remedy: stopped at `failed`" in case["data"]["seen"]
    assert len(world.submitted) == 1  # the diagnosis job
    world.answer({"diagnosis": "the reader swaps axes", "cause": "data", "remedies": [{"do": "go", "note": "use zyx"}]})
    world.sweep()
    # the rule's remedy resumed the round; the diagnosis has advice for the match, which only a new round reads
    assert cleaner_actions(world, number) == ["cancel", "resume", "go"] and world.cases()[0]["state"] == "waiting"
    world.stage("dataset", number, "failed", "the crops do not match the annotation")
    world.sweep()
    (case,) = world.cases()
    assert case["state"] == "gave-up" and "the remedy did not help" in case["data"]["needs"]
    assert len(world.submitted) == 1  # no second diagnosis


def test_the_weekly_limit_stops_the_cleaner_from_running_a_subject_again(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch, max_remedies=1)
    number = world.dataset(run=False)
    world.sweep()  # cancel, go
    world.stage("dataset", number, "matching")  # the new round, which loses its run too
    world.sweep(6.0)  # not helped: a diagnosis
    world.answer({"diagnosis": "its run was lost again", "cause": "unknown", "remedies": [{"do": "go"}]})
    world.sweep(9.0)
    (case,) = world.cases()
    assert case["state"] == "gave-up" and "started work on this subject 1 times" in case["data"]["needs"]
    gos = [
        action
        for action in world.timeline("dataset", number)["actions"]
        if action["actor"] == "cleaner" and action["action"] in ("go", "resume")
    ]
    assert [action["action"] for action in gos] == ["resume"]  # the resume counted; the go after it was not sent


def test_a_run_no_worker_can_execute_is_terminated(world):
    world.temporal.open = [{"namespace": "mia-buffers", "id": "old/1", "run_id": "run-0", "type": "MiaBufferPoll"}]
    world.temporal.running = True
    world.sweep()
    (case,) = world.cases()
    assert (case["kind"], case["key"], case["state"]) == ("run", "mia-buffers/old/1", "resolved")
    assert world.temporal.terminated == [("mia-buffers", "old/1", "run-0")]
    world.sweep()
    assert len(world.cases()) == 1


def test_a_job_left_alive_after_its_round_ended_is_killed(world):
    number = world.dataset()
    world.job(number)
    world.stage("dataset", number, "cancelled")
    world.sweep(0.3)  # the round ended 18 minutes ago
    (case,) = world.cases()
    assert (case["rule"], case["key"]) == ("left-over job", f"issue-{number}/i{number}-e1/crops-1")
    assert world.csub.killed == ["901"] and case["state"] == "working"
    world.sweep(0.5)
    assert world.cases()[0]["state"] == "resolved"


def test_a_request_whose_datasets_are_all_prepared_is_told_to_look_again(world):
    number = act(world.service, "/prep", {"spec": PREP_TEXT})[1]["number"]
    world.temporal.runs[number] = dict(RUN)
    world.stage("prep", number, "needs-human")
    dataset = world.dataset("rec-a", "done", run=False)
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "request behind" and case["state"] == "waiting"
    assert world.temporal.sent[-1] == ("prep", number, "recheck")
    assert dataset


# --- stopped rounds and what they left behind (spec §4.5: remedies, leftovers) ---


def cleaner_actions(world, number, kind="dataset"):
    asked = world.timeline(kind, number)["actions"]
    return [action["action"] for action in asked if action["actor"].startswith("cleaner")]


def test_autodevs_failed_run_is_asked_for_again_and_the_dataset_follows_once_autodev_took_it(world):
    number = world.dataset(run=False)
    world.stage(
        "dataset", number, "blocked", "mia-snippets issue #72: autodev stopped before it opened a pull request."
    )
    world.snippets.issue(
        72, number, ["snippets:needs-human"], said=["### snippets: stopped agent job 1 ended EXIT (exit 140)"]
    )
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "autodev's run failed" and case["state"] == "working"
    assert [step["do"] for step in case["data"]["steps"]] == ["snippets_go"] and cleaner_actions(world, number) == []
    assert "snippets:go" in world.snippets.labels(72) and len(world.snippets.comments[72]) == 2
    assert "**Cleaner:**" in world.timeline("dataset", number)["entries"][-1]["body"]
    world.snippets.issues[72]["labels"] = [{"name": "snippets:working"}]  # autodev read the label
    world.sweep()
    (case,) = world.cases()
    assert case["state"] == "waiting" and cleaner_actions(world, number) == ["resume"]
    assert len(world.snippets.comments[72]) == 2  # asked once


def test_a_dataset_is_not_run_again_when_autodev_does_not_take_the_command(world):
    number = world.dataset(run=False)
    world.stage(
        "dataset", number, "blocked", "mia-snippets issue #72: autodev stopped before it opened a pull request."
    )
    world.snippets.issue(72, number, ["snippets:needs-human"], said=["agent job 1 ended EXIT (exit 1)"])
    world.sweep(3.0)
    world.sweep(4.0)  # the label is still there after the quiet time
    (case,) = world.cases()
    assert case["state"] == "gave-up" and "autodev did not take it within 30 min" in case["data"]["needs"]
    assert cleaner_actions(world, number) == []


def test_a_merge_conflict_is_handed_to_autodev_as_a_revision(world):
    number = world.dataset(run=False)
    world.snippets.issue(72, number, ["snippets:needs-human"], said=["Approved, but merging failed: conflicts"])
    world.snippets.pull(44, 72, mergeable=False)
    world.stage(
        "dataset",
        number,
        "blocked",
        "snippet PR https://github.example/o/snippets/pull/44: its issue has `snippets:needs-human`",
    )
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "merge conflict" and world.snippets.comments[44][-1]["body"].startswith(
        "/revise Merge `main`"
    )


def test_a_dataset_whose_pull_request_merged_after_it_stopped_is_run_again(world):
    """zenodo-8235843 (2026-11-18): GitHub refused the merge once because the base branch moved, autodev labelled
    the issue needs-human and merged the pull request two minutes later; the dataset had stopped as blocked in
    between, its approved crops unsaved. The reader is on main: a new round matches it."""
    number = world.dataset(run=False)
    world.snippets.issue(72, number, state="closed", said=["Approved, but merging failed: Base branch was modified"])
    world.snippets.pull(44, 72, merged=True)
    world.stage(
        "dataset",
        number,
        "blocked",
        "snippet PR https://github.example/o/snippets/pull/44: its issue has `snippets:needs-human`",
    )
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "merged since" and cleaner_actions(world, number) == ["resume"]


def test_a_passing_cause_is_run_again_after_the_retry_time(world):
    number = world.dataset(run=False)
    world.stage("dataset", number, "failed", "fetch failed: read timed out after 300 s")
    world.sweep(3.0)
    world.sweep(4.0)
    assert world.cases()[0]["state"] == "working" and cleaner_actions(world, number) == []  # two hours are not over
    world.sweep(5.5)
    (case,) = world.cases()
    assert case["rule"] == "passing cause" and case["state"] == "waiting"
    assert cleaner_actions(world, number) == ["resume"]


def test_a_record_gap_waits_for_a_person_and_runs_again_once_the_record_changed(world):
    number = world.dataset(run=False)
    world.stage("dataset", number, "blocked", "match blocked: Someone has to decide what class the label arrays mark")
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "record gap" and case["state"] == "gave-up" and cleaner_actions(world, number) == []
    assert call(world.service, "GET", "/subjects")[1]["subjects"][0]["repair"]["needs"].startswith(
        "its catalog record needs a decision"
    )
    world.sweep()
    assert len(world.cases()) == 1  # nothing changed: nothing new
    changed = {
        "id": "rec-a",
        "data": {"download_url": "https://data.example.org/rec-a.zip"},
        "classes": ["nucleus"],
        **SHAPE,
    }
    records = {"records": [{"path": "rec-a.yaml", "record": changed}]}
    assert worker(world.service, "PUT", f"/records/{'c' * 40}", records)[0] == 200
    world.sweep()
    new, old = world.cases()
    assert (old["state"], old["data"]["note"]) == ("resolved", "its record changed")
    assert new["rule"] == "input changed" and cleaner_actions(world, number) == ["go"]


def test_snippet_work_nobody_follows_is_closed_pull_request_first(world):
    number = world.dataset(stage="done", run=False)
    world.snippets.issue(72, number)
    world.snippets.pull(44, 72)
    busy = world.dataset("rec-b", stage="done", run=False)
    world.snippets.issue(73, busy, ["snippets:working"])  # autodev is at work on it: left alone
    world.sweep()
    (case,) = world.cases()
    assert (case["rule"], case["key"], case["state"]) == ("left-over snippet work", "issue-72", "resolved")
    assert world.snippets.issues[72]["state"] == world.snippets.pulls[44]["state"] == "closed"
    assert "prepared through another reader" in world.snippets.comments[44][-1]["body"]
    assert world.snippets.issues[73]["state"] == "open"


def test_a_pull_request_opened_after_its_issue_was_closed_is_closed(world):
    number = world.dataset(stage="done", run=False)
    world.snippets.issue(72, number, state="closed")
    world.snippets.pull(45, 72)  # autodev's run went on after the close and opened it
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "pull request of a closed issue" and world.snippets.pulls[45]["state"] == "closed"


def test_a_request_is_not_set_going_over_a_dataset_that_stopped(world):
    request = act(world.service, "/prep", {"spec": PREP_TEXT})[1]["number"]  # names rec-a
    world.stage("prep", request, "requested")
    dataset = world.dataset("rec-a", stage="failed", run=False)
    world.stage("dataset", dataset, "failed", "fetch failed: read timed out")  # its own case runs it again later
    world.sweep()
    case = next(case for case in world.cases() if case["kind"] == "prep")
    assert case["rule"] == "no run" and [step["do"] for step in case["data"]["steps"]] == ["cancel", "kill_jobs"]
    assert cleaner_actions(world, request, "prep") == ["cancel"]  # no resume: it would run rec-a's round
    world.stage("dataset", dataset, "review")  # the dataset came to a person's turn
    world.sweep()
    assert cleaner_actions(world, request, "prep") == ["cancel", "resume"]


def test_a_request_whose_dataset_needs_a_person_needs_one_too(world):
    request = act(world.service, "/prep", {"spec": PREP_TEXT})[1]["number"]
    world.stage("prep", request, "requested")
    dataset = world.dataset("rec-a", stage="blocked", run=False)
    world.stage("dataset", dataset, "blocked", "match blocked: Someone has to decide the label classes")
    world.sweep()
    case = next(case for case in world.cases() if case["kind"] == "prep")
    assert case["state"] == "gave-up" and "its dataset `rec-a` could not be repaired" in case["data"]["needs"]
    assert cleaner_actions(world, request, "prep") == ["cancel"]


def test_the_cleaner_resumes_a_request_and_never_restarts_its_prepared_datasets(world):
    """spec resume-restart §9: a request whose text says `restart` would start every dataset it names over on a
    `go`, a prepared one too. The cleaner sends it a `resume`, which leaves a prepared dataset alone whatever
    the text says, and no `go` at all."""
    text = PREP_TEXT + "restart: true\n"
    request = act(world.service, "/prep", {"spec": text})[1]["number"]
    world.stage("prep", request, "requested")
    dataset = world.dataset("rec-a", stage="done", run=False)  # prepared: the resume leaves it as it is
    world.sweep()
    case = next(case for case in world.cases() if case["kind"] == "prep")
    assert cleaner_actions(world, request, "prep") == ["cancel", "resume"] and case["state"] == "waiting"
    assert world.temporal.sent[-1][:3] == ("prep", request, "go") and world.temporal.sent[-1][3].kind == "resume"
    assert cleaner_actions(world, dataset) == []


def test_a_case_that_holds_a_go_for_a_request_sends_it_a_resume(world):
    """A stored plan may hold a `go` for a request. A request's `go` starts every dataset it names over, so
    what the cleaner sends is the `resume`."""
    request = act(world.service, "/prep", {"spec": PREP_TEXT + "restart: true\n"})[1]["number"]
    world.stage("prep", request, "needs-human")
    world.dataset("rec-a", stage="done", run=False)  # prepared: nothing the resume would run
    revision = world.timeline("prep", request)["subject"]["revision"]
    data = {"seen": "it rests", "what": "stopped", "revision": revision, "plan": [{"do": "go"}], "steps": []}
    world.service.ledger.open_case("prep", request, f"prep-{request}", "passing cause", "open", data)
    world.sweep()
    assert cleaner_actions(world, request, "prep") == ["resume"]


# --- the diagnosis (spec §4.6) ---

QUIET = "at `developing`"  # a round that is open, with nothing going on and no rule that says why


def test_what_no_rule_covers_is_diagnosed_and_only_the_menu_is_carried_out(world):
    number = world.dataset()
    world.job(number)
    world.csub.alive.clear()  # its job is over
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "diagnosis" and case["state"] == "working" and QUIET in case["data"]["seen"]
    ((key, spec),) = world.submitted
    evidence = Path(spec.workspace, "evidence")
    assert key == f"cleaner-case-{case['id']}-0" and spec.agent == "claude" and spec.allow_hosts == []
    assert "You are the diagnosing agent" in (evidence / "question.md").read_text()
    assert QUIET in (evidence / "case.md").read_text() and (evidence / "jobs" / "crops-1.json").is_file()
    assert json.loads((evidence / "run.json").read_text())["run_id"] == "run-1"
    world.sweep()
    assert len(world.submitted) == 1 and world.cases()[0]["state"] == "working"  # the job runs
    asked = [{"do": "cancel"}, {"do": "go", "note": "read the labels as zyx"}, {"do": "approve"}, {"do": "rm -rf"}]
    world.answer({"diagnosis": "the round waits on a signal that was lost", "cause": "defect", "remedies": asked})
    world.sweep()
    (case,) = world.cases()
    assert [step["do"] for step in case["data"]["steps"]] == ["diagnose", "cancel", "kill_jobs"]
    assert [step["do"] for step in case["data"]["plan"]] == ["go"]  # nothing that is not on the menu
    assert case["data"]["diagnosis"]["cause"] == "defect" and case["data"]["diagnosis"]["job"].startswith(
        "cleaner/case-"
    )
    (said,) = [
        entry["body"] for entry in world.timeline("dataset", number)["entries"] if "**Cleaner:**" in entry["body"]
    ]
    assert "Diagnosis (defect): the round waits on a signal that was lost" in said and "#job/cleaner/case-" in said
    world.temporal.runs.pop(number)
    world.stage("dataset", number, "cancelled")  # the round answered the cancel
    world.sweep()
    go = world.timeline("dataset", number)["actions"][-1]
    assert (go["action"], go["actor"], go["payload"]["note"]) == ("go", "cleaner-advice", "read the labels as zyx")
    spent = call(world.service, "GET", "/budgets")[1]["budgets"]
    assert [row["spent_usd"] for row in spent if row["root"].startswith("cleaner-")] == [0.5]


def test_a_diagnosis_that_proposes_nothing_says_what_a_person_must_do(world):
    world.activities.cfg.autonomy.price_factor = 0.1  # here a dollar Claude Code counts costs ten cents
    number = world.dataset()
    world.sweep()
    world.answer({"diagnosis": "the dataset has no labels at all", "cause": "data", "remedies": [], "needs": "drop it"})
    world.sweep()
    (case,) = world.cases()
    assert case["state"] == "gave-up" and case["data"]["needs"] == "drop it"
    (said,) = [
        entry["body"] for entry in world.timeline("dataset", number)["entries"] if "**Cleaner:**" in entry["body"]
    ]
    assert "Diagnosis (data): the dataset has no labels at all What it takes: drop it" in said  # once, in one entry
    spent = call(world.service, "GET", "/budgets")[1]["budgets"]  # the job counted 0.5
    assert [row["spent_usd"] for row in spent if row["root"].startswith("cleaner-")] == [pytest.approx(0.05)]
    assert cleaner_actions(world, number) == []


def test_a_defect_is_kept_on_its_case_when_no_repository_is_named(world):
    world.dataset()
    world.sweep()
    defect = {"title": "the look never fires", "evidence": "run.json has no timer"}
    world.answer(
        {
            "diagnosis": "a timer is missing",
            "cause": "defect",
            "remedies": [{"do": "wait", "hours": 1}],
            "defect": defect,
        }
    )
    world.sweep()
    (case,) = world.cases()
    reported = next(step for step in case["data"]["steps"] if step["do"] == "report_defect")
    assert reported["title"] == "the look never fires" and "no `cleaner.defects_repo`" in reported["result"]
    assert case["data"]["diagnosis"]["defect"]["evidence"] == "run.json has no timer"


def test_an_answer_that_cannot_be_read_ends_at_a_person(world):
    world.dataset()
    world.sweep()
    world.answer("I could not decide.")
    world.sweep()
    (case,) = world.cases()
    assert case["state"] == "gave-up" and "no answer that can be read" in case["data"]["needs"]


def test_a_diagnosis_waits_while_the_days_budget_is_spent(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch, max_usd_per_day=1)  # one diagnosis may cost 2
    world.dataset()
    world.sweep()
    world.sweep(9.0)
    (case,) = world.cases()
    assert case["state"] == "working" and world.submitted == [] and "needs" not in case["data"]


def test_a_defect_is_filed_once_in_the_repository_named_for_it(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch, defects_repo="o/code")
    defect = {"title": "The look never fires", "evidence": "run.json has no timer"}
    answer = {
        "diagnosis": "a timer is missing",
        "cause": "defect",
        "remedies": [{"do": "wait", "hours": 1}],
        "defect": defect,
    }
    for record in ("rec-a", "rec-b"):
        world.dataset(record)
    world.sweep()
    for _ in range(2):  # one diagnosis job at a time ends
        job = next(case["data"]["diagnosis_job"] for case in world.cases() if "diagnosis_job" in case["data"])
        Path(job["workspace"], "claude.json").write_text(
            json.dumps({"result": json.dumps(answer), "total_cost_usd": 0.1})
        )
        world.csub.alive.discard(job["id"])
        world.sweep()
    (filed,) = [issue for issue in world.issues.issues.values() if "title" in issue]
    assert filed["title"] == "cleaner: The look never fires" and "run.json has no timer" in filed["body"]
    assert [comment["body"].split(".")[0] for comment in world.issues.comments[filed["number"]]] == ["Seen again"]
    results = [
        step["result"] for case in world.cases() for step in case["data"]["steps"] if step["do"] == "report_defect"
    ]
    assert sorted(results) == [f"added to issue #{filed['number']}", f"filed as issue #{filed['number']}"]


# --- what the cleaner must not do (the review of the first build) ---


def test_a_job_of_a_round_at_a_persons_turn_is_not_left_over(world):
    number = world.dataset(stage="review")
    world.job(number, "judge-1")  # the crop judge runs after the stage was written
    world.sweep(0.5)
    assert world.cases() == [] and world.csub.killed == []


def test_a_go_the_round_has_not_answered_yet_is_neither_resolved_nor_not_helped(world):
    number = world.dataset(run=False)
    world.sweep()  # cancel, go: the subject reads `cancelled` until the new round writes its first stage
    world.sweep(3.2)
    assert world.cases()[0]["state"] == "waiting" and world.submitted == []
    world.temporal.queues["mia-data"] = False  # and while its worker is away nothing is concluded at all
    world.sweep(9.0)
    assert world.cases()[0]["state"] == "waiting"
    world.temporal.queues["mia-data"] = True
    world.sweep(9.0)  # six hours and nothing moved: the remedy did not help
    assert world.cases()[0]["state"] == "working" and "nothing changed in 2 h" in world.cases()[0]["data"]["seen"]
    world.sweep(9.2)
    assert len(world.submitted) == 1  # its diagnosis
    assert cleaner_actions(world, number) == ["cancel", "resume"]


def test_runs_that_freeze_one_after_another_are_a_finding_from_the_second_on(world):
    first = world.dataset("rec-a")
    world.temporal.runs[first] = {**RUN, "task": 40}
    world.sweep()
    assert world.temporal.terminated == [("dataset", first, "run-1")]
    second = world.dataset("rec-b")  # frozen by the same cause, due a pass later
    world.temporal.runs[second] = {**RUN, "task": 12}
    report = world.sweep()
    assert [finding["id"] for finding in report["findings"]] == ["tasks dataset"]
    assert world.temporal.terminated == [("dataset", first, "run-1")] and len(world.cases()) == 1


def test_a_step_that_fails_on_every_pass_ends_its_case_at_a_person(world):
    number = world.dataset(run=False)
    world.job(number)

    def refuse(job_ids):
        raise RuntimeError("csub: not_found")

    world.csub.kill = refuse
    report = world.sweep(3.0)
    assert any("csub: not_found" in error for error in report["errors"])
    (case,) = world.cases()
    assert case["state"] == "working" and [step["do"] for step in case["data"]["steps"]] == ["cancel"]  # kept
    world.sweep(6.0)
    (case,) = world.cases()
    assert case["state"] == "gave-up" and "kept failing for 2 h: csub: not_found" in case["data"]["needs"]


def test_an_issue_number_in_a_timeline_that_is_not_this_datasets_is_not_acted_on(world):
    number = world.dataset(run=False)
    world.stage("dataset", number, "blocked", "the agent wrote: see mia-snippets issue #99, autodev stopped")
    world.snippets.issue(99, number + 1, ["snippets:needs-human"], said=["agent job 1 ended EXIT (exit 1)"])
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "diagnosis" and "party" not in case["data"]
    assert world.snippets.labels(99) == ["snippets:needs-human"] and len(world.snippets.comments[99]) == 1


def test_runs_are_not_judged_unrunnable_while_a_worker_owes_a_restart(world, monkeypatch):
    stray = {"namespace": "mia-buffers", "id": "new/1", "run_id": "run-0", "type": "MiaNewThing"}
    theirs = {
        "namespace": "mia-buffers",
        "id": "temporal-sys-scheduler:x",
        "run_id": "r",
        "type": "temporal-sys-scheduler-workflow",
    }
    world.temporal.open = [stray, theirs]
    worker(world.service, "PUT", "/workers/mia-buffers", {"commit": "a" * 40, "code": "0" * 16})
    report = world.sweep()
    assert [finding["id"] for finding in report["findings"]] == ["restart mia-buffers"] and world.cases() == []
    worker(world.service, "PUT", "/workers/mia-buffers", {"commit": "c" * 40, "code": "b" * 16})
    world.temporal.running = True
    world.sweep()
    assert [case["key"] for case in world.cases()] == ["mia-buffers/new/1"]  # ours by its name; never Temporal's own


def test_a_request_that_selects_its_datasets_is_never_run_again_by_the_cleaner(world):
    import yaml

    from tests.test_buffers_api import SPEC_TEXT

    spec = yaml.safe_load(PREP_TEXT)
    spec.pop("records")
    spec["select"] = yaml.safe_load(SPEC_TEXT)["select"]
    request = act(world.service, "/prep", {"spec": yaml.safe_dump(spec)})[1]["number"]
    world.stage("prep", request, "requested")
    world.sweep()
    (case,) = world.cases()
    assert case["state"] == "gave-up" and "it selects its datasets" in case["data"]["needs"]
    assert cleaner_actions(world, request, "prep") == ["cancel"]


def gave_up(world, kind, key, plan):
    body = {"kind": kind, "key": key, "rule": "left over", "state": "gave-up", "data": {"plan": plan, "steps": []}}
    return worker(world.service, "POST", "/cases", body)[1]


def test_a_left_behind_job_the_agent_gave_up_on_is_resolved_once_it_is_gone(world):
    world.csub.alive.add("901")
    case = gave_up(world, "job", "issue-7/i7-e1/crops-1", [{"do": "kill_job", "job_id": "901", "address": "a"}])
    world.sweep()
    assert world.cases()[0]["state"] == "gave-up"  # the job still runs
    world.csub.alive.discard("901")
    world.sweep()
    (after,) = world.cases()
    assert (after["id"], after["state"], after["data"]["note"]) == (case["id"], "resolved", "it is gone")


def test_a_left_behind_run_the_agent_gave_up_on_stays_until_a_pass_that_looked_finds_it_gone(world):
    stray = {"namespace": "mia-buffers", "id": "old/1", "run_id": "run-0", "type": "MiaBufferPoll"}
    world.temporal.open = [stray]
    gave_up(world, "run", "mia-buffers/old/1", [{"do": "terminate"}])
    world.sweep()
    assert world.cases()[0]["state"] == "gave-up"  # it is still there
    world.temporal.open = []
    worker(world.service, "PUT", "/workers/mia-buffers", {"commit": "a" * 40, "code": "0" * 16})
    report = world.sweep()  # a worker owes a restart: open runs are not looked at, so nothing is known to be gone
    assert [finding["id"] for finding in report["findings"]] == ["restart mia-buffers"]
    assert world.cases()[0]["state"] == "gave-up"
    worker(world.service, "PUT", "/workers/mia-buffers", {"commit": "c" * 40, "code": "b" * 16})
    world.sweep()
    assert (world.cases()[0]["state"], world.cases()[0]["data"]["note"]) == ("resolved", "it is gone")
