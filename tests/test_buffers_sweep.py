"""One pass of the cleaner against a buffer-api in this process: what it opens a case for, what it sends, and
what it leaves alone (spec cleaner §4)."""

import asyncio
import time
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
from tests.test_buffers_api import PREP_TEXT, REF_A, Gateway, act, call, worker
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

    def pull(self, number, issue, mergeable=True):
        head = {"ref": f"snippets/issue-{issue}"}
        self.pulls[number] = {
            "number": number,
            "state": "open",
            "head": head,
            "mergeable": mergeable,
            "user": {"login": "bot"},
        }
        self.issues[number] = {"number": number, "state": "open", "labels": [], "body": "", "pull_request": {}}
        self.comments.setdefault(number, [])

    def labels(self, number):
        return [label["name"] for label in self.issues[number]["labels"]]

    async def get_issue(self, number):
        return self.issues[number]

    async def get_pr(self, number):
        return self.pulls[number]

    async def find_pr(self, branch):
        return next(
            (pull for pull in self.pulls.values() if pull["head"]["ref"] == branch and pull["state"] == "open"), None
        )

    async def list_issues(self, state):
        return [issue for issue in self.issues.values() if issue["state"] == state and "pull_request" not in issue]

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
        self.activities = SimpleNamespace(
            cfg=cfg, registry=registry, jobs=SimpleNamespace(client=self.csub), gh_snippets=self.snippets, bot="bot"
        )
        monkeypatch.setattr(sweep_module, "head_commit", lambda: "b" * 40)
        monkeypatch.setattr(sweep_module.autonomy, "paused", lambda manifest: False)
        records = [
            {
                "path": f"{name}.yaml",
                "record": {"id": name, "data": {"download_url": f"https://data.example.org/{name}.zip"}},
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
    assert [step["do"] for step in case["data"]["plan"]] == ["cancel", "kill_jobs", "go"]
    assert report["opened"] == ["rec-a: no run: cancel the round, then end the round's cluster jobs, then run it again"]
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
    assert cleaner == [("cancel", f"cleaner/{case['id']}/0"), ("go", f"cleaner/{case['id']}/2")]
    assert world.temporal.sent[-1][:3] == ("dataset", number, "restart")
    assert case["state"] == "waiting" and [step["do"] for step in case["data"]["steps"]] == [
        "cancel",
        "kill_jobs",
        "go",
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
    assert [step["do"] for step in case["data"]["steps"]] == ["cancel", "kill_jobs", "go"] and case[
        "state"
    ] == "waiting"


def test_a_job_that_cannot_be_seen_dead_ends_the_case_at_a_person(world):
    number = world.dataset(run=False)
    world.job(number)
    world.csub.kill = lambda job_ids: None  # the kill does nothing
    world.sweep(3.0)
    world.sweep(6.0)
    (case,) = world.cases()
    assert case["state"] == "needs-person" and "end the round's cluster jobs was not possible" in case["data"]["needs"]
    assert "needs a person" in world.timeline("dataset", number)["entries"][-1]["body"]
    assert all(sent[3].actor == "page" for sent in world.temporal.sent if sent[2] == "restart")  # no go of its own


def test_a_frozen_run_is_stopped_by_its_run_id(world):
    number = world.dataset()
    world.temporal.runs[number] = {**RUN, "task": 40}
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "frozen run" and world.temporal.terminated == [("dataset", number, "run-1")]
    assert [step["do"] for step in case["data"]["steps"]] == ["stop", "kill_jobs", "go"]


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


def test_a_remedy_that_did_not_help_ends_at_a_person(world):
    number = world.dataset(run=False)
    world.sweep()
    world.stage("dataset", number, "failed")
    world.sweep()
    (case,) = world.cases()
    assert case["state"] == "needs-person" and case["data"]["diagnosed"] is True
    assert "After the remedy: stopped at `failed`" in case["data"]["seen"]


def test_the_weekly_limit_stops_the_cleaner_from_running_a_subject_again(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch, max_remedies=1)
    number = world.dataset(run=False)
    world.sweep()  # cancel, go
    world.stage("dataset", number, "matching")  # the new round, which loses its run too
    world.sweep(6.0)
    world.sweep(9.0)
    states = {case["rule"]: case["state"] for case in world.cases()}
    assert list(states.values()).count("needs-person") == 1
    gos = [
        action
        for action in world.timeline("dataset", number)["actions"]
        if action["actor"] == "cleaner" and action["action"] == "go"
    ]
    assert len(gos) == 1


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
    return [action["action"] for action in world.timeline(kind, number)["actions"] if action["actor"] == "cleaner"]


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
    assert case["state"] == "waiting" and cleaner_actions(world, number) == ["go"]
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
    assert case["state"] == "needs-person" and "autodev did not take it within 30 min" in case["data"]["needs"]
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


def test_a_passing_cause_is_run_again_after_the_retry_time(world):
    number = world.dataset(run=False)
    world.stage("dataset", number, "failed", "fetch failed: read timed out after 300 s")
    world.sweep(3.0)
    world.sweep(4.0)
    assert world.cases()[0]["state"] == "working" and cleaner_actions(world, number) == []  # two hours are not over
    world.sweep(5.5)
    (case,) = world.cases()
    assert case["rule"] == "passing cause" and case["state"] == "waiting" and cleaner_actions(world, number) == ["go"]


def test_a_record_gap_waits_for_a_person_and_runs_again_once_the_record_changed(world):
    number = world.dataset(run=False)
    world.stage("dataset", number, "blocked", "match blocked: Someone has to decide what class the label arrays mark")
    world.sweep()
    (case,) = world.cases()
    assert case["rule"] == "record gap" and case["state"] == "needs-person" and cleaner_actions(world, number) == []
    assert call(world.service, "GET", "/needs")[1]["needs"][0]["cleaner"].startswith(
        "its catalog record needs a decision"
    )
    world.sweep()
    assert len(world.cases()) == 1  # nothing changed: nothing new
    changed = {"id": "rec-a", "data": {"download_url": "https://data.example.org/rec-a.zip"}, "classes": ["nucleus"]}
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
    assert cleaner_actions(world, request, "prep") == ["cancel"]  # no go: it would start rec-a's round
    world.stage("dataset", dataset, "review")  # the dataset came to a person's turn
    world.sweep()
    assert cleaner_actions(world, request, "prep") == ["cancel", "go"]


def test_a_request_whose_dataset_needs_a_person_needs_one_too(world):
    request = act(world.service, "/prep", {"spec": PREP_TEXT})[1]["number"]
    world.stage("prep", request, "requested")
    dataset = world.dataset("rec-a", stage="blocked", run=False)
    world.stage("dataset", dataset, "blocked", "match blocked: Someone has to decide the label classes")
    world.sweep()
    case = next(case for case in world.cases() if case["kind"] == "prep")
    assert case["state"] == "needs-person" and "its dataset `rec-a` needs a person" in case["data"]["needs"]
    assert cleaner_actions(world, request, "prep") == ["cancel"]


def test_a_request_that_says_again_is_never_run_again_by_the_cleaner(world):
    text = PREP_TEXT + "again: true\n"
    request = act(world.service, "/prep", {"spec": text})[1]["number"]
    world.stage("prep", request, "requested")
    world.sweep()
    (case,) = world.cases()
    assert case["state"] == "needs-person" and "its text says `again`" in case["data"]["needs"]
    assert cleaner_actions(world, request, "prep") == ["cancel"]
