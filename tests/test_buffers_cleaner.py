"""The cleaner's rules against the incidents they come from (spec cleaner §2, §4.3 to §4.5)."""

from datetime import UTC, datetime

from mia_buffers.cleaner import (
    ALL_KINDS,
    SNIPPETS,
    entry_text,
    evidence_findings,
    last_person,
    plan_for,
    plan_words,
    service_findings,
    sighting,
)
from mia_buffers.config import CleanerCfg

NOW = 1_800_000_000.0
CFG = CleanerCfg()
QUEUES = {"mia-data": ["dataset"], "mia-buffers": ["buffer", "prep"], "snippets": [SNIPPETS]}
HEALTHY = {"temporal": True, "queues": {"mia-data": True, "mia-buffers": True, "snippets": True}}


def at(minutes_ago: float) -> str:
    return datetime.fromtimestamp(NOW - minutes_ago * 60, UTC).isoformat()


def findings(health=HEALTHY, workers=None, schedules=(), checkout="b" * 40, pause=False):
    found = service_findings(health, workers or {}, list(schedules), checkout, pause, QUEUES, NOW)
    return {finding["id"]: finding for finding in found}


def test_a_healthy_installation_has_no_finding():
    workers = {"mia-data": {"commit": "b" * 40}, "mia-buffers": {"commit": None}}
    schedules = [{"id": "keeper", "every_s": 1800.0, "last": NOW - 1700}, {"id": "new", "every_s": 600.0, "last": None}]
    assert findings(workers=workers, schedules=schedules) == {}


def test_temporal_away_is_the_one_finding_and_stops_everything():
    found = findings(health={"temporal": False, "queues": {}}, pause=True)
    assert list(found) == ["temporal"] and found["temporal"]["kinds"] == [*ALL_KINDS, SNIPPETS]


def test_a_queue_without_a_worker_skips_the_kinds_its_worker_runs():
    # the snippets and autodev workers were dead for two days; six datasets looked slow
    health = {"temporal": True, "queues": {"mia-data": True, "mia-buffers": False, "snippets": False}}
    found = findings(health=health)
    assert found["worker snippets"]["kinds"] == [SNIPPETS] and found["worker mia-buffers"]["kinds"] == [
        "buffer",
        "prep",
    ]
    assert "worker mia-data" not in found


def test_a_worker_on_another_commit_than_the_checkout_owes_a_restart():
    found = findings(workers={"mia-data": {"commit": "a" * 40}, "mia-buffers": {"commit": "b" * 40}})
    assert list(found) == ["restart mia-data"] and found["restart mia-data"]["kinds"] == ["dataset"]
    assert (
        "`aaaaaaa`" in found["restart mia-data"]["text"] and "mia restart mia-data" in found["restart mia-data"]["text"]
    )
    assert findings(workers={"mia-data": {"commit": "a" * 40}}, checkout=None) == {}  # the checkout cannot be read


def test_a_schedule_three_intervals_late_and_the_pause_are_findings():
    found = findings(schedules=[{"id": "mia-data-poll", "every_s": 120.0, "last": NOW - 400}], pause=True)
    assert found["schedule mia-data-poll"]["kinds"] == [] and found["pause"]["kinds"] == [*ALL_KINDS, SNIPPETS]


def item(status="working", stage="developing", kind="dataset", minutes=45.0, run="open", jobs=(), party=None, **more):
    subject = {"kind": kind, "number": 224, "key": "rec-a", "stage": stage, "status": status, "revision": 3}
    entry = {
        "at": at(minutes),
        "stage": stage,
        "round": "i224-e9",
        "body": "**Stage:** `data:x`\n\nwhy it stopped <!-- m -->",
    }
    open_run = {"status": "RUNNING", "run_id": "run-1", "task": None, "activities": []}
    return {
        "subject": subject,
        "entries": [entry],
        "actions": [],
        "run": {**open_run, **more} if run == "open" else run,
        "jobs": list(jobs),
        "party": party,
    }


def test_a_working_subject_is_not_looked_at_before_the_quiet_time():
    assert sighting(item(minutes=29, run=None), CFG, NOW) is None


def test_no_run_behind_a_working_subject_is_cancelled_and_run_again():
    # empiar-10554 read `developing` for four days with nothing running
    evidence = item(run=None)
    found = sighting(evidence, CFG, NOW)
    assert found["rule"] == "no run" and "no open run" in found["seen"] and "45 min" in found["seen"]
    assert [step["do"] for step in plan_for(found, evidence)] == ["cancel", "kill_jobs", "go"]
    assert (
        sighting(item(run={"status": "TERMINATED", "run_id": "r", "task": None, "activities": []}), CFG, NOW)["rule"]
        == "no run"
    )


def test_a_run_whose_workflow_task_keeps_failing_is_stopped_and_run_again():
    # every workflow task failed for 94 minutes after the checkout changed under the workers
    evidence = item(task=40)
    found = sighting(evidence, CFG, NOW)
    assert found["rule"] == "frozen run" and "40 times" in found["seen"]
    assert (
        plan_words(plan_for(found, evidence))
        == "force-stop the run, then end the round's cluster jobs, then run it again"
    )


def test_an_activity_that_retries_for_ever_says_which_and_why():
    # `read_feedback` was never registered on the worker and retried for six hours
    unregistered = {
        "type": "read_feedback",
        "attempt": 412,
        "failure": "Activity function read_feedback is not registered",
    }
    evidence = item(activities=[unregistered])
    found = sighting(evidence, CFG, NOW)
    assert found["rule"] == "activity retrying" and "attempt 412" in found["seen"]
    (step,) = plan_for(found, evidence)
    assert step["do"] == "needs" and "restart the worker" in step["text"] and "`read_feedback`" in step["text"]
    refused = {"type": "put_dataset", "attempt": 30, "failure": "RegistryError: 503 Service Unavailable"}
    evidence = item(activities=[refused])
    assert plan_for(sighting(evidence, CFG, NOW), evidence) == [{"do": "wait", "hours": 1.0}]
    odd = {"type": "save", "attempt": 30, "failure": "PermissionError: [Errno 13] Permission denied"}
    evidence = item(activities=[odd])
    assert plan_for(sighting(evidence, CFG, NOW), evidence) == [{"do": "diagnose"}]


def test_a_subject_with_something_going_on_is_not_quiet():
    assert sighting(item(jobs=[{"address": "issue-224/i224-e9/crops-1", "live": True}]), CFG, NOW) is None
    assert sighting(item(activities=[{"type": "await_job", "attempt": 1, "failure": ""}]), CFG, NOW) is None
    assert sighting(item(party={"working": True}), CFG, NOW) is None  # autodev's agent round is running


def test_a_working_subject_with_nothing_going_on_goes_to_the_diagnosis():
    evidence = item(jobs=[{"address": "issue-224/i224-e9/crops-1", "live": False}], party={"working": False})
    found = sighting(evidence, CFG, NOW)
    assert (
        found == {"what": "quiet", "rule": None, "seen": found["seen"]} and "no job, activity or party" in found["seen"]
    )
    assert plan_for(found, evidence) == [{"do": "diagnose"}]


def test_a_stopped_subject_is_seen_after_the_stopped_time_with_its_own_words():
    assert sighting(item(status="stuck", stage="blocked", minutes=10), CFG, NOW) is None
    found = sighting(item(status="stuck", stage="blocked", minutes=120), CFG, NOW)
    assert found["what"] == "stopped" and found["seen"] == "stopped at `blocked` 2.0 h ago: why it stopped"


def test_what_is_a_persons_turn_or_done_is_never_stuck():
    for status in ("waiting", "done", "not started", "shelved"):
        assert sighting(item(status=status, minutes=10_000, run=None), CFG, NOW) is None


def test_findings_across_subjects():
    frozen = [item(task=9), {**item(task=12), "subject": {"kind": "dataset", "key": "rec-b"}}]
    (found,) = evidence_findings(frozen, CFG.pend_hours, NOW)
    assert found["id"] == "tasks dataset" and found["kinds"] == ["dataset"] and "rec-a, rec-b" in found["text"]
    assert evidence_findings([item(task=9)], CFG.pend_hours, NOW) == []  # one frozen run is that run's problem
    refused = {"type": "put_dataset", "attempt": 30, "failure": "connection refused"}
    (found,) = evidence_findings([item(activities=[refused]), item(activities=[refused])], CFG.pend_hours, NOW)
    assert found["id"] == "service" and found["kinds"] == []
    waiting = {"address": "issue-9/i9-e1/crops-1", "state": "PENDING", "submitted": NOW - 7 * 3600, "live": True}
    fresh = {**waiting, "submitted": NOW - 3600}
    (found,) = evidence_findings([item(jobs=[waiting, fresh])], CFG.pend_hours, NOW)
    assert found["id"] == "cluster queue" and "1 jobs" in found["text"]


def test_entry_text_and_the_last_person():
    long = {"body": "**Result:** `data:developing`\n\n**blocked**: " + "x" * 700 + "\n<!-- mia-data: stage -->"}
    assert (
        entry_text(long).startswith("**blocked**: xxx")
        and entry_text(long).endswith("…")
        and len(entry_text(long)) == 601
    )
    actions = [{"actor": "page", "at": at(300)}, {"actor": "prep:7", "at": at(200)}, {"actor": "cleaner", "at": at(5)}]
    assert last_person(actions) == NOW - 200 * 60 and last_person([{"actor": "debugger", "at": at(1)}]) is None
