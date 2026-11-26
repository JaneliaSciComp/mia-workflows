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
    standing,
)
from mia_buffers.config import CleanerCfg
from mia_data.jobspec import NoReachableFiles

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
    workers = {"mia-data": {"commit": "c" * 40, "code": "b" * 40}, "mia-buffers": {"commit": None, "code": None}}
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


def test_incident_2026_11_17_a_snippets_worker_on_old_code_owes_a_restart_and_holds_its_issues():
    """The snippets worker ran the code of an earlier commit for seven hours: every agent job that ended in an
    error failed its workflow task for good, four snippets issues read `working` with no job, and their
    datasets read `developing`. A worker that says what it started on is compared like any other."""
    old = {"commit": "a" * 40, "code": "0" * 16}
    found = findings(workers={"snippets": old, "mia-data": {"commit": "c" * 40, "code": "b" * 40}})
    assert list(found) == ["restart snippets"] and found["restart snippets"]["kinds"] == [SNIPPETS]
    assert "mia restart snippets" in found["restart snippets"]["text"]


def test_a_worker_on_another_commit_than_the_checkout_owes_a_restart():
    old = {"commit": "a" * 40, "code": "0" * 16}  # the code it loaded is not the checkout's (`b…`) any more
    found = findings(workers={"mia-data": old, "mia-buffers": {"commit": "c" * 40, "code": "b" * 40}})
    assert list(found) == ["restart mia-data"] and found["restart mia-data"]["kinds"] == ["dataset"]
    assert (
        "`aaaaaaa`" in found["restart mia-data"]["text"] and "mia restart mia-data" in found["restart mia-data"]["text"]
    )
    assert findings(workers={"mia-data": old}, checkout=None) == {}  # the checkout cannot be read


def test_incident_2026_11_26_a_worker_row_of_a_service_that_left_the_installation_owes_no_restart():
    """The ledger kept the last heartbeat of an `autodev` service that `mia.yaml` no longer names. Once the
    checkout's code moved on, every sweep said that worker owed a restart, which nobody could give it."""
    old = {"commit": "a" * 40, "code": "0" * 16}
    assert findings(workers={"autodev": old, "mia-data": {"commit": "c" * 40, "code": "b" * 40}}) == {}


def test_a_schedule_three_intervals_late_and_the_pause_are_findings():
    found = findings(schedules=[{"id": "mia-data-poll", "every_s": 120.0, "last": NOW - 400}], pause=True)
    assert found["schedule mia-data-poll"]["kinds"] == [] and found["pause"]["kinds"] == [*ALL_KINDS, SNIPPETS]


def item(phase="working", stage="developing", kind="dataset", minutes=45.0, run="open", jobs=(), party=None, **more):
    subject = {"kind": kind, "number": 224, "key": "rec-a", "stage": stage, "phase": phase, "revision": 3}
    subject["shelved"] = None
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
    assert [step["do"] for step in plan_for(found, evidence)] == ["cancel", "kill_jobs", "resume"]
    request = item(kind="prep", stage="requested", run=None)
    assert [step["do"] for step in plan_for(sighting(request, CFG, NOW), request)][-1] == "resume"
    buffer = item(kind="buffer", stage="sourcing", run=None)  # a buffer round takes no resume: it is planned again
    assert [step["do"] for step in plan_for(sighting(buffer, CFG, NOW), buffer)][-1] == "go"
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
        plan_words(plan_for(found, evidence)) == "force-stop the run, then end the round's cluster jobs, then resume it"
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
    assert plan_for(sighting(evidence, CFG, NOW), evidence) == [{"do": "wait", "hours": 1.0}, {"do": "diagnose"}]
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
    assert sighting(item(phase="stopped", stage="blocked", minutes=10), CFG, NOW) is None
    found = sighting(item(phase="stopped", stage="blocked", minutes=120), CFG, NOW)
    assert found["what"] == "stopped" and found["seen"] == "stopped at `blocked` 2.0 h ago: why it stopped"


def test_what_is_a_persons_turn_or_done_is_never_stuck():
    for phase in ("waiting", "done", "not started"):
        assert sighting(item(phase=phase, minutes=10_000, run=None), CFG, NOW) is None
    held = item(phase="stopped", stage="blocked", minutes=10_000, run=None)
    held["subject"]["shelved"] = {"reason": "later"}
    assert sighting(held, CFG, NOW) is None


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


# --- stopped rounds: which remedy a stop calls for (spec §4.5, the incidents of §2 B) ---


def stopped(said, party=None, worker=None, earlier=(), minutes=120.0, hosts=()):
    evidence = item(phase="stopped", stage="blocked", minutes=minutes, party=party)
    evidence["subject"]["hosts"] = list(hosts)  # the hosts its jobs were refused that no one approved yet
    evidence["entries"][0]["body"] = f"**Result:** `data:developing`\n\n**blocked**: {said}"
    evidence |= {"worker": worker, "earlier": list(earlier), "retry_hours": CFG.retry_hours}
    found = sighting(evidence, CFG, NOW)
    return found["rule"], plan_for(found, evidence)


AUTODEV_STOPPED = {"issue": 72, "state": "open", "stopped": True, "pr": None, "mergeable": None, "last_word": ""}


def test_autodevs_failed_run_is_asked_for_again_once_per_issue():
    # air-ug-…: "Refusing to create empty bundle"; empiar-10554: agent job ended EXIT (exit 140)
    said = "mia-snippets issue #72: autodev stopped before it opened a pull request."
    party = {
        **AUTODEV_STOPPED,
        "last_word": "### snippets: stopped **Error:** agent job 155317003 ended EXIT (exit 140)",
    }
    rule, plan = stopped(said, party)
    assert rule == "autodev's run failed"
    assert plan == [{"do": "snippets_go", "issue": 72}, {"do": "await_autodev", "issue": 72}, {"do": "resume"}]
    before = [{"rule": rule, "data": {"steps": [{"do": "snippets_go", "issue": 72}]}}]
    assert stopped(said, party, earlier=before)[1] == [{"do": "diagnose"}]
    other = [{"rule": rule, "data": {"steps": [{"do": "snippets_go", "issue": 49}]}}]
    assert stopped(said, party, earlier=other)[1][0]["do"] == "snippets_go"  # another issue's try does not count


def test_a_merge_refused_for_conflicts_asks_autodev_to_merge_main():
    # empiar-10070 waited seven hours at `merging` for a pull request that could not merge
    party = {**AUTODEV_STOPPED, "pr": 44, "mergeable": False, "last_word": "Approved, but merging failed: conflicts"}
    rule, plan = stopped("snippet PR #44: its issue has `snippets:needs-human`", party)
    assert rule == "merge conflict" and [step["do"] for step in plan] == ["revise", "await_autodev", "resume"]
    spent = {**party, "last_word": "Needs a human: revision limit (3) reached; merging failed: conflicts"}
    rule, plan = stopped("snippet PR #44: its issue has `snippets:needs-human`", spent)
    # with the pull request closed the round has no reader to keep: it starts over, on today's readers
    assert rule == "merge conflict" and [step["do"] for step in plan] == ["close", "go"] and plan[0]["pr"] == 44


def test_a_pull_request_that_merged_after_the_stop_runs_the_dataset_again_once():
    # zenodo-8235843: stopped on autodev's needs-human 46 seconds before autodev's next try merged the pull request
    said = "snippet PR #230: its issue has `snippets:needs-human`. Its last comment: merging failed: 405"
    party = {"issue": 229, "state": "closed", "stopped": True, "pr": 230, "mergeable": None, "merged": True}
    # resumed: the round finds the merge and saves the crops that were approved, with no job
    assert stopped(said, party) == ("merged since", [{"do": "resume"}])
    before = [{"rule": "merged since", "data": {"party": {"issue": 229, "pr": 230}}}]
    assert stopped(said, party, earlier=before)[1] == [{"do": "diagnose"}]
    # a stop that came after the merge is not about the merge: the crops at the merge commit failed
    assert stopped("the merge commit's crops failed; nothing is saved", party)[0] != "merged since"
    # and a pull request that is still open is the conflict rule's, or nobody's
    assert stopped(said, {**party, "state": "open", "merged": False})[0] != "merged since"


def test_autodev_at_its_revision_limit_goes_to_the_diagnosis():
    party = {**AUTODEV_STOPPED, "pr": 53, "mergeable": True, "last_word": "Needs a human: revision limit (3) reached"}
    assert stopped("snippet PR #53: its issue has `snippets:needs-human`", party) == (
        "revision limit",
        [{"do": "diagnose"}],
    )
    assert stopped("autodev left nothing", {**AUTODEV_STOPPED, "pr": 53, "mergeable": True}) == (
        None,
        [{"do": "diagnose"}],
    )


def test_a_passing_cause_is_run_again_later_and_at_most_twice():
    # EBI: timeouts, truncated bodies, a 403 after hours of good answers
    for said in (
        "download failed: read timed out",
        "IncompleteRead(0 bytes read)",
        "HTTP 403 for https://ftp.example.org/x",
    ):
        assert stopped(said) == ("passing cause", [{"do": "wait", "hours": 2.0}, {"do": "resume"}]), said
    once = [{"rule": "passing cause", "data": {}}]
    assert stopped("read timed out", earlier=once)[1] == [{"do": "wait", "hours": 12.0}, {"do": "resume"}]
    assert stopped("read timed out", earlier=once * 2)[1] == [{"do": "diagnose"}]


def test_a_stop_of_the_workers_own_is_run_again_once_the_worker_runs_a_new_commit():
    # a 310 KB binding in a comment: GitHubError 422, fixed and restarted the same day
    said = "GitHubError: 422 body is too long (maximum is 65536 characters)"
    assert stopped(said, worker={"commit": "new", "since": NOW - 3600}) == ("fixed since", [{"do": "resume"}])
    assert stopped(said, worker={"commit": "old", "since": NOW - 3 * 3600})[0] is None  # no new commit since the stop
    tried = [{"rule": "fixed since", "data": {"commit": "new"}}]
    assert stopped(said, worker={"commit": "new", "since": NOW - 3600}, earlier=tried)[1] == [{"do": "diagnose"}]


def test_what_only_a_person_can_settle_says_so():
    rule, (step,) = stopped("smoke failed (host not allowed: ftp.example.org, s3.example.org)")
    assert rule == "host not approved" and "ftp.example.org, s3.example.org" in step["text"] and step["do"] == "needs"
    rule, (step,) = stopped("match blocked: Someone has to decide what class the two label arrays mark")
    assert rule == "record gap" and step["do"] == "needs"
    assert stopped("crops job 9 ended EXIT (exit 140)") == ("time limit", [{"do": "diagnose"}])
    assert stopped("the crops do not match the annotation") == (None, [{"do": "diagnose"}])


def test_a_match_that_was_refused_a_host_waits_for_the_host_not_for_its_record():
    """Incident 2026-11-17: every URL of a record redirected to an S3 host the match agent's job was refused. Its
    stop began `match blocked:` and quoted the harness, so the cleaner asked for a corrected catalog record."""
    said = (
        "match blocked: Add the host `s3.example.org` to the hosts this job may reach: all three zips redirect "
        "there. `harness list` failed with `HostNotAllowed: host 's3.example.org' is not allowed`."
    )
    rule, (step,) = stopped(said, hosts=["cdn.example.org", "s3.example.org"])
    assert rule == "host not approved" and step["do"] == "needs" and "cdn.example.org, s3.example.org" in step["text"]
    assert stopped(said)[0] == "record gap"  # no host of its jobs waits: what is left is the record's
    assert stopped("match blocked: no class is named", hosts=["s3.example.org"])[0] == "record gap"


def test_a_reader_whose_job_could_reach_no_file_waits_for_the_host():
    """2026-11-19, RV-PBS: files on raw.githubusercontent.com alone, which the reader's job was not granted.
    The round says so before autodev is asked, in the words a refused host is known by."""
    rule, (step,) = stopped(str(NoReachableFiles(["raw.githubusercontent.com"])))
    assert rule == "host not approved" and step["do"] == "needs" and "(raw.githubusercontent.com)" in step["text"]
    assert stopped(str(NoReachableFiles([]))) == (None, [{"do": "diagnose"}])  # nothing a click could approve


def test_a_round_its_budget_refused_is_left_to_a_person_and_not_diagnosed():
    """Incident 2026-11-16: a request for 154 datasets used its budget up after 20, and the cleaner ran its
    diagnosing agent on each of the other 134 to learn what their stop said already."""
    refusal = "root JaneliaSciComp/mia-buffers#27657338: usd 100.00 reserved or spent + 5.00 would exceed max_usd"
    rule, (step,) = stopped(f"root budget: {refusal} 100\n\nThe dataset rests at `data:blocked`.")
    assert rule == "budget used up" and step["do"] == "needs" and "would exceed max_usd 100)" in step["text"]
    assert stopped(f"root budget: {refusal} 500")[0] == "budget used up"  # not the 500 of a passing cause


def test_left_over_snippet_work_is_named_by_why_it_is_left():
    from mia_buffers.cleaner import leftover_reason

    def reason(phase="stopped", stage="blocked", days=1.0, labels=(), newer=False, shelved=None):
        view = {"phase": phase, "shelved": shelved, "stage": stage, "updated": NOW - days * 86400}
        return leftover_reason(view, set(labels), newer, CFG.leftover_days, NOW)

    assert reason(phase="done", stage="done") == "its dataset is prepared through another reader"
    assert reason(shelved={"reason": "later"}) == "its dataset is shelved"
    assert reason(newer=True) == "its dataset has opened a newer snippets issue"
    assert reason(days=4) == "its dataset has rested at `blocked` for 4 days" and reason(days=2) is None
    assert reason(phase="done", stage="cancelled", days=4) == "its dataset has rested at `cancelled` for 4 days"
    assert reason(phase="working", stage="developing", days=9) is None
    assert reason(phase="done", stage="done", labels=["snippets:working"]) is None  # autodev is at work on it
    assert leftover_reason(None, set(), False, CFG.leftover_days, NOW) is None  # no dataset of ours behind it


# --- the diagnosis' answer: what the worker keeps of it (spec §4.6, §4.7) ---


def test_the_agents_answer_is_found_fenced_or_bare():
    from mia_buffers.diagnose import parse_outcome

    assert parse_outcome('Done.\n```json\n{"diagnosis": "x", "cause": "data"}\n```')["cause"] == "data"
    assert parse_outcome('I think {"diagnosis": "y"} is it')["diagnosis"] == "y"
    assert (
        parse_outcome("no JSON here") is None
        and parse_outcome('{"cause": "data"}') is None
        and parse_outcome("") is None
    )


def test_only_remedies_on_the_menu_are_kept_with_the_cases_own_issue_and_pull_request():
    from mia_buffers.diagnose import reduce_outcome

    asked = [
        {"do": "revise", "pr": 999},  # the number is the agent's: the case's own is used
        {"do": "snippets_go", "issue": 1},
        {"do": "wait", "hours": 500},
        {"do": "recheck"},  # a dataset takes no recheck
        {"do": "shelve"},
        "go",
    ]
    outcome = {
        "diagnosis": "d <!-- x -->",
        "cause": "magic",
        "remedies": asked,
        "defect": {"title": "t", "evidence": "e"},
    }
    kept = reduce_outcome(outcome, "dataset", {"issue": 72, "pr": 44})
    assert kept["plan"] == [
        {"do": "revise", "issue": 72, "pr": 44},
        {"do": "await_autodev", "issue": 72},
        {"do": "snippets_go", "issue": 72, "pr": 44},
        {"do": "await_autodev", "issue": 72},
        {"do": "wait", "hours": 48.0},
    ]
    assert (
        kept["cause"] == "unknown"
        and kept["defect"] == {"title": "t", "evidence": "e"}
        and "<!--" not in kept["diagnosis"]
    )
    none = reduce_outcome({"diagnosis": "d", "remedies": [{"do": "close"}, {"do": "revise"}]}, "dataset", None)
    assert none["plan"] == [] and none["defect"] is None  # no snippets issue behind the case: nothing to close
    request = reduce_outcome(
        {"diagnosis": "d", "remedies": [{"do": "stop"}, {"do": "go"}, {"do": "recheck"}]}, "prep", None
    )
    # a request is never set going from scratch by the cleaner: a proposed go is a resume of it
    assert [step["do"] for step in request["plan"]] == ["stop", "kill_jobs", "resume", "recheck"]
    resumed = reduce_outcome(
        {"diagnosis": "d", "remedies": [{"do": "cancel"}, {"do": "resume", "note": "dropped"}]}, "dataset", None
    )
    assert resumed["plan"] == [{"do": "cancel"}, {"do": "kill_jobs"}, {"do": "resume"}]  # on the menu; no note
    buffer = reduce_outcome({"diagnosis": "d", "remedies": [{"do": "resume"}]}, "buffer", None)
    assert buffer["plan"] == []  # a buffer round takes none


def test_a_buffer_spec_is_never_stopped_by_the_cleaner():
    from mia_buffers.diagnose import reduce_outcome

    evidence = item(kind="buffer", stage="sourcing", task=40)
    found = sighting(evidence, CFG, NOW)
    (step,) = plan_for(found, evidence)  # a stop retires every buffer of the spec, also one that is served
    assert found["rule"] == "frozen run" and step["do"] == "needs" and "Force stop" in step["text"]
    asked = {"diagnosis": "d", "remedies": [{"do": "stop"}, {"do": "cancel"}, {"do": "wait", "hours": 1}, {"do": "go"}]}
    assert [step["do"] for step in reduce_outcome(asked, "buffer", None)["plan"]] == [
        "cancel",
        "wait",
        "kill_jobs",
        "go",
    ]
    # a round that was ended has its jobs seen dead before a go, whatever lies between the two
    assert [step["do"] for step in reduce_outcome(asked, "dataset", None)["plan"]] == [
        "stop",
        "cancel",
        "wait",
        "kill_jobs",
        "go",
    ]


def test_a_decision_reads_the_phase_and_a_held_subject_stands_shelved():
    assert standing({"phase": "stopped", "shelved": None}) == "stopped"
    assert standing({"phase": "stopped", "shelved": {"by": "ann", "at": 1.0, "reason": "later"}}) == "shelved"
