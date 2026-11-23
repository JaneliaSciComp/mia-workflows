"""Dataclasses passed between the mia-data workflow and its activities.

Temporal serializes them as JSON, so they hold only strings, numbers, bools, lists and dicts. Job
reports are reduced to these small, typed results before they enter workflow history.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DEBUGGER = "debugger"  # the actor of the go that runs a round again after it was debugged
CLEANER = "cleaner"  # the actor of what the cleaner asks for (spec cleaner §4.7)
CLEANER_ADVISED = "cleaner-advice"  # the cleaner's go that carries its diagnosis' advice as its note
ADVISERS = (DEBUGGER, CLEANER_ADVISED)  # whose go matches first, with its note as the match agent's advice
RERUNNERS = (DEBUGGER, CLEANER, CLEANER_ADVISED)  # who runs a round again for whoever asked for the one before


@dataclass
class LabelEvent:
    """An action somebody asked of a subject through buffer-api, delivered to its workflow as a signal."""

    kind: str  # go | approve | reject; mia-buffers adds plan
    event_id: str  # the action's id in the registry
    actor: str
    at: str  # ISO-8601, as buffer-api stored the action
    set: str = ""  # a verdict: the set of crops it is about (`set_id`); "" from a sender that names none


@dataclass
class PrUpdate:
    """The current state of a snippet PR in mia-snippets, as the poller or the pr_state activity reads it."""

    pr: int
    head_sha: str
    state: str
    merge_sha: str | None
    snippet_issue: int
    needs_human: bool  # the snippets issue has `snippets:needs-human`
    url: str | None = None  # the PR's html_url


@dataclass
class AutodevLook:
    """What a round that waits for autodev finds when it looks at the snippets issue itself."""

    pr: int | None  # the PR the round follows, else the open one on the issue's branch
    working: bool  # the issue has `snippets:working`: an agent round is running
    stopped: bool  # the issue is closed or has `snippets:needs-human`
    # autodev's last comment on the issue or the PR: one line without markers or heading marks, cut and neutralized
    last_word: str
    # the agent's summary of autodev's newest round that changed nothing (cut and neutralized), and when autodev
    # posted it (ISO time); "" when no round ended so
    answer: str = ""
    answer_at: str = ""
    # the request (`CropSet.request`) autodev's newest validated "nothing needs changing" answers; "" when none.
    # Only this says the crops are right as they are: a round without commits alone does not.
    no_change: str = ""


@dataclass
class Pins:
    """What a round is pinned to at admission: the record, the snippets commit and the harness version.

    A round never moves off its pins, so every job in it sees the same inputs."""

    record: str
    record_commit: str
    snippets_commit: str
    harness_commit: str
    env_digest: str
    root: str = ""  # the root request charged for this round (`<repo>#<issue>`); "" in a history that has none


@dataclass
class JobDone:
    job_id: str
    state: str
    exit_code: int | None
    bytes_fetched: int = 0  # what a crop job says it downloaded (its report.json); 0 for any other job
    cost_usd: float | None = None  # what Claude Code says an agent job cost, at list price; None: no agent, or unknown


@dataclass
class Test:
    """The test crops run against one snippets commit: whether it passed and where its gallery is."""

    __test__ = False  # not a pytest class
    sha: str
    test_id: str
    ok: bool | None = None
    finished_at: str | None = None
    gallery_prefix: str | None = None
    tree_sha: str | None = None
    crop_workspace: str | None = None  # the round's set (spec §13): where its crops and verify report are
    verify_workspace: str | None = None
    approved_at: str | None = None  # the `data:approve` event's time, once a reviewer approved this set (spec §14)
    approved_by: str = ""  # who approved it: a person's actor, `judge`, or `policy:developer`
    evidence: str = ""  # the crop judge's workspace for this set, once it looked: what it saw is under `evidence/`


def set_id(round_id: str | None, test: Test) -> str:
    """The name a verdict uses for the set of crops `test` made in the round."""
    return f"{round_id}:{test.test_id}"


@dataclass
class CropSet:
    """One set of sample crops with everything a later verdict needs, kept after the round that made it (spec
    judge-decides §5.4): the set that was saved, and a set that was sent back for rework."""

    set_id: str  # "<round_id>:<test_id>"
    round_id: str
    test: Test
    pins: Pins
    binding: str | None = None
    binding_hash: str | None = None
    pr: int | None = None
    snippet_issue: int | None = None
    sent_back_by: str = ""  # `judge` or the person's actor; "" for a set that was not sent back
    worst: str = ""  # the judge's worst finding on it: "bad" | "suspect" | ""
    request: str = ""  # the id of the rework request the developer answers: "<set_id>#<n>"
    opened_issue: bool = False  # the send-back opened the snippets issue: the set's approval closes it
    findings: list[dict] = field(default_factory=list)  # the judge's findings that are not `ok`, as it wrote them
    # what the set's round needs when it becomes the dataset's round again: the snippet a merge looks for, the
    # round's seed, and the stage passes the round had begun (their numbers go on, so no entry is written twice)
    snippet_name: str | None = None
    seed: str | None = None
    passes: dict[str, int] = field(default_factory=dict)


@dataclass
class DatasetState:
    """The durable state of one dataset issue's workflow, carried across continue_as_new.

    Most fields describe the current round and are reset when a new `go` starts one."""

    issue: int
    schema: int = 1
    handled: list[str] = field(default_factory=list)  # go + approve event ids, newest last; keep <= 50
    round_id: str | None = None  # i<issue>-e<event id>
    seed: str | None = None
    stage: str = "new"
    pins: Pins | None = None
    binding: str | None = None  # path under state_dir
    binding_hash: str | None = None
    snippet_name: str | None = None
    snippet_issue: int | None = None
    pr: int | None = None
    tests: list[Test] = field(default_factory=list)
    attempts: dict[str, int] = field(default_factory=dict)  # stage -> attempts used
    # which attempt of its steps the round is on: a step that runs again under the same round takes the next
    # one, and with it a job key, a workspace, a test id and timeline markers of its own
    attempt: int = 1
    pending: list[PrUpdate] = field(default_factory=list)  # PR updates not yet acted on
    published: bool = False  # some round of this issue published crops: there is something a data:revoke withdraws
    restart: LabelEvent | None = None  # the go a run was rolled over for: the new run's first step is its round
    debugs: int = 0  # debugging cycles used since the last go that was not the debugger's
    # the stage pass whose result entry is still to be written, and what the round has gathered for it; each
    # stage write carries it out (spec job-records §3.6)
    passes: dict[str, int] = field(default_factory=dict)  # stage -> passes begun this round
    pass_stage: str = ""  # the open pass; "" when no result is due
    pass_jobs: list[str] = field(default_factory=list)  # workspaces of the jobs the open pass submitted
    result: str = ""  # what the open pass found
    result_binding: bool = False  # whether its result entry shows the round's binding
    waiting_since: str | None = None  # when the round last heard from autodev while it waits for it (ISO time)
    # when a reviewer's comments went to the snippet PR as a revision request (ISO time); None once a head or
    # autodev's answer came back for it
    revised_at: str | None = None
    # sets that outlive their round (spec judge-decides §5.4): the one sent back for rework and not superseded by
    # an approved set, and the one that is saved; neither is touched when a new round starts
    held: CropSet | None = None
    saved: CropSet | None = None
    reworks: int = 0  # how often the crop judge sent this dataset's sets back since a person last acted
    accepting: str | None = None  # read by nothing: states in recorded histories carry the field
    revoked: bool = False  # a person's revoke ended the round: it takes no resume, and a new round clears this


def sets_view(state: DatasetState, stage: str) -> dict:
    """What the page needs to know about the dataset's sets, for the subject's data: the set a verdict at
    `review` is about and the saved one (a reject may name it at `done`)."""
    saved = state.saved
    return {
        "review": set_id(state.round_id, state.tests[-1]) if stage == "review" and state.tests else None,
        "saved": {"set": saved.set_id, "approved_by": saved.test.approved_by} if saved else None,
        "revoked": state.revoked,  # buffer-api offers no resume for it (spec resume-restart §4.3)
    }


def resume_step(state: DatasetState, judge_on: bool) -> str:
    """Where a resumed round starts: its first step with no result on `state` (spec resume-restart §4.2). A step
    that finished is kept whatever changed since; a rule that voids a result when something changed is a
    condition here (§8).

    `restart`: the round was never admitted and has nothing to keep. `follow`: it follows a reader's snippets
    issue, and acts on that as it is now. `saved`: its approved set is the saved one already, so nothing is left
    (a set that was saved and then revoked reads so too, whatever marks the revoke). `save`: its set is approved
    and not saved. `judge`: its set passed and the crop judge (`judge_on`) never looked. `review`: its set waits
    for a verdict. `crops`: it has a binding and no passing set. `match`: it has no binding."""
    if state.pins is None:
        return "restart"
    if state.snippet_issue and not merge_taken(state):
        return "follow"
    last = state.tests[-1] if state.tests else None
    if last and last.ok:
        if last.approved_at:
            saved = state.saved is not None and state.saved.set_id == set_id(state.round_id, last)
            return "saved" if saved else "save"
        return "judge" if judge_on and not last.evidence else "review"
    return "crops" if state.binding else "match"


def merge_taken(state: DatasetState) -> bool:
    """Whether the round has taken its reader's merge: it follows no pull request any more, its pins name the
    merge commit, and its sets are its own, as a matched binding's are. Read from what the merge leaves on the
    state: no `pr`, and a set made at a commit that is not the round's (a pull request's head).

    # ponytail: a merge taken with no head ever tested leaves no such set and reads as not taken; the resumed
    # round then waits on its closed issue and stops, and a Restart runs it. A `merged` field on the state
    # when that is seen.
    """
    pins = state.pins
    return state.pr is None and pins is not None and any(test.sha != pins.snippets_commit for test in state.tests)


def job_key(issue: int, round_id: str, kind: str, attempt: int) -> str:
    """csub job key, unique per round, stage and attempt (the workspace is <kind>-<attempt> under the round)."""
    return f"mia-{issue}-{round_id}-{kind}-{attempt}"


@dataclass
class JobArgs:
    """Arguments of the `submit` activity. Which fields matter depends on the job kind."""

    workspace: str  # from prepare_match / prepare_job
    round_id: str
    pins: Pins
    sha: str | None = None  # smoke/crop: mia-snippets commit to load; default pins.snippets_commit
    n_crops: int = 0  # crop: how many crops to make
    seed: str = ""
    batch: int = 0
    trusted: bool = False  # crop: reviewed code, dataset cache read-write
    existing: bool = False  # crop/verify: --existing <dataset folder>/manifest.json, when there is one
    crop_workspace: str | None = None  # verify: the crop job's workspace


@dataclass
class MatchResult:
    """What the match agent concluded about the dataset, after the worker checked its binding."""

    outcome: str  # matched | needs_snippet | blocked | failed (the last two stop the round, with the reason)
    reason: str = ""
    binding_path: str | None = None  # matched: the checked binding, under state_dir
    binding_hash: str | None = None
    shapes: dict | None = None  # wire format: recorded histories carry it; unused
    draft: dict | None = None  # needs_snippet: draft_binding, what_fails, tried
    reasons: list[str] = field(default_factory=list)  # matched: the binding's reasons


@dataclass
class DebugResult:
    """What the debugging agent concluded about a failed round: `rerun` (a new round may pass, with `advice` for
    its match agent), `blocked` (a person has to decide or change something first) or `failed` (it left nothing
    readable). `diagnosis` is what it found, for the timeline."""

    outcome: str
    diagnosis: str = ""
    advice: str = ""


@dataclass
class SmokeResult:
    """The worker's own quick check of a binding: can the data be opened and read as the record says?"""

    ok: bool
    eligible: bool = True  # wire format: recorded histories carry it; smoke.json does not send it
    shapes: dict | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    bytes_fetched: int = 0
    host_not_allowed: str | None = None


@dataclass
class VerifyResult:
    """The verify job's report, reduced to the crops that passed."""

    ok_count: int
    n: int  # how many crops verify was asked to check (named `n` on the wire: it is in recorded histories)
    errors: list[str]
    crops: list[dict]  # verified crops only: [{"crop_id", "files": {rel: sha256}}]
    n_effective: int | None = None  # the crop job's n, lowered by capacity (its report.json); None if unknown
    bytes_fetched: int = 0  # the crop job's, from its report.json
    crop_errors: list[str] = field(default_factory=list)  # the crop job's own errors (report.json): why it failed
    host_not_allowed: str | None = None  # the host the crop job was refused (report.json), with its redirect chain


@dataclass
class RoundLimits:
    """The config values a round runs with, read at admission (the workflow can't read config)."""

    test_n: int
    per_round: int
    walltime_min: int
    max_pr_revisions: int = 5  # PR heads tested per round (develop path)
    poll_minutes: int = 2  # how long a round waits between tries of a lock someone else holds
    agent_max_budget_usd: float = 5.0  # what a match agent run may cost: reserved against the root before it runs
    crop_judge: str = "off"  # judge.crops.mode: off | shadow | active (spec autonomous-gates §9)
    judge_max_budget_usd: float = 2.0  # what one judge run may cost: reserved like the match agent's
    debug_cycles: int = 0  # limits.debug_cycles: failed rounds debugged and run again before one stops as failed
    debug_wait_min: int = 5  # limits.debug_wait_min: the wait between a failure and its debugging agent
    autodev_quiet_hours: float = (
        2.0  # limits.autodev_quiet_hours: how long a round waits on an autodev that is not at work
    )
    max_reworks: int = 1  # judge.crops.max_reworks: how often the crop judge may send a dataset's sets back


@dataclass
class AssessmentWritten:
    """A human verdict recorded as an assessment (spec autonomous-gates §5), and whether the subject is now
    authorized under the policy; `reason` is the policy's answer either way."""

    id: str
    authorized: bool
    reason: str = ""


@dataclass
class Revocation:
    """What `data:revoke` recorded (spec autonomous-gates §6): the reject assessment superseding the approve, the
    staged artifacts quarantined and the buffers that drew from the dataset."""

    assessment: str
    artifacts: list[str] = field(default_factory=list)
    buffers: list[str] = field(default_factory=list)


@dataclass
class Reservation:
    """Whether a job may be charged to its root request (spec autonomous-gates §8); `reason` names the cap when not."""

    ok: bool
    reason: str = ""
    wait: bool = False  # the refusal is the concurrency cap: wait and ask again rather than fail


@dataclass
class LockTaken:
    """The take_lock activity's answer: taken, or not, and then who holds the lock (its owner string)."""

    ok: bool
    holder: str = ""


@dataclass
class GalleryResult:
    prefix: str
    urls: dict[str, str]  # rel -> ?raw=true URL
    table: str  # the markdown table from render_crops_table


@dataclass
class JudgeResult:
    """What the judge said about a subject, after policy read it (spec autonomous-gates §4): `verdict` is the
    judge's own, `effective` what the assessment records (a reject without a `bad` finding is an approve, low
    confidence is an escalate). `comment` is rendered for the timeline; `assessment` is "" when its write to the
    registry failed. `authorized`: the gate is active and the policy takes this approve as the subject's
    authorization, so the round needs no person's verdict."""

    verdict: str
    effective: str
    confidence: float
    summary: str
    assessment: str = ""
    cost_usd: float | None = None
    comment: str = ""
    findings: list[dict] = field(default_factory=list)  # <= 64 of {"item", "severity", "note"}
    authorized: bool = False
    worst: str = ""  # the worst finding's severity: "bad" | "suspect" | "" (every crop `ok`)
