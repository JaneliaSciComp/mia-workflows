"""The mia-data Temporal activities: the only code that touches GitHub, csub and the filesystem.

The worker keeps each round's trusted inputs (record, downloads entry, pins, checked bindings) under
state_dir/rounds/<round_id>, which is never mounted into a job. Jobs get copies in their workspaces under
work_root. Every file a job wrote is read back with `read_untrusted` and parsed defensively, and the
results that enter workflow history are the small dataclasses in models.py.
"""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import hashlib
import json
import logging
import re
import shutil
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote

import yaml
from temporalio import activity
from temporalio.exceptions import ApplicationError

from autodev.activities import PERMANENT_CSUB, permanent_errors
from autodev.jobs import JobSpec
from mia_core import autonomy
from mia_core.assets import raw_asset
from mia_core.config import manifest_path
from mia_core.jobrecord import address, job_link, record_path
from mia_core.judge.evidence import build as build_evidence
from mia_core.judge.evidence import evidence_digest, read_claude_json
from mia_core.judge.policy import POLICY_VERSION, authorized, new_assessment
from mia_core.judge.schema import Verdict, effective_verdict, parse_verdict, prompt_text, prompt_version
from mia_core.locks import LockLost, heartbeat_through, release_or_log, take_or_holder
from mia_core.registry import Registry, RegistryError, best_effort
from mia_core.untrusted import loads as _loads
from mia_core.untrusted import neutralize, read_untrusted
from mia_core.untrusted import text as _text
from mia_core.untrusted import texts as _texts
from mia_core.untrusted import write_atomic as write_atomic

from . import catalog
from . import publish as crop_publish
from .binding import check_binding, hosts, record_gaps, record_urls
from .config import HUMAN_LABELS, STAGE_SENTENCES, STAGES, dataset_state, round_open
from .gallery import gallery_files, publish_gallery
from .jobspec import (
    ADVICE_FILE,
    SHA_RE,
    HostsNotAllowed,
    PinMoved,
    check_hosts_allowed,
    crop_job,
    debug_job,
    debug_prompt,
    harness_pin,
    harness_snapshot,
    judge_job,
    lock_dir,
    match_job,
    match_prompt,
    smoke_job,
    snapshot,
    verify_job,
)
from .models import (
    DEBUGGER,
    AssessmentWritten,
    DatasetState,
    DebugResult,
    GalleryResult,
    JobArgs,
    JobDone,
    JudgeResult,
    LockTaken,
    MatchResult,
    Pins,
    PrUpdate,
    Reservation,
    Revocation,
    RoundLimits,
    SmokeResult,
    VerifyResult,
)
from .poll import run_poll
from .publish import CROP_ID_RE, REVOKED_FILE, Conflict
from .seed import ID_RE
from .status import (
    render_binding,
    render_crops_table,
    render_jobs,
    render_judge_comment,
    render_links,
    render_result,
    stage_entries,
)

if TYPE_CHECKING:
    from temporalio.client import Client

    from autodev.github import GitHub
    from autodev.jobs import CsubJobs

    from .config import Config

log = logging.getLogger(__name__)

TEST_PR_RE = re.compile(r"i0-e(\d+)-[0-9a-f]{7}")  # `mia-data test-pr`'s round id: PR <n> at a head, no data issue
ROUND_RE = re.compile(rf"i\d+-e\d+|{TEST_PR_RE.pattern}")
# what a job leaves in its workspace that says how it went: copied for the debugging agent, each cut at this size
ROUND_FILES = (
    "binding.yaml",
    "outcome.json",
    "smoke.json",
    "crops/report.json",
    "verify.json",
    "claude.json",
    "claude.err",
    "exit_code",
)
EVIDENCE_BYTES = 1 << 20
LOG_BYTES = 64 << 10  # the end of a job's stdout and stderr
KIND_RE = re.compile(r"[a-z][a-z0-9-]{0,60}")  # a job kind, optionally with a suffix (crop-<sha>)
HEX64 = re.compile(r"[0-9a-f]{64}")
NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")  # a snippet directory
CROP_STATUS = {
    "pending": "test crops running",
    "success": "test crops passed",
    "failure": "test crops failed",
    "error": "test crops could not run",
}
PERMANENT = (
    HostsNotAllowed,
    PinMoved,
    Conflict,
    ValueError,
    FileNotFoundError,
    yaml.YAMLError,
)  # inputs that a retry can't fix


def mia_permanent_errors[Fn: Callable[..., Awaitable]](fn: Fn) -> Fn:
    """Decorator: autodev's permanent_errors, plus mia-data's own permanent failures.

    A moved pin, a host outside the policy, a publication conflict, a bad input (malformed YAML too) or a
    missing state file stop the round instead of burning retries."""
    inner = permanent_errors(fn)

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await inner(*args, **kwargs)
        except PERMANENT as error:  # the type names the exception: an ApplicationError's text begins with it
            raise ApplicationError(str(error), type=type(error).__name__, non_retryable=True) from error

    return cast(Fn, wrapper)


def _small_dict(value: Any, limit: int = 16384) -> dict | None:
    """Job-reported JSON goes into workflow history only as a small dict (the result fields are typed dict)."""
    try:
        return value if isinstance(value, dict) and len(json.dumps(value)) <= limit else None
    except (TypeError, ValueError):
        return None


def _job_json(workspace: str | Path, rel: str) -> Any:
    """A JSON file a job wrote, or None if missing, unreadable or malformed."""
    return _loads(read_untrusted(Path(workspace), rel, 1 << 20))


def _action(event_id: str | None) -> int | None:
    """The action id an event id is (buffer-api sends an action as an event with its id); None for no event."""
    return int(event_id) if event_id and event_id.isdecimal() else None


def _when(iso: str | None) -> datetime | None:
    """An ISO-8601 time as an aware datetime (GitHub's `Z`, or an offset; none given is UTC); None if unreadable."""
    try:
        parsed = datetime.fromisoformat((iso or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _fenced(text: str, lang: str = "") -> str:
    """`text` in a markdown code fence longer than any backtick run inside it."""
    fence = "`" * max(3, 1 + max(map(len, re.findall(r"`+", text)), default=0))
    return f"{fence}{lang}\n{text.rstrip()}\n{fence}"


def withdrawn_snippets(snapshot_dir: Path) -> dict[str, str]:
    """snippet name -> reason, from the snapshot's WITHDRAWN.yaml."""
    entries = yaml.safe_load(read_untrusted(snapshot_dir, "WITHDRAWN.yaml", 1 << 20) or "") or []
    return {
        entry["name"]: _text(entry.get("reason"))
        for entry in entries
        if isinstance(entry, dict) and isinstance(entry.get("name"), str)
    }


def visible_snippets(snapshot_dir: Path) -> dict[str, str]:
    """snippet name -> its snippet.yaml text, hiding withdrawn snippets and ones another snippet supersedes."""
    root = snapshot_dir / "snippets"
    catalog_text = {
        entry.name: text
        for entry in sorted(root.iterdir() if root.is_dir() else [])
        if (text := read_untrusted(snapshot_dir, f"snippets/{entry.name}/snippet.yaml", 1 << 20)) is not None
    }
    hidden = set(withdrawn_snippets(snapshot_dir))
    for text in catalog_text.values():
        parsed = yaml.safe_load(text)
        superseded = parsed and isinstance(parsed, dict) and parsed.get("supersedes")
        if isinstance(superseded, str):
            hidden.add(superseded)
        elif isinstance(superseded, list):
            hidden |= {name for name in superseded if isinstance(name, str)}
    return {name: text for name, text in catalog_text.items() if name not in hidden}


class Activities:
    """The activity implementations, bound to the three GitHub clients (data, snippets and catalog repos),
    the csub job runner and the Temporal client.

    `bot` is the GitHub login mia-data posts as. `probe` returns the csub policy's allowed_hosts patterns;
    it is injectable for tests."""

    def __init__(
        self,
        cfg: Config,
        gh_data: GitHub,
        gh_snippets: GitHub,
        gh_catalog: GitHub,
        jobs: CsubJobs,
        bot: str,
        client: Client | None = None,
        probe: Callable[[], list[str]] | None = None,
    ):
        self.cfg, self.gh_data, self.gh_snippets, self.gh_catalog = cfg, gh_data, gh_snippets, gh_catalog
        self.jobs, self.bot, self.client = jobs, bot, client
        if cfg.service is None:
            raise ValueError("mia-data needs a `service` section: buffer-api holds its stages and timelines")
        self.registry = Registry.from_config(cfg.service)  # buffer-api: stages, timelines, actions, budgets
        self.page_url = cfg.service.url.rstrip("/")
        self.probe = probe or (lambda: list(jobs.client.probe().allowed_hosts))
        # ponytail: in memory, so a worker restart re-sends each PR once (the workflow dedupes by pr_state)
        self._sent: dict[int, tuple] = {}  # poll: PR number -> the (head, state, merge, needs_human) last signalled

    @property
    def temporal(self) -> Client:
        """The Temporal client; only the worker's activities (poll) have one."""
        if self.client is None:
            raise RuntimeError("no Temporal client: this activity runs only inside the worker")
        return self.client

    def all(self) -> list:
        """The main queue's activities; await_job runs on the -await queue."""
        return [
            self.admit,
            self.limits,
            self.poll,
            self.set_stage,
            self.stage_result,
            self.comment,
            self.prepare_match,
            self.prepare_debug,
            self.read_debug,
            self.debug_go,
            self.prepare_job,
            self.probe_hosts,
            self.submit,
            self.read_match,
            self.read_smoke,
            self.read_verify,
            self.take_lock,
            self.release_lock,
            self.publish_gallery,
            self.publish_crops,
            self.refuse_label,
            self.read_feedback,
            self.open_snippet_issue,
            self.pr_state,
            self.set_crop_status,
            self.tree_sha,
            self.adopt_pr_binding,
            self.paused,
            self.reserve,
            self.settle,
            self.assess,
            self.revoke_dataset,
            self.prepare_judge,
            self.read_judge,
            self.kill_job,
        ]

    # --- helpers -----------------------------------------------------------------------------

    def _clean(self, text: str) -> str:
        return neutralize(text, self.gh_data.tokens())

    async def _bot_comment(self, gh: GitHub, number: int, marker: str) -> dict | None:
        """The bot's own comment carrying `marker` (an outsider's copy must not count)."""
        return next(
            (
                comment
                for comment in await gh.issue_comments(number)
                if (comment.get("user") or {}).get("login") == self.bot and marker in (comment.get("body") or "")
            ),
            None,
        )

    async def _comment_once(self, gh: GitHub, number: int, body: str, marker: str) -> str | None:
        """Post the comment unless one with this marker exists. Returns the comment's URL (the earlier one, on a retry)."""
        return (await self._bot_comment(gh, number, marker) or await gh.comment(number, body)).get("html_url")

    async def _post(
        self, issue: int, body: str, marker: str, *, round_id: str | None = None, stage: str | None = None
    ) -> None:
        """Write what a round has to say about its dataset: a stage, a text (once per marker), or both. It is the
        subject's stage and a timeline entry in the registry, a required write, and no GitHub call: the mirror
        copies it to the issue whenever GitHub answers (spec webapp-actions §6, §7)."""
        write = functools.partial(
            self.registry.append_entry, "dataset", issue, body=body, marker=marker, round_id=round_id, stage=stage
        )
        await asyncio.to_thread(write)

    async def _action(self, number: int, event_id: str) -> dict | None:
        """The action whose id is this event id, with its `subject` and the subject's `actions`; None when the
        event is not an action (a signal the workflow made itself). An action of another subject is an error."""
        registry = self.registry
        action = await asyncio.to_thread(registry.action, event_id)
        if action is None:
            return None
        if (action["kind"], action["number"]) != ("dataset", number):
            raise ValueError(f"event {event_id} is not an action on dataset #{number}")
        found = await asyncio.to_thread(registry.timeline, "dataset", number)
        if found is None:
            raise ValueError(f"dataset #{number} is not in the registry")
        return {**action, "subject": found["subject"], "actions": found["actions"]}

    def _round_dir(self, round_id: str) -> Path:
        """state_dir/rounds/<round_id>: the round's record, downloads entry, pins and bindings (never mounted)."""
        if not ROUND_RE.fullmatch(round_id):
            raise ValueError(f"bad round id {round_id!r}")
        return self.cfg.paths.state_dir / "rounds" / round_id

    def _pins(self, round_id: str) -> Pins:
        return Pins(**json.loads((self._round_dir(round_id) / "pins.json").read_text()))

    def _round_workspace(self, issue: int, round_id: str) -> Path:
        """work_root/issue-<n>/<round>, the parent of the round's job workspaces; a test-pr round's is
        work_root/test-pr/<pr>/<head>."""
        round_dir = self._round_dir(round_id)
        if match := TEST_PR_RE.fullmatch(round_id):
            return self.cfg.paths.work_root / "test-pr" / match.group(1) / self._pins(round_id).snippets_commit
        return self.cfg.paths.work_root / f"issue-{int(issue)}" / round_dir.name

    def _make_workspace(self, issue: int, round_id: str, kind: str, attempt: int) -> Path:
        """Create (if needed) and return the workspace for one job: <round workspace>/<kind>-<attempt>."""
        if not (KIND_RE.fullmatch(kind) and type(attempt) is int and attempt >= 1):
            raise ValueError(f"bad job kind/attempt {kind!r}/{attempt!r}")
        workspace = self._round_workspace(issue, round_id) / f"{kind}-{attempt}"
        workspace.mkdir(parents=True, exist_ok=True)
        return workspace

    def _job_workspace(self, issue: int, round_id: str, workspace: str) -> Path:
        """Check that a workspace path (an activity argument) really is one of this issue's round."""
        path = Path(workspace)
        if path.parent != self._round_workspace(issue, round_id) or path.is_symlink():
            raise ValueError(f"{workspace} is not a workspace of issue {issue} round {round_id}")
        return path

    @staticmethod
    def _crops_dir(workspace: Path) -> Path:
        """The crops/ directory a crop job wrote, which must be a real directory inside its workspace."""
        crops_dir = workspace / "crops"
        if crops_dir.is_symlink() or not crops_dir.is_dir():
            raise ValueError(f"{crops_dir} is not a directory")
        return crops_dir

    def _record(self, round_id: str) -> dict:
        return yaml.safe_load((self._round_dir(round_id) / "record.yaml").read_text()) or {}

    def _dataset_dir(self, round_id: str, pins: Pins) -> Path:
        """Where the dataset's crops are published under storage_root."""
        return self.cfg.paths.storage_root / catalog.dataset_folder(self._record(round_id), pins.record)

    def dataset_fields(self, issue: int, round_id: str, stage: str) -> dict:
        """The registry row for the dataset at `stage`: its state (`dataset_state`: the stage decides, and `done`
        counts as prepared only when the folder holds a binding and a manifest that matches it), the manifest's
        facts when the folder has them, and the reason when a `done` round saved nothing."""
        try:
            pins = self._pins(round_id)
            dataset_dir = self._dataset_dir(round_id, pins)
            crop_publish.check_owner(dataset_dir, pins.record)
            if (dataset_dir / REVOKED_FILE).is_file():  # revoked stays revoked until a new round saves its crops
                return {"state": "revoked", "data_stage": stage, "issue": issue}
            row = crop_publish.prepared_row(dataset_dir)
            why = f"`{dataset_dir.name}` holds no binding.yaml with a matching manifest.json"
        except (ValueError, OSError, Conflict) as err:
            # no derivable folder (unclear record), unreadable, or another record's folder: a state row only
            log.warning("round %s: no prepared lookup: %s", round_id, err)
            row, why = None, str(err)
        facts = {name: value for name, value in (row or {}).items() if name not in ("record", "state")}
        unsaved = stage == "done" and row is None
        reason = f"the round is `done` but its crops are not saved: {why}. Run it again." if unsaved else None
        state = dataset_state(stage, saved=row is not None)
        return {**facts, "state": state, "reason": reason, "data_stage": stage, "issue": issue}

    def _report_dataset(self, registry: Registry, issue: int, round_id: str, stage: str) -> None:
        """Put the dataset's row to the registry; never raises: a missing pins file, a catalog record of the wrong
        shape (it is untrusted) or a down service is logged and the stage goes on."""
        try:
            record = self._pins(round_id).record
        except (ValueError, OSError) as err:
            log.warning("round %s: registry write skipped: %s", round_id, err)
            return
        try:
            registry.put_dataset(record, **self.dataset_fields(issue, round_id, stage))
        except Exception:
            log.exception("%s: registry write skipped", record)

    async def _snapshot(self, sha: str) -> Path:
        return await asyncio.to_thread(snapshot, self.cfg, sha, self.gh_snippets.tokens)

    # --- admission and display ---------------------------------------------------------------

    @activity.defn(name="admit")
    @mia_permanent_errors
    async def admit(self, issue: int, event_id: str) -> Pins:
        """Pin the round: the record is the subject's key, record_commit comes from the downloads state (or the
        catalog's main when the record was never downloaded: its jobs fetch on demand), then snippets main and the
        harness checkout. The round is charged to whoever asked for it: the root its `go` carries, else the
        dataset itself. A record that lacks what a round needs (`record_gaps`), or whose dataset folder is another
        record's, is refused here, before any job runs. Written once to state_dir/rounds/<round_id>/; a retry
        returns the same pins."""
        if not re.fullmatch(r"\d+", str(event_id)):
            raise ValueError(f"bad event id {event_id!r}")
        round_dir = self._round_dir(f"i{int(issue)}-e{event_id}")
        if (round_dir / "pins.json").is_file():
            return Pins(**json.loads((round_dir / "pins.json").read_text()))
        action = await self._action(issue, event_id)
        if action is None:
            raise ValueError(f"event {event_id} is not an action: a round is started by one")
        record_id = action["subject"]["key"]
        if not ID_RE.fullmatch(record_id):
            raise ValueError(f"dataset #{issue} has no valid record id: {record_id!r}")
        root = action["payload"].get("root") or self.cfg.root_of(issue)
        entry = (await catalog.downloaded(self.gh_catalog)).get(record_id)
        if entry is None:
            entry = {"record_commit": await self.gh_catalog.get_ref_sha("main"), "downloaders": []}
        if not SHA_RE.fullmatch(str(entry.get("record_commit"))):
            raise ValueError(f"{record_id}: the downloads entry has no record_commit")
        path = (await catalog.record_paths(self.gh_catalog)).get(record_id)
        if path is None:
            raise ValueError(f"{record_id}: no record on main")
        record = await catalog.record(self.gh_catalog, path, entry["record_commit"])
        if gaps := record_gaps(record):
            raise ValueError(f"{record_id}: the record cannot be run: {'; '.join(gaps)}")
        crop_publish.check_owner(self.cfg.paths.storage_root / catalog.dataset_folder(record, record_id), record_id)
        pins = Pins(
            record_id,
            entry["record_commit"],
            await self.gh_snippets.get_ref_sha("main"),
            *await asyncio.to_thread(harness_pin, self.cfg),
            root=root,
        )
        await asyncio.to_thread(harness_snapshot, self.cfg, pins.harness_commit)  # freeze the source now
        write_atomic(round_dir / "record.yaml", yaml.safe_dump(record, sort_keys=False))
        write_atomic(round_dir / "downloads.json", json.dumps(entry, indent=1))
        write_atomic(round_dir / "pins.json", json.dumps(dataclasses.asdict(pins)))  # last: marks the round admitted
        return pins

    @activity.defn(name="limits")
    async def limits(self) -> RoundLimits:
        """The config values the workflow needs but cannot read itself."""
        cfg = self.cfg
        return RoundLimits(
            cfg.crops.test_per_round,
            cfg.crops.per_round,
            cfg.limits.job_walltime_min,
            cfg.limits.max_pr_revisions,
            cfg.temporal.poll_minutes,
            cfg.limits.agent_max_budget_usd,
            cfg.judge.crops.mode,
            cfg.judge.max_budget_usd,
            cfg.limits.debug_cycles,
            cfg.limits.debug_wait_min,
        )

    # --- assessments: a human verdict on record, and the policy's answer (spec autonomous-gates §5) ---

    @activity.defn(name="assess")
    async def assess(
        self,
        issue: int,
        round_id: str,
        gate: str,
        subject_kind: str,
        subject_ref: str,
        verdict: str,
        by: str,
        summary: str = "",
        supersedes: str | None = None,
        action_id: str | None = None,
    ) -> AssessmentWritten:
        """Write the verdict as an assessment in the registry, then ask `authorized()` over everything written about
        the subject; an approve also names its assessment on the dataset row. `action_id` is the event id of the
        person's action, written on the assessment so that what they wrote with it can be shown beside it."""
        registry = self.registry
        row = new_assessment(
            gate=gate,
            subject_kind=subject_kind,
            subject_ref=subject_ref,
            verdict=verdict,
            by=by,
            at=time.time(),
            summary=summary,
            supersedes=supersedes,
            action=_action(action_id),
        )
        await asyncio.to_thread(registry.put_assessment, row)
        rows = await asyncio.to_thread(lambda: registry.assessments(subject_kind=subject_kind, subject_ref=subject_ref))
        auth = authorized(rows)
        if verdict == "approve":
            record = self._pins(round_id).record
            await asyncio.to_thread(
                best_effort, lambda: registry.put_dataset(record, assessment=row["id"]), f"{record}: assessment", log
            )
        return AssessmentWritten(row["id"], auth.ok, auth.reason)

    # --- the judge: evidence in, an assessment out (spec autonomous-gates §4, §9) -------------------

    @activity.defn(name="prepare_judge")
    @mia_permanent_errors
    async def prepare_judge(
        self,
        issue: int,
        round_id: str,
        gate: str,
        crop_workspace: str,
        verify_workspace: str,
        verified: list[dict],
        attempt: int = 1,
        tag: str = "",
    ) -> str:
        """Make the crop judge's workspace, `<round workspace>/judge-crops[-<tag>]-<attempt>` (`tag`: the tested
        commit, so every head of a round keeps its own evidence), with its evidence directory: the question, raw.png
        and overlay.png of every verified crop, verify.json, the binding without the author's `reasons`, and the
        record. Nothing from the match agent's workspace. Returns the workspace path."""
        if gate != "crops":
            raise ValueError(f"prepare_judge knows the crops gate, not {gate!r}")
        workspace = self._make_workspace(issue, round_id, f"judge-{gate}" + (f"-{tag}" if tag else ""), attempt)
        crop_dir = self._job_workspace(issue, round_id, crop_workspace)
        verify_dir = self._job_workspace(issue, round_id, verify_workspace)
        files = await asyncio.to_thread(gallery_files, crop_dir, verify_dir, verified)
        binding = yaml.safe_load(files.pop("binding.yaml")) or {}
        if isinstance(binding, dict):
            binding.pop("reasons", None)  # the author arguing its case is not evidence (spec §4)
        items: dict[str, bytes | Path] = {
            **files,
            "binding.yaml": yaml.safe_dump(binding, sort_keys=False).encode(),
            "record.yaml": self._round_dir(round_id) / "record.yaml",
        }
        crops = "\n".join(f"- `{crop['crop_id']}`" for crop in verified) or "(no verified crop)"
        question = f"{prompt_text(gate)}\n# The crops in this set\n\n{crops}\n"
        author = self._round_workspace(issue, round_id) / "match-1"
        await asyncio.to_thread(build_evidence, workspace, question, items, author_workspace=author)
        return str(workspace)

    @activity.defn(name="read_judge")
    @mia_permanent_errors
    async def read_judge(
        self, issue: int, round_id: str, workspace: str, gate: str, subject_kind: str, subject_ref: str, mode: str
    ) -> JudgeResult:
        """Read the judge's claude.json, apply policy (serious issues only, the confidence floor), write the
        assessment `by: judge` to the registry and `verdict.json` beside the evidence, and render the comment.
        A missing or unreadable verdict is an `escalate` on record, never an error: shadow must not block."""
        path = self._job_workspace(issue, round_id, workspace)
        text, cost, model = read_claude_json(path)
        try:
            verdict = parse_verdict(text)
        except ValueError as error:
            verdict = Verdict("escalate", 0.0, (), f"unreadable verdict: {_text(str(error), 400)}")
        max_suspect = self.cfg.judge.crops.max_suspect if gate == "crops" else None
        effective, why = effective_verdict(verdict, self.cfg.judge.min_confidence, max_suspect)
        findings = [
            {"item": self._clean(f.item), "severity": f.severity, "note": self._clean(f.note)} for f in verdict.findings
        ]
        summary = self._clean(verdict.summary)
        row = new_assessment(
            gate=gate,
            subject_kind=subject_kind,
            subject_ref=subject_ref,
            verdict=effective,
            by="judge",
            at=time.time(),
            summary=summary,
            findings=findings,
            confidence=verdict.confidence,
            evidence_digest=evidence_digest(path),
            checks={"verify": self._pins(round_id).harness_commit, "why": why},
            model=model or self.cfg.judge.model,
            prompt_version=prompt_version(gate),
        )
        write_atomic(
            path / "verdict.json",
            json.dumps({"assessment": row, "judge": dataclasses.asdict(verdict), "cost_usd": cost}, indent=1),
        )
        registry = self.registry  # shadow never blocks: a registry outage is logged, the round goes on
        written = await asyncio.to_thread(
            best_effort, lambda: registry.put_assessment(row), f"{round_id}: judge assessment", log
        )
        assessment_id = row["id"] if written else ""
        comment = render_judge_comment(
            gate,
            verdict.verdict,
            effective,
            why,
            summary,
            findings,
            verdict.confidence,
            row["model"],
            row["prompt_version"],
            POLICY_VERSION,
            cost,
            mode,
        )
        if where := address(self.cfg.paths.work_root, path):  # the entry that reports a job links its record
            comment += f"\n\n[The judge's conversation]({job_link(self.page_url, where)})\n"
        return JudgeResult(
            verdict.verdict, effective, verdict.confidence, summary, assessment_id, cost, comment, findings
        )

    # --- the operator's switches: the pause and the root budget (spec autonomous-gates §8) -----------

    @activity.defn(name="paused")
    async def paused(self) -> bool:
        """Whether `autonomy.pause` is set in mia.yaml now (re-read: `mia pause` edits the file in place)."""
        return await asyncio.to_thread(autonomy.paused, manifest_path())

    @activity.defn(name="reserve")
    async def reserve(self, root: str, reservation_id: str, kind: str, usd: float) -> Reservation:
        """Charge a job to its root before it launches; a refusal names the cap. Without a root there is no
        budget to check."""
        if not root:
            return Reservation(True, "no budget")
        caps = self.cfg.autonomy.caps()
        out = await asyncio.to_thread(self.registry.reserve, root, reservation_id, kind, usd, caps)
        ok = bool(out.get("ok"))
        return Reservation(ok, "" if ok else str(out.get("reason", "")), wait=bool(out.get("wait")) and not ok)

    @activity.defn(name="settle")
    async def settle(self, root: str, reservation_id: str, usd: float | None = None, fetched_bytes: int = 0) -> None:
        """Close the job's reservation with what it cost and moved; an accounting write never fails a round."""
        if not root:
            return
        registry = self.registry
        await asyncio.to_thread(
            best_effort,
            lambda: registry.settle(root, reservation_id, usd=usd, fetched_bytes=fetched_bytes),
            f"settle {reservation_id}",
            log,
        )

    @activity.defn(name="set_stage")
    @mia_permanent_errors
    async def set_stage(
        self, issue: int, stage: str, state: DatasetState | None = None, detail: str = "", extra: str = ""
    ) -> None:
        """Write the dataset's stage: its registry row, the subject's stage and, within a round, the timeline
        entries of the stage passes involved (`stage_entries`), each once.

        A stage a round works or waits at starts a pass: its entry has the stage line, what the stage is doing
        (`detail`, or the stage's sentence), the caller's `extra` and links to the folders to look at. A pass
        still open in `state` gets its result entry first: what it found (`state.result`), its binding and the
        records of its jobs. A stage the dataset rests at is written by the open pass's result itself, which
        says how the pass ended and where the dataset rests; with no pass open it has an entry of its own.
        Entries are never edited: each start and each result is a new one."""
        if stage not in STAGES:
            raise ValueError(f"unknown stage {stage!r}")
        round_id = state.round_id if state else None
        if round_id:  # the dataset's row: what buffers and requests read
            await asyncio.to_thread(self._report_dataset, self.registry, issue, round_id, stage)
        if not round_id:
            await self._post(issue, "", "", stage=stage)
            return
        gh = self.gh_data
        paths = self.cfg.paths
        links = {"Workspace": paths.link(self._round_workspace(issue, round_id))}
        if stage == "done":
            pins = self._pins(round_id)
            links["Crops"] = paths.link(self._dataset_dir(round_id, pins))
            links["Gallery"] = (
                f"https://github.com/{gh.repo}/tree/{self.cfg.gallery.branch}/galleries/{pins.record}/{round_id}"
            )
        assert state is not None
        entries = stage_entries(
            "data",
            round_id,
            stage,
            is_pass=round_open(stage),
            start_n=state.passes.get(stage, 0) + 1 if round_open(stage) else 1,
            sentence=STAGE_SENTENCES.get(stage, ""),
            detail=self._clean(detail),
            extra=self._clean(extra) if extra else "",
            links=render_links(links),
            open_stage=state.pass_stage,
            open_n=state.passes.get(state.pass_stage, 1),
            result=self._clean(state.result),
            shown=self._shown(state) if state.pass_stage else "",
        )
        for body, marker_text, sets in entries:
            marker = self.cfg.marker(marker_text)
            await self._post(issue, f"{body}\n{marker}", marker, round_id=round_id, stage=sets)

    def _shown(self, state: DatasetState) -> str:
        """What goes under the result of the pass open in `state`: the round's binding when the result is about
        it, and a link to the record of each job the pass submitted (a job that left none is not listed)."""
        links = {}
        for workspace in state.pass_jobs:
            where = address(self.cfg.paths.work_root, workspace)
            if where and record_path(self.cfg.paths.work_root, where).is_file():
                links[Path(workspace).name] = job_link(self.page_url, where)
        binding = render_binding(state.binding, state.binding_hash) if state.result_binding else ""
        return "\n".join(part for part in (binding, render_jobs(links)) if part)

    @activity.defn(name="stage_result")
    @mia_permanent_errors
    async def stage_result(self, issue: int, state: DatasetState) -> None:
        """Write the result entry of the pass open in `state`, once: for a pass that ended where no stage write
        follows (a PR head whose crops failed waits for the next head)."""
        if not (state.round_id and state.pass_stage):
            return
        body = render_result(state.pass_stage, self._clean(state.result))
        shown = self._shown(state)
        count = state.passes.get(state.pass_stage, 1)
        marker = self.cfg.marker(f"result {state.round_id} {state.pass_stage} {count}")
        text = "\n".join(part for part in (body, shown) if part)
        await self._post(issue, f"{text}\n{marker}", marker, round_id=state.round_id)

    @activity.defn(name="comment")
    @mia_permanent_errors
    async def comment(
        self,
        issue: int,
        body: str,
        marker: str,
        repo: str = "data",
        binding_path: str | None = None,
        binding_hash: str | None = None,
    ) -> str | None:
        """Post once per marker, with the binding appended when one is given: a comment on a snippets issue or PR
        (repo="snippets"; returns its URL), or an entry of the dataset's timeline (returns None; see `_post`)."""
        full_marker = self.cfg.marker(marker)
        body = self._clean(body) + (f"\n\n{render_binding(binding_path, binding_hash)}" if binding_path else "")
        if repo == "snippets":
            return await self._comment_once(self.gh_snippets, issue, f"{body}\n\n{full_marker}", full_marker)
        await self._post(issue, f"{body}\n\n{full_marker}", full_marker)
        return None

    @activity.defn(name="refuse_label")
    @mia_permanent_errors
    async def refuse_label(self, issue: int, label: str, reason: str, event_id: str) -> None:
        """Refuse an action we won't act on and say why, once per event (busy `go`, stray `approve`, ...); an event
        that is no action gets the entry alone."""
        actions = (*HUMAN_LABELS, "cancel")  # every action a person can send
        if label not in {self.cfg.label(kind) for kind in actions} or not re.fullmatch(r"\d+", str(event_id)):
            raise ValueError(f"refuse_label: bad label/event {label!r}/{event_id!r}")
        said = f"Ignoring `{label}`: {self._clean(reason)}"
        if await self._action(issue, event_id) is not None:  # a click: the page shows the reason against it
            await asyncio.to_thread(self.registry.refuse_action, event_id, self._clean(reason))
            said = f"Refused `{label.partition(':')[2]}`: {self._clean(reason)}"
        marker = self.cfg.marker(f"refused {label} {event_id}")
        await self._post(issue, f"{said}\n\n{marker}", marker)

    # --- jobs --------------------------------------------------------------------------------

    @activity.defn(name="prepare_match")
    @mia_permanent_errors
    async def prepare_match(self, issue: int, round_id: str, pins: Pins, attempt: int = 1) -> str:
        """Make the match agent's workspace: prompt.md, what `_agent_inputs` writes and, in a round the debugger
        asked for, its advice as advice.md. Returns its path."""
        workspace = self._make_workspace(issue, round_id, "match", attempt)
        await self._agent_inputs(workspace, round_id, pins)
        advice = await self._advice(issue, round_id)
        if advice:
            (workspace / ADVICE_FILE).write_text(advice + "\n")
        (workspace / "prompt.md").write_text(match_prompt(pins, advised=bool(advice)))
        return str(workspace)

    async def _agent_inputs(self, workspace: Path, round_id: str, pins: Pins) -> None:
        """What an agent reads about the round's dataset, written into its workspace: record.yaml, downloads.json,
        the downloader's conversion.json / manifest.json, catalog/<name>.yaml (visible snippets only) and SPEC.md."""
        round_dir = self._round_dir(round_id)
        snapshot_dir = await self._snapshot(pins.snippets_commit)
        shutil.rmtree(workspace / "catalog", ignore_errors=True)
        (workspace / "catalog").mkdir()
        for name, text in visible_snippets(snapshot_dir).items():
            (workspace / "catalog" / f"{name}.yaml").write_text(text)
        shutil.copyfile(round_dir / "record.yaml", workspace / "record.yaml")
        shutil.copyfile(round_dir / "downloads.json", workspace / "downloads.json")
        dataset_dir = json.loads((round_dir / "downloads.json").read_text()).get("dataset_dir")
        for name in ("conversion.json", "manifest.json"):
            if isinstance(dataset_dir, str) and (data := read_untrusted(Path(dataset_dir), name, 16 << 20)) is not None:
                (workspace / name).write_text(data)
        (workspace / "SPEC.md").write_text(read_untrusted(snapshot_dir, "SPEC.md", 1 << 20) or "")

    async def _advice(self, issue: int, round_id: str) -> str:
        """The debugger's advice for this round: the note of the go that started it, when that go is the
        debugger's. "" for any other round."""
        if TEST_PR_RE.fullmatch(round_id):
            return ""
        action = await self._action(issue, round_id.split("-e", 1)[1])
        if action is None or action["actor"] != DEBUGGER:
            return ""
        return self._clean(_text(action["payload"].get("note"), 16000))

    # --- debugging a failed round ------------------------------------------------------------

    @activity.defn(name="prepare_debug")
    @mia_permanent_errors
    async def prepare_debug(self, issue: int, round_id: str, pins: Pins, failure: str, binding: str | None) -> str:
        """Make the debugging agent's workspace: what the match agent reads, `failure.md` (what stopped the round
        and the limits its jobs ran under), the round's checked binding when it got that far, and `round/<job>/`
        with what each job of the round left (ROUND_FILES and the end of its stdout and stderr), read as untrusted,
        cut to size and cleaned of anything that looks like a credential. Returns its path.

        It takes exactly the arguments the workflow sends: Temporal converts an argument to its annotated type
        only when their number matches the parameters', and `pins` must arrive as `Pins`."""
        workspace = self._make_workspace(issue, round_id, "debug", 1)
        await self._agent_inputs(workspace, round_id, pins)
        (workspace / "binding.yaml").unlink(missing_ok=True)
        if binding:
            if not Path(binding).resolve().is_relative_to(self._round_dir(round_id).resolve()):
                raise ValueError(f"binding {binding} is not one of round {round_id}'s")
            shutil.copyfile(binding, workspace / "binding.yaml")
        shutil.rmtree(workspace / "round", ignore_errors=True)
        for job_dir in sorted(self._round_workspace(issue, round_id).iterdir()):
            if job_dir == workspace or job_dir.is_symlink() or not job_dir.is_dir():
                continue
            left = {rel: read_untrusted(job_dir, rel, EVIDENCE_BYTES) for rel in ROUND_FILES}
            jobs = job_dir / ".csub" / "jobs"
            runs = sorted(entry.name for entry in jobs.iterdir() if entry.name.isdecimal()) if jobs.is_dir() else []
            for log in ("stdout", "stderr") if runs else ():
                left[log] = read_untrusted(job_dir, f".csub/jobs/{runs[-1]}/{log}", LOG_BYTES, tail=True)
            for rel, data in left.items():
                if data:
                    (workspace / "round" / job_dir.name / rel).parent.mkdir(parents=True, exist_ok=True)
                    (workspace / "round" / job_dir.name / rel).write_text(self._clean(data))  # no credential
        limits = self.cfg.limits
        (workspace / "failure.md").write_text(
            f"# What stopped the round\n\n{self._clean(_text(failure, 8000))}\n\n"
            "# The limits its jobs ran under\n\n"
            f"- Time: {limits.job_walltime_min} minutes per job. A job the cluster kills at this limit ends with exit "
            "code 140 and writes no report.\n"
            f"- Data: {int(limits.job_bytes)} bytes fetched per job.\n"
            "- Your own job runs under the same two limits.\n"
        )
        (workspace / "prompt.md").write_text(debug_prompt(pins))
        return str(workspace)

    @activity.defn(name="read_debug")
    @mia_permanent_errors
    async def read_debug(self, issue: int, round_id: str, workspace: str) -> DebugResult:
        """Read the debugging agent's outcome.json into a DebugResult; anything else it left is `failed`."""
        outcome = _job_json(self._job_workspace(issue, round_id, workspace), "outcome.json")
        if not isinstance(outcome, dict) or outcome.get("outcome") not in ("rerun", "blocked"):
            return DebugResult("failed", "the debugging agent left no readable outcome.json")
        return DebugResult(
            outcome["outcome"],
            self._clean(_text(outcome.get("diagnosis"), 6000)) or "(the debugging agent gave no diagnosis)",
            self._clean(_text(outcome.get("advice"), 6000)),
        )

    @activity.defn(name="debug_go")
    @mia_permanent_errors
    async def debug_go(self, issue: int, round_id: str, advice: str) -> None:
        """Ask for the dataset's next round as the debugger: a `go` through buffer-api like any other, charged to the
        root the debugged round was, carrying the advice as its note. One go per debugged round, however often this
        is retried."""
        pins = self._pins(round_id)
        go = functools.partial(
            self.registry.send_action,
            f"/datasets/{pins.record}/actions",
            "go",
            key=f"debug {round_id}",
            actor=DEBUGGER,
            **({"root": pins.root} if pins.root else {}),
            **({"note": advice} if advice else {}),
        )
        await asyncio.to_thread(go)

    @activity.defn(name="prepare_job")
    @mia_permanent_errors
    async def prepare_job(self, issue: int, round_id: str, kind: str, attempt: int, binding: str | None) -> str:
        """Make a smoke/crop/verify workspace with record.yaml and (copied from state_dir) binding.yaml.
        Returns its path."""
        workspace, round_dir = self._make_workspace(issue, round_id, kind, attempt), self._round_dir(round_id)
        shutil.copyfile(round_dir / "record.yaml", workspace / "record.yaml")
        if binding:
            if not Path(binding).resolve().is_relative_to(round_dir.resolve()):
                raise ValueError(f"binding {binding} is not one of round {round_id}'s")
            shutil.copyfile(binding, workspace / "binding.yaml")
        return str(workspace)

    @activity.defn(name="probe_hosts")
    @mia_permanent_errors
    async def probe_hosts(self) -> list[str]:
        """`csub probe`: the policy's allowed_hosts patterns."""
        return list(await asyncio.to_thread(self.probe))

    def _spec(self, kind: str, args: JobArgs, snapshot_dir: Path | None) -> JobSpec:
        """Build the JobSpec for one job kind from its arguments."""
        cfg = self.cfg
        if kind == "match":
            return match_job(cfg, args)
        if kind == "judge":
            return judge_job(cfg, args)
        if kind == "debug":
            return debug_job(cfg, args)
        existing = self._dataset_dir(args.round_id, args.pins) / "manifest.json" if args.existing else None
        existing = existing if existing and existing.is_file() else None
        if kind in ("smoke", "crop"):
            if snapshot_dir is None:
                raise ValueError(f"{kind} job needs a snippets snapshot")
            if kind == "smoke":
                return smoke_job(cfg, args, snapshot_dir)
            return crop_job(cfg, args, snapshot_dir, existing)
        if kind == "verify":
            if args.crop_workspace is None:
                raise ValueError("verify job needs crop_workspace")
            crop_workspace = Path(args.crop_workspace)
            if crop_workspace.parent != Path(args.workspace).parent:
                raise ValueError(f"crop workspace {args.crop_workspace} is not of this round")
            return verify_job(cfg, args, self._crops_dir(crop_workspace), existing)
        raise ValueError(f"unknown job kind {kind!r}")

    @activity.defn(name="submit")
    @mia_permanent_errors
    async def submit(self, key: str, kind: str, args: JobArgs) -> str:
        """Build the job (the builders refuse a moved harness pin), refuse hosts a fresh `csub probe` doesn't
        allow, and submit at most once per key. Returns the job id."""
        from csub import CsubError  # pixi-only dependency, imported where used (as autodev does)

        patterns = await asyncio.to_thread(self.probe)
        snapshot_dir = (
            await self._snapshot(args.sha or args.pins.snippets_commit) if kind in ("smoke", "crop") else None
        )

        def build() -> JobSpec:
            spec = self._spec(kind, args, snapshot_dir)
            check_hosts_allowed(spec, patterns)
            return spec

        try:
            return await self.jobs.submit(key, build)
        except CsubError as error:
            if error.code in PERMANENT_CSUB:
                raise ApplicationError(str(error), non_retryable=True) from error
            raise

    @activity.defn(name="await_job")
    async def await_job(self, job_id: str, workspace: str, lock: list[str] | None = None) -> JobDone:
        """Wait for the job, heartbeating; with `lock` ([kind, key, owner]) each heartbeat also refreshes that lock.
        A lock found gone kills the job and fails the activity permanently (spec autonomous-gates §3)."""
        heartbeat = heartbeat_through(lock_dir(self.cfg), lock, activity.heartbeat) if lock else activity.heartbeat
        try:
            outcome = await self.jobs.wait(job_id, workspace, heartbeat)
        except LockLost as error:  # the runner killed the job; the round fails for good rather than retrying the wait
            raise ApplicationError(str(error), type="LockLost", non_retryable=True) from error
        return JobDone(outcome.job_id, outcome.state, outcome.exit_code)

    @activity.defn(name="kill_job")
    async def kill_job(self, job_id: str) -> None:
        """Kill a cluster job by id: the job of a round that was cancelled (spec webapp-actions §5). The wait on
        it is cancelled by the workflow; a job that already ended is not an error to the runner."""
        await asyncio.to_thread(self.jobs.client.kill, [job_id])

    # --- locks -------------------------------------------------------------------------------

    @activity.defn(name="take_lock")
    @mia_permanent_errors
    async def take_lock(self, kind: str, key: str, owner: str, ttl_s: int) -> LockTaken:
        """Take the lock file `<local_root>/locks/<kind>/<key>`; when someone else holds it, say who."""
        return LockTaken(*await asyncio.to_thread(take_or_holder, lock_dir(self.cfg), kind, key, owner, ttl_s))

    @activity.defn(name="release_lock")
    @mia_permanent_errors
    async def release_lock(self, kind: str, key: str, owner: str) -> None:
        """Release the lock if `owner` still holds it; a lock that lapsed or changed hands is logged and left."""
        await asyncio.to_thread(release_or_log, lock_dir(self.cfg), kind, key, owner)

    # --- job results -------------------------------------------------------------------------

    @activity.defn(name="read_match")
    @mia_permanent_errors
    async def read_match(self, workspace: str) -> MatchResult:
        """Read the match agent's outcome.json and, for matched, check its binding.yaml.

        A checked binding is stored (with the round's snippets_commit) as
        state_dir/rounds/<round>/binding-<attempt>.yaml. The agent says whether it stopped `blocked` (a person has
        to decide or change something) or `failed` (running again may pass); anything we can't trust is `failed`
        with the reason."""
        path = Path(workspace)
        if not (match := re.fullmatch(r"match-(\d+)", path.name)):
            raise ValueError(f"{workspace} is not a match workspace")
        round_id = path.parent.name
        round_dir, pins = self._round_dir(round_id), self._pins(round_id)
        outcome = _job_json(workspace, "outcome.json")
        if not isinstance(outcome, dict):
            return MatchResult("failed", "outcome.json is missing, unreadable or not a JSON object")
        kind = outcome.get("outcome")
        if kind in ("blocked", "failed"):
            return MatchResult(kind, _text(outcome.get("reason")) or f"the match agent reported {kind}, with no reason")
        if kind == "needs_snippet":
            draft = _small_dict({key: outcome.get(key) for key in ("draft_binding", "what_fails", "tried")}, 32768)
            return MatchResult("needs_snippet", draft=draft or {"what_fails": "(the agent's draft was too large)"})
        if kind != "matched":  # size is the buffer plan's call, not ours: no outcome says a dataset is too small
            return MatchResult("failed", f"unknown match outcome {_text(kind, 100)!r}")
        binding, errors = await self._check_binding(
            read_untrusted(path, "binding.yaml", 1 << 20), round_id, pins, pins.snippets_commit
        )
        if errors:
            return MatchResult("failed", _text("binding check failed: " + "; ".join(errors)))
        binding_path, digest = self._store_binding(
            round_dir / f"binding-{match.group(1)}.yaml", {**binding, "snippets_commit": pins.snippets_commit}
        )
        return MatchResult(
            kind,
            binding_path=binding_path,
            binding_hash=digest,
            reasons=_texts(binding.get("reasons")),
        )

    async def _check_binding(self, text: str | None, round_id: str, pins: Pins, sha: str) -> tuple[dict, list[str]]:
        """Parse an agent-written binding and check it against the round's record and the snippets at `sha`.
        Returns (binding, errors); the binding is empty when there are errors."""
        if text is None:
            return {}, ["the binding is missing or unreadable"]
        try:
            binding = yaml.safe_load(text)
            errors = check_binding(
                binding, self._record(round_id), set(visible_snippets(await self._snapshot(sha))), pins=pins
            )
            if not errors:  # hosts() raises if a URL would grant a GitHub host
                hosts(
                    [url for array in binding["raw"] + (binding.get("labels") or []) for url in array["urls"]], self.cfg
                )
        except (yaml.YAMLError, ValueError, RecursionError) as error:  # a malformed URL raises in urlsplit
            errors = [f"{type(error).__name__}: {error}"]
        return binding if not errors else {}, errors

    @staticmethod
    def _store_binding(path: Path, binding: dict) -> tuple[str, str]:
        """Write the binding as YAML. Returns (path, sha256 of the written bytes)."""
        write_atomic(path, data := yaml.safe_dump(binding, sort_keys=False).encode())
        return str(path), hashlib.sha256(data).hexdigest()

    def _put_raw_asset(self, record: str, workspace: str) -> None:
        """Report the record's raw cache (what `_spec` mounts) to the registry, best effort; its urls are those of
        the binding.yaml in `workspace`, read as untrusted since the job could write there."""
        if not ID_RE.fullmatch(record):
            raise ValueError(f"bad record id {record!r}")
        registry = self.registry
        try:
            binding = yaml.safe_load(read_untrusted(Path(workspace), "binding.yaml", 1 << 20) or "")
        except yaml.YAMLError:
            binding = None
        arrays = [
            array
            for name in ("raw", "labels")
            for array in ((binding.get(name) if isinstance(binding, dict) else None) or [])
            if isinstance(array, dict) and isinstance(array.get("urls"), list)
        ]
        urls = [url for array in arrays for url in array["urls"] if isinstance(url, str)]
        cache_dir = self.cfg.paths.work_root / "_cache" / record
        row = {**raw_asset(cache_dir, urls), "last_used": time.time()}
        best_effort(lambda: registry.put_asset("raw", record, row), f"raw asset {record}", log)

    @activity.defn(name="read_smoke")
    @mia_permanent_errors
    async def read_smoke(self, workspace: str, record: str | None = None) -> SmokeResult:
        """Read the smoke job's smoke.json into a SmokeResult, tolerating any shape. With `record` (the job held
        its raw-cache lock), the record's raw asset row is put to the registry first."""
        if record is not None:
            await asyncio.to_thread(self._put_raw_asset, record, workspace)
        report = _job_json(workspace, "smoke.json")
        if not isinstance(report, dict):
            return SmokeResult(False, errors=["smoke.json is missing, unreadable or not a JSON object"])
        fetched = report.get("bytes_fetched")
        return SmokeResult(
            report.get("ok") is True,
            report.get("eligible") is True,
            _small_dict(report.get("shapes")),
            _texts(report.get("errors")),
            _texts(report.get("warnings")),
            fetched if type(fetched) is int and fetched >= 0 else 0,
            _text(report.get("host_not_allowed"), 300) or None,
        )

    @activity.defn(name="read_verify")
    @mia_permanent_errors
    async def read_verify(
        self, workspace: str, crop_workspace: str | None = None, record: str | None = None
    ) -> VerifyResult:
        """Read verify.json: only crops with ok, a valid id and sha256s for crop.json and raw.png count.

        Top-level errors or n == 0 mean nothing was verified. With `crop_workspace`, n_effective, bytes_fetched,
        crop_errors and host_not_allowed come from the crop job's report.json. With `record` (a trusted crop job that held the raw-cache
        lock), the record's raw asset row is put to the registry first, with the urls of the crop job's binding."""
        if record is not None:
            await asyncio.to_thread(self._put_raw_asset, record, crop_workspace or workspace)
        report = _job_json(workspace, "verify.json")
        if not isinstance(report, dict):
            return VerifyResult(0, 0, ["verify.json is missing, unreadable or not a JSON object"], [])
        errors = _texts(report.get("errors"))
        n_asked, crops = report.get("n"), report.get("crops")
        n_crops = n_asked if type(n_asked) is int and n_asked >= 0 else 0
        verified, failed, seen = [], [], set()
        for crop in (crops if isinstance(crops, list) else [])[:1000]:
            if not isinstance(crop, dict):
                continue
            crop_id, files = crop.get("crop_id"), crop.get("files")
            if (
                crop.get("ok") is True
                and isinstance(crop_id, str)
                and CROP_ID_RE.fullmatch(crop_id)
                and crop_id not in seen
                and isinstance(files, dict)
                and len(files) <= 64
                and {"crop.json", "raw.png"} <= files.keys()
                and all(
                    isinstance(rel, str) and isinstance(digest, str) and HEX64.fullmatch(digest)
                    for rel, digest in files.items()
                )
            ):
                verified.append({"crop_id": crop_id, "files": files})
                seen.add(crop_id)
            else:
                failed.append(f"{_text(crop_id, 100)}: {'; '.join(_texts(crop.get('errors'), 5)) or 'not verified'}")
        if not n_crops and not errors:
            errors = ["verify checked no crops"]
        if errors:
            verified = []
        crop_report = _job_json(crop_workspace, "crops/report.json") if crop_workspace else None
        crop_report = crop_report if isinstance(crop_report, dict) else {}

        def count(key: str) -> int | None:
            value = crop_report.get(key)
            return value if type(value) is int and value >= 0 else None

        return VerifyResult(
            len(verified),
            n_crops,
            (errors + failed)[:20],
            verified,
            count("n"),
            count("bytes_fetched") or 0,
            _texts(crop_report.get("errors"))[:10],
            _text(crop_report.get("host_not_allowed"), 300) or None,
        )

    # --- publication -------------------------------------------------------------------------

    @activity.defn(name="publish_gallery")
    @mia_permanent_errors
    async def publish_gallery(
        self, issue: int, round_id: str, kind: str, crop_workspace: str, verify_workspace: str, verified: list[dict]
    ) -> GalleryResult:
        """Publish raw.png + overlay.png of the verified crops, with the binding and verify report, under
        galleries/<record>/<round>/<kind> (kind = the tested SHA) on the galleries branch; a test-pr round's go
        under galleries/_test-pr/<record>/<pr>/<kind>. Returns the URLs and a crops table."""
        if not SHA_RE.fullmatch(kind):
            raise ValueError(f"bad gallery kind {kind!r}")
        crop_dir = self._job_workspace(issue, round_id, crop_workspace)
        verify_dir = self._job_workspace(issue, round_id, verify_workspace)
        record_id, test_pr = self._pins(round_id).record, TEST_PR_RE.fullmatch(round_id)
        prefix = (
            f"galleries/_test-pr/{record_id}/{test_pr.group(1)}/{kind}"
            if test_pr
            else f"galleries/{record_id}/{round_id}/{kind}"
        )
        files = await asyncio.to_thread(gallery_files, crop_dir, verify_dir, verified)
        urls = await publish_gallery(self.gh_data, self.cfg.gallery.branch, prefix, files)
        rows = []
        for crop in verified:
            crop_id = crop["crop_id"]
            raw = read_untrusted(crop_dir, f"crops/{crop_id}/crop.json", 1 << 20, binary=True)
            if raw is None or hashlib.sha256(raw).hexdigest() != crop["files"]["crop.json"]:
                raise ValueError(f"{crop_id}/crop.json is missing or differs from the verified checksum")
            crop_json = parsed if isinstance(parsed := _loads(raw), dict) else {}
            rows.append(
                {
                    "crop_id": crop_id,
                    "plane": crop_json.get("plane") if isinstance(crop_json.get("plane"), dict) else {},
                    "box": crop_json.get("box") if isinstance(crop_json.get("box"), dict) else {},
                    "raw_url": urls[f"{crop_id}/raw.png"],
                    "overlay_url": urls[f"{crop_id}/overlay.png"],
                }
            )
        return GalleryResult(prefix, urls, render_crops_table(rows))

    @activity.defn(name="publish_crops")
    @mia_permanent_errors
    async def publish_crops(
        self,
        issue: int,
        round_id: str,
        crop_workspace: str,
        verified: list[dict],
        sha: str,
        binding: str | None = None,
    ) -> dict:
        """Publish the verified crops into the dataset folder, marking crops of snippets withdrawn in
        mia-snippets at `sha`, and the approved binding (one of this round's, from state_dir) as binding.yaml
        beside them for mia-buffers to read (spec §6.1). Each crop's overlay.png (drawn by the round's verify jobs
        into their preview/ dir) goes beside its files for the web UI's preview. Returns {"published", "already",
        "total"}."""
        crops_dir = self._crops_dir(self._job_workspace(issue, round_id, crop_workspace))
        previews = sorted(self._round_workspace(issue, round_id).glob("verify-*/preview"), reverse=True)
        if binding and not Path(binding).resolve().is_relative_to(self._round_dir(round_id).resolve()):
            raise ValueError(f"binding {binding} is not one of round {round_id}'s")
        withdrawn = withdrawn_snippets(await self._snapshot(sha))
        timeout = activity.info().start_to_close_timeout if activity.in_activity() else None
        pins = self._pins(round_id)
        dataset_dir = self._dataset_dir(round_id, pins)
        crop_publish.check_owner(dataset_dir, pins.record)
        revoked = (dataset_dir / REVOKED_FILE).is_file()
        published = await asyncio.to_thread(
            crop_publish.publish,
            dataset_dir,
            crops_dir,
            verified,
            withdrawn,
            tmp_max_age_s=2 * (timeout.total_seconds() if timeout else 3600),
            binding=Path(binding) if binding else None,
            previews=previews,
        )
        if revoked:  # a new publication repairs a revocation (spec autonomous-gates §6.5)
            (dataset_dir / REVOKED_FILE).unlink(missing_ok=True)
            await self._release_revoked(round_id, pins.record)
        return published

    async def _release_revoked(self, round_id: str, record: str) -> None:
        """After a repair: every quarantined staged artifact of the record gets a `policy:repair` approve (the newest
        assessment, so buffers may reuse it) and its quarantine mark cleared. Best effort: the publication stands."""
        registry = self.registry
        try:
            assets = (await asyncio.to_thread(registry.assets, "staged", None, record))["assets"]
            for asset in assets:
                data = asset.get("data") or {}
                if not data.get("quarantined"):
                    continue
                key, now = str(asset["key"]), time.time()
                approve = new_assessment(
                    gate="staged",
                    subject_kind="artifact",
                    subject_ref=key,
                    verdict="approve",
                    by="policy:repair",
                    at=now,
                    summary=f"repaired by {round_id}: {record} was saved again",
                )
                await asyncio.to_thread(registry.put_assessment, approve)
                release = {"quarantined": False, "repaired_at": now, "repaired_by": round_id}
                await asyncio.to_thread(registry.put_asset, "staged", key, release)
        except RegistryError as error:
            log.warning("round %s: the revoked artifacts of %s were not released: %s", round_id, record, error)

    @activity.defn(name="revoke_dataset")
    @mia_permanent_errors
    async def revoke_dataset(self, issue: int, round_id: str, actor: str, event_id: str) -> Revocation:
        """A revoke (spec autonomous-gates §6): a reject assessment superseding the dataset's approve, a reject
        and a quarantine mark on every staged artifact of the record, the dataset row flipped to `revoked`, and
        `revoked.json` written last beside the manifest (the folder is no longer prepared; nothing is deleted).
        A retry reads the marker and returns what the first run recorded."""
        registry = self.registry
        pins = self._pins(round_id)
        record, folder = pins.record, self._dataset_dir(round_id, pins)
        marker = folder / REVOKED_FILE
        if marker.is_file():
            return Revocation(**json.loads(marker.read_text())["revocation"])
        if not (folder / "manifest.json").is_file():  # nothing was ever published here: nothing to withdraw
            return Revocation("")
        by, at = f"human:{actor}", time.time()
        summary = f"data:revoke by {actor} (event {event_id})"
        row = await asyncio.to_thread(registry.dataset, record) or {}
        approve_id = row.get("assessment")
        approve = await asyncio.to_thread(registry.assessment, approve_id) if approve_id else None
        subject = approve["subject"] if approve else {"kind": "crop_set", "ref": f"{round_id}:revoked"}
        reject = new_assessment(
            gate="crops",
            subject_kind=subject["kind"],
            subject_ref=subject["ref"],
            verdict="reject",
            by=by,
            at=at,
            summary=summary,
            supersedes=approve_id,
            action=_action(event_id),
        )
        await asyncio.to_thread(registry.put_assessment, reject)
        assets = (await asyncio.to_thread(registry.assets, "staged", None, record))["assets"]
        artifacts: list[str] = []
        for asset in assets:
            if not asset.get("data"):
                continue
            key = str(asset["key"])
            artifact_reject = new_assessment(
                gate="staged",
                subject_kind="artifact",
                subject_ref=key,
                verdict="reject",
                by=by,
                at=at,
                summary=summary,
                action=_action(event_id),
            )
            await asyncio.to_thread(registry.put_assessment, artifact_reject)
            quarantine = {"quarantined": True, "quarantined_at": at, "quarantined_by": by}
            await asyncio.to_thread(registry.put_asset, "staged", key, quarantine)
            artifacts.append(key)
        buffers = sorted({str(ref) for asset in assets for ref in asset.get("refs") or []})
        await asyncio.to_thread(
            registry.put_dataset,
            record,
            state="revoked",
            revoked_at=datetime.fromtimestamp(at, UTC).isoformat(),
            revoked_by=by,
            assessment=reject["id"],
        )
        revocation = Revocation(reject["id"], sorted(artifacts), buffers)
        folder.mkdir(parents=True, exist_ok=True)
        record_of = {"record": record, "by": by, "at": at, "issue": issue, "event_id": event_id}
        write_atomic(marker, json.dumps({**record_of, "revocation": dataclasses.asdict(revocation)}, indent=1))
        return revocation

    # --- snippet development -----------------------------------------------------------------

    @activity.defn(name="read_feedback")
    @mia_permanent_errors
    async def read_feedback(self, issue: int, since: str | None, until: str) -> str:
        """The reviewer's feedback: the note of each reject action after `since` (the set's report) and up to
        `until` (the reject; the workflow passes the action's own `at`, which is why an action's time is stored
        exactly as it was sent). It goes to the agent fixing the snippet (spec §14). Neutralized and capped;
        "(no details)" when there is none."""
        found = await asyncio.to_thread(self.registry.timeline, "dataset", issue)
        after, upto = _when(since), _when(until)

        def in_window(action: dict) -> bool:
            acted = _when(action["at"])
            return bool(acted and upto and (after is None or after < acted) and acted <= upto)

        notes = [
            f"{action['actor']} wrote:\n{_text(action['payload'].get('note'), 8000)}"
            for action in (found or {}).get("actions", [])
            if action["action"] == "reject" and in_window(action) and action["payload"].get("note")
        ]
        return self._clean("\n\n".join(notes))[:16000] or "(no details)"

    @activity.defn(name="open_snippet_issue")
    @mia_permanent_errors
    async def open_snippet_issue(
        self, issue: int, round_id: str, draft: dict, snippet_issue: int | None = None, binding: str | None = None
    ) -> int:
        """Open the mia-snippets issue asking autodev for a new snippet, or find it again: the one the workflow
        remembers as `snippet_issue` from an earlier round, while it is still open, else by this round's
        first-line marker on a retry. Returns the issue number.

        With `binding` (one of this round's, from state_dir), the request is to supersede the snippets that
        binding names: a reviewer rejected its crops and `draft["what_fails"]` is their comments (spec §14)."""
        gh, github_cfg, pins = self.gh_snippets, self.cfg.github, self._pins(round_id)
        if binding and not Path(binding).resolve().is_relative_to(self._round_dir(round_id).resolve()):
            raise ValueError(f"binding {binding} is not one of round {round_id}'s")
        marker = self.cfg.marker(f"issue={int(issue)} round={round_id}")

        def ours(existing: dict, prefix: str) -> bool:  # filter by author: an outsider can copy the marker
            return (existing.get("user") or {}).get("login") == self.bot and (existing.get("body") or "").startswith(
                prefix
            )

        if snippet_issue:  # any round's marker for this data issue will do: the round id is new on a re-trigger
            remembered = await gh.get_issue(int(snippet_issue))
            any_round = self.cfg.marker(f"issue={int(issue)} round=").removesuffix(" -->")
            if remembered.get("state") != "closed" and ours(remembered, any_round):
                return remembered["number"]
        # ponytail: lists every issue; one call per round.
        for existing in await gh.list_issues(state="all"):
            if ours(existing, marker):
                return existing["number"]
        record = self._record(round_id)
        urls = record_urls(record)
        data_format = _text(((record.get("data") or {}).get("formats") or ["unknown"])[0], 40)
        # ponytail: container guessed from the record's URLs; the agent's what_fails is the authority
        container = (
            "zip"
            if any("::" in url or url.split("?")[0].endswith(".zip") for url in urls)
            else "s3"
            if any(url.startswith("s3://") for url in urls)
            else "http"
        )
        excerpt = yaml.safe_dump(
            {key: record.get(key) for key in ("title", "imaging", "data", "technical")}, sort_keys=False
        )
        draft = draft or {}
        # the dataset's page, not its issue: a dataset may have no issue yet, and its number is not an issue
        # number (spec webapp-actions §4.1, §9)
        data_link = f"[{pins.record}]({self.page_url}/#datasets/{quote(pins.record, safe='')})"
        spec_link = f"[SPEC.md](https://github.com/{github_cfg.snippets_repo}/blob/main/SPEC.md)"
        if binding:  # a reviewer rejected this binding's crops: supersede its snippets
            bound = yaml.safe_load(Path(binding).read_text()) or {}
            names = sorted(
                {
                    array["snippet"]
                    for section in ("raw", "labels")
                    for array in bound.get(section) or []
                    if isinstance(array, dict) and isinstance(array.get("snippet"), str)
                }
            )
            listed = ", ".join(f"`{name}`" for name in names) or "(none named)"
            parts = [
                marker,
                f"For {data_link}: dataset `{pins.record}` is read by {listed}, but a reviewer rejected the crops "
                "made with this binding.",
                "**The reviewer's comments:**",
                _fenced(self._clean(_text(draft.get("what_fails"), 16000))),
                f"Read {spec_link} first. A merged snippet is never edited: add a new one that **supersedes** the "
                f"one at fault (`supersedes: <name>` in its `snippet.yaml`, SPEC §1), and add the binding you test, "
                f"using the new snippet, as `examples/{pins.record}.yaml`.",
                "**Record** (at `{pins.record_commit}`):".replace("{pins.record_commit}", pins.record_commit),
                _fenced(self._clean(excerpt[:8000]), "yaml"),
                "**The rejected binding:**",
                _fenced(self._clean(yaml.safe_dump(bound, sort_keys=False)[:16000]), "yaml"),
            ]
            title = f"Fix the crops of {self._clean(pins.record)}: supersede {self._clean(', '.join(names))}"
        else:
            parts = [
                marker,
                f"For {data_link}: dataset `{pins.record}` has no snippet that reads it.",
                f"Read {spec_link} first, and add the binding you test as `examples/{pins.record}.yaml`.",
                f"**Record** (at `{pins.record_commit}`):",
                _fenced(self._clean(excerpt[:8000]), "yaml"),
                "**What fails** (the match agent's report):",
                _fenced(self._clean(_text(draft.get("what_fails"), 4000))),
                "**Tried:**",
                _fenced(self._clean(json.dumps(draft.get("tried"), indent=1)[:8000]), "json"),
                "**Draft binding:**",
                _fenced(self._clean(yaml.safe_dump(draft.get("draft_binding"), sort_keys=False)[:16000]), "yaml"),
            ]
            title = f"New snippet for {self._clean(data_format)}/{container}: {self._clean(pins.record)}"
        return (await gh.create_issue(title, "\n\n".join(parts), ["snippets:go"]))["number"]

    @activity.defn(name="pr_state")
    @mia_permanent_errors
    async def pr_state(self, pr: int) -> PrUpdate:
        """The PR's current head, state and merge SHA, read fresh, and whether its issue needs a human."""
        pr_data = await self.gh_snippets.get_pr(pr)
        match = re.fullmatch(r"snippets/issue-(\d+)", pr_data["head"]["ref"])
        if not match or ((pr_data["head"].get("repo") or {}).get("full_name")) != self.gh_snippets.repo:
            raise ValueError(f"PR #{pr} is not on a snippets/issue-<K> branch of {self.gh_snippets.repo}")
        snippet_issue, merged = int(match.group(1)), bool(pr_data.get("merged"))
        labels = {label["name"] for label in (await self.gh_snippets.get_issue(snippet_issue)).get("labels", [])}
        return PrUpdate(
            pr,
            pr_data["head"]["sha"],
            "merged" if merged else pr_data["state"],
            pr_data.get("merge_commit_sha") if merged else None,
            snippet_issue,
            "snippets:needs-human" in labels,
            pr_data.get("html_url"),
        )

    @activity.defn(name="set_crop_status")
    @mia_permanent_errors
    async def set_crop_status(self, sha: str, state: str, url: str | None, description: str = "") -> None:
        """Post the `mia-data/crops` commit status on a snippets PR head."""
        if not SHA_RE.fullmatch(sha) or state not in CROP_STATUS:
            raise ValueError(f"bad status {sha!r}/{state!r}")
        await self.gh_snippets.set_status(sha, "mia-data/crops", state, description or CROP_STATUS[state], url)

    @activity.defn(name="tree_sha")
    @mia_permanent_errors
    async def tree_sha(self, sha: str, name: str) -> str | None:
        """Tree SHA of snippets/<name> at `sha` (tells whether a merge changed the snippet); None if absent."""
        if not SHA_RE.fullmatch(sha) or not NAME_RE.fullmatch(name):
            raise ValueError(f"bad tree lookup {sha!r}/{name!r}")
        return await self.gh_snippets.dir_sha(f"snippets/{name}", sha)

    @activity.defn(name="adopt_pr_binding")
    @mia_permanent_errors
    async def adopt_pr_binding(self, round_id: str, pr: int, sha: str) -> dict:
        """Take the PR's example binding, `snippets/<name>/examples/<record>.yaml` at `sha` (a head or the merge
        commit), where <name> is the one snippet directory the PR adds.

        It is checked like the match agent's binding (this record, its hosts, the schema) and must use <name>,
        then stored with `snippets_commit: <sha>` as state_dir/rounds/<round>/binding-pr-<sha7>.yaml. Returns
        {"path", "hash", "name", "errors": []}, or {"errors": [...]}: a bad example fails that head's test."""
        if not SHA_RE.fullmatch(sha):
            raise ValueError(f"bad sha {sha!r}")
        gh, round_dir, pins = self.gh_snippets, self._round_dir(round_id), self._pins(round_id)
        names = sorted(
            {file.split("/")[1] for file in await gh.pr_files(pr) if file.startswith("snippets/") and "/" in file[9:]}
        )
        if len(names) != 1 or not NAME_RE.fullmatch(name := names[0]):
            return {"errors": [_text(f"the PR must add exactly one snippets/<name>/ directory, not {names}", 500)]}
        if not await gh.dir_sha(f"snippets/{name}", sha):
            return {"errors": [f"snippets/{name} is not in {sha}"]}
        rel = f"snippets/{name}/examples/{pins.record}.yaml"
        raw = await gh.get_file(rel, sha)
        text = raw.decode(errors="replace") if raw is not None and len(raw) <= 1 << 20 else None
        binding, errors = await self._check_binding(text, round_id, pins, sha)
        if binding and name not in {array["snippet"] for array in binding["raw"] + (binding.get("labels") or [])}:
            errors = [f"{rel} does not use snippet {name!r}"]
        if errors:
            return {"errors": [f"{rel}: {error}" for error in _texts(errors)]}
        binding_path, digest = self._store_binding(
            round_dir / f"binding-pr-{sha[:7]}.yaml", {**binding, "snippets_commit": sha}
        )
        return {"path": binding_path, "hash": digest, "name": name, "errors": []}

    # --- the poller --------------------------------------------------------------------------

    @activity.defn(name="poll")
    @mia_permanent_errors
    async def poll(self) -> list[str]:
        """One scheduled pass over GitHub: labels become signals, changed snippet PRs wake their workflows
        (see poll.py). Returns a log of what was done."""
        return await run_poll(self)
