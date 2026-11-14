"""buffer-api: the one sampler and ledger for a buffer, as a small HTTP service (spec v0.5 §5).

`BufferService.handle` does all routing and validation without sockets; `make_server` wraps it in the standard
library's threading HTTP server. Consumers are untrusted (§7): bodies are validated strictly and a consumer can
only name its split.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import random
import re
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import asdict, replace
from datetime import UTC, datetime
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import parse_qsl, quote, unquote, urlparse, urlsplit

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from mia_core.assets import ASSET_STATES
from mia_core.fs import require_local_dir
from mia_core.hosts import PolicyError, generate_policy
from mia_core.jobrecord import RECORDS_DIR
from mia_core.judge.policy import GATES, SUBJECT_KINDS, VERDICTS
from mia_core.locks import LockDir
from mia_core.registry import read_token
from mia_core.untrusted import neutralize, read_untrusted
from mia_data import config as mia_data_config
from mia_data.binding import record_gaps
from mia_data.models import LabelEvent

from .config import PREP_STAGE_ACTIONS, STAGE_ACTIONS, Config
from .dispatch import DispatchError
from .explore import (
    DATASET_FILTERS,
    DATASET_SORTS,
    PLAN_SORTS,
    Page,
    dataset_haystack,
    dataset_row,
    facets,
    page_query,
    paged,
    plan_view,
    record_row,
)
from .hosts import HostCheck, Needed
from .jobspec import levels_key
from .ledger import BOARD_STATUSES, EVENT_KINDS, Ledger, head_of
from .plan import build_plan
from .sampler import Level, Volume, capacity, draw, draw_frozen, fits, servable
from .spec import SpecError, load_prep_text, load_spec_text, spec_text_hash
from .stream import Changes
from .sync import scan_assets, scan_storage

RETRY_AFTER_S = 30
DEFAULT_SPLIT_SIZE = 256
MAX_N = 1024  # descriptors per request; consumers are untrusted (§7)
# no row = unprepared; `ineligible` ended with spec §12; `revoked` since autonomous-gates §6
DATASET_STATES = ("in-progress", "prepared", "needs-human", "revoked")
CROP_PNG_LIMIT = 8 << 20  # a 256x256 PNG is kilobytes; the cap guards a tampered storage folder
# a job's gzipped record is kilobytes, an agent's with a long conversation a few megabytes; a larger file is not sent
JOB_RECORD_LIMIT = 16 << 20
CATALOG_REF_RE = re.compile(r"^[0-9a-f]{40}$")  # a commit, never a branch
log = logging.getLogger(__name__)
UI_NAME_RE = re.compile(r"^[A-Za-z0-9_-][A-Za-z0-9._-]*$")  # a file of the page by basename; no dotfile, no "/"
UI_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript",
    ".css": "text/css",
    ".svg": "image/svg+xml",
    ".png": "image/png",
}
EMPTY_WINDOW_S = 3600  # the usage route's "last hour"
JSON_NULL: tuple[bytes, dict[str, str]] = (b"null", {"Content-Type": "application/json"})
# a dict (or None) goes out as JSON; bytes go out as-is, with the Content-Type the handler put in the headers;
# an iterator of bytes is streamed chunk by chunk until its reader leaves (`GET /stream`)
Response = tuple[int, dict | bytes | Iterator[bytes] | None, dict[str, str]]
# kind of subject -> its stage table: stage -> (status word, the actions it accepts) (spec webapp-actions §5.1)
SUBJECT_RULES = {"dataset": mia_data_config.STAGE_ACTIONS, "buffer": STAGE_ACTIONS, "prep": PREP_STAGE_ACTIONS}
# where a round can go from any step: not steps of a lifecycle themselves
OFF_LINE = ("debugging", "failed", "blocked", "needs-human", "cancelled")
# kind of subject -> its lifecycle as the page draws it: the stages of its table, in the table's order
LINES = {kind: [stage for stage in table if stage not in OFF_LINE] for kind, table in SUBJECT_RULES.items()}
NEEDS_A_PERSON = ("waiting", "stuck")  # the status words `GET /needs` lists
# Shelving (spec shelved-subjects): a person sets a subject aside with a reason. Its stage stays; its status reads
# `shelved`, it takes `unshelve` alone, and a shelved dataset reads `shelved` to every plan and request. A subject
# with one of these status words (no round in progress, none at review) also takes `shelve`.
SHELVABLE = ("not started", "stuck", "done")
HOLD_ACTIONS = ("shelve", "unshelve")  # carried out by this service: the subject's workflow hears of neither
ISSUE_URL_RE = re.compile(r"^https://github\.com/([^/]+/[^/]+)/issues/(\d+)")
RECORD_ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,120}")  # a catalog record id (mia_data.seed.ID_RE)
DROP_QUIET_S = 60  # `dropped` is refused while a file under incoming/ changed this recently
# a key sent again this soon is the same click again (a retry after a lost answer) and is not checked a second
# time; later it is a new decision about an old notice, and the stage table applies to it like to any click
RETRY_WINDOW_S = 120
# How long a sent action counts as asked while its workflow has written nothing: the page offers no button for that
# long, then the stage's own again (a worker that is away must not hold a subject's buttons for ever).
ASKED_WAIT_S = 60

SERVING = ("approved", "ready", "idle")  # latest events under which consumers draw
UNAPPROVED = ("created", "planned", "sourcing", "staged")  # a round not yet approved: only picks the 409 message


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BufferConfig(_Strict):
    spec_name: str
    spec_hash: str
    catalog_ref: str | None = None
    levels: list[int]
    patch: int
    channels: int | Literal["all"]
    seed: int = 0
    tensorizer: str = "muvit-2d"
    tensorizer_params: dict = {}
    splits: dict[str, float]
    split_sizes: dict[str, int] = {}  # frozen list lengths for val and test
    recipe: dict | None = None
    plan_hash: str | None = None
    idle_hours: float = 72
    retire_after_days: float = 14
    auto_approve: bool = False
    max_empty_fraction: float = 0.05  # spec health: the share of empty answers (204) the page flags above


class LevelBody(_Strict):
    factor: int
    shape: list[int]
    scale_nm: dict[str, float]
    translation_nm: dict[str, float]

    @model_validator(mode="after")
    def _has_y_x(self) -> LevelBody:
        for name, mapping in (("scale_nm", self.scale_nm), ("translation_nm", self.translation_nm)):
            if "y" not in mapping or "x" not in mapping:
                raise ValueError(f"level {self.factor}: {name} needs y and x")
        return self


class VolumeBody(_Strict):
    path: str
    record: str
    axes: str
    shape: list[int]
    levels: list[LevelBody]
    split_planes: dict[str, list[int]]
    labels: dict[str, str] = {}
    weight: float
    channels_stored: int
    recipe: dict | None = None
    voxel_size_nm: dict[str, float]
    normalize: dict | None = None  # {"lo": [per channel], "hi": [per channel]} for normalize.scope volume|dataset
    artifact: str | None = None  # the staged artifact's id; without one the volume references its directory
    samples: dict[str, list[dict]] = {}  # planned crops per split of a sparse (0.6) artifact; {} for a 0.5 one


class ConsumerBody(_Strict):
    name: str
    host: str
    pid: int
    rank: int = 0
    split: str
    mode: Literal["local", "remote"] = "local"


class RecordBody(_Strict):
    path: str
    record: dict  # a missing or odd id is the planner's to report, not a reason to refuse the catalog


class RecordsBody(_Strict):
    records: list[RecordBody]


class PlanPage(_Strict):
    """The page of a plan's rows the designer asks for, with the toggles that narrow them (explore.plan_view)."""

    limit: int = 50
    offset: int = 0
    sort: str | None = None
    dir: str = "asc"
    q: str = ""
    fit_only: bool = False
    prepared_only: bool = False
    raw_cached: bool = False
    staged_geom: bool = False


class PlanBody(_Strict):
    spec: str  # the spec's YAML text
    page: PlanPage | None = None  # with it: one page of joined rows and the plan's summary instead of every row


class MissingBody(_Strict):
    records: list[str] | None = None  # catalog record ids
    prep: int | None = None  # a preparation request's number
    spec: str | None = None  # a buffer spec's name


class BoardBody(_Strict):
    status: str
    reason: str = ""
    job: str | None = None


class EventBody(_Strict):
    kind: str
    issue: int | None = None
    event_id: str | None = None
    detail: str = ""


class SubjectBody(_Strict):
    kind: str
    ref: str
    source_digest: str = ""


class AdoptBody(_Strict):
    """An existing issue adopted as a subject: its key, the stage its label names, what its body carried."""

    key: str
    stage: str = "new"
    data: dict = {}


class EntryBody(_Strict):
    """One write of a round: a stage, a timeline entry (text with the marker that makes it once-only), or both."""

    body: str = ""
    marker: str = ""
    round: str | None = None
    stage: str | None = None

    @model_validator(mode="after")
    def _something_to_write(self) -> EntryBody:
        if bool(self.body) != bool(self.marker):
            raise ValueError("an entry needs both its body and its marker")
        if not self.body and self.stage is None:
            raise ValueError("nothing to write: give a stage, an entry, or both")
        return self


class MirroredBody(_Strict):
    comment_url: str | None = None


def _action_entry(action: dict) -> tuple[str, str]:
    """The timeline entry (body, marker) of an action that was carried out: who asked for what, and with what."""
    payload = action["payload"]
    text = f"**Action:** `{action['action']}` by `{action['actor']}`"
    if payload.get("plan_first"):
        text += " (plan first)"
    if payload.get("records"):
        text += ": " + ", ".join(f"`{neutralize(record)}`" for record in payload["records"])
    if payload.get("record"):
        text += f": `{neutralize(payload['record'])}`"
    if note := payload.get("note"):
        # the mirror posts this under the bot's name: it must ping nobody (no mentions; spec §2 non-goals)
        quiet = neutralize(note)[:16000].replace("@", "@\u200b")
        text += "\n\n" + "\n".join(f"> {line}" for line in quiet.splitlines())
    if spec := payload.get("spec"):  # the timeline, and so the issue, says exactly what ran
        fenced = neutralize(spec).replace("```", "` ` `")
        text += f"\n\n```yaml\n{fenced}{'' if fenced.endswith(chr(10)) else chr(10)}```"
    return text, f"<!-- mia: action {action['id']} -->"


class ActionBody(_Strict):
    """One thing a person (or a worker for a request) asks of a subject (spec webapp-actions §4.2)."""

    action: Literal[
        "go", "approve", "reject", "revoke", "keep", "retire", "dropped", "cancel", "stop", "shelve", "unshelve"
    ]
    key: str | None = None  # the sender's own name for this request: a repeat is the same action
    revision: int | None = None  # approve, reject: the subject's revision the person was looking at
    note: str = ""  # reject: the feedback for the agent; shelve: why it can wait
    root: str | None = None  # a worker's go: the root request this round is charged to
    actor: str | None = None  # a worker's name for itself (`prep:<number>`); needs the worker token
    spec: str | None = None  # a buffer's or a request's go: the YAML text that runs (default: the stored one)
    plan_first: bool = False  # a buffer's go: stop at the plan and wait for an approve before staging anything
    records: list[str] = []  # a buffer's revoke: the records that leave it
    record: str | None = None  # dropped: the dataset whose files are in place
    approve_hosts: list[str] = []  # a start action: hosts the run needs that the person ticked (spec host-approval §5)


class SpecBody(_Strict):
    """A buffer spec saved under its name, and run when `action` is `go`; or a preparation request, always run."""

    spec: str
    action: Literal["go"] | None = None
    plan_first: bool = False
    key: str | None = None
    approve_hosts: list[str] = []  # a preparation request: hosts the run needs that the person ticked


class RefusalBody(_Strict):
    state: Literal["refused"]
    detail: str


class IssueBody(_Strict):
    issue: int


class AssessmentBody(_Strict):
    """One assessment as `mia_core.judge.policy.new_assessment` shapes it (spec autonomous-gates §5)."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())  # `model` is a field, not a namespace

    id: str
    gate: str
    subject: SubjectBody
    verdict: str
    by: str
    at: str
    policy_version: int
    confidence: float | None = None
    findings: list[dict] = []
    summary: str = ""
    supersedes: str | None = None
    evidence_digest: str = ""
    checks: dict | None = None
    model: str = ""
    prompt_version: str = ""
    coverage: dict | None = None
    action: int | None = None


class ReserveBody(_Strict):
    reservation_id: str
    kind: str
    usd: float = 0.0
    caps: dict[str, float] = {}


class SettleBody(_Strict):
    reservation_id: str
    usd: float | None = None
    fetched_bytes: int = 0
    staged_bytes: int = 0


def _data(name: str) -> Any:
    """A sort key: the named field of a row's `data` (an asset without a row has none)."""
    return lambda row: (row.get("data") or {}).get(name)


# what a paged list may be sorted by: the name the page sends -> the row's value for it
ASSET_SORTS = {
    "dataset": lambda row: row.get("record"),
    "artifact": lambda row: row["key"],
    "state": _data("state"),
    "bytes": _data("bytes"),
    "last_used": _data("last_used"),
    "buffers": lambda row: len(row.get("refs") or []),
    "levels_key": _data("levels_key"),
    "job": _data("job"),
}
LOCK_SORTS = {
    name: (lambda row, name=name: row.get(name)) for name in ("kind", "key", "owner", "taken_at", "heartbeat_at")
}


def _field(name: str) -> Any:
    """A sort key: the named field of a row."""
    return lambda row: row.get(name)


SUBJECT_SORTS = {name: _field(name) for name in ("key", "kind", "stage", "status", "updated", "number")}
BUFFER_SORTS = {
    "buffer": _field("id"),
    "spec": lambda row: _spec_name(row),
    "updated": lambda row: (row.get("latest_event") or {}).get("at"),
    "volumes": _field("volumes"),
    "missing": _field("missing"),
    "consumers_seen": _field("consumers_seen"),
    "draws_today": _field("draws_today"),
}
BOARD_SORTS = {
    "dataset": _field("record"),
    "status": _field("status"),
    "job": _field("job"),
    "updated": _field("updated_at"),
}
EVENT_SORTS = {name: _field(name) for name in ("seq", "kind", "at")}
VOLUME_SORTS = {"volume": _field("id"), "draws": _field("draws"), "missing": _field("missing")}
CONSUMER_SORTS = {name: _field(name) for name in ("name", "host", "split", "rank", "last_seen")}
USAGE_SORTS = {name: _field(name) for name in ("day", "draws", "consumers", "empty")}
ROUND_ID_RE = re.compile(r"^i(\d+)-e(\d+)$")  # a buffer's id: i<its spec's number>-e<the go that started the round>


def _round_of(buffer: str) -> tuple[int, int]:
    """(the spec's number, the round's event) of a buffer id; (0, 0) for an id that is not a round's."""
    found = ROUND_ID_RE.match(buffer)
    return (int(found.group(1)), int(found.group(2))) if found else (0, 0)


def _spec_name(buffer: dict) -> str | None:
    """What a buffer's spec is called: its spec's key, else the name its config recorded."""
    return buffer.get("spec") or (buffer.get("config") or {}).get("spec_name")


def _subject_haystack(row: dict) -> tuple:
    return (row["key"], row["stage"], row["status"])


def _asset_haystack(row: dict) -> tuple:
    data = row.get("data") or {}
    return (row["key"], row.get("record"), data.get("state"), data.get("job"), data.get("levels_key"))


ROUND_REF_RE = re.compile(r"^i(\d+)-e")  # a round id starts a crop set's ref: i<subject number>-e<event>


def _assessed_record(
    subject: dict, numbers: dict[int, str], artifacts: dict[str, str], heads: dict[str, str]
) -> str | None:
    """The dataset a decision is about: a staged artifact's record, the dataset of the round a crop set came from,
    or, for a snippet PR's head, the dataset whose crops were made from that head (`heads`: head -> record)."""
    ref = str(subject.get("ref") or "")
    if subject.get("kind") == "artifact":
        return artifacts.get(ref.rsplit("/", 1)[-1])
    if subject.get("kind") == "crop_set" and (found := ROUND_REF_RE.match(ref)):
        return numbers.get(int(found.group(1)))
    if subject.get("kind") == "pr_head":
        return heads.get(head_of(subject) or "")
    return None


def _prep_records(request: dict) -> list[str]:
    """The records a preparation request's stored spec names; none when it selects by rule or does not parse."""
    try:
        return list(load_prep_text(str((request.get("data") or {}).get("spec") or "")).records)
    except SpecError:
        return []


class HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _parse[Model: _Strict](model: type[Model], body: bytes) -> Model:
    try:
        return model.model_validate_json(body or b"null")
    except ValidationError as error:
        raise HttpError(422, str(error)) from None


def _int(query: dict[str, str], key: str, default: int | None = None, *, low: int, high: int) -> int:
    value = query.get(key)
    if value is None and default is not None:
        return default
    try:
        parsed = int(value or "")
    except ValueError:
        raise HttpError(422, f"query {key} must be an integer") from None
    if not low <= parsed <= high:
        raise HttpError(422, f"query {key} must be between {low} and {high}, got {parsed}")
    return parsed


# (method, pattern, handler, capability): consumer and read routes are open, worker routes need the bearer token
ROUTES = [
    ("PUT", re.compile(r"^/buffers/([^/]+)$"), "put_buffer", "worker"),
    ("PUT", re.compile(r"^/buffers/([^/]+)/volumes/([^/]+)$"), "put_volume", "worker"),
    ("POST", re.compile(r"^/buffers/([^/]+)/consumers$"), "add_consumer", "consumer"),
    ("GET", re.compile(r"^/buffers/([^/]+)/next$"), "next", "consumer"),
    ("GET", re.compile(r"^/buffers/([^/]+)/split/([^/]+)$"), "split", "consumer"),
    ("GET", re.compile(r"^/buffers/([^/]+)/status$"), "status", "consumer"),
    ("GET", re.compile(r"^/buffers$"), "list_buffers", "read"),
    ("PUT", re.compile(r"^/buffers/([^/]+)/datasets/([^/]+)$"), "put_board", "worker"),
    ("GET", re.compile(r"^/buffers/([^/]+)/datasets$"), "board", "read"),
    ("POST", re.compile(r"^/buffers/([^/]+)/events$"), "add_event", "worker"),
    ("GET", re.compile(r"^/buffers/([^/]+)/events$"), "events", "read"),
    ("DELETE", re.compile(r"^/buffers/([^/]+)/refs$"), "retire", "worker"),
    ("PUT", re.compile(r"^/assets/([a-z]+)/(.+)$"), "put_asset", "worker"),
    ("GET", re.compile(r"^/assets/([a-z]+)/(.+)$"), "get_asset", "read"),
    ("GET", re.compile(r"^/assets/([a-z]+)$"), "list_assets", "read"),
    ("PUT", re.compile(r"^/records/([^/]+)$"), "put_records", "worker"),
    ("GET", re.compile(r"^/records$"), "records", "read"),
    ("GET", re.compile(r"^/catalogs$"), "catalogs", "read"),
    ("GET", re.compile(r"^/datasets$"), "list_datasets", "read"),
    ("GET", re.compile(r"^/catalog/datasets$"), "catalog_datasets", "read"),
    ("GET", re.compile(r"^/catalog/datasets/([^/]+)$"), "catalog_dataset", "read"),
    ("PUT", re.compile(r"^/datasets/([^/]+)$"), "put_dataset", "worker"),
    ("GET", re.compile(r"^/datasets/([^/]+)$"), "get_dataset", "read"),
    ("GET", re.compile(r"^/crops/([^/]+)/([^/]+)/(raw|overlay)\.png$"), "crop_png", "read"),
    ("GET", re.compile(r"^/gallery/i(\d+)-e(\d+)/([0-9a-f]{40})/([^/]+)/(raw|overlay)\.png$"), "gallery_png", "read"),
    ("GET", re.compile(r"^/jobs/(.+)$"), "job_record", "read"),
    ("POST", re.compile(r"^/plan$"), "plan", "read"),  # a POST only to carry the spec text; it writes nothing
    ("POST", re.compile(r"^/sync/datasets$"), "sync_datasets", "worker"),
    ("POST", re.compile(r"^/sync/assets$"), "sync_assets", "worker"),
    ("GET", re.compile(r"^/locks$"), "locks", "read"),
    ("PUT", re.compile(r"^/assessments/([^/]+)$"), "put_assessment", "worker"),
    ("GET", re.compile(r"^/assessments/([^/]+)$"), "get_assessment", "read"),
    ("GET", re.compile(r"^/assessments$"), "list_assessments", "read"),
    ("GET", re.compile(r"^/decisions$"), "decisions", "read"),
    ("POST", re.compile(r"^/budgets/([^/]+)/reserve$"), "reserve", "worker"),
    ("POST", re.compile(r"^/budgets/([^/]+)/settle$"), "settle", "worker"),
    ("GET", re.compile(r"^/budgets/([^/]+)$"), "get_budget", "read"),
    ("GET", re.compile(r"^/budgets$"), "list_budgets", "read"),
    ("DELETE", re.compile(r"^/buffers/([^/]+)/volumes/([^/]+)$"), "delete_volume", "worker"),
    ("GET", re.compile(r"^/buffers/([^/]+)/volumes$"), "volumes", "read"),
    ("GET", re.compile(r"^/buffers/([^/]+)/consumers$"), "consumers", "read"),
    ("GET", re.compile(r"^/$"), "page", "read"),
    ("GET", re.compile(r"^/ui/([^/]+)$"), "ui", "read"),
    ("GET", re.compile(r"^/meta$"), "meta", "read"),
    ("GET", re.compile(r"^/usage/([^/]+)$"), "usage", "read"),
    ("PUT", re.compile(r"^/keeper$"), "put_keeper", "worker"),
    ("GET", re.compile(r"^/keeper$"), "keeper", "read"),
    ("PUT", re.compile(r"^/subjects/([a-z]+)/(\d+)$"), "adopt_subject", "worker"),
    ("GET", re.compile(r"^/subjects/([a-z]+)$"), "list_subjects", "read"),
    ("GET", re.compile(r"^/subjects/([a-z]+)/(.+)$"), "get_subject", "read"),
    ("GET", re.compile(r"^/needs$"), "needs", "read"),
    ("GET", re.compile(r"^/lookup$"), "lookup", "read"),
    ("GET", re.compile(r"^/stream$"), "stream", "read"),
    ("POST", re.compile(r"^/timeline/([a-z]+)/(\d+)$"), "append_entry", "worker"),
    ("GET", re.compile(r"^/timeline/([a-z]+)/(\d+)$"), "timeline", "read"),
    ("PUT", re.compile(r"^/timeline/([a-z]+)/(\d+)/(\d+)$"), "mark_mirrored", "worker"),
    # the routes the page's buttons call: no token; JSON from this origin only (`_guard_action`)
    ("POST", re.compile(r"^/datasets/([^/]+)/actions$"), "dataset_action", "action"),
    ("POST", re.compile(r"^/specs$"), "put_spec", "action"),
    ("POST", re.compile(r"^/specs/([^/]+)/actions$"), "spec_action", "action"),
    ("POST", re.compile(r"^/prep$"), "file_prep", "action"),
    ("POST", re.compile(r"^/prep/(\d+)/actions$"), "prep_action", "action"),
    ("GET", re.compile(r"^/actions/(\d+)$"), "get_action", "read"),
    ("PUT", re.compile(r"^/actions/(\d+)$"), "refuse_action", "worker"),
    ("PUT", re.compile(r"^/subjects/([a-z]+)/(\d+)/issue$"), "set_subject_issue", "worker"),
    ("GET", re.compile(r"^/health$"), "health", "read"),
    ("GET", re.compile(r"^/run/([a-z]+)/(\d+)$"), "run_status", "read"),
    ("POST", re.compile(r"^/hosts/missing$"), "hosts_missing", "read"),
]


class BufferService:
    """Routes → ledger operations. One train rng per buffer, seeded from the config; frozen lists seeded per split."""

    def __init__(
        self,
        ledger: Ledger,
        stale_s: float = 300.0,
        *,
        token: str | None = None,
        storage_root: Path | None = None,
        work_root: Path | None = None,
        raw_root: Path | None = None,
        staged_root: Path | None = None,
        locks: LockDir | None = None,
        archive_dir: Path | None = None,
        meta: dict | None = None,
        ui_dir: Path | None = None,
        dispatcher: Any = None,
        hosts: HostCheck | None = None,
    ):
        self.ledger = ledger
        self.hosts = hosts  # which hosts a run needs that the csub policy lacks; None cannot say
        # the way to Temporal (`dispatch.Dispatcher`, or a test's stand-in); None reaches no workflow
        self.dispatcher = dispatcher
        self.token = token  # None disables the worker check (tests, a dev service)
        self.stale_s = stale_s
        self.storage_root = storage_root  # mia-data's published datasets; None disables the datasets sync
        self.work_root = work_root  # the rounds' job workspaces, for a round's crops before it publishes; None: none
        self.raw_root, self.staged_root = raw_root, staged_root  # the assets sync scans these; None skips one
        self.locks = locks  # the workers' lock files, shown read-only; None shows none
        self.archive_dir = archive_dir  # a retire writes the buffer's draws here; None keeps them (a dev service)
        self.meta = meta  # what the page needs from mia.yaml (page_meta); None answers nulls
        self.ui_dir = ui_dir  # the page's files; None serves no page
        self.changes = Changes()  # every write to a subject is announced here; `GET /stream` follows it
        # (kind, number) -> (the action sent to its workflow, when): there until the workflow writes anything.
        # ponytail: in memory, so a restart forgets it and the page offers the stage's buttons again for the
        # rest of the minute; a column on `actions` if that ever matters.
        self._asked: dict[tuple[str, int], tuple[dict, float]] = {}
        self._empty: dict[str, deque[float]] = {}  # per buffer, the times of the last hour's 204s
        self._rng: dict[str, random.Random] = {}
        self._lock = threading.Lock()
        self._sync_lock = threading.Lock()
        self._record_rows: tuple[str | None, list[dict]] = (None, [])  # a catalog ref's rows never change
        self._last_plan: tuple[Any, dict] = (None, {})  # the last plan built, with what it was built from

    def _authorized(self, headers: dict[str, str]) -> bool:
        if self.token is None:
            return True
        given = {key.lower(): value for key, value in headers.items()}.get("authorization", "")
        return hmac.compare_digest(given.encode(), f"Bearer {self.token}".encode())

    def handle(
        self, method: str, path: str, query: dict[str, str], body: bytes, headers: dict[str, str] | None = None
    ) -> Response:
        try:
            for route_method, pattern, name, capability in ROUTES:
                match = pattern.match(path)
                if match and route_method == method:
                    if capability == "worker" and not self._authorized(headers or {}):
                        return 401, {"error": "unauthorized"}, {}
                    segments = [unquote(group) for group in match.groups()]  # volume ids hold "/" (record/v0.zarr)
                    if capability == "action":
                        self._guard_action(headers or {})
                        return getattr(self, f"_{name}")(*segments, query=query, body=body, headers=headers or {})
                    return getattr(self, f"_{name}")(*segments, query=query, body=body)
            raise HttpError(404, f"no route {method} {path}")
        except HttpError as error:
            return error.status, {"error": str(error)}, {}
        except Exception as error:  # a bug answers 500 instead of dropping the connection
            return 500, {"error": f"{type(error).__name__}: {error}"}, {}

    def _config(self, buffer: str) -> BufferConfig:
        config = self.ledger.buffer(buffer)
        if config is None:
            raise HttpError(404, f"unknown buffer {buffer}")
        return BufferConfig.model_validate(config)

    def _serving(self, buffer: str) -> BufferConfig:
        """The config of a buffer consumers may use. An allowlist (spec autonomous-gates §3): the latest event must
        be `approved`, `ready` or `idle`. `retired` is 410; a round not yet approved, a `needs-human`, or no event
        at all (a hand registration that never authorized the buffer) is 409 with the reason."""
        config = self._config(buffer)
        kind = (self.ledger.latest_event(buffer) or {}).get("kind")
        if kind == "retired":
            raise HttpError(410, "buffer retired")
        if kind in SERVING:
            return config
        if kind is None:
            raise HttpError(409, "buffer has no approved event")
        if kind in UNAPPROVED:
            raise HttpError(409, "buffer not approved")
        raise HttpError(409, "buffer needs a human")

    def _consumer(self, buffer: str, query: dict[str, str]) -> str:
        consumer_id = query.get("consumer", "")
        if not self.ledger.touch(buffer, consumer_id):
            raise HttpError(404, f"unknown consumer {consumer_id!r} for buffer {buffer}")
        return consumer_id

    def _put_buffer(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        config = _parse(BufferConfig, body)
        self.ledger.put_buffer(buffer, config.model_dump())
        with self._lock:
            self._rng.pop(buffer, None)
        return 200, {"buffer": buffer}, {}

    def _put_volume(self, buffer: str, volume_id: str, *, query: dict[str, str], body: bytes) -> Response:
        config = self._config(buffer)
        parsed = _parse(VolumeBody, body)
        levels = tuple(
            Level(level.factor, tuple(level.shape), level.scale_nm, level.translation_nm) for level in parsed.levels
        )
        factors = [level.factor for level in levels]
        if not factors or factors[0] != 1 or not set(config.levels) <= set(factors):
            raise HttpError(422, f"pyramid levels {factors} do not cover the buffer's levels {config.levels}")
        volume = Volume(
            id=volume_id,
            path=parsed.path,
            record=parsed.record,
            axes=parsed.axes,
            shape=tuple(parsed.shape),
            levels=levels,
            split_planes={split: tuple(planes) for split, planes in parsed.split_planes.items()},
            labels=parsed.labels,
            weight=parsed.weight,
            channels_stored=parsed.channels_stored,
            recipe=parsed.recipe,
            voxel_size_nm=parsed.voxel_size_nm,
            normalize=parsed.normalize,
            artifact=parsed.artifact,
            samples={split: tuple(candidates) for split, candidates in parsed.samples.items()},
        )
        if len(volume.shape) != len(volume.axes):
            raise HttpError(422, f"shape {volume.shape} does not match axes {volume.axes!r}")
        reason = fits(volume, config.patch, config.levels)
        if reason:
            raise HttpError(422, reason)
        staged = parsed.artifact or str(PurePosixPath(parsed.path).parent)  # the artifact, else its directory
        # a rebuild under a new artifact (or new candidates) invalidates every frozen row: they index candidates
        previous = next((known for known in self.ledger.volumes(buffer) if known.id == volume_id), None)
        changed = previous is not None and (previous.artifact, previous.samples) != (volume.artifact, volume.samples)
        self.ledger.put_volume(buffer, volume, refs=[("staged", staged), ("raw", parsed.record)])
        cleared = self.ledger.clear_frozen(buffer) if changed else 0
        return 200, {"buffer": buffer, "volume": volume_id, "frozen_cleared": cleared}, {}

    # --- the registry: what exists on disk, and which buffers use it -----------------------------

    def _put_asset(self, kind: str, key: str, *, query: dict[str, str], body: bytes) -> Response:
        try:
            data = json.loads(body)
        except ValueError as error:
            raise HttpError(422, f"invalid JSON: {error}") from None
        if not isinstance(data, dict):
            raise HttpError(422, "an asset is a JSON object")
        if "state" in data and data["state"] not in ASSET_STATES:
            raise HttpError(422, f"state must be one of {list(ASSET_STATES)}, got {data['state']!r}")
        return 200, {"kind": kind, "key": key, "data": self.ledger.put_asset(kind, key, data)}, {}

    def _get_asset(self, kind: str, key: str, *, query: dict[str, str], body: bytes) -> Response:
        data, refs = self.ledger.asset(kind, key), self.ledger.refs(kind, key)
        if data is None and not refs:
            raise HttpError(404, f"no {kind} asset {key}")
        return 200, {"kind": kind, "key": key, "data": data, "refs": refs}, {}

    @staticmethod
    def _page_of(query: dict[str, str], sorts: dict[str, Any]) -> Page | None:
        """The page a list route is asked for (None: the whole list, as the workers call it); 422 on a bad one."""
        try:
            return page_query(query, sorts)
        except ValueError as error:
            raise HttpError(422, str(error)) from None

    def _list_assets(self, kind: str, *, query: dict[str, str], body: bytes) -> Response:
        page = self._page_of(query, ASSET_SORTS)
        assets = self.ledger.assets(kind, state=query.get("state"), record=query.get("record"))
        out: dict[str, Any] = {"kind": kind, "bytes": self.ledger.asset_bytes(kind)}
        if page is None:
            return 200, {**out, "assets": assets}, {}
        # the page's row names its dataset: a raw download is keyed by it, a staged artifact carries it
        issues = self._dataset_issues()
        for asset in assets:
            asset["record"] = asset["key"] if kind == "raw" else (asset["data"] or {}).get("record")
            asset["issue"] = issues.get(asset["record"])
        rows, total = paged(assets, page, ASSET_SORTS, _asset_haystack)
        return 200, {**out, "assets": rows, "total": total}, {}

    def _sync_assets(self, *, query: dict[str, str], body: bytes) -> Response:
        if self.raw_root is None and self.staged_root is None:
            raise HttpError(503, "the service has neither raw_root nor staged_root")
        if not self._sync_lock.acquire(blocking=False):
            raise HttpError(409, "a sync is running")
        try:
            started = time.time()
            known = {("raw", row["key"]): row["data"] for row in self.ledger.assets("raw") if row["data"]}
            known |= {("staged", row["key"]): row["data"] for row in self.ledger.assets("staged") if row["data"]}
            found, building, skipped, empty = scan_assets(self.raw_root, self.staged_root, known)
            kinds = tuple(kind for kind in ("raw", "staged") if kind not in skipped)
            counts = self.ledger.sync_assets(found, older_than=started, kinds=kinds)
            paths = {"building": [str(path) for path in building], "empty": [str(path) for path in empty]}
            return 200, {**counts, **paths, "skipped": skipped}, {}
        finally:
            self._sync_lock.release()

    def _locks(self, *, query: dict[str, str], body: bytes) -> Response:
        page = self._page_of(query, LOCK_SORTS)
        locks = [asdict(holder) for holder in self.locks.holders()] if self.locks else []
        if page is None:
            return 200, {"locks": locks}, {}
        rows, total = paged(locks, page, LOCK_SORTS, lambda row: (row["kind"], row["key"], row["owner"]))
        return 200, {"locks": rows, "total": total}, {}

    # --- records, datasets and the plan: what the catalog holds and which of it is prepared ------

    def _put_records(self, ref: str, *, query: dict[str, str], body: bytes) -> Response:
        if not CATALOG_REF_RE.match(ref):
            raise HttpError(422, f"ref {ref!r} is not a 40-hex commit")
        records = [(item.path, item.record) for item in _parse(RecordsBody, body).records]
        return 200, {"ref": ref, "count": self.ledger.put_records(ref, records)}, {}

    def _records(self, *, query: dict[str, str], body: bytes) -> Response:
        ref = query.get("ref") or self.ledger.latest_catalog()
        if ref is None:
            raise HttpError(404, "no catalog synced")
        if ref not in {catalog["ref"] for catalog in self.ledger.catalogs()}:
            raise HttpError(404, f"catalog ref {ref} is not synced")
        records = [{"path": path, "record": record} for path, record in self.ledger.records(ref)]
        return 200, {"ref": ref, "records": records}, {}

    def _catalogs(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, {"catalogs": self.ledger.catalogs()}, {}

    def _put_dataset(self, record: str, *, query: dict[str, str], body: bytes) -> Response:
        try:
            fields = json.loads(body)
        except ValueError as error:
            raise HttpError(422, f"invalid JSON: {error}") from None
        if not isinstance(fields, dict):
            raise HttpError(422, "a dataset is a JSON object")
        if "state" in fields and fields["state"] not in DATASET_STATES:
            raise HttpError(422, f"state must be one of {list(DATASET_STATES)}, got {fields['state']!r}")
        row = self.ledger.put_dataset(record, fields)
        if fields.get("state") == "prepared":
            self._recheck_requests(record)
        return 200, row, {}

    def _recheck_requests(self, record: str) -> None:
        """A dataset was shelved or became prepared: tell each request resting at `needs-human` that names it to
        look at its datasets again (`MiaPrep.recheck`); the request moves itself to `done` when each is prepared
        or shelved. One that selects by rule names no record here, so it is told too. Best effort: the write that
        led here stands, and the next dataset to settle tells the request again."""
        if self.dispatcher is None:
            return
        for request in self.ledger.find_subjects(kind="prep"):  # ponytail: reads every request, as `_catalog_dataset`
            if request["stage"] != "needs-human" or record not in (_prep_records(request) or [record]):
                continue
            try:
                self.dispatcher.signal_running("prep", request["number"], "recheck")
            except DispatchError as error:
                log.warning("request %s not told that %s settled: %s", request["key"], record, error)

    def _get_dataset(self, record: str, *, query: dict[str, str], body: bytes) -> Response:
        row = self.ledger.dataset(record)
        if row is None:
            raise HttpError(404, f"no dataset row for {record}")
        return 200, row, {}

    # --- datasets as the page shows them: a catalog record joined with what the registry knows ------

    def _dataset_rows(self) -> tuple[str, list[dict]]:
        """(the newest catalog ref, one row per dataset): every record of that catalog, then every dataset the
        registry knows whose record is not in it."""
        ref = self.ledger.latest_catalog()
        if ref is None:
            raise HttpError(404, "no catalog synced")
        if self._record_rows[0] != ref:  # a ref is a commit: its records are parsed once
            self._record_rows = (ref, [record_row(path, record) for path, record in self.ledger.records(ref)])
        datasets = {row["id"]: row for row in self.ledger.datasets()}
        raw = {asset["key"]: asset for asset in self.ledger.assets("raw")}
        staged: dict[str, list[dict]] = {}
        for asset in self.ledger.assets("staged"):
            staged.setdefault(str((asset["data"] or {}).get("record")), []).append(asset)
        statics = list(self._record_rows[1])
        known = {static["id"] for static in statics}
        statics += [record_row(None, {"id": record}) for record in datasets if record not in known]
        rows = [
            dataset_row(static, datasets.get(static["id"]), raw.get(static["id"]), staged.get(static["id"], []))
            for static in statics
        ]
        issues = self._dataset_issues()
        for row in rows:
            row["issue"] = issues.get(row["id"])
        return ref, rows

    def _dataset_issues(self) -> dict[str, int | None]:
        """Each dataset's GitHub issue. A dataset row's `issue` is the number of the workflow that prepares it:
        for a dataset a label started that is its issue, but for one started from the page it is the subject's
        number, and the issue is the one the mirror opened for the subject (none until it has)."""
        issues = {row["id"]: row.get("issue") for row in self.ledger.datasets()}
        return issues | {row["key"]: row["issue"] for row in self.ledger.find_subjects(kind="dataset")}

    def _request_records(self, number: str) -> list[str]:
        """The records a preparation request names. One that selects by rule names none here: the round's first
        timeline entry lists what it admitted."""
        if not number.isdigit():
            raise HttpError(422, "query request must be a request's number")
        found = self.ledger.find_subjects(kind="prep", number=int(number))
        if not found:
            raise HttpError(404, f"no preparation request {number}")
        return _prep_records(found[0])

    def _catalog_datasets(self, *, query: dict[str, str], body: bytes) -> Response:
        """The Datasets list: one page of datasets, filtered by state, modality, dims, licence or a request's
        records, with how many datasets have each filter value."""
        page = self._page_of({"limit": "50", **query}, DATASET_SORTS)
        assert page is not None
        ref, rows = self._dataset_rows()
        counts = facets(rows)
        if "request" in query:
            wanted = set(self._request_records(query["request"]))
            rows = [row for row in rows if row["id"] in wanted]
        for name in DATASET_FILTERS:
            if name in query:
                rows = [row for row in rows if row[name] == query[name]]
        found, total = paged(rows, page, DATASET_SORTS, dataset_haystack)
        return 200, {"ref": ref, "rows": found, "total": total, "facets": counts}, {}

    def _catalog_dataset(self, record: str, *, query: dict[str, str], body: bytes) -> Response:
        """Everything known about one dataset: its row, its catalog record, its preparation row, its raw download,
        its staged artifacts, the requests that name it, and its subject. The buffers whose pool holds it are
        `GET /buffers?record=`."""
        ref, rows = self._dataset_rows()
        row = next((row for row in rows if row["id"] == record), None)
        if row is None:
            raise HttpError(404, f"no dataset {record}")
        found = self.ledger.record(ref, record)
        requests = [
            {name: request[name] for name in ("key", "number", "stage", "status", "updated")}
            for request in map(self._subject_view, self.ledger.find_subjects(kind="prep"))
            if record in _prep_records(request)
        ]  # ponytail: reads every request; an index of request -> records when there are thousands
        subject = self.ledger.subject("dataset", record)
        raw = self.ledger.assets("raw", record=None)
        return (
            200,
            {
                "ref": ref,
                "row": row,
                "path": found[0] if found else None,
                "record": found[1] if found else None,
                "dataset": self.ledger.dataset(record),
                "raw_asset": next((asset for asset in raw if asset["key"] == record and asset["data"]), None),
                "staged_assets": self.ledger.assets("staged", record=record),
                "requests": requests,
                "subject": self._subject_view(subject) if subject else None,
            },
            {},
        )

    # --- subjects: a dataset, a buffer spec or a preparation request, with what its stage accepts ------

    def _rules(self, row: dict, hold: dict | None, gaps: list[str]) -> tuple[str, frozenset[str]]:
        """A subject's status word and the actions it accepts: its stage's (`SUBJECT_RULES`), without `go` for a
        dataset whose record cannot be run (`gaps`: no Start until a synced catalog holds one that can), with
        `shelve` while no round is open and none was just asked for. A shelved subject (`hold`) is `shelved` and
        takes `unshelve` alone."""
        if hold is not None:
            return "shelved", frozenset({"unshelve"})
        status, actions = SUBJECT_RULES[row["kind"]][row["stage"]]
        if gaps:
            actions = actions - {"go"}
        if status in SHELVABLE and self._asking(row["kind"], row.get("number")) is None:
            actions = actions | {"shelve"}
        return status, actions

    def _asking(self, kind: str, number: int | None) -> dict | None:
        """The action that was sent to the subject's workflow and that the workflow has not answered yet, while
        its wait lasts."""
        if number is None or (held := self._asked.get((kind, number))) is None:
            return None
        action, since = held
        if (wait_s := ASKED_WAIT_S - (time.monotonic() - since)) <= 0:
            return None
        return {"id": action["id"], "action": action["action"], "at": action["at"], "wait_s": round(wait_s, 1)}

    def _subject_view(self, row: dict, holds: dict[tuple[str, int], dict] | None = None) -> dict:
        """The row plus its status word, the actions it accepts (`_rules`; sorted, for a stable page), its hold
        when it is shelved, and `asked`: the action that was sent to its workflow and that the workflow has not
        answered yet. Until it does the stage is still the old one, and the page must not offer that stage's
        buttons as if nothing was clicked. `holds`: `Ledger.shelved()`, read once by a caller with many rows."""
        hold = (self.ledger.shelved() if holds is None else holds).get((row["kind"], row["number"]))
        gaps = self._gaps(row["key"]) if row["kind"] == "dataset" else []
        status, actions = self._rules(row, hold, gaps)
        asked = self._asking(row["kind"], row["number"])
        return {**row, "status": status, "actions": sorted(actions), "asked": asked, "gaps": gaps, "shelved": hold}

    def _gaps(self, record: str) -> list[str]:
        """What the dataset's record, as the newest synced catalog that holds it has it, lacks for a round to run
        (`record_gaps`); none for a record no synced catalog holds. Read from the catalog on every call, so a
        record fixed there can be started after the next catalog sync and nothing has to be cleared here."""
        found = self._catalog_record(record)
        return record_gaps(found) if found is not None else []

    def _adopt_subject(self, kind: str, number: str, *, query: dict[str, str], body: bytes) -> Response:
        if kind not in SUBJECT_RULES:
            raise HttpError(404, f"no subjects of kind {kind!r}")
        parsed = _parse(AdoptBody, body)
        if parsed.stage not in SUBJECT_RULES[kind]:
            raise HttpError(422, f"stage must be one of {list(SUBJECT_RULES[kind])}, got {parsed.stage!r}")
        try:
            row = self.ledger.adopt_subject(kind, int(number), parsed.key, stage=parsed.stage, data=parsed.data)
        except ValueError as error:
            raise HttpError(409, str(error)) from None
        self.changes.announce(kind, row["number"])
        return 200, self._subject_view(row), {}

    def _get_subject(self, kind: str, key: str, *, query: dict[str, str], body: bytes) -> Response:
        row = self.ledger.subject(kind, key) if kind in SUBJECT_RULES else None
        if row is None:
            raise HttpError(404, f"no {kind} {key!r}")
        return 200, self._subject_view(row), {}

    def _list_subjects(self, kind: str, *, query: dict[str, str], body: bytes) -> Response:
        if kind not in SUBJECT_RULES:
            raise HttpError(404, f"no subjects of kind {kind!r}")
        page = self._page_of(query, SUBJECT_SORTS)
        holds = self.ledger.shelved()
        subjects = [self._subject_view(row, holds) for row in self.ledger.find_subjects(kind=kind)]
        if page is None:
            return 200, {"subjects": subjects}, {}
        rows, total = paged(subjects, page, SUBJECT_SORTS, _subject_haystack)
        return 200, {"subjects": rows, "total": total}, {}

    def _needs(self, *, query: dict[str, str], body: bytes) -> Response:
        """Everything that waits on a person: the subjects waiting or stuck, a buffer round whose dataset waits
        for its files (`drops`: the round is working, but only a person can copy them), and the actions of the
        last day that did not reach Temporal (each with its subject, for the Retry). A shelved subject is not
        among them: `shelved` counts those, and `?shelved=1` lists them instead. `kind` narrows the list to
        one kind of subject; `kinds` counts every kind's share of the whole list."""
        holds = self.ledger.shelved()
        views = [self._subject_view(row, holds) for row in self.ledger.find_subjects()]
        # ponytail: reads every subject; a status column and an index when there are tens of thousands
        for index, view in enumerate(views):
            if view["kind"] == "buffer" and "dropped" in view["actions"]:
                # the round's buffer has the id of the go that started it
                gos = [a for a in self.ledger.actions("buffer", view["number"]) if a["action"] == "go"]
                board = self.ledger.buffer_datasets(f"i{view['number']}-e{gos[-1]['id']}") if gos else []
                if drops := [row["record"] for row in board if row["status"] == "waiting-drop"]:
                    views[index] = {**view, "drops": drops}
        failed = []
        for action in self.ledger.undelivered(time.time() - 86400):
            found = self.ledger.find_subjects(kind=action["kind"], number=action["number"])
            failed.append({**action, "subject": self._subject_view(found[0], holds) if found else None})
        if query.get("shelved"):
            needs = [view for view in views if view["shelved"]]
        else:
            needs = [view for view in views if view["status"] in NEEDS_A_PERSON or view.get("drops")]
        kinds = {kind: sum(1 for view in needs if view["kind"] == kind) for kind in SUBJECT_RULES}
        if (kind := query.get("kind")) is not None:
            if kind not in SUBJECT_RULES:
                raise HttpError(422, f"kind must be one of {list(SUBJECT_RULES)}, got {kind!r}")
            needs = [view for view in needs if view["kind"] == kind]
        page = self._page_of(query, SUBJECT_SORTS)
        if page is None:
            return 200, {"needs": needs, "failed": failed, "kinds": kinds, "shelved": len(holds)}, {}
        rows, total = paged(needs, page, SUBJECT_SORTS, _subject_haystack)
        return 200, {"needs": rows, "total": total, "failed": failed, "kinds": kinds, "shelved": len(holds)}, {}

    def _lookup(self, *, query: dict[str, str], body: bytes) -> Response:
        """From what a person has in hand to the subjects: an issue number (`234`, `#234`) or a subject's own
        number in every kind, an issue URL in the kinds of its repo, and always an exact key."""
        text = query.get("q", "").strip()
        if not text:
            raise HttpError(422, "q is required: an issue number, an issue URL or a subject's key")
        rows = self.ledger.find_subjects(key=text)
        url = ISSUE_URL_RE.match(text)
        if url:
            meta = self.meta or {}
            kinds = {meta.get("datasets_repo"): ("dataset",), meta.get("buffers_repo"): ("buffer", "prep")}
            rows += [
                row for row in self.ledger.find_subjects(issue=int(url[2])) if row["kind"] in kinds.get(url[1], ())
            ]
        elif text.removeprefix("#").isdecimal():
            rows += self.ledger.find_subjects(issue=int(text.removeprefix("#")))
            rows += self.ledger.find_subjects(number=int(text.removeprefix("#")))  # as a round or buffer id shows it
        unique = {(row["kind"], row["number"]): row for row in rows}  # a key that is also its issue number
        return 200, {"subjects": [self._subject_view(row) for row in unique.values()]}, {}

    def _stream(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, self.changes.stream(), {"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}

    # --- the timeline: what happened to a subject; rounds write it, the page reads it, the mirror copies it ---

    def _numbered(self, kind: str, number: str) -> dict:
        found = self.ledger.find_subjects(kind=kind, number=int(number)) if kind in SUBJECT_RULES else []
        if not found:
            raise HttpError(404, f"no {kind} #{number}")
        return found[0]

    def _changed(self, kind: str, number: int) -> None:
        """Tell the page's stream and the subject's mirror that something was written."""
        self.changes.announce(kind, number)
        if self.dispatcher is not None:
            self.dispatcher.kick_mirror(kind, number)

    def kick_unmirrored(self) -> None:
        """Kick the mirror of every subject with an entry not yet posted: run at start, so an entry appended just
        before a crash, or while Temporal was away, is not left behind."""
        if self.dispatcher is not None:
            for kind, number in self.ledger.unmirrored_subjects():
                self.dispatcher.kick_mirror(kind, number)

    def _append_entry(self, kind: str, number: str, *, query: dict[str, str], body: bytes) -> Response:
        self._numbered(kind, number)
        parsed = _parse(EntryBody, body)
        if parsed.stage is not None and parsed.stage not in SUBJECT_RULES[kind]:
            raise HttpError(422, f"stage must be one of {list(SUBJECT_RULES[kind])}, got {parsed.stage!r}")
        written = self.ledger.append_entry(
            kind, int(number), body=parsed.body, marker=parsed.marker, round_id=parsed.round, stage=parsed.stage
        )
        assert written is not None  # the subject was just found, and nothing deletes one
        self._asked.pop((kind, int(number)), None)  # the workflow wrote: whatever was asked of it is answered
        self._changed(kind, int(number))
        return 200, {"subject": self._subject_view(written[0]), "entry": written[1]}, {}

    def _timeline(self, kind: str, number: str, *, query: dict[str, str], body: bytes) -> Response:
        subject = self._numbered(kind, number)
        entries, actions = self.ledger.timeline(kind, int(number)), self.ledger.actions(kind, int(number))
        return 200, {"subject": self._subject_view(subject), "entries": entries, "actions": actions}, {}

    # --- actions: a click on the page, recorded and sent to its workflow in one request (spec §5, §10) ---

    @staticmethod
    def _guard_action(headers: dict[str, str]) -> None:
        """Action routes take no token, so they take JSON only and no other site's request: without this a web
        page open in a browser on the same network could post a form here. This is not identity."""
        given = {key.lower(): value for key, value in headers.items()}
        if given.get("content-type", "").split(";")[0].strip().lower() != "application/json":
            raise HttpError(415, "an action is a JSON body (Content-Type: application/json)")
        origin = given.get("origin")
        if origin and urlsplit(origin).netloc != given.get("host"):
            raise HttpError(403, "an action must come from this service's own page")

    def _actor(self, headers: dict[str, str], parsed: ActionBody) -> str:
        """Who asks (spec §10): `page` for anyone, since there is no sign-in yet; a worker, which proves itself
        with the token, may name itself and the root request its round is charged to. Sign-in changes this
        function and nothing else."""
        if parsed.actor is None and parsed.root is None:
            return "page"
        if self.token is not None and not self._authorized(headers):
            raise HttpError(401, "naming an actor or a root needs the worker token")
        return parsed.actor or "page"

    def _catalog_record(self, record: str) -> dict | None:
        """The record with this id in the newest synced catalog that holds it, or None."""
        for catalog in self.ledger.catalogs():
            if found := self.ledger.record(catalog["ref"], record):
                return found[1]
        return None

    def _catalog_records(self, records: list[str]) -> dict[str, dict]:
        """Record id -> its synced catalog entry, for those of the records a synced catalog holds."""
        return {record: row for record in records if (row := self._catalog_record(record)) is not None}

    def _needed(
        self,
        *,
        records: list[str] | None = None,
        prep: int | None = None,
        spec: str | None = None,
        request: list[str] | None = None,
    ) -> Needed:
        """The hosts the named catalog records, a request's records (`request`, or the request numbered `prep`)
        or a buffer's planned pool need (host -> the record ids that need it). A named record no synced catalog
        holds is a 404; a request's contributes nothing (the worker refuses it). A buffer's host is one its
        published binding names that its record's synced catalog entry also names: a binding lies under a root a
        job can mount read-write."""
        if self.hosts is None:
            raise HttpError(503, "this service has no csub policy to check hosts against")
        if prep is not None:
            request = self._request_records(str(prep))
        if request is not None:
            return self.hosts.for_records(self._catalog_records(request))
        if records is not None:
            if missing := [record for record in records if self._catalog_record(record) is None]:
                raise HttpError(404, f"{missing[0]!r} is not a record of a synced catalog")
            return self.hosts.for_records(self._catalog_records(records))
        if spec is None:
            raise HttpError(422, "give records, prep or spec")
        subject = self.ledger.subject("buffer", spec)
        if subject is None:
            raise HttpError(404, f"no buffer spec {spec!r}")
        rounds = [
            entry["round"]
            for entry in self.ledger.timeline("buffer", subject["number"])
            if entry["stage"] == "plan-review" and entry["round"]
        ]
        try:
            if not rounds:
                raise ValueError("no round at plan-review")
            pool = self.hosts.pool(rounds[-1])
        except (OSError, ValueError):
            raise HttpError(409, f"buffer {spec!r} has no plan to read") from None
        folders = {record: (self.ledger.dataset(record) or {}).get("folder") for record in pool}
        bound = self.hosts.for_bindings({record: folder for record, folder in folders.items() if folder})
        named = self.hosts.for_records(self._catalog_records(pool))
        kept = {host: [record for record in ids if record in named.get(host, [])] for host, ids in bound.items()}
        return {host: ids for host, ids in kept.items() if ids}

    def _hosts_missing(self, *, query: dict[str, str], body: bytes) -> Response:
        """The hosts a run needs that the csub policy lacks. A read route: it writes nothing."""
        parsed = _parse(MissingBody, body)
        given = {name: value for name, value in parsed if value is not None}
        if len(given) != 1:
            raise HttpError(422, "give exactly one of records, prep or spec")
        needed = self._needed(**given)
        assert self.hosts is not None  # _needed answered 503 otherwise
        try:
            absent = self.hosts.missing(needed)
        except (OSError, ValueError):  # a TOMLDecodeError is a ValueError
            raise HttpError(503, "the generated csub policy cannot be read: `mia up` regenerates it") from None
        return 200, {"hosts": [{"host": host, "records": ids} for host, ids in sorted(absent.items())]}, {}

    def _dataset_action(self, record: str, *, query: dict[str, str], body: bytes, headers: dict[str, str]) -> Response:
        parsed = _parse(ActionBody, body)
        actor = self._actor(headers, parsed)
        if parsed.action == "go" and (gaps := self._gaps(record)):  # before a subject is created for it
            raise HttpError(409, f"the record cannot be run: {'; '.join(gaps)}")
        subject = self.ledger.subject("dataset", record)
        if subject is None:
            if parsed.action != "go":
                raise HttpError(404, f"no dataset {record!r} has been started")
            if self._catalog_record(record) is None:
                raise HttpError(404, f"{record!r} is not a record of a synced catalog")
            self._check_hosts(parsed, partial(self._needed, records=[record]), "dataset", None)
            # a dataset whose registry row already names an issue keeps that number: its
            # workflow id, rounds and budget root are built from it
            known = (self.ledger.dataset(record) or {}).get("issue")
            subject = self.ledger.create_subject("dataset", record, number=known if isinstance(known, int) else None)
        # a subject no workflow has written (revision 0) is at the stage its dataset's row holds: a round that
        # runs without a subject writes its stage there alone, and it takes what that stage takes
        stage = (self.ledger.dataset(record) or {}).get("data_stage")
        if subject["revision"] == 0 and stage in SUBJECT_RULES["dataset"] and stage != subject["stage"]:
            subject = self.ledger.adopt_subject("dataset", subject["number"], record, stage=stage, data=subject["data"])
            self.changes.announce("dataset", subject["number"])
        payload = {name: value for name, value in (("note", parsed.note), ("root", parsed.root)) if value}
        needed = (partial(self._needed, records=[record])) if parsed.action == "go" else None
        return self._act_on(subject, parsed, actor, payload, needed)

    # --- buffer specs and preparation requests: created and run from the page (spec §4.4, §8.1) ---

    @staticmethod
    def _spec_name(text: str) -> str:
        try:
            return load_spec_text(text).name
        except SpecError as error:
            raise HttpError(422, f"not a buffer spec: {error}") from None

    def _save_spec(self, text: str) -> dict:
        """The buffer subject named by the spec's `name`, holding this text: created, or its text replaced. A
        spec posted under a name that exists is an edit of that subject, never a second one."""
        subject = self.ledger.create_subject("buffer", self._spec_name(text), data={"spec": text})
        if subject["data"].get("spec") != text:
            edited = self.ledger.set_subject_data("buffer", subject["number"], {**subject["data"], "spec": text})
            assert edited is not None  # it was just read
            subject = edited
        self.changes.announce("buffer", subject["number"])
        return subject

    def _put_spec(self, *, query: dict[str, str], body: bytes, headers: dict[str, str]) -> Response:
        parsed = _parse(SpecBody, body)
        if parsed.action is None:
            if parsed.approve_hosts:  # spec host-approval §5: only the four start actions carry hosts
                raise HttpError(422, "approve_hosts goes with an action that starts jobs")
            return 200, {"subject": self._subject_view(self._save_spec(parsed.spec))}, {}
        name = self._spec_name(parsed.spec)
        go = ActionBody(
            action="go",
            key=parsed.key,
            spec=parsed.spec,
            plan_first=parsed.plan_first,
            approve_hosts=parsed.approve_hosts,
        )
        return self._buffer_action(name, go, headers, create=True)

    def _spec_action(self, name: str, *, query: dict[str, str], body: bytes, headers: dict[str, str]) -> Response:
        return self._buffer_action(name, _parse(ActionBody, body), headers)

    def _buffer_action(self, name: str, parsed: ActionBody, headers: dict[str, str], create: bool = False) -> Response:
        actor = self._actor(headers, parsed)
        subject = self.ledger.subject("buffer", name)
        if subject is None and not create:
            raise HttpError(404, f"no buffer spec {name!r}")
        payload: dict = {}
        if parsed.action == "go":
            text = parsed.spec if parsed.spec is not None else (subject or {}).get("data", {}).get("spec")
            if not text:
                raise HttpError(422, f"buffer spec {name!r} has no spec text to run: send one")
            if self._spec_name(text) != name:
                raise HttpError(
                    422, f"the spec's name is not {name!r}: a spec's name is its subject, post it to /specs"
                )
            self._accepts(subject or {"kind": "buffer", "key": name, "stage": "new", "revision": 0}, parsed)
            self._check_hosts(parsed, None, "buffer", subject["number"] if subject else None)  # a go: no `needed`
            subject = self._save_spec(text)  # only once the stage takes a go: what is stored is what ran last
            payload = {"spec": text, **({"plan_first": True} if parsed.plan_first else {})}
        assert subject is not None
        needed = None
        if parsed.action == "approve" and subject["stage"] == "plan-review":
            needed = partial(self._needed, spec=name)
        if parsed.action == "revoke":
            if not parsed.records:
                raise HttpError(422, "a revoke names the records that leave the buffer: `records`")
            payload = {"records": parsed.records}
        if parsed.action == "dropped":
            payload = {"record": self._dropped_record(subject, parsed)}
        return self._act_on(subject, parsed, actor, payload, needed)

    def _dropped_record(self, subject: dict, parsed: ActionBody) -> str:
        """The record of a `dropped`, once its files look complete. The workflow takes any non-empty file and
        records its hash, so a click during a copy would record a truncated file as good: refuse while anything
        under `incoming/` changed in the last minute (the poller waited a whole interval for the same reason)."""
        record = parsed.record or ""
        if not RECORD_ID_RE.fullmatch(record):
            raise HttpError(422, "`dropped` names the record whose files are in place: `record`")
        self._accepts(subject, parsed)
        incoming = self.raw_root / record / "incoming" if self.raw_root else None
        files = [path for path in incoming.rglob("*") if path.is_file()] if incoming and incoming.is_dir() else []
        if not files:
            raise HttpError(409, f"no file under {incoming}: copy the dataset's files there first")
        newest = max(path.stat().st_mtime for path in files)
        if time.time() - newest < DROP_QUIET_S:
            raise HttpError(409, f"a file under {incoming} is still being written; try again in a minute")
        return record

    def _file_prep(self, *, query: dict[str, str], body: bytes, headers: dict[str, str]) -> Response:
        """File a preparation request and run it: a new subject `prep-<number>` holding the request's text."""
        parsed = _parse(SpecBody, body)
        try:
            records = load_prep_text(parsed.spec).records
        except SpecError as error:
            raise HttpError(422, f"not a preparation request: {error}") from None
        go = ActionBody(action="go", key=parsed.key, spec=parsed.spec, approve_hosts=parsed.approve_hosts)
        needed = partial(self._needed, request=records)
        repeat = self.ledger.action_by_key(parsed.key) if parsed.key else None
        if repeat is not None and repeat["kind"] == "prep":  # a double click: the request filed by the first one
            subject = self.ledger.find_subjects(kind="prep", number=repeat["number"])[0]
        else:
            self._check_hosts(go, needed, "prep", None)
            subject = self.ledger.create_subject("prep", None, data={"spec": parsed.spec})
        return self._act_on(subject, go, self._actor(headers, go), {"spec": parsed.spec}, needed)

    def _prep_action(self, number: str, *, query: dict[str, str], body: bytes, headers: dict[str, str]) -> Response:
        parsed = _parse(ActionBody, body)
        subject = self._numbered("prep", number)
        payload: dict = {}
        needed = None
        if parsed.action == "go":
            text = parsed.spec if parsed.spec is not None else subject["data"].get("spec")
            if not text:
                raise HttpError(422, f"request {subject['key']} has no text to run: send one as `spec`")
            try:
                records = load_prep_text(text).records
            except SpecError as error:
                raise HttpError(422, f"not a preparation request: {error}") from None
            payload = {"spec": text}
            needed = partial(self._needed, request=records)
        return self._act_on(subject, parsed, self._actor(headers, parsed), payload, needed)

    def _stop(self, subject: dict, action: dict, stopped: bool) -> None:
        """The force stop, for a run that does not answer a cancel (one frozen by nondeterminism), once its
        workflow is terminated (`stopped`: one was running): retire the subject's buffers with their `retired`
        events and set the stage a closed issue got (`retired`, or `done` for a request). The entry says what
        was done and what is left running."""
        kind, number = subject["kind"], subject["number"]
        retired = []
        if kind == "buffer":
            for buffer in self.ledger.buffers():
                mine = re.fullmatch(rf"i{number}-e\d+", buffer["id"]) is not None
                if mine and (buffer.get("latest_event") or {}).get("kind") != "retired":
                    self.ledger.retire(buffer["id"], self.archive_dir)
                    self.ledger.add_event(buffer["id"], "retired", issue=number, event_id=None, detail="stopped")
                    retired.append(buffer["id"])
        what = "workflow stopped" if stopped else "no workflow was running"
        if retired:
            what += "; retired " + ", ".join(f"`{buffer}`" for buffer in retired)
        left = "A cluster job that was already running is not killed."
        if kind == "prep":
            left += " The dataset rounds this request started keep running: cancel them on their own pages."
        marker = f"<!-- mia: stopped {action['id']} -->"
        self.ledger.append_entry(
            kind,
            number,
            body=f"stopped: {what}. {left}\n\n{marker}",
            marker=marker,
            stage="retired" if kind == "buffer" else "done",
        )

    def _accepts(self, subject: dict, parsed: ActionBody) -> None:
        """Steps 1 and 2 of spec §5: the subject's stage accepts the action (409 with what it does take), and a
        verdict names the revision that was looked at (409 when the subject has moved on). A key seen before is
        the same action: one already sent, or sent again within RETRY_WINDOW_S of the click, is not checked
        (the first try may have arrived and moved the stage; the workflow drops an event id it has handled).
        An older undelivered one is checked like a new click, so a stale Retry cannot pass the stage table."""
        if parsed.key and (seen := self.ledger.action_by_key(parsed.key)) is not None:
            age = time.time() - datetime.fromisoformat(seen["at"]).timestamp()
            if seen["state"] == "sent" or age < RETRY_WINDOW_S:
                return
        kind, key, stage = subject["kind"], subject["key"], subject["stage"]
        number = subject.get("number")  # none yet for a buffer spec that its first go creates
        hold = self.ledger.shelved().get((kind, number)) if number is not None else None
        status, accepted = self._rules(subject, hold, self._gaps(key) if kind == "dataset" else [])
        if parsed.action not in accepted:
            takes = ", ".join(f"`{name}`" for name in sorted(accepted)) or "nothing"
            raise HttpError(409, f"{kind} {key!r} is at `{stage}` ({status}): it takes {takes}, not `{parsed.action}`")
        if parsed.action in ("approve", "reject"):
            if parsed.revision is None:
                raise HttpError(422, f"`{parsed.action}` must carry the revision of what was looked at")
            if parsed.revision != subject["revision"]:
                raise HttpError(409, f"{kind} {key!r} changed since it was shown (reload it): nothing was sent")

    def _check_hosts(
        self, parsed: ActionBody, needed: Callable[[], Needed] | None, kind: str, number: int | None
    ) -> bool:
        """Refuse approve_hosts that cannot be approved here: the action starts no job (422), the service has no
        policy (503), or a host is not one the run needs (422); a key used for another action is a 409. The key
        comes first: one that carried this same action is a repeat, whatever the stage is by now.
        Nothing is written. False when there is nothing to approve: no hosts, or a key that already carried this
        action (the first try checked and approved them). `number` is None while the subject is not created."""
        if not parsed.approve_hosts:
            return False
        if parsed.key and (seen := self.ledger.action_by_key(parsed.key)) is not None:
            if (seen["kind"], seen["number"], seen["action"]) != (kind, number, parsed.action):
                raise HttpError(409, f"the key {parsed.key!r} was used for another action")  # as Ledger.add_action
            return False
        if needed is None:
            raise HttpError(422, "approve_hosts goes with an action that starts jobs")
        if self.hosts is None:
            raise HttpError(503, "this service has no csub policy to approve hosts in")
        try:
            self.hosts.check(parsed.approve_hosts, needed())
        except ValueError as error:
            raise HttpError(422, str(error)) from None
        return True

    def _act_on(
        self,
        subject: dict,
        parsed: ActionBody,
        actor: str,
        payload: dict,
        needed: Callable[[], Needed] | None = None,
    ) -> Response:
        """Steps 1 to 6 of spec §5 for one subject: check the stage and the revision (`_accepts`), record the
        action, send it to Temporal, and settle it with its timeline entry. A key seen before is the same action
        sent again under its id, whatever the stage is by now. `approve_hosts` (only with `needed`, the hosts this
        run needs) is checked, approved and written to the timeline after every other refusal and before the
        action is recorded."""
        kind, number = subject["kind"], subject["number"]
        self._accepts(subject, parsed)
        if parsed.action in HOLD_ACTIONS:
            return self._hold(subject, parsed, actor)
        if self.dispatcher is None:
            raise HttpError(503, "this service has no way to Temporal: no workflow can be started from it")
        if self._check_hosts(parsed, needed, kind, number):
            assert self.hosts is not None  # _check_hosts answered 503 otherwise
            try:
                added = self.hosts.approve(parsed.approve_hosts, by=actor, kind=kind, number=number)
            except PolicyError as error:
                raise HttpError(503, str(error)) from None
            if added:
                names = ", ".join(f"`{neutralize(host)}`" for host in added)
                self.ledger.append_entry(
                    kind,
                    number,
                    body=f"**Hosts approved:** {names} by `{neutralize(actor)}`",
                    marker=f"<!-- mia: hosts {time.time_ns()} -->",
                )
        try:
            action, _ = self.ledger.add_action(kind, number, parsed.action, actor, key=parsed.key, payload=payload)
        except ValueError as error:
            raise HttpError(409, str(error)) from None
        if action["state"] == "sent":
            return 202, action, {}
        payload = action["payload"]
        # the workflow's signal: a buffer's `plan` is a go that waits at the plan too (its event's kind says so)
        signal = "plan" if payload.get("plan_first") else parsed.action
        event = LabelEvent(signal, str(action["id"]), action["actor"], action["at"])
        running = True
        self._asked[(kind, number)] = (action, time.monotonic())  # before the send: the answer may beat this request
        try:
            if parsed.action == "cancel":  # only a running round can be cancelled: nothing is started for it
                running = self.dispatcher.signal_running(kind, number, "cancel", event)
            elif parsed.action == "dropped":  # the workflow's plain `dropped(record)`, as the poller sent it
                if not self.dispatcher.signal_running(kind, number, "dropped", payload["record"]):
                    raise DispatchError("no workflow is running for it")
            elif parsed.action == "stop":  # a terminate needs no replay: it also ends a run nondeterminism froze
                reason = f"stopped from the page (action {action['id']})"
                running = self.dispatcher.terminate(kind, number, reason)
            else:
                # a dataset's go is its workflow's `restart`: the round starts in a new run, on today's code
                self.dispatcher.send(kind, number, "restart" if (kind, signal) == ("dataset", "go") else signal, event)
        except DispatchError as error:
            self._asked.pop((kind, number), None)  # nothing was asked of any workflow
            failed = self.ledger.settle_action(action["id"], "failed", str(error))
            self.changes.announce(kind, number)
            return 503, {"error": f"not delivered: {error}", "action": failed}, {}
        sent = self.ledger.settle_action(action["id"], "sent", entry=_action_entry(action))
        if parsed.action == "stop" or not running:
            self._asked.pop((kind, number), None)  # the service answers these itself, just below
        if parsed.action == "stop":
            self._stop(subject, action, running)
        elif not running:
            # the registry says a round is in progress and Temporal has no run for it (terminated by hand, or
            # failed): the subject must not read "working" for ever, so the route ends the round itself
            marker = f"<!-- mia: cancelled {action['id']} -->"
            said = f"cancelled: no workflow was running for this {kind}\n\n{marker}"
            self.ledger.append_entry(kind, number, body=said, marker=marker, stage="cancelled")
        self._changed(kind, number)
        return 202, sent, {}

    def _hold(self, subject: dict, parsed: ActionBody, actor: str) -> Response:
        """A `shelve` (its `note` says why) or an `unshelve`: recorded like any action and carried out here, in
        the transaction that puts it on the timeline. A key seen before is the same action again."""
        kind, number = subject["kind"], subject["number"]
        reason = parsed.note.strip()
        if parsed.action == "shelve" and not reason:
            raise HttpError(422, "a shelve says why the subject can wait: `note`")
        self._check_hosts(parsed, None, kind, number)  # 422: nothing here starts a job
        payload = {"note": reason} if parsed.action == "shelve" else {}
        try:
            action, _ = self.ledger.add_action(kind, number, parsed.action, actor, key=parsed.key, payload=payload)
        except ValueError as error:
            raise HttpError(409, str(error)) from None
        if action["state"] != "sent":
            held = action["payload"].get("note") if parsed.action == "shelve" else None
            action = self.ledger.settle_hold(action["id"], held, _action_entry(action)) or action
            self._changed(kind, number)
            if (kind, parsed.action) == ("dataset", "shelve"):
                self._recheck_requests(subject["key"])
        return 202, action, {}

    def _get_action(self, action_id: str, *, query: dict[str, str], body: bytes) -> Response:
        action = self.ledger.action(int(action_id))
        if action is None:
            raise HttpError(404, f"no action {action_id}")
        return 200, action, {}

    def _refuse_action(self, action_id: str, *, query: dict[str, str], body: bytes) -> Response:
        """A workflow would not act on an action that reached it (a click that crossed a stage change)."""
        parsed = _parse(RefusalBody, body)
        action = self.ledger.settle_action(int(action_id), parsed.state, parsed.detail)
        if action is None:
            raise HttpError(404, f"no action {action_id}")
        self._asked.pop((action["kind"], action["number"]), None)
        self.changes.announce(action["kind"], action["number"])
        return 200, action, {}

    def _set_subject_issue(self, kind: str, number: str, *, query: dict[str, str], body: bytes) -> Response:
        subject = self.ledger.set_subject_issue(kind, int(number), _parse(IssueBody, body).issue)
        if subject is None:
            raise HttpError(404, f"no {kind} #{number}")
        self.changes.announce(kind, int(number))
        return 200, self._subject_view(subject), {}

    def _health(self, *, query: dict[str, str], body: bytes) -> Response:
        """Whether Temporal answers and whether a worker polls each task queue: the page's banner (spec §8.3)."""
        if self.dispatcher is None:
            return 200, {"temporal": False, "queues": {}}, {}
        return 200, self.dispatcher.health(), {}

    def _run_status(self, kind: str, number: str, *, query: dict[str, str], body: bytes) -> Response:
        self._numbered(kind, number)
        if self.dispatcher is None:
            raise HttpError(503, "this service has no way to Temporal")
        try:
            return 200, {"status": self.dispatcher.run_status(kind, int(number))}, {}
        except DispatchError as error:
            raise HttpError(503, str(error)) from None

    def _mark_mirrored(self, kind: str, number: str, seq: str, *, query: dict[str, str], body: bytes) -> Response:
        parsed = _parse(MirroredBody, body)
        if not self.ledger.mark_mirrored(kind, int(number), int(seq), parsed.comment_url):
            raise HttpError(404, f"no entry {seq} of {kind} #{number}")
        self.changes.announce(kind, int(number))
        return 200, {"ok": True}, {}

    # --- assessments and budgets: what the gates decided and what each root request has spent ------

    def _put_assessment(self, assessment_id: str, *, query: dict[str, str], body: bytes) -> Response:
        parsed = _parse(AssessmentBody, body)
        if parsed.id != assessment_id:
            raise HttpError(422, f"the body's id {parsed.id!r} is not the path's {assessment_id!r}")
        for value, allowed, what in (
            (parsed.gate, GATES, "gate"),
            (parsed.subject.kind, SUBJECT_KINDS, "subject kind"),
            (parsed.verdict, VERDICTS, "verdict"),
        ):
            if value not in allowed:
                raise HttpError(422, f"{what} must be one of {list(allowed)}, got {value!r}")
        return 200, self.ledger.put_assessment(parsed.model_dump()), {}

    def _get_assessment(self, assessment_id: str, *, query: dict[str, str], body: bytes) -> Response:
        row = self.ledger.assessment(assessment_id)
        if row is None:
            raise HttpError(404, f"no assessment {assessment_id}")
        return 200, row, {}

    def _list_assessments(self, *, query: dict[str, str], body: bytes) -> Response:
        rows = self.ledger.assessments(
            subject_kind=query.get("subject_kind"),
            subject_ref=query.get("subject_ref"),
            gate=query.get("gate"),
            by=query.get("by"),
            before=query.get("before"),
            limit=_int(query, "limit", 200, low=1, high=1000),
        )
        return 200, {"assessments": rows}, {}

    def _decisions(self, *, query: dict[str, str], body: bytes) -> Response:
        """The page's Decisions view (spec autonomous-gates §10): the newest assessments and, per gate, how the
        judge and the humans agree."""
        gate = query.get("gate")
        if gate is not None and gate not in GATES:
            raise HttpError(422, f"gate must be one of {list(GATES)}, got {gate!r}")
        page = self._page_of({"limit": "50", **query}, {})
        assert page is not None
        # ponytail: reads every assessment of the gate to name its dataset; a record column when there are 100k
        rows = self.ledger.assessments(gate=gate, limit=100_000)
        numbers = {row["issue"]: row["id"] for row in self.ledger.datasets() if row.get("issue")}
        numbers |= {row["number"]: row["key"] for row in self.ledger.find_subjects(kind="dataset")}
        artifacts = {
            asset["key"].rsplit("/", 1)[-1]: asset["data"].get("record")
            for asset in self.ledger.assets("staged")
            if asset["data"]
        }
        heads: dict[str, str] = {}  # a snippet PR head -> the dataset whose crop set was made from it
        for row in rows if gate is None else self.ledger.assessments(limit=100_000):
            subject = row.get("subject") or {}
            if subject.get("kind") == "crop_set" and (head := head_of(subject)):
                if record := _assessed_record(subject, numbers, artifacts, {}):
                    heads.setdefault(head, record)
        approved = {row["assessment"]: row["id"] for row in self.ledger.datasets() if row.get("assessment")}
        rejected: set[tuple] = set()  # subjects with a reject among the rows seen so far, which are the newer ones
        for row in rows:
            subject = row.get("subject") or {}
            # an approve names its dataset on the dataset's row, whatever it is recorded on
            if record := _assessed_record(subject, numbers, artifacts, heads) or approved.get(row.get("id")):
                row["record"] = record
            about = (subject.get("kind"), subject.get("ref"))
            if row.get("verdict") == "approve" and about in rejected:
                row["overruled"] = True  # a later reject (a revocation, or a person disagreeing) stands instead
            elif row.get("verdict") == "reject":
                rejected.add(about)
        if "record" in query:
            rows = [row for row in rows if row.get("record") == query["record"]]
        found, total = paged(rows, page, {})
        for row in found:  # what a person wrote with their verdict is on the action the assessment names
            action = self.ledger.action(row["action"]) if row.get("action") is not None else None
            if action and (note := action["payload"].get("note")):
                row["note"] = note
        gates = [gate] if gate else list(GATES)
        agreement = {name: self.ledger.agreement(name) for name in gates}
        return 200, {"assessments": found, "total": total, "agreement": agreement}, {}

    def _reserve(self, root: str, *, query: dict[str, str], body: bytes) -> Response:
        parsed = _parse(ReserveBody, body)
        return 200, self.ledger.reserve(root, parsed.reservation_id, parsed.kind, parsed.usd, parsed.caps), {}

    def _settle(self, root: str, *, query: dict[str, str], body: bytes) -> Response:
        parsed = _parse(SettleBody, body)
        row = self.ledger.settle(
            root,
            parsed.reservation_id,
            usd=parsed.usd,
            fetched_bytes=parsed.fetched_bytes,
            staged_bytes=parsed.staged_bytes,
        )
        return 200, row, {}

    def _get_budget(self, root: str, *, query: dict[str, str], body: bytes) -> Response:
        row = self.ledger.budget(root)
        if row is None:
            raise HttpError(404, f"no budget for {root}")
        return 200, row, {}

    def _list_budgets(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, {"budgets": self.ledger.budgets()}, {}

    def _delete_volume(self, buffer: str, volume_id: str, *, query: dict[str, str], body: bytes) -> Response:
        self._config(buffer)
        if not self.ledger.delete_volume(buffer, volume_id):
            raise HttpError(404, f"no volume {volume_id} in {buffer}")
        return 200, {"deleted": volume_id}, {}

    def _volumes(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        self._config(buffer)
        page = self._page_of(query, VOLUME_SORTS)
        volumes = self.ledger.volume_rows(buffer)
        if page is None:
            return 200, {"volumes": volumes}, {}
        draws = self.ledger.draws_per_volume(buffer)
        for volume in volumes:
            volume["draws"] = draws.get(volume["id"], 0)
        rows, total = paged(volumes, page, VOLUME_SORTS, lambda row: (row["id"], row["artifact"]))
        return 200, {"volumes": rows, "total": total}, {}

    def _consumers(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        """The training jobs registered with the buffer: one page of them."""
        self._config(buffer)
        page = self._page_of({"limit": "50", **query}, CONSUMER_SORTS)
        assert page is not None
        consumers = self.ledger.consumers(buffer, self.stale_s)
        rows, total = paged(consumers, page, CONSUMER_SORTS, lambda row: (row.get("name"), row.get("host")))
        return 200, {"consumers": rows, "total": total}, {}

    def _crop_png(self, folder: str, crop_id: str, name: str, *, query: dict[str, str], body: bytes) -> Response:
        """A published crop's raw.png or overlay.png from storage_root: the page's previews."""
        if self.storage_root is None:
            raise HttpError(503, "the service has no storage_root")
        if not (UI_NAME_RE.match(folder) and UI_NAME_RE.match(crop_id)):
            raise HttpError(404, "no such crop")
        data = read_untrusted(self.storage_root, f"{folder}/{crop_id}/{name}.png", CROP_PNG_LIMIT, binary=True)
        if data is None:
            raise HttpError(404, "no such crop")
        return 200, data, {"Content-Type": "image/png", "Cache-Control": "public, max-age=3600"}

    def _gallery_png(
        self, issue: str, event: str, sha: str, crop_id: str, name: str, *, query: dict[str, str], body: bytes
    ) -> Response:
        """A crop of a dataset round that has published nothing yet (one at review), from the round's workspace
        under work_root: raw.png as its crop job at snippets commit `sha` wrote it, overlay.png as its verify job
        drew it, each from the newest attempt that has the file. The page shows them in the round's timeline."""
        if self.work_root is None:
            raise HttpError(503, "the service has no work_root")
        if not UI_NAME_RE.match(crop_id):
            raise HttpError(404, "no such crop")
        round_rel = f"issue-{int(issue)}/i{issue}-e{event}"
        job, rel = (
            ("crop", f"crops/{crop_id}/raw.png") if name == "raw" else ("verify", f"preview/{crop_id}/overlay.png")
        )
        attempts = sorted(
            (self.work_root / round_rel).glob(f"{job}-{sha}-*"),
            key=lambda path: (len(path.name), path.name),
            reverse=True,
        )
        for workspace in attempts:  # job output is untrusted: no symlink, nothing outside its workspace, a size cap
            data = None if workspace.is_symlink() else read_untrusted(workspace, rel, CROP_PNG_LIMIT, binary=True)
            if data is not None:
                return 200, data, {"Content-Type": "image/png", "Cache-Control": "public, max-age=3600"}
        raise HttpError(404, "no such crop")

    def _job_record(self, where: str, *, query: dict[str, str], body: bytes) -> Response:
        """A job's record (`mia_core.jobrecord`) as its file is on disk, gzip passed through: `where` is the record's
        address, the job's workspace relative to work_root. Every segment of it is a plain name (none empty, none
        starting with a dot, so no `.` or `..`), and the file is a regular file inside the records directory:
        a symlink is not followed to one, nor out of the directory. Nothing here writes or deletes a record."""
        if self.work_root is None:
            raise HttpError(503, "the service has no work_root")
        if not all(UI_NAME_RE.fullmatch(segment) for segment in where.split("/")):
            raise HttpError(404, "no record for this job")
        data = read_untrusted(self.work_root / RECORDS_DIR, f"{where}.json.gz", JOB_RECORD_LIMIT + 1, binary=True)
        if data is None:
            raise HttpError(404, "no record for this job")
        if len(data) > JOB_RECORD_LIMIT:
            # ponytail: a record over the cap is refused, since a gzip cut short is no JSON; stream the file in
            # chunks when records outgrow the cap
            raise HttpError(500, f"the record of this job is over {JOB_RECORD_LIMIT >> 20} MB")
        return 200, data, {"Content-Type": "application/json", "Content-Encoding": "gzip", "Cache-Control": "no-cache"}

    def _list_datasets(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, {"datasets": self.ledger.datasets(query.get("state"))}, {}

    def _plan(self, *, query: dict[str, str], body: bytes) -> Response:
        parsed = _parse(PlanBody, body)
        text = parsed.spec
        try:
            spec = load_spec_text(text)
        except SpecError as error:
            raise HttpError(422, str(error)) from None
        ref = spec.catalog.ref
        states = {row["id"]: row.get("state", "unprepared") for row in self.ledger.datasets()}
        # the designer asks for the same plan once per page turn and sort: keep the last one while nothing it
        # depends on has changed (the spec's text, which names the ref, and every dataset's state)
        key = (text, frozenset(states.items()))
        kept_for, plan = self._last_plan  # read once: another request may replace it meanwhile
        if kept_for != key:
            records = self.ledger.records(ref)
            if not records:
                raise HttpError(409, f"catalog ref {ref} is not synced; run: mia-buffers catalog sync --ref {ref}")
            prepared = {record for record, state in states.items() if state == "prepared"}
            plan = build_plan(spec, spec_text_hash(text), ref, records, prepared=prepared).to_dict(states)
            self._last_plan = (key, plan)
        if parsed.page is None:
            return 200, plan, {}
        asked = parsed.page
        fields = {"limit": str(asked.limit), "offset": str(asked.offset), "dir": asked.dir, "q": asked.q}
        page = self._page_of(fields | ({"sort": asked.sort} if asked.sort else {}), PLAN_SORTS)
        assert page is not None
        datasets = {row["id"]: row for row in self._dataset_rows()[1]}
        staged_as = levels_key(int((spec.patch_for("2D") or {}).get("y") or 0), spec.sampling.levels)
        view = plan_view(
            plan,
            datasets,
            staged_as,
            page,
            fit_only=asked.fit_only,
            prepared_only=asked.prepared_only,
            raw_cached=asked.raw_cached,
            staged_geom=asked.staged_geom,
        )
        return 200, {**plan, **view}, {}

    def _sync_datasets(self, *, query: dict[str, str], body: bytes) -> Response:
        root = self.storage_root
        if root is None:
            raise HttpError(503, "the service has no storage_root")
        if not self._sync_lock.acquire(blocking=False):
            raise HttpError(409, "a datasets sync is running")
        try:
            # an unmounted or empty storage_root would demote every prepared row
            if not (root.is_dir() and any(root.iterdir())):
                raise HttpError(503, f"storage_root {root} is missing or empty; not syncing")
            started = time.time()
            return 200, self.ledger.sync_datasets(scan_storage(root), older_than=started), {}
        finally:
            self._sync_lock.release()

    # --- a buffer's status board, events, summary and retire: the page and the lifecycle -----------

    def _list_buffers(self, *, query: dict[str, str], body: bytes) -> Response:
        """Every buffer's summary. Paged, each also names its spec and, when its spec has a newer round, that
        round (`superseded_by`); filters: `spec` (a spec's number), `record` (a dataset on the buffer's board, whose
        row is added as `board`). Without a sort: the newest spec first, its newest round first."""
        page = self._page_of(query, BUFFER_SORTS)
        buffers = self.ledger.buffers()
        if page is None:
            return 200, {"buffers": buffers}, {}
        specs = {row["number"]: row["key"] for row in self.ledger.find_subjects(kind="buffer")}
        buffers.sort(key=lambda row: _round_of(row["id"]), reverse=True)
        newest: dict[int, str] = {}
        for buffer in buffers:
            number = _round_of(buffer["id"])[0]
            newest.setdefault(number, buffer["id"])
            buffer["spec"] = specs.get(number)  # its spec's page; a buffer of a spec never adopted has none
            buffer["superseded_by"] = newest[number] if number and newest[number] != buffer["id"] else None
        if "spec" in query:
            if not query["spec"].isdigit():
                raise HttpError(422, "query spec must be a spec's number")
            buffers = [buffer for buffer in buffers if _round_of(buffer["id"])[0] == int(query["spec"])]
        if "record" in query:
            boards = {board["buffer"]: board for board in self.ledger.boards_of(query["record"])}
            buffers = [{**buffer, "board": boards[buffer["id"]]} for buffer in buffers if buffer["id"] in boards]
        rows, total = paged(buffers, page, BUFFER_SORTS, lambda row: (row["id"], _spec_name(row)))
        return 200, {"buffers": rows, "total": total}, {}

    def _put_board(self, buffer: str, record: str, *, query: dict[str, str], body: bytes) -> Response:
        self._config(buffer)
        row = _parse(BoardBody, body)
        if row.status not in BOARD_STATUSES:
            raise HttpError(422, f"status must be one of {list(BOARD_STATUSES)}, got {row.status!r}")
        return 200, self.ledger.put_buffer_dataset(buffer, record, row.status, row.reason, row.job), {}

    def _board(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        self._config(buffer)
        page = self._page_of(query, BOARD_SORTS)
        board = self.ledger.buffer_datasets(buffer)
        if page is None:
            return 200, {"buffer": buffer, "datasets": board}, {}
        # the page's row names its dataset: its issue, its crops, and the staged artifact this buffer draws from
        datasets = {row["id"]: row for row in self.ledger.datasets()}
        staged = {
            asset["data"].get(
                "record"
            ): f"{asset['data'].get('levels_key')}/{asset['data'].get('record')}/{asset['key']}"
            for asset in self.ledger.assets("staged")
            if asset["data"] and buffer in asset["refs"] and asset["data"].get("levels_key")
        }
        issues = self._dataset_issues()
        for row in board:
            known = datasets.get(row["record"]) or {}
            row.update({name: known.get(name) for name in ("folder", "previews", "crops")})
            row["issue"] = issues.get(row["record"])
            row["staged"] = staged.get(row["record"])
        rows, total = paged(
            board, page, BOARD_SORTS, lambda row: (row["record"], row["status"], row["reason"], row["job"])
        )
        return 200, {"buffer": buffer, "datasets": rows, "total": total}, {}

    def _add_event(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        # no config check: `created` and `planned` precede the buffer's config (create_buffer runs at sourcing)
        event = _parse(EventBody, body)
        if event.kind not in EVENT_KINDS:
            raise HttpError(422, f"kind must be one of {list(EVENT_KINDS)}, got {event.kind!r}")
        added = self.ledger.add_event(
            buffer, event.kind, issue=event.issue, event_id=event.event_id, detail=event.detail
        )
        return 200, added, {}

    def _events(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        page = self._page_of(query, EVENT_SORTS)
        events = self.ledger.events(buffer)
        if page is None:
            return 200, {"buffer": buffer, "events": events}, {}
        events.reverse()  # the page reads the newest first
        rows, total = paged(events, page, EVENT_SORTS, lambda row: (row["kind"], row["detail"]))
        return 200, {"buffer": buffer, "events": rows, "total": total}, {}

    def _retire(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        self._config(buffer)
        return 200, self.ledger.retire(buffer, self.archive_dir), {}

    def _add_consumer(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        self._serving(buffer)
        info = _parse(ConsumerBody, body)
        return 201, {"consumer_id": self.ledger.add_consumer(buffer, info.model_dump())}, {}

    def _split_of(self, buffer: str, consumer_id: str) -> str:
        consumer = self.ledger.consumer(buffer, consumer_id)
        if consumer is None:
            raise HttpError(404, f"unknown consumer {consumer_id!r}")
        return str(consumer["split"])

    def _next(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        config = self._serving(buffer)
        consumer_id = self._consumer(buffer, query)
        n = _int(query, "n", 1, low=1, high=MAX_N)
        split = self._split_of(buffer, consumer_id)
        volumes = self.ledger.live_volumes(buffer)  # a quarantined (revoked) volume is never drawn
        with self._lock:
            if buffer not in self._rng:  # seed from what the ledger already served, so a restart draws fresh crops
                self._rng[buffer] = random.Random(f"{config.seed}:{self.ledger.max_seq(buffer)}")
            rng = self._rng[buffer]
            descriptors = [
                d
                for _ in range(n)
                if (d := draw(rng, volumes, split, patch=config.patch, levels=config.levels, channels=config.channels))
            ]
        if not descriptors:
            return self._no_content(buffer)
        return 200, {"descriptors": self.ledger.record_draws(buffer, consumer_id, descriptors)}, {}

    def _split(self, buffer: str, split: str, *, query: dict[str, str], body: bytes) -> Response:
        """A window of the split's frozen list, appended to and never reordered; a planned candidate is listed at most
        once. `exhausted` means the list is capped by the candidates, shorter than the configured size: it is set from
        the first read on (every drawable volume has samples and they number fewer than the size), with `size` the
        capped length, so a client sizes itself from any read. `size` never drops below the rows already frozen: a
        volume flagged missing later takes its candidates out of the cap, never its listed rows out of reach."""
        config = self._serving(buffer)
        self._consumer(buffer, query)
        start, n = _int(query, "from", 0, low=0, high=10**9), _int(query, "n", 1, low=1, high=MAX_N)
        size = config.split_sizes.get(split, DEFAULT_SPLIT_SIZE)
        if start >= size:
            raise HttpError(416, f"from {start} is past the {split} list of {size}")
        volumes = self.ledger.live_volumes(buffer)  # a quarantined (revoked) volume is never listed
        if not servable(volumes, split):
            return self._no_content(buffer)
        candidates, exhausted = capacity(volumes, split), False
        if candidates == 0:  # only zero-weight volumes hold candidates: nothing drawable yet
            return self._no_content(buffer)
        if candidates is not None and candidates < size:  # capped by the candidates, shorter than the configured size
            size, exhausted = candidates, True
        patch, levels, channels = config.patch, config.levels, config.channels
        # ponytail: the replay reproduces the frozen prefix only while the volumes' candidates are those it was drawn
        # from; a re-PUT with other candidates keeps the listed rows (never re-drawn) but draws the rest from a
        # diverged sequence, and a re-PUT that drops `samples` turns the volume legacy, so its new rows are uniform
        # centres over a staged box. Freeze the candidate lists per list if a re-planned record must be re-registered.
        with self._lock:
            frozen = self.ledger.frozen(buffer, split)
            # a frozen row stays addressable: a volume flagged missing drops its candidates from `capacity`, not rows
            size = max(size, len(frozen))
            if start >= size:
                raise HttpError(416, f"from {start} is past the {split} list of {size}, every candidate listed")
            end = min(start + n, size)
            if len(frozen) < end:
                rng = random.Random(f"{config.seed}:{split}")
                used: set[tuple[str, int]] = set()
                fresh = []
                for index in range(end):  # replay the whole prefix so the list never depends on request order
                    d = draw_frozen(rng, volumes, split, used, patch=patch, levels=levels, channels=channels)
                    if d is None:  # a defence: unreachable while the candidates are unchanged (capacity caps end)
                        break
                    entry = frozen[index] if index < len(frozen) else {**d, "seq": index}
                    if index >= len(frozen):
                        fresh.append(entry)
                    if "candidate" in entry:
                        used.add((entry["volume"], entry["candidate"]))
                self.ledger.extend_frozen(buffer, split, fresh)
                frozen += fresh
        return 200, {"descriptors": frozen[start:end], "size": size, "exhausted": exhausted}, {}

    def _no_content(self, buffer: str) -> Response:
        """A 204 (nothing servable yet), counted for the buffer's usage: the day's row and the last hour's window."""
        now = time.time()
        with self._lock:
            self._empty_window(buffer, now).append(now)
        self.ledger.count_empty(buffer, datetime.fromtimestamp(now, UTC).strftime("%Y-%m-%d"))
        return 204, None, {"Retry-After": str(RETRY_AFTER_S)}

    def _empty_window(self, buffer: str, now: float) -> deque[float]:
        """The buffer's 204 times within the last hour, older ones dropped; call under `_lock`."""
        window = self._empty.setdefault(buffer, deque())
        while window and window[0] < now - EMPTY_WINDOW_S:
            window.popleft()
        return window

    # --- the page: its files, what it needs from mia.yaml, usage and the keeper's last pass --------

    def _page(self, *, query: dict[str, str], body: bytes) -> Response:
        return self._ui("index.html", query=query, body=body)

    def _ui(self, name: str, *, query: dict[str, str], body: bytes) -> Response:
        """A file of the page by basename only (the segment is unquoted already, so check it here). The browser
        asks again on every load, so the page's modules are never a mix of two deployments."""
        content_type = UI_TYPES.get(PurePosixPath(name).suffix)
        if self.ui_dir is None or not UI_NAME_RE.match(name) or content_type is None:
            raise HttpError(404, "no such file")
        path = self.ui_dir / name
        if not path.is_file():
            raise HttpError(404, "no such file")
        # ponytail: no validator, so each load sends every file again (about 250 kB); an ETag when that is felt
        return 200, path.read_bytes(), {"Content-Type": content_type, "Cache-Control": "no-cache"}

    def _meta(self, *, query: dict[str, str], body: bytes) -> Response:
        if self.meta is not None:
            return 200, self.meta, {}
        names = (
            "buffers_repo",
            "datasets_repo",
            "catalog_repo",
            "service_url",
            "datasets_workflow_url",
            "gallery_branch",
        )
        return 200, {**dict.fromkeys(names), "roots": {}, "quota": {}, "keeper": {}, "lines": LINES}, {}

    def _usage(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        config = self._config(buffer)
        with self._lock:
            empty = len(self._empty_window(buffer, time.time()))
        last_hour = {**self.ledger.last_hour(buffer), "empty": empty}
        days = self.ledger.usage(buffer)
        out = {"buffer": buffer, "last_hour": last_hour, "max_empty_fraction": config.max_empty_fraction}
        page = self._page_of(query, USAGE_SORTS)
        if page is None:
            return 200, {**out, "days": days}, {}
        days.sort(key=lambda day: day["day"], reverse=True)  # the page's table reads the newest day first
        rows, total = paged(days, page, USAGE_SORTS)
        return 200, {**out, "days": rows, "total": total}, {}

    def _put_keeper(self, *, query: dict[str, str], body: bytes) -> Response:
        try:
            report = json.loads(body)
        except ValueError as error:
            raise HttpError(422, f"invalid JSON: {error}") from None
        if not isinstance(report, dict):
            raise HttpError(422, "a keeper report is a JSON object")
        report.setdefault("at", time.time())
        self.ledger.put_kv("keeper", report)
        return 200, report, {}

    def _keeper(self, *, query: dict[str, str], body: bytes) -> Response:
        report = self.ledger.kv("keeper")
        if report is None:
            return 200, *JSON_NULL
        return 200, report, {}

    def _status(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        """The buffer's summary; with a staged root, first stats each volume's pyramid once and flags the missing."""
        config = self._config(buffer)
        volumes = self.ledger.volumes(buffer)
        if self.staged_root is not None:  # a service without one (tests, a dev service) has no view of the disk
            for index, volume in enumerate(volumes):
                missing = not os.path.exists(volume.path)
                if missing != volume.missing:
                    self.ledger.set_volume_missing(buffer, volume.id, missing, volume.path)
                    volumes[index] = replace(volume, missing=missing)
        splits = sorted({split for volume in volumes for split in volume.split_planes})
        quarantined = self.ledger.quarantined_volumes(buffer)
        live = [volume for volume in volumes if volume.id not in quarantined]
        return (
            200,
            {
                "buffer": buffer,
                "config": config.model_dump(),
                "volumes": len(volumes),
                "servable": {split: len(servable(live, split)) for split in splits},
                "missing": [volume.id for volume in volumes if volume.missing],
                "quarantined": quarantined,
                "draws": self.ledger.draws_per_volume(buffer),
                "consumers": self.ledger.consumers(buffer, self.stale_s),
            },
            {},
        )


def make_server(service: BufferService, host: str, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def _dispatch(self) -> None:
            parts = urlsplit(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            status, payload, headers = service.handle(
                self.command, parts.path, dict(parse_qsl(parts.query)), body, dict(self.headers.items())
            )
            if payload is not None and not isinstance(payload, (bytes, dict)):
                # a stream: no Content-Length, one flushed write per chunk, until the reader leaves
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.close_connection = True
                try:
                    for chunk in payload:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                except OSError:
                    pass  # the reader left; its thread ends here
                return
            if isinstance(payload, bytes):
                data = payload  # the handler's headers carry its Content-Type
            else:
                data = json.dumps(payload).encode() if payload is not None else b""
                if data:
                    headers = {**headers, "Content-Type": "application/json"}
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if data:
                self.wfile.write(data)

        do_GET = do_PUT = do_POST = do_DELETE = _dispatch

        def log_message(self, format: str, *args: object) -> None:  # quiet; the ledger is the record
            return

    return ThreadingHTTPServer((host, port), Handler)


def ledger_path(cfg: Config) -> Path:
    """Where buffer-api keeps its SQLite file: host-local disk, never the shared state directory."""
    if cfg.paths.local_root is None:
        raise RuntimeError("set paths.local_root in mia.yaml: buffer-api keeps its SQLite file on host-local disk")
    return cfg.paths.local_root / "service" / "buffers.sqlite"


def open_ledger(cfg: Config) -> Ledger:
    """The ledger on the local root; a missing ledger beside existing backups is refused, never silently started
    empty."""
    path = ledger_path(cfg)
    require_local_dir(path.parent)
    if not path.exists():
        backups = sorted((cfg.paths.state_dir / "backups").glob("buffers-*.sqlite"))
        if backups:
            raise RuntimeError(
                f"the ledger {path} is missing but backups exist; the newest is {backups[-1]}: "
                f"copy it to {path} to restore it"
            )
    return Ledger(path)


def prune_backups(directory: Path, keep: int = 48) -> None:
    """Delete the oldest `buffers-*.sqlite` files beyond `keep` (the UTC stamp in the name sorts by age)."""
    for stale in sorted(directory.glob("buffers-*.sqlite"))[:-keep]:
        stale.unlink(missing_ok=True)


def backup_forever(ledger: Ledger, directory: Path, minutes: int, stop: threading.Event) -> None:
    """Back the ledger up every `minutes` until `stop` is set; a failed backup is logged, never raised."""
    while not stop.wait(minutes * 60):
        try:
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            ledger.backup(directory / f"buffers-{stamp}.sqlite")
            prune_backups(directory)
        except Exception:
            log.exception("ledger backup failed")


def page_meta(cfg: Config) -> dict:
    """What the page reads from mia.yaml (`GET /meta`): the repos, the service URL, the Temporal UI prefix of a
    dataset's workflow (append the issue number; None without `temporal.ui_url`), each root's browser URL (None when
    unset), the quota, the keeper's cadence and each kind's lifecycle line."""
    github, paths, ui_url = cfg.github, cfg.paths, cfg.temporal.ui_url
    # ponytail: mia-data's namespace is its default; read services.mia-data.temporal.namespace when a deployment renames it
    workflow_prefix = quote(f"mia-data/{github.datasets_repo}/issue-", safe="")
    return {
        "buffers_repo": github.buffers_repo,
        "datasets_repo": github.datasets_repo,
        "catalog_repo": github.catalog_repo,
        "service_url": cfg.service.url,
        "datasets_workflow_url": f"{ui_url.rstrip('/')}/namespaces/mia-data/workflows/{workflow_prefix}"
        if ui_url
        else None,
        # ponytail: mia-data's gallery branch as this service's own config reads it (the default unless the
        # shared section sets it); read services.<mia-data>.gallery.branch when a deployment renames it there
        "gallery_branch": cfg.gallery.branch,
        "roots": {root: getattr(paths, f"{root}_url") for root in paths.ROOTS},
        "quota": cfg.quota.model_dump(),
        "keeper": cfg.keeper.model_dump(),
        "lines": LINES,
    }


def serve(cfg: Config) -> None:
    """Run buffer-api until interrupted, as configured: URL, ledger on the local root, backups, worker token."""
    if cfg.service.token_file is None:
        raise SystemExit("set service.token_file in mia.yaml and run `mia init-token`")
    token = read_token(cfg.service.token_file)
    if cfg.paths.local_root is None:
        raise SystemExit("set paths.local_root in mia.yaml")
    try:
        ledger = open_ledger(cfg)
    except RuntimeError as error:
        raise SystemExit(str(error)) from error
    url = urlparse(cfg.service.url)
    assert url.port is not None  # ServiceCfg requires an explicit port

    check = HostCheck.from_config(cfg)
    try:
        generate_policy(check.base_policy, check.policy_dir, check.writable)
    except PolicyError as error:
        raise SystemExit(str(error)) from error
    from .dispatch import dispatcher_for  # here, not at the top: it reaches autodev's CLI helpers

    service = BufferService(
        ledger,
        token=token,
        storage_root=cfg.paths.storage_root,
        work_root=cfg.paths.work_root,
        raw_root=cfg.paths.raw_root,
        staged_root=cfg.paths.staged_root,
        locks=LockDir(cfg.paths.local_root / "locks"),
        archive_dir=cfg.paths.state_dir / "archive",
        meta=page_meta(cfg),
        ui_dir=Path(__file__).parent / "ui",
        dispatcher=dispatcher_for(cfg),
        hosts=check,
    )
    service.kick_unmirrored()  # what a crash, or Temporal being away, left unposted
    server = make_server(service, url.hostname or "127.0.0.1", url.port)
    backups = cfg.paths.state_dir / "backups"
    stop = threading.Event()
    threading.Thread(target=backup_forever, args=(ledger, backups, cfg.keeper.minutes, stop), daemon=True).start()
    host_name, port_number = server.server_address[0], server.server_address[1]
    print(f"buffer-api on http://{host_name!s}:{port_number} ledger {ledger_path(cfg)} backups {backups}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
