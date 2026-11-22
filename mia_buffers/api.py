"""buffer-api: the one sampler and ledger for a buffer, as a small HTTP service (spec v0.5 §5).

`BufferService.handle` does all routing and validation without sockets; `make_server` wraps it in the standard
library's threading HTTP server. Consumers are untrusted (§7): bodies are validated strictly and a consumer can
only name its split.
"""

from __future__ import annotations

import base64
import hmac
import json
import logging
import math
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
from typing import Any, Literal, NamedTuple
from urllib.parse import parse_qsl, quote, unquote, urlparse, urlsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from autodev.config import RAW_GITHUB
from mia_core.assets import ASSET_STATES
from mia_core.fs import require_local_dir
from mia_core.hosts import HOST_RE, PolicyError, generate_policy, is_ip_literal
from mia_core.jobrecord import RECORDS_DIR
from mia_core.judge.policy import GATES, SUBJECT_KINDS, VERDICTS
from mia_core.locks import LockDir
from mia_core.registry import read_token
from mia_core.transfers import MAX_ROWS, reduce_row
from mia_core.untrusted import neutralize, read_untrusted
from mia_data import config as mia_data_config
from mia_data.models import LabelEvent

from .config import PREP_STAGE_ACTIONS, STAGE_ACTIONS, Config
from .dispatch import DispatchError, dispatcher_for
from .explore import (
    ANYTHING,
    DATASET_FILTERS,
    DATASET_SORTS,
    LACKS,
    NOTHING,
    PLAN_SORTS,
    Page,
    dataset_haystack,
    dataset_row,
    facets,
    lacks_filter,
    page_query,
    paged,
    plan_view,
    record_row,
    start_gaps,
)
from .hosts import HostCheck, Needed
from .jobspec import levels_key
from .ledger import BOARD_STATUSES, EVENT_KINDS, Ledger, head_of
from .peek import Peeker, PeekFailed
from .plan import build_plan
from .sampler import Level, Volume, capacity, draw, draw_frozen, fits, servable
from .spec import SpecError, load_prep_text, load_spec_text, spec_text_hash
from .specmap import facts
from .stream import Changes
from .sync import scan_assets, scan_storage
from .throughput import by_level, by_minute, summary

RETRY_AFTER_S = 30
DEFAULT_SPLIT_SIZE = 256
MAX_N = 1024  # descriptors per request; consumers are untrusted (§7)
# no row = unprepared; `revoked`: withdrawn (spec autonomous-gates §6)
DATASET_STATES = ("in-progress", "prepared", "needs-human", "revoked")
PEEK_SPLITS = ("train", "val", "test")  # what `GET /buffers/<id>/peek` may be asked for
PEEK_WAIT_S = 5.0  # how long a peek waits for the one before it; past that it is refused, so no queue builds up
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
# kind of subject -> its stage table: stage -> (phase, the actions it accepts) (spec webapp-actions §5.1)
SUBJECT_RULES = {"dataset": mia_data_config.STAGE_ACTIONS, "buffer": STAGE_ACTIONS, "prep": PREP_STAGE_ACTIONS}
# where a round can go from any step: not steps of a lifecycle themselves
OFF_LINE = ("debugging", "failed", "blocked", "needs-human", "cancelled")
# kind of subject -> its lifecycle as the page draws it: the stages of its table, in the table's order
LINES = {kind: [stage for stage in table if stage not in OFF_LINE] for kind, table in SUBJECT_RULES.items()}
# Shelving (spec shelved-subjects): a person sets a subject aside with a reason. Its stage stays; its status reads
# `shelved`, it takes `unshelve` alone, and a shelved dataset reads `shelved` to every plan and request. A subject
# in one of these phases (no round open) also takes `shelve`; so does one whose round waits for a person's
# verdict (`waiting`, and its stage takes `cancel`), and that shelve cancels the round first.
SHELVABLE = ("not started", "stopped", "done")
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
    spec: str | None = None  # a buffer spec's key


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
    """One write of a round: a stage, a timeline entry (text with the marker that makes it once-only), or both.
    `data` rides with either: keys of the subject's data that this write replaces (a dataset's `sets`)."""

    body: str = ""
    marker: str = ""
    round: str | None = None
    stage: str | None = None
    data: dict | None = None

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
        "go",
        "resume",
        "approve",
        "reject",
        "revoke",
        "keep",
        "retire",
        "dropped",
        "cancel",
        "stop",
        "shelve",
        "unshelve",
        "recheck",
        "edit",
    ]
    key: str | None = None  # the sender's own name for this request: a repeat is the same action
    # the subject's revision the sender was looking at: required of a verdict, checked on any action that has it
    revision: int | None = None
    run: str | None = None  # stop: the run to end; one that is over already is not replaced by a later run
    set: str | None = None  # a dataset's verdict: the set of crops it is about (spec judge-decides §5.4)
    note: str = ""  # reject: the feedback for the agent; shelve: why it can wait
    root: str | None = None  # a worker's go: the root request this round is charged to
    actor: str | None = None  # a worker's name for itself (`prep:<number>`); needs the worker token
    # a buffer's or a request's go: the YAML text that runs (default: the stored one); a buffer's edit: its new text
    spec: str | None = None
    plan_first: bool = False  # a buffer's go: stop at the plan and wait for an approve before staging anything
    records: list[str] = []  # a buffer's revoke: the records that leave it
    record: str | None = None  # dropped: the dataset whose files are in place
    approve_hosts: list[str] = []  # a start action: hosts the run needs that the person checked (spec host-approval §5)


class SpecBody(_Strict):
    """A buffer spec saved as a new subject, and run when `action` is `go`; or a preparation request, always run."""

    spec: str
    action: Literal["go"] | None = None
    plan_first: bool = False
    key: str | None = None
    approve_hosts: list[str] = []  # a preparation request: hosts the run needs that the person checked
    buffer: int | None = None  # a preparation request: the number of the buffer whose Prepare step filed it


class RefusalBody(_Strict):
    state: Literal["refused"]
    detail: str


class IssueBody(_Strict):
    issue: int


# a case is in work (`open`, `working`, `waiting`) or has ended: `resolved`, `dropped` (somebody acted), or
# `gave-up` (the repair agent could not repair it; the case says what it takes). Whether a human is needed is the
# subject's status to say, never a case's (spec overview §4).
CASE_STATES = ("open", "working", "waiting", "resolved", "gave-up", "dropped")
# what `GET /cases` lists whatever its age: the sweep reads what it gave up on for as long as the case stands
CASES_OPEN = ("open", "working", "waiting", "gave-up")
CaseState = Literal["open", "working", "waiting", "resolved", "gave-up", "dropped"]


STATUSES = ("shelved", "in repair", "needs human", "done", "not started", "working")  # what `status_of` gives


def status_of(phase: str, shelved: bool, repair: dict | None, acting: bool) -> str:
    """Who moves the subject next, the one word a person reads and the only one that says a human is needed
    (spec overview §3.2): `shelved`, `in repair`, `needs human`, `done`, `not started` or `working`. `repair` is
    the repair agent's case about the subject as it stands (`BufferService._repair_of`), `acting` whether the
    agent carries its cases out. A case with steps left makes the subject `in repair` at any phase: a stage its
    own step produced (`cancelled` before its resume) is part of the repair. One it gave up on needs a human
    wherever it left the subject."""
    if shelved:
        return "shelved"
    state = repair["state"] if repair else None
    if acting and (
        state == "working"
        or (state == "open" and phase in ("working", "waiting", "stopped"))
        or (state == "waiting" and phase in ("waiting", "stopped"))
    ):
        return "in repair"
    if state == "gave-up":
        return "needs human"
    return phase if phase in ("done", "not started", "working") else "needs human"


REPORT_STALE = 3  # a sweep's report is overdue once this many of its periods passed with none after it
REPORT_EVERY_S = 600.0  # the period of a report that names none


class Repairs(NamedTuple):
    """What the repair agent has on the subjects, read once for many views."""

    acting: bool  # its last sweep ran in `act` mode: a case in work is being carried out
    cases: dict[tuple[str, int], dict]  # (kind, number) -> the subject's newest case in work or given up on


class CaseBody(_Strict):
    """A case the cleaner opens: what is stuck (a subject, or with no number something left behind), the rule
    that matched and what was seen (spec cleaner §4.8)."""

    kind: str
    number: int | None = None
    key: str
    rule: str
    state: CaseState = "open"
    data: dict = {}


class CaseUpdate(_Strict):
    state: CaseState | None = None
    rule: str | None = None
    data: dict | None = None


class TerminateBody(_Strict):
    namespace: str
    id: str
    run_id: str
    reason: str


class RefusedHostsBody(_Strict):
    hosts: list[str] = Field(max_length=20)  # hosts a job of the dataset was refused (`host_not_allowed`)


class WorkerBody(_Strict):
    commit: str | None  # the commit the checkout was at when the worker started; None when it could not tell
    code: str | None = None  # the digest of the code it loaded (`mia_core.checkout.code_digest`)


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
    "bytes": _field("bytes"),
    "raw": _field("raw_bytes"),
    "staged": _field("staged_bytes"),
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
    """What a buffer's spec is called: the name its config recorded, else its spec's key."""
    return (buffer.get("config") or {}).get("spec_name") or buffer.get("spec")


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
    ("GET", re.compile(r"^/buffers/([^/]+)/peek$"), "peek", "read"),  # a crop to look at; the buffer keeps no trace
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
    ("PUT", re.compile(r"^/transfers/(.+)$"), "put_transfers", "worker"),
    ("GET", re.compile(r"^/records$"), "records", "read"),
    ("GET", re.compile(r"^/catalogs$"), "catalogs", "read"),
    ("GET", re.compile(r"^/datasets$"), "list_datasets", "read"),
    ("GET", re.compile(r"^/catalog/datasets$"), "catalog_datasets", "read"),
    ("GET", re.compile(r"^/catalog/datasets/([^/]+)$"), "catalog_dataset", "read"),
    ("PUT", re.compile(r"^/datasets/([^/]+)$"), "put_dataset", "worker"),
    ("GET", re.compile(r"^/datasets/([^/]+)$"), "get_dataset", "read"),
    ("PUT", re.compile(r"^/datasets/([^/]+)/hosts$"), "put_refused_hosts", "worker"),
    ("GET", re.compile(r"^/datasets/([^/]+)/hosts$"), "refused_hosts", "read"),
    ("GET", re.compile(r"^/crops/([^/]+)/([^/]+)/(raw|overlay)\.png$"), "crop_png", "read"),
    ("GET", re.compile(r"^/gallery/i(\d+)-e(\d+)/([0-9a-f]{40})/([^/]+)/(raw|overlay)\.png$"), "gallery_png", "read"),
    ("GET", re.compile(r"^/jobs/(.+)$"), "job_record", "read"),
    ("POST", re.compile(r"^/plan$"), "plan", "read"),  # a POST only to carry the spec text; it writes nothing
    ("POST", re.compile(r"^/specs/parse$"), "parse_spec", "read"),  # the same: the spec as the service reads it
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
    ("GET", re.compile(r"^/subjects$"), "by_status", "read"),
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
    ("GET", re.compile(r"^/hosts$"), "host_summaries", "read"),
    ("GET", re.compile(r"^/hosts/([^/]+)/minutes$"), "host_minutes", "read"),
    ("GET", re.compile(r"^/hosts/([^/]+)$"), "host_levels", "read"),
    ("POST", re.compile(r"^/cases$"), "open_case", "worker"),
    ("PUT", re.compile(r"^/cases/(\d+)$"), "update_case", "worker"),
    ("GET", re.compile(r"^/cases$"), "cases", "read"),
    ("PUT", re.compile(r"^/cleaner$"), "put_cleaner", "worker"),
    ("GET", re.compile(r"^/cleaner$"), "cleaner", "read"),
    ("PUT", re.compile(r"^/workers/([^/]+)$"), "put_worker", "worker"),
    ("GET", re.compile(r"^/workers$"), "workers", "read"),
    ("GET", re.compile(r"^/runs$"), "runs", "read"),
    ("POST", re.compile(r"^/runs/terminate$"), "terminate_run", "worker"),
    ("GET", re.compile(r"^/schedules$"), "schedules", "read"),
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
        snippets_work_root: Path | None = None,
        snippets_repo: str | None = None,
        raw_root: Path | None = None,
        staged_root: Path | None = None,
        locks: LockDir | None = None,
        archive_dir: Path | None = None,
        meta: dict | None = None,
        ui_dir: Path | None = None,
        dispatcher: Any = None,
        hosts: HostCheck | None = None,
        peeker: Any = None,
    ):
        self.ledger = ledger
        self.peeker = peeker  # draws a peek's crop (`peek.Peeker`, or a test's stand-in); None: no harness, no peek
        self._peek_lock = threading.Lock()  # one harness process at a time, however many pages ask
        self.hosts = hosts  # which hosts a run needs that the csub policy lacks; None cannot say
        # the way to Temporal (`dispatch.Dispatcher`, or a test's stand-in); None reaches no workflow
        self.dispatcher = dispatcher
        self.token = token  # None disables the worker check (tests, a dev service)
        self.stale_s = stale_s
        self.storage_root = storage_root  # mia-data's published datasets; None disables the datasets sync
        self.work_root = work_root  # the rounds' job workspaces, for a round's crops before it publishes; None: none
        # where autodev's jobs on the snippets repo leave their records, and the directory they are under there
        # (`<owner>__<repo>`, autodev's `repo_slug`); None: that service's records are not served
        self.snippets_work_root = snippets_work_root if snippets_repo else None
        self.snippets_slug = (snippets_repo or "").replace("/", "__")
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

    # --- what each host gave: the transfer rows read per host, by load level and by minute -------

    @staticmethod
    def _number(query: dict[str, str], key: str, default: float) -> float:
        try:
            value = float(query.get(key, default))
        except ValueError:
            value = math.nan
        if not math.isfinite(value):
            raise HttpError(422, f"query {key} must be a finite number")
        return value

    def _window(self, query: dict[str, str]) -> tuple[float, int, int]:
        """The `days` asked for (default 7) and the minutes it covers, ending with the current one."""
        days = self._number(query, "days", 7)
        if not 0 < days <= 366:
            raise HttpError(422, f"query days must be over 0 and at most 366, got {days}")
        until = int(time.time() // 60)
        return days, until - int(days * 1440), until

    def _host_summaries(self, *, query: dict[str, str], body: bytes) -> Response:
        """Every host with rows in the window: its minutes added over jobs and read as one row, most bytes first."""
        days, since, until = self._window(query)
        minutes: dict[str, list[dict]] = {}
        for minute in by_minute(self.ledger.transfers(since, until)):
            minutes.setdefault(minute["host"], []).append(minute)
        hosts = [{"host": host, **summary(rows)} for host, rows in minutes.items()]
        return 200, {"days": days, "hosts": sorted(hosts, key=lambda row: (-row["bytes"], row["host"]))}, {}

    def _host_levels(self, host: str, *, query: dict[str, str], body: bytes) -> Response:
        """One host's window by load level; a host with no rows has no levels."""
        days, since, until = self._window(query)
        host = host.lower()
        return (
            200,
            {"host": host, "days": days, "levels": by_level(by_minute(self.ledger.transfers(since, until, host)))},
            {},
        )

    def _host_minutes(self, host: str, *, query: dict[str, str], body: bytes) -> Response:
        """One host's minutes between `from` and `to` (unix seconds; `to` defaults to now, `from` to a day before)."""
        end = self._number(query, "to", time.time())
        start = self._number(query, "from", end - 86400)
        if not 0 <= end - start <= 7 * 86400:
            raise HttpError(422, "from is not after to, and they span at most 7 days")
        host = host.lower()
        rows = self.ledger.transfers(int(start // 60), int(end // 60), host)
        return 200, {"host": host, "minutes": by_minute(rows)}, {}

    # --- records, datasets and the plan: what the catalog holds and which of it is prepared ------

    def _put_records(self, ref: str, *, query: dict[str, str], body: bytes) -> Response:
        if not CATALOG_REF_RE.match(ref):
            raise HttpError(422, f"ref {ref!r} is not a 40-hex commit")
        records = [(item.path, item.record) for item in _parse(RecordsBody, body).records]
        return 200, {"ref": ref, "count": self.ledger.put_records(ref, records)}, {}

    def _put_transfers(self, job: str, *, query: dict[str, str], body: bytes) -> Response:
        """Store one job's transfer rows (job: the record's address); one invalid row refuses them all."""
        if not all(UI_NAME_RE.fullmatch(segment) for segment in job.split("/")):
            raise HttpError(422, f"{job!r} is not a job address")
        try:
            data = json.loads(body)
        except ValueError as error:
            raise HttpError(422, f"invalid JSON: {error}") from None
        if not (isinstance(data, dict) and isinstance(data.get("service"), str) and isinstance(data.get("rows"), list)):
            raise HttpError(422, 'the body is {"service": str, "rows": [...]}')
        if len(data["rows"]) > MAX_ROWS:
            raise HttpError(422, f"at most {MAX_ROWS} rows")
        rows = []
        for item in data["rows"]:
            row = reduce_row(item)
            if row is None:
                raise HttpError(422, "a row is not a transfer row")
            rows.append(row)
        return 200, {"job": job, "rows": self.ledger.put_transfers(job, data["service"], rows)}, {}

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

    def recheck_resting(self) -> None:
        """Tell every request resting at `needs-human` to look at its datasets again: run at start, so one whose
        datasets settled with nobody telling it (Temporal was away, or they settled before it rested) is found."""
        self._recheck_requests(None)

    def _recheck_requests(self, record: str | None) -> None:
        """A dataset was shelved or became prepared: tell each request resting at `needs-human` that names it to
        look at its datasets again (`MiaPrep.recheck`); the request moves itself to `done` when each is prepared
        or shelved. One that selects by rule names no record here, so it is told too, and with no record every
        resting request is. Best effort: the write that led here stands, and the next start tells them again."""
        if self.dispatcher is None:
            return
        for request in self.ledger.find_subjects(kind="prep"):  # ponytail: reads every request, as `_catalog_dataset`
            if request["stage"] != "needs-human" or (record and record not in (_prep_records(request) or [record])):
                continue
            try:
                self.dispatcher.signal_running("prep", request["number"], "recheck")
            except DispatchError as error:
                log.warning("request %s not told to look again (%s): %s", request["key"], record or "start", error)

    def _get_dataset(self, record: str, *, query: dict[str, str], body: bytes) -> Response:
        row = self.ledger.dataset(record)
        if row is None:
            raise HttpError(404, f"no dataset row for {record}")
        return 200, row, {}

    def _refused_hosts(self, record: str, *, query: dict[str, str], body: bytes) -> Response:
        """What is held for the dataset alone: the hosts its jobs were refused, and those a person approved for it
        and no other dataset (`_approve_for`)."""
        refused = (self.ledger.kv("refused_hosts") or {}).get(record, [])
        return 200, {"hosts": refused, "approved": (self.ledger.kv("dataset_hosts") or {}).get(record, [])}, {}

    def _put_refused_hosts(self, record: str, *, query: dict[str, str], body: bytes) -> Response:
        """A worker says which hosts a job of the dataset was refused (a redirect target, mostly). They join the
        hosts the dataset's runs need, so its next start asks a person to approve them; a name that is no exact
        hostname is dropped."""
        parsed = _parse(RefusedHostsBody, body)
        with self._lock:
            refused = self.ledger.kv("refused_hosts") or {}
            named = (host for host in parsed.hosts if HOST_RE.fullmatch(host) and not is_ip_literal(host))
            refused[record] = sorted({*refused.get(record, []), *named})
            self.ledger.put_kv("refused_hosts", refused)
        return 200, {"hosts": refused[record]}, {}

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
        """The Datasets list: one page of datasets, filtered by state, modality, dims, licence, what the record
        lacks (`lacks`: one of `LACKS`, `anything` or `nothing`) or a request's records, with how many datasets have each
        filter value."""
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
        if "lacks" in query:
            if query["lacks"] not in (*LACKS, ANYTHING, NOTHING):
                raise HttpError(422, f"lacks must be one of {[*LACKS, ANYTHING, NOTHING]}, got {query['lacks']!r}")
            rows = lacks_filter(rows, query["lacks"])
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
        """A subject's phase and the actions it accepts: its stage's (`SUBJECT_RULES`), without `go` for a
        dataset whose record cannot be run (`gaps`: no Start until a synced catalog holds one that can), with
        `shelve` while no round is working and none was just asked for (a round that waits for a verdict is
        cancelled by the shelve, `_hold`). A shelved subject (`hold`) keeps its phase and takes `unshelve` alone."""
        phase, actions = SUBJECT_RULES[row["kind"]][row["stage"]]
        if hold is not None:
            return phase, frozenset({"unshelve"})
        if gaps:
            actions = actions - {"go", "resume"}
        if row["kind"] == "dataset":
            # at `done` a reject needs the saved set to be about (spec judge-decides §5.2)
            sets = (row.get("data") or {}).get("sets") or {}
            if row["stage"] == "done" and not sets.get("saved"):
                actions = actions - {"reject"}
            # a resume would save the revoked set again (spec resume-restart §4.3): the round says it was
            # revoked, or the dataset's row does, which also holds for a round that carries no such mark
            if sets.get("revoked") or (self.ledger.dataset(row["key"]) or {}).get("state") == "revoked":
                actions = actions - {"resume"}
        resting = phase in SHELVABLE or (phase == "waiting" and "cancel" in actions)
        if resting and self._asking(row["kind"], row.get("number")) is None:
            actions = actions | {"shelve"}
        return phase, actions

    def _asking(self, kind: str, number: int | None) -> dict | None:
        """The action that was sent to the subject's workflow and that the workflow has not answered yet, while
        its wait lasts."""
        if number is None or (held := self._asked.get((kind, number))) is None:
            return None
        action, since = held
        if (wait_s := ASKED_WAIT_S - (time.monotonic() - since)) <= 0:
            return None
        return {"id": action["id"], "action": action["action"], "at": action["at"], "wait_s": round(wait_s, 1)}

    def _repairs(self) -> Repairs:
        cases: dict[tuple[str, int], dict] = {}
        for case in self.ledger.cases(CASES_OPEN):  # newest first
            if case["number"] is not None:
                cases.setdefault((case["kind"], case["number"]), case)
        report = self.ledger.kv("cleaner") or {}
        # the agent acts while its sweeps report in `act` mode: a report that is overdue (the schedule is off, the
        # worker is away) repairs nothing, whatever mode it was in
        fresh = time.time() - (report.get("at") or 0) <= REPORT_STALE * (report.get("every_s") or REPORT_EVERY_S)
        return Repairs(report.get("mode") == "act" and fresh, cases)

    @staticmethod
    def _repair_of(row: dict, case: dict | None) -> dict | None:
        """The repair result that holds for the subject as it stands: its newest case while that is in work, or
        once the agent gave up on it for as long as the subject has not moved since. That is read from the
        revision the case ended at; a case that ended with none holds until the subject is next written."""
        if case is None:
            return None
        if case["state"] == "gave-up":
            ended = case["data"].get("ended_revision")
            holds = ended == row["revision"] if ended is not None else case["updated"] >= (row.get("updated") or 0)
            if not holds:
                return None
        return {"id": case["id"], "rule": case["rule"], "state": case["state"], "needs": case["data"].get("needs")}

    def _subject_view(
        self, row: dict, holds: dict[tuple[str, int], dict] | None = None, repairs: Repairs | None = None
    ) -> dict:
        """The row plus its name, its phase (what its round does at its stage: what a worker decides by), its
        status (what a person reads), the actions it accepts (`_rules`; sorted, for a stable page), its
        hold when it is shelved, a dataset's `hosts` (`_unapproved`), and `asked`: the action that was sent to its workflow and that the workflow has not
        answered yet. Until it does the stage is still the old one, and the page must not offer that stage's
        buttons as if nothing was clicked. `repair` is the repair agent's result for the subject as it stands.
        `holds` (`Ledger.shelved()`) and `repairs` (`_repairs`) are read once by a caller with many rows."""
        hold = (self.ledger.shelved() if holds is None else holds).get((row["kind"], row["number"]))
        gaps = self._gaps(row["key"]) if row["kind"] == "dataset" else []
        phase, actions = self._rules(row, hold, gaps)
        asked = self._asking(row["kind"], row["number"])
        repairs = self._repairs() if repairs is None else repairs
        repair = self._repair_of(row, repairs.cases.get((row["kind"], row["number"])))
        status = status_of(phase, hold is not None, repair, repairs.acting)
        view = {**row, "phase": phase, "status": status, "repair": repair}
        view |= {"actions": sorted(actions), "asked": asked, "gaps": gaps, "shelved": hold}
        if row["kind"] == "dataset":
            view["hosts"] = self._unapproved(row["key"])
        view["name"] = (row.get("data") or {}).get("name") or row["key"]  # a buffer's name is not its key
        return view

    def _unapproved(self, record: str) -> list[str]:
        """The hosts the dataset's jobs were refused that the csub policy still lacks: its next start lists them
        for a person to approve. All of them when the policy cannot be read."""
        refused = (self.ledger.kv("refused_hosts") or {}).get(record, [])
        if not refused or self.hosts is None:
            return refused
        try:
            return sorted(self.hosts.missing(self.hosts.group({record: refused}), self.ledger.kv("dataset_hosts")))
        except (OSError, ValueError):
            return refused

    def _gaps(self, record: str) -> list[str]:
        """What the dataset's record, as the newest synced catalog that holds it has it, lacks for the dataset to
        be started (`start_gaps`); none for a record no synced catalog holds. Read from the catalog on every call,
        so a record fixed there can be started after the next catalog sync and nothing has to be cleared here."""
        found = self._catalog_record(record)
        return start_gaps(found) if found is not None else []

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
        repairs = self._repairs()
        subjects = [self._subject_view(row, holds, repairs) for row in self.ledger.find_subjects(kind=kind)]
        if kind == "buffer":
            serving = self._served_versions()
            subjects = [{**view, "serving": serving.get(view["number"])} for view in subjects]
        if (owner := query.get("buffer")) is not None:  # the requests a buffer's Prepare step filed
            if not owner.isdecimal():
                raise HttpError(422, f"buffer must be a buffer's number, got {owner!r}")
            subjects = [view for view in subjects if (view.get("data") or {}).get("buffer") == int(owner)]
        if page is None:
            return 200, {"subjects": subjects}, {}
        rows, total = paged(subjects, page, SUBJECT_SORTS, _subject_haystack)
        return 200, {"subjects": rows, "total": total}, {}

    def _served_versions(self) -> dict[int, str]:
        """A buffer's number -> the id of the version consumers draw from now: its newest round's buffer whose
        latest event is one of `SERVING`. That id is what a training run names."""
        serving: dict[int, str] = {}
        for buffer in sorted(self.ledger.buffers(), key=lambda row: _round_of(row["id"])):
            number = _round_of(buffer["id"])[0]
            if number and (buffer.get("latest_event") or {}).get("kind") in SERVING:
                serving[number] = buffer["id"]
        return serving

    def _by_status(self, *, query: dict[str, str], body: bytes) -> Response:
        """The subjects of every kind with one status (`status`; `needs human` when none is named), which is
        Overview's lists (spec overview §5). Under `needs human` also a buffer round whose dataset waits for
        its files (`drops`: the round is working, but only a person can copy them). `kind` narrows the list to
        one kind of subject; `kinds` counts every kind's share of the list, `counts` every kind's subjects by
        status. `failed`: the actions of the last day that did not reach Temporal, each with its subject, for
        the Retry."""
        status = query.get("status", "needs human")
        if status not in STATUSES:
            raise HttpError(422, f"status must be one of {list(STATUSES)}, got {status!r}")
        holds, repairs = self.ledger.shelved(), self._repairs()
        waits = status == "needs human"
        counts: dict[str, dict[str, int]] = {kind: {} for kind in SUBJECT_RULES}
        views = []
        # ponytail: reads every subject; a status column and an index when there are tens of thousands
        for row in self.ledger.find_subjects():
            # the status needs no view: one is built only for a subject this list may hold
            hold = holds.get((row["kind"], row["number"]))
            repair = self._repair_of(row, repairs.cases.get((row["kind"], row["number"])))
            is_now = status_of(SUBJECT_RULES[row["kind"]][row["stage"]][0], hold is not None, repair, repairs.acting)
            counts[row["kind"]][is_now] = counts[row["kind"]].get(is_now, 0) + 1
            if is_now == status or (waits and row["kind"] == "buffer"):  # a buffer may wait for files while working
                views.append(self._subject_view(row, holds, repairs))
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
            failed.append({**action, "subject": self._subject_view(found[0], holds, repairs) if found else None})
        needs = [view for view in views if view["status"] == status or (waits and view.get("drops"))]
        kinds = {kind: sum(1 for view in needs if view["kind"] == kind) for kind in SUBJECT_RULES}
        if (kind := query.get("kind")) is not None:
            if kind not in SUBJECT_RULES:
                raise HttpError(422, f"kind must be one of {list(SUBJECT_RULES)}, got {kind!r}")
            needs = [view for view in needs if view["kind"] == kind]
        page = self._page_of(query, SUBJECT_SORTS)
        out = {"failed": failed, "kinds": kinds, "counts": counts}
        if page is None:
            return 200, {"subjects": needs, **out}, {}
        rows, total = paged(needs, page, SUBJECT_SORTS, _subject_haystack)
        return 200, {"subjects": rows, "total": total, **out}, {}

    def _lookup(self, *, query: dict[str, str], body: bytes) -> Response:
        """From what a person has in hand to the subjects: an issue number (`234`, `#234`) or a subject's own
        number in every kind, an issue URL in the kinds of its repo, and always an exact key or a buffer spec's name."""
        text = query.get("q", "").strip()
        if not text:
            raise HttpError(422, "q is required: an issue number, an issue URL or a subject's key")
        rows = self.ledger.find_subjects(key=text)
        rows += [row for row in self.ledger.find_subjects(kind="buffer") if row["data"].get("name") == text]
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
            kind,
            int(number),
            body=parsed.body,
            marker=parsed.marker,
            round_id=parsed.round,
            stage=parsed.stage,
            data=parsed.data,
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
        or a buffer's planned pool need (host -> the record ids that need it). A dataset needs its record's hosts
        and those its jobs were refused. A named record no synced catalog
        holds is a 404; a request's contributes nothing (the worker refuses it). A buffer's host is one its
        published binding names that its record's synced catalog entry also names: a binding lies under a root a
        job can mount read-write."""
        if self.hosts is None:
            raise HttpError(503, "this service has no csub policy to check hosts against")
        if prep is not None:
            request = self._request_records(str(prep))
        refused = self.ledger.kv("refused_hosts") or {}
        if request is not None:
            return self.hosts.for_records(self._catalog_records(request), refused)
        if records is not None:
            if missing := [record for record in records if self._catalog_record(record) is None]:
                raise HttpError(404, f"{missing[0]!r} is not a record of a synced catalog")
            return self.hosts.for_records(self._catalog_records(records), refused)
        if spec is None:
            raise HttpError(422, "give records, prep or spec")
        subject = self.ledger.subject("buffer", spec)
        if subject is None:
            raise HttpError(404, f"no buffer {spec!r}")
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
            absent = self.hosts.missing(needed, self.ledger.kv("dataset_hosts"))
        except (OSError, ValueError):  # a TOMLDecodeError is a ValueError
            raise HttpError(503, "the generated csub policy cannot be read: `mia up` regenerates it") from None
        # `per_dataset`: approving it lets these datasets' jobs reach it and no other dataset's
        scope = {RAW_GITHUB: {"per_dataset": True}}
        rows = [{"host": host, "records": ids, **scope.get(host, {})} for host, ids in absent.items()]
        return 200, {"hosts": rows}, {}

    def _dataset_action(self, record: str, *, query: dict[str, str], body: bytes, headers: dict[str, str]) -> Response:
        parsed = _parse(ActionBody, body)
        actor = self._actor(headers, parsed)
        runs = parsed.action in ("go", "resume")  # the actions that run a round: a go from scratch, a resume on
        if runs and (gaps := self._gaps(record)):  # before a subject is created for it
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
        fields = (("note", parsed.note), ("root", parsed.root), ("set", parsed.set))
        payload = {name: value for name, value in fields if value}
        # a reject of the saved set starts the rework's jobs, as a go starts a round's (spec judge-decides §5.2)
        starts = runs or (parsed.action == "reject" and subject["stage"] == "done")
        needed = (partial(self._needed, records=[record])) if starts else None
        return self._act_on(subject, parsed, actor, payload, needed)

    # --- buffer specs and preparation requests: created and run from the page (spec §4.4, §8.1) ---

    @staticmethod
    def _spec_name(text: str) -> str:
        try:
            return load_spec_text(text).name
        except SpecError as error:
            raise HttpError(422, f"not a buffer's definition: {error}") from None

    def _new_spec(self, text: str) -> dict:
        """A new buffer subject holding this text. Its name is the spec's and need not be unique: the first
        subject of a name has it as its key, a later one `buffer-<number>`."""
        name = self._spec_name(text)
        # ponytail: two posts of one new name at the same moment share a subject; a lock here if that ever happens
        key = None if self.ledger.subject("buffer", name) is not None else name
        subject = self.ledger.create_subject("buffer", key, data={"spec": text, "name": name})
        self.changes.announce("buffer", subject["number"])
        return subject

    def _put_spec(self, *, query: dict[str, str], body: bytes, headers: dict[str, str]) -> Response:
        parsed = _parse(SpecBody, body)
        self._spec_name(parsed.spec)  # a 422 for what is not a buffer spec, before anything is saved
        if parsed.action is None:
            if parsed.approve_hosts:  # spec host-approval §5: only the four start actions carry hosts
                raise HttpError(422, "approve_hosts goes with an action that starts jobs")
            return 200, {"subject": self._subject_view(self._new_spec(parsed.spec))}, {}
        go = ActionBody(
            action="go",
            key=parsed.key,
            spec=parsed.spec,
            plan_first=parsed.plan_first,
            approve_hosts=parsed.approve_hosts,
        )
        seen = self.ledger.action_by_key(parsed.key) if parsed.key else None
        if seen is not None and seen["kind"] == "buffer":  # the same click again: the spec it made, not another
            return self._buffer_action(self._numbered("buffer", str(seen["number"]))["key"], go, headers)
        self._check_hosts(go, None, "buffer", None)  # a go: no `needed`; refused before the spec is saved
        return self._buffer_action(self._new_spec(parsed.spec)["key"], go, headers)

    def _parse_spec(self, *, query: dict[str, str], body: bytes) -> Response:
        """The spec text as the service reads it, every default filled in: what the designer's controls are set
        from when a spec is cloned or edited."""
        try:
            spec = load_spec_text(_parse(PlanBody, body).spec)
        except SpecError as error:
            raise HttpError(422, str(error)) from None
        return 200, {"spec": spec.model_dump(mode="json", by_alias=True, exclude={"warnings"})}, {}

    def _spec_action(self, name: str, *, query: dict[str, str], body: bytes, headers: dict[str, str]) -> Response:
        return self._buffer_action(name, _parse(ActionBody, body), headers)

    def _buffer_action(self, key: str, parsed: ActionBody, headers: dict[str, str]) -> Response:
        """An action on the buffer with this key. A `go` runs the text the buffer holds, or the `spec` sent with
        it, which is stored (with the name in it) once the stage takes the go. A serving buffer (`ready`, `idle`)
        is fixed: a go there runs the stored text and refuses another. An `edit` stores a new text and runs
        nothing (`_edit`)."""
        actor = self._actor(headers, parsed)
        subject = self.ledger.subject("buffer", key)
        if subject is None:
            raise HttpError(404, f"no buffer {key!r}")
        if parsed.action == "edit":
            return self._edit(subject, parsed, actor)
        payload: dict = {}
        if parsed.action == "go":
            stored = subject["data"].get("spec")
            text = parsed.spec if parsed.spec is not None else stored
            if not text:
                raise HttpError(422, f"buffer {key!r} has no definition to run: send one as `spec`")
            name = self._spec_name(text)
            if text != stored and subject["stage"] in ("ready", "idle"):
                raise HttpError(409, f"buffer {key!r} is serving and does not change: clone it")
            self._accepts(subject, parsed)
            self._check_hosts(parsed, None, "buffer", subject["number"])  # a go: no `needed`
            if text != stored:  # only once the stage takes a go: what is stored is what ran last
                edited = self.ledger.set_subject_data(
                    "buffer", subject["number"], {**subject["data"], "spec": text, "name": name}
                )
                assert edited is not None  # it was just read
                subject = edited
                self.changes.announce("buffer", subject["number"])
            payload = {"spec": text, **({"plan_first": True} if parsed.plan_first else {})}
        assert subject is not None
        needed = None
        if parsed.action == "approve" and subject["stage"] == "plan-review":
            needed = partial(self._needed, spec=key)
        if parsed.action == "revoke":
            if not parsed.records:
                raise HttpError(422, "a revoke names the records that leave the buffer: `records`")
            payload = {"records": parsed.records}
        if parsed.action == "dropped":
            payload = {"record": self._dropped_record(subject, parsed)}
        return self._act_on(subject, parsed, actor, payload, needed)

    def _edit(self, subject: dict, parsed: ActionBody, actor: str) -> Response:
        """An `edit`: the buffer's definition becomes the `spec` sent with it, and its name the name in that text.
        The stage table says where (a buffer that rests); the action is recorded and carried out here, with its
        timeline entry, and no workflow hears of it. A key seen before is the same action again."""
        number = subject["number"]
        self._accepts(subject, parsed)
        if parsed.spec is None:
            raise HttpError(422, "an edit carries the buffer's new definition: `spec`")
        name = self._spec_name(parsed.spec)  # 422 for what is not a buffer spec, before anything is recorded
        self._check_hosts(parsed, None, "buffer", number)  # 422: nothing here starts a job
        try:
            action, _ = self.ledger.add_action(
                "buffer", number, "edit", actor, key=parsed.key, payload={"spec": parsed.spec}
            )
        except ValueError as error:
            raise HttpError(409, str(error)) from None
        if action["state"] != "sent":
            text = action["payload"]["spec"]
            changed = {**subject["data"], "spec": text, "name": name}
            self.ledger.set_subject_data("buffer", number, changed, revise=True)
            action = self.ledger.settle_action(action["id"], "sent", entry=_action_entry(action)) or action
            self._changed("buffer", number)
        return 202, action, {}

    def _dropped_record(self, subject: dict, parsed: ActionBody) -> str:
        """The record of a `dropped`, once its files look complete. The workflow takes any non-empty file and
        records its hash, so a click during a copy would record a truncated file as good: refuse while anything
        under `incoming/` changed in the last minute."""
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
        if parsed.buffer is not None and not self.ledger.find_subjects(kind="buffer", number=parsed.buffer):
            raise HttpError(422, f"no buffer #{parsed.buffer} for this request to belong to")
        go = ActionBody(action="go", key=parsed.key, spec=parsed.spec, approve_hosts=parsed.approve_hosts)
        needed = partial(self._needed, request=records)
        repeat = self.ledger.action_by_key(parsed.key) if parsed.key else None
        if repeat is not None and repeat["kind"] == "prep":  # a double click: the request filed by the first one
            subject = self.ledger.find_subjects(kind="prep", number=repeat["number"])[0]
        else:
            self._check_hosts(go, needed, "prep", None)
            owner = {} if parsed.buffer is None else {"buffer": parsed.buffer}
            subject = self.ledger.create_subject("prep", None, data={"spec": parsed.spec, **owner})
        return self._act_on(subject, go, self._actor(headers, go), {"spec": parsed.spec}, needed)

    def _prep_action(self, number: str, *, query: dict[str, str], body: bytes, headers: dict[str, str]) -> Response:
        parsed = _parse(ActionBody, body)
        subject = self._numbered("prep", number)
        payload: dict = {}
        needed = None
        if parsed.action in ("go", "resume"):  # both run the request: its datasets from scratch, or resumed
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
        events and set the stage a stopped subject rests at (`retired`, `done` for a request, `cancelled` for a
        dataset). The entry says what was done and what is left running."""
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
            stage={"buffer": "retired", "prep": "done", "dataset": "cancelled"}[kind],
        )

    def _accepts(self, subject: dict, parsed: ActionBody) -> None:
        """Steps 1 and 2 of spec §5: the subject's stage accepts the action (409 with what it does take), and a
        verdict names the revision that was looked at (409 when the subject has moved on; so does any other
        action that names one). A key seen before is
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
        phase, accepted = self._rules(subject, hold, self._gaps(key) if kind == "dataset" else [])
        if parsed.action not in accepted:
            takes = ", ".join(f"`{name}`" for name in sorted(accepted)) or "nothing"
            standing = "shelved" if hold else phase
            raise HttpError(
                409, f"{kind} {key!r} is at `{stage}` ({standing}): it takes {takes}, not `{parsed.action}`"
            )
        if parsed.action in ("approve", "reject") and parsed.revision is None:
            raise HttpError(422, f"`{parsed.action}` must carry the revision of what was looked at")
        if parsed.revision is not None and parsed.revision != subject["revision"]:
            raise HttpError(409, f"{kind} {key!r} changed since it was shown (reload it): nothing was sent")
        if kind == "dataset" and parsed.action in ("approve", "reject"):
            self._names_its_set(subject, parsed)

    @staticmethod
    def _names_its_set(subject: dict, parsed: ActionBody) -> None:
        """A dataset's verdict names the set it is about, and only the set the stage lets it be about (spec
        judge-decides §5.4): at `review` the set under review, at `done` (a reject) the saved set. A subject
        whose round wrote no sets (a run from before them) takes a verdict that names none."""
        sets = (subject.get("data") or {}).get("sets")
        if sets is None:
            return
        if not parsed.set:
            raise HttpError(422, "a verdict names the set it is about: `set`")
        if subject["stage"] == "review":
            allowed, refusal = sets.get("review"), "that set is not the one under review"
        else:
            allowed, refusal = (sets.get("saved") or {}).get("set"), "that set is not the saved one"
        if parsed.set != allowed:
            raise HttpError(409, f"{refusal}: nothing was sent")

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

    def _approve_for(self, hosts: list[str], needed: Callable[[], Needed] | None) -> list[str]:
        """Write down the approvals that hold for one dataset at a time: `raw.githubusercontent.com`, for each
        record of the run that needs it. The policy allows that host to every job once it is approved anywhere;
        a worker grants it only to jobs of a dataset named here. Returns the hosts newly approved for a record."""
        if RAW_GITHUB not in hosts or needed is None:
            return []
        with self._lock:
            approved = self.ledger.kv("dataset_hosts") or {}
            new = [record for record in needed().get(RAW_GITHUB, []) if RAW_GITHUB not in approved.get(record, [])]
            for record in new:
                approved[record] = sorted({*approved.get(record, []), RAW_GITHUB})
            self.ledger.put_kv("dataset_hosts", approved)
        return [RAW_GITHUB] if new else []

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
            added = list(dict.fromkeys([*added, *self._approve_for(parsed.approve_hosts, needed)]))
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
        event = LabelEvent(signal, str(action["id"]), action["actor"], action["at"], payload.get("set") or "")
        running = True
        self._asked[(kind, number)] = (action, time.monotonic())  # before the send: the answer may beat this request
        try:
            if parsed.action == "cancel":  # only a running round can be cancelled: nothing is started for it
                running = self.dispatcher.signal_running(kind, number, "cancel", event)
            elif parsed.action == "dropped":  # the workflow's plain `dropped(record)`
                if not self.dispatcher.signal_running(kind, number, "dropped", payload["record"]):
                    raise DispatchError("no workflow is running for it")
            elif parsed.action == "stop":  # a terminate needs no replay: it also ends a run nondeterminism froze
                reason = f"stopped from the page (action {action['id']})"
                running = self.dispatcher.terminate(kind, number, reason, parsed.run)
                if parsed.run and not running and self.dispatcher.run_status(kind, number) == "RUNNING":
                    # the run it names is over and a later one is open: that one's round is not this stop's
                    self._asked.pop((kind, number), None)
                    detail = f"run {parsed.run} is over and a later run is open: nothing was stopped"
                    refused = self.ledger.settle_action(action["id"], "refused", detail)
                    self.changes.announce(kind, number)
                    return 409, {"error": detail, "action": refused}, {}
            elif parsed.action == "recheck":  # the request's plain `recheck()`: it looks at its datasets again
                if not self.dispatcher.signal_running(kind, number, "recheck"):
                    raise DispatchError("no workflow is running for it")
            else:
                # a dataset's go and its resume are its workflow's `restart`: the round runs in a new run, on
                # today's code, and the event's kind says which of the two it is
                # a request's resume is its workflow's `go`: its round reads from the action how it runs
                restarts = kind == "dataset" and signal in ("go", "resume")
                name = "restart" if restarts else "go" if signal == "resume" else signal
                self.dispatcher.send(kind, number, name, event)
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
        the transaction that puts it on the timeline. A key seen before is the same action again. A shelve at a
        stage that takes `cancel` (a round waits there for a verdict) sends that cancel first, as an action of
        its own; one that is not delivered shelves nothing."""
        kind, number = subject["kind"], subject["number"]
        reason = parsed.note.strip()
        if parsed.action == "shelve" and not reason:
            raise HttpError(422, "a shelve says why the subject can wait: `note`")
        self._check_hosts(parsed, None, kind, number)  # 422: nothing here starts a job
        again = parsed.key is not None and self.ledger.action_by_key(parsed.key) is not None  # its cancel went
        if parsed.action == "shelve" and not again and "cancel" in SUBJECT_RULES[kind][subject["stage"]][1]:
            cancel = ActionBody(action="cancel", key=f"{parsed.key}:cancel" if parsed.key else None)
            answer = self._act_on(subject, cancel, actor, {})
            if answer[0] != 202:
                return answer
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
        """The Temporal status of the subject's latest run; with `?detail=1` what the cleaner reads of it (its
        run id, a workflow task that keeps failing, its pending activities)."""
        self._numbered(kind, number)
        if self.dispatcher is None:
            raise HttpError(503, "this service has no way to Temporal")
        try:
            if query.get("detail"):
                return 200, self.dispatcher.run(kind, int(number)) or {"status": None}, {}
            return 200, {"status": self.dispatcher.run_status(kind, int(number))}, {}
        except DispatchError as error:
            raise HttpError(503, str(error)) from None

    def _runs(self, *, query: dict[str, str], body: bytes) -> Response:
        """Every open run of every namespace the services use."""
        if self.dispatcher is None:
            raise HttpError(503, "this service has no way to Temporal")
        try:
            return 200, {"runs": self.dispatcher.open_runs()}, {}
        except DispatchError as error:
            raise HttpError(503, str(error)) from None

    def _terminate_run(self, *, query: dict[str, str], body: bytes) -> Response:
        """End one run by its ids: a run no worker can execute has no subject to stop it through."""
        parsed = _parse(TerminateBody, body)
        if self.dispatcher is None:
            raise HttpError(503, "this service has no way to Temporal")
        try:
            done = self.dispatcher.terminate_run(parsed.namespace, parsed.id, parsed.run_id, parsed.reason)
        except DispatchError as error:
            raise HttpError(503, str(error)) from None
        return 200, {"terminated": done}, {}

    def _schedules(self, *, query: dict[str, str], body: bytes) -> Response:
        """Every Temporal schedule and when it last fired; none when Temporal cannot be asked."""
        return 200, {"schedules": self.dispatcher.schedules() if self.dispatcher is not None else []}, {}

    # --- the cleaner: its cases, its last sweep and the commit each worker started on (spec cleaner §4.8) ---

    def _open_case(self, *, query: dict[str, str], body: bytes) -> Response:
        parsed = _parse(CaseBody, body)
        case = self.ledger.open_case(parsed.kind, parsed.number, parsed.key, parsed.rule, parsed.state, parsed.data)
        self._case_changed(case)
        return 201, case, {}

    def _update_case(self, case_id: str, *, query: dict[str, str], body: bytes) -> Response:
        parsed = _parse(CaseUpdate, body)
        case = self.ledger.update_case(int(case_id), state=parsed.state, rule=parsed.rule, data=parsed.data)
        if case is None:
            raise HttpError(404, f"no case {case_id}")
        self._case_changed(case)
        return 200, case, {}

    def _case_changed(self, case: dict) -> None:
        if case["kind"] in SUBJECT_RULES and case["number"] is not None:  # the subject's status may have changed
            self.changes.announce(case["kind"], case["number"])

    def _cases(self, *, query: dict[str, str], body: bytes) -> Response:
        """The cases, newest first: `state` (comma-separated; default: every state that is not over) and, with
        `days`, also every case updated within that many days."""
        states = tuple(part for part in query.get("state", ",".join(CASES_OPEN)).split(",") if part)
        if unknown := [state for state in states if state not in CASE_STATES]:
            raise HttpError(422, f"state must be among {list(CASE_STATES)}, got {unknown}")
        try:
            since = time.time() - float(query["days"]) * 86400 if "days" in query else None
        except ValueError:
            raise HttpError(422, "days must be a number") from None
        cases = self.ledger.cases(states, since)
        for case in cases:
            case["jobs"] = self._case_jobs(case)
        return 200, {"cases": cases}, {}

    def _case_jobs(self, case: dict) -> list[str]:
        """The addresses of the job records a case is about, as they are on disk now: the jobs of the round it
        saw, autodev's jobs on the snippets issue it is about (a dataset's `party`, a left-over issue's key), then
        the cleaner's own jobs for it (its diagnoses). Only what `GET /jobs/<address>` serves."""
        data = case["data"]
        round_id, issue = data.get("round"), (data.get("party") or {}).get("issue")
        if case["kind"] == "snippets" and (named := re.fullmatch(r"issue-(\d+)", case["key"])):
            issue = int(named.group(1))
        places: list[tuple[Path | None, str, str]] = []  # (work root, directory of addresses, the records in it)
        if case["number"] is not None and isinstance(round_id, str) and UI_NAME_RE.fullmatch(round_id):
            places.append((self.work_root, f"issue-{case['number']}/{round_id}", "*.json.gz"))
        if isinstance(issue, int) and not isinstance(issue, bool):  # one directory per run of autodev on the issue
            places.append((self.snippets_work_root, f"{self.snippets_slug}/issue-{issue}", "*/*.json.gz"))
        places.append((self.work_root, f"cleaner/case-{case['id']}", "*.json.gz"))
        found = []
        for root, directory, pattern in places:
            if root is None:
                continue
            records = root / RECORDS_DIR / directory
            for path in sorted(records.glob(pattern)):
                where = path.relative_to(records).as_posix().removesuffix(".json.gz")
                plain = all(UI_NAME_RE.fullmatch(segment) for segment in where.split("/"))
                if plain and path.is_file() and not path.is_symlink() and not path.parent.is_symlink():
                    found.append(f"{directory}/{where}")
        return found

    def _put_cleaner(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, self._put_report("cleaner", body), {}

    def _cleaner(self, *, query: dict[str, str], body: bytes) -> Response:
        """The cleaner's last sweep, without what a restart since has settled: a worker that started on other
        code after the sweep is not the one the sweep found on old code, so that finding is left out. Whether
        the new one owes a restart too is the next sweep's to say."""
        report = self.ledger.kv("cleaner")
        if report is None:
            return 200, *JSON_NULL
        swept, workers = report.get("at") or 0, self.ledger.kv("workers") or {}
        restarted = {f"restart {service}" for service, row in workers.items() if (row.get("since") or 0) > swept}
        findings = [finding for finding in report.get("findings") or [] if finding.get("id") not in restarted]
        return 200, {**report, "findings": findings}, {}

    def _put_report(self, name: str, body: bytes) -> dict:
        """Store a scheduled pass's last report whole under `name` (`at` added when absent)."""
        try:
            report = json.loads(body)
        except ValueError as error:
            raise HttpError(422, f"invalid JSON: {error}") from None
        if not isinstance(report, dict):
            raise HttpError(422, f"a {name} report is a JSON object")
        report.setdefault("at", time.time())
        self.ledger.put_kv(name, report)
        return report

    def _put_worker(self, service: str, *, query: dict[str, str], body: bytes) -> Response:
        """A worker says what it started on; the cleaner compares its code with the checkout's."""
        parsed = _parse(WorkerBody, body)
        now = time.time()
        with self._lock:
            workers = self.ledger.kv("workers") or {}
            known = workers.get(service) or {}
            # `since`: when the worker first started on this code; a restart on the same code keeps it
            since = known.get("since", now) if known.get("code") == parsed.code else now
            workers[service] = {"commit": parsed.commit, "code": parsed.code, "at": now, "since": since}
            self.ledger.put_kv("workers", workers)
        return 200, workers[service], {}

    def _workers(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, {"workers": self.ledger.kv("workers") or {}}, {}

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
        roots = [root for root in (self.work_root, self.snippets_work_root) if root is not None]
        if not roots:
            raise HttpError(503, "the service has no work_root")
        if not all(UI_NAME_RE.fullmatch(segment) for segment in where.split("/")):
            raise HttpError(404, "no record for this job")
        data = None
        for root in roots:  # the rounds' records, then those of autodev's jobs on the snippets repo
            data = read_untrusted(root / RECORDS_DIR, f"{where}.json.gz", JOB_RECORD_LIMIT + 1, binary=True)
            if data is not None:
                break
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
            buffer["spec"] = specs.get(number)  # its spec's page; a buffer whose spec has no subject has none
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
        # the page's row names its dataset: its issue, its crops, the staged artifact this buffer draws from, what
        # the plan gave it (size upstream, crops per split, notes), and what its raw cache and its pyramid hold
        datasets = {row["id"]: row for row in self.ledger.datasets()}
        staged = {
            asset["data"].get("record"): asset
            for asset in self.ledger.assets("staged")
            if asset["data"] and buffer in asset["refs"] and asset["data"].get("levels_key")
        }
        raw = {asset["key"]: asset["data"] or {} for asset in self.ledger.assets("raw")}
        issues = self._dataset_issues()
        plan, revoked = self._plan_rows(buffer), self._revoked(buffer)
        for row in board:
            record = row["record"]
            known, pyramid, planned = datasets.get(record) or {}, staged.get(record), plan.get(record) or {}
            row.update({name: known.get(name) for name in ("folder", "previews", "crops")})
            row["issue"] = issues.get(record)
            row["staged"] = f"{pyramid['data']['levels_key']}/{record}/{pyramid['key']}" if pyramid else None
            row["staged_bytes"] = pyramid["data"].get("bytes") if pyramid else None
            row["raw_bytes"] = (raw.get(record) or {}).get("bytes")
            row.update(
                bytes=planned.get("bytes"), planned=planned.get("planned") or {}, notes=planned.get("notes") or []
            )
            row["revoked"] = row["status"] != "servable" and record in revoked
        if query.get("included"):  # what the version serves, and what a person took out of it
            board = [row for row in board if row["status"] == "servable" or row["revoked"]]
        rows, total = paged(
            board, page, BOARD_SORTS, lambda row: (row["record"], row["status"], row["reason"], row["job"])
        )
        return 200, {"buffer": buffer, "datasets": rows, "total": total}, {}

    def _plan_rows(self, buffer: str) -> dict[str, dict]:
        """The plan's row of each dataset of the buffer's pool (a buffer's id is its round's); none when the plan
        cannot be read."""
        if self.hosts is None:
            return {}
        try:
            return self.hosts.plan_rows(buffer)
        except ValueError:
            return {}

    def _revoked(self, buffer: str) -> set[str]:
        """The records a person's `revoke` named since this version was created."""
        # ponytail: a revocation that reaches the buffer from a dataset's reject (the keeper's signal) names no
        # record here and its row reads `failed`; a board status of its own is the upgrade
        events = self.ledger.events(buffer)
        if not events or events[0]["issue"] is None:
            return set()
        return {
            str(record)
            for action in self.ledger.actions("buffer", events[0]["issue"])
            if action["action"] == "revoke" and datetime.fromisoformat(action["at"]).timestamp() >= events[0]["at"]
            for record in action["payload"].get("records", [])
        }

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

    def _peek(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        """One crop of a split as the tensorizer returns it, for the page to show: the descriptor a consumer would
        have been handed, the pictures of every level and label layer, and the dataset's facts. The buffer keeps
        no trace of it: the draw has a generator of its own, no consumer is registered, nothing is recorded or
        counted, and a `val` or `test` crop is one of the split's candidates, not a row of its frozen list."""
        config = self._serving(buffer)
        split = query.get("split", "train")
        if split not in PEEK_SPLITS:
            raise HttpError(422, f"split must be train, val or test, got {split!r}")
        if self.peeker is None:
            raise HttpError(503, "no harness configured")
        volumes = self.ledger.live_volumes(buffer)
        descriptor = draw(
            random.Random(), volumes, split, patch=config.patch, levels=config.levels, channels=config.channels
        )
        if descriptor is None:
            return 204, None, {}
        volume = next(volume for volume in volumes if volume.id == descriptor["volume"])
        # what `BufferDataset._tensorize` hands the tensorizer
        params = {**config.tensorizer_params, "channels": descriptor["channels"], "stats": descriptor["normalize"]}
        if not self._peek_lock.acquire(timeout=PEEK_WAIT_S):
            raise HttpError(503, "another peek is being drawn; try again")
        try:
            pictures = self.peeker.render(volume.path, descriptor, config.tensorizer, params, sorted(volume.labels))
        except PeekFailed as error:
            raise HttpError(502, str(error)) from None
        finally:
            self._peek_lock.release()
        pixel_nm = {level.factor: level.scale_nm["x"] for level in volume.levels}
        return (
            200,
            {
                "split": split,
                "descriptor": descriptor,
                "dataset": self._peek_dataset(volume.record),
                "samples": len(volume.samples[split]) if volume.samples else None,
                "levels": [
                    {"factor": factor, "pixel_nm": pixel_nm.get(factor), "img": base64.b64encode(picture).decode()}
                    for factor, picture in zip(descriptor["levels"], pictures["img"], strict=True)
                ],
                "labels": [
                    {
                        "key": key,
                        "levels": [base64.b64encode(picture).decode() for picture in layer],
                    }
                    for key, layer in pictures["labels"].items()
                ],
            },
            {"Cache-Control": "no-store"},
        )

    def _peek_dataset(self, record_id: str) -> dict:
        """What a peek says about its crop's dataset, from the newest catalog's record; the id alone when the
        record is in no synced catalog or cannot be read."""
        ref = self.ledger.latest_catalog()
        found = self.ledger.record(ref, record_id) if ref else None
        if found is None:
            return {"id": record_id}
        try:
            known = facts(*found)
        except (TypeError, ValueError, AttributeError):
            return {"id": record_id}
        return {
            "id": record_id,
            "title": str(found[1].get("title") or ""),
            "modality": known.modality_group,
            "dims": known.dims,
            "organisms": known.organisms,
            "annotation_types": known.annotation_types,
        }

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
        return 200, self._put_report("keeper", body), {}

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
    service = BufferService(
        ledger,
        token=token,
        storage_root=cfg.paths.storage_root,
        work_root=cfg.paths.work_root,
        snippets_work_root=cfg.snippets_work_root,
        snippets_repo=cfg.github.snippets_repo,
        raw_root=cfg.paths.raw_root,
        staged_root=cfg.paths.staged_root,
        locks=LockDir(cfg.paths.local_root / "locks"),
        archive_dir=cfg.paths.state_dir / "archive",
        meta=page_meta(cfg),
        ui_dir=Path(__file__).parent / "ui",
        dispatcher=dispatcher_for(cfg),
        hosts=check,
        peeker=Peeker.from_config(cfg),
    )
    service.kick_unmirrored()  # what a crash, or Temporal being away, left unposted
    # a request whose datasets settled with nobody telling it; beside the start, which Temporal being away must not hold
    threading.Thread(target=service.recheck_resting, daemon=True).start()
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
