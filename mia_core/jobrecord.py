"""The record every cluster job leaves: what was sent, what the cluster answered, how the job ended and what it
used, and for an agent its prompt and its conversation with every tool output dropped.

One gzipped JSON file per job, `<work_root>/_jobs/<workspace relative to work_root>.json.gz`, written by the job
runner in two halves (at submit, at the end of the wait) and by nothing else. No job mounts `_jobs`. Writing is
best effort: a record that cannot be written is logged and never fails a submit or a wait."""

from __future__ import annotations

import dataclasses
import gzip
import json
import logging
import os
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .untrusted import SECRET_RE, read_untrusted, write_atomic

log = logging.getLogger(__name__)

RECORDS_DIR = "_jobs"
SCHEMA = 1
LOG_BYTES = 64 << 10  # the end of a job's stdout and of its stderr
PROMPT_BYTES = 1 << 20
# ponytail: an agent's stream is read whole (a judge's holds every image it looked at); a line-by-line read when
# streams outgrow this.
STREAM_BYTES = 256 << 20
INPUT_CHARS = 2000  # a string of a tool call's input is cut here
JOB_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,64}")
USAGE_FIELDS = ("max_mem", "avg_mem", "memlimit", "cpu_used", "run_time", "slots")
STREAM_FLAGS = "--output-format stream-json --verbose"  # how an agent command prints its whole conversation
TRANSCRIPT_FILE = "claude.jsonl"  # where an agent job's script keeps that stream, in its workspace


def keep_result(result_file: str, stream_file: str = TRANSCRIPT_FILE) -> str:
    """The shell lines that follow `claude -p … --output-format stream-json … > <stream_file>` in a job script:
    the stream's last line is the result object `--output-format json` prints, so it is copied to `result_file`, where the result
    is read from; the agent's exit status is left as `$?` for the script's own `rc=$?`."""
    return f'rc=$?\ntail -n 1 {stream_file} > {result_file}\n(exit "$rc")'


def address(work_root: Path, workspace: str | Path) -> str | None:
    """A record's address: its job's workspace relative to `work_root`. None for a workspace that is not under
    `work_root`, is `work_root` itself or lies in the records directory: such a job leaves no record."""
    try:
        relative = Path(os.path.realpath(workspace)).relative_to(os.path.realpath(work_root))
    except ValueError:
        return None
    if not relative.parts or relative.parts[0] == RECORDS_DIR:
        return None
    return relative.as_posix()


def record_path(work_root: Path, where: str) -> Path:
    """The file of the record at address `where`."""
    return Path(work_root) / RECORDS_DIR / f"{where}.json.gz"


def job_link(page_url: str, where: str) -> str:
    """The page's view of the record at address `where`."""
    return f"{page_url.rstrip('/')}/#job/{where}"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _scrub(value: Any) -> Any:
    """`value` with everything that looks like a credential replaced, in every string at any depth."""
    if isinstance(value, str):
        return SECRET_RE.sub("[redacted]", value)
    if isinstance(value, dict):
        return {key: _scrub(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_scrub(item) for item in value]
    return value


def _plain(value: Any) -> dict | None:
    """csub's answer to a submit as a plain dict, whatever object the client returned."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, dict):
        return dict(value)
    return dict(vars(value)) if hasattr(value, "__dict__") else None


def _load(path: Path) -> dict:
    try:
        with gzip.open(path, "rt") as handle:
            record = json.load(handle)
    except (OSError, ValueError, EOFError):
        return {}
    return record if isinstance(record, dict) else {}


def _store(path: Path, record: dict) -> None:
    write_atomic(path, gzip.compress(json.dumps(_scrub(record), default=str).encode(), mtime=0))


def write_submitted(
    work_root: Path | None,
    workspace: str | Path,
    *,
    service: str,
    key: str,
    job_id: str,
    request: dict,
    submit: Any = None,
    prompt_file: str | None = None,
    transcript_file: str | None = None,
) -> None:
    """The first half, when a job is submitted: the request as it went to csub (its `env` as names only: a value
    can be an API key) and csub's answer. `prompt_file` and `transcript_file` name, relative to the workspace,
    an agent's prompt and its stream; the second half reads them."""
    try:
        where = address(work_root, workspace) if work_root else None
        if where is None or work_root is None:
            return
        sent = {name: value for name, value in request.items() if name not in ("env", "shell", "session", "cwd")}
        sent["env"] = sorted(request.get("env") or {})
        record = {
            "schema": SCHEMA,
            "service": service,
            "key": key,
            "job_id": str(job_id),
            "workspace": where,
            "submitted_at": _now(),
            "request": sent,
            "submit": _plain(submit),
            "prompt_file": prompt_file,
            "transcript_file": transcript_file,
        }
        _store(record_path(work_root, where), record)
    except Exception:  # best effort: a record never fails a submit
        log.warning("job record of %s (submit) was not written", workspace, exc_info=True)


def write_finished(work_root: Path | None, workspace: str | Path, *, job_id: str, outcome: dict) -> None:
    """The second half, when the wait saw the job end: how it ended, what LSF says it used, the ends of its
    stdout and stderr, and for an agent its prompt and reduced conversation. Without a first half (a job
    submitted before records existed) it writes what it has."""
    try:
        where = address(work_root, workspace) if work_root else None
        if where is None or work_root is None:
            return
        path, job_dir = record_path(work_root, where), Path(workspace)
        record = _load(path) or {"schema": SCHEMA, "job_id": str(job_id), "workspace": where}
        record["finished_at"], record["outcome"] = _now(), outcome
        if usage := lsf_usage(str(job_id)):
            record["usage"] = usage
        if JOB_ID_RE.fullmatch(str(job_id)):
            logs = f".csub/jobs/{job_id}"
            record["logs"] = {
                name: read_untrusted(job_dir, f"{logs}/{name}", LOG_BYTES, tail=True) or ""
                for name in ("stdout", "stderr")
            }
        if transcript := record.get("transcript_file"):
            agent = reduce_stream(read_untrusted(job_dir, str(transcript), STREAM_BYTES) or "")
            prompt = record.get("prompt_file")
            agent["prompt"] = (read_untrusted(job_dir, str(prompt), PROMPT_BYTES) or "") if prompt else ""
            record["agent"] = agent
        _store(path, record)
    except Exception:  # best effort: a record never fails a wait
        log.warning("job record of %s (end) was not written", workspace, exc_info=True)


def _cut(value: Any, depth: int = 0) -> Any:
    """A tool call's input with each string cut at INPUT_CHARS, saying how much was cut."""
    if isinstance(value, str):
        over = len(value) - INPUT_CHARS
        return value if over <= 0 else f"{value[:INPUT_CHARS]}… ({over} more characters)"
    if depth > 8:
        return "…"
    if isinstance(value, dict):
        return {str(key): _cut(item, depth + 1) for key, item in value.items()}
    if isinstance(value, list):
        return [_cut(item, depth + 1) for item in value]
    return value if isinstance(value, int | float | bool) or value is None else str(value)[:INPUT_CHARS]


def _blocks(message: Any) -> list[dict]:
    content = message.get("content") if isinstance(message, dict) else None
    return [block for block in content if isinstance(block, dict)] if isinstance(content, list) else []


def reduce_stream(text: str) -> dict:
    """An agent's `--output-format stream-json` output as the conversation worth keeping: every assistant text
    and thinking block whole, every tool call as its name and input, and no tool output (a failed call is
    marked). Hook, rate-limit and other bookkeeping lines are dropped. The stream is a job's output: a line
    that is not the JSON expected is skipped."""
    turns: list[dict] = []
    calls: dict[str, dict] = {}
    found: dict[str, Any] = {"model": "", "cost_usd": None, "num_turns": None, "duration_ms": None}
    for line in text.splitlines():
        try:
            event = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if not isinstance(event, dict):
            continue
        kind, sub = event.get("type"), bool(event.get("parent_tool_use_id"))
        if kind == "system" and event.get("subtype") == "init" and isinstance(event.get("model"), str):
            found["model"] = event["model"]
        elif kind == "assistant":
            for block in _blocks(event.get("message")):
                turn: dict[str, Any] | None = None
                if block.get("type") == "text" and block.get("text"):
                    turn = {"role": "assistant", "text": str(block["text"])}
                elif block.get("type") == "thinking" and block.get("thinking"):
                    turn = {"role": "thinking", "text": str(block["thinking"])}
                elif block.get("type") == "tool_use":
                    turn = {"role": "tool", "name": str(block.get("name")), "input": _cut(block.get("input"))}
                    turn["error"] = False
                    calls[str(block.get("id"))] = turn
                if turn is not None:
                    if sub:  # a sub-agent's turn, in the order it came
                        turn["sub"] = True
                    turns.append(turn)
        elif kind == "user":
            for block in _blocks(event.get("message")):
                if block.get("type") == "tool_result" and block.get("is_error"):
                    calls.get(str(block.get("tool_use_id")), {})["error"] = True
        elif kind == "result":
            cost, usage = event.get("total_cost_usd"), event.get("modelUsage")
            if isinstance(cost, int | float) and not isinstance(cost, bool):
                found["cost_usd"] = float(cost)
            for name in ("num_turns", "duration_ms"):
                if isinstance(event.get(name), int):
                    found[name] = event[name]
            if not found["model"] and isinstance(usage, dict):
                found["model"] = next((key for key in usage if isinstance(key, str)), "")
    return {**found, "turns": turns}


def lsf_usage(job_id: str) -> dict | None:
    """What LSF says a job used: peak and average memory, its memory limit, CPU and run time, slots. None when
    `bjobs` is not there, fails, or has forgotten the job."""
    # ponytail: asks LSF directly because csub's status carries no usage; csub reporting it is the upgrade.
    if not job_id.isdigit():
        return None
    try:
        done = subprocess.run(
            ["bjobs", "-o", " ".join(USAGE_FIELDS), "-json", job_id],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        row = json.loads(done.stdout)["RECORDS"][0]
        usage = {name: str(row[name.upper()]) for name in USAGE_FIELDS if row.get(name.upper()) not in (None, "", "-")}
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, IndexError, TypeError):
        return None
    return usage or None
