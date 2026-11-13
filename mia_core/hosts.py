"""mia's own csub policy: the operator's base policy plus the hosts approved from the page.

`approved-hosts.jsonl` (append only, written by buffer-api) lists the approvals; `broker.toml` is generated from
the base policy and that list, and published by a rename that is safe against other generations in this process
or another. Every generation checks where the policy directory is before it writes anything.
"""

from __future__ import annotations

import fnmatch
import ipaddress
import json
import os
import re
import tempfile
import threading
import tomllib
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

POLICY_NAME = "broker.toml"
APPROVALS_NAME = "approved-hosts.jsonl"
# an exact lowercase name with a dot, bounded as csub bounds a job's hosts (csub/protocol.py HOSTNAME_RE)
HOST_RE = re.compile(r"(?=.{1,253}$)[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+")

_approval_lock = threading.Lock()


class PolicyError(RuntimeError):
    """The policy cannot be generated or the approvals cannot be read; the message names the file or key."""


def is_ip_literal(host: str) -> bool:
    """Whether the host is an IPv4 or IPv6 address rather than a name: a canonical address, or a name whose last
    label is a number (decimal, or `0x` and hex digits), which resolvers read as IPv4 (`127.1`, `0x7f.1`)."""
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        pass
    return bool(re.fullmatch(r"[0-9]+|0x[0-9a-f]*", host.lower().rstrip(".").rsplit(".", 1)[-1]))


def missing_hosts(hosts: list[str], patterns: list[str]) -> list[str]:
    """Hosts no `csub probe` allowed_hosts pattern matches (csub/broker/resolve.py: check_hosts)."""
    return [
        host for host in hosts if not any(host == pattern or fnmatch.fnmatchcase(host, pattern) for pattern in patterns)
    ]


def approved_hosts(policy_dir: Path) -> list[str]:
    """The approved hosts in approval order, each once; empty when there is no approvals file."""
    path = policy_dir / APPROVALS_NAME
    if not path.exists():
        return []
    hosts: list[str] = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            host = record["host"]
            if not (isinstance(host, str) and HOST_RE.fullmatch(host)):
                raise ValueError(f"not a hostname: {host!r}")
        except (ValueError, KeyError, TypeError) as error:
            raise PolicyError(f"{path} line {number}: {error}") from error
        if host not in hosts:
            hosts.append(host)
    return hosts


def placement_problem(base_policy: Path, policy_dir: Path, writable: Sequence[Path] = ()) -> str | None:
    """What is wrong with where `policy_dir` is (the base itself, under home, or a worker-writable mount), or None."""
    base = base_policy.resolve()
    directory = policy_dir.resolve()
    candidate = directory / POLICY_NAME
    if base in (candidate, candidate.resolve()) or candidate == base_policy.parent.resolve() / base_policy.name:
        return f"{policy_dir} would hold the base policy {base_policy} itself; choose another directory"
    if not directory.is_relative_to(Path.home().resolve()):
        return f"{policy_dir} is not under the home directory, where a job cannot mount it"
    for mount in writable:
        resolved = mount.resolve()
        if directory.is_relative_to(resolved) or resolved.is_relative_to(directory):
            return f"{policy_dir} overlaps {mount}, which a worker sandbox binds read-write"
    return None


def _check_placement(base_policy: Path, policy_dir: Path, writable: Sequence[Path]) -> None:
    if problem := placement_problem(base_policy, policy_dir, writable):
        raise PolicyError(problem)


def _read_base(base_policy: Path) -> dict:
    try:
        return tomllib.loads(base_policy.read_text())
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise PolicyError(f"base policy {base_policy}: {error}") from error


def approve_hosts(
    base_policy: Path,
    policy_dir: Path,
    hosts: list[str],
    *,
    by: str,
    kind: str,
    number: int,
    writable: Sequence[Path] = (),
) -> list[str]:
    """Append an approval line for each host not yet approved, regenerate the policy, return the hosts added. The
    policy for the known and the new hosts is rendered first, so a failed approval records nothing."""
    if bad := [host for host in hosts if not HOST_RE.fullmatch(host) or is_ip_literal(host)]:
        raise PolicyError(f"not an approvable hostname (lowercase, exact, no port, not an IP): {', '.join(bad)}")
    with _approval_lock:
        _check_placement(base_policy, policy_dir, writable)
        policy_dir.mkdir(parents=True, exist_ok=True)
        known = approved_hosts(policy_dir)
        added = list(dict.fromkeys(host for host in hosts if host not in known))
        _render(base_policy, known + added)
        now = datetime.now(UTC).isoformat()
        lines = [json.dumps({"host": host, "at": now, "by": by, "kind": kind, "number": number}) for host in added]
        if lines:
            path = policy_dir / APPROVALS_NAME
            text = path.read_text() if path.exists() else ""
            with path.open("a") as approvals:  # a hand edit may have left the last line without its newline
                approvals.write(
                    ("\n" if text and not text.endswith("\n") else "") + "".join(f"{line}\n" for line in lines)
                )
        generate_policy(base_policy, policy_dir, writable)
        return added


def _scalar(value: object, key: str) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return "[" + ", ".join(json.dumps(item) for item in value) + "]"
    raise PolicyError(f"cannot write {key}: {type(value).__name__} value")


def _key(name: str) -> str:
    return name if re.fullmatch(r"[A-Za-z0-9_-]+", name) else json.dumps(name)


def _table_lines(dotted: str, table: dict, depth: int) -> list[str]:
    """The lines of one table: its header, its scalar keys, then each sub-table's lines."""
    scalars = {name: value for name, value in table.items() if not isinstance(value, dict)}
    tables = {name: value for name, value in table.items() if isinstance(value, dict)}
    lines = [f"[{dotted}]"] if dotted and (scalars or not tables) else []
    lines += [f"{_key(name)} = {_scalar(value, f'{dotted}.{name}'.lstrip('.'))}" for name, value in scalars.items()]
    for name, value in tables.items():
        child = f"{dotted}.{_key(name)}".lstrip(".")
        if depth >= 2:
            raise PolicyError(f"cannot write {child}: tables nest more than two deep")
        lines += ["", *_table_lines(child, value, depth + 1)]
    return lines


def dump_toml(data: dict) -> str:
    """TOML text for tables one or two deep holding strings, numbers, booleans and lists of strings."""
    # ponytail: only the shapes a csub policy has; upgrade to tomli-w if a policy needs more.
    return "\n".join(_table_lines("", data, 0)).lstrip("\n") + "\n"


def _publish(text: str, policy_dir: Path) -> None:
    """Write the policy to a temp file of its own in `policy_dir` and rename it over broker.toml."""
    descriptor, temp_name = tempfile.mkstemp(dir=policy_dir, prefix=".broker-", suffix=".tmp")
    try:
        with os.fdopen(descriptor, "w") as temp_file:
            temp_file.write(text)
        os.replace(temp_name, policy_dir / POLICY_NAME)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise


def _render(base_policy: Path, approved: list[str]) -> str:
    """The policy text: the base with each approved host it lacks added to allowed_hosts, checked to read back."""
    merged = _read_base(base_policy)
    allowed = merged.get("broker", {}).get("allowed_hosts", [])
    if not (isinstance(allowed, list) and all(isinstance(host, str) for host in allowed)):
        raise PolicyError(f"base policy {base_policy}: broker.allowed_hosts is not a list of strings")
    if added := missing_hosts(approved, allowed):
        merged.setdefault("broker", {})["allowed_hosts"] = allowed + added
    text = f"# Generated by mia from {base_policy}; edits are overwritten.\n" + dump_toml(merged)
    try:
        if tomllib.loads(text) == merged:
            return text
    except tomllib.TOMLDecodeError:
        pass
    raise PolicyError(f"generated policy for {base_policy} does not read back as written")


def generate_policy(base_policy: Path, policy_dir: Path, writable: Sequence[Path] = ()) -> Path:
    """Write `<policy_dir>/broker.toml`: the base with each approved host it lacks added to allowed_hosts."""
    _check_placement(base_policy, policy_dir, writable)
    policy_dir.mkdir(parents=True, exist_ok=True)
    while True:
        approved = approved_hosts(policy_dir)
        _publish(_render(base_policy, approved), policy_dir)
        if approved_hosts(policy_dir) == approved:
            return policy_dir / POLICY_NAME


def allowed_patterns(policy_dir: Path) -> list[str]:
    """`broker.allowed_hosts` of the generated policy."""
    return list(tomllib.loads((policy_dir / POLICY_NAME).read_text()).get("broker", {}).get("allowed_hosts", []))
