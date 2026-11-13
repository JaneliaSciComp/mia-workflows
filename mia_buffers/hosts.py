"""Which hosts a dataset round, a preparation request or a buffer's pool needs that the csub policy lacks, and
approving them: what buffer-api uses to answer an approval."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import yaml

from autodev.config import is_github_host
from mia_core.hosts import HOST_RE, allowed_patterns, approve_hosts, is_ip_literal, missing_hosts
from mia_core.untrusted import read_untrusted
from mia_data.binding import network_hosts, record_urls
from mia_data.config import NetworkCfg

from .config import Config
from .status import _in_pool

BINDING_LIMIT = 1_000_000  # a binding is a few KB
PLAN_LIMIT = 100_000_000  # a plan for a large catalog is several MB
Needed = dict[str, list[str]]  # host -> the sorted ids of the records that need it


@dataclass(frozen=True)
class HostCheck:
    """What buffer-api needs to say which hosts a run lacks, and to approve them."""

    base_policy: Path
    policy_dir: Path
    network: NetworkCfg
    storage_root: Path
    rounds_dir: Path  # <state_dir>/rounds, where a buffer round writes plan.json
    writable: tuple[Path, ...] = ()  # this service's worker-writable roots, for placement_problem
    own_hosts: tuple[str, ...] = ()  # the hosts of service.url, temporal.address and temporal.ui_url: never approved

    @classmethod
    def from_config(cls, cfg: Config) -> HostCheck:
        paths = cfg.paths
        roots = (paths.state_dir, paths.work_root, paths.storage_root, paths.raw_root, paths.staged_root)
        urls = [cfg.service.url, f"//{cfg.temporal.address}", *([cfg.temporal.ui_url] if cfg.temporal.ui_url else [])]
        return cls(
            base_policy=cfg.csub.base_policy,
            policy_dir=cfg.csub.policy_dir,
            network=cfg.network,
            storage_root=paths.storage_root,
            rounds_dir=paths.state_dir / "rounds",
            writable=(*roots, *([paths.local_root] if paths.local_root else [])),
            own_hosts=tuple(sorted({host for url in urls if (host := urlsplit(url).hostname)})),
        )

    def _never_approved(self, host: str) -> str | None:
        """Why a host is never approved (a GitHub host, an IP address, this deployment's own host), or None."""
        if is_github_host(host):
            return "is a GitHub host"
        if is_ip_literal(host):
            return "is an IP address"
        if host in self.own_hosts:
            return "is this deployment's own host"
        return None

    def _group(self, hosts_by_record: dict[str, list[str]]) -> Needed:
        """Host -> the sorted ids of the records that need it, leaving out every host that is never approved."""
        needed: dict[str, list[str]] = {}
        for record, hosts in hosts_by_record.items():
            for host in hosts:
                if self._never_approved(host) is None:
                    needed.setdefault(host, []).append(record)
        return {host: sorted(records) for host, records in sorted(needed.items())}

    def for_records(self, records: dict[str, dict]) -> Needed:
        """Hosts each catalog record's URLs need (record id -> catalog record), without the hosts that are never
        approved; a record whose hosts cannot be computed (a GitHub URL) contributes nothing."""
        hosts_by_record = {}
        for record_id, record in records.items():
            try:
                hosts_by_record[record_id] = network_hosts(record_urls(record), self.network)
            except (ValueError, AttributeError, TypeError):
                continue
        return self._group(hosts_by_record)

    def for_bindings(self, folders: dict[str, str]) -> Needed:
        """Hosts each published binding's URLs need (record id -> its folder under storage_root), without the hosts
        that are never approved. A binding that cannot be read, or names a manual:// URL (the worker sends it to the
        manual drop), contributes nothing."""
        hosts_by_record = {}
        for record_id, folder in folders.items():
            text = read_untrusted(self.storage_root, f"{folder}/binding.yaml", BINDING_LIMIT)
            if text is None:
                continue
            try:
                binding = yaml.safe_load(text)
                urls = [
                    str(url)
                    for array in (binding.get("raw") or []) + (binding.get("labels") or [])
                    for url in (array.get("urls") or [])
                ]
                if not any(url.startswith("manual://") for url in urls):
                    hosts_by_record[record_id] = network_hosts(urls, self.network)
            except (ValueError, yaml.YAMLError, AttributeError, TypeError):
                continue
        return self._group(hosts_by_record)

    def pool(self, round_id: str) -> list[str]:
        """Ids of the in_pool rows of the round's plan.json; ValueError when it cannot be read as a plan."""
        text = read_untrusted(self.rounds_dir, f"{round_id}/plan.json", PLAN_LIMIT)
        try:
            if text is None:
                raise ValueError("no plan")
            return [row["id"] for row in json.loads(text)["rows"] if _in_pool(row)]
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            raise ValueError(f"round {round_id!r} has no readable plan") from error

    def missing(self, needed: Needed) -> Needed:
        """The needed hosts the csub policy does not allow."""
        absent = set(missing_hosts(list(needed), allowed_patterns(self.policy_dir)))
        return {host: records for host, records in needed.items() if host in absent}

    def check(self, hosts: list[str], needed: Needed) -> None:
        """Raise ValueError naming the first host that is not an exact hostname, is never approved, or is not
        needed."""
        for host in hosts:
            if not HOST_RE.fullmatch(host):
                raise ValueError(f"{host!r} is not an approvable hostname (lowercase, exact, no port)")
            if reason := self._never_approved(host):
                raise ValueError(f"{host!r} {reason} and is never approved")
            if host not in needed:
                raise ValueError(f"{host!r} is not a host this run needs")

    def approve(self, hosts: list[str], *, by: str, kind: str, number: int) -> list[str]:
        """Add the hosts to the approvals and regenerate the policy; return the hosts it added."""
        return approve_hosts(
            self.base_policy, self.policy_dir, hosts, by=by, kind=kind, number=number, writable=self.writable
        )
