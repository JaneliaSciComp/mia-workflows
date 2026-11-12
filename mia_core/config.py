"""Configuration shared by every MIA application: the common models, .env loading and the mia.yaml merge.

`mia.yaml` holds the sections every service shares plus a `services` map; `service_config` turns it into the
one-service dict an application's own config model validates.
"""

from __future__ import annotations

import copy
import os
import sys
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, Field, field_validator

PROJECT_DIR = Path(__file__).resolve().parent.parent

MANIFEST_ONLY = frozenset({"sandbox_dir", "services"})  # top-level keys no service config receives
TEMPORAL_MANIFEST_ONLY = frozenset({"managed", "state_dir"})  # temporal keys read by the ops CLI, not by services


class TemporalCfg(BaseModel):
    address: str = "localhost:7233"
    namespace: str = "default"
    ui_url: str | None = None  # the Temporal web UI; the buffers page links each dataset's workflow when set
    via_https_proxy: bool = False  # inside agentic-sandbox: reach Temporal through the allowlist proxy
    poll_minutes: int = 2


class ServiceCfg(BaseModel):
    url: str  # buffer-api base URL, reachable from the worker
    token_file: Path | None = None  # the worker token the service checks

    @field_validator("url")
    @classmethod
    def _has_port(cls, url: str) -> str:
        """An explicit port, so `serve` and `mia up` read the same one from the URL."""
        if urlparse(url).port is None:
            raise ValueError("service.url needs an explicit port, e.g. http://host.example.org:8765")
        return url


class KeeperCfg(BaseModel):
    minutes: int = Field(default=30, ge=1)  # SQLite backup cadence of buffer-api
    catalog_sync_hours: int = Field(default=24, ge=1)


class QuotaCfg(BaseModel):
    """Per disk root: how much the shared raw and staged caches may hold; the keeper evicts free assets to stay
    under. Per job is `LimitsCfg` (mia_data.config); per request is `AutonomyCfg`."""

    raw_bytes: float = Field(default=0, ge=0)  # 0 = unlimited
    staged_bytes: float = Field(default=0, ge=0)
    headroom_bytes: float = Field(default=200e9, ge=0)
    hold_hours: float = Field(default=48, ge=0)  # an asset unused this long is evictable


class RootBudgetCfg(BaseModel):
    """What one root request (a buffer, prep or dataset issue) may spend in all (spec autonomous-gates §8)."""

    max_usd: float = Field(default=100, ge=0)
    max_fetch_bytes: float = Field(default=500e9, ge=0)
    max_jobs: int = Field(default=200, ge=1)
    max_concurrent_jobs: int = Field(default=20, ge=1)
    max_days: float = Field(default=14, gt=0)


class AutonomyCfg(BaseModel):
    """Per request, and the operator's switches over the autonomous gates: the pause, the quarantine hold and the
    `root` caps that add up every job one issue causes. Per job is `LimitsCfg`; per disk root is `QuotaCfg`."""

    pause: bool = False  # `mia pause` flips it; every workflow stops at its next job while it is set
    quarantine_days: float = Field(default=30, ge=0)  # a revoked artifact is held this long before eviction
    root: RootBudgetCfg = RootBudgetCfg()

    def caps(self) -> dict[str, float]:
        """The root caps as the `caps` dict buffer-api's reserve route takes."""
        return dict(self.root.model_dump())


class GateCfg(BaseModel):
    """One judged gate (spec autonomous-gates §9): `off` is today, `shadow` records the verdict beside the human's and
    decides nothing, `active` lets the verdict authorize. `max_suspect` is how many `suspect` crops a set may carry
    and still be approved (§4.2)."""

    mode: Literal["off", "shadow", "active"] = "off"
    max_suspect: int = Field(default=2, ge=0)

    @field_validator("mode", mode="before")
    @classmethod
    def _bare_off(cls, value: object) -> object:
        """YAML reads a bare `off` as false; take it as the mode it was meant to be. `active` waits for milestone N:
        nothing grants on a judge's verdict yet, so the mode would only post a false "approved automatically"."""
        if value == "active":
            raise ValueError("mode: active is not available yet (milestone N); use shadow")
        return "off" if value is False else value


class JudgeCfg(BaseModel):
    """The judge job (spec autonomous-gates §4): the model (a different one from the authors' is a precaution, not
    a rule), what one run may cost and run for, the confidence under which it abstains, and the mode of each gate."""

    model: str = "opus"
    max_budget_usd: float = Field(default=2.0, gt=0)
    walltime_min: int = Field(default=30, gt=0)
    min_confidence: float = Field(default=0.6, ge=0, le=1)
    crops: GateCfg = GateCfg()
    review: GateCfg = GateCfg()

    def _gates(self) -> dict[str, GateCfg]:
        return {"crops": self.crops, "review": self.review}

    def on_gates(self) -> frozenset[str]:
        """The gates whose judge runs at all (shadow or active)."""
        return frozenset(gate for gate, cfg in self._gates().items() if cfg.mode != "off")

    def enabled_gates(self) -> frozenset[str]:
        """The gates whose judge verdict authorizes (`authorized(enabled_gates=...)`)."""
        return frozenset(gate for gate, cfg in self._gates().items() if cfg.mode == "active")


def load_dotenv(path: Path = PROJECT_DIR / ".env") -> None:
    """Minimal .env loader: KEY=VALUE lines; variables already in the environment win."""
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.removeprefix("export ").partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def manifest_path(path: str | os.PathLike | None = None) -> Path:
    """The manifest to read: `path`, else $MIA_CONFIG, else mia.yaml in the project directory."""
    return Path(path or os.environ.get("MIA_CONFIG") or PROJECT_DIR / "mia.yaml")


def read_manifest(path: Path) -> dict:
    """Parse the manifest, exiting with a one-line message if it is missing, not YAML, or not a mapping."""
    if not path.is_file():
        sys.exit(f"mia: config not found at {path} (copy mia.yaml.example, or set $MIA_CONFIG)")
    try:
        data = yaml.safe_load(path.read_text())
    except yaml.YAMLError as err:
        sys.exit(f"mia: invalid config {path}: {err}")
    if not isinstance(data, dict):
        sys.exit(f"mia: invalid config {path}: expected a mapping at the top level")
    return data


def _app_of(block: object) -> str | None:
    return block.get("app") if isinstance(block, dict) else None


def service_config(manifest: dict, name: str) -> dict:
    """Merge the shared sections with service `name`'s block (one level deep); the input is not modified."""
    services = manifest.get("services")
    if not isinstance(services, dict) or name not in services:
        known = sorted(services) if isinstance(services, dict) else []
        raise ValueError(f"no service {name!r} in mia.yaml; services: {known}")
    block = services[name]
    if not isinstance(block, dict) or not isinstance(block.get("app"), str):
        raise ValueError(f"services.{name}: needs app: autodev | mia-data | mia-buffers")
    result = copy.deepcopy({key: value for key, value in manifest.items() if key not in MANIFEST_ONLY})
    if isinstance(result.get("temporal"), dict):
        result["temporal"] = {k: v for k, v in result["temporal"].items() if k not in TEMPORAL_MANIFEST_ONLY}
    for key, value in block.items():
        if key in ("app", "config"):  # `config` is the legacy per-service file pointer
            continue
        if not isinstance(value, dict):
            raise ValueError(f"services.{name}.{key}: a service block holds sections; {key!r} is not a mapping")
        shared = result.get(key) or {}
        if not isinstance(shared, dict):
            raise ValueError(f"{key}: the shared section must be a mapping")
        result[key] = {**shared, **copy.deepcopy(value)}
    result["deployment"] = name
    if block["app"] == "mia-buffers":
        # buffer-api starts and signals mia-data's workflows: tell it their namespace and task queue (the task
        # queue is the service's name). A manifest without such a service leaves the model's default.
        data_name = next((key for key, other in services.items() if _app_of(other) == "mia-data"), None)
        if data_name is not None:
            temporal = service_config(manifest, data_name).get("temporal")
            # mia-data's own default namespace when it has no temporal section; pydantic's when it has one
            namespace = temporal.get("namespace", "default") if isinstance(temporal, dict) else "mia-data"
            result["datasets_pipeline"] = {"namespace": namespace, "task_queue": data_name}
    paths = result.get("paths")
    if isinstance(paths, dict) and "state_root" in paths:
        state_root = paths.pop("state_root")
        if not isinstance(state_root, str):
            raise ValueError("paths.state_root must be a path string")
        paths.setdefault("state_dir", str(Path(state_root) / name))
    return result


def load_service_config(name: str, path: str | os.PathLike | None = None) -> dict:
    """`service_config` of the manifest on disk, exiting with a readable message instead of raising."""
    try:
        return service_config(read_manifest(manifest_path(path)), name)
    except ValueError as err:
        sys.exit(f"mia: {err}")
