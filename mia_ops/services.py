"""The services in mia.yaml and how to run them: a Temporal dev server (or an external one), and per app
service one tmux session holding a csub broker and a worker inside agentic-sandbox (mia-buffers adds buffer-api,
which runs on the host like the broker)."""

from __future__ import annotations

import asyncio
import contextlib
import getpass
import importlib
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from autodev.config import PROJECT_DIR
from mia_core.config import CsubCfg
from mia_core.fs import require_local_dir, require_secret_file
from mia_core.hosts import PolicyError, generate_policy, placement_problem

from .manifest import Manifest

PY = PROJECT_DIR / ".pixi/envs/default/bin/python"
BROKER = PROJECT_DIR / ".pixi/envs/default/bin/csub-broker"
CSUB = PROJECT_DIR / ".pixi/envs/default/bin/csub"  # `csub relay` and `csub probe`
SCRATCH = Path("/scratch")  # node-local; the csub broker's unix socket lives under it unless csub.run_dir says
TEMPORAL_CLI_URL = "https://temporal.download/cli/archive/latest?platform=linux&arch=amd64"


# ---- tmux


def _tmux(*args: str, **kwargs: Any) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["tmux", *args], text=True, **kwargs)
    except FileNotFoundError:
        sys.exit("tmux not found on PATH")


def is_running(session: str) -> bool:
    return _tmux("has-session", "-t", f"={session}", capture_output=True).returncode == 0


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    return True


DRAIN_WAIT_S = 60  # how long `down` waits for a worker to stop: its DRAIN, and the reports of what it hands back


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def worker_pids(groups: list[int]) -> list[int]:
    """The worker processes (`python -m <app> worker`) in these process groups. The sandbox around one, whose
    command line ends the same way, is not one: stopping it would take the proxy the worker reports through."""
    if not groups:
        return []
    listed = subprocess.run(
        ["pgrep", "-g", ",".join(str(pgid) for pgid in groups), "-f", r"^\S*python\S* -m \S+ worker$"],
        capture_output=True,
        text=True,
    )
    return [int(pid) for pid in listed.stdout.split() if pid.isdigit()]


def drain(session: str, groups: list[int]) -> None:
    """Tell the session's worker to stop and wait for it. On SIGTERM a worker takes no new task, lets its
    running activities finish for `mia_core.temporal.DRAIN` and hands the rest back to Temporal, which gives
    them to the next worker at once. A worker that is killed leaves each to its timeout, minutes later."""
    pids = worker_pids(groups)
    for pid in pids:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGTERM)
    for _ in range(DRAIN_WAIT_S * 5):
        if not any(_alive(pid) for pid in pids):
            return
        time.sleep(0.2)
    print(f"{session}: its worker has not stopped after {DRAIN_WAIT_S} s; killing it", file=sys.stderr)


def kill(session: str) -> int:
    """Stop the session's worker (`drain`), then kill the session and wait for its processes to exit: tmux only
    delivers SIGHUP, and a Temporal server still closing its SQLite database when `up` starts the next one makes
    that one fail with `database is locked`."""
    if not is_running(session):
        print(f"{session}: not running")
        return 0
    panes = _tmux("list-panes", "-s", "-t", f"={session}", "-F", "#{pane_pid}", capture_output=True).stdout
    groups = [int(pid) for pid in panes.split() if pid.isdigit()]  # each pane's command leads its process group
    drain(session, groups)
    rc = _tmux("kill-session", "-t", f"={session}").returncode
    if rc == 0:
        for _ in range(300):
            if not any(_group_alive(pgid) for pgid in groups):
                break
            time.sleep(0.1)
        else:
            print(f"{session}: still exiting after 30 s", file=sys.stderr)
        print(f"{session}: stopped")
    return rc


def windows(session: str) -> str:
    fmt = "  #{window_name}: #{?pane_dead,dead,alive}"
    return _tmux("list-windows", "-t", f"={session}", "-F", fmt, capture_output=True).stdout


def dead_windows(session: str) -> list[str]:
    """The session's windows whose process has exited; such a window stays, showing the process's last lines."""
    return [line.split(":")[0].strip() for line in windows(session).splitlines() if line.endswith(": dead")]


def last_lines(session: str, window: str, count: int = 15) -> str:
    """The last lines a window's process wrote."""
    target = f"={session}:{window}"
    shown = _tmux("capture-pane", "-p", "-S", "-200", "-t", target, capture_output=True).stdout
    return "\n".join([line for line in shown.splitlines() if line.strip()][-count:])


def attach(session: str) -> int:
    if not is_running(session):
        sys.exit(f"{session} is not running (run `mia up {session}` first)")
    return _tmux("attach", "-t", f"={session}").returncode


KEEP = (";", "set-option", "-w", "remain-on-exit", "on")  # in the same tmux call: a window outlives its process


def new_session(session: str, window: str, command: str, keep: bool = False) -> None:
    """Start a session with one window. `keep`: the window stays when its process exits, marked dead."""
    _tmux("new-session", "-d", "-s", session, "-n", window, command, *(KEEP if keep else ()), check=True)


def new_window(session: str, window: str, command: str) -> None:
    """Add a window that stays when its process exits, marked dead, so `mia ps` can say so."""
    _tmux("new-window", "-t", f"={session}", "-n", window, command, *KEEP, check=True)


def shell(*parts: object) -> str:
    """One shell command line for tmux from argv parts; quoting handles any path, quotes included."""
    return " ".join(shlex.quote(str(p)) for p in parts)


# ---- apps and services


@dataclass(frozen=True)
class App:
    package: str  # `python -m <package>.cli`; `<package>.config.load_config`
    config_env: str  # names the deprecated per-app config file
    proxy_env: str

    @property
    def module(self) -> str:
        return f"{self.package}.cli"

    @property
    def has_local_root(self) -> bool:  # host-local disk for the lock directory and the service state
        return self.package in ("mia_data", "mia_buffers")

    def load_config(self, path: Path, service: str | None = None) -> Any:
        """The app's Config: service `service` of mia.yaml at `path`, or the legacy per-app file when None."""
        return importlib.import_module(f"{self.package}.config").load_config(path, service=service)

    def extra_mounts(self, cfg: Any) -> list[tuple[str, Path]]:
        mounts: list[tuple[str, Path]] = []
        if self.package == "mia_data":
            mounts = [("--ro", cfg.harness.dir), ("--rw", cfg.paths.storage_root)]
        if self.package == "mia_buffers":  # bindings are read from mia-data's storage_root; the caches are written
            mounts = [
                ("--ro", cfg.harness.dir),
                ("--ro", cfg.paths.storage_root),
                ("--rw", cfg.paths.raw_root),
                ("--rw", cfg.paths.staged_root),
            ]
        if self.has_local_root and cfg.paths.local_root:
            mounts.append(("--rw", cfg.paths.local_root / "locks"))
        return mounts

    def api_endpoint(self, cfg: Any) -> tuple[str, int] | None:
        """(host, port) of buffer-api from `service.url`: `mia up` runs it beside the broker and lets the worker
        reach that host through the sandbox proxy. None for the apps without a service."""
        if self.package != "mia_buffers":
            return None
        url = urlparse(cfg.service.url)
        return url.hostname or "localhost", url.port


APPS = {
    "autodev": App("autodev", "AUTODEV_CONFIG", "AUTODEV_TEMPORAL_VIA_PROXY"),
    "mia-data": App("mia_data", "MIA_DATA_CONFIG", "MIA_DATA_TEMPORAL_VIA_PROXY"),
    "mia-buffers": App("mia_buffers", "MIA_BUFFERS_CONFIG", "MIA_BUFFERS_TEMPORAL_VIA_PROXY"),
}


@dataclass
class Service:
    name: str  # also the tmux session
    app: App
    config_path: Path  # the file the worker reads: mia.yaml, or the legacy per-app file
    cfg: Any  # the app's validated Config
    legacy: bool = False  # mia.yaml named a per-app config file (`services.<name>.config`)
    run_dir: Path | None = None  # csub.run_dir: where the broker sockets go; None: /scratch/<user>

    def env(self) -> dict[str, str]:
        """The variables that point the app's CLI at its config."""
        if self.legacy:
            return {self.app.config_env: str(self.config_path)}
        return {"MIA_SERVICE": self.name, "MIA_CONFIG": str(self.config_path)}

    @property
    def state(self) -> Path:
        return self.cfg.paths.state_dir

    @property
    def host(self) -> Path:  # host-only files beside the state dir, which the sandbox binds read-write
        return self.state.with_name(self.state.name + "-host")

    @property
    def run(self) -> Path:  # the csub broker's (or relay's) unix socket dir
        return (self.run_dir or SCRATCH / (os.environ.get("USER") or getpass.getuser())) / self.name

    @property
    def secret(self) -> Path | None:
        g = self.cfg.github
        path = g.private_key_path if g.auth == "app" else g.token_file
        return Path(path).expanduser().resolve() if path else None

    @property
    def token_file(self) -> Path | None:  # the buffer-api worker token, for apps that have a `service` section
        path = getattr(getattr(self.cfg, "service", None), "token_file", None)
        return Path(path).expanduser().resolve() if path else None

    def logs(self) -> dict[str, Path]:
        logs = {"worker": self.state / "logs/worker.log", "broker": self.host / "logs/broker.log"}
        if self.app.api_endpoint(self.cfg):
            logs["api"] = self.host / "logs/api.log"
        return logs


class Stack:
    """mia.yaml loaded: every service with its config, and the one Temporal address they share."""

    def __init__(self, manifest: Manifest):
        self.manifest = manifest
        self.services: dict[str, Service] = {}
        for name, block in manifest.services.items():
            app = APPS[block.app]
            run_dir = manifest.csub.run_dir
            if block.config:
                print(f"{name}: services.{name}.config is deprecated; move its sections into mia.yaml", file=sys.stderr)
                cfg = app.load_config(block.config)
                self.services[name] = Service(name, app, block.config, cfg, legacy=True, run_dir=run_dir)
            elif manifest.path is None:
                sys.exit(f"mia: service {name!r} has no config: file and the manifest was not loaded from a file")
            else:
                cfg = app.load_config(manifest.path, service=name)
                self.services[name] = Service(name, app, manifest.path, cfg, run_dir=run_dir)
        addresses = {s.cfg.temporal.address for s in self.services.values()}
        if len(addresses) != 1:
            sys.exit(f"mia: services disagree on temporal.address: {sorted(addresses)}")
        self.temporal_address: str = addresses.pop()
        # Two workers binding one state dir read-write, or two deployments with one label prefix, would corrupt
        # each other: the usual slip is copying a config and forgetting to change these.
        seen: dict[tuple[str, object], str] = {}
        for name, svc in self.services.items():
            for key, value in (("paths.state_dir", svc.state), ("deployment", svc.cfg.deployment)):
                if (key, value) in seen:
                    sys.exit(f"mia: services {seen[key, value]!r} and {name!r} share {key} {value}")
                seen[key, value] = name

    def service(self, name: str) -> Service:
        if name not in self.services:
            sys.exit(f"mia: unknown service {name!r}; mia.yaml lists {list(self.services)} (and temporal)")
        return self.services[name]

    def select(self, names: list[str]) -> tuple[bool, list[Service]]:
        """(temporal selected?, app services in manifest order). No names means everything."""
        if not names:
            return True, list(self.services.values())
        for name in names:
            if name != "temporal":
                self.service(name)
        return "temporal" in names, [s for n, s in self.services.items() if n in names]


# ---- temporal


class Temporal:
    SESSION = "temporal"

    def __init__(self, stack: Stack):
        self.managed = stack.manifest.temporal.managed
        self.state = stack.manifest.temporal.state_dir
        self.address = stack.temporal_address
        self.log = self.state / "logs/temporal.log"
        self._reported = False

    @property
    def host(self) -> str:
        return self.address.rsplit(":", 1)[0] if ":" in self.address else self.address

    def cli_if_present(self) -> Path | None:
        """The `temporal` CLI on PATH or already downloaded; None if a managed server would have to download it."""
        if found := shutil.which("temporal"):
            return Path(found)
        if not self.managed:
            sys.exit("mia: the `temporal` CLI must be on PATH when temporal.managed is false")
        target = self.state / "bin/temporal"
        return target if target.exists() else None

    def cli(self) -> Path:
        if present := self.cli_if_present():
            return present
        target = self.state / "bin/temporal"
        target.parent.mkdir(parents=True, exist_ok=True)
        print(f"downloading the temporal CLI to {target}")
        cmd = f"curl -sSL {shlex.quote(TEMPORAL_CLI_URL)} | tar xz -C {shlex.quote(str(target.parent))} temporal"
        subprocess.run(cmd, shell=True, check=True)
        return target

    def _run(self, *args: str, download: bool = True) -> int:
        cli = self.cli() if download else self.cli_if_present()
        if cli is None:  # no CLI and not allowed to fetch one: treat the server as unreachable
            return 1
        return subprocess.run([str(cli), *args, "--address", self.address], capture_output=True).returncode

    def healthy(self, download: bool = True) -> bool:
        return self._run("operator", "cluster", "health", download=download) == 0

    def status(self) -> str:
        """Read-only: never downloads the CLI or writes anything."""
        if self.managed:
            return "running" if is_running(self.SESSION) else "external" if self.healthy(download=False) else "stopped"
        return "external (healthy)" if self.healthy() else "external (unreachable)"

    def up(self) -> None:
        if self.healthy():
            if not self._reported:  # every service_up checks Temporal; report the result once per command
                print(f"temporal: {self.status()} at {self.address}")
            self._reported = True
            return
        if not self.managed:
            sys.exit(f"temporal: unreachable at {self.address} (temporal.managed is false; see your cluster)")
        try:
            require_local_dir(self.state)  # temporal.db is SQLite: its locks do not work on a network filesystem
        except RuntimeError as error:
            sys.exit(f"temporal: {error} (temporal.state_dir)")
        self.log.parent.mkdir(parents=True, exist_ok=True)
        server = shell(self.cli(), "server", "start-dev", "--db-filename", self.state / "temporal.db")
        new_session(self.SESSION, "temporal", f"{server} 2>&1 | tee -a {shell(self.log)}")
        for _ in range(60):
            if self.healthy():
                print(f"temporal: started at {self.address}")
                self._reported = True
                return
            time.sleep(1)
        sys.exit(f"temporal: did not come up; see {self.log}")

    def down(self, explicit: bool) -> int:
        if not self.managed:
            if explicit:
                sys.exit("temporal is external (temporal.managed is false); see your cluster")
            print("temporal: external, left alone")
            return 0
        return kill(self.SESSION)

    def ensure_namespace(self, ns: str) -> None:
        if self._run("operator", "namespace", "describe", "-n", ns) == 0:
            return
        if self._run("operator", "namespace", "create", "-n", ns) != 0:
            sys.exit(f"temporal: could not create namespace {ns!r} at {self.address}")
        for _ in range(30):  # registration is async: the frontend's namespace cache refreshes every ~10 s
            if self._run("workflow", "list", "-n", ns, "--limit", "1") == 0:
                return
            time.sleep(1)


# ---- app services


def _rw_binds(svc: Service) -> list[Path]:
    """What the service's worker sandbox binds with `--rw` (its state dir, the cwd, is bound read-write too)."""
    return [*(p for flag, p in svc.app.extra_mounts(svc.cfg) if flag == "--rw"), svc.cfg.paths.work_root, svc.run]


def writable_mounts(stack: Stack) -> list[Path]:
    """Every path any worker can write: the placement rules keep the generated policy out of all of them."""
    return [path for svc in stack.services.values() for path in (svc.state, *_rw_binds(svc))]


def ensure_policy(stack: Stack) -> Path:
    """Generate mia's csub policy from the operator's base policy and the approved hosts; exit naming the
    mia.yaml key at fault when the placement or the base policy is refused."""
    csub = stack.manifest.csub
    writable = writable_mounts(stack)
    if problem := placement_problem(csub.base_policy, csub.policy_dir, writable):
        sys.exit(f"mia: csub.policy_dir: {problem} (the base policy is csub.base_policy)")
    try:
        return generate_policy(csub.base_policy, csub.policy_dir, writable)
    except PolicyError as error:
        sys.exit(f"mia: csub.base_policy: {error}")


def broker_command(svc: Service, csub: CsubCfg, policy: Path, sock: Path) -> str:
    """The broker window's command line: `csub-broker --serve` on this host, or with `csub.broker: relay` a
    `csub relay` on the same socket that forwards each request over ssh to this checkout's csub-broker on the
    submit host, on the same generated policy (the home directory is shared with it)."""
    log = svc.host / "logs/broker.log"
    if csub.broker == "local":
        command = shell(BROKER, "--policy", policy, "--serve", sock)
    else:
        env = ["CSUB_TRANSPORT=ssh", f"CSUB_SSH_HOST={csub.ssh_host}"]
        if csub.ssh_user:
            env.append(f"CSUB_SSH_USER={csub.ssh_user}")
        env += [f"CSUB_SSH_KEY={csub.ssh_key}", f"CSUB_BROKER_CMD={BROKER} --policy {policy}"]
        command = shell("env", *env, CSUB, "relay", sock)
    return f"{command} 2>&1 | tee -a {shell(log)}"


BROKER_START_S = 15  # how long `mia up` waits for a relay's socket before probing the broker through it


def broker_answers(sock: Path) -> str | None:
    """Ask the broker behind `sock` what it allows (`csub probe`) once the socket is there: None when it answers,
    else the last line of what csub said, so a key the submit host refuses is found at start, not at the first job."""
    for _ in range(BROKER_START_S * 5):
        if sock.exists():
            break
        time.sleep(0.2)
    else:
        return f"relay: no socket at {sock} after {BROKER_START_S} s"
    env = {**os.environ, "CSUB_TRANSPORT": "unix", "CSUB_SOCKET": str(sock)}
    done = subprocess.run([str(CSUB), "probe"], env=env, capture_output=True, text=True)
    if done.returncode == 0:
        return None
    lines = (done.stderr or done.stdout).strip().splitlines()
    return lines[-1] if lines else f"csub probe exited {done.returncode}"


WORKER_START_S = 90  # how long `mia up` waits for a new worker's first poll
API_START_S = 30  # ... and for buffer-api to take connections, before any worker that writes to it starts


def api_started(svc: Service, endpoint: tuple[str, int]) -> bool:
    """Wait until buffer-api takes connections. False when its window dies first or `API_START_S` pass."""
    for _ in range(API_START_S * 5):
        if "api" in dead_windows(svc.name):
            return False
        try:
            socket.create_connection(endpoint, timeout=1).close()
        except OSError:
            time.sleep(0.2)
        else:
            return True
    return False


async def _polls_since(svc: Service, since: float) -> bool:
    from autodev.cli import _client
    from mia_core.temporal import last_poll

    client = await _client(svc.cfg)
    for _ in range(WORKER_START_S):
        if dead_windows(svc.name):
            return False
        polled = await last_poll(client, svc.cfg.temporal.namespace, svc.cfg.task_queue)
        # ponytail: this host's clock against Temporal's; a server more than ~30 s behind fails a good start
        if polled is not None and polled >= since:
            return True
        await asyncio.sleep(1)
    return False


def worker_started(svc: Service, since: float) -> bool:
    """Wait for the service's worker to poll its task queue after `since` (Unix seconds). False when a window of
    the service dies first, or `WORKER_START_S` pass without a poll."""
    return asyncio.run(_polls_since(svc, since))


def service_up(svc: Service, stack: Stack, temporal: Temporal) -> int:
    """Start one app service: broker + sandboxed worker in tmux (buffer-api before the worker, which starts once
    it answers), then apply its poll schedule, then wait for the worker's first poll: 1 when it never comes. Temporal is started (managed) or verified (external) first.
    Re-running on a live session only re-applies the schedule, so a run that failed partway is repaired by
    running it again; a session with a dead window is reported, not repaired."""
    cfg = svc.cfg
    if cfg.agent.api_key_env:
        sys.exit(f"{svc.name}: agent.api_key_env is not supported by `mia up` yet; give the account a Claude login")
    if not svc.secret:  # the sandbox masks .env and tmux passes no env, so the GitHub secret must be a file
        sys.exit(f"{svc.name}: set github.token_file (auth: token) or github.private_key_path (auth: app)")
    sandbox = stack.manifest.sandbox_dir / "sandbox-run.sh"
    csub = stack.manifest.csub
    prerequisites = [
        (PY, "run `pixi install`"),
        (BROKER, "run `pixi install`"),
        (sandbox, "set sandbox_dir in mia.yaml"),
    ]
    if csub.broker == "relay":
        prerequisites.append((CSUB, "run `pixi install`"))
    for path, hint in prerequisites:
        if not os.access(path, os.X_OK):
            sys.exit(f"{svc.name}: {path} not found ({hint})")
    endpoint = svc.app.api_endpoint(cfg)
    if endpoint and endpoint[0] in ("localhost", "127.0.0.1", "::1"):  # the sandbox bypasses its proxy for these
        sys.exit(f"{svc.name}: service.url must name this host, not {endpoint[0]}: inside the sandbox that is the "
                 "sandbox's own loopback, where nothing listens")  # fmt: skip
    if svc.app.has_local_root:
        local_root = cfg.paths.local_root
        # ponytail: a legacy per-app file may omit local_root (no lock dir); drop the exemption with `config:`
        if local_root is None and not svc.legacy:
            sys.exit(f"{svc.name}: set paths.local_root in mia.yaml (host-local disk for the lock directory and the "
                     "service state)")  # fmt: skip
        if local_root is not None:
            try:
                require_local_dir(local_root)
            except RuntimeError as error:
                sys.exit(f"{svc.name}: {error}")
            for d in (local_root / "locks", local_root / "service"):
                d.mkdir(parents=True, exist_ok=True)
    token_file = svc.token_file
    if svc.app.package == "mia_buffers" and token_file is None and not svc.legacy:
        sys.exit(f"{svc.name}: set service.token_file in mia.yaml and run `mia init-token`")
    if token_file is not None:
        try:
            require_secret_file(token_file)
        except RuntimeError as error:
            sys.exit(f"{svc.name}: {error}")
    policy = ensure_policy(stack)
    try:
        require_local_dir(svc.run)  # the socket dir: host-local, like local_root
    except RuntimeError as error:
        sys.exit(f"{svc.name}: csub.run_dir: {error}")
    for d in (svc.state / "logs", svc.host / "logs", *_rw_binds(svc)):
        d.mkdir(parents=True, exist_ok=True)

    temporal.up()
    temporal.ensure_namespace(cfg.temporal.namespace)

    schedule = [str(PY), "-m", svc.app.module, "schedule", "apply"]
    env = {**os.environ, **svc.env()}
    if is_running(svc.name):
        subprocess.run(schedule, env=env, check=True)
        if dead := dead_windows(svc.name):
            print(f"{svc.name}: its {', '.join(dead)} has exited: mia restart {svc.name}", file=sys.stderr)
            return 1
        print(f"{svc.name}: already running (tmux attach -t {svc.name})")
        return 0

    sock = svc.run / "csub.sock"
    new_session(svc.name, "broker", broker_command(svc, csub, policy, sock), keep=True)
    if csub.broker == "relay" and (problem := broker_answers(sock)) is not None:
        print(f"{svc.name}: NOT RUNNING: the relay cannot reach the broker: {problem}", file=sys.stderr)
        print(f"{svc.name}: see `mia logs {svc.name} broker`; then `mia restart {svc.name}`", file=sys.stderr)
        return 1
    allow = ["github.com", "api.github.com", temporal.host]
    if (service := getattr(cfg, "service", None)) and (host := urlparse(service.url).hostname):
        allow.append(host)  # every worker with a `service` block writes the registry (mia-data's dataset rows too)
    if endpoint:  # buffer-api: on the host, unsandboxed like the broker
        env_pairs = [f"{name}={value}" for name, value in svc.env().items()]
        api = shell("env", *env_pairs, PY, "-m", svc.app.module, "serve")
        new_window(svc.name, "api", f"{api} 2>&1 | tee -a {shell(svc.host / 'logs/api.log')}")
        if not api_started(svc, endpoint):  # no worker starts on a registry that does not answer
            print(
                f"{svc.name}: NOT RUNNING: buffer-api did not come up in {API_START_S} s. Its last lines:",
                file=sys.stderr,
            )
            print(last_lines(svc.name, "api"), file=sys.stderr)
            print(f"{svc.name}: see `mia logs {svc.name} api`; then `mia restart {svc.name}`", file=sys.stderr)
            return 1
    # cwd = the state dir, which sandbox-run.sh binds read-write; the repo (code + pixi env), config and secret
    # are read-only. The worker reaches Temporal through the sandbox's allowlist proxy.
    mounts: list[object] = ["--ro", PROJECT_DIR, "--ro", svc.config_path, "--ro", svc.secret]
    if token_file is not None and token_file != svc.secret:
        mounts += ["--ro", token_file]
    for flag, path in svc.app.extra_mounts(cfg):
        if flag == "--ro":
            mounts += [flag, path]
    for path in _rw_binds(svc):
        mounts += ["--rw", path]
    worker = shell(
        sandbox, *mounts, *(a for h in allow for a in ("--allow", h)), "--",
        "env", *(f"{key}={value}" for key, value in svc.env().items()), "CSUB_TRANSPORT=unix", f"CSUB_SOCKET={sock}",
        f"{svc.app.proxy_env}=1", PY, "-m", svc.app.module, "worker",
    )  # fmt: skip
    since = time.time()
    new_window(svc.name, "worker", f"cd {shell(svc.state)} && {worker}")
    subprocess.run(schedule, env=env, check=True)
    if not worker_started(svc, since):
        dead = dead_windows(svc.name)
        what = f"its {', '.join(dead)} exited" if dead else f"its worker has not polled Temporal in {WORKER_START_S} s"
        print(f"{svc.name}: NOT RUNNING: {what}. Its last lines:", file=sys.stderr)
        print(last_lines(svc.name, dead[0] if dead else "worker"), file=sys.stderr)
        print(f"{svc.name}: see `mia attach {svc.name}` and `mia logs {svc.name}`", file=sys.stderr)
        return 1
    print(f"{svc.name}: started (attach: mia attach {svc.name}; logs: mia logs {svc.name} -f)")
    return 0
