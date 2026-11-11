"""The `mia` command line: start, stop and inspect every service in mia.yaml, like docker compose for the
tmux sessions. `mia up` with no service acts on all of them (Temporal first); `mia down` stops them all
(Temporal last); naming services restricts each command to those, and never touches Temporal unless named."""

from __future__ import annotations

import argparse
import os
import secrets
import stat
import subprocess
import sys
from pathlib import Path

from mia_core.autonomy import set_pause
from mia_core.config import JudgeCfg, manifest_path, read_manifest
from mia_core.judge import eval as judge_eval

from . import services
from .manifest import load_manifest
from .services import Stack, Temporal, attach, is_running, kill, service_up, windows


def cmd_up(stack: Stack, args: argparse.Namespace) -> int:
    temporal = Temporal(stack)
    want_temporal, selected = stack.select(args.services)
    if want_temporal and not selected:  # `mia up temporal`
        temporal.up()
    for svc in selected:  # each call starts or verifies Temporal first; after the first that is a health check
        service_up(svc, stack, temporal)
    return 0


def cmd_down(stack: Stack, args: argparse.Namespace) -> int:
    want_temporal, selected = stack.select(args.services)
    rc = 0
    for svc in reversed(selected):
        rc |= kill(svc.name)
    if want_temporal:
        rc |= Temporal(stack).down(explicit="temporal" in args.services)
    return rc


def cmd_restart(stack: Stack, args: argparse.Namespace) -> int:
    return cmd_down(stack, args) or cmd_up(stack, args)


def cmd_ps(stack: Stack, args: argparse.Namespace) -> int:
    want_temporal, selected = stack.select(args.services)
    rc = 0
    if want_temporal:
        temporal = Temporal(stack)
        status = temporal.status()
        print(f"temporal: {status} ({temporal.address})")
        rc |= "stopped" in status or "unreachable" in status
    for svc in selected:
        if is_running(svc.name):
            print(f"{svc.name}: running")
            print(windows(svc.name), end="")
        else:
            print(f"{svc.name}: stopped")
            rc = 1
    return int(rc)


def cmd_logs(stack: Stack, args: argparse.Namespace) -> int:
    if args.service == "temporal":
        temporal = Temporal(stack)
        if not temporal.managed:
            sys.exit("temporal is external (temporal.managed is false); see your cluster")
        path = temporal.log
    else:
        logs = stack.service(args.service).logs()
        if args.window not in logs:
            sys.exit(f"{args.service} has no {args.window} window (it has {', '.join(logs)})")
        path = logs[args.window]
    if not path.is_file():
        sys.exit(f"no log at {path}")
    cmd = ["tail", "-n", str(args.lines), *(["-F"] if args.follow else []), str(path)]
    try:
        return subprocess.run(cmd).returncode
    except KeyboardInterrupt:
        return 0


def cmd_attach(stack: Stack, args: argparse.Namespace) -> int:
    if args.service == "temporal":
        if not Temporal(stack).managed:
            sys.exit("temporal is external (temporal.managed is false); see your cluster")
        return attach(Temporal.SESSION)
    return attach(stack.service(args.service).name)


def cmd_exec(stack: Stack, args: argparse.Namespace) -> int:
    svc = stack.service(args.service)
    env = {**os.environ, **svc.env()}
    return subprocess.run([str(services.PY), "-m", svc.app.module, *args.args], env=env).returncode


def cmd_init_token(stack: Stack, args: argparse.Namespace) -> int:
    """Create each worker token file named by service.token_file: 32 random bytes as hex, mode 600, never
    overwritten."""
    for path in sorted({svc.token_file for svc in stack.services.values() if svc.token_file}):
        if path.exists():
            mode = stat.S_IMODE(path.stat().st_mode)
            print(f"{path} exists" + ("" if mode == 0o600 else f" (mode {mode:o}; chmod 600 it)"), file=sys.stderr)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as token:
            token.write(secrets.token_hex(32))
        print(f"wrote {path}")
    return 0


def cmd_pause(stack: Stack, args: argparse.Namespace) -> int:
    """Set `autonomy.pause: true` in the manifest; every worker re-reads it before its next job."""
    set_pause(manifest_path(args.manifest), True)
    print("paused: every workflow stops at its next job; mia resume lifts it")
    return 0


def cmd_resume(stack: Stack, args: argparse.Namespace) -> int:
    set_pause(manifest_path(args.manifest), False)
    print("resumed")
    return 0


def cmd_judge(stack: Stack, args: argparse.Namespace) -> int:
    """`mia judge eval <gate> <dir>`: run the judge over an evaluation set with the manifest's `judge` block
    (spec autonomous-gates §9) and print the per-category table."""
    data = read_manifest(manifest_path(args.manifest))
    cfg = JudgeCfg(**(data.get("judge") or {}))
    if args.model:
        cfg = cfg.model_copy(update={"model": args.model})
    rows = judge_eval.evaluate(Path(args.dir), cfg, limit=args.limit)
    for row in rows:
        cost = f"${row.cost_usd:.2f}" if row.cost_usd is not None else "-"
        print(f"{row.item}\t{row.category}\texpected {row.expected}\tgot {row.got}\t{row.confidence:g}\t{cost}")
    print()
    print(judge_eval.render(judge_eval.summarize(rows)), end="")
    return 0


COMMANDS = {
    "up": cmd_up,
    "down": cmd_down,
    "restart": cmd_restart,
    "ps": cmd_ps,
    "logs": cmd_logs,
    "attach": cmd_attach,
    "exec": cmd_exec,
    "init-token": cmd_init_token,
    "pause": cmd_pause,
    "resume": cmd_resume,
    "judge": cmd_judge,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mia", description=__doc__)
    parser.add_argument("--manifest", help="path to mia.yaml (default: $MIA_CONFIG or ./mia.yaml)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd, help_text in (
        ("up", "start all services (Temporal first), or the named ones"),
        ("down", "stop all services (Temporal last), or the named ones"),
        ("restart", "down, then up, same selection"),
        ("ps", "show which services are running"),
    ):
        sub.add_parser(cmd, help=help_text).add_argument("services", nargs="*", metavar="SERVICE")
    logs = sub.add_parser(
        "logs", help="tail a service's log (default window: worker; `mia logs temporal` for Temporal)"
    )
    logs.add_argument("service")
    logs.add_argument("window", nargs="?", default="worker", choices=["worker", "broker", "api"])
    logs.add_argument("-n", "--lines", type=int, default=50)
    logs.add_argument("-f", "--follow", action="store_true", help="keep following (default: print the last lines)")
    sub.add_parser("attach", help="attach to a service's tmux session").add_argument("service")
    ex = sub.add_parser("exec", help="run a service's app CLI with its config, e.g. `mia exec snippets check`")
    ex.add_argument("service")
    ex.add_argument("args", nargs=argparse.REMAINDER)
    sub.add_parser("init-token", help="create the worker token file named by service.token_file")
    sub.add_parser("pause", help="stop every workflow at its next job (sets autonomy.pause in mia.yaml)")
    sub.add_parser("resume", help="lift the pause")
    judge = sub.add_parser("judge", help="the judges: `mia judge eval <gate> <dir>` scores one over an evaluation set")
    judge_sub = judge.add_subparsers(dest="judge_cmd", required=True)
    ev = judge_sub.add_parser("eval", help="run the judge over <dir>/<item>/evidence and compare with expected.json")
    ev.add_argument("gate", choices=["crops", "review"])
    ev.add_argument("dir")
    ev.add_argument("--limit", type=int, help="only the first N items")
    ev.add_argument("--model", help="override judge.model for this run")
    args = parser.parse_args(argv)
    stack = Stack(load_manifest(args.manifest))
    return COMMANDS[args.cmd](stack, args)


if __name__ == "__main__":
    sys.exit(main())
