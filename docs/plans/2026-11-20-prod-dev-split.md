# Prod and dev installations: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `mia` runs an installation on a host without `bsub` (csub relay), refuses to run on the wrong host, and
puts a tagged release live with `mia release <tag>`; the docs describe the two installations and the cutover.

**Architecture:** Everything is in `mia_ops` and `mia_core.config`; no workflow or activity changes, so no replay
risk. The `csub` block of `mia.yaml` grows the relay and socket-dir keys (Task 1) that `service_up` uses to start
`csub relay` instead of `csub-broker --serve` (Task 2). The manifest grows `host:` (Task 3). A new module
`mia_ops/release.py` holds the release flow (Task 4). The docs are rewritten as the system then is (Task 5).

**Tech Stack:** Python 3.12, pydantic, argparse, subprocess, tmux; csub 0.1 (`csub relay`, the `ssh` transport);
pytest with the `fake` fixture of `tests/test_mia_ops_services.py`.

**Spec:** `docs/specs/2026-11-20-prod-dev-split.md`

## Global Constraints

- Build on a branch `prod-dev-split` in a worktree from the main checkout's HEAD; run pytest there with
  `PYTHONPATH=$PWD` (the shared pixi python imports the main checkout otherwise).
- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` is empty.
- `pixi run fmt`, `lint`, `typecheck` clean; run only the test files a task touches, not the full suite.
- `mia.yaml` keys exactly as the spec writes them: `csub.broker` (`local` | `relay`, default `local`),
  `csub.ssh_host`, `csub.ssh_user`, `csub.ssh_key` (default `~/.ssh/csub_ed25519`), `csub.run_dir` (default
  `/scratch/<user>`), top-level `host`.
- The relay's broker window runs exactly: `env CSUB_TRANSPORT=ssh CSUB_SSH_HOST=<ssh_host> [CSUB_SSH_USER=<ssh_user>]
  CSUB_SSH_KEY=<ssh_key> CSUB_BROKER_CMD="<checkout>/.pixi/envs/default/bin/csub-broker --policy <policy>" csub relay
  <sock>`, logged to `broker.log` like the local broker.
- The release layout is `<root>/releases/<tag>` and `<root>/current`, `<root>/mia.yaml` linked into each checkout as
  `mia.yaml -> ../../mia.yaml`. Rolling back is `mia release <earlier tag>`. The command never makes a tag.
- Comments and docs say what the code does, never what it replaced. One-line docstring on every new function.

## Review Focus

1. `csub.broker: relay` without `ssh_host`: the manifest is refused at load naming `csub.ssh_host` (Task 1).
2. The relay is up but the submit host refuses the key or the user: `mia up` returns 1 printing csub's message, and
   the broker window stays for `mia logs <service> broker` (Task 2).
3. `host:` compared against a fully qualified or a short hostname: `host: prod1` matches `prod1.example.org` and
   `prod1` alike; any other name is refused before anything runs (Task 3).
4. `mia release` when a service's `check` fails, or when the clone fails because the tag is not on `origin`:
   `current` is untouched and nothing restarts; the failed clone leaves no `releases/<tag>` behind (Task 4).
5. `mia release <tag>` of a tag already under `releases/`: no clone and no install, only the flip and the restart
   (Task 4).

---

### Task 1: the `csub` block learns the relay and the socket dir

**Files:**
- Modify: `mia_core/config.py:51-63` (`CsubCfg`)
- Test: `tests/test_core_config.py`

**Interfaces:**
- Produces: `CsubCfg.broker: Literal["local", "relay"] = "local"`, `CsubCfg.ssh_host: str | None = None`,
  `CsubCfg.ssh_user: str | None = None`, `CsubCfg.ssh_key: Path = Path("~/.ssh/csub_ed25519")` (expanded and
  absolute like the other paths), `CsubCfg.run_dir: Path | None = None` (expanded and absolute when set; None
  means `/scratch/<user>`, which `mia_ops.services` resolves). A validator refuses `broker: relay` without
  `ssh_host` with the message `csub.ssh_host: required when csub.broker is relay`.

- [ ] **Step 1: Write the failing tests** in `tests/test_core_config.py`, beside
  `test_csub_policy_dir_defaults_beside_the_state_root`:

```python
def test_csub_defaults_to_a_local_broker(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg = csub_config({})
    assert cfg.broker == "local" and cfg.ssh_host is None and cfg.run_dir is None
    assert cfg.ssh_key == tmp_path / ".ssh/csub_ed25519"


def test_csub_relay_carries_the_ssh_settings_and_the_run_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg = csub_config({"csub": {"broker": "relay", "ssh_host": "submit.example.org", "ssh_user": "svc",
                                "ssh_key": "~/.ssh/other", "run_dir": "/opt/mia/run"}})
    assert cfg.broker == "relay" and cfg.ssh_host == "submit.example.org" and cfg.ssh_user == "svc"
    assert cfg.ssh_key == tmp_path / ".ssh/other" and cfg.run_dir == Path("/opt/mia/run")


def test_csub_relay_needs_a_submit_host():
    with pytest.raises(ValueError, match="csub.ssh_host: required when csub.broker is relay"):
        csub_config({"csub": {"broker": "relay"}})
```

- [ ] **Step 2: Run them to verify they fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_core_config.py -k csub -v`
Expected: the two new tests FAIL (`broker` has no such attribute; no ValueError).

- [ ] **Step 3: Add the fields and the validator to `CsubCfg`** in `mia_core/config.py`. Expand `ssh_key` and
  `run_dir` in the existing `_expand` validator; the relay check is a line in the same validator. Update the class
  docstring to name the relay.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_core_config.py tests/test_mia_ops_manifest.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add mia_core/config.py tests/test_core_config.py
git commit -m "config: the csub block names its broker (local or relay) and its socket dir"
```

---

### Task 2: `mia up` starts a relay, probes it, and puts the socket under `csub.run_dir`

**Files:**
- Modify: `mia_ops/services.py` (`SCRATCH` and `BROKER` constants at 30-32; `Service.run` at 235-237; `Stack.__init__`
  at 260-270; `service_up` at 459-535; `_rw_binds` at 395-397)
- Test: `tests/test_mia_ops_services.py`

**Interfaces:**
- Consumes: `CsubCfg` fields of Task 1.
- Produces: `CSUB = PROJECT_DIR / ".pixi/envs/default/bin/csub"` beside `BROKER`; `Service.run_dir: Path | None`
  (a dataclass field, set by `Stack` from `manifest.csub.run_dir`), `Service.run` = `(run_dir or SCRATCH / <user>) /
  name`; `broker_command(svc: Service, csub: CsubCfg, policy: Path, sock: Path) -> str` returns the shell line of the
  broker window for either mode; `broker_answers(sock: Path) -> str | None` runs `[CSUB, "probe"]` with
  `CSUB_TRANSPORT=unix CSUB_SOCKET=<sock>` once the socket file exists (polling up to `BROKER_START_S = 15` s),
  returning None when it exits 0 and otherwise its stderr's last line (or `relay: no socket after 15 s`).

- [ ] **Step 1: Write the failing tests** in `tests/test_mia_ops_services.py`. `make_stack` takes a `csub` keyword
  (default: the `CsubCfg` it builds today) so a test can pass a relay config:

```python
def test_up_relay_runs_csub_relay_on_the_sock_and_probes_it(tmp_path, fake):
    (tmp_path / "csub").write_text("#!/bin/sh\n"); (tmp_path / "csub").chmod(0o755)
    fake.probe_rc = 0
    s = make_stack(tmp_path, csub=CsubCfg(base_policy=tmp_path / ".config/csub/broker.toml", policy_dir=tmp_path / "csub-policy",
                                          broker="relay", ssh_host="submit.example.org", ssh_user="svc", run_dir=tmp_path / "run"))
    assert services.service_up(s.services["autodev"], s, Temporal(s)) == 0
    broker_call = tmux_calls(fake, "new-session")[1]
    broker = broker_call[broker_call.index("-n") + 2]
    sock = f"{tmp_path}/run/autodev/csub.sock"
    for piece in ("CSUB_TRANSPORT=ssh", "CSUB_SSH_HOST=submit.example.org", "CSUB_SSH_USER=svc",
                  f"CSUB_SSH_KEY={tmp_path}/.ssh/csub_ed25519",
                  shlex.quote(f"CSUB_BROKER_CMD={tmp_path}/csub-broker --policy {tmp_path}/csub-policy/broker.toml"),
                  f"{tmp_path}/csub relay {sock}", "broker.log"):
        assert piece in broker, piece
    assert "--serve" not in broker
    probe = [(c, kw) for c, kw in fake.calls if c[-1:] == ["probe"]]
    assert len(probe) == 1 and probe[0][1]["env"]["CSUB_SOCKET"] == sock
    assert f"CSUB_SOCKET={sock}" in worker_command(fake, "autodev") and f"--rw {tmp_path}/run/autodev" in worker_command(fake, "autodev")


def test_up_relay_fails_when_the_submit_host_does_not_answer(tmp_path, fake, capsys):
    (tmp_path / "csub").write_text("#!/bin/sh\n"); (tmp_path / "csub").chmod(0o755)
    fake.probe_rc, fake.probe_err = 1, "ssh submit.example.org: Permission denied (publickey)\n"
    s = make_stack(tmp_path, csub=CsubCfg(base_policy=tmp_path / ".config/csub/broker.toml", policy_dir=tmp_path / "csub-policy",
                                          broker="relay", ssh_host="submit.example.org"))
    assert services.service_up(s.services["autodev"], s, Temporal(s)) == 1
    assert "Permission denied" in capsys.readouterr().err
    assert "autodev" in fake.running  # the session stays: mia logs autodev broker shows why
    assert not any(c[:2] == ["tmux", "new-window"] for c, _ in fake.calls)  # no worker was started


def test_up_relay_needs_the_csub_cli(tmp_path, fake):
    s = make_stack(tmp_path, csub=CsubCfg(base_policy=tmp_path / ".config/csub/broker.toml", policy_dir=tmp_path / "csub-policy",
                                          broker="relay", ssh_host="submit.example.org"))
    with pytest.raises(SystemExit, match="csub not found"):
        services.service_up(s.services["autodev"], s, Temporal(s))


def test_run_dir_is_host_local(tmp_path, fake, monkeypatch):
    def refuse(p):
        raise RuntimeError(f"{p} is on nfs")
    monkeypatch.setattr(services, "require_local_dir", refuse)
    s = make_stack(tmp_path, csub=CsubCfg(base_policy=tmp_path / ".config/csub/broker.toml", policy_dir=tmp_path / "csub-policy",
                                          run_dir=tmp_path / "run"))
    with pytest.raises(SystemExit, match="csub.run_dir"):
        services.service_up(s.services["autodev"], s, Temporal(s))
```

  Extend the `fake` fixture: `state.probe_rc, state.probe_err = 0, ""`; its `run` answers an argv ending in
  `["probe"]` with `CompletedProcess(argv, state.probe_rc, stdout="", stderr=state.probe_err)` and creates the
  socket file named by `kw["env"]["CSUB_SOCKET"]` first (the relay would have). The fixture also monkeypatches
  `services.CSUB` to `tmp_path / "csub"`; only the tests that need the file create it.

- [ ] **Step 2: Run them to verify they fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_mia_ops_services.py -k "relay or run_dir" -v`
Expected: FAIL (`make_stack` takes no `csub`; `CSUB` missing; `--serve` present).

- [ ] **Step 3: Implement in `mia_ops/services.py`**
  - `CSUB` constant; `Service.run_dir` field and the `run` property; `Stack.__init__` passes
    `manifest.csub.run_dir` to every `Service`.
  - `broker_command(svc, csub, policy, sock)`: the `local` line as today; the `relay` line per Global Constraints,
    built with `shell(...)`, `CSUB_SSH_USER` only when set.
  - `broker_answers(sock)`: as the Interfaces say; `subprocess.run([CSUB, "probe"], env={**os.environ, "CSUB_TRANSPORT":
    "unix", "CSUB_SOCKET": str(sock)}, capture_output=True, text=True)`.
  - `service_up`: in `prerequisites`, `(CSUB, "run `pixi install`")` when `csub.broker == "relay"` (message
    `csub not found`); `require_local_dir(svc.run)` wrapped like `local_root`'s, exiting
    `f"{svc.name}: csub.run_dir: {error}"`; after `new_session(... broker ...)`, when relay, `if (problem :=
    broker_answers(sock)) is not None: print(f"{svc.name}: NOT RUNNING: the relay cannot reach the broker: {problem}
    (see `mia logs {svc.name} broker`)", file=sys.stderr); return 1` before buffer-api or the worker start.
  - Drop nothing else; the `_rw_binds` entry `svc.run` already follows `run`.

- [ ] **Step 4: Run the file's tests to verify they pass**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_mia_ops_services.py tests/test_mia_ops_cli.py -v`
Expected: PASS, the existing socket assertions (`scratch/me/<service>/csub.sock`) unchanged.

- [ ] **Step 5: Commit**

```bash
git add mia_ops/services.py tests/test_mia_ops_services.py
git commit -m "mia up: csub.broker relay runs csub relay to a submit host; the socket dir is csub.run_dir"
```

---

### Task 3: `host:` pins a manifest to its host

**Files:**
- Modify: `mia_ops/manifest.py` (`Manifest`, `load_manifest`), `mia_core/config.py:21` (`MANIFEST_ONLY`)
- Test: `tests/test_mia_ops_manifest.py`, `tests/test_core_config.py`

**Interfaces:**
- Produces: `Manifest.host: str | None = None`; `load_manifest` exits with `mia: <path> is for host <host>; this is
  <hostname>` when the first label of `socket.gethostname()` differs from the first label of `host`;
  `"host"` is in `MANIFEST_ONLY`, so no service config receives it.

- [ ] **Step 1: Write the failing tests**

In `tests/test_mia_ops_manifest.py` (use `write_mia_yaml`/the pattern of `test_load_manifest_reads_mia_config_env`
to write a manifest with a top-level `host: prod1`):

```python
@pytest.mark.parametrize("hostname", ["prod1", "prod1.example.org"])
def test_host_matches_this_host_by_its_first_label(tmp_path, monkeypatch, hostname):
    monkeypatch.setattr(manifest_module.socket, "gethostname", lambda: hostname)
    assert load_manifest(write_manifest(tmp_path, host="prod1")).host == "prod1"


def test_host_of_another_host_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(manifest_module.socket, "gethostname", lambda: "dev1.example.org")
    with pytest.raises(SystemExit, match="is for host prod1; this is dev1"):
        load_manifest(write_manifest(tmp_path, host="prod1"))
```

In `tests/test_core_config.py`, extend `test_service_block_keys_are_sections` or add
`test_host_is_manifest_only`: `service_config({"host": "prod1", **minimal}, "autodev")` has no `host` key.

- [ ] **Step 2: Run them to verify they fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_mia_ops_manifest.py tests/test_core_config.py -k host -v`
Expected: FAIL.

- [ ] **Step 3: Implement**: the field with a one-line comment (`the host this file is for; mia refuses to run
  elsewhere`), the check in `load_manifest` after `Manifest(...)`, `import socket`, and `"host"` in `MANIFEST_ONLY`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_mia_ops_manifest.py tests/test_core_config.py tests/test_mia_config.py -v`
Expected: PASS (`test_example_manifest_loads` still passes with no `host` in the example).

- [ ] **Step 5: Commit**

```bash
git add mia_ops/manifest.py mia_core/config.py tests/test_mia_ops_manifest.py tests/test_core_config.py
git commit -m "mia: a manifest's host: refuses to run on another host"
```

---

### Task 4: `mia release <tag> [--to HOST:ROOT]`

**Files:**
- Create: `mia_ops/release.py`
- Modify: `mia_ops/cli.py` (parser and dispatch in `main`, 182-221)
- Test: `tests/test_mia_ops_release.py`

**Interfaces:**
- Produces, in `mia_ops/release.py`:
  - `class LayoutError(Exception)`.
  - `installation_root(checkout: Path) -> Path`: `checkout.resolve()`; when its parent is named `releases`, returns
    the grandparent; otherwise raises `LayoutError(f"{checkout} is not <root>/releases/<tag>: mia release runs from
    an installation laid out as <root>/releases/<tag> with <root>/current -> releases/<tag> and <root>/mia.yaml")`.
  - `MIA_BIN = Path(".pixi/envs/default/bin/mia")` (relative to a checkout).
  - `release(tag: str, checkout: Path = PROJECT_DIR, run: Callable[..., subprocess.CompletedProcess] =
    subprocess.run) -> int`. Order, each `run(..., check=True)` and a `CalledProcessError` printed as one line and
    returned as 1:
    1. `root = installation_root(checkout)`; `target = root / "releases" / tag`.
    2. If `target` does not exist: `origin = run(["git", "-C", str(checkout), "remote", "get-url", "origin"],
       capture_output=True, text=True, check=True).stdout.strip()`; `building = root / "releases" / f".{tag}-new"`
       (removed first if left over); `run(["git", "clone", "--branch", tag, "--depth", "1", origin, str(building)])`;
       `run(["pixi", "install", "--locked"], cwd=building)`; `(building / "mia.yaml").symlink_to(Path("../../mia.yaml"))`;
       `building.rename(target)`.
    3. For each service name in `read_manifest(root / "mia.yaml")["services"]`: `run([str(target / MIA_BIN), "exec",
       name, "check"], cwd=target)`; a failure returns 1 before step 4.
    4. Flip: `new = root / "current.new"`; `new.symlink_to(Path("releases") / tag)`; `new.replace(root / "current")`.
    5. `return run([str(root / "current" / MIA_BIN), "restart"], cwd=root / "current", check=False).returncode`.
  - `release_to(tag: str, target: str, checkout: Path = PROJECT_DIR, run=subprocess.run) -> int`: `host, _, root =
    target.partition(":")`; both non-empty or `LayoutError("--to takes HOST:ROOT")`; `run(["git", "-C", str(checkout),
    "push", "origin", tag], check=True)`; `return run(["ssh", host, f"{root}/current/{MIA_BIN} release {tag}"],
    check=False).returncode`.
- In `mia_ops/cli.py`: `cmd_release(args) -> int` (no `Stack`: `main` dispatches `release` before loading the
  manifest, so it works from any checkout with an `origin`); it catches `LayoutError` and exits with its message.
  Parser: `release` with `tag` and `--to METAVAR HOST:ROOT` (help: `push the tag and run the release on that
  installation over ssh`).

- [ ] **Step 1: Write the failing tests** in `tests/test_mia_ops_release.py`, with a recording fake:

```python
def fake_run(calls, fail_on=None, origin="https://example.org/mia.git"):
    def run(argv, **kw):
        argv = [str(a) for a in argv]
        calls.append((argv, kw))
        if fail_on and fail_on(argv):
            raise subprocess.CalledProcessError(1, argv, stderr="fatal: Remote branch v9 not found")
        if argv[:2] == ["git", "-C"] and "get-url" in argv:
            return subprocess.CompletedProcess(argv, 0, stdout=origin + "\n", stderr="")
        if argv[0] == "git" and argv[1] == "clone":
            Path(argv[-1]).mkdir(parents=True)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
    return run


def layout(tmp_path, tag="v1"):
    root = tmp_path / "mia-prod"
    (root / "releases" / tag).mkdir(parents=True)
    (root / "mia.yaml").write_text("services:\n  autodev: {app: autodev}\n  mia-buffers: {app: mia-buffers}\n")
    (root / "current").symlink_to(Path("releases") / tag)
    return root


def test_installation_root_and_its_refusal(tmp_path):
    root = layout(tmp_path)
    assert installation_root(root / "current") == root
    assert installation_root(root / "releases/v1") == root
    with pytest.raises(LayoutError, match="is not <root>/releases/<tag>"):
        installation_root(tmp_path / "dev/mia-workflows")


def test_release_clones_installs_checks_flips_and_restarts(tmp_path):
    root, calls = layout(tmp_path), []
    assert release("v2", root / "current", run=fake_run(calls)) == 0
    argvs = [c for c, _ in calls]
    assert argvs[0][-2:] == ["get-url", "origin"]
    assert argvs[1][:5] == ["git", "clone", "--branch", "v2", "--depth"] and argvs[1][-1] == str(root / "releases/.v2-new")
    assert argvs[2] == ["pixi", "install", "--locked"] and calls[2][1]["cwd"] == root / "releases/.v2-new"
    assert (root / "releases/v2/mia.yaml").readlink() == Path("../../mia.yaml")
    assert [a[-2:] for a in argvs[3:5]] == [["autodev", "check"], ["mia-buffers", "check"]]
    assert argvs[5] == [str(root / "current/.pixi/envs/default/bin/mia"), "restart"]
    assert (root / "current").readlink() == Path("releases/v2")


def test_release_of_an_existing_tag_only_flips_and_restarts(tmp_path):
    root, calls = layout(tmp_path), []
    (root / "releases/v0").mkdir()
    assert release("v0", root / "current", run=fake_run(calls)) == 0
    assert [c[-1] for c, _ in calls] == ["check", "check", "restart"]  # no git, no pixi
    assert (root / "current").readlink() == Path("releases/v0")


def test_release_stops_before_the_flip_when_a_check_fails(tmp_path, capsys):
    root, calls = layout(tmp_path), []
    assert release("v2", root / "current", run=fake_run(calls, fail_on=lambda a: a[-1] == "check")) == 1
    assert (root / "current").readlink() == Path("releases/v1")
    assert not any(c[-1] == "restart" for c, _ in calls)


def test_release_of_a_tag_not_on_origin_leaves_nothing_behind(tmp_path, capsys):
    root, calls = layout(tmp_path), []
    assert release("v9", root / "current", run=fake_run(calls, fail_on=lambda a: a[:2] == ["git", "clone"])) == 1
    assert "Remote branch v9 not found" in capsys.readouterr().err
    assert sorted(p.name for p in (root / "releases").iterdir()) == ["v1"]
    assert (root / "current").readlink() == Path("releases/v1")


def test_release_to_pushes_the_tag_then_releases_over_ssh(tmp_path):
    calls = []
    assert release_to("v2", "prod.example.org:/home/svc/mia-prod", tmp_path, run=fake_run(calls)) == 0
    assert [c for c, _ in calls] == [
        ["git", "-C", str(tmp_path), "push", "origin", "v2"],
        ["ssh", "prod.example.org", "/home/svc/mia-prod/current/.pixi/envs/default/bin/mia release v2"],
    ]


def test_release_to_needs_host_and_root(tmp_path):
    with pytest.raises(LayoutError, match="HOST:ROOT"):
        release_to("v2", "prod.example.org", tmp_path, run=fake_run([]))
```

  And in `tests/test_mia_ops_cli.py`: `test_release_is_parsed_without_a_manifest(monkeypatch)`: monkeypatch
  `cli.release.release` to record `(tag,)` and `cli.release.release_to` to record `(tag, target)`; `cli.main(["release",
  "v2"])` and `cli.main(["release", "v2", "--to", "h:/r"])` call them and never call `cli.load_manifest` (monkeypatch
  it to raise).

- [ ] **Step 2: Run them to verify they fail**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_mia_ops_release.py tests/test_mia_ops_cli.py -k release -v`
Expected: FAIL with `No module named mia_ops.release`.

- [ ] **Step 3: Implement `mia_ops/release.py` and the CLI wiring** as the Interfaces say. Module docstring: one
  line on the layout and the five steps. The clean-up on a failed clone or install is `shutil.rmtree(building,
  ignore_errors=True)` in a `try/except CalledProcessError` around step 2.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_mia_ops_release.py tests/test_mia_ops_cli.py -v && pixi run typecheck`
Expected: PASS; mypy clean.

- [ ] **Step 5: Commit**

```bash
git add mia_ops/release.py mia_ops/cli.py tests/test_mia_ops_release.py tests/test_mia_ops_cli.py
git commit -m "mia release <tag>: clone, install, check, flip current, restart; --to runs it on another installation"
```

---

### Task 5: the config example and the docs describe the two installations

**Files:**
- Modify: `mia.yaml.example` (the `csub` block at 94-103; a `host:` line near the top; `sandbox_dir` comment),
  `docs/deployment.md` (rewrite), `docs/development.md:534-545` (the `mia up` broker paragraph), `README.md:12`
  (the link's words if they change)
- Test: `tests/test_mia_ops_manifest.py::test_example_manifest_loads` (unchanged, must still pass)

- [ ] **Step 1: `mia.yaml.example`**: a commented `# host: prod1` line with one line on what it does, below
  `sandbox_dir`; the `csub` block gains `broker: local` and commented `ssh_host`, `ssh_user`, `ssh_key`, `run_dir`
  lines with the comment text from spec §6 (what the relay runs, where the broker is, that the key is the user's);
  `sandbox_dir`'s comment says it may be absolute.

- [ ] **Step 2: Rewrite `docs/deployment.md`** as the system is after Tasks 1 to 4, no history, in this order:
  1. **The two installations** (spec §1, §2, §5): named installations under `~/<name>`, the layout block, the
     shared-versus-local path table (spec §5, copied), `host:`, GitHub repos per installation (decision 4).
  2. **The relay** (spec §6): the config, the command line, the probe at `mia up`, the key, the forced-command
     hardening as a later step.
  3. **Releases** (spec §7): `mia release <tag>`, `--to HOST:ROOT`, rollback, what a user sees, the
     `workflow.patched()` rule.
  4. **Runbook: the cutover of this installation to prod** (spec §8, the six steps), with the SQL as a code block
     with placeholder prefixes `/OLD/prefix` and `/NEW/prefix`:
     `UPDATE volumes SET data = replace(data, '/OLD/prefix', '/NEW/prefix'); UPDATE frozen SET descriptor =
     replace(descriptor, ...); UPDATE assets SET key = replace(key, ...), data = replace(data, ...); UPDATE cases SET
     data = replace(data, ...);` preceded by a `cp` of the ledger and `mia down`, and the symlink step.
  5. **Runbook: bringing up a new installation** (spec §9, the dev bring-up list), including the dev repos.
  6. **Not built: the service account** — the systemd, TLS, namespace, API-key and backup items of today's
     "Changes needed" list, kept as a list of what is still to do, without the plan prose around them.
  No real paths or hostnames; placeholders only.

- [ ] **Step 3: `docs/development.md`**: in the `mia up mia-buffers` / **Hosts** paragraphs (534-545), one sentence
  that with `csub.broker: relay` the broker window runs `csub relay` and the broker is `csub-broker` on
  `csub.ssh_host`, reading the same generated policy through the shared home; and that the socket dir is
  `csub.run_dir`.

- [ ] **Step 4: Verify**

Run: `PYTHONPATH=$PWD pixi run pytest tests/test_mia_ops_manifest.py tests/test_core_config.py -v && pixi run lint && git diff | grep -n 'rokickik\|/nrs/' ; echo "grep rc=$? (1 = clean)"`
Expected: PASS, lint clean, grep finds nothing.

- [ ] **Step 5: Commit**

```bash
git add mia.yaml.example docs/deployment.md docs/development.md README.md
git commit -m "docs: the prod and dev installations, the relay, releases and the cutover runbook"
```

---

### After the plan

Not code, done by hand with the user, in `docs/deployment.md`'s runbooks: the cutover of this installation to prod
once the data-share paths and the first tag are known; the bring-up of dev on its host with its repos. The
`pixi.lock` is what `pixi install --locked` installs, so a release is reproducible without pushing anything else.
