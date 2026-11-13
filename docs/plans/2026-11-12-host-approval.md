# Approving data hosts from the page — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a person starting a preparation or a buffer run from the page is shown the hosts the run needs that the
csub policy lacks, approves them there, and the run's first job can reach them.

**Architecture:** mia generates its own csub policy (the operator's `broker.toml` plus an append-only file of
approved hosts) and `mia up` starts every broker with `--policy` on it. buffer-api computes a run's needed hosts
with the code the jobs use, answers what is missing on a read route, and approves the hosts that arrive with a
start action before it sends the action. The page asks in one dialog. autodev agent jobs name the hosts their
broker's policy allows.

**Tech Stack:** Python 3.12, pydantic, `tomllib`, the stdlib HTTP server of buffer-api, one HTML page with plain
JavaScript, pytest.

**Spec:** `docs/specs/2026-11-12-host-approval.md` (v0.4). Read it first; section numbers below are its.

## Global Constraints

- Branch `host-approval`, in the worktree `.claude/worktrees/host-approval`. The main checkout is not touched.
- Run tests from the worktree with the main checkout's environment:
  `PY="$(git rev-parse --git-common-dir)/../.pixi/envs/default/bin/python"; $PY -m pytest tests/<file> -q`.
  A task runs only its own test files; Task 9 runs `fmt`, `lint`, `typecheck` and the full suite once.
- csub is not changed. Nothing writes the operator's `broker.toml` (`csub.base_policy`).
- No workflow, no stage table and no `models.py` field changes; no replay fixture is re-recorded.
- No lock files: the policy directory may be NFS. buffer-api alone appends approvals, holding one in-process
  lock across the append and the generation; every generation publishes from a temp file of its own and
  re-reads the approvals afterwards (Task 1).
- Action routes take no token, JSON only, this origin only. The page holds no token and calls only `read`,
  `consumer` and `action` routes (`tests/test_buffers_ui.py`).
- No test may read or write the real home directory: every test that reaches `Path.home()` sets `HOME` first.
- No real path or hostname in a tracked file (the grep of AGENTS.md stays empty). Line length 120. No `noqa`
  or `type: ignore` without a reason. Descriptive names, a plain-language line on each module, class and
  non-obvious function. Comments say what the code does. A deliberate shortcut carries `# ponytail:` with its
  ceiling and upgrade path.
- An approved host is a lowercase exact hostname: no `*`, no port, no trailing dot, never a GitHub host.
- Commit after each task, staging explicit paths. Never push.

## Review Focus

1. The operator edits `approved-hosts.jsonl` by hand and leaves a broken line: generation fails naming the file
   and the line; it never quietly drops approvals.
2. The base policy becomes invalid while services run: an approval answers 503, the generated policy on disk is
   unchanged, and the action is not sent.
3. Two people approve at the same moment (buffer-api is threaded): both hosts are in the file and in the policy.
4. `approve_hosts` carrying `Data.4TU.nl`, `data.4tu.nl.`, `*.4tu.nl`, `data.4tu.nl:443` or `github.com`: 422
   naming the host, nothing approved.
5. The base policy allows a host by pattern (`*.s3.amazonaws.com`): the dialog never offers it and generation
   does not add it twice.
6. A catalog record that names `s3://bucket/key`: the needed host is `bucket.s3.amazonaws.com`.

---

## Slice A: the policy file

### Task 1: generate the policy

**Files:** Create `mia_core/hosts.py`, `tests/test_core_hosts.py`. Modify `mia_data/binding.py` (its
`missing_hosts` becomes an import from `mia_core.hosts`; every existing importer keeps working).

**Interfaces (produces):**

```python
POLICY_NAME = "broker.toml"
APPROVALS_NAME = "approved-hosts.jsonl"
HOST_RE: re.Pattern[str]  # a lowercase exact hostname with at least one dot

class PolicyError(RuntimeError): ...

def missing_hosts(hosts: list[str], patterns: list[str]) -> list[str]
def approved_hosts(policy_dir: Path) -> list[str]            # approval order, each host once; [] without a file
def approve_hosts(
    base_policy: Path, policy_dir: Path, hosts: list[str], *, by: str, kind: str, number: int
) -> list[str]                                               # the hosts it added; the policy is regenerated
def dump_toml(data: dict) -> str
def generate_policy(base_policy: Path, policy_dir: Path) -> Path   # the generated file's path
def allowed_patterns(policy_dir: Path) -> list[str]          # broker.allowed_hosts of the generated policy
```

- An approvals line is `{"host", "at", "by", "kind", "number"}`, `at` an ISO UTC time. `approve_hosts` creates
  `policy_dir`, appends one line per host not yet in the file, regenerates the policy and returns the hosts it
  added. It holds one module-level `threading.Lock` from its first read to the publish, so two approvals in one
  process never interleave. It parses the base before it appends: a base that cannot be read records nothing.
- `approved_hosts` raises `PolicyError("<file> line <n>: …")` for a line that is not a JSON object whose `host`
  matches `HOST_RE`.
- `dump_toml` writes tables one or two deep holding strings, integers, floats, booleans and lists of strings;
  strings as `json.dumps` writes them. Anything else raises `PolicyError` naming the dotted key. Mark it
  `# ponytail:` (ceiling: the shapes a csub policy has; upgrade: `tomli-w`).
- `generate_policy` and `approve_hosts` first refuse, with a `PolicyError` naming both paths, a `policy_dir`
  whose `broker.toml` is the base policy itself: `(policy_dir / POLICY_NAME).resolve() == base_policy.resolve()`
  (so a symlinked directory is caught too). This is the one place every writer passes through.
- `generate_policy`: parse the base (`PolicyError` naming the file when it is missing or not TOML); append to
  `broker.allowed_hosts` each approved host that `missing_hosts` says the base lacks; `dump_toml`; require
  `tomllib.loads(text) == merged` (`PolicyError` otherwise). The text starts with
  `# Generated by mia from <base_policy>; edits are overwritten.` Nothing is published when any step fails.
- **Publishing** is safe against another generation in this process or another (`mia up` beside buffer-api),
  without a lock file:
  - the text goes to a temp file with a unique name in `policy_dir` (`tempfile.mkstemp`), then `os.replace`; the
    temp file is removed when a step fails. `mia_core.untrusted.write_atomic` is not used here: its temp name
    is fixed per target, so two writers of one file collide.
  - after the rename, the approvals file is read again; if it differs from what was just published, generate
    again. A generation that read older approvals and published last therefore catches up, and the policy on
    disk ends up holding every approval made before the last publish.

- [ ] **Step 1: write the failing tests** in `tests/test_core_hosts.py`, with a `BASE` text that has the shapes
  of a real policy (a `[broker]` table with `allowed_hosts = ["zenodo.org", "*.huggingface.co"]`, a string with a
  quote and a backslash, an integer, a float, a boolean, `[agents] claude = [...]`, `[queues.short]`):
  - `test_with_no_approvals_the_generated_policy_equals_the_base`: `tomllib.loads` of both are equal.
  - `test_approved_hosts_are_added_in_order_and_every_base_key_is_kept`: approve `b.example.org` then
    `a.example.org`; `allowed_hosts` ends `[..., "b.example.org", "a.example.org"]`; every other key equal.
  - `test_a_host_the_base_matches_by_pattern_is_not_added`: approve `cdn.huggingface.co`; the list is unchanged.
  - `test_a_policy_dir_that_holds_the_base_is_refused_and_the_base_keeps_its_bytes`: parametrized over
    `policy_dir` being the base's own directory and a symlink to it, for `generate_policy` and for
    `approve_hosts`; each raises `PolicyError`, the base is byte-for-byte unchanged, and no approvals file or
    temp file appears beside it.
  - `test_a_missing_or_invalid_base_is_an_error_and_writes_nothing`: `PolicyError` naming the base path; an
    earlier generated file keeps its bytes.
  - `test_a_value_the_writer_cannot_write_names_its_key`: a base with `when = 2026-01-01` under `[limits]` raises
    `PolicyError` matching `limits.when`.
  - `test_an_approval_is_recorded_once`: the same host twice gives one line; the second call returns `[]`.
  - `test_a_broken_approvals_line_names_the_file_and_line`: line 2 is `not json`; `PolicyError` matches
    `approved-hosts.jsonl line 2`.
  - `test_a_base_that_cannot_be_read_records_no_approval`: `approve_hosts` raises and no approvals file exists.
  - `test_approvals_from_many_threads_all_land_in_the_policy`: 20 threads each approve their own host; 20 lines
    in the file and all 20 hosts in the generated policy's `allowed_hosts`.
  - `test_generations_at_once_never_fail_and_never_publish_a_partial_file`: 20 threads call `generate_policy`;
    none raises, the result parses, and no temp file is left in `policy_dir`.
  - `test_a_generation_that_read_older_approvals_catches_up`: wrap `dump_toml` so its first call appends an
    approval line for `late.example.org` before returning; after `generate_policy` the policy holds that host.
- [ ] **Step 2:** run `tests/test_core_hosts.py`; expect import errors.
- [ ] **Step 3:** implement `mia_core/hosts.py`; replace the body in `mia_data/binding.py` with the import.
- [ ] **Step 4:** run `tests/test_core_hosts.py tests/test_mia_binding.py tests/test_mia_jobspec.py`; all pass.
- [ ] **Step 5:** commit: `hosts: mia generates its csub policy from the operator's and the approved hosts`.

### Task 2: the `csub` block of mia.yaml

**Files:** Modify `mia_core/config.py`, `mia_buffers/config.py`, `mia_ops/manifest.py`, `mia.yaml.example`,
`tests/test_core_config.py`, `tests/test_mia_ops_manifest.py`.

**Interfaces (produces):**

```python
class CsubCfg(BaseModel):                       # mia_core/config.py; both paths expanded and absolute
    base_policy: Path = Path("~/.config/csub/broker.toml")
    policy_dir: Path = Path("~/.local/state/csub")

def csub_config(manifest: dict) -> CsubCfg     # policy_dir defaults to <paths.state_root>/csub when that is set
```

`service_config` puts `csub_config(manifest)` into every service's dict as `csub`. `mia_buffers.config.Config`
gains `csub: CsubCfg = CsubCfg()`. `mia_ops.manifest.Manifest` gains the same field and `load_manifest` fills it
from the file's data. `mia.yaml.example` gets the commented block of spec §3 after `network:`, saying what each
path is and the two rules for `policy_dir`.

- [ ] **Step 1: failing tests.** In `tests/test_core_config.py`:
  `test_csub_policy_dir_defaults_beside_the_state_root` (`paths.state_root: /x/state` gives `/x/state/csub`;
  no `paths.state_root` gives `~/.local/state/csub` expanded; an explicit `csub.policy_dir` wins; `~` in
  `base_policy` is expanded) and `test_every_service_receives_the_csub_block`
  (`service_config(manifest, name)["csub"]` is the same for two services). In
  `tests/test_mia_ops_manifest.py`: `test_manifest_carries_the_csub_paths`.
- [ ] **Step 2:** run both files; the new tests fail.
- [ ] **Step 3:** implement.
- [ ] **Step 4:** run `tests/test_core_config.py tests/test_mia_ops_manifest.py tests/test_buffers_config.py`.
- [ ] **Step 5:** commit: `config: a csub block names the operator's policy and where mia keeps its own`.

### Task 3: `mia up` starts brokers on the generated policy; `mia exec` uses the service's broker

**Files:** Modify `mia_ops/services.py`, `mia_ops/cli.py`, `tests/test_mia_ops_services.py`,
`tests/test_mia_ops_cli.py`.

**Interfaces:** consumes `generate_policy`, `PolicyError`, `Manifest.csub`. Produces
`services.ensure_policy(stack: Stack) -> Path`: the three checks of spec §3, then `generate_policy`; each failure
is a `sys.exit` naming `csub.policy_dir` or `csub.base_policy` and the path. Home is `Path.home().resolve()`.
A `policy_dir` overlaps a state dir when either contains the other.

- `service_up` calls `ensure_policy` after its prerequisite checks and before the `is_running` return, so a
  second `mia up` regenerates. The broker's command becomes
  `csub-broker --policy <policy> --serve <socket>`.
- `cmd_exec` adds `CSUB_TRANSPORT=unix` and `CSUB_SOCKET=<svc.run>/csub.sock` to the environment when that
  socket exists.

- [ ] **Step 1:** in the `fake` fixture set `HOME` to `tmp_path` and write a minimal valid base policy to
  `tmp_path/.config/csub/broker.toml`. Then the failing tests:
  - `test_up_generates_the_policy_and_starts_the_broker_on_it` (v2 stack): the broker command contains
    `--policy {tmp_path}/state/csub/broker.toml --serve`, and that file parses equal to the base.
  - `test_up_refuses_a_policy_dir_outside_home`: `csub.policy_dir` names a directory outside `tmp_path` (the
    test's home); `SystemExit` matching `csub.policy_dir` and `home`; no tmux session is started and nothing is
    created at that path.
  - `test_up_refuses_a_policy_dir_inside_a_state_dir`: `csub: {policy_dir: <state_root>/mia-data/csub}`.
  - `test_up_refuses_a_policy_dir_that_holds_the_base_policy`: `csub: {policy_dir: <home>/.config/csub}`;
    `SystemExit` matching `csub.policy_dir` and `csub.base_policy`; the base's bytes are unchanged and no tmux
    session is started.
  - `test_up_refuses_a_missing_base_policy`: the base removed; `SystemExit` matching `csub.base_policy`.
  - `test_up_on_a_running_service_regenerates_the_policy`: delete the generated file, `service_up` again, it is
    back.
  - in `tests/test_mia_ops_cli.py`, `test_exec_uses_the_services_broker_when_it_is_running`: with
    `scratch/me/mia-data/csub.sock` present the env has both variables; without it neither.
- [ ] **Step 2:** run both files; the new tests fail, the old ones pass.
- [ ] **Step 3:** implement.
- [ ] **Step 4:** run `tests/test_mia_ops_services.py tests/test_mia_ops_cli.py`.
- [ ] **Step 5:** commit: `mia up: every broker reads the generated policy; mia exec talks to the service's broker`.

---

## Slice B: the service

### Task 4: what a run needs

**Files:** Create `mia_buffers/hosts.py`, `tests/test_buffers_hosts.py`. Modify `mia_data/binding.py`.

**Interfaces:** consumes Task 1. In `mia_data/binding.py`, `hosts(urls, cfg)` becomes a call to the new
`network_hosts(urls: list[str], network: NetworkCfg) -> list[str]` (same result, same GitHub refusal). Produces:

```python
Needed = dict[str, list[str]]   # host -> the sorted ids of the records that need it

@dataclass(frozen=True)
class HostCheck:
    """What buffer-api needs to say which hosts a run lacks, and to approve them."""
    base_policy: Path
    policy_dir: Path
    network: NetworkCfg
    storage_root: Path
    rounds_dir: Path                         # <state_dir>/rounds, where a buffer round writes plan.json

    @classmethod
    def from_config(cls, cfg: Config) -> HostCheck
    def for_records(self, records: dict[str, dict]) -> Needed     # record id -> catalog record
    def for_bindings(self, folders: dict[str, str]) -> Needed     # record id -> its folder under storage_root
    def pool(self, round_id: str) -> list[str]                    # ids of the in_pool rows of the round's plan.json
    def missing(self, needed: Needed) -> Needed
    def check(self, hosts: list[str], needed: Needed) -> None     # ValueError naming the first bad host
    def approve(self, hosts: list[str], *, by: str, kind: str, number: int) -> list[str]   # the hosts it added
```

- `for_records` is `network_hosts(record_urls(record), network)` per record. `for_bindings` reads
  `<storage_root>/<folder>/binding.yaml`, takes the `urls` of its `raw` and `labels` arrays without the
  `manual://` ones, and applies `network_hosts`; a record with no readable binding contributes nothing.
- `pool` refuses a round id that is not one (`mia_buffers.activities.ROUND_RE`) with `ValueError`, and lets
  `OSError` and `ValueError` out when the file is missing or not a plan.
- `missing` keeps the hosts `missing_hosts(..., allowed_patterns(policy_dir))` returns.
- `check` raises for a host that fails `HOST_RE`, is a GitHub host (`autodev.config.is_github_host`) or is not a
  key of `needed`. `approve` is `mia_core.hosts.approve_hosts`; `PolicyError` goes to the caller.

- [ ] **Step 1: failing tests** in `tests/test_buffers_hosts.py` (a `check(tmp_path)` helper writes a base
  policy allowing `zenodo.org` and `*.s3.amazonaws.com`, generates, and returns a `HostCheck` with
  `NetworkCfg(package_hosts=["pypi.org"], redirect_hosts={"data.example.org": ["cdn.example.net"]})`):
  - `test_a_records_needs_are_its_url_hosts_their_redirect_targets_and_the_package_hosts`: a record with
    `data.download_url: https://data.example.org/f.zip` needs `data.example.org`, `cdn.example.net`, `pypi.org`.
  - `test_an_s3_url_needs_its_bucket_host`: `s3://bucket/key` gives `bucket.s3.amazonaws.com`, and `missing`
    leaves it out.
  - `test_missing_groups_the_records_by_host`: two records on one host give `{"h": ["rec-a", "rec-b"]}`.
  - `test_a_bindings_needs_skip_manual_urls_and_unreadable_bindings`.
  - `test_the_pool_is_the_plans_in_pool_rows`, and a bad round id or a missing plan raises.
  - `test_check_refuses_a_host_that_is_not_exact_not_needed_or_github`: parametrized over the five hosts of
    Review Focus 4 plus `other.example.org`; each `ValueError` names its host.
  - `test_approve_adds_the_host_to_the_generated_policy`: afterwards `missing` no longer lists it.
- [ ] **Step 2:** run the file; import errors.
- [ ] **Step 3:** implement.
- [ ] **Step 4:** run `tests/test_buffers_hosts.py tests/test_mia_binding.py tests/test_buffers_activities.py`.
- [ ] **Step 5:** commit: `hosts: the hosts a dataset round, a request and a buffer's pool need`.

### Task 5: `POST /hosts/missing`

**Files:** Modify `mia_buffers/api.py`, `tests/test_buffers_api.py`.

**Interfaces:** consumes `HostCheck`. Produces:

- `BufferService(..., hosts: HostCheck | None = None)`; `serve` passes `HostCheck.from_config(cfg)` and calls
  `generate_policy` once before it listens (`SystemExit` with the `PolicyError`'s text).
- `class MissingBody(_Strict)`: `records: list[str] | None`, `prep: int | None`, `spec: str | None`; exactly one
  is given, else 422.
- `BufferService._needed(self, *, records: list[str] | None = None, prep: int | None = None,
  spec: str | None = None) -> Needed`, used again by Task 6:
  - `records`: each id must be a record of a synced catalog (404 naming it), looked up as `_dataset_action`
    does; then `for_records`.
  - `prep`: the request's named records (`_request_records`), as above; a request that names none needs nothing.
  - `spec`: the buffer subject of that name (404). Its round is the `round` of its newest timeline entry whose
    stage is `plan-review`; no such entry, or a plan that cannot be read, is a 409 saying there is no plan to
    read. Then `for_bindings` over the pool with each record's `folder` from `ledger.dataset`.
  - `self.hosts is None` is a 503 (`this service has no csub policy to check hosts against`).
- Route `("POST", r"^/hosts/missing$", "hosts_missing", "read")`. It answers
  `{"hosts": [{"host": ..., "records": [...]}]}` sorted by host.

- [ ] **Step 1: failing tests**, with a helper `_checking(tmp_path)` that is `_acting` plus a `HostCheck` whose
  base policy allows `zenodo.org`, and records `rec-a` (`https://data.example.org/a.zip`) and `rec-b`
  (`https://zenodo.org/b.zip`):
  - `test_hosts_missing_lists_what_the_named_records_need_and_the_policy_lacks`:
    `{"records": ["rec-a", "rec-b"]}` answers `[{"host": "data.example.org", "records": ["rec-a"]}]`;
    `{"records": ["rec-b"]}` answers `[]`; `{"records": ["nope"]}` is 404; `{}` and two keys at once are 422.
  - `test_hosts_missing_for_a_request_reads_its_records`.
  - `test_hosts_missing_for_a_buffer_reads_the_rounds_plan_and_its_bindings`: a buffer subject with a
    `plan-review` entry of round `i7-e1`, `rounds/i7-e1/plan.json` with one `in_pool` row, that dataset's
    `binding.yaml` naming `https://data.example.org/x`; then the same with the plan file removed is 409.
  - `test_hosts_missing_without_a_policy_is_503` (the `_acting` service).
  - `test_hosts_missing_writes_nothing`: no approvals file exists afterwards.
- [ ] **Step 2:** run them; they fail on the route.
- [ ] **Step 3:** implement.
- [ ] **Step 4:** run `tests/test_buffers_api.py tests/test_buffers_ui.py`.
- [ ] **Step 5:** commit: `buffer-api: POST /hosts/missing says which hosts a run needs and the policy lacks`.

### Task 6: `approve_hosts` on the actions that start jobs

**Files:** Modify `mia_buffers/api.py`, `tests/test_buffers_api.py`.

**Interfaces:** `ActionBody.approve_hosts: list[str] = []` and `SpecBody.approve_hosts: list[str] = []`.
`_act_on` gains `needed: Callable[[], Needed] | None = None`. The four callers that pass it:

| Caller | When | `needed` |
|---|---|---|
| `_dataset_action` | `go` | `_needed(records=[record])` |
| `_file_prep` | always | `_needed(records=<the records the text names>)` |
| `_prep_action` | `go` | `_needed(records=<the records of the text that runs>)` |
| `_buffer_action` | `approve` while the subject is at `plan-review` | `_needed(spec=name)` |

In `_act_on`, right after `_accepts` and only when `approve_hosts` is not empty:

1. `needed is None`: 422, `approve_hosts goes with an action that starts jobs`.
2. `self.hosts is None`: 503.
3. `self.hosts.check(...)`: `ValueError` becomes a 422 with its text.
4. `self.hosts.approve(..., by=actor, kind=kind, number=number)`: `PolicyError` becomes a 503 with its text.
5. If any host was added, `ledger.append_entry` with the body
   ``**Hosts approved:** `a`, `b` by `<actor>` `` (hosts through `neutralize`) and the marker
   `<!-- mia: hosts <time.time_ns()> -->`.

Then the action is recorded and sent as before. `_dataset_action` and `_file_prep` can create their subject, so
they run steps 1 to 3 before they do: a refused host leaves no new dataset subject and files no request.

- [ ] **Step 1: failing tests** (with `_checking`):
  - `test_a_go_with_approve_hosts_approves_them_then_starts_the_round`: 202; the timeline bodies are
    ``["**Hosts approved:** `data.example.org` by `page`", "**Action:** `go` by `page`"]``; the approvals line
    has `by: page`, `kind: dataset` and the subject's number; `/hosts/missing` for `rec-a` is now `[]`; one
    action sent.
  - `test_a_host_the_run_does_not_name_is_refused_and_nothing_happens`: parametrized over the hosts of Review
    Focus 4 and `other.example.org`; 422 naming the host; no approvals file, no subject, nothing sent.
  - `test_approve_hosts_on_an_action_that_starts_nothing_is_422`: `cancel` on a dataset at `testing`.
  - `test_a_refused_action_approves_nothing`: `go` at `testing` is 409 and the approvals file does not exist.
  - `test_a_policy_that_cannot_be_generated_is_503_and_nothing_is_sent`: overwrite the base with `not toml`;
    503; the generated policy's bytes are unchanged; `gateway.sent == []`.
  - `test_a_repeat_of_the_key_adds_no_line_and_no_entry`.
  - `test_filing_a_request_approves_its_hosts`, and with a bad host no `prep` subject exists.
  - `test_running_a_buffers_plan_approves_its_hosts`: `approve` with the revision at `plan-review`.
  - `test_two_approvals_at_once_both_land`: two threads, two datasets, two hosts; both in the generated policy.
- [ ] **Step 2:** run them; they fail.
- [ ] **Step 3:** implement.
- [ ] **Step 4:** run `tests/test_buffers_api.py`.
- [ ] **Step 5:** commit: `buffer-api: a start action carries the hosts a person approved for it`.

---

## Slice C: the page

### Task 7: the dialog and its four entry points

**Files:** Modify `mia_buffers/ui/index.html`, `tests/test_buffers_ui.py`.

**Interfaces:** `async function askHosts(run, send)`. `run` is the body for `POST /hosts/missing`
(`{records}`, `{prep}` or `{spec}`). It calls `send(hosts)` once: with `[]` when nothing is missing or the
person starts without approving, with the ticked hosts on "Approve and start". On Cancel, and when the check
fails (a banner: `Hosts not checked (<status>): <message>. Nothing was started.`), `send` is not called.

- The dialog is built like `askReject`: a `<dialog class="ask">` appended to `document.body`, removed on close.
  Its title is `Hosts to approve`; its first line is
  `Jobs of this deployment may download from an approved host from then on.`
- One row per host: a checkbox, unticked, the host, and `<n> dataset(s)` with the record ids as its tooltip.
  Above the rows a `Select all` checkbox that ticks and unticks every row.
- Buttons and their hover texts, as new `BUTTON_HELP` entries:
  - `approveHosts`, "Approve and start", disabled until a host is ticked:
    `Allow jobs to download from the ticked hosts, then start.`
  - `startUnapproved`, "Start without approving":
    `Start with the policy as it is. A dataset on one of these hosts stops and needs a person; a buffer waits
    for that dataset's files to be copied in by hand, or refuses its job.`
  - `cancelHosts`, "Cancel": `Close this without starting anything.`
- Entry points, each sending `approve_hosts` only when the list is not empty:
  - `actionBar`: a dataset's `go` (`{records: [subject.key]}`), a request's `go` (`{prep: subject.number}`), a
    buffer's `approve` at `plan-review` (`{spec: subject.key}`). The buttons are disabled and relabelled only
    once `send` runs, so Cancel leaves the bar usable.
  - `prepareButton`: after its `confirm`, `{records: todo}`; the `POST /prep` body gains `approve_hosts`.

- [ ] **Step 1: failing tests** in `tests/test_buffers_ui.py`:
  - `test_the_page_asks_which_hosts_a_run_lacks_before_it_starts`: `("POST", "/hosts/missing")` is in
    `requested_paths()` and resolves to `read`; `askHosts(` occurs at least 3 times (its definition, the action
    bar, `prepareButton`); `{ records: [subject.key] }`,
    `{ prep: subject.number }`, `{ spec: subject.key }` and `{ records: todo }` are in the page.
  - `test_the_hosts_dialog_offers_approve_start_anyway_and_cancel`: the three button words, `Select all`,
    `Hosts to approve`, the three `BUTTON_HELP` keys, and `approveButton.disabled = ` are in the page.
  - `test_an_approval_travels_with_the_start_action`: `approve_hosts` is in the page and
    `"/hosts/approve"` is not.
  - Fix the count in `test_an_asked_action_holds_the_buttons_until_its_workflow_answers` only if this task
    changed how many buttons read `disabled: !!subject.asked`.
- [ ] **Step 2:** run the file; the new tests fail.
- [ ] **Step 3:** implement.
- [ ] **Step 4:** run `tests/test_buffers_ui.py`; then load the page from a dev service (`mia-buffers serve`
  against a scratch ledger) and click through one dataset whose host is missing: the dialog lists it, Select
  all ticks it, Approve and start starts the round, and the timeline shows the approval above the action.
- [ ] **Step 5:** commit: `page: starting a run asks for the hosts it lacks`.

---

## Slice D: autodev

### Task 8: an agent job names the hosts its broker allows

**Files:** Modify `autodev/agent.py`, `mia.yaml.example`, `tests/test_autodev_agent.py`.

**Interfaces (produces):**

```python
MAX_JOB_HOSTS = 64   # csub's limit on a job's allow_hosts
def agent_hosts(configured: list[str], policy_hosts: list[str]) -> list[str]
```

`agent_hosts` returns the configured hosts in order, then the policy's hosts in policy order, leaving out
patterns (`*`), GitHub hosts and repeats. Over 64 in all, it drops policy hosts from the front (the oldest) until
64 remain; configured hosts are never dropped. Mark the cap `# ponytail:` (upgrade: a per-issue host list, or a
higher limit in csub). `AgentJobs.submit` reads `self.jobs.client.probe().allowed_hosts` in a thread before it
builds the spec and passes `agent_hosts(...)` as `allow_hosts`. The judge job keeps `allow_hosts=[]`. In
`mia.yaml.example`, the `snippets` block's `agent.allow_hosts` keeps the package hosts only, with a comment that
the policy's hosts are added.

- [ ] **Step 1: failing tests:** give `FakeCsub` a `policy_hosts` list (default empty) and a `probe()`.
  - `test_an_agent_job_names_its_configured_hosts_then_the_policys`: configured `["pypi.org"]`, policy
    `["pypi.org", "zenodo.org", "*.huggingface.co", "github.com", "data.4tu.nl"]` gives
    `["pypi.org", "zenodo.org", "data.4tu.nl"]`.
  - `test_past_the_cap_the_newest_policy_hosts_are_kept`: 2 configured and 70 policy hosts `h0`…`h69` give 64
    hosts: the 2 configured, then `h8`…`h69`.
  - the existing `test_submit_is_idempotent_and_scoped` still expects `["pypi.org"]`.
- [ ] **Step 2:** run the file; the new tests fail.
- [ ] **Step 3:** implement.
- [ ] **Step 4:** run `tests/test_autodev_agent.py tests/test_autodev_activities.py`.
- [ ] **Step 5:** commit: `autodev: an agent job names the hosts its broker's policy allows`.

---

### Task 9: the docs, then everything

**Files:** Modify `AGENTS.md`, `docs/development.md`, `README.md`.

- [ ] **Step 1:** `AGENTS.md`, under "Rules that will bite you", one rule: **Only a person's click approves a
  host.** It says that brokers read `<csub.policy_dir>/broker.toml`, generated from the operator's policy and
  `approved-hosts.jsonl`; that buffer-api alone appends to that file and only hosts the run's records name; that
  nothing writes the operator's `broker.toml`; and that `policy_dir` stays under home.
- [ ] **Step 2:** `docs/development.md` and `README.md`: where the brokers are described, say that they run on
  the generated policy and how to withdraw an approval (delete its lines, `mia up`). Describe the system as it
  is.
- [ ] **Step 3:** `pixi run fmt`, `pixi run lint`, `pixi run typecheck`, `pixi run test` from the worktree with
  the main checkout's environment; all clean, and the real-path grep of AGENTS.md over `git diff main...` is empty.
- [ ] **Step 4:** commit: `docs: hosts are approved from the page into mia's own csub policy`.

After the last task: spec §10's rollout (`mia down` and `mia up` each service once) is the operator's step and is
not part of this plan.
