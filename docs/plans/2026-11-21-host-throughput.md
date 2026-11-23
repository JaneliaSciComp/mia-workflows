# What each host gives us: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record, per host and clock minute, the load every harness job put on a host and what came back, keep it
in the job's record and in the ledger, and serve it from three read routes.

**Architecture:** The harness's `IO` counts every attempt in a meter that appends one JSON line per host per closed
minute to a file in the job's workspace. The job runner tells every job where that directory is, reads it when the
job ends, reduces it to typed rows, stores them in the job's record and hands them to a sink (the worker's
registry client). buffer-api keeps the rows in a `transfers` table and computes host summaries on read.

**Tech Stack:** Python 3.12, httpx (harness), SQLite (ledger), pytest. No new dependency.

**Spec:** `docs/specs/2026-11-21-host-throughput.md` (v0.2). Section numbers below are the spec's.

## Global Constraints

- **Two repositories.** Part A is mia-snippets (`harness/`), Part B is mia-workflows. Part A deploys first (§6); a
  job on an older harness writes nothing and Part B treats that as no rows.
- **Where the work happens.** Part B: this worktree, branch `host-throughput`. Part A: a new worktree of the
  mia-snippets dev checkout, branch `host-throughput` from `origin/main` (fetch first; it must contain the merged
  stage-on-demand commit, PR #296). Never commit in the `harness.dir` checkout the workers run from.
- **No push, no PR, no restart** by the implementer. Rollout (last section) is the user's.
- **No workflow code changes**: nothing in any `workflows.py`, no activity result dataclass, no replay fixture.
- **Nothing stored says what an answer meant**: no list of refusal codes, no `throttled` label, anywhere.
- **Recording never fails, waits or slows a job or a wait**: every write and delivery is best effort and logged.
- **Job output is untrusted**: `read_untrusted`, typed reduction, the bounds of §5.2.
- **No real paths or hostnames in tracked files**: `git diff | grep -n 'rokickik\|/nrs/'` is empty. Public
  archive hosts in tests (`a.org`) are fine.
- Limits, verbatim from the spec: 256 files, 8 MiB, 20,000 rows per job; counts 0 to 1,000,000; `bytes` 0 to
  2^50; seconds fields 0 to 86,400; 32 keys per map; a key is three digits or one of `timeout`, `connect`,
  `dropped`, `truncated`; a minute not after collection and not more than 7 days before it; a `4xx` naming no
  wait is asked once more after 16 s; a `404` or `410` is final at once.
- Style: descriptive names, a plain line on each module, class and non-obvious function, line length 120, docs
  and comments say what the code does. A small change gets the tests that cover it, not the full suite; the full
  suite runs once at the end of each part.
- Commands. Part A, from `harness/`: `pixi run python -m pytest tests/<file> -q`. Part B, from this worktree:
  `PYTHONPATH=$PWD pixi run pytest tests/<file> -q` (without `PYTHONPATH` pixi imports the main checkout).

## Review Focus

Inputs the spec implies and its test list (§7) does not name. Each has its test in the task that owns the code.

1. **A harness process killed while writing** leaves a cut last line. The collector drops that line and keeps the
   rest (Task 4).
2. **The transfers directory cannot be written** (it is a file, or the disk is full). Reads go on, one warning,
   no exception (Task 1).
3. **The node's clock steps back** into a minute already written. That minute is not written again; the event
   lands in the next unwritten minute (Task 1).
4. **A job plants a symlink** as its `transfers` directory or as a file in it. Nothing outside the workspace is
   read (Task 4).
5. **Four threads count at once** (`PARALLEL`). The totals equal what the server saw (Task 2).

## File Structure

| File | Repo | Responsibility |
|---|---|---|
| `harness/src/mia_harness/meter.py` (new) | snippets | The meter: counts per host and minute, writes closed minutes |
| `harness/src/mia_harness/io.py` | snippets | Tells the meter of every attempt, byte and wait; the retry rules |
| `harness/tests/test_meter.py` (new), `tests/test_io.py`, `tests/rangeserver.py` | snippets | Tests |
| `mia_core/transfers.py` (new) | workflows | Reading a job's transfer files into bounded typed rows |
| `mia_core/jobrecord.py` | workflows | The record gets the rows; listing records that ended lately |
| `autodev/jobs.py` | workflows | The env variable at submit; the sink at the end of the wait |
| `mia_buffers/ledger.py` | workflows | Table `transfers`, put and read |
| `mia_buffers/throughput.py` (new) | workflows | The arithmetic of the read routes, pure functions |
| `mia_buffers/api.py` | workflows | `PUT /transfers/<job>`, `GET /hosts…` |
| `mia_core/registry.py` | workflows | `put_transfers`; putting again at a worker's start |
| `autodev/activities.py`, `mia_data/activities.py`, `mia_buffers/activities.py`, the three `cli.py` | workflows | Set the sink; pass the work root to the start-up report |
| `docs/development.md`, `AGENTS.md` | workflows | The record's new fields, the routes, the rule |

---

# Part A: the harness measures (mia-snippets)

### Task 1: The meter

**Files:**
- Create: `harness/src/mia_harness/meter.py`
- Test: `harness/tests/test_meter.py`

**Interfaces:**
- Produces:

```python
def failed(outcome: str) -> bool            # any outcome but a "2xx" or "3xx" status text

class Meter:
    directory: Path | None
    def __init__(self, directory: Path | None = None, clock: Callable[[], float] = time.time, timer: bool = True)
    def sent(self, host: str) -> None                             # an attempt goes out: `requests`, one more open
    def received(self, host: str, count: int) -> None             # body bytes arrived
    def ended(self, host: str, outcome: str) -> None              # an attempt closed: `answers`, one fewer open
    def waited(self, host: str, seconds: float, asked: bool) -> None   # a sleep about to begin
    # `failures`: (host, outcome) of each failed attempt of one request that has now ended
    def settled(self, failures: list[tuple[str, str]], served: bool) -> None
    def tick(self) -> None                                        # write every minute that has ended
    def close(self) -> None                                       # write the open minute too; stop the timer
```

Decisions the tests do not show:

- With `directory=None` every method returns at once: nothing is counted, no thread runs.
- An event is counted in minute `max(floor(clock() / 60), last written minute + 1)`, and time that went
  backwards adds no connection-seconds.
- One line per host per minute in which any field is not zero, fields exactly as §5.1's table, plus `minute` and
  `host`: `{"minute", "host", "requests", "connection_seconds", "peak", "bytes", "answers", "recovered", "final",
  "asked_wait_seconds", "backoff_seconds"}`. `connection_seconds` is rounded to 3 places.
- `peak` of a minute counts the attempts already open when it began.
- The file is `<directory>/<pid>-<start in milliseconds>.jsonl`, created with its directory on the first write,
  appended to and flushed at each write.
- A write that fails is logged once (`logging.getLogger(__name__).warning`) and the meter stops writing; it never
  raises.
- The timer is a daemon thread started in `__init__` when there is a directory and `timer` is true. It waits on a
  `threading.Event` for `60 - clock() % 60` seconds (at least 0.05), calls `tick()`, and ends when `close()` sets
  the event. `close()` twice is harmless.
- All methods take one lock; `IO` calls them from `PARALLEL` threads.

- [ ] **Step 1: Write the failing tests** in `harness/tests/test_meter.py`

```python
class Clock:
    def __init__(self, now=6000.0):        # 6000 s is the start of minute 100
        self.now = now

    def __call__(self):
        return self.now


def rows(directory):
    paths = sorted(directory.glob("*.jsonl")) if directory.is_dir() else []
    return [json.loads(line) for path in paths for line in path.read_text().splitlines()]


def test_a_minute_holds_what_happened_in_it(tmp_path):
    clock = Clock()
    m = Meter(tmp_path / "t", clock, timer=False)
    m.sent("a.org"); clock.now += 10; m.received("a.org", 500); clock.now += 10; m.ended("a.org", "206")
    m.sent("a.org"); m.sent("a.org"); clock.now += 5; m.ended("a.org", "503"); m.ended("a.org", "timeout")
    m.waited("a.org", 7, asked=True); m.waited("a.org", 4, asked=False)
    m.close()
    assert rows(tmp_path / "t") == [{
        "minute": 100, "host": "a.org", "requests": 3, "connection_seconds": 30.0, "peak": 2, "bytes": 500,
        "answers": {"206": 1, "503": 1, "timeout": 1}, "recovered": {}, "final": {},
        "asked_wait_seconds": 7.0, "backoff_seconds": 4.0}]


def test_an_attempt_open_across_a_boundary_gives_each_minute_its_seconds(tmp_path):
    clock = Clock(6050.0)
    m = Meter(tmp_path / "t", clock, timer=False)
    m.sent("a.org")
    clock.now = 6130.0
    m.tick()                                   # minutes 100 and 101 are on disk while the attempt is open
    written = rows(tmp_path / "t")
    assert [(r["minute"], r["requests"], r["connection_seconds"], r["peak"], r["answers"]) for r in written] == [
        (100, 1, 10.0, 1, {}), (101, 0, 60.0, 1, {})]
    m.received("a.org", 9); m.ended("a.org", "200"); m.close()
    after = rows(tmp_path / "t")
    assert after[:2] == written                # a written minute is never changed
    assert (after[2]["minute"], after[2]["requests"], after[2]["connection_seconds"], after[2]["bytes"],
            after[2]["answers"]) == (102, 0, 10.0, 9, {"200": 1})


def test_recovered_and_final_are_written_in_the_minute_the_request_ends(tmp_path):
    clock = Clock()
    m = Meter(tmp_path / "t", clock, timer=False)
    m.sent("a.org"); m.ended("a.org", "403"); clock.now += 60
    m.sent("a.org"); m.ended("a.org", "503"); clock.now += 60
    m.sent("a.org"); m.ended("a.org", "200")
    m.settled([("a.org", "403"), ("a.org", "503")], served=True)
    m.settled([("b.org", "dropped")], served=False)
    m.close()
    by = {(r["minute"], r["host"]): r for r in rows(tmp_path / "t")}
    assert by[(100, "a.org")]["answers"] == {"403": 1} and by[(100, "a.org")]["recovered"] == {}
    assert by[(102, "a.org")]["recovered"] == {"403": 1, "503": 1}
    assert by[(102, "b.org")]["final"] == {"dropped": 1} and by[(102, "b.org")]["requests"] == 0


def test_the_clock_going_back_changes_no_written_minute(tmp_path):
    clock = Clock()
    m = Meter(tmp_path / "t", clock, timer=False)
    m.sent("a.org"); m.ended("a.org", "200")
    clock.now = 6060.0; m.tick()
    clock.now = 5990.0                         # back into minute 99
    m.sent("a.org"); m.ended("a.org", "200"); m.close()
    assert [r["minute"] for r in rows(tmp_path / "t")] == [100, 101]


def test_a_directory_that_cannot_be_written_harms_nothing(tmp_path, caplog):
    (tmp_path / "t").write_text("a file where the directory should be")
    m = Meter(tmp_path / "t", Clock(), timer=False)
    m.sent("a.org"); m.ended("a.org", "200"); m.close(); m.close()
    assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1


def test_the_timer_writes_a_closed_minute_while_the_process_runs(tmp_path):
    clock = Clock(6059.9)
    m = Meter(tmp_path / "t", clock)           # its timer first waits 0.1 s
    m.sent("a.org"); m.ended("a.org", "200")
    clock.now = 6061.0
    deadline = time.time() + 5
    while not rows(tmp_path / "t") and time.time() < deadline:
        time.sleep(0.05)
    assert [r["minute"] for r in rows(tmp_path / "t")] == [100]
    m.close()
```

Also: `test_a_meter_without_a_directory_counts_nothing` (no file anywhere, no thread: `threading.active_count()`
unchanged); `test_each_process_writes_its_own_file` (two meters on one directory leave two files, each name
matching `^\d+-\d+\.jsonl$`); `test_failed_is_any_outcome_but_2xx_and_3xx` (`"200"`, `"206"`, `"302"` false;
`"403"`, `"503"`, `"timeout"`, `"truncated"` true).

- [ ] **Step 2: Run them, expect failure** (`No module named 'mia_harness.meter'`):
  `pixi run python -m pytest tests/test_meter.py -q`
- [ ] **Step 3: Implement `meter.py`.** Keep per host the number open and the time it was last integrated; on
  every event and on `tick`, first add `open × (elapsed within each minute)` up to now, minute by minute.
- [ ] **Step 4: Run them, expect pass.**
- [ ] **Step 5: Commit** `harness: a meter of what each host is asked and gives, per minute`
  (`harness/src/mia_harness/meter.py`, `harness/tests/test_meter.py`).

### Task 2: `IO` tells the meter

**Files:**
- Modify: `harness/src/mia_harness/io.py` (`IO.__init__`, `close`, `_send`, `_head_once`, `_get`, `from_env`)
- Test: `harness/tests/test_io.py`

**Interfaces:**
- Consumes: `Meter` (Task 1).
- Produces: `IO(..., client=None, meter: Meter | None = None)` with `io.meter` (a `Meter()` when none is given);
  `IO.close()` closes it. `IO._answer(method, url, headers=None)`: a context manager that yields the response of
  `_send`, closes it, and tells the meter how the attempt ended. Task 3 uses `_answer` and `_send` as the two
  places where an attempt ends.

How an attempt's outcome is named (§5.1):

| When | Outcome |
|---|---|
| `client.send` raises `httpx.TimeoutException` | `timeout` |
| it raises `httpx.ConnectError` | `connect` |
| it raises any other `httpx.TransportError` | `dropped` |
| a redirect hop | its status as text, at the host that answered it |
| the `_answer` block ends with `httpx.HTTPStatusError` | that status as text |
| the block ends with `httpx.TimeoutException` | `timeout` |
| the block ends with `httpx.RemoteProtocolError` or an `OSError` (a body shorter than announced, or a range that starts elsewhere) | `truncated` |
| the block ends with any other `httpx.TransportError` | `dropped` |
| anything else, an exception of the harness's own included (`NeedsFetch`, the cap) | the status as text |

The host of an attempt is the lower-case host of the URL it was sent to. `_send` calls `meter.sent` just before
`client.send`, and `meter.waited(host, pause, asked=True)` before its rate-limit pause. `_get` calls
`meter.received` for each chunk, kept or not. `_head_once` and `_get` use `self._answer(...)` where they use
`closing(self._send(...))`. `from_env` builds `Meter(Path(env["MIA_HARNESS_TRANSFERS_DIR"]))` when the variable
is set and not empty, else `Meter(work / "transfers")`.

- [ ] **Step 1: Write the failing tests.** `make_io` gives each `IO` a
  `Meter(tmp_path / "transfers", timer=False)`. Add this helper beside it:

```python
def totals(io, tmp_path):
    """What the io's meter wrote, added up per host, after closing it."""
    io.meter.close()
    out = {}
    for path in sorted((tmp_path / "transfers").glob("*.jsonl")):
        for row in map(json.loads, path.read_text().splitlines()):
            host = out.setdefault(row["host"], {"answers": {}, "recovered": {}, "final": {}})
            for name, value in row.items():
                if isinstance(value, dict):
                    for key, count in value.items():
                        host[name][key] = host[name].get(key, 0) + count
                elif name == "peak":
                    host[name] = max(host.get(name, 0), value)
                elif name not in ("minute", "host"):
                    host[name] = host.get(name, 0) + value
    return out


def test_requests_bytes_and_answers_are_counted_per_host(server, make_io, tmp_path):
    server.files["/d.bin"] = DATA
    io = make_io()
    io.open(server.url("/d.bin")).read(BLOCK)
    host = totals(io, tmp_path)["127.0.0.1"]
    assert host["requests"] == len(server.requests) == 2 and host["bytes"] == BLOCK
    assert host["answers"] == {"200": 1, "206": 1} and host["peak"] == 1 and host["connection_seconds"] > 0


def test_a_body_cut_short_is_truncated_and_no_206(server, make_io, tmp_path, monkeypatch):
    monkeypatch.setattr(io_module, "RETRY_WAIT", (0, 0))
    server.files["/d.bin"], server.fail = DATA, 1
    io = make_io()
    io.open(server.url("/d.bin")).read(BLOCK)
    host = totals(io, tmp_path)["127.0.0.1"]
    assert host["answers"] == {"200": 1, "truncated": 1, "206": 1}
    assert BLOCK < host["bytes"] <= BLOCK + BLOCK // 2 and io.bytes_fetched == BLOCK   # delivered, not kept


def test_a_redirect_hop_is_an_attempt_at_the_host_that_answered_it(server, make_io, tmp_path):
    server.files["/d.bin"] = DATA
    server.redirects["/r.bin"] = f"http://localhost:{server.port()}/d.bin"
    io = make_io(hosts=("127.0.0.1", "localhost"))
    assert io.fetch(server.url("/r.bin")).read_bytes() == DATA
    by = totals(io, tmp_path)
    assert set(by["127.0.0.1"]["answers"]) == {"302"} and by["127.0.0.1"]["bytes"] == 0
    assert by["localhost"]["bytes"] == len(DATA)


def test_counts_are_exact_with_four_requests_at_once(server, make_io, small_pieces, tmp_path):
    names = [f"/{i}.bin" for i in range(6)]
    server.files.update(dict.fromkeys(names, DATA))
    server.delay = 0.02
    io = make_io(cap=100 * BLOCK)
    io.fetch_all([server.url(name) for name in names])
    host = totals(io, tmp_path)["127.0.0.1"]
    assert host["requests"] == len(server.requests) == sum(host["answers"].values())
    assert host["bytes"] == 6 * len(DATA) and 1 < host["peak"] <= 4
```

  (`server.port` is a method or a property in `rangeserver.py`: use it as that file defines it.) Also:
  `test_a_refused_connection_is_counted_as_connect` (a URL on a port nothing listens on, `RETRY_WAIT` `(0, 0)`:
  `httpx.ConnectError` is raised and `answers == {"connect": 3}`);
  `test_the_pause_for_a_nearly_used_window_is_an_asked_wait` (the set-up of
  `test_requests_pause_until_the_reset_when_the_server_says_few_are_left`, one read: `25 <
  asked_wait_seconds <= 30`, `backoff_seconds == 0`); and in `test_from_env`: with
  `MIA_HARNESS_TRANSFERS_DIR` set, `io.meter.directory` is that path; without it, `work / "transfers"`.

- [ ] **Step 2: Run, expect failure:** `pixi run python -m pytest tests/test_io.py -q`
- [ ] **Step 3: Implement** as the Interfaces block says.
- [ ] **Step 4: Run `tests/test_io.py` and `tests/test_meter.py`, expect pass.**
- [ ] **Step 5: Commit** `harness: every attempt, byte and rate-limit pause is metered per host`.

### Task 3: What is asked again, and what a failure came to

**Files:**
- Modify: `harness/src/mia_harness/io.py` (`_retry`, `_send`, `_answer`), `harness/tests/rangeserver.py`
- Test: `harness/tests/test_io.py`

**Interfaces:**
- Consumes: `Meter.settled`, `Meter.waited`, `failed` (Task 1); `_answer`, `_send` (Task 2).
- Produces: the retry rules of §5.1, nothing a later task calls.

Decisions:

- `IO` keeps the failed attempts of the request a thread is making in a `threading.local`: `_retry` puts an
  empty list there when it starts and puts back what was there when it ends (a `_retry` inside another is its own
  request); `_send` and `_answer` append `(host, outcome)` for each attempt whose outcome is `failed`. An attempt
  that fails outside any `_retry` is settled at once as `final`.
- `_retry` calls `meter.settled(failures, served=True)` when the attempt returns and
  `meter.settled(failures, served=False)` when it gives up, and before each sleep
  `meter.waited(host of the last failed attempt, pause, asked)`; `asked` is true when the pause came from a
  header. A failure that is no attempt (a local `OSError`) sleeps as today and counts no wait.
- The rules, first that fits, for the exception `_retry` catches:
  1. status `404` or `410`: raise.
  2. status `429`, or any other status whose `Retry-After` gives `_until(...) > 0`: today's `429` branch (for a
     `429` the `X-RateLimit-Reset` header counts as before; for another status `Retry-After` alone). A
     `Retry-After` of `0` or one that does not parse names no wait.
  3. any other status under `500`: raise if this request has had its repeat, else sleep `RETRY_WAIT[-1]`.
  4. anything else: as today, the next of `RETRY_WAIT`, raise when used up.
  The repeat of rule 3, the iterator of rule 4 and the `waited` total of rule 2 are locals of one `_retry` call.
- `rangeserver.py` gains `fail_modes: list[str]`: while it is not empty, each request for a file's path takes its
  first item as that request's `fail_mode` (`"ok"` answers normally), before `fail` is looked at.

- [ ] **Step 1: Write the failing tests** (each takes `sleeps`; `totals` from Task 2). A `HEAD` answered `403` is
  not an error (`HEAD_REFUSED`), so each test has the `HEAD` answered first and fails the `GET`s after it:

```python
def opened(server, make_io, path="/d.bin"):
    """An io and a file of it whose validators are known: what fails from here on is a GET."""
    server.files[path] = DATA
    io = make_io()
    return io, io.open(server.url(path))


def test_a_403_is_asked_once_more_and_counted_as_recovered(server, make_io, sleeps, tmp_path):
    io, f = opened(server, make_io)
    server.fail, server.fail_mode = 1, "403"
    assert f.read(10) == DATA[:10]
    host = totals(io, tmp_path)["127.0.0.1"]
    assert sleeps == [16.0] and host["recovered"] == {"403": 1} and host["final"] == {}
    assert host["backoff_seconds"] == 16.0 and host["asked_wait_seconds"] == 0


def test_a_403_twice_is_final(server, make_io, sleeps, tmp_path):
    io, f = opened(server, make_io)
    server.fail, server.fail_mode = 2, "403"
    with pytest.raises(httpx.HTTPStatusError):
        f.read(10)
    assert server.gets() == ["/d.bin"] * 2 and totals(io, tmp_path)["127.0.0.1"]["final"] == {"403": 2}


def test_the_one_repeat_is_not_renewed_by_another_failure_between(server, make_io, sleeps, tmp_path):
    io, f = opened(server, make_io)
    server.fail_modes = ["403", "503", "403"]
    with pytest.raises(httpx.HTTPStatusError) as refused:
        f.read(10)
    assert refused.value.response.status_code == 403 and sleeps == [16.0, 1.0] and len(server.gets()) == 3
    assert totals(io, tmp_path)["127.0.0.1"]["final"] == {"403": 2, "503": 1}


@pytest.mark.parametrize("status", ["404", "410"])
@pytest.mark.parametrize("headers", [{}, {"Retry-After": "5"}])
def test_a_404_or_410_is_final_at_once_whatever_it_carries(server, make_io, sleeps, tmp_path, status, headers):
    io, f = opened(server, make_io)
    server.fail, server.fail_mode, server.extra_headers = 3, status, headers
    with pytest.raises(httpx.HTTPStatusError):
        f.read(10)
    assert sleeps == [] and len(server.gets()) == 1
    assert totals(io, tmp_path)["127.0.0.1"]["final"] == {status: 1}


@pytest.mark.parametrize("status", ["403", "503"])
def test_any_status_that_names_a_wait_is_waited_for_like_a_429(server, make_io, sleeps, tmp_path, monkeypatch, status):
    io, f = opened(server, make_io)
    server.fail, server.fail_mode, server.extra_headers = 5, status, {"Retry-After": "7"}
    assert f.read(10) == DATA[:10]                                   # five waits: more than either other rule gives
    host = totals(io, tmp_path)["127.0.0.1"]
    assert sleeps == [7] * 5 and host["asked_wait_seconds"] == 35 and host["recovered"] == {status: 5}
    monkeypatch.setattr(io_module, "RATE_PATIENCE", 20)
    _, g = opened(server, make_io, "/e.bin")
    server.fail = 5
    with pytest.raises(httpx.HTTPStatusError):                       # a third wait would pass 20 s
        g.read(10)
    assert server.gets().count("/e.bin") == 3
```

  (`io_module` is `mia_harness.io`, imported at the top of the test file.) Also: in
  `test_a_429_without_a_usable_hint_backs_off`, `backoff_seconds == 5.0` and `asked_wait_seconds == 0`; in
  `test_a_host_that_refuses_head_is_asked_for_its_first_byte`, `recovered` holds the refused `HEAD`'s status
  once (a failed attempt of a request that was then served). Change
  `test_a_host_that_refuses_the_get_too_is_an_error`: it takes `sleeps`, `server.gets() == ["/d.bin"] * 2`,
  `sleeps == [16.0]`, `final == {"403": 4}` (two `HEAD`s, two `GET`s).

- [ ] **Step 2: Run, expect failure.**
- [ ] **Step 3: Implement.** Rewrite `_retry`'s docstring to state the four rules as they are.
- [ ] **Step 4: Run the whole harness suite once:** `pixi run test`. A test elsewhere that meets a `4xx` other
  than `404` now sleeps 16 s for real: give it the `sleeps` fixture (move the fixture to `conftest.py` if another
  file needs it) and correct its expected request count. Expected: all pass, no test slower than before.
- [ ] **Step 5: Commit** `harness: a 4xx naming no wait is asked once more; failures are counted as recovered or final`.

---

# Part B: the job runner collects, the ledger keeps (mia-workflows)

### Task 4: Reading a job's transfers

**Files:**
- Create: `mia_core/transfers.py`
- Test: `tests/test_core_transfers.py`

**Interfaces:**
- Produces:

```python
TRANSFERS_DIR = "transfers"
TRANSFERS_ENV = "MIA_HARNESS_TRANSFERS_DIR"
MAX_FILES, MAX_BYTES, MAX_ROWS = 256, 8 << 20, 20_000
MAX_COUNT, MAX_BODY_BYTES, MAX_SECONDS, MAX_KEYS = 1_000_000, 1 << 50, 86_400, 32
KINDS = frozenset({"timeout", "connect", "dropped", "truncated"})
WINDOW_S = 7 * 86_400

def reduce_row(value: Any) -> dict | None
def collect(workspace: Path, allow_hosts: list[str] | None, now: float) -> dict
```

- `reduce_row`: the typed row of §5.1 or None. `minute` (an int, 0 or more) and `host` (a non-empty string of at
  most 253 characters, returned in lower case) are required. `requests` and `peak` are ints from 0 to
  `MAX_COUNT`, `bytes` an int from 0 to `MAX_BODY_BYTES`, the three seconds fields finite numbers from 0 to
  `MAX_SECONDS`; `answers`, `recovered`, `final` are maps of at most `MAX_KEYS` keys, each key three digits or
  one of `KINDS`, each value an int from 0 to `MAX_COUNT`. A `bool` is not an int. A field that is absent reads
  as 0 or `{}`; any other field is left out of the result. It checks nothing about the host list or the time.
- `collect` returns one of:
  - `{}`: `<workspace>/transfers` is not a real directory inside the workspace, or holds no file to read;
  - `{"transfers_refused": "<why, one sentence>"}`: the directory has more than `MAX_FILES` entries of any name;
    the files hold more than `MAX_BYTES` in all; more than `MAX_ROWS` rows are left after adding up; or
    `allow_hosts` is None while there are files to read;
  - `{"transfers": [rows], "transfers_dropped": n}` otherwise, rows sorted by (`minute`, `host`).
- Only entries named `<digits>-<digits>.jsonl` are read, in order of name, each with `read_untrusted`. A line is
  dropped (and counted) when it is not JSON, `reduce_row` gives None, its host is not in `allow_hosts` (compared
  in lower case), or its minute is after `now // 60` or before `(now - WINDOW_S) // 60`.
- Rows of one host and minute are added, `peak` too; a sum over its field's limit is cut to it, and a map that
  grows past `MAX_KEYS` keeps its first `MAX_KEYS` keys in sorted order.

- [ ] **Step 1: Write the failing tests.**

```python
NOW = 6_000_000.0                     # minute 100000
OK = {"minute": 100000, "host": "a.org", "requests": 2, "connection_seconds": 3.5, "peak": 1, "bytes": 10,
      "answers": {"206": 2}, "recovered": {}, "final": {}, "asked_wait_seconds": 0, "backoff_seconds": 0}


def write(workspace, name, *lines):
    (workspace / "transfers").mkdir(parents=True, exist_ok=True)
    text = "\n".join(line if isinstance(line, str) else json.dumps(line) for line in lines)
    (workspace / "transfers" / name).write_text(text)


def test_rows_of_several_files_are_added_up(tmp_path):
    write(tmp_path, "5-1.jsonl", OK, {**OK, "minute": 99999})
    write(tmp_path, "6-2.jsonl", {**OK, "answers": {"206": 1, "503": 1}, "recovered": {"503": 1}})
    out = collect(tmp_path, ["a.org"], NOW)
    assert out["transfers_dropped"] == 0 and [row["minute"] for row in out["transfers"]] == [99999, 100000]
    assert out["transfers"][1] == {**OK, "requests": 4, "connection_seconds": 7.0, "peak": 2, "bytes": 20,
                                   "answers": {"206": 3, "503": 1}, "recovered": {"503": 1}}


def test_a_line_that_is_not_a_row_is_dropped_and_counted(tmp_path):
    bad = [
        "not json",
        '{"minute": 100000, "host": "a.org", "bytes": NaN}',
        {**OK, "requests": 1_000_001},
        {**OK, "bytes": -1},
        {**OK, "peak": True},
        {**OK, "backoff_seconds": 86_401},
        {**OK, "answers": {str(100 + i): 1 for i in range(33)}},
        {**OK, "answers": {"refused": 1}},
        {**OK, "host": "b.org"},                       # not a host the job was allowed
        {**OK, "minute": 100001},                      # after the collection
        {**OK, "minute": 100000 - 7 * 1440 - 1},       # over 7 days old
        '{"minute": 100000, "host": "a.o',             # the line a killed process was writing
    ]
    write(tmp_path, "5-1.jsonl", {**OK, "host": "A.ORG"}, *bad)
    out = collect(tmp_path, ["A.org"], NOW)
    assert out == {"transfers": [OK], "transfers_dropped": len(bad)}


def test_a_job_over_its_bounds_keeps_no_rows(tmp_path, monkeypatch):
    many, big, rows = tmp_path / "many", tmp_path / "big", tmp_path / "rows"
    for i in range(257):
        write(many, f"{i}-1.jsonl", OK)
    write(big, "5-1.jsonl", OK, " " * (8 << 20))
    write(rows, "5-1.jsonl", OK, {**OK, "minute": 99999}, {**OK, "minute": 99998})
    monkeypatch.setattr(transfers, "MAX_ROWS", 2)
    for workspace in (many, big, rows):
        out = collect(workspace, ["a.org"], NOW)
        assert set(out) == {"transfers_refused"} and out["transfers_refused"]


def test_a_record_without_its_hosts_keeps_no_rows_and_no_files_is_nothing(tmp_path):
    assert collect(tmp_path, ["a.org"], NOW) == {}
    write(tmp_path, "5-1.jsonl", OK)
    assert set(collect(tmp_path, None, NOW)) == {"transfers_refused"}


def test_nothing_is_read_through_a_symlink(tmp_path):
    outside, linked_dir, linked_file = tmp_path / "outside", tmp_path / "w1", tmp_path / "w2"
    write(outside, "5-1.jsonl", OK)
    linked_dir.mkdir()
    (linked_dir / "transfers").symlink_to(outside / "transfers")
    (linked_file / "transfers").mkdir(parents=True)
    (linked_file / "transfers" / "5-1.jsonl").symlink_to(outside / "transfers" / "5-1.jsonl")
    assert collect(linked_dir, ["a.org"], NOW) == {}
    assert collect(linked_file, ["a.org"], NOW).get("transfers", []) == []


def test_a_sum_is_cut_to_its_limit(tmp_path):
    write(tmp_path, "5-1.jsonl", {**OK, "bytes": 1 << 50})
    write(tmp_path, "6-1.jsonl", {**OK, "bytes": 1 << 50})
    assert collect(tmp_path, ["a.org"], NOW)["transfers"][0]["bytes"] == 1 << 50
```

- [ ] **Step 2: Run, expect failure:** `PYTHONPATH=$PWD pixi run pytest tests/test_core_transfers.py -q`
- [ ] **Step 3: Implement `mia_core/transfers.py`.** `json.loads` accepts `NaN`: check `math.isfinite`.
- [ ] **Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `core: a job's transfer files are read as bounded, typed rows`.

### Task 5: The job runner tells every job where, and keeps what it finds

**Files:**
- Modify: `autodev/jobs.py` (`CsubJobs.__init__`, `submit`, `wait`), `mia_core/jobrecord.py` (`write_finished`)
- Test: `tests/test_autodev_jobs.py`, `tests/test_core_jobrecord.py`

**Interfaces:**
- Consumes: `collect`, `TRANSFERS_DIR`, `TRANSFERS_ENV` (Task 4).
- Produces:
  - `CsubJobs.submit` sends every job `env[TRANSFERS_ENV] = <spec.workspace>/transfers`, beside `spec.env`.
  - `write_finished(...) -> list[dict]`: the record also gets `collect(job_dir, hosts, time.time())`, `hosts`
    being `record["request"]["allow_hosts"]` when the record has a `request`, else None. It returns the rows the
    stored record holds, `[]` when it holds none or was not stored.
  - `CsubJobs.transfers_sink: Callable[[str, list[dict], str], Any] | None`, None from `__init__`. At the end of
    `wait`, when `write_finished` returned rows and a sink is set, it is called in a thread with the record's
    address, the rows and `self.session`. Whatever it raises is logged as a warning and not raised.

- [ ] **Step 1: Write the failing tests.** In `tests/test_autodev_jobs.py`:

```python
def _row(minute, **fields):
    return json.dumps({"minute": minute, "host": "a.org", "requests": 1, "bytes": 5, "answers": {"206": 1}, **fields})


async def test_every_job_is_told_where_its_transfers_go(tmp_path):
    fake, ws = FakeCsub(), str(tmp_path / "ws")
    await _jobs(tmp_path, fake).submit("k1", JobSpec("j", "true", ws, 10))
    assert fake.submitted[0]["env"] == {"MIA_HARNESS_TRANSFERS_DIR": f"{ws}/transfers"}


async def test_a_jobs_transfers_go_to_its_record_and_to_the_sink(tmp_path, monkeypatch):
    monkeypatch.setattr(jobrecord, "lsf_usage", lambda job_id: None)
    root, ws = tmp_path / "work", tmp_path / "work" / "r1" / "stage-1"
    (ws / "transfers").mkdir(parents=True)
    (ws / "transfers" / "5-1.jsonl").write_text(_row(int(time.time() // 60)))
    jobs, given = CsubJobs("dep", tmp_path / "ids", client_factory=lambda: FakeCsub(), work_root=root), []
    jobs.transfers_sink = lambda where, rows, service: given.append((where, rows, service))
    job_id = await jobs.submit("k1", JobSpec("j", "true", str(ws), 10, allow_hosts=["a.org"]))
    await jobs.wait(job_id, str(ws), lambda state: None)
    record = _record(root, "r1/stage-1")
    assert record["transfers"][0]["bytes"] == 5 and record["transfers_dropped"] == 0
    assert given == [("r1/stage-1", record["transfers"], "dep")]
    jobs.transfers_sink = lambda *args: 1 / 0
    assert (await jobs.wait(job_id, str(ws), lambda state: None)).state == "DONE"   # a sink that raises


async def test_a_job_found_again_keeps_the_minutes_before_it_was_found(tmp_path, monkeypatch):
    monkeypatch.setattr(jobrecord, "lsf_usage", lambda job_id: None)
    root, ws = tmp_path / "work", tmp_path / "work" / "r1" / "stage-1"
    (ws / "transfers").mkdir(parents=True)
    (ws / "transfers" / "5-1.jsonl").write_text(_row(int(time.time() // 60) - 30))   # half an hour ago
    fake = FakeCsub(existing=[SimpleNamespace(job_id="556", cwd=str(ws))])
    jobs = CsubJobs("dep", tmp_path / "ids", client_factory=lambda: fake, work_root=root)
    job_id = await jobs.submit("k1", JobSpec("j", "true", str(ws), 10, allow_hosts=["a.org"]))   # found, not sent
    await jobs.wait(job_id, str(ws), lambda state: None)
    assert fake.submitted == [] and len(_record(root, "r1/stage-1")["transfers"]) == 1
```

  In `test_plain_job_spec`, `s["env"]` is `{"A": "1", "MIA_HARNESS_TRANSFERS_DIR": f"{ws}/transfers"}`. In
  `tests/test_core_jobrecord.py`, `test_the_end_without_a_first_half_keeps_no_transfers`: a workspace with a
  valid transfers file and no first half; `write_finished` returns `[]`, and the record has
  `transfers_refused` and no `transfers`.

- [ ] **Step 2: Run both files, expect failure.**
- [ ] **Step 3: Implement.** `autodev/jobs.py` gets a module logger.
- [ ] **Step 4: Run both files, expect pass.** Then `PYTHONPATH=$PWD pixi run pytest tests -q -k "jobspec or
  jobs or agent or activities"`: a test that compares a whole submitted request's `env` gains the variable.
- [ ] **Step 5: Commit** `jobs: every job is told where its transfers go, and its record keeps them`.

### Task 6: The ledger's table, the put route, the client

**Files:**
- Modify: `mia_buffers/ledger.py` (`SCHEMA`, two methods), `mia_buffers/api.py` (`ROUTES`, `_put_transfers`),
  `mia_core/registry.py` (`Registry.put_transfers`)
- Test: `tests/test_buffers_ledger.py`, `tests/test_buffers_api.py`, `tests/test_core_registry.py`

**Interfaces:**
- Consumes: `reduce_row`, `MAX_ROWS` (Task 4).
- Produces:
  - Table `transfers (host TEXT, minute INTEGER, job TEXT, service TEXT, requests INTEGER, connection_seconds
    REAL, peak INTEGER, bytes INTEGER, asked_wait_seconds REAL, backoff_seconds REAL, answers TEXT NOT NULL,
    recovered TEXT NOT NULL, final TEXT NOT NULL, PRIMARY KEY (host, minute, job))`.
  - `Ledger.put_transfers(job: str, service: str, rows: list[dict]) -> int`: `INSERT OR REPLACE` of each row in
    one transaction; the number written.
  - `Ledger.transfers(since_minute: int, until_minute: int, host: str | None = None) -> list[dict]`: the stored
    rows with `since_minute <= minute <= until_minute` (of one host when given), the three maps parsed, each
    with its `job` and `service`, ordered by host, minute, job.
  - `("PUT", re.compile(r"^/transfers/(.+)$"), "put_transfers", "worker")`. Body `{"service": str, "rows":
    [...]}`. `422` when a segment of the address does not match `UI_NAME_RE`, the body is not that shape, there
    are more than `MAX_ROWS` rows, or any row does not come back from `reduce_row`; nothing is stored then.
    Answers `{"job": <address>, "rows": <number written>}`.
  - `Registry.put_transfers(job: str, rows: list[dict], service: str = "") -> dict`: `PUT
    /transfers/<quote(job)>` (slashes kept).

- [ ] **Step 1: Write the failing tests.**

```python
ROW = {"minute": 100, "host": "a.org", "requests": 2, "connection_seconds": 3.5, "peak": 1, "bytes": 10,
       "answers": {"206": 2}, "recovered": {}, "final": {}, "asked_wait_seconds": 0.0, "backoff_seconds": 0.0}


def test_a_transfer_row_put_again_replaces_itself(tmp_path):                      # test_buffers_ledger.py
    ledger = Ledger(tmp_path / "l.sqlite")
    assert ledger.put_transfers("r1/stage-1", "buffers", [ROW, {**ROW, "minute": 101}]) == 2
    ledger.put_transfers("r1/stage-1", "buffers", [{**ROW, "bytes": 99}])
    ledger.put_transfers("r1/stage-2", "buffers", [ROW])
    assert ledger.transfers(100, 100, "a.org") == [
        {**ROW, "bytes": 99, "job": "r1/stage-1", "service": "buffers"},
        {**ROW, "job": "r1/stage-2", "service": "buffers"}]
    assert len(ledger.transfers(0, 200)) == 3 and ledger.transfers(100, 101, "b.org") == []


def test_put_transfers_takes_the_token_and_only_rows(tmp_path):                   # test_buffers_api.py
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="secret")
    body = json.dumps({"service": "buffers", "rows": [ROW]}).encode()
    assert s.handle("PUT", "/transfers/r1/stage-1", {}, body, {})[0] == 401
    ok = {"Authorization": "Bearer secret"}
    assert s.handle("PUT", "/transfers/r1/stage-1", {}, body, ok)[:2] == (200, {"job": "r1/stage-1", "rows": 1})
    bad_row = json.dumps({"service": "buffers", "rows": [ROW, {**ROW, "bytes": -1}]}).encode()
    assert s.handle("PUT", "/transfers/r1/stage-2", {}, bad_row, ok)[0] == 422
    assert s.handle("PUT", "/transfers/r1/../x", {}, body, ok)[0] == 422
    assert [row["job"] for row in s.ledger.transfers(0, 200)] == ["r1/stage-1"]


def test_put_transfers():                                                         # test_core_registry.py
    seen = []
    reg = Registry("http://svc", "t", transport=transport(seen, body={"job": "r1/stage-1", "rows": 1}))
    assert reg.put_transfers("r1/stage-1", [ROW], "buffers") == {"job": "r1/stage-1", "rows": 1}
    assert seen[0].method == "PUT" and seen[0].url.path == "/transfers/r1/stage-1"
    assert json.loads(seen[0].read()) == {"service": "buffers", "rows": [ROW]}
```

- [ ] **Step 2: Run the three tests, expect failure.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run them and `tests/test_buffers_ui.py` (it walks `ROUTES`), expect pass.**
- [ ] **Step 5: Commit** `buffer-api: the transfers table and its worker route`.

### Task 7: The read routes

**Files:**
- Create: `mia_buffers/throughput.py`
- Modify: `mia_buffers/api.py` (`ROUTES`, three handlers)
- Test: `tests/test_buffers_throughput.py` (new), `tests/test_buffers_api.py`

**Interfaces:**
- Consumes: `Ledger.transfers` (Task 6).
- Produces, in `mia_buffers/throughput.py` (pure functions, no ledger, no clock):

```python
def failed(outcome: str) -> bool                    # any outcome but a "2xx" or "3xx" status text
def by_minute(rows: list[dict]) -> list[dict]       # stored rows added over jobs: one per (host, minute), with `jobs`
def level(connection_seconds: float) -> int         # a minute's mean connections open: int(seconds / 60 + 0.5)
def summary(minutes: list[dict]) -> dict            # one host's minutes as one row
def by_level(minutes: list[dict]) -> list[dict]     # one host's minutes grouped by level, ascending
```

  A row of `by_minute` holds `host`, `minute`, `jobs` (how many jobs had a row), the numeric fields summed (`peak`
  too) and the three maps added. `summary` and each row of `by_level` hold `minutes`, `bytes`,
  `bytes_per_second` (`bytes / (60 × minutes)`), `bytes_per_connection_second` (None when no connection-second),
  `requests`, `answers`, `recovered`, `final`, `asked_wait_seconds`, `backoff_seconds`; `summary` adds
  `last_minute`; a level row adds `level`, `jobs_per_minute` (mean) and `failed_share` (failed outcomes in
  `answers` over all of `answers`; None when `answers` is empty).
- Routes, all `"read"`, placed before any pattern that could shadow them:
  - `GET /hosts?days=7` → `{"days": 7.0, "hosts": [{"host": …, …summary}]}`, most bytes first.
  - `GET /hosts/<host>?days=7` → `{"host": …, "days": 7.0, "levels": […]}`; a host with no rows has no levels.
  - `GET /hosts/<host>/minutes?from=&to=` → `{"host": …, "minutes": [by_minute rows]}`; `from` and `to` are unix
    seconds, `to` defaults to now and `from` to a day before `to`; a span over 7 days is `422`.
  - `days` is a number over 0 and at most 366, else `422`; the window is the last `days × 1440` minutes to now.
    The host in the path is compared in lower case.

- [ ] **Step 1: Write the failing tests.** `tests/test_buffers_throughput.py`:

```python
def row(minute, job, connection_seconds, bytes_, answers, host="a.org", **more):
    return {"host": host, "minute": minute, "job": job, "service": "buffers", "requests": sum(answers.values()),
            "connection_seconds": connection_seconds, "peak": 1, "bytes": bytes_, "answers": answers,
            "recovered": {}, "final": {}, "asked_wait_seconds": 0.0, "backoff_seconds": 0.0, **more}


ROWS = [
    row(100, "j1", 120, 6000, {"206": 9, "503": 1}, recovered={"503": 1}),
    row(100, "j2", 60, 3000, {"206": 5}),                                   # minute 100: a mean of 3 open
    row(101, "j1", 240, 1200, {"206": 2, "403": 2}, final={"403": 2}, asked_wait_seconds=7.0, backoff_seconds=16.0),
    row(102, "j1", 20, 600, {"200": 1}),                                    # under half a connection: level 0
]


def test_minutes_are_added_over_jobs():
    first = by_minute(ROWS)[0]
    assert (first["minute"], first["jobs"], first["connection_seconds"], first["bytes"], first["peak"]) == (
        100, 2, 180, 9000, 2)
    assert first["answers"] == {"206": 14, "503": 1} and first["recovered"] == {"503": 1}


def test_a_host_is_read_by_its_mean_connections_open():
    zero, three, four = by_level(by_minute(ROWS))
    assert [(r["level"], r["minutes"], r["jobs_per_minute"]) for r in (zero, three, four)] == [
        (0, 1, 1.0), (3, 1, 2.0), (4, 1, 1.0)]
    assert (three["bytes_per_second"], three["bytes_per_connection_second"], three["requests"]) == (150.0, 50.0, 15)
    assert three["failed_share"] == pytest.approx(1 / 15) and four["failed_share"] == 0.5
    assert (four["bytes_per_second"], four["final"], four["asked_wait_seconds"], four["backoff_seconds"]) == (
        20.0, {"403": 2}, 7.0, 16.0)
    assert zero["failed_share"] == 0.0 and zero["bytes_per_connection_second"] == 30.0


def test_a_hosts_summary():
    out = summary(by_minute(ROWS))
    assert (out["minutes"], out["bytes"], out["bytes_per_second"], out["requests"], out["last_minute"]) == (
        3, 10800, 60.0, 20, 102)
    assert out["bytes_per_connection_second"] == pytest.approx(10800 / 440)


def test_levels_round_to_the_nearest_and_nothing_divides_by_zero():
    assert [level(seconds) for seconds in (0, 29, 30, 89, 90, 240)] == [0, 0, 1, 1, 2, 4]
    idle = by_level(by_minute([row(100, "j1", 0, 0, {}, asked_wait_seconds=30.0)]))[0]
    assert idle["failed_share"] is None and idle["bytes_per_connection_second"] is None
    assert failed("403") and failed("timeout") and not failed("206") and not failed("302")
```

  In `tests/test_buffers_api.py`, `test_host_routes_read_the_transfers`: on a service with a token, put `ROWS`
  shifted so that minute 102 is the current minute (`Ledger.put_transfers` per job), plus one row of `b.org` with
  fewer bytes. With no `Authorization` header: `GET /hosts` lists `a.org` then `b.org` and `a.org`'s `bytes` is
  10800; `GET /hosts/A.org` has levels `[0, 3, 4]`; `GET /hosts/a.org/minutes` has three minutes, the first with
  `jobs == 2`; `GET /hosts/none.org` is `200` with `levels == []`; `GET /hosts?days=x`, `days=0` and a
  `from`/`to` span of 8 days are `422`.

- [ ] **Step 2: Run, expect failure.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run both files and `tests/test_buffers_ui.py`, expect pass.**
- [ ] **Step 5: Commit** `buffer-api: what each host gave, per host, by load level and by minute`.

### Task 8: Workers deliver, and deliver again when they start

**Files:**
- Modify: `mia_core/jobrecord.py` (new `ended_since`), `mia_core/registry.py` (`put_transfers_again`,
  `report_worker`), `autodev/activities.py`, `mia_data/activities.py`, `mia_buffers/activities.py` (each
  `Activities.__init__`), `autodev/cli.py` (`report_code`), `mia_data/cli.py`, `mia_buffers/cli.py` (the
  `report_worker` call), `docs/development.md`, `AGENTS.md`
- Test: `tests/test_core_jobrecord.py`, `tests/test_core_registry.py`, one line in each activities test file

**Interfaces:**
- Consumes: `CsubJobs.transfers_sink` (Task 5), `Registry.put_transfers` (Task 6).
- Produces:
  - `jobrecord.ended_since(work_root: Path, since: float) -> Iterator[tuple[str, dict]]`: (address, record) of
    each record file under `<work_root>/_jobs` last written at or after `since` (its mtime: a record is written
    last when its job ends) that has a `finished_at`.
  - `async def put_transfers_again(registry: Registry, work_root: Path, log: logging.Logger, days: float = 7.0)
    -> int`: for each such record with `transfers`, `registry.put_transfers(address, rows, record's service)`
    through `best_effort`, one record per thread hop so a cancel lands between records; the number delivered.
  - `report_worker(registry, service, log, retry_s=30.0, work_root: Path | None = None)`: once the worker's
    commit is written, and when `work_root` is given, it awaits `put_transfers_again`.
  - Each `Activities.__init__` sets the sink where it has both: mia-data `jobs.transfers_sink =
    self.registry.put_transfers`; mia-buffers the same when `jobs` is not None; autodev
    `jobs.jobs.transfers_sink = …` (its `AgentJobs` holds the `CsubJobs`) when `self.registry` is not None.
  - The three `cli.py` pass `work_root=cfg.paths.work_root`; `report_code(registry, task_queue, work_root)`.

- [ ] **Step 1: Write the failing tests.**

```python
def test_records_that_ended_lately_are_listed(tmp_path):                          # test_core_jobrecord.py
    root = tmp_path / "work"
    ended = {"finished_at": "x"}
    for where, extra in (("r1/a", {**ended, "transfers": [ROW]}), ("r1/b", {}), ("r1/old", ended)):
        jobrecord._store(jobrecord.record_path(root, where), {"service": "dep", **extra})
    os.utime(jobrecord.record_path(root, "r1/old"), (1.0, 1.0))
    assert [where for where, _ in jobrecord.ended_since(root, time.time() - 60)] == ["r1/a"]
    assert list(jobrecord.ended_since(tmp_path / "nothing", 0.0)) == []


async def test_a_starting_worker_puts_its_records_transfers_again(tmp_path):      # test_core_registry.py
    root, seen = tmp_path / "work", []
    jobrecord._store(jobrecord.record_path(root, "r1/a"), {"service": "dep", "finished_at": "x", "transfers": [ROW]})
    jobrecord._store(jobrecord.record_path(root, "r1/b"), {"service": "dep", "finished_at": "x"})
    reg = Registry("http://svc", "t", transport=transport(seen, body={}))
    assert await put_transfers_again(reg, root, logging.getLogger("t")) == 1
    assert seen[0].url.path == "/transfers/r1/a" and json.loads(seen[0].read()) == {"service": "dep", "rows": [ROW]}
    down = Registry("http://svc", "t", transport=transport([], status=503))
    assert await put_transfers_again(down, root, logging.getLogger("t")) == 0     # logged, not raised
```

  (`ROW` as in Task 6.) Also `test_the_start_up_report_puts_transfers_again_once_the_commit_is_written`:
  `report_worker(reg, "dep", log, work_root=root)` sends the worker's commit first and the `PUT /transfers/r1/a`
  after it. In each activities test file, where an `Activities` is built with a job runner and a service: assert
  the job runner's `transfers_sink == activities.registry.put_transfers` (and for autodev, None with no
  `service` block).

- [ ] **Step 2: Run, expect failure.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Write the docs.** `docs/development.md`, in "Every job leaves a record": the record's
  `transfers`, `transfers_dropped` and `transfers_refused`, where the rows come from, the sink and the
  put-again at start; in the route list: the four routes; one paragraph on reading a host by level and what a
  level cannot say (§5.4, jobs we kill ourselves included). `AGENTS.md`, the rule "Every job leaves a record":
  one sentence that the job runner also reads `<workspace>/transfers` into the record and the ledger, and that
  nothing stored names what an answer meant. Describe the system as it is.
- [ ] **Step 5: Run the gate once for Part B:** `PYTHONPATH=$PWD pixi run test`, `pixi run lint`,
  `pixi run typecheck`, and `git diff main... | grep -n 'rokickik\|/nrs/'` (empty). Expected: all clean, no
  replay test touched.
- [ ] **Step 6: Commit** `workers: transfers go to the ledger at a job's end and again at a worker's start` and
  `docs: what each host gives us`.

---

## Rollout (the user's; nothing here is done by the implementer)

1. Before anything restarts: each deployment's broker allows the variable. `csub probe` of the mia-data,
   mia-buffers and autodev sessions lists an `env_allow` pattern that covers `MIA_HARNESS_TRANSFERS_DIR`
   (`MIA_HARNESS_*` does). A broker without it would refuse or drop the variable on every submit.
2. Harness first: push the mia-snippets branch, merge it, bring the `harness.dir` checkout to it.
3. Workflows: check the branch out where the workers run, restart buffer-api and the three workers.
4. The spec's check (§7): run a round; `GET /hosts` lists its hosts with bytes that agree with the jobs'
   `bytes_fetched` to within what cut-short bodies explain; a job killed at its walltime has rows up to its last
   minute; `GET /hosts/<host>` shows a level for the `sourcing.per_source` the round ran at.

## Not in this plan

Jobs we kill ourselves leaving their rows (§5.4: the kill finishing the record); a page view; anything that acts
on the numbers; retention of old rows.
