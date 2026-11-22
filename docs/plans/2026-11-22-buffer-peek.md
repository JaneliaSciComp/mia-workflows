# Buffer peek Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The serve step of a buffer shows one random crop as the tensorizer returns it, level by level, with a
Next crop button, and the buffer cannot tell.

**Architecture:** buffer-api draws a descriptor with a generator of its own and runs the harness's python as a
subprocess; `harness tensorize --png` writes one PNG per level and label layer; buffer-api answers them inline and
the page lays them out in a row.

**Tech Stack:** Python 3.12 (stdlib `subprocess`, `tempfile`, `base64`), numpy and pypng in the harness, ES
modules with no build step.

**Spec:** `docs/specs/2026-11-22-buffer-peek.md`

## Global Constraints

- Two repositories, each in a worktree on a branch `buffer-peek`: mia-snippets (`<harness.dir>/.claude/worktrees/buffer-peek`,
  on `origin/main`) and mia-workflows (`.claude/worktrees/buffer-peek`, on `crops-per-split`). The harness deploys first.
- A peek registers no consumer, records no draw, counts no usage or empty draw, freezes no list row, and leaves
  `BufferService._rng` alone.
- The peek is a `read` route; the page inserts what it gets as images (`data:image/png;base64,`) and text.
- No real path or hostname in a tracked file. Comments say what the code does.
- Tests in mia-workflows run as `PYTHONPATH=$PWD pixi run pytest <file> -k <name>` from the worktree; only the
  tests that cover the change.

## Review Focus

- A buffer whose every servable volume is missing or quarantined: a 204, and `draws_today`/the empty count unchanged.
- A harness older than `--png` (argparse exits 2): a 502 whose text holds the harness's last stderr line, never a 500.
- A label layer some level lacks, or a volume with no labels: the image row alone, no error.
- `img` with 2 or 5 channels (`channels: all`): the first channel in grey, not a crash in the PNG writer.
- Two people pressing Next crop at once: the second waits for the first; neither starts a second process beside it.

---

### Task 1 (mia-snippets): `harness tensorize --png <dir>`

**Files:**
- Modify: `harness/src/mia_harness/png.py`, `harness/src/mia_harness/cli.py` (the `tensorize` parser and branch)
- Test: `harness/tests/test_tensorize.py`

**Interfaces:**
- Produces: `png.sample_pngs(sample: dict, out: Path) -> list[str]` writes `img-<i>.png` and
  `label-<key>-<i>.png` (`i` the level's position, `key` the part of `label/<key>` after the slash) as 8-bit PNGs
  and returns the names written; the CLI flag `--png DIR` calls it after the `.npz` is saved (creating `DIR`).

- [ ] **Step 1: the failing tests**, beside `test_tensorize_cli`:

```python
def test_tensorize_cli_writes_a_png_per_level_and_label(staged, planned, tmp_path):
    code = cli.main(["tensorize", str(staged), "--tensorizers", str(TENSORIZERS), "--name", "muvit-2d",
                     "--descriptor", json.dumps(planned), "--out", str(tmp_path / "s.npz"), "--png", str(tmp_path / "p")])
    assert code == 0
    assert sorted(f.name for f in (tmp_path / "p").iterdir()) == sorted(
        [f"img-{i}.png" for i in range(3)] + [f"label-gt-{i}.png" for i in range(3)])
    width, height, _, info = png.Reader(filename=str(tmp_path / "p" / "img-0.png")).read()
    assert (width, height, info["bitdepth"], info["greyscale"]) == (64, 64, 8, True)
    assert png.Reader(filename=str(tmp_path / "p" / "label-gt-0.png")).read()[3]["planes"] == 3


def test_sample_pngs_colours_and_channel_counts(tmp_path):
    lab = np.zeros((1, 4, 4), np.int32); lab[0, 0, 0], lab[0, 1, 1] = 7, 7; lab[0, 2, 2] = 8
    for channels, planes in ((1, 1), (3, 3), (2, 1), (5, 1)):
        img = np.full((1, channels, 4, 4), 2.0, np.float32)            # above 1: clipped, not wrapped
        assert sample_pngs({"img": img, "label/a": lab, "meta": {}}, tmp_path) == ["img-0.png", "label-a-0.png"]
        _, _, rows, info = png.Reader(filename=str(tmp_path / "img-0.png")).read()
        assert info["planes"] == planes and set(np.array(list(rows)).ravel()) == {255}
    rgb = np.array(list(png.Reader(filename=str(tmp_path / "label-a-0.png")).read()[2])).reshape(4, 4, 3)
    assert not rgb[3, 3].any() and (rgb[0, 0] == rgb[1, 1]).all() and (rgb[0, 0] != rgb[2, 2]).any() and rgb[0, 0].any()
```

- [ ] **Step 2:** `pixi run python -m pytest tests/test_tensorize.py -k png -q` in `harness/`: both fail (no `--png`, no `sample_pngs`).
- [ ] **Step 3:** implement `sample_pngs` in `png.py` with `write_png`; a label id's colour is
  `(id * k) % 200 + 55` for `k` in `(67, 131, 197)`, 0 black. Add `--png` to the parser and the branch.
- [ ] **Step 4:** the same command passes; `pixi run python -m pytest tests/test_tensorize.py tests/test_png.py -q` passes.
- [ ] **Step 5:** commit `harness: tensorize --png writes a picture per level of the image and of each label layer`.

### Task 2 (mia-workflows): `GET /buffers/<id>/peek`

**Files:**
- Create: `mia_buffers/peek.py`
- Modify: `mia_buffers/api.py` (`ROUTES`, `BufferService.__init__` gains `peeker: Any = None`, `_peek`, `serve`)
- Test: `tests/test_buffers_peek.py`
- Docs: `docs/development.md` (the routes buffer-api serves; the serve step), `AGENTS.md` only if a rule changes (none does)

**Interfaces:**
- Consumes: Task 1's file names.
- Produces, in `peek.py`:
  - `class PeekFailed(RuntimeError)`: the harness did not produce the pictures; its text is for the page.
  - `@dataclass(frozen=True) class Peeker: python: Path; snippets: Path`, with
    `render(self, pyramid: str, descriptor: dict, tensorizer: str, params: dict, labels: list[str]) -> dict`
    returning `{"img": [bytes, …], "labels": {key: [bytes, …]}}`: one entry per level of `descriptor["levels"]`;
    a label key with a level's file missing is left out. Runs
    `[python, "-P", "-c", HARNESS_MAIN, "tensorize", pyramid, "--tensorizers", snippets, "--name", tensorizer,
    "--descriptor", json, "--params", json, "--out", <tmp>/sample.npz, "--png", <tmp>/png]` with
    `OMP_NUM_THREADS=OPENBLAS_NUM_THREADS=MKL_NUM_THREADS=1`, `PEEK_TIMEOUT_S = 60`, reads each PNG with
    `read_untrusted(…, PNG_LIMIT, binary=True)`; a non-zero exit, a timeout, an `OSError` or a missing `img-<i>.png`
    raises `PeekFailed` with the last 300 characters of stderr.
  - `Peeker.from_config(cfg) -> Peeker`: `harness_env(cfg)` and `cfg.harness.dir`.
- Produces, in `api.py`: the route `("GET", r"^/buffers/([^/]+)/peek$", "peek", "read")` answering the JSON of spec
  §5.2; 422 for a `split` that is not `train`, `val` or `test`; 503 `no harness configured` when `peeker` is None;
  502 with the `PeekFailed` text; 204 with no `Retry-After` counting when nothing is servable. `self._peek_lock`
  holds one render at a time. `dataset` comes from `ledger.record(latest_catalog, record)` through
  `specmap.facts`, and is `{"id": record}` alone when the record is not there or unreadable. The params are
  `{**config.tensorizer_params, "channels": descriptor["channels"], "stats": descriptor["normalize"]}`.
  A level's `pixel_nm` is the volume's `Level.scale_nm["x"]` for that factor.

- [ ] **Step 1: the failing tests** in `tests/test_buffers_peek.py`, using `body`, `CFG`, `CONSUMER`, `call`, `svc`,
  `approve` from `test_buffers_api`:
  - `test_a_peek_answers_the_tensorizers_pictures_with_the_datasets_facts`: a stand-in peeker
    (`render` returns `{"img": [b"p0", b"p1", b"p2"], "labels": {"gt": [b"l0", b"l1", b"l2"]}}` and records its
    arguments); `GET /buffers/b/peek` is 200; `levels` has factors `[1, 2, 4]`, `pixel_nm` `[10.0, 20.0, 40.0]`,
    `img` the base64 of each; `labels[0]["key"] == "gt"`; `descriptor["path"] == "/staged/a/v0.zarr"` with no `seq`;
    `dataset["id"] == "rec"`; the peeker got `tensorizer == config.tensorizer` and params holding `channels`.
  - `test_a_buffer_cannot_tell_a_peek_from_no_peek`: two services with the same config, volume and seed; one is
    peeked 5 times (train and val) first. Then both: `status` has `draws == {}` and `consumers == []` after the
    peeks; the same consumer's `/next?n=5` and `/split/val?from=0&n=2` answer equal descriptors on both; the
    peeked one's `ledger.frozen("b", "val")` was empty before the split read.
  - `test_a_peek_of_nothing_servable_is_a_204_that_is_not_counted`: a buffer with no volume: 204, and
    `list_buffers` shows no empty draw for it (the field the page reads for the day's empties stays 0).
  - `test_a_peek_refuses_what_a_consumer_is_refused`: 409 before `approve`, 404 for an unknown buffer, 422 for
    `split=other`, 503 with no peeker, 502 carrying the text of a peeker that raises `PeekFailed("unrecognized arguments: --png")`.
  - `test_the_peeker_runs_the_harness_and_reads_its_pictures`: a stand-in "python" (an executable script with
    `sys.executable` as its interpreter that finds `--png` in its arguments and writes `img-0.png`, `img-1.png`
    and `label-gt-0.png` only): `render(…, labels=["gt", "other"])` returns two `img` and no label (`gt` lacks
    level 1); the script saw `OMP_NUM_THREADS=1` and `--tensorizers <snippets>`.
  - `test_the_peeker_says_why_the_harness_failed`: a script that prints `no such option` to stderr and exits 2,
    and one that writes nothing and exits 0: both raise `PeekFailed`, the first holding `no such option`.
- [ ] **Step 2:** `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_peek.py -q`: all fail on the import.
- [ ] **Step 3:** implement `peek.py`, the route and the wiring in `serve` (`peeker=Peeker.from_config(cfg)`).
- [ ] **Step 4:** the file passes; `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_api.py tests/test_buffers_ui.py -q` passes
  (the UI test lists every route the page may ask for); `pixi run lint` and `pixi run typecheck` are clean.
- [ ] **Step 5:** commit `buffers: a peek answers one crop as the tensorizer returns it and leaves the buffer as it was`.

### Task 3 (mia-workflows): the page

**Files:**
- Create: `mia_buffers/ui/peek.js` (imports `dom.js` and `api.js` only)
- Modify: `mia_buffers/ui/flow.js` (`serveStep`: the card takes `peekPanel(version.id)` under the snippet),
  `mia_buffers/ui/app.css`, `docs/development.md` (the module table and the serve step), `mia_buffers/ui/guide.html`
  (one sentence where the guide shows the snippet)
- Test: `tests/test_buffers_ui.py`

**Interfaces:**
- Consumes: Task 2's answer.
- Produces: `peek.js` exports `length(nm: number) -> string` (`250 nm`, `1 µm`, `512 µm`, `1.3 mm`: three
  significant digits at most, the largest unit that keeps the number at 1 or more), `peekView(peek: object) -> Node`
  (the rows, the facts and the line that says it is a peek; pure, no fetch) and `peekPanel(buffer: string) -> Node`
  (the split selector, **Next crop**, and where `peekView` goes; the button is disabled and reads `Reading…` while a
  peek is out; a failure shows its text in the panel, not in the banner).

- [ ] **Step 1: the failing tests**, in the style of the file's `run_modules` tests:
  - `test_a_peek_shows_every_level_side_by_side_with_its_size`: `peekView` of an answer with factors `[1, 2, 4, 8]`,
    `pixel_nm` `[250, 500, 1000, 2000]`, patch 256, one label layer: four `img` whose `src` starts
    `data:image/png;base64,` in one row, captions `×1 … ×8`, `250 nm/px`, `1 µm/px`, widths `64 µm`, `512 µm`; the
    label row names its key and type; the facts link to `#datasets/<id>`; the text holds `nothing is counted as drawn`.
  - `test_lengths_read_in_the_unit_that_fits`: `length` of 250, 1000, 64000, 512000, 1250000, 0.5 → `250 nm`,
    `1 µm`, `64 µm`, `512 µm`, `1.25 mm`, `0.5 nm`.
  - `test_next_crop_asks_for_a_peek_of_the_chosen_split_and_draws_it`: `peekPanel("b")` with `fetch` stood in:
    nothing is fetched until **Next crop** is fired; then `GET /buffers/b/peek?split=train`; choosing `val` and
    firing again asks `split=val`; a 502 answer puts its `error` in the panel.
- [ ] **Step 2:** run those three: they fail on the missing module.
- [ ] **Step 3:** implement `peek.js`, the call in `serveStep`, the CSS (`.peek .levels{display:flex;gap:10px;overflow-x:auto}`,
  images `image-rendering:pixelated` at 192 px).
- [ ] **Step 4:** `PYTHONPATH=$PWD pixi run pytest tests/test_buffers_ui.py -q` passes; `pixi run lint` clean.
- [ ] **Step 5:** the real thing: a throwaway `BufferService` with `Peeker(harness python, snippets worktree)` and
  `PYTHONPATH=<snippets worktree>/harness/src`, a volume row pointing at a staged artifact of the served buffer, the
  page opened in headless Chromium, **Next crop** pressed, a screenshot looked at.
- [ ] **Step 6:** commit `page: the serve step peeks at a crop, every level side by side`.
