# MIA: a peek at a served buffer — Spec

**Status:** v0.1 · 2026-11-22 · approved 2026-11-22. The direction (a peek on the serve step that shows the
tensorizer's output) is the user's decision; what is marked *call* is the author's. Plan:
`docs/plans/2026-11-22-buffer-peek.md`. Rests on the training-buffers spec and the buffer-flow spec.

## 1. Intent

The serve step of a buffer shows the two lines a training run writes to read it. Nothing on the page shows what
those lines return. A person who has just approved a buffer should be able to do, on the page, what a training
pipeline does: take a sample, look at it, take the next one. That is how a blank crop, a wrong normalisation or
a label layer that does not line up gets seen before a training run meets it.

## 2. Goals, non-goals, anti-goals

- **Goal:** under "The version being served", one random crop of a chosen split: every level side by side,
  each named by its factor, pixel size and width; the same for each label layer; the dataset's facts
  (id, modality, dimensionality, organism, annotation types); the volume, plane and centre. A **Next crop**
  button takes another.
- **Goal:** the pixels are the tensorizer's output for the descriptor buffer-api would serve: what
  `BufferDataset[i]["img"]` holds, not a separate rendering of the staged volume.
- **Non-goal:** a viewer (zoom, pan, contrast), batches, a history of peeks, a peek at a version that is not
  being served.
- **Anti-goal:** a peek that a buffer can tell from no peek. It registers no consumer, records no draw, counts
  no usage, freezes no row of a `val` or `test` list, does not move the generator consumers draw from and does
  not reset the idle clock.

## 3. Constraints

- buffer-api's environment has no numpy and no zarr; the harness environment has both, and `harness tensorize`
  already turns a pyramid and a descriptor into a checked sample (0.84 s measured for four levels of 256 px).
- The page holds no token and has no build step: the peek is a read route and the page inserts what it gets as
  images and text.
- buffer-api runs on the host, outside a sandbox. The peek makes it run the harness and a tensorizer from the
  `harness.dir` checkout. The user accepted this on 2026-11-22: the alternative, the raw staged levels with the
  harness's own stretch, runs no snippet code but is not what training receives.

## 4. Approaches

1. **buffer-api runs the harness's python as a subprocess** (chosen). True to what a consumer gets, about a
   second per crop, no new dependency in buffer-api.
2. The browser reads the staged Zarr itself. Needs codecs in the page, so a build step or a vendored library,
   and would re-implement normalisation and channel recipes in JavaScript.
3. The stage job writes a PNG per sample. Every record restages (the harness commit is in the artifact id) and
   the disk holds pictures nobody asked for.
4. buffer-api reads Zarr in process. Puts numpy and zarr in the service and a second pyramid reader beside the
   harness's.

## 5. Design

### 5.1 Harness: `harness tensorize --png <dir>`

Beside the `.npz`, the command writes 8-bit PNGs into `<dir>`, numbered by the position of the level in the
descriptor's `levels`:

- `img-<i>.png`: level `i` of `img`, clipped to [0, 1]. One channel is greyscale, three are RGB; any other
  count shows the first channel (*call*).
- `label-<key>-<i>.png`: level `i` of `label/<key>`, each id a colour that depends only on the id, 0 black.

### 5.2 buffer-api: `GET /buffers/<id>/peek?split=train`

A read route. `split` is `train`, `val` or `test` and defaults to `train`.

- The buffer must be serving (`_serving`: 409 or 410 otherwise, as for a consumer). The descriptor comes from
  `sampler.draw` over the live volumes with a generator made for this one request. Nothing servable is a 204
  that is **not** counted as an empty draw.
- The harness runs as `harness_env -P -c … tensorize <volume path> --tensorizers <harness.dir> --name
  <config.tensorizer> --descriptor … --params … --out <tmp>/sample.npz --png <tmp>`, with the params
  `BufferDataset._tensorize` builds, in a temporary directory, with one BLAS thread, a 60 s timeout, one peek
  at a time, and of the service's environment only `PATH`, `HOME`, `TMPDIR` and `LANG` (no token). A peek waits
  5 s for the one before it and is then a 503, so an open route cannot queue processes. A failure is a 502 with
  the last of its stderr; a service with no harness configured answers 503.
- The PNGs are read with `read_untrusted` (the staged volume is a job's output) and returned inline as base64.
- The answer:

```
{"split": "train",
 "descriptor": {…},                        the one a consumer would have been handed, without `seq`
 "dataset": {"id", "title", "modality", "dims", "organisms", "annotation_types"},
 "samples": 128,                            the volume's candidates in this split; null for a 0.5 artifact
 "levels": [{"factor": 1, "pixel_nm": 250.0, "img": "<base64 png>"}, …],
 "labels": [{"key": "…", "levels": ["<base64 png>", …]}]}
```

- *Call:* the peek runs the `harness.dir` checkout as it stands, not the snapshot the buffer's round pinned:
  a pinned snapshot older than §5.1 cannot write PNGs. The tensorizer changes rarely; if that stops being true
  the peek passes the pinned snapshot as `--tensorizers`.
- *Call:* a `val` or `test` peek draws among the split's candidates and does not follow the frozen list, since
  reading that list is what freezes it.

### 5.3 The page

In the card "The version being served", under the snippet: a split selector, **Next crop**, and the result.
Nothing is fetched until the button is pressed (a peek costs a process).

```
split [train ▾]  [Next crop]
 ×1          ×2          ×4          ×8
[img]       [img]       [img]       [img]
250 nm/px   500 nm/px   1 µm/px     2 µm/px
64 µm       128 µm      256 µm      512 µm
labels manual_gt-brain_structure-semantic
[label]     [label]     [label]     [label]
dataset <link> · brightfield · 2D · human · semantic
volume v0.zarr · sample 3 of 128 · centre 6148, 8402 µm
A peek: no consumer is registered and nothing is counted as drawn.
```

The levels sit in one row across the card and scroll sideways on a narrow window. Images are drawn pixelated
at a fixed size, so a level's pixels stay visible.

## 6. Order

The harness deploys first: buffer-api passes `--png`, which an older harness refuses (the peek then answers
502 and says so; nothing else is affected). Then buffer-api restarts.

## 7. Left to the builder

File and function names, the colour rule for label ids, the size the images are drawn at, the tests (the
harness command's PNGs; that a peek leaves the ledger's draws, consumers, frozen rows and empty counts as they
were; the page's formatting of sizes).
