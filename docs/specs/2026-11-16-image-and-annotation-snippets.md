# MIA: image readers and annotation readers — Spec

**Status:** v0.1 · 2026-11-16 · draft, not approved. Written after the user's ruling of 2026-11-16 that image
snippets and annotation snippets are decoupled. Choices the user has not ruled on are marked *my call*.

## 1. Intent

A snippet today is one function, `open_array(src, io) -> Array`, for a raw array and a label array alike. When a
dataset's labels are not pixels (boxes in XML, contours in an IMOD model) or are pixels that need a step before
they are a label (one class id of a map, one channel, a mask at a coarser scale), that step is written into the
snippet, next to the code that reads the image format. Snippets may not import each other, so every combination
is a new snippet that repeats one half of an older one.

What the rounds so far show:

- 20 of the 39 datasets tried in November needed a new snippet, and 5 of the last 9. The rate is not falling.
- In the October bulk run, 31 of 37 snippet requests involved an annotation file (XML, JSON, CSV, ROI).
- Of the 25 snippets in the catalog, these carry both halves, or a label half alone:

| Snippet | Image half | Label half |
|---|---|---|
| `tiff-jpeg-voc-zip-glob` | TIFF or JPEG files as z planes | Pascal VOC XML boxes painted on the image grid |
| `jpeg-xml-boxes-numbers` | numbered JPEG files as z planes | VOC-like XML boxes, painted the same way, written again |
| `czi-plane-tiff-grid` | one CZI plane | a TIFF mask repeated k x k onto the CZI's grid (it opens both files) |
| `tiff-zip-member-class` | one TIFF | `array == class_id` |
| `tiff-zip-member-channel` | one TIFF | one channel, or one-hot channels to ids |
| `imod-model-mask` | none | IMOD contours rasterised on the grid in the model's own header, which is checked against the raw only afterwards |

**Goals**

- A new image format is one reader, written once, usable for raw and for mask files.
- A new annotation format is one reader, written once, usable with every image reader.
- The steps that turn a mask file into a label (select, repeat, place) are binding data the harness applies,
  tested once, not snippet code.
- The match agent says which half is missing, so a snippet issue asks for one reader of one format.

**Non-goals**

- Rewriting or withdrawing the snippets above, or changing any saved binding. An artifact id is a recipe hash:
  a migrated binding would restage its volumes.
- Separating how a set of files is chosen (glob, numbers, count) from how one file is decoded. It is the same
  kind of coupling and is left for its own spec (§7.2).
- Writing annotation readers in advance of a dataset that needs them.

**Anti-goal:** an annotation reader that opens an image to learn its size. That is the coupling again.

## 2. Two kinds of snippet

`snippet.yaml` gains `kind: image | annotation` (absent = `image`, so every snippet of today is one).

**Image reader** (unchanged): `open_array(src: Source, io: IO) -> Array`. It reads pixels from files and knows
nothing of labels. A reader of a set of files also reports `names`: one file stem per z plane, in plane order
(a new optional attribute of `Array`; `None` for a single file).

**Annotation reader** (new): `open_annotation(src: Source, io: IO, grid: Grid) -> Array`.

```python
@dataclass(frozen=True)
class Grid:                       # mia_harness.api; what the paired raw array is, without its pixels
    axes: str                     # the raw's axes without c, e.g. "zyx"
    shape: tuple[int, ...]
    voxel_size_nm: dict[str, float] | None
    names: tuple[str, ...] | None  # the raw's file stem per z plane, when it is a set of files
```

- The harness opens the raw array a label `pairs_with` first and hands its grid in. The annotation reader returns
  an `Array` of that grid's axes and shape; `check_binding` verifies it, as it verifies every label today.
- It reads annotation files only. For one file per plane it pairs a file with its plane by `grid.names`; a file
  that names no plane, or a plane count that differs when `names` is `None`, is an error at open.
- What it paints is its params' business (`boxes: instance | semantic`, `classes`, `objects`, `draw: outline |
  fill`), as in the snippets of today.
- CI holds it to the kind (*my call*): `check.yml` rejects an annotation snippet whose `requires` names an image
  codec (`tifffile`, `imagecodecs`) and an image snippet that defines `open_annotation`.

## 3. Label steps in the binding

A label that is a mask file is read by an **image reader**, and the binding lists the steps that make it a label
on the raw's grid. The harness applies them, in order, between the reader and `check_binding`:

```yaml
labels:
  - snippet: tiff-zip-member          # an image reader
    urls: [https://example.org/masks.tif]
    params: {}
    steps:                            # each one deterministic, and each with a reason in `reasons`
      - channel: 2                    # one channel of a multi-channel mask
      - equals: 3                     # uint8 1 where the value is 3 (one class of a class-id map)
      - repeat: 4                     # each pixel k x k on y and x, never interpolated
    pairs_with: 0
    label_key: manual_gt-nucleus-semantic
    segmentation_type: binary
```

The steps of v0.1 (*my call*: only those a tested dataset already needs):

| Step | What it does | Takes the place of |
|---|---|---|
| `channel: i` | one index of the c axis | `tiff-zip-member-channel` (by index) |
| `equals: id` | `array == id` as uint8 | `tiff-zip-member-class` |
| `repeat: k` | integer upscale on y and x; the result must have the raw's shape, less than k short on an edge | `czi-plane-tiff-grid`'s label form |
| `colors: {id: [r, g, b], …}` | an RGB mask to class ids; a pixel within `tolerance` of no listed colour is an error | none yet (the Ivy GAP legend is the first user) |

"Pure readers" stays the rule for snippets: a snippet never selects, repeats or places. The harness does, so
every label goes through one tested implementation, and the steps are in the binding hash and so in the artifact
id.

A label smaller than its raw (a mask that covers a corner of the volume) is not a step in v0.1: outside the mask
the truth is "not annotated", not "background", and the sampler and the staged pyramid have no word for that yet
(§7.3).

## 4. What stays as it is

- Every snippet and binding of today. A binding without `steps`, naming a snippet without `kind`, is read as it
  is now. The coupled snippets stay in the catalog and stay immutable; the match agent is told to prefer an image
  reader with steps, or an annotation reader, when both would fit.
- `Source`, `IO`, the access rules, immutability, withdrawal, the `mia-data/crops` status.
- The crop sampler, `verify`, staging and the tensorizers: they see `Array`s, as before.

## 5. The match agent and autodev

- The catalog entry of each snippet says its kind. The match prompt asks, per label array, three questions in
  order: is it a mask file an image reader reads (then which steps), is it an annotation file an annotation
  reader reads, or is a reader missing.
- `needs_snippet` says which reader is missing: `{"needs": "image" | "annotation", "format": …}`, one entry per
  missing reader. The round opens one snippets issue per entry; an annotation reader's example binding pairs it
  with an existing image reader, so its PR is tested on real data like any other.
- `harness describe` (the file's own account of its format, channels and object names) is the agent's evidence
  for the choice and for each step's reason.

## 6. Build order

| # | What | Done when |
|---|---|---|
| H1 | harness: `Grid`, `Array.names`, the loader's `kind`, `open_arrays` opens raws first and passes the grid, `steps` in the binding schema and their implementation | harness tests: each step, an annotation reader on a synthetic grid, a binding of today read unchanged |
| H2 | `SPEC.md` version 2 and `check.yml`: the kind, the entry function per kind, the codec rule | a PR that mixes the kinds fails `check` |
| H3 | two annotation readers ported from tested snippets: VOC XML boxes and the IMOD model | on the datasets the coupled snippets were tested on, the new bindings give byte-identical label PNGs |
| W1 | mia-workflows: catalog kind, match prompt, `needs_snippet` entries, one snippets issue per entry | a dataset whose image format is known and whose annotation format is new opens one issue, for an annotation reader |

H1 to H3 deploy before W1: a round pins the harness HEAD.

## 7. Open questions (defaults in bold)

1. **Steps live in the harness**, not in a third kind of snippet. A step that needs dataset-specific code is a
   sign the thing is an annotation format after all.
2. File-set selection (glob, `numbers`, `skip`, `count`) as binding data instead of reader params: **not in
   this spec**. It would halve the image readers again (`tiff-series-glob`, `tiff-series-glob-range`,
   `tiff-http-numbered`, `tiff-series-http` differ only there).
3. A label that covers part of its raw: **left out**, until "not annotated" has a representation in crops and in
   the staged pyramid.
4. New bindings **must not** name `tiff-zip-member-class`, `tiff-zip-member-channel` or
   `czi-plane-tiff-grid`'s label form once steps exist; the match prompt says so, CI does not enforce it.
