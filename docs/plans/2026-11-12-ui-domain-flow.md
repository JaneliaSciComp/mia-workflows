# The page follows the domain model: plan

**Goal:** every tab of the buffer-api page is one kind of thing (datasets, preparation requests, buffers, the cache,
decisions), each thing has one page that shows all we know about it, every table names a dataset the same way, and
every table is paged by the server.

**Approved by the user 2026-11-12** (the four decisions at the end of the proposal: all as recommended).

**Builds on:** `docs/specs/2026-11-11-webapp-actions.md` (subjects, actions, the timeline). Nothing here changes a
workflow, an activity or a stage table.

## Constraints

- Existing page URLs keep working: `#datasets/<record>`, `#specs/<name>`, `#requests/<key>`, `#buffers/<id>`,
  `#find/<text>`. The mirrored GitHub issues link to them.
- Workers are untouched: a list route called without `limit` answers exactly as before (`mia_core.registry`, the
  keeper and `BufferDataset` call them).
- The page stays one file, holds no token, calls only read and action routes (`tests/test_buffers_ui.py`).
- Nothing is parsed as HTML; text from a job stays text.

## The paging contract

A list route called with `limit` answers one page:

- Query: `limit` (1–200), `offset` (≥ 0, default 0), `sort` (a column key the route names), `dir` (`asc` | `desc`),
  `q` (text searched in the row's haystack), plus the route's own filters.
- Answer: the route's usual envelope with its list cut to the page, plus `total` (rows matching the filters).
- An unknown `sort` is a 422. A missing value sorts last in either direction.
- `GET /decisions` is the page's own route: always paged, newest first, no `sort`.
- `POST /plan` carries the same fields under `page` in its JSON body.

`mia_buffers/explore.py` holds what the lists are made of: `page_query`, `paged`, the dataset join, the plan summary.
ponytail: a page is cut from the whole list in Python (lists are thousands of rows); SQL `LIMIT` when one reaches
tens of thousands.

## Routes

| route | what changes |
|---|---|
| `GET /catalog/datasets` | new: one row per record of the newest catalog, joined with its preparation row, raw download, staged artifacts and buffers. Filters `state`, `modality`, `dims`, `licence`, `request` (a preparation request's number). `ids=1` answers the matching ids only. Answers `facets` for the filter boxes. |
| `GET /catalog/datasets/<record>` | new: the joined row plus the record itself, its raw asset, staged artifacts, the buffers whose pool holds it, the requests that name it, its subject. 404 when the record is in no synced catalog. |
| `GET /assets/<kind>` | paged; each row gains `record` and `issue` (the dataset's). Filter `record`. |
| `GET /subjects/<kind>` | paged. |
| `GET /buffers` | paged; each row gains `spec` (the spec's key). Filters `spec` (a spec's number), `record`. |
| `GET /buffers/<id>/events`, `/datasets`, `/volumes` | paged; a volume row gains `draws` and `missing`. |
| `GET /buffers/<id>/consumers` | new read route: the registered consumers, paged. |
| `GET /locks`, `GET /needs` | paged. |
| `GET /decisions` | `limit`, `offset`, `gate`, `record`; each row gains `record` or `spec` when its subject maps to one; answers `total`. `before` is dropped. |
| `POST /plan` | with `page`: one page of joined rows, `total`, and `summary` (counts, bytes, funnel, by modality, weight share, heaviest, `to_prepare`). Without `page`: as before. |

A decision maps to a dataset by its subject: a ref that starts with a round id `i<number>-e…` is the dataset subject
of that number; an `artifact` is its staged asset's `record`.

A request's datasets are the `records` of its stored spec. A request that selects by rule has no list in the
registry: its page says so. ponytail: no worker write; add one when a rule-selected request needs the table.

## The page

Tabs: Needs you · Datasets · Preparation · Buffers · Cache · Decisions · Docs.

- **Datasets** (`#datasets`, `#datasets?state=<state>`): search, filters (state, modality, dims, licence), the paged
  table with a tick per row, "Select all N matching" (at most 200), "Prepare selected".
- **Dataset page** (`#datasets/<record>`): header (state, buttons, links: landing page, catalog record, issue,
  Temporal, files), Record, Preparation (stage, reason, binding, requests), Crops, Decisions, Cache (raw download,
  staged artifacts), Buffers, Timeline.
- **Preparation**: requests, paged; links to Datasets filtered to in progress and needs a human. A request's page
  gains "Datasets of this request".
- **Buffers**: "New buffer" (`#buffers/new`: the designer that was the Datasets tab, its plan table paged, with
  "Prepare the N that fit but are not prepared"), specs, buffers.
- **Cache**: quotas, keeper, Raw cache, Staged pyramids, Lock holders.
- **Decisions**, **Needs you**: paged; subjects linked by name.

Rules for every table:

- A dataset is the first column, its name links to its page, and it is the default sort. Then `issue` (a GitHub mark
  linking the issue; empty without one), then `artifact` where there is one, then `files`.
- Staged pyramids: dataset, issue, artifact, files, state, bytes, last used, buffers, levels key, binding, harness,
  job. Raw cache: dataset, issue, files, state, bytes, last used, buffers, job, streams.
- A registry number is never shown as `#<number>`: the thing's name links to its page.
- One component (`pagedTable`): header click sorts, a search box, "1–50 of 2,220", previous and next. Its state
  (sort, search, offset) lasts the session, per table.

## Steps

1. Tab order; `explore.py` paging; `pagedTable`; Cache tables.
2. `GET /catalog/datasets` and `/<record>`; decisions by record; the Datasets list and the dataset page.
3. `POST /plan` paged with its summary; the designer under Buffers.
4. Preparation and the request's datasets.
5. Every other table paged; names for numbers; wording that still says labels; docs.

Each step: route tests first (`tests/test_buffers_api.py`, `tests/test_buffers_explore.py`), then the page, then a
headless-browser pass over the changed views against a real `BufferService`.

## Review focus

- A list route without `limit` is byte-for-byte what it was.
- A search or filter that matches nothing, an `offset` past the end, and a sort on a column most rows lack.
- A record id with characters a URL escapes; a record in the registry that left the catalog.
- A person typing on a dataset page while the stream reports a change (the existing `keepTyping` behaviour).
- The designer's numbers against the old client-side ones for the same spec.
