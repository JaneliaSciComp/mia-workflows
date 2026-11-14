// The Datasets list, and the cards of a dataset's own page.
import {
  $,
  assetPill,
  bufferCount,
  bufferLink,
  bufferLinks,
  card,
  code,
  cropPng,
  datasetLink,
  el,
  fmtBytes,
  folderLink,
  ghIcon,
  kv,
  link,
  num,
  pill,
  plural,
  short,
  statePill,
  SUBJECT_HASH,
  thumb,
  VERDICT_KIND,
  when,
} from "./dom.js";
import { S } from "./api.js";
import { pagedTable, pageOf, TABLE_STATE } from "./table.js";
import { BUTTON_HELP, prepareButton } from "./actions.js";
import { specFromDatasets } from "./designer.js";
import { stageOf } from "./buffers.js";

// ---- datasets: every dataset of the catalog with what the registry knows about it, one page at a time
const DS_FILTER = { state: "", modality: "", dims: "", licence: "" };
const CHOSEN = new Set(),
  SETTLED = new Set(); // the ticked datasets, and those of them that are prepared or in a round
export const ROUND_OPEN = new Set(["matching", "developing", "testing", "review", "merging", "debugging"]); // the stages of a round under way or at review
const PREPARABLE = row => row.state !== "prepared" && !ROUND_OPEN.has(row.stage);
export function viewDatasetList(_arg, params) {
  const root = $("datasetsBody");
  if (!S.ref) {
    root.replaceChildren(
      el("div", { class: "empty" }, "No catalog synced; run ", el("code", {}, "mia-buffers catalog sync"), "."),
    );
    return;
  }
  // a link into the list (Preparation's "in progress") names its filters; a bare #datasets keeps what was set
  if (params && [...params.keys()].length) {
    for (const name of Object.keys(DS_FILTER)) DS_FILTER[name] = params.get(name) || "";
    const state = TABLE_STATE.get("datasets");
    if (state) {
      state.offset = 0;
      state.q = "";
      delete state.last;
    }
  }
  const roots = S.meta.roots || {},
    repo = S.meta.datasets_repo;
  const filters = () => Object.fromEntries(Object.entries(DS_FILTER).filter(([, value]) => value));
  const selects = Object.fromEntries(
    Object.keys(DS_FILTER).map(name => {
      const select = el("select", { "aria-label": `filter by ${name}` });
      select.addEventListener("change", () => {
        DS_FILTER[name] = select.value;
        list.reload(true);
      });
      return [name, select];
    }),
  );
  const fill = facets => {
    // each box offers the values the catalog has, with how many datasets carry them
    for (const [name, select] of Object.entries(selects)) {
      const values = Object.entries(facets?.[name] || {});
      if (DS_FILTER[name] && !values.some(([value]) => value === DS_FILTER[name])) values.push([DS_FILTER[name], 0]);
      select.replaceChildren(
        el("option", { value: "" }, `any ${name}`),
        ...values.map(([value, n]) => el("option", { value }, `${value} (${num(n)})`)),
      );
      select.value = DS_FILTER[name];
    }
  };
  // what can be done with the ticked datasets: a box of its own, shown only while something is ticked
  const prepare = el("span"),
    count = el("b");
  const spec = el(
    "button",
    { type: "button", class: "btn", title: BUTTON_HELP.specFrom, onclick: () => specFromDatasets([...CHOSEN]) },
    "Create buffer",
  );
  const box = el("span", { class: "chosen", hidden: true }, count, prepare, spec);
  const showChosen = () => {
    box.hidden = !CHOSEN.size;
    if (!CHOSEN.size) return;
    count.textContent = `${plural(CHOSEN.size, "dataset")} selected`;
    prepareButton(
      prepare,
      [...CHOSEN],
      null,
      sent =>
        sent.forEach(id => {
          CHOSEN.delete(id);
          SETTLED.delete(id);
        }),
      SETTLED.size,
      "Prepare",
    );
  };
  const ticked = (row, on) => {
    if (on && !PREPARABLE(row)) SETTLED.add(row.id);
    else SETTLED.delete(row.id);
    showChosen();
  };
  const selectAll = el(
    "button",
    {
      type: "button",
      class: "btn",
      title: "ticks every dataset on this page",
      onclick: () => {
        for (const row of TABLE_STATE.get("datasets").last?.rows || []) {
          CHOSEN.add(row.id);
          if (!PREPARABLE(row)) SETTLED.add(row.id);
        }
        list.reload();
      },
    },
    "Select all",
  );
  const clear = el(
    "button",
    {
      type: "button",
      class: "btn",
      title: BUTTON_HELP.clearSelection,
      onclick: () => {
        CHOSEN.clear();
        SETTLED.clear();
        list.reload();
      },
    },
    "Clear selection",
  );
  const list = pagedTable("datasets", {
    source: pageOf("/catalog/datasets", "rows", filters),
    sort: "dataset",
    search: "search id, title, modality, licence",
    tools: Object.values(selects),
    empty: "No dataset matches these filters.",
    select: { chosen: CHOSEN, id: row => row.id, onchange: ticked },
    onPage: page => {
      fill(page.out.facets);
      showChosen();
    },
    cols: [
      {
        key: "dataset",
        label: "dataset",
        cell: x => {
          const a = datasetLink(x.id);
          a.title = x.title || x.id;
          return a;
        },
      },
      { label: "issue", cell: x => ghIcon(repo, x.issue) },
      { label: "files", cell: x => folderLink(roots.storage_root, x.folder) },
      { key: "crops", label: "preview", cell: x => thumb(x) },
      {
        key: "state",
        label: "state",
        title: 'preparation, then the raw cache\'s state, then "staged" when a pyramid of it exists',
        cell: x => {
          const pills = el("span", { class: "pills" }, statePill(x.state));
          if (x.raw) pills.append(" ", assetPill(x.raw.state));
          if (x.staged) pills.append(" ", pill("staged", "pool"));
          if (x.quarantined) pills.append(" ", assetPill("quarantined"));
          if (x.gaps?.length)
            pills.append(
              " ",
              pill(
                "cannot start",
                "warn",
                x.gaps.map(t => ({ t })),
              ),
            );
          return pills;
        },
      },
      { key: "stage", label: "stage", cell: x => x.stage || "" },
      { key: "dims", label: "dims", cell: x => x.dims || "" },
      { key: "modality", label: "modality", cell: x => x.modality || "" },
      { key: "licence", label: "licence", cell: x => x.licence || "" },
      { key: "bytes", label: "size", num: true, cell: x => (x.bytes ? fmtBytes(x.bytes) : "") },
      { key: "raw", label: "raw cache", num: true, cell: x => (x.raw?.bytes ? fmtBytes(x.raw.bytes) : "") },
      { key: "staged", label: "staged", num: true, cell: x => (x.staged ? num(x.staged) : "") },
      { key: "buffers", label: "buffers", num: true, cell: x => bufferCount(x.buffers) },
      { key: "updated", label: "updated", cell: x => when(x.updated_at) },
    ],
  });
  root.replaceChildren(
    el("h2", {}, "Datasets"),
    el(
      "p",
      { class: "small" },
      "Every dataset of the catalog and what is known about it. Open one for its record, crops, decisions, cache and timeline. Tick datasets to prepare them in one request, or to start a buffer spec from them.",
    ),
    el("div", { class: "actions" }, selectAll, clear, box),
    list,
  );
}
// what a dataset's own page shows besides its state, buttons and timeline (GET /catalog/datasets/<record>)
export function datasetCards(d) {
  const row = d.row,
    rec = d.record || {},
    ds = d.dataset || {},
    meta = S.meta,
    roots = meta.roots || {},
    data = a => a.data || {};
  const imaging = rec.imaging || {},
    ann = rec.annotations || {},
    files = rec.data || {};
  const join = xs => [].concat(xs ?? []).join(", "); // a record may give one value where a list is expected
  const voxel = imaging.voxel_size_nm
    ? ["z", "y", "x"]
        .filter(axis => imaging.voxel_size_nm[axis] != null)
        .map(axis => `${axis} ${imaging.voxel_size_nm[axis]}`)
        .join(" · ")
    : "";
  const links = el(
    "div",
    { class: "links" },
    link(rec.landing_url, "source"),
    rec.doi ? link(`https://doi.org/${rec.doi}`, `doi ${rec.doi}`) : null,
    d.path && meta.catalog_repo
      ? link(
          `https://github.com/${meta.catalog_repo}/blob/${d.ref}/${d.path.split("/").map(encodeURIComponent).join("/")}`,
          "catalog record",
          "this dataset's record in the catalog repository on GitHub",
        )
      : null,
    !d.subject ? ghIcon(meta.datasets_repo, row.issue, true) || null : null,
    ds.folder && roots.storage_root ? el("span", {}, "saved crops ", folderLink(roots.storage_root, ds.folder)) : null,
  );
  const cards = [links];
  if (!row.in_catalog)
    cards.push(
      el(
        "div",
        { class: "notice" },
        "This dataset's record is not in the newest synced catalog: only what the registry holds is shown.",
      ),
    );
  if (d.record)
    cards.push(
      card(
        "Record",
        null,
        el("p", {}, el("b", {}, rec.title || row.id)),
        rec.description ? el("p", { class: "small" }, rec.description) : null,
        kv([
          ["modality", join(imaging.modality)],
          ["dimensionality", imaging.dimensionality],
          ["voxel size (nm)", voxel],
          ["channels", imaging.channels == null ? "" : String(imaging.channels)],
          ["organism", join(imaging.organism)],
          ["sample", imaging.sample],
          ["size", files.size_bytes ? fmtBytes(files.size_bytes) : ""],
          ["items", files.n_items == null ? "" : num(files.n_items)],
          ["formats", join(files.formats)],
          [
            "annotations",
            ann.present ? [join(ann.types), ann.source, ann.coverage].filter(Boolean).join(" · ") || "present" : "none",
          ],
          ["licence", rec.license?.spdx ? link(rec.license.url, rec.license.spdx) : ""],
          ["repository", [rec.repository, rec.accession].filter(Boolean).join(" ")],
          ["published", rec.date_published],
        ]),
      ),
    );
  cards.push(
    card(
      "Preparation",
      "What mia-data made of it: a binding to a reader, and reviewed crops.",
      kv([
        ["state", el("span", { class: "pills" }, statePill(row.state))],
        ["stage", row.stage],
        ["reason", ds.reason],
        ["crops saved", ds.crops == null ? "" : num(ds.crops)],
        ["saved", ds.published_at ? when(ds.published_at) : ""],
        [
          "binding",
          ds.binding_hash
            ? el(
                "span",
                {},
                code(ds.binding_hash, 12),
                ds.binding_commit ? " at snippets " : "",
                code(ds.binding_commit, 10),
              )
            : "",
        ],
        ds.revoked_at
          ? ["revoked", el("span", {}, when(ds.revoked_at), ds.revoked_by ? ` by ${ds.revoked_by}` : "")]
          : null,
        ["updated", when(row.updated_at)],
        [
          "preparation requests",
          d.requests.length
            ? el(
                "span",
                {},
                ...d.requests.flatMap((r, i) => [i ? " " : "", el("a", { href: SUBJECT_HASH.prep(r.key) }, r.key)]),
              )
            : "none named it",
        ],
      ]),
    ),
  );
  if (ds.folder && (ds.previews || []).length)
    cards.push(
      card(
        "Crops",
        `${num(ds.crops || ds.previews.length)} saved; the first ${num(ds.previews.length)} with their annotation overlay. Click one to open it.`,
        el(
          "div",
          { class: "gallery" },
          ...ds.previews.map(id =>
            el(
              "a",
              { href: cropPng(ds, id, "overlay"), target: "_blank", rel: "noopener", title: id },
              el("img", { src: cropPng(ds, id, "overlay"), loading: "lazy", alt: `crop ${id} with its overlay` }),
            ),
          ),
        ),
      ),
    );
  cards.push(
    card(
      "Decisions",
      "Every verdict about this dataset, newest first: its crops and its staged artifacts, by a person or a judge.",
      pagedTable("dataset-decisions:" + row.id, {
        source: pageOf("/decisions", "assessments", () => ({ record: row.id })),
        search: false,
        empty: "No decision yet.",
        cols: [
          { label: "when", cell: r => when(r.at) },
          { label: "gate", cell: r => r.gate },
          {
            label: "about",
            cell: r => el("span", {}, `${(r.subject?.kind || "").replace("_", " ")} `, code(r.subject?.ref, 24)),
          },
          { label: "verdict", cell: r => pill(r.verdict, VERDICT_KIND[r.verdict] ?? "") },
          { label: "by", cell: r => r.by },
          { label: "confidence", num: true, cell: r => (r.confidence == null ? "" : Number(r.confidence).toFixed(2)) },
          { label: "summary", wrap: true, cell: r => r.note || r.summary || "" },
        ],
      }),
    ),
  );
  const raw = d.raw_asset;
  cards.push(
    card(
      "Cache",
      "Its download in the raw cache, and each staged artifact: this dataset staged for one crop geometry by one recipe.",
      raw
        ? kv([
            [
              "raw download",
              el(
                "span",
                { class: "pills" },
                assetPill(data(raw).state),
                ...[
                  data(raw).bytes ? fmtBytes(data(raw).bytes) : "",
                  data(raw).last_used ? el("span", {}, "last used ", when(data(raw).last_used)) : "",
                  folderLink(roots.raw_root, raw.key),
                ]
                  .filter(Boolean)
                  .flatMap((part, i) => [i ? " · " : " ", part]),
                " ",
                bufferLinks(raw.refs),
              ),
            ],
          ])
        : el("div", { class: "small" }, "Not in the raw cache."),
      pagedTable("dataset-staged:" + row.id, {
        source: pageOf("/assets/staged", "assets", () => ({ record: row.id })),
        sort: "last_used",
        dir: "desc",
        search: false,
        empty: "Not staged yet.",
        cols: [
          {
            key: "artifact",
            label: "artifact",
            cell: a => el("code", { title: a.key }, short(a.key.split("/").pop(), 16)),
          },
          {
            label: "files",
            cell: a => folderLink(roots.staged_root, data(a).levels_key && `${data(a).levels_key}/${row.id}/${a.key}`),
          },
          {
            key: "state",
            label: "state",
            cell: a => {
              const pills = el("span", { class: "pills" }, assetPill(data(a).state));
              if (data(a).quarantined) pills.append(" ", assetPill("quarantined"));
              return pills;
            },
          },
          { key: "bytes", label: "bytes", num: true, cell: a => (data(a).bytes ? fmtBytes(data(a).bytes) : "") },
          { key: "last_used", label: "last used", cell: a => when(data(a).last_used) },
          { key: "buffers", label: "buffers", num: true, cell: a => bufferCount(a.refs) },
          { key: "levels_key", label: "levels key", cell: a => el("code", {}, data(a).levels_key || "") },
          { label: "binding", cell: a => el("code", {}, short(data(a).binding_hash, 10)) },
          { label: "harness", cell: a => el("code", {}, short(data(a).harness_commit, 10)) },
        ],
      }),
    ),
  );
  cards.push(
    card(
      "Buffers",
      "Each buffer whose pool holds this dataset, the newest first.",
      pagedTable("dataset-buffers:" + row.id, {
        source: pageOf("/buffers", "buffers", () => ({ record: row.id })),
        search: false,
        empty: "In no buffer's pool.",
        cols: [
          { key: "buffer", label: "buffer", cell: b => bufferLink(b.id) },
          {
            key: "spec",
            label: "spec",
            cell: b => (b.spec ? el("a", { href: SUBJECT_HASH.buffer(b.spec) }, b.spec) : b.config?.spec_name || ""),
          },
          { label: "in its pool", cell: b => statePill(b.board?.status) },
          { label: "reason", wrap: true, cell: b => b.board?.reason || "" },
          {
            key: "updated",
            label: "buffer state",
            cell: b =>
              b.latest_event ? el("span", {}, statePill(stageOf(b.latest_event)), " ", when(b.latest_event.at)) : "",
          },
        ],
      }),
    ),
  );
  return cards;
}
