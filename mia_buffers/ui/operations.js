// The operator's views: Cache, Preparation and Decisions.
import {
  $,
  assetPill,
  bufferCount,
  card,
  code,
  datasetLink,
  el,
  fmtBytes,
  folderLink,
  ghIcon,
  kv,
  num,
  pct,
  pill,
  short,
  stagePill,
  SUBJECT_HASH,
  VERDICT_KIND,
  when,
} from "./dom.js";
import { load, S } from "./api.js";
import { pagedTable, pageOf } from "./table.js";

// ---- cache
function meter(label, used, quota) {
  const over = quota > 0 && used > quota;
  return el(
    "div",
    {},
    el("div", { class: "label small" }, label),
    el(
      "div",
      {
        class: "meter" + (over ? " over" : ""),
        role: "meter",
        "aria-valuemin": "0",
        "aria-valuemax": String(quota > 0 ? quota : used || 1),
        "aria-valuenow": String(used || 0),
        "aria-label": label,
      },
      el("i", { style: `width:${quota > 0 ? Math.min(100, (used / quota) * 100) : 0}%` }),
    ),
    el(
      "div",
      { class: "small" },
      quota > 0 ? `${fmtBytes(used)} of ${fmtBytes(quota)} (${pct(used / quota)})` : `${fmtBytes(used)} · unlimited`,
    ),
  );
}
const list = xs => (xs?.length ? el("div", {}, ...xs.map(x => el("div", { class: "mono" }, x))) : "none");
export async function viewCache() {
  const root = $("cacheBody");
  root.replaceChildren(el("div", { class: "empty" }, "loading…"));
  const [raw, staged, keeper] = await Promise.all([
    load("/assets/raw?limit=1"),
    load("/assets/staged?limit=1"),
    load("/keeper"),
  ]);
  const quota = S.meta.quota || {},
    roots = S.meta.roots || {},
    repo = S.meta.datasets_repo;
  const k = keeper;
  const data = a => a.data || {};
  const assetCols = [
    // what a raw download and a staged artifact share, in the one order every table uses
    {
      key: "state",
      label: "state",
      cell: a => {
        const td = el("span", { class: "pills" }, assetPill(data(a).state));
        if (data(a).quarantined) td.append(" ", assetPill("quarantined"));
        return td;
      },
    },
    { key: "bytes", label: "bytes", num: true, cell: a => (data(a).bytes ? fmtBytes(data(a).bytes) : "") },
    { key: "last_used", label: "last used", cell: a => when(data(a).last_used) },
    { key: "buffers", label: "buffers", num: true, cell: a => bufferCount(a.refs) },
  ];
  const dataset = [
    { key: "dataset", label: "dataset", cell: a => datasetLink(a.record) },
    { label: "issue", cell: a => ghIcon(repo, a.issue) },
  ];
  const job = { key: "job", label: "job", cell: a => (data(a).job ? el("code", {}, data(a).job) : "") };
  root.replaceChildren(
    el(
      "section",
      { class: "grid2" },
      card(
        "Totals against the quotas",
        quota.hold_hours ? `A free asset is held ${num(quota.hold_hours)} h before the keeper may evict it.` : null,
        meter("raw cache", raw?.bytes || 0, quota.raw_bytes),
        meter("staged pyramids", staged?.bytes || 0, quota.staged_bytes),
        quota.headroom_bytes
          ? el("div", { class: "small" }, `headroom kept free: ${fmtBytes(quota.headroom_bytes)}`)
          : null,
      ),
      card(
        "The keeper's last pass",
        S.meta.keeper?.minutes
          ? `Runs every ${num(S.meta.keeper.minutes)} min; syncs the catalog every ${num(S.meta.keeper.catalog_sync_hours)} h.`
          : null,
        k
          ? kv([
              ["at", el("span", {}, when(k.at), k.dry_run ? " (dry run)" : "")],
              ["scanned / missing / new", `${num(k.scanned)} / ${num(k.missing)} / ${num(k.new)}`],
              ["evicted", list(k.evicted)],
              ["reset stale", list(k.stale)],
              ["removed building", list(k.removed_building)],
              ["removed empty", list(k.removed_empty)],
              ["spared (locked)", list(k.locked)],
              [
                "over quota",
                Object.keys(k.over_quota || {}).length
                  ? Object.entries(k.over_quota)
                      .map(([kind, b]) => `${kind} ${fmtBytes(b)}`)
                      .join(" · ")
                  : "none",
              ],
              ["signalled", list(k.signalled)],
              ["catalog", k.catalog || "not due"],
              ["errors", list(k.errors)],
            ])
          : el("div", { class: "empty" }, "The keeper has not reported a pass yet."),
      ),
    ),
    card(
      "Raw cache",
      "One row per dataset's download.",
      pagedTable("cache-raw", {
        source: pageOf("/assets/raw", "assets"),
        sort: "dataset",
        empty: "Nothing in the raw cache yet.",
        cols: [
          ...dataset,
          { label: "files", cell: a => folderLink(roots.raw_root, a.key) },
          ...assetCols,
          job,
          { label: "streams", num: true, cell: a => (Array.isArray(data(a).stream) ? num(data(a).stream.length) : "") },
        ],
      }),
    ),
    card(
      "Staged pyramids",
      "One row per staged artifact: a dataset staged for one crop geometry by one recipe.",
      pagedTable("cache-staged", {
        source: pageOf("/assets/staged", "assets"),
        sort: "dataset",
        empty: "Nothing staged yet.",
        cols: [
          ...dataset,
          {
            key: "artifact",
            label: "artifact",
            cell: a => el("code", { title: a.key }, short(a.key.split("/").pop(), 16)),
          },
          {
            label: "files",
            cell: a =>
              folderLink(
                roots.staged_root,
                data(a).levels_key && a.record && `${data(a).levels_key}/${a.record}/${a.key}`,
              ),
          },
          ...assetCols,
          { key: "levels_key", label: "levels key", cell: a => el("code", {}, data(a).levels_key || "") },
          { label: "binding", cell: a => el("code", {}, short(data(a).binding_hash, 10)) },
          { label: "harness", cell: a => el("code", {}, short(data(a).harness_commit, 10)) },
          job,
        ],
      }),
    ),
    card(
      "Lock holders",
      "The workers' lock files, read-only.",
      pagedTable("cache-locks", {
        source: pageOf("/locks", "locks"),
        sort: "key",
        empty: "No locks held.",
        cols: [
          { key: "kind", label: "kind", cell: l => l.kind },
          { key: "key", label: "key", cell: l => el("code", {}, l.key) },
          { key: "owner", label: "owner", cell: l => el("code", {}, l.owner) },
          { key: "taken_at", label: "taken", cell: l => when(l.taken_at) },
          { key: "heartbeat_at", label: "heartbeat", cell: l => when(l.heartbeat_at) },
          { label: "", cell: l => (l.stale ? pill("stale", "warn") : "") },
        ],
      }),
    ),
  );
}
// ---- preparation
export async function viewPreparation() {
  const root = $("prepBody"),
    hash = location.hash;
  const counts = el("div", { class: "links" });
  root.replaceChildren(
    card(
      "Preparation requests",
      "A request prepares a set of datasets under one budget. File one from Datasets (tick datasets, Prepare) or from a new buffer's plan. Open one for its datasets, to cancel it or to run it again.",
      pagedTable("requests", {
        source: pageOf("/subjects/prep", "subjects"),
        sort: "updated",
        dir: "desc",
        empty: "No request yet.",
        cols: [
          { key: "key", label: "request", cell: x => el("a", { href: SUBJECT_HASH.prep(x.key) }, x.key) },
          { label: "issue", cell: x => ghIcon(S.meta.buffers_repo, x.issue) },
          { key: "status", label: "stage", cell: x => stagePill(x.stage) },
          { key: "updated", label: "updated", cell: x => when(x.updated) },
        ],
      }),
    ),
    card("Datasets", "Where each dataset stands; each count opens the Datasets list filtered to it.", counts),
  );
  const states = (await load("/catalog/datasets?limit=1", true))?.facets?.state || {};
  if (location.hash !== hash) return;
  counts.replaceChildren(
    ...[
      ["in-progress", "in progress"],
      ["needs-human", states["needs-human"] === 1 ? "needs a human" : "need a human"],
      ["prepared", "prepared"],
      ["revoked", "revoked"],
      ["unprepared", "unprepared"],
    ].map(([state, words]) => el("a", { href: "#datasets?state=" + state }, `${num(states[state] || 0)} ${words}`)),
  );
}
// ---- decisions: every assessment (spec autonomous-gates §5, §10) and how the judge agrees with the humans
function agreementLine(gate, a) {
  if (!a?.judged) return el("div", { class: "small" }, `${gate}: no judge verdict yet`);
  return el(
    "div",
    { class: "small" },
    `${gate}: ${num(a.judged)} judged · ${num(a.agree)} agree · ${num(a.disagree)} disagree · ${num(a.abstain)} abstained · ${num(a.pending)} awaiting a human`,
  );
}
export function viewDecisions(arg) {
  const root = $("decBody");
  const gate = ["crops", "review", "code", "staged"].includes(arg) ? arg : "";
  const gates = el(
    "div",
    { class: "links" },
    ...["", "crops", "review", "code", "staged"].map(g =>
      el(
        "a",
        { href: "#decisions" + (g ? "/" + g : ""), "aria-current": g === gate ? "page" : null },
        g || "all gates",
      ),
    ),
  );
  const agreement = el("div", {}, el("div", { class: "small" }, "loading…"));
  root.replaceChildren(
    gates,
    card(
      "Judge agreement",
      "Per gate: subjects with a judge verdict, and whether the newest human label agrees. Shadow mode records the judge's verdict and lets the human decide; a gate goes active on these numbers.",
      agreement,
    ),
    card(
      "Assessments",
      "Every decision on record, newest first: human verdicts with what the person wrote, judge verdicts, policy events (repairs). A human approve that a later reject overrules is marked.",
      pagedTable("decisions:" + gate, {
        source: pageOf("/decisions", "assessments", () => (gate ? { gate } : {})),
        search: false,
        empty: "No assessment yet.",
        onPage: page =>
          agreement.replaceChildren(...Object.entries(page.out.agreement || {}).map(([g, a]) => agreementLine(g, a))),
        cols: [
          { label: "when", cell: r => when(r.at) },
          { label: "gate", cell: r => r.gate },
          { label: "dataset", cell: r => datasetLink(r.record) },
          {
            label: "about",
            wrap: true,
            cell: r => el("span", {}, `${(r.subject?.kind || "").replace("_", " ")} `, code(r.subject?.ref, 24)),
          },
          {
            label: "verdict",
            cell: r =>
              el(
                "span",
                {},
                pill(r.verdict, VERDICT_KIND[r.verdict] ?? ""),
                r.overruled ? el("span", { class: "small" }, " overruled") : null,
              ),
          },
          { label: "by", cell: r => r.by },
          { label: "confidence", num: true, cell: r => (r.confidence == null ? "" : Number(r.confidence).toFixed(2)) },
          { label: "model", cell: r => (r.model ? code(r.model, 24) : "") },
          { label: "summary", wrap: true, cell: r => r.note || r.summary || "" },
        ],
      }),
    ),
  );
}
