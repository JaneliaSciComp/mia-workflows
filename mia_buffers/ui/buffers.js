// The Buffers view: the specs, the buffers, and a buffer's own page.
import {
  $,
  ago,
  assetPill,
  bufferLink,
  card,
  code,
  datasetLink,
  el,
  flowRow,
  fmt,
  folderLink,
  ghIcon,
  hoverable,
  kv,
  lifecycle,
  num,
  pct,
  pill,
  PILL_HELP,
  plural,
  short,
  stagePill,
  statePill,
  SUBJECT_HASH,
  svgEl,
  thumb,
  when,
} from "./dom.js";
import { load, S } from "./api.js";
import { pagedTable, pageOf, reloadTables } from "./table.js";
import { actionWord } from "./actions.js";

// ---- buffers
export function servableText(servable) {
  const e = Object.entries(servable || {});
  return e.length ? e.map(([k, v]) => `${k} ${num(v)}`).join(" · ") : "none";
}
const issueOf = id => (/^i(\d+)-/.exec(id) || [])[1];
// a buffer's lifecycle event as the issue's stage label: the inverse of mia_buffers.activities.STAGE_EVENTS.
// `approved` is the approve at the staged gate: consumers draw from then on, so it reads as ready.
const STAGE_OF = {
  created: "planning",
  planned: "plan-review",
  sourcing: "sourcing",
  staged: "staged",
  approved: "ready",
  ready: "ready",
  idle: "idle",
  retired: "retired",
  "needs-human": "needs-human",
};
export const stageOf = ev => (ev ? STAGE_OF[ev.kind] || ev.kind : "");
export function viewBuffers(id, refresh) {
  const root = $("buffersBody");
  if (id) return viewBuffer(root, id, refresh);
  if (refresh) return reloadTables(root);
  root.replaceChildren(
    card(
      "Buffer specs",
      "A spec is what you run; each run builds a buffer. Open one to run its plan, approve, plan again, revoke, retire or cancel.",
      el("p", {}, el("a", { class: "btn", href: "#buffers/new" }, "New buffer")),
      pagedTable("specs", {
        source: pageOf("/subjects/buffer", "subjects"),
        sort: "updated",
        dir: "desc",
        empty: "No spec yet: start with New buffer.",
        cols: [
          { key: "key", label: "spec", cell: x => el("a", { href: SUBJECT_HASH.buffer(x.key) }, x.key) },
          { label: "issue", cell: x => ghIcon(S.meta.buffers_repo, x.issue) },
          { key: "status", label: "stage", cell: x => stagePill(x.stage) },
          {
            label: "waits for",
            cell: x =>
              x.status === "waiting" || x.status === "stuck"
                ? x.actions
                    .filter(a => a !== "stop" && a !== "cancel")
                    .map(a => actionWord(x, a))
                    .join(", ")
                : "",
          },
          { key: "updated", label: "updated", cell: x => when(x.updated) },
        ],
      }),
    ),
    card(
      "Buffers",
      "One row per buffer a run built, the newest spec first and its newest round first; an older round is superseded by the newest. Open one for its events, pool, volumes, consumers and usage.",
      pagedTable("buffers", {
        source: pageOf("/buffers", "buffers"),
        empty: "No buffer yet: start with New buffer.",
        rowAttrs: b => ({
          class: "click" + (b.superseded_by ? " superseded" : ""),
          title: b.superseded_by ? `superseded by ${b.superseded_by}, its spec's newest round` : "",
          onclick: e => {
            if (!e.target.closest("a")) location.hash = "#buffers/" + encodeURIComponent(b.id);
          },
        }),
        cols: [
          { key: "buffer", label: "buffer", cell: b => bufferLink(b.id) },
          {
            key: "spec",
            label: "spec",
            cell: b => (b.spec ? el("a", { href: SUBJECT_HASH.buffer(b.spec) }, b.spec) : b.config?.spec_name || ""),
          },
          {
            key: "updated",
            label: "state",
            cell: b =>
              el(
                "span",
                {},
                ...(b.superseded_by ? [pill("superseded", ""), " "] : []),
                ...(b.latest_event
                  ? [statePill(stageOf(b.latest_event)), " ", when(b.latest_event.at)]
                  : [
                      el(
                        "span",
                        { class: "small", title: "registered before buffers recorded lifecycle events" },
                        "no events",
                      ),
                    ]),
              ),
          },
          { key: "volumes", label: "volumes", num: true, cell: b => num(b.volumes) },
          {
            key: "missing",
            label: "missing",
            num: true,
            cell: b =>
              b.missing
                ? pill(num(b.missing), "warn", [
                    {
                      b: "missing",
                      t:
                        PILL_HELP.missing +
                        " These are volumes registered to the buffer whose pyramid the last scan did not find.",
                    },
                  ])
                : "0",
          },
          { label: "servable", cell: b => servableText(b.servable) },
          {
            key: "consumers_seen",
            label: "consumer last seen",
            cell: b => (b.consumers_seen ? when(b.consumers_seen) : "never"),
          },
          { key: "draws_today", label: "draws today", num: true, cell: b => num(b.draws_today) },
        ],
      }),
    ),
  );
}
function sparkline(days) {
  if (!days.length) return el("div", { class: "empty" }, "No draw recorded yet.");
  const W = 560,
    H = 130,
    left = 40,
    right = 8,
    top = 10,
    bottom = 22,
    plotW = W - left - right,
    plotH = H - top - bottom;
  const max = Math.max(1, ...days.map(d => Math.max(d.draws || 0, d.empty || 0)));
  const svg = svgEl("svg", {
    viewBox: `0 0 ${W} ${H}`,
    role: "img",
    "aria-label": "Crops drawn per day, and requests answered empty per day",
  });
  svg.append(svgEl("line", { x1: left, x2: W - right, y1: top + plotH, y2: top + plotH, class: "base" }));
  const t = svgEl("text", { x: left - 6, y: top + 8, "text-anchor": "end", class: "tick" });
  t.textContent = fmt(max);
  svg.append(t);
  const slot = plotW / days.length,
    bw = Math.max(2, Math.min(18, slot * 0.7));
  days.forEach((d, i) => {
    const x = left + slot * i + (slot - bw) / 2,
      h = ((d.draws || 0) / max) * plotH,
      he = ((d.empty || 0) / max) * plotH;
    svg.append(svgEl("rect", { x, y: top + plotH - h, width: bw, height: h, fill: "var(--accent)", rx: 2 }));
    if (d.empty)
      svg.append(svgEl("rect", { x: x + bw / 2 - 1, y: top + plotH - he, width: 2, height: he, fill: "var(--warn)" }));
    const hit = svgEl("rect", { x: left + slot * i, y: top, width: slot, height: plotH + bottom, fill: "transparent" });
    svg.append(hit);
    hoverable(hit, [
      { b: d.day, t: "" },
      { k: "var(--accent)", b: num(d.draws || 0), t: " crops drawn" },
      {
        k: "var(--warn)",
        b: num(d.empty || 0),
        t: d.empty === 1 ? " request answered empty" : " requests answered empty",
      },
      { t: plural(d.consumers || 0, "consumer") },
    ]);
  });
  for (const [i, anchor] of [
    [0, "start"],
    [days.length - 1, "end"],
  ]) {
    const l = svgEl("text", { x: i ? W - right : left, y: H - 6, "text-anchor": anchor, class: "tick" });
    l.textContent = days[i].day;
    svg.append(l);
  }
  return el(
    "div",
    {},
    el(
      "div",
      { class: "legend" },
      el("span", {}, el("i", { style: "background:var(--accent)" }), "crops drawn"),
      el("span", {}, el("i", { style: "background:var(--warn)" }), "requests answered empty (204)"),
    ),
    svg,
  );
}
function healthLine(usage) {
  if (!usage) return el("div", { class: "small" }, "usage did not load");
  const h = usage.last_hour || {},
    total = (h.requests ?? h.draws ?? 0) + (h.empty || 0),
    limit = usage.max_empty_fraction ?? 0.05;
  if (!total)
    return el("div", { class: "health" }, `no requests in the last hour (degraded above ${pct(limit)} empty)`);
  const frac = (h.empty || 0) / total,
    bad = frac > limit;
  return el(
    "div",
    { class: "health " + (bad ? "bad" : "ok") },
    `${bad ? "degraded" : "healthy"}: ${pct(frac)} of the last hour's ${plural(total, "request")} ${total === 1 ? "was" : "were"} empty (limit ${pct(limit)})`,
  );
}
async function viewBuffer(root, id, refresh) {
  const hash = location.hash,
    e = encodeURIComponent(id);
  if (refresh && root.contains(document.activeElement) && document.activeElement.matches("input[type=search]"))
    return reloadTables(root); // not under someone typing a search
  if (!refresh) root.replaceChildren(el("div", { class: "empty" }, "loading…"));
  const n = issueOf(id);
  // status and usage exist from sourcing on (create_buffer writes the config); a planned buffer has only events
  const [status, events, usage, found] = await Promise.all([
    load(`/buffers/${e}/status`, true),
    load(`/buffers/${e}/events?limit=200`),
    load(`/usage/${e}`, true),
    n ? load(`/lookup?q=${n}`) : null,
  ]);
  if (location.hash !== hash) return; // a slow load of a buffer the reader already left
  const meta = S.meta,
    cfg = status?.config || {},
    evs = events?.events || []; // newest first
  const retired = evs.find(x => x.kind === "retired");
  const archive = retired && (/archive (\S+)/.exec(retired.detail || "") || [])[1];
  const roots = meta.roots || {},
    spec = (found?.subjects || []).find(x => x.kind === "buffer");
  const days = (usage?.days || []).slice().sort((a, b) => (a.day < b.day ? -1 : 1));
  const levelsKey = cfg.patch && cfg.levels?.length ? `p${cfg.patch}-l${cfg.levels.join("-")}` : null; // jobspec.levels_key
  root.replaceChildren(
    ...[
      // filtered: a native replaceChildren would print a null as "null"
      el("h2", {}, id),
      el(
        "div",
        { class: "links" }, // its spec and the spec's issue, then this round's own folders
        spec
          ? el(
              "span",
              {},
              "spec ",
              el(
                "a",
                {
                  href: SUBJECT_HASH.buffer(spec.key),
                  title: "the spec this buffer was built from: its buttons and its timeline",
                },
                spec.key,
              ),
            )
          : null,
        spec ? ghIcon(meta.buffers_repo, spec.issue, true) || null : null,
        n
          ? folderLink(
              roots.work_root,
              `issue-${n}/${id}`,
              "workspace",
              "this round's plan and the workspaces of its jobs",
            ) || null
          : null,
        folderLink(
          roots.staged_root,
          levelsKey,
          "staged pyramids",
          "the staged pyramids of this buffer's patch size and levels",
        ) || null,
      ),
      // a buffer starts at its plan (`new` is its spec's); the step is marked while its spec's round is working there
      flowRow(
        "buffer",
        lifecycle(
          meta.lines?.buffer.slice(1),
          stageOf(evs[0]),
          evs.map(stageOf),
          spec?.status === "working" && spec.stage === stageOf(evs[0]),
        ),
      ),
      retired
        ? el(
            "div",
            { class: "notice" },
            `Retired ${ago(retired.at)}; the draws are archived at `,
            el("code", {}, archive || "none"),
            ".",
          )
        : null,
      status && evs[0]?.kind === "staged"
        ? el(
            "div",
            { class: "notice" },
            "Staged, waiting for ",
            el("b", {}, "Approve"),
            " on its spec's page: the approve serves what the pool shows as servable, partial or not; consumers are refused until then.",
          )
        : null,
      !status && evs.length
        ? el(
            "div",
            { class: "notice" },
            "Not sourced yet: a buffer is registered when sourcing starts, after ",
            el("b", {}, "Run"),
            " on its spec's page. Until then it has only its events.",
          )
        : null,
      !status && !evs.length ? el("div", { class: "notice" }, "No buffer with this id is registered.") : null,
      el(
        "section",
        { class: "grid2" },
        card(
          "Usage",
          "Crops drawn per day, and the requests answered empty (different units: one request draws n crops).",
          ...(status
            ? [healthLine(usage), sparkline(days.slice(-60))]
            : [el("div", { class: "empty" }, "Not sourced yet.")]),
          retired
            ? pagedTable("buffer-usage:" + id, {
                source: pageOf(`/usage/${e}`, "days", null, true),
                search: false,
                empty: "No usage yet.",
                cols: [
                  { key: "day", label: "day", cell: d => d.day },
                  { key: "draws", label: "draws", num: true, cell: d => num(d.draws) },
                  { key: "consumers", label: "consumers", num: true, cell: d => num(d.consumers) },
                  { key: "empty", label: "empty", num: true, cell: d => num(d.empty) },
                ],
              })
            : null,
        ),
        card(
          "Recipe",
          "What this buffer was built with, and what it serves.",
          status
            ? kv([
                ["spec", spec ? el("a", { href: SUBJECT_HASH.buffer(spec.key) }, spec.key) : cfg.spec_name],
                ["spec hash", code(cfg.spec_hash, 12)],
                ["catalog commit", code(cfg.catalog_ref, 12)],
                ["pyramid levels", (cfg.levels || []).join(", ")],
                ["patch size (px)", num(cfg.patch)],
                ["channels", String(cfg.channels ?? "")],
                [
                  "tensoriser",
                  cfg.recipe
                    ? `${cfg.recipe.tensorizer || cfg.tensorizer || ""} at harness ${short(cfg.recipe.harness_commit, 10)}`
                    : cfg.tensorizer,
                ],
                ["plan hash", code(cfg.plan_hash, 12)],
                [
                  "split fractions",
                  Object.entries(cfg.splits || {})
                    .map(([k, v]) => `${k} ${v}`)
                    .join(" · "),
                ],
                ["servable volumes", servableText(status.servable)],
                ["volumes registered", num(status.volumes)],
                ["empty answers allowed", cfg.max_empty_fraction != null ? pct(cfg.max_empty_fraction) : null],
                (status.quarantined || []).length
                  ? [
                      "quarantined",
                      el("span", { class: "pills" }, assetPill("quarantined"), " " + status.quarantined.join(", ")),
                    ]
                  : null,
              ])
            : el("div", { class: "empty" }, evs.length ? "Not sourced yet." : "The buffer's status did not load."),
        ),
      ),
      card(
        "Events",
        "What happened to this buffer, newest first.",
        pagedTable("buffer-events:" + id, {
          source: pageOf(`/buffers/${e}/events`, "events"),
          empty: "No event yet.",
          cols: [
            { key: "seq", label: "#", num: true, cell: x => num(x.seq) },
            { key: "kind", label: "event", cell: x => statePill(x.kind) },
            { key: "at", label: "when", cell: x => when(x.at) },
            { label: "detail", wrap: true, cell: x => x.detail || "" },
          ],
        }),
      ),
      status
        ? card(
            "Dataset pool",
            "One row per dataset of the plan's pool: servable, fetching with its job, staging, blocked by quota, waiting for a drop, failed with the reason.",
            pagedTable("buffer-pool:" + id, {
              source: pageOf(`/buffers/${e}/datasets`, "datasets"),
              sort: "dataset",
              empty: "No dataset in the pool yet.",
              cols: [
                { key: "dataset", label: "dataset", cell: x => datasetLink(x.record) },
                { label: "issue", cell: x => ghIcon(meta.datasets_repo, x.issue) },
                {
                  label: "files",
                  title: "the dataset's saved crops",
                  cell: x => folderLink(roots.storage_root, x.folder),
                },
                {
                  label: "staged",
                  title: "the staged artifact this buffer draws from",
                  cell: x => folderLink(roots.staged_root, x.staged || (levelsKey && `${levelsKey}/${x.record}`)),
                },
                { label: "preview", cell: x => thumb(x) },
                { key: "status", label: "status", cell: x => statePill(x.status) },
                { label: "reason", wrap: true, cell: x => x.reason || "" },
                { key: "job", label: "job", cell: x => (x.job ? el("code", {}, x.job) : "") },
                { key: "updated", label: "updated", cell: x => when(x.updated_at) },
              ],
            }),
          )
        : null,
      status
        ? el(
            "section",
            { class: "grid2" },
            card(
              "Volumes",
              `${num(status.volumes || 0)} registered, the most drawn from first.`,
              pagedTable("buffer-volumes:" + id, {
                source: pageOf(`/buffers/${e}/volumes`, "volumes"),
                sort: "draws",
                dir: "desc",
                empty: "No volume registered.",
                cols: [
                  { key: "volume", label: "volume", cell: v => el("code", {}, v.id) },
                  { key: "draws", label: "draws", num: true, cell: v => num(v.draws) },
                  { key: "missing", label: "", cell: v => (v.missing ? pill("missing", "warn") : "") },
                ],
              }),
            ),
            card(
              "Consumers",
              "The training jobs registered with this buffer.",
              pagedTable("buffer-consumers:" + id, {
                source: pageOf(`/buffers/${e}/consumers`, "consumers"),
                sort: "last_seen",
                dir: "desc",
                search: false,
                empty: "No consumer has registered.",
                cols: [
                  { key: "name", label: "name", cell: c => c.name },
                  { key: "host", label: "host", cell: c => c.host },
                  { label: "pid", num: true, cell: c => String(c.pid ?? "") },
                  { key: "rank", label: "rank", num: true, cell: c => (c.rank == null ? "" : String(c.rank)) },
                  { key: "split", label: "split", cell: c => c.split },
                  { key: "last_seen", label: "last seen", cell: c => when(c.last_seen) },
                  { label: "", cell: c => (c.stale ? pill("stale", "warn") : "") },
                ],
              }),
            ),
          )
        : null,
    ].filter(Boolean),
  );
}
