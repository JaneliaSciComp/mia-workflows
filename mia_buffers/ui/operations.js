// The operator's views: Cache, Cleaner, Preparation and Decisions.
import {
  $,
  ago,
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
  tipOn,
  VERDICT_KIND,
  when,
} from "./dom.js";
import { mdInline } from "./markdown.js";
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
// ---- cleaner: what the last sweep found wrong with the installation, and its cases (spec cleaner §4.8)
const CASE_HREF = c => (SUBJECT_HASH[c.kind] ? SUBJECT_HASH[c.kind](c.key) : c.kind === "job" ? "#job/" + c.key : null);
// a step in words, as the timeline's Cleaner entries say it (mia_buffers.cleaner.STEP_WORDS)
const STEP_WORDS = {
  cancel: "cancel the round",
  stop: "force-stop the run",
  kill_jobs: "end the round's cluster jobs",
  go: "run it again",
  recheck: "tell the request to look at its datasets again",
  terminate: "terminate the run",
  kill_job: "kill the job",
  snippets_go: "ask autodev to try again",
  revise: "ask autodev to merge main into the pull request",
  await_autodev: "wait until autodev has taken it",
  close: "close the snippet pull request and issue",
  wait: "wait",
  diagnose: "diagnose it",
  report_defect: "report the defect",
};
const plain = text => String(text || "").replaceAll("`", "");
// a rule (mia_buffers.cleaner) for someone who has not read the rules: the problem in a few words, then what it
// means and what the cleaner does about it
const RULE_HELP = {
  "no run": [
    "No workflow run",
    "The registry says a round is in progress, but Temporal has no open run for it, so nothing will ever move it. The cleaner cancels the round, ends its cluster jobs and resumes it; a buffer is planned again.",
  ],
  "frozen run": [
    "Workflow run is frozen",
    "The run's workflow task fails every time, so the run cannot take a step. The cleaner force-stops it, ends its cluster jobs and resumes it. A force stop loses what the round had, so it starts from the beginning. A buffer's run is left to a person, because stopping it retires the buffer's versions.",
  ],
  "activity retrying": [
    "A step keeps failing",
    "One activity of the run has failed many times in a row. If its worker does not know the activity, the worker needs a restart. If a service looks away, the cleaner looks again in an hour. Otherwise it diagnoses.",
  ],
  "request behind": [
    "Request has not noticed its datasets",
    "A preparation request rests although each of its datasets is prepared or shelved. The cleaner tells it to look at them again.",
  ],
  "autodev's run failed": [
    "Snippet agent's job failed",
    "The dataset waited for a reader on a snippets issue, and autodev's agent job there failed before it opened a pull request. The cleaner asks autodev to try once more, waits until it has started, then resumes the dataset's round. A second failure on the same issue is diagnosed.",
  ],
  "merge conflict": [
    "Snippet pull request cannot merge",
    "The pull request the dataset waited for conflicts with main. The cleaner asks autodev to merge main into it and resumes the dataset's round. When autodev has no revision left it closes the pull request and restarts the dataset.",
  ],
  "revision limit": [
    "Revisions used up",
    "The work stopped at its limit of revisions. No remedy is automatic: the cleaner diagnoses it.",
  ],
  "time limit": [
    "Job ran out of time",
    "A cluster job was ended at its time limit. No remedy is automatic: the cleaner diagnoses it.",
  ],
  "passing cause": [
    "Passing failure",
    "The round stopped on something that usually passes by itself: a dropped connection, a time-out, a busy service. The cleaner waits, then resumes it, at most twice, and diagnoses it after that.",
  ],
  "merged since": [
    "Merged after the stop",
    "The round stopped because autodev reported a problem with the reader's pull request, and that pull request was merged afterwards. The cleaner resumes the dataset, once per pull request: the round finds the merge and saves the crops that were approved.",
  ],
  "fixed since": [
    "Worker restarted since",
    "The round stopped on an error of the worker, and the worker has since started on newer code. The cleaner resumes it, once per commit.",
  ],
  "budget used up": [
    "Budget used up",
    "The round was refused a job because the budget of what started it is used up. Only a person raises a cap: `autonomy.root` in mia.yaml, then a restart of the workers. After that, resume it.",
  ],
  "host not approved": [
    "Host not approved",
    "A job needs a host nobody has approved. Only a person approves hosts: Resume on the subject's page, and check them.",
  ],
  "record gap": [
    "Catalog record is incomplete",
    "The round could not go on because the dataset's catalog record lacks something or needs a decision. The cleaner cannot change a record; it restarts the dataset once the record has changed.",
  ],
  "input changed": [
    "Catalog record changed",
    "The record a stopped dataset waited on has changed, so the cleaner restarts the dataset: a round is pinned to the record it began with.",
  ],
  "diagnosis": [
    "Cause not known",
    "No rule covers this stop, or the rule's remedy did not help. A read-only agent reads the subject's timeline and job records and proposes what to do. The cleaner carries out only what is on its own list of steps and leaves the rest to a person.",
  ],
  "unrunnable run": [
    "Run no worker can run",
    "An open workflow run of a type no worker registers: it logs an error on every task and never ends. The cleaner terminates it.",
  ],
  "left-over job": [
    "Left-over cluster job",
    "A cluster job is still alive though the round it belonged to has ended. The cleaner kills it.",
  ],
  "left-over snippet work": [
    "Left-over snippet issue",
    "A snippets issue, with its pull request, is still open though no dataset waits for it. The cleaner closes them.",
  ],
};
// a case's state as the row says it: [words, pill kind, what it means]
const CASE_STATUS = {
  "open": ["queued", "", "Found, and not started on. In report mode a case stays here."],
  "working": ["working", "", "The cleaner is part-way through its steps."],
  "waiting": ["waiting", "", "The steps are done; the cleaner is watching whether the subject moves on."],
  "gave-up": ["gave up", "warn", "The cleaner could not repair it; the case says what it takes."],
  "resolved": ["resolved", "pool", "The subject moved on, or the left-over thing is gone."],
  "dropped": ["dropped", "", "Somebody acted on the subject, so the cleaner let go of it."],
};
const stepWords = step => STEP_WORDS[step.do] || step.do;
// a job's record under its job's name; one of autodev's on a snippets issue (`<owner>__<repo>/issue-<n>/…`) says so
const jobName = where => where.replace(/^[^/]+__[^/]+\/issue-(\d+)\/.*\//, "snippets #$1 ").replace(/^.*\//, "");
export function caseRows(cases) {
  // a case as the page shows it. The row: what it is about, the problem in a few words, a status and how far the
  // cleaner got. The drill-down: what the problem means, what was found, each step with its result, the steps
  // left, what a person must do, the diagnosis, and the records (conversation and cluster logs) of the jobs it is
  // about: its round's, then the cleaner's own
  return cases.map(c => {
    const data = c.data || {},
      [problem, meaning] = RULE_HELP[c.rule] || [c.rule, ""],
      done = (data.steps || []).filter(step => step.do !== "needs"), // what a person must do is the case's `needs`
      plan = data.plan || [],
      found = data.diagnosis;
    return {
      href: CASE_HREF(c),
      about: c.key,
      rule: c.rule,
      problem,
      meaning,
      state: c.state,
      status: (CASE_STATUS[c.state] || [c.state])[0],
      progress: [done.length ? `${done.length} done` : "", plan.length ? `${plan.length} to go` : ""]
        .filter(Boolean)
        .join(", "),
      found: data.seen || "",
      did: done.map(step => ({
        what: stepWords(step),
        result: step.result,
        at: step.at,
        title: step.title,
        evidence: step.evidence,
      })),
      next: plan.map(stepWords),
      needs: c.state === "gave-up" ? data.needs || null : null,
      note: data.note || null,
      diagnosis: found ? { cause: found.cause, text: found.diagnosis ?? String(found) } : null,
      jobs: (c.jobs || []).map(where => ({ href: "#job/" + where, name: jobName(where) })),
      opened: c.opened,
      updated: c.updated,
    };
  });
}
// what a job or a record wrote, as inline markdown: text, never HTML
const said = text => el("p", {}, ...mdInline(String(text)));
function caseDetail(row) {
  const part = (title, ...kids) => (kids.some(Boolean) ? el("section", {}, el("h4", {}, title), ...kids) : null);
  return el(
    "div",
    { class: "casebody" },
    part("What it needs from you", row.needs && said(row.needs)),
    part(
      "What this kind of problem is",
      row.meaning ? el("p", {}, row.meaning) : null,
      el(
        "p",
        { class: "small" },
        `Rule “${row.rule}”. Opened `,
        when(row.opened),
        ", last change ",
        when(row.updated),
        ".",
      ),
    ),
    part("What the cleaner found", row.found && said(row.found)),
    part(
      "Diagnosis",
      row.diagnosis && said((row.diagnosis.cause ? `**${row.diagnosis.cause}:** ` : "") + row.diagnosis.text),
    ),
    part(
      "What the cleaner did",
      row.did.length
        ? el(
            "ol",
            {},
            ...row.did.map(step =>
              el(
                "li",
                {},
                el("b", {}, step.what),
                step.at ? el("span", { class: "small" }, " · ", when(step.at)) : null,
                step.title ? said(step.title) : null,
                step.evidence ? said(step.evidence) : null,
                step.result ? said("Result: " + step.result) : null,
              ),
            ),
          )
        : null,
    ),
    part(
      row.state === "open" ? "What it would do" : "What it will do next",
      row.next.length ? el("ol", {}, ...row.next.map(words => el("li", {}, words))) : null,
    ),
    part("Note", row.note && said(row.note)),
    part(
      "Job logs",
      row.jobs.length
        ? el("p", {}, ...row.jobs.flatMap((job, at) => [at ? ", " : "", el("a", { href: job.href }, job.name)]))
        : null,
    ),
  );
}
// the cases of one card: a short row each, which opens into the case's full description
function caseList(cases, empty) {
  if (!cases.length) return el("div", { class: "empty" }, empty);
  return el(
    "div",
    { class: "cases" },
    el(
      "div",
      { class: "caserow casehead" },
      ...["Subject", "Problem", "Status", "Steps", "Last change"].map(h => el("span", {}, h)),
    ),
    ...caseRows(cases).map(row => {
      const [, kind, help] = CASE_STATUS[row.state] || [];
      const status = el("span", { class: "pill" + (kind ? " " + kind : "") }, row.status);
      return el(
        "details",
        { class: "case" },
        el(
          "summary",
          { class: "caserow" },
          el("span", { class: "subject" }, row.href ? el("a", { href: row.href }, row.about) : row.about),
          el("span", {}, row.problem),
          el("span", {}, help ? tipOn(status, [{ b: row.status, t: help }]) : status),
          el("span", {}, row.progress || "—"),
          el("span", {}, when(row.updated)),
        ),
        caseDetail(row),
      );
    }),
  );
}
export async function viewCleaner() {
  const root = $("cleanerBody");
  root.replaceChildren(el("div", { class: "empty" }, "loading…"));
  const [report, listed] = await Promise.all([load("/cleaner"), load("/cases?days=7")]);
  const cases = listed?.cases || [];
  const inState = (...states) => cases.filter(c => states.includes(c.state));
  const findings = report?.findings || [];
  // act mode is the normal one and goes unsaid
  const modes = { off: " The cleaner is off.", report: " Report mode: cases are listed and nothing is done." };
  root.replaceChildren(
    card(
      "System status",
      report
        ? `Last checked ${ago(report.at)}.${modes[report.mode] || ""}${findings.length ? " The subjects behind a problem listed here are left alone until it clears." : ""}`
        : "The cleaner has not reported a sweep yet.",
      findings.length
        ? el("div", {}, ...findings.map(finding => el("div", { class: "notice" }, plain(finding.text))))
        : el("div", { class: "empty" }, "All services operating, no problems found."),
      report?.errors?.length ? kv([["errors of the last sweep", list(report.errors)]]) : null,
    ),
    card(
      "Needs you",
      "The cleaner could not resolve these. Each opens to what it takes: a decision, a correction to a record, a fix in the code.",
      caseList(inState("gave-up"), "Nothing."),
    ),
    card(
      "In work",
      "What the cleaner is resolving now, or would resolve in act mode.",
      caseList(inState("open", "working", "waiting"), "Nothing is stuck."),
    ),
    card(
      "Closed in the last week",
      "Resolved, or dropped because somebody acted on the subject.",
      caseList(inState("resolved", "dropped"), "Nothing yet."),
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
      "A request prepares a set of datasets under one budget. File one from Datasets (check datasets, Prepare) or from a buffer's Prepare step. Open one for its datasets, to cancel it or to run it again.",
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
    `${gate}: ${num(a.judged)} judged · ${num(a.agree)} agree · ${num(a.disagree)} disagree (${num(a.overturned || 0)} overturned by a person afterwards) · ${num(a.abstain)} abstained · ${num(a.pending)} awaiting a human`,
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
