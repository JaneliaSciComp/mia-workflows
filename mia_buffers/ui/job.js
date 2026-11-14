// A job's record (`#job/<address>`): what ran and how it ended, an agent's prompt and conversation, the script,
// what the cluster was asked and what it answered, and the ends of stdout and stderr. A record holds what a
// sandboxed job printed: every value of it is inserted as a text node, never parsed as HTML or as markdown.
import { $, ago, card, el, kv } from "./dom.js";
import { api, banner } from "./api.js";

const STILL_RUNNING = "This job is still running, or its end was not recorded.";
const NO_RECORD = "No record for this job."; // a job that ran before records were kept has none
const PREVIEW_CHARS = 120; // how much of a tool call's input its one line shows
// what LSF says a job used, in the order shown
const USAGE_LABELS = [
  ["max_mem", "peak memory"],
  ["avg_mem", "average memory"],
  ["memlimit", "memory limit"],
  ["cpu_used", "CPU used"],
  ["run_time", "run time"],
  ["slots", "slots"],
];

// a value of a record as text: a string as it is, anything else as JSON
const text = value => (typeof value === "string" ? value : JSON.stringify(value));
const shown = value => value != null && value !== "";
// a part of a record that should be an object, or {} when it is absent or anything else
const part = value => (value && typeof value === "object" && !Array.isArray(value) ? value : {});
const lastOf = address => address.split("/").pop();
function stamp(time) {
  // a time as the record has it, and how long ago that was
  if (!shown(time)) return null;
  const since = ago(time);
  return since ? `${text(time)} (${since})` : text(time);
}
// the fields of `object` that hold something, as rows of a key/value list
const rows = object =>
  Object.entries(object)
    .filter(([, value]) => shown(value))
    .map(([key, value]) => [key, text(value)]);
const block = value => el("pre", {}, text(value));

function summaryRows(record) {
  // how the job ended and what it used; for an agent also its model, cost and number of turns
  const outcome = part(record.outcome),
    usage = part(record.usage),
    agent = part(record.agent);
  const state = shown(outcome.state)
    ? el("span", { class: "pill" + (outcome.state === "DONE" ? "" : " warn") }, text(outcome.state))
    : null;
  const found = [
    ["exit code", outcome.exit_code],
    ["queue", outcome.queue ?? part(record.submit).queue],
    ["host", outcome.exec_host],
    ["job id", record.job_id],
    ["submitted", stamp(record.submitted_at)],
    ["finished", stamp(record.finished_at)],
    ...USAGE_LABELS.map(([name, label]) => [label, usage[name]]),
    ["model", agent.model],
    ["cost", typeof agent.cost_usd === "number" ? "$" + agent.cost_usd.toFixed(2) : null],
    ["turns", agent.num_turns],
  ];
  return [
    ...(state ? [["state", state]] : []),
    ...found.filter(([, value]) => shown(value)).map(([label, value]) => [label, text(value)]),
  ];
}

function turnNode(turn) {
  // one turn of an agent's conversation: what it said as it wrote it, its thinking folded, a tool call folded
  // to its name and one line of its input; a sub-agent's turn is indented
  const from = part(turn),
    sub = from.sub ? " subagent" : "";
  if (from.role === "assistant") return el("div", { class: "turn said" + sub }, text(from.text));
  if (from.role === "thinking")
    return el(
      "details",
      { class: "turn" + sub },
      el("summary", {}, "Thinking"),
      el("div", { class: "said" }, text(from.text)),
    );
  if (from.role !== "tool") return null;
  const line = JSON.stringify(from.input) ?? "";
  return el(
    "details",
    { class: "turn" + sub },
    el(
      "summary",
      {},
      el("b", {}, text(from.name)),
      " ",
      el("code", {}, line.length > PREVIEW_CHARS ? line.slice(0, PREVIEW_CHARS) + "…" : line),
      from.error === true ? " " : null,
      from.error === true ? el("span", { class: "pill warn" }, "failed") : null,
    ),
    el("pre", {}, JSON.stringify(from.input, null, 2) ?? ""),
  );
}

export function jobView(record, address) {
  // the nodes of a record's page, top to bottom
  const whole = part(record),
    logs = part(whole.logs),
    agent = whole.agent ? part(whole.agent) : null;
  const { name, command, ...asked } = part(whole.request);
  const askedRows = rows(asked),
    answerRows = rows(part(whole.submit));
  const outputs = ["stdout", "stderr"].filter(stream => shown(logs[stream]));
  return [
    el("h2", {}, shown(name) ? text(name) : lastOf(address)),
    el("div", { class: "small mono" }, address),
    whole.outcome ? null : el("div", { class: "notice" }, STILL_RUNNING),
    kv(summaryRows(whole)),
    agent &&
      card(
        "Conversation",
        "What the agent was asked and what it said, thought and called, in order. What a tool answered is not kept.",
        el("details", { class: "turn" }, el("summary", {}, "Prompt"), block(agent.prompt ?? "")),
        ...(Array.isArray(agent.turns) ? agent.turns : []).map(turnNode),
      ),
    shown(command) && card("Script", null, block(command)),
    (askedRows.length || answerRows.length) &&
      card(
        "Cluster",
        null,
        askedRows.length ? el("div", { class: "sub" }, "What the job asked for.") : null,
        askedRows.length ? kv(askedRows) : null,
        answerRows.length ? el("div", { class: "sub" }, "What csub answered when it was submitted.") : null,
        answerRows.length ? kv(answerRows) : null,
      ),
    outputs.length &&
      card(
        "Output",
        "The end of what the job printed.",
        ...outputs.flatMap(stream => [el("div", { class: "sub" }, stream), block(logs[stream])]),
      ),
  ].filter(Boolean);
}

export async function viewJob(address) {
  const root = $("jobBody"),
    hash = location.hash;
  root.replaceChildren(el("div", { class: "empty" }, "loading…"));
  const path = address.split("/").map(encodeURIComponent).join("/"); // an address is a path: its slashes stay
  let record = null,
    failure = null;
  try {
    record = await api(`/jobs/${path}`);
  } catch (e) {
    failure = e;
  }
  if (location.hash !== hash) return; // the reader moved on while this loaded
  if (!failure) return root.replaceChildren(...jobView(record, address));
  if (failure.status !== 404) banner(`GET /jobs/${address} → ${failure.status || "network error"}: ${failure.message}`);
  root.replaceChildren(
    el("h2", {}, lastOf(address)),
    el("div", { class: "empty" }, failure.status === 404 ? NO_RECORD : "The record did not load."),
  );
}
