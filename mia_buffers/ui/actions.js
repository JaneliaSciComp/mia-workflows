// Asking the service for something: what each button says and does, the dialogs before a send, a subject's
// action bar, and the Prepare button.
import { toPrepYaml } from "./spec.js";
import { el, num, toEpoch } from "./dom.js";
import { api, banner, JSON_HEADERS, newKey, S } from "./api.js";

// ---- a subject's actions (spec webapp-actions §8). The server decides what a stage accepts (`subject.actions`)
// and refuses anything else; the page only offers it.
// What a button does, said on hover. BUTTON_HELP: the buttons with a control of their own. ACTION_HELP: a subject's
// action bar, by kind and action; a function where the stage changes what the action means.
export const BUTTON_HELP = {
  plan: "Save this spec and start a round that plans the pool and stops there. Nothing is downloaded until you press Run.",
  dropped: "Tell the round that this dataset's files are in its incoming/ folder. Press it once the copy has finished: the worker checks them and moves them into the cache.",
  revokeTicked: "Withdraw the ticked datasets from this buffer: their volumes are unregistered and their staged pyramids are quarantined. Nothing is deleted.",
  retry: "Send the same action again. It carries the same key, so it cannot run twice.",
  prepare: "File one preparation request for these datasets: each one gets a run of the preparation workflow, and the request's page follows them.",
  prepareTicked: "File one preparation request for the ticked datasets: each one gets a run of the preparation workflow, also one that is already prepared or that stopped. One in a round is left to it.",
  specFrom: "Open New buffer on a spec that selects exactly these datasets. Only the prepared ones can enter a buffer; the plan there says which are in.",
  clearSelection: "Untick every dataset.",
  sendReject: "Send the rejection with this feedback to the agent that fixes the dataset's reader.",
  cancelReject: "Close this without sending anything.",
  approveHosts: "Allow jobs to download from the ticked hosts, then start.",
  startUnapproved: "Start with the policy as it is. A dataset on one of these hosts stops and needs a person; a buffer waits for that dataset's files to be copied in by hand, or refuses its job.",
  cancelHosts: "Close this without starting anything.",
  showChange: "Redraw the page with what changed; what you typed here is dropped.",
};
export const ACTION_HELP = {
  dataset: {
    go: s => s.stage === "debugging" ? "Run a new round now, without waiting for the debugging agent. Use it when you already know what was wrong and have fixed it."
      : s.stage === "review" ? "Start over: the crops under review are dropped unsaved and a new round prepares the dataset again, with a judge's verdict on its crops. If a reader's pull request made these crops, its current head is tested again."
      : s.stage !== "new" ? "Start a new round of the preparation workflow from the beginning. After a stop it picks up the reader's open pull request, if there is one." + (s.stage === "done" ? " Until its new crops are approved the dataset is not prepared, and a new buffer plan leaves it out." : "")
      : s.again ? "Run the preparation workflow again: a new round makes more sample crops, which are saved once you approve them. Until then the dataset is not prepared, and a new buffer plan leaves it out."
      : "Start the preparation workflow: an agent matches the dataset to a reader, then sample crops are made for you to review.",
    approve: "Save the crops under review: the dataset becomes prepared and buffers can use it. If a new reader made them, its pull request is merged first.",
    reject: "Send the crops back with your feedback: the agent fixes the reader, and new crops come back for review.",
    revoke: "Withdraw this dataset's saved crops: its staged data is quarantined and buffers stop drawing from it. Nothing is deleted, and Run again repairs it.",
    cancel: "Stop the round in progress: its cluster jobs are killed and nothing is saved.",
  },
  buffer: {
    approve: s => s.stage === "plan-review" ? "Accept the plan and start the jobs: the pool's datasets are downloaded and converted, then the round stops for your approval."
      : "Serve the buffer as the board shows it. The share each weighting group has now becomes the floor it is judged against.",
    keep: "Keep this idle buffer: it goes back to ready instead of being retired.",
    retire: "Release the buffer's staged data: training jobs are refused from then on, and the space can be reclaimed.",
    cancel: "Stop the round in progress: its cluster jobs are killed and its buffer is retired.",
    stop: "For a run that does not answer Cancel: the workflow is terminated and the spec's buffers are retired. A cluster job already running is not killed.",
  },
  prep: {
    go: "Run the request again: its datasets that have not settled are handed to the preparation workflow again.",
    cancel: "Stop the request, and the dataset rounds it started.",
    stop: "For a request that does not answer Cancel: its workflow is terminated. The dataset rounds it started keep running.",
  },
};
const actionHelp = (subject, action) => { const help = ACTION_HELP[subject.kind]?.[action]; return typeof help === "function" ? help(subject) : help; };
export const ACTION_ORDER = ["approve", "reject", "keep", "go", "revoke", "retire", "cancel", "stop"];
const ACTION_WORDS = { approve: "Approve", reject: "Reject…", keep: "Keep", go: "Run again", revoke: "Revoke", retire: "Retire", cancel: "Cancel the round", stop: "Force stop" };
// the word on an action's button; the Buffers list names what a spec waits for by the same words
export const actionWord = (subject, action) => action === "approve" && subject.kind === "buffer" && subject.stage === "plan-review" ? "Run"  // the plan's gate: this starts the jobs
  : action === "go" && subject.kind === "buffer" ? "Plan" : action === "dropped" ? "Files are in place"  // a spec's own controls
  : action === "go" && subject.kind === "dataset" && subject.stage === "debugging" ? "Run again now"  // or wait: the debugger runs it again itself
  : action === "go" && subject.kind === "dataset" && subject.stage === "review" ? "Restart"  // beside Approve and Reject: neither verdict, the set is dropped
  : action === "go" && subject.stage === "new" ? (subject.kind === "dataset" ? (subject.again ? "Start again" : "Start") : "Run") : ACTION_WORDS[action] || action;
const ACTION_CONFIRM = {
  revoke: "Revoke this dataset's saved crops? Buffers that drew from it are degraded; nothing is deleted.",
  retire: "Retire this buffer? Training jobs reading it are refused and its staged data is released.",
  cancel: s => s.kind === "prep" ? "Cancel the request's round in progress? The dataset rounds it started are stopped too."
    : "Cancel the round in progress? Its cluster jobs are killed and the work so far is not kept.",
  stop: "Force stop? Use it when Cancel gets no answer. The workflow is terminated and the buffers are retired; a cluster job already running is NOT killed, and a request's dataset rounds keep running.",
};
const actionConfirm = (subject, action) => { const text = ACTION_CONFIRM[action]; return typeof text === "function" ? text(subject) : text; };
const UNDELIVERED_AFTER_S = 10;  // an action still pending this long was not delivered: the service died, or the answer was lost

// ---- sending an action
// What is on screen, kept by whoever draws it: `subject` is the subject page's {kind, key, number} (what the
// stream refetches), `view` the view's name, and `refresh()` redraws it the way the stream does.
export const onScreen = { subject: null, view: "", refresh: () => {} };
let askedTimer = 0;  // looks again when an asked action's wait ends, should its workflow never answer
export async function sendAction(subject, body, after) {  // the answer is shown by the redraw that follows (`after`), a refusal by the banner
  const send = { method: "POST", headers: JSON_HEADERS, body: JSON.stringify(body) };
  try {
    if (subject.kind === "dataset") await api(`/datasets/${encodeURIComponent(subject.key)}/actions`, { method: "POST", headers: JSON_HEADERS, body: send.body });
    else if (subject.kind === "buffer") await api(`/specs/${encodeURIComponent(subject.key)}/actions`, { method: "POST", headers: JSON_HEADERS, body: send.body });
    else await api(`/prep/${subject.number}/actions`, { method: "POST", headers: JSON_HEADERS, body: send.body });
  } catch (e) { banner(e.status === 409 ? `Not sent. ${e.message}` : `${body.action}: not delivered (${e.status || "network error"}): ${e.message}`); }
  after();
}

// ---- the dialogs before a send
// Reject has more to enter: a dialog takes the feedback, and a rejection with none cannot be sent. The dialog hangs
// off the body, so a redraw of the view behind it (the stream's) does not take what was typed.
function askReject(subject, send) {
  const note = el("textarea", { rows: 7, placeholder: "What is wrong with these crops?", "aria-label": "what is wrong with these crops" });
  const sendButton = el("button", { type: "submit", class: "btn danger", value: "send", title: BUTTON_HELP.sendReject, disabled: true }, "Send rejection");
  note.addEventListener("input", () => { sendButton.disabled = !note.value.trim(); });
  const box = el("dialog", { class: "ask", "aria-labelledby": "askTitle" }, el("form", { method: "dialog" },
    el("h2", { id: "askTitle" }, "Reject these crops"), el("div", { class: "mono small" }, subject.key),
    el("p", { class: "small" }, "What you write goes to the agent that fixes the dataset's reader. It then makes new crops, which come back for review."),
    note,
    el("div", { class: "actions" }, sendButton, el("button", { type: "submit", class: "btn", value: "cancel", title: BUTTON_HELP.cancelReject }, "Cancel"))));
  box.addEventListener("close", () => { if (box.returnValue === "send" && note.value.trim()) send(note.value.trim()); box.remove(); });
  document.body.append(box);
  box.showModal();
}
// Before a run starts, the service says which hosts it needs that the csub policy lacks (`run`: {records}, {prep} or
// {spec}). `send(hosts)` is called once: [] when nothing is missing or the person starts without approving, the ticked
// hosts on "Approve and start". On Cancel, and when the check fails, it is not called. The promise settles when the
// check has failed or the dialog has closed, whether or not `send` ran.
export async function askHosts(run, send) {
  let missing;
  try { missing = (await api("/hosts/missing", { method: "POST", headers: JSON_HEADERS, body: JSON.stringify(run) })).hosts; }
  catch (e) { banner(`Hosts not checked (${e.status || "network error"}): ${e.message}. Nothing was started.`); return; }
  if (!missing.length) { send([]); return; }
  const ticks = missing.map(() => el("input", { type: "checkbox" }));
  const all = el("input", { type: "checkbox", "aria-label": "select all hosts" });
  const approveButton = el("button", { type: "submit", class: "btn", value: "approve", title: BUTTON_HELP.approveHosts }, "Approve and start");
  const refresh = () => { approveButton.disabled = !ticks.some(tick => tick.checked); all.checked = ticks.every(tick => tick.checked); };
  all.addEventListener("change", () => { ticks.forEach(tick => { tick.checked = all.checked; }); refresh(); });
  ticks.forEach(tick => tick.addEventListener("change", refresh));
  const rows = missing.map((m, n) => el("label", { class: "row" }, ticks[n], " ", el("span", { class: "mono" }, m.host), " ",
    el("span", { class: "small", title: m.records.join(", ") }, `${m.records.length} ${m.records.length === 1 ? "dataset" : "datasets"}`)));
  const box = el("dialog", { class: "ask hosts", "aria-labelledby": "askTitle" }, el("form", { method: "dialog" },
    el("h2", { id: "askTitle" }, "Hosts to approve"),
    el("p", { class: "small" }, "Jobs of this deployment may download from an approved host from then on."),
    el("label", {}, all, " Select all"), ...rows,
    el("div", { class: "actions" }, approveButton,
      el("button", { type: "submit", class: "btn", value: "start", title: BUTTON_HELP.startUnapproved }, "Start without approving"),
      el("button", { type: "submit", class: "btn", value: "cancel", title: BUTTON_HELP.cancelHosts }, "Cancel"))));
  refresh();
  await new Promise(settle => {
  box.addEventListener("close", () => {
    if (box.returnValue === "approve") send(missing.filter((m, n) => ticks[n].checked).map(m => m.host));
    else if (box.returnValue === "start") send([]);
    box.remove();
    settle();
  });
  document.body.append(box);
  box.showModal();
  });
}

// ---- a subject's action bar
// what the service is asked about before this subject's action starts a run; null for the actions that start none
const hostsRun = (subject, action) => action === "go" && subject.kind === "dataset" ? { records: [subject.key] }
  : action === "go" && subject.kind === "prep" ? { prep: subject.number }
  : action === "approve" && subject.kind === "buffer" && subject.stage === "plan-review" ? { spec: subject.key } : null;
export function actionBar(subject, after, skip) {  // `skip`: actions that have their own control on the page (a spec's go, revoke)
  const bar = el("div", { class: "actions" });
  const offered = ACTION_ORDER.filter(a => subject.actions.includes(a) && !(skip || []).includes(a));
  // an action was sent and its workflow has not answered: the stage, and so these buttons, are still the old
  // ones, so none can be pressed until the answer (or the end of the wait) reloads this
  const asked = subject.asked;
  if (asked) { clearTimeout(askedTimer); askedTimer = setTimeout(onScreen.refresh, asked.wait_s * 1000 + 300); }
  let checking = false;  // a click while the host check or its dialog is open starts nothing
  for (const action of offered) {
    const word = actionWord(subject, action);
    const sending = action === "reject" ? "Rejecting…" : word + "…";  // Reject… already ends in the dots of "more to enter"
    // a verdict names the revision this page is showing: the server refuses it if the subject has moved on
    const verdict = action === "approve" || action === "reject" ? { revision: subject.revision } : {};
    bar.append(el("button", { type: "button", class: "btn" + (ACTION_CONFIRM[action] ? " danger" : ""), title: actionHelp(subject, action), disabled: !!subject.asked, onclick: e => {
      const send = more => {
        bar.querySelectorAll("button").forEach(b => { b.disabled = true; });
        e.target.textContent = sending;
        sendAction(subject, { action, key: newKey(), ...verdict, ...more }, after);
      };
      if (action === "reject") return askReject(subject, note => send({ note }));
      if (ACTION_CONFIRM[action] && !confirm(actionConfirm(subject, action))) return;
      const run = hostsRun(subject, action);
      if (run) {
        if (checking) return;
        checking = true;
        return askHosts(run, hosts => send(hosts.length ? { approve_hosts: hosts } : {})).finally(() => { checking = false; });
      }
      send({});
    } }, asked && asked.action === action ? sending : word));
  }
  if (asked) bar.append(el("span", { class: "small" }, "Sent: waiting for its workflow to answer."));
  if (!subject.actions.length) bar.append(el("span", { class: "small" }, "Nothing can be asked of it at this stage."));
  return bar;
}
export function undelivered(subject, actions, after) {  // an action that did not reach its workflow, with a Retry that sends the same one
  const box = el("div"), now = Date.now() / 1000;
  for (const a of actions) {
    const lost = a.state === "failed" || (a.state === "pending" && now - (toEpoch(a.at) || now) > UNDELIVERED_AFTER_S);
    if (!lost) continue;
    const line = el("div", { class: "notice" }, el("b", {}, a.action), ` was not delivered${a.detail ? ": " + a.detail : ""}. `);
    const again = { action: a.action, key: a.key, ...(a.payload?.note ? { note: a.payload.note } : {}), ...(a.payload?.spec ? { spec: a.payload.spec, plan_first: !!a.payload.plan_first } : {}), ...(a.payload?.records ? { records: a.payload.records } : {}), ...(a.payload?.record ? { record: a.payload.record } : {}) };
    if (a.key) line.append(el("button", { type: "button", class: "btn", title: BUTTON_HELP.retry, onclick: e => { e.target.disabled = true; sendAction(subject, again, after); } }, "Retry"));
    box.append(line);
  }
  return box;
}

// ---- the Prepare button: a preparation request is posted to the service, which runs it
const PREP_MAX = 200;  // the most datasets one preparation request takes (mia_buffers.spec.MAX_PREP_RECORDS)
// `again` (the Datasets list's ticks): every ticked dataset gets a round, also one that is prepared or that stopped;
// its value is how many of them are prepared or in a round already. Without it only what is not prepared is asked for.
export function prepareButton(container, all, blocker, done, again, text) {  // `text`: the button's word, where the count stands beside it
  // a request takes at most PREP_MAX datasets: a longer list is prepared in parts, the first PREP_MAX now
  const todo = all.slice(0, PREP_MAX), why = blocker;
  let checking = false;  // a click while the host check or its dialog is open starts nothing
  const label = all.length > PREP_MAX ? `Prepare the first ${PREP_MAX} of ${num(all.length)} datasets` : text || (todo.length === 1 ? "Prepare this dataset" : `Prepare these ${num(todo.length)} datasets`);
  if (why) { container.replaceChildren(el("button", { type: "button", class: "btn", disabled: true, title: why }, label), el("span", { class: "small" }, why)); return; }
  container.replaceChildren(el("button", { type: "button", class: "btn", ...(again == null ? { title: BUTTON_HELP.prepare } : { title: BUTTON_HELP.prepareTicked }), onclick: async e => {
    if (checking) return;
    checking = true;
    askHosts({ records: todo }, hosts => start(e.target, hosts)).finally(() => { checking = false; });
  } }, label));
  async function start(button, hosts) {
    button.disabled = true;
    // more than 25 records need the count written into the request: the button's label names it
    const yaml = toPrepYaml(todo, S.ref, S.meta.catalog_repo) + (again == null ? "" : "again: true\n") + (todo.length > 25 ? `max_records: ${todo.length}\n` : "");
    try {
      const action = await api("/prep", { method: "POST", headers: JSON_HEADERS, body: JSON.stringify({ spec: yaml, key: newKey(), ...(hosts.length ? { approve_hosts: hosts } : {}) }) });
      if (done) done(todo);
      location.hash = "#find/" + action.number;
    } catch (err) { banner(err.status === 422 ? `The request was refused: ${err.message}` : `Not started (${err.status || "network error"}): ${err.message}`); button.disabled = false; }
  }
}
