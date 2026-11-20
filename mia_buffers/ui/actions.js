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
  plan: "Plan this buffer over the datasets that are prepared now. The round stops at its plan: nothing is downloaded until you press Run.",
  run: "Accept the plan and start the jobs: its datasets are downloaded and staged, then the round stops for your approval.",
  serve:
    "Serve this version as the table shows it. The share each weighting group has now becomes the floor it is judged against.",
  back: "Cancel the round in progress, so that the buffer can be changed. Its cluster jobs are killed and the work so far is not kept.",
  planAgain:
    "Plan this buffer again over the datasets that are prepared now. The version being served keeps serving until the new one is approved.",
  save: "Save this definition as a new buffer. Nothing runs yet: the next step prepares its datasets.",
  saveChanges: "Store the changed definition of this buffer. Nothing runs.",
  startOver: "Drop what is entered here and start from the defaults.",
  clone: "Open New buffer on a copy of this buffer's definition. Saving there makes a buffer of its own.",
  dropped:
    "Tell the round that this dataset's files are in its incoming/ folder. Press it once the copy has finished: the worker checks them and moves them into the cache.",
  revokeTicked:
    "Withdraw the checked datasets from this buffer: their volumes are unregistered and their staged pyramids are quarantined. Nothing is deleted.",
  retry: "Send the same action again. It carries the same key, so it cannot run twice.",
  prepare:
    "Prepare the selected datasets: one that never ran starts, one that stopped runs what is left, one that is prepared is left alone. Crops that wait at review are kept, and judged if the judge never looked. One whose round is running is left to it.",
  prepareRestart:
    "Prepare every selected dataset from scratch, also one that is prepared or waits for review: its crops are dropped and made again on today's catalog, readers and harness. One whose round is running is left to it.",
  specFrom:
    "Open New buffer on a definition that selects exactly these datasets. Only the prepared ones are in a plan; step 2 there says which fit.",
  sendReject: "Send the rejection with this feedback to the agent that fixes the dataset's reader.",
  cancelReject: "Close this without sending anything.",
  sendShelve: "Shelve it with this reason. Unshelve on its page takes it back.",
  cancelShelve: "Close this without shelving anything.",
  approveHosts: "Allow jobs to download from the checked hosts, then start.",
  startUnapproved:
    "Start with the policy as it is. A dataset on one of these hosts stops and needs a human; a buffer waits for that dataset's files to be copied in by hand, or refuses its job.",
  cancelHosts: "Close this without starting anything.",
  showChange: "Redraw the page with what changed; what you typed here is dropped.",
};
// Shelving is the same for every kind of subject: the service carries it out itself, and a workflow hears only the
// cancel of a round that was waiting for a verdict.
const SHELF_HELP = {
  shelve:
    "Set this aside with a reason. It leaves what Overview lists as needing a human, and nothing can be asked of it until someone unshelves it; a shelved dataset is also left out of every buffer plan and started by no preparation request. A round that waits for a verdict is cancelled first; otherwise its stage stays as it is.",
  unshelve: "Take this back: it is listed, planned and started again as its stage says.",
};
// how a dataset's Restart differs from its Resume: the first words of its tooltip at every stage
const FROM_SCRATCH = "Start over from scratch on today's catalog, readers and harness. ";
export const ACTION_HELP = {
  dataset: {
    ...SHELF_HELP,
    resume:
      "Run what is left. Every step that finished is kept (the match, the reader's pull request, the crops, the judge's verdict), and the round stays on the catalog, reader and harness versions it began with. A step that failed because of a fault in one of those fails again: use Restart.",
    go: s =>
      s.stage === "debugging"
        ? FROM_SCRATCH +
          "Run a new round now, without waiting for the debugging agent. Use it when you already know what was wrong and have fixed it."
        : s.stage === "review"
          ? FROM_SCRATCH +
            "The crops under review are dropped unsaved and a new round prepares the dataset again, with a judge's verdict on its crops. If a reader's pull request made these crops, its current head is tested again."
          : s.stage !== "new"
            ? FROM_SCRATCH +
              "A new round of the preparation workflow runs from the beginning. After a stop it picks up the reader's open pull request, if there is one." +
              (s.stage === "done"
                ? " Until its new crops are approved the dataset is not prepared, and a new buffer plan leaves it out."
                : "")
            : s.again
              ? "Run the preparation workflow again: a new round makes more sample crops, which are saved once you approve them. Until then the dataset is not prepared, and a new buffer plan leaves it out."
              : "Start the preparation workflow: an agent matches the dataset to a reader, then sample crops are made for you to review.",
    approve:
      "Save the crops under review: the dataset becomes prepared and buffers can use it. If a new reader made them, its pull request is merged first.",
    reject: s =>
      s.stage === "done"
        ? "Revoke the saved crops and send them back with your feedback: buffers stop drawing from the dataset, the agent fixes the reader, and new crops come back for review."
        : "Send the crops back with your feedback: the agent fixes the reader, and new crops come back for review.",
    revoke:
      "Withdraw this dataset's saved crops: its staged data is quarantined and buffers stop drawing from it. Nothing is deleted, and Restart repairs it.",
    cancel: "Stop the round in progress: its cluster jobs are killed and nothing is saved.",
    stop: "For a round that does not answer Cancel: its workflow is terminated. A cluster job already running is not killed.",
  },
  buffer: {
    ...SHELF_HELP,
    approve: s =>
      s.stage === "plan-review"
        ? "Accept the plan and start the jobs: the pool's datasets are downloaded and converted, then the round stops for your approval."
        : "Serve the buffer as the board shows it. The share each weighting group has now becomes the floor it is judged against.",
    keep: "Keep this idle buffer: it goes back to ready instead of being retired.",
    retire: "Release the buffer's staged data: training jobs are refused from then on, and the space can be reclaimed.",
    cancel: "Stop the round in progress: its cluster jobs are killed and its buffer is retired.",
    stop: "For a run that does not answer Cancel: the workflow is terminated and the buffer's versions are retired. A cluster job already running is not killed.",
  },
  prep: {
    ...SHELF_HELP,
    resume:
      "Run the request again: each of its datasets that never ran starts, and each that stopped runs what is left of its round. One that is prepared, shelved or in a round is left as it is.",
    go: "Run the request again from scratch: every dataset it names starts over on today's catalog, readers and harness, also one that is prepared or waits for review. One that is shelved or in a round is left as it is.",
    cancel: "Stop the request, and the dataset rounds it started.",
    stop: "For a request that does not answer Cancel: its workflow is terminated. The dataset rounds it started keep running.",
    recheck: "Look at the request's datasets again: it is done once each of them is prepared or shelved.",
  },
};
const actionHelp = (subject, action) => {
  const help = ACTION_HELP[subject.kind]?.[action];
  return typeof help === "function" ? help(subject) : help;
};
export const ACTION_ORDER = [
  "approve",
  "reject",
  "keep",
  "resume",
  "go",
  "recheck",
  "revoke",
  "retire",
  "shelve",
  "unshelve",
  "cancel",
  "stop",
];
const ACTION_WORDS = {
  approve: "Approve",
  reject: "Reject…",
  keep: "Keep",
  resume: "Resume",
  go: "Run again",
  recheck: "Check again",
  revoke: "Revoke",
  retire: "Retire",
  shelve: "Shelve…",
  unshelve: "Unshelve",
  cancel: "Cancel the round",
  stop: "Force stop",
};
// the word on an action's button; the Buffers list names what a spec waits for by the same words
export const actionWord = (subject, action) =>
  action === "approve" && subject.kind === "buffer" && subject.stage === "plan-review"
    ? "Run" // the plan's gate: this starts the jobs
    : action === "go" && subject.kind === "buffer"
      ? "Plan"
      : action === "dropped"
        ? "Files are in place" // a spec's own controls
        : action === "go" && subject.kind === "dataset" && subject.stage === "debugging"
          ? "Restart now" // or wait: the debugger runs it again itself
          : action === "go" && subject.stage === "new"
            ? subject.kind === "dataset"
              ? subject.again
                ? "Start again"
                : "Start"
              : "Run"
            : action === "go" && (subject.kind === "dataset" || subject.kind === "prep")
              ? "Restart" // from scratch; Resume, where it is offered, goes on with what stopped
              : ACTION_WORDS[action] || action;
const ACTION_CONFIRM = {
  revoke: "Revoke this dataset's saved crops? Buffers that drew from it are degraded; nothing is deleted.",
  retire: "Retire this buffer? Training jobs reading it are refused and its staged data is released.",
  cancel: s =>
    s.kind === "prep"
      ? "Cancel the request's round in progress? The dataset rounds it started are stopped too."
      : "Cancel the round in progress? Its cluster jobs are killed and the work so far is not kept.",
  stop: s =>
    s.kind === "dataset"
      ? "Force stop? Use it when Cancel gets no answer. The workflow is terminated; a cluster job already running is NOT killed."
      : "Force stop? Use it when Cancel gets no answer. The workflow is terminated and the buffers are retired; a cluster job already running is NOT killed, and a request's dataset rounds keep running.",
};
const actionConfirm = (subject, action) => {
  const text = ACTION_CONFIRM[action];
  return typeof text === "function" ? text(subject) : text;
};
const UNDELIVERED_AFTER_S = 10; // an action still pending this long was not delivered: the service died, or the answer was lost

// ---- sending an action
// What is on screen, kept by whoever draws it: `subject` is the subject page's {kind, key, number} (what the
// stream refetches), `view` the view's name, and `refresh()` redraws it the way the stream does. A view that
// redraws itself (a buffer's page) sets `redraw`, which `refresh()` then calls; a view that follows the stream
// puts its redraw under its name in `streams`. `health` is the last health check's answer.
export const onScreen = { subject: null, view: "", refresh: () => {}, redraw: null, streams: {}, health: null };
let askedTimer = 0; // looks again when an asked action's wait ends, should its workflow never answer
export async function sendAction(subject, body, after) {
  // the answer is shown by the redraw that follows (`after`), a refusal by the banner
  const send = { method: "POST", headers: JSON_HEADERS, body: JSON.stringify(body) };
  try {
    if (subject.kind === "dataset")
      await api(`/datasets/${encodeURIComponent(subject.key)}/actions`, {
        method: "POST",
        headers: JSON_HEADERS,
        body: send.body,
      });
    else if (subject.kind === "buffer")
      await api(`/specs/${encodeURIComponent(subject.key)}/actions`, {
        method: "POST",
        headers: JSON_HEADERS,
        body: send.body,
      });
    else await api(`/prep/${subject.number}/actions`, { method: "POST", headers: JSON_HEADERS, body: send.body });
  } catch (e) {
    banner(
      e.status === 409
        ? `Not sent. ${e.message}`
        : `${body.action}: not delivered (${e.status || "network error"}): ${e.message}`,
    );
  }
  after();
}

// ---- the dialogs before a send
// Reject and Shelve have more to enter: a dialog takes the feedback or the reason, and neither can be sent with
// none. The dialog hangs off the body, so a redraw of the view behind it (the stream's) does not take what was typed.
const NOTE_DIALOGS = {
  reject: {
    title: "Reject these crops",
    about:
      "What you write goes to the agent that fixes the dataset's reader. It then makes new crops, which come back for review.",
    ask: "What is wrong with these crops?",
    sending: "Rejecting…",
    send: { class: "btn danger", title: BUTTON_HELP.sendReject },
    sendWord: "Send rejection",
    cancel: { title: BUTTON_HELP.cancelReject },
  },
  shelve: {
    title: "Shelve this",
    about:
      "It leaves what Overview lists as needing a human, and no request or buffer plan takes it until it is unshelved. The reason is shown wherever it is listed, and in its timeline.",
    cancels: "Its round is waiting for a verdict: shelving cancels that round first, as Cancel does.",
    ask: "Why can it wait?",
    sending: "Shelving…",
    send: { class: "btn", title: BUTTON_HELP.sendShelve },
    sendWord: "Shelve",
    cancel: { title: BUTTON_HELP.cancelShelve },
  },
};
function askNote(subject, dialog, send) {
  const note = el("textarea", { rows: 7, placeholder: dialog.ask, "aria-label": dialog.ask });
  const sendButton = el("button", { type: "submit", value: "send", disabled: true, ...dialog.send }, dialog.sendWord);
  note.addEventListener("input", () => {
    sendButton.disabled = !note.value.trim();
  });
  const box = el(
    "dialog",
    { class: "ask", "aria-labelledby": "askTitle" },
    el(
      "form",
      { method: "dialog" },
      el("h2", { id: "askTitle" }, dialog.title),
      el("div", { class: "mono small" }, subject.name || subject.key),
      el("p", { class: "small" }, dialog.about),
      dialog.cancels && subject.actions.includes("cancel") ? el("p", { class: "small" }, dialog.cancels) : "",
      note,
      el(
        "div",
        { class: "actions" },
        sendButton,
        el("button", { type: "submit", class: "btn", value: "cancel", ...dialog.cancel }, "Cancel"),
      ),
    ),
  );
  box.addEventListener("close", () => {
    if (box.returnValue === "send" && note.value.trim()) send(note.value.trim());
    box.remove();
  });
  document.body.append(box);
  box.showModal();
}
// Before a run starts, the service says which hosts it needs that the csub policy lacks (`run`: {records}, {prep} or
// {spec}). `send(hosts)` is called once: [] when nothing is missing or the person starts without approving, the checked
// hosts on "Approve and start". On Cancel, and when the check fails, it is not called. The promise settles when the
// check has failed or the dialog has closed, whether or not `send` ran.
export async function askHosts(run, send) {
  let missing;
  try {
    missing = (await api("/hosts/missing", { method: "POST", headers: JSON_HEADERS, body: JSON.stringify(run) })).hosts;
  } catch (e) {
    banner(`Hosts not checked (${e.status || "network error"}): ${e.message}. Nothing was started.`);
    return;
  }
  if (!missing.length) {
    send([]);
    return;
  }
  const ticks = missing.map(() => el("input", { type: "checkbox" }));
  const all = el("input", { type: "checkbox", "aria-label": "select all hosts" });
  const approveButton = el(
    "button",
    { type: "submit", class: "btn", value: "approve", title: BUTTON_HELP.approveHosts },
    "Approve and start",
  );
  const refresh = () => {
    approveButton.disabled = !ticks.some(tick => tick.checked);
    all.checked = ticks.every(tick => tick.checked);
  };
  all.addEventListener("change", () => {
    ticks.forEach(tick => {
      tick.checked = all.checked;
    });
    refresh();
  });
  ticks.forEach(tick => tick.addEventListener("change", refresh));
  const rows = missing.map((m, n) =>
    el(
      "label",
      { class: "row" },
      ticks[n],
      " ",
      el("span", { class: "mono" }, m.host),
      " ",
      el(
        "span",
        { class: "small", title: m.records.join(", ") },
        `${m.records.length} ${m.records.length === 1 ? "dataset" : "datasets"}`,
      ),
      // a host approved for one dataset at a time (raw GitHub): no other dataset's jobs get it
      m.per_dataset
        ? el("span", { class: "small" }, m.records.length === 1 ? " · this dataset only" : " · these datasets only")
        : null,
    ),
  );
  const box = el(
    "dialog",
    { class: "ask hosts", "aria-labelledby": "askTitle" },
    el(
      "form",
      { method: "dialog" },
      el("h2", { id: "askTitle" }, "Hosts to approve"),
      el("p", { class: "small" }, "Jobs of this deployment may download from an approved host from then on."),
      el("label", {}, all, " Select all"),
      ...rows,
      el(
        "div",
        { class: "actions" },
        approveButton,
        el(
          "button",
          { type: "submit", class: "btn", value: "start", title: BUTTON_HELP.startUnapproved },
          "Start without approving",
        ),
        el("button", { type: "submit", class: "btn", value: "cancel", title: BUTTON_HELP.cancelHosts }, "Cancel"),
      ),
    ),
  );
  refresh();
  await new Promise(settle => {
    box.addEventListener("close", () => {
      if (box.returnValue === "approve") send(missing.filter((_host, n) => ticks[n].checked).map(m => m.host));
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
export const hostsRun = (subject, action) =>
  // a reject of the saved set starts its rework's jobs, as a go starts a round's
  (action === "go" || action === "resume" || (action === "reject" && subject.stage === "done")) &&
  subject.kind === "dataset"
    ? { records: [subject.key] }
    : (action === "go" || action === "resume") && subject.kind === "prep"
      ? { prep: subject.number }
      : action === "approve" && subject.kind === "buffer" && subject.stage === "plan-review"
        ? { spec: subject.key }
        : null;
// what a verdict carries beside its action: the revision this page is showing (the server refuses the verdict if
// the subject has moved on) and, for a dataset, the set of crops it is about: the one under review or, at done,
// the saved one (a reject)
export const verdictOf = (subject, action) => {
  if (action !== "approve" && action !== "reject") return {};
  const sets = subject.kind === "dataset" ? subject.data?.sets : null;
  if (!sets) return { revision: subject.revision };
  const set = subject.stage === "review" ? sets.review : sets.saved?.set;
  return { revision: subject.revision, ...(set ? { set } : {}) };
};

export function actionBar(subject, after, skip) {
  // `skip`: actions that have their own control on the page (a spec's go, revoke)
  const bar = el("div", { class: "actions" });
  const offered = ACTION_ORDER.filter(a => subject.actions.includes(a) && !(skip || []).includes(a));
  // an action was sent and its workflow has not answered: the stage, and so these buttons, are still the old
  // ones, so none can be pressed until the answer (or the end of the wait) reloads this
  const asked = subject.asked;
  if (asked) {
    clearTimeout(askedTimer);
    askedTimer = setTimeout(onScreen.refresh, asked.wait_s * 1000 + 300);
  }
  let checking = false; // a click while the host check or its dialog is open starts nothing
  for (const action of offered) {
    const word = actionWord(subject, action);
    const dialog = NOTE_DIALOGS[action]; // its word already ends in the dots of "more to enter"
    const sending = dialog ? dialog.sending : word + "…";
    bar.append(
      el(
        "button",
        {
          type: "button",
          // Resume is the one to press where it is offered: Restart beside it throws the round's work away
          class: "btn" + (ACTION_CONFIRM[action] ? " danger" : action === "resume" ? " primary" : ""),
          title: actionHelp(subject, action),
          disabled: !!subject.asked,
          onclick: e => {
            const send = more => {
              bar.querySelectorAll("button").forEach(b => {
                b.disabled = true;
              });
              e.target.textContent = sending;
              sendAction(subject, { action, key: newKey(), ...verdictOf(subject, action), ...more }, after);
            };
            // an action that starts a run asks first which hosts the run lacks; what was typed rides along
            const start = more => {
              const run = hostsRun(subject, action);
              if (!run) return send(more);
              if (checking) return;
              checking = true;
              return askHosts(run, hosts =>
                send({ ...more, ...(hosts.length ? { approve_hosts: hosts } : {}) }),
              ).finally(() => {
                checking = false;
              });
            };
            if (dialog) return askNote(subject, dialog, note => start({ note }));
            if (ACTION_CONFIRM[action] && !confirm(actionConfirm(subject, action))) return;
            start({});
          },
        },
        asked && asked.action === action ? sending : word,
      ),
    );
  }
  if (asked) bar.append(el("span", { class: "small" }, "Sent: waiting for its workflow to answer."));
  if (!subject.actions.length) bar.append(el("span", { class: "small" }, "Nothing can be asked of it at this stage."));
  return bar;
}
export function undelivered(subject, actions, after) {
  // an action that did not reach its workflow, with a Retry that sends the same one
  const box = el("div"),
    now = Date.now() / 1000;
  for (const a of actions) {
    const lost = a.state === "failed" || (a.state === "pending" && now - (toEpoch(a.at) || now) > UNDELIVERED_AFTER_S);
    if (!lost) continue;
    const line = el(
      "div",
      { class: "notice" },
      el("b", {}, a.action),
      ` was not delivered${a.detail ? ": " + a.detail : ""}. `,
    );
    const again = {
      action: a.action,
      key: a.key,
      ...(a.payload?.note ? { note: a.payload.note } : {}),
      ...(a.payload?.spec ? { spec: a.payload.spec, plan_first: !!a.payload.plan_first } : {}),
      ...(a.payload?.records ? { records: a.payload.records } : {}),
      ...(a.payload?.record ? { record: a.payload.record } : {}),
    };
    if (a.key)
      line.append(
        el(
          "button",
          {
            type: "button",
            class: "btn",
            title: BUTTON_HELP.retry,
            onclick: e => {
              e.target.disabled = true;
              sendAction(subject, again, after);
            },
          },
          "Retry",
        ),
      );
    box.append(line);
  }
  return box;
}

// ---- the Prepare button: a preparation request is posted to the service, which runs it
const PREP_MAX = 200; // the most datasets one preparation request takes (mia_buffers.spec.MAX_PREP_RECORDS)
// One preparation request for the datasets of `all`. It resumes each (one that never ran starts, one that stopped
// runs what is left, a prepared one is left alone); with `restart` it says so, and every one starts over from
// scratch, also one that is prepared or at review.
// `owner` (a buffer's Prepare step): the number of the buffer the request is filed for; the person stays on that step.
export function prepareButton(container, all, blocker, done, text, owner, restart) {
  // `text`: the button's word, where the count stands beside it
  // a request takes at most PREP_MAX datasets: a longer list is prepared in parts, the first PREP_MAX now
  const todo = all.slice(0, PREP_MAX),
    why = blocker;
  let checking = false; // a click while the host check or its dialog is open starts nothing
  const label =
    all.length > PREP_MAX
      ? `${text || "Prepare"} the first ${PREP_MAX} of ${num(all.length)} datasets`
      : text || (todo.length === 1 ? "Prepare this dataset" : `Prepare these ${num(todo.length)} datasets`);
  if (why) {
    container.replaceChildren(
      el("button", { type: "button", class: "btn", disabled: true, title: why }, label),
      el("span", { class: "small" }, why),
    );
    return;
  }
  container.replaceChildren(
    el(
      "button",
      {
        type: "button",
        class: "btn",
        ...(restart ? { title: BUTTON_HELP.prepareRestart } : { title: BUTTON_HELP.prepare }),
        onclick: async e => {
          if (checking) return;
          checking = true;
          askHosts({ records: todo }, hosts => start(e.target, hosts)).finally(() => {
            checking = false;
          });
        },
      },
      label,
    ),
  );
  async function start(button, hosts) {
    button.disabled = true;
    // more than 25 records need the count written into the request: the button's label names it
    const yaml =
      toPrepYaml(todo, S.ref, S.meta.catalog_repo) +
      (restart ? "restart: true\n" : "") +
      (todo.length > 25 ? `max_records: ${todo.length}\n` : "");
    try {
      const action = await api("/prep", {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify({
          spec: yaml,
          key: newKey(),
          ...(hosts.length ? { approve_hosts: hosts } : {}),
          ...(owner == null ? {} : { buffer: owner }),
        }),
      });
      if (done) done(todo);
      if (owner == null) location.hash = "#find/" + action.number;
    } catch (err) {
      banner(
        err.status === 422
          ? `The request was refused: ${err.message}`
          : `Not started (${err.status || "network error"}): ${err.message}`,
      );
      button.disabled = false;
    }
  }
}
