// A buffer's page: its name and state, the strip of its six steps, the step being shown, and its timeline.
import { editable, STEP_TITLES, STEPS, stepOf, stepToShow, stripOf } from "./steps.js";
import {
  $,
  bufferLink,
  card,
  datasetLink,
  el,
  fmtBytes,
  ghIcon,
  num,
  pill,
  stagePill,
  statePill,
  SUBJECT_HASH,
  when,
} from "./dom.js";
import { api, banner, JSON_HEADERS, load, newKey, S } from "./api.js";
import { pagedTable, pageOf } from "./table.js";
import { cropsOf, gist, markdown, mdInline } from "./markdown.js";
import { actionBar, askHosts, BUTTON_HELP, onScreen, prepareButton, sendAction, undelivered } from "./actions.js";
import { poolTable, servableText, stageOf } from "./buffers.js";
import {
  baseRevision,
  changed,
  definition,
  ensureDefinition,
  forgetNew,
  keepAsNew,
  planSummary,
  problem,
  setLocked,
  setOnDesign,
  showDesigner,
  startOver,
  unsavedFor,
} from "./designer.js";
import { shelvedNotice, timelineCard } from "./subjects.js";

let turn = 0; // the newest load of the page: one that started earlier never paints over it

// The address of a step: of a saved buffer under its key, of one that is not saved yet under New buffer.
const stepHash = (subject, step) => (subject ? `${SUBJECT_HASH.buffer(subject.key)}/${step}` : `#buffers/new/${step}`);
const goTo = (subject, step) => {
  location.hash = stepHash(subject, step);
};
// Why a saved buffer's definition cannot change now, or null when it can (one that is not saved always can).
function lockReason(subject) {
  if (editable(subject)) return null;
  if (subject.shelved) return "This buffer is shelved: unshelve it, under Actions, to change it.";
  if (stepOf(subject.stage) === "serve") return "This buffer is serving, so its definition is fixed.";
  return "A round is in progress: the definition cannot change until the round ends or is cancelled.";
}
// Going on from steps 1 and 2: a buffer that is not saved is saved, a changed one is edited, and then `step`
// opens. A refusal stays on the step, with what was entered.
async function leave(subject, step, button) {
  if (button) button.disabled = true;
  let saved = subject;
  try {
    if (!subject) {
      const out = await api("/specs", {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify({ spec: definition() }),
      });
      forgetNew();
      saved = out.subject;
    } else if (changed() && !lockReason(subject)) {
      // the revision the change was made at: an edit over someone else's newer one is refused
      const edit = { action: "edit", spec: definition(), revision: baseRevision(), key: newKey() };
      await api(`/specs/${encodeURIComponent(subject.key)}/actions`, {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify(edit),
      });
    }
  } catch (e) {
    banner(
      e.status === 422
        ? `The definition was refused: ${e.message}`
        : e.status === 409
          ? `Not saved. ${e.message}`
          : `Not saved (${e.status || "network error"}): ${e.message}`,
    );
    if (button) button.disabled = false;
    return;
  }
  goTo(saved, step);
}

// The menu under Actions…: Clone, which asks nothing of the service, then the actions the buffer accepts now.
function menu(subject, redraw, skip) {
  const bar = actionBar(subject, redraw, skip);
  bar.prepend(
    el(
      "button",
      {
        type: "button",
        class: "btn",
        title: BUTTON_HELP.clone,
        onclick: async () => {
          await ensureDefinition(subject);
          keepAsNew();
          location.hash = "#buffers/new";
        },
      },
      "Clone",
    ),
  );
  return bar;
}

// ---- the heading: the name, where it stands, its links, and what can be asked of it that is not a step
function head(subject, redraw, skip) {
  const name = subject ? subject.name || subject.key : "New buffer";
  const parts = [
    el(
      "div",
      { class: "viewhead" },
      el("h2", {}, name, " ", subject ? stagePill(subject.stage) : pill("not saved", "")),
      subject
        ? el("details", { class: "more" }, el("summary", { class: "btn" }, "Actions…"), menu(subject, redraw, skip))
        : null,
    ),
    subject
      ? el(
          "div",
          { class: "links" },
          // a name is a label and may repeat: the key is what tells two buffers apart
          subject.name && subject.name !== subject.key ? el("span", { class: "mono" }, subject.key) : null,
          subject.issue ? ghIcon(S.meta.buffers_repo, subject.issue, true) || `#${subject.issue}` : null,
        )
      : null,
  ];
  $("bufferHead").replaceChildren(...parts.filter(Boolean)); // a null would be printed as "null"
}

// ---- the strip: the step shown, the steps that can be opened, the steps not reached
function strip(subject, step) {
  $("bufferSteps").replaceChildren(
    ...stripOf(subject?.stage, step).map(item =>
      el(
        "li",
        { class: item.state, "aria-current": item.state === "here" ? "step" : null },
        item.state === "done"
          ? el(
              "a",
              {
                href: stepHash(subject, item.step),
                // leaving steps 1 and 2 for a later one saves a changed definition first
                onclick: e => {
                  if (STEPS.indexOf(item.step) < 2 || !subject || !changed() || lockReason(subject)) return;
                  e.preventDefault();
                  leave(subject, item.step);
                },
              },
              STEP_TITLES[item.step],
            )
          : STEP_TITLES[item.step],
      ),
    ),
  );
}

// ---- what a person should know before the step: the newest entry, a round that is not running, a hold
function notes(subject, entries, run, actions, redraw) {
  const parts = [];
  const said = gist(entries.at(-1)?.body);
  if (said.text) parts.push(el("div", { class: "latest" }, when(entries.at(-1).at), " · ", ...mdInline(said.text)));
  // "working" must be true: a round the registry calls in progress whose workflow is not running is said so
  if (subject.status === "working" && run && run.status !== "RUNNING")
    parts.push(
      el(
        "div",
        { class: "notice" },
        `The registry says a round is in progress, but its workflow is ${(run.status || "not there").toLowerCase()}. Cancel the round (under Actions) to settle it.`,
      ),
    );
  parts.push(undelivered(subject, actions, redraw));
  if (subject.shelved) parts.push(shelvedNotice(subject.shelved));
  $("bufferNotes").replaceChildren(...parts);
}

// ---- the newest version of a buffer (its newest round's buffer) and that version's datasets
async function newestVersion(subject) {
  const version = ((await load(`/buffers?limit=1&spec=${subject.number}`))?.buffers || [])[0] || null;
  const board = version
    ? (await load(`/buffers/${encodeURIComponent(version.id)}/datasets`, true))?.datasets || []
    : [];
  return { version, board };
}
// a dataset whose files a person copies in by hand: the round waits until it is told they are in place
const droppedNotices = (subject, board, redraw) =>
  !subject.actions.includes("dropped")
    ? []
    : board
        .filter(d => d.status === "waiting-drop")
        .map(d =>
          el(
            "div",
            { class: "notice" },
            el("b", {}, d.record),
            " waits for its files to be copied into its incoming/ folder. ",
            el(
              "button",
              {
                type: "button",
                class: "btn",
                title: BUTTON_HELP.dropped,
                disabled: !!subject.asked,
                onclick: e => {
                  e.target.disabled = true;
                  sendAction(subject, { action: "dropped", record: d.record, key: newKey() }, redraw);
                },
              },
              "Files are in place",
            ),
          ),
        );
function revokeCard(subject, board, redraw) {
  const records = board.filter(d => d.status === "servable").map(d => d.record);
  const ticks = el(
    "div",
    { class: "ticks" },
    ...records.map(r => el("label", { class: "inline" }, el("input", { type: "checkbox", value: r }), " " + r)),
  );
  return card(
    "Revoke datasets",
    "The checked datasets leave this version: their volumes are unregistered and their staged pyramids quarantined.",
    records.length ? ticks : el("div", { class: "empty" }, "No dataset is being served."),
    el(
      "div",
      { class: "actions" },
      el(
        "button",
        {
          type: "button",
          class: "btn danger",
          title: BUTTON_HELP.revokeTicked,
          disabled: !!subject.asked,
          onclick: e => {
            const chosen = [...ticks.querySelectorAll("input:checked")].map(i => i.value);
            if (!chosen.length) return banner("Check the datasets to revoke first.");
            if (
              !confirm(
                "Revoke the checked datasets? Their volumes leave this version and their staged pyramids are quarantined; nothing is deleted.",
              )
            )
              return;
            e.target.disabled = true;
            sendAction(subject, { action: "revoke", records: chosen, key: newKey() }, redraw);
          },
        },
        "Revoke the checked",
      ),
    ),
  );
}
// every version of the buffer, the newest first: each round that reached its plan made one, under its own id
const versionsCard = subject =>
  card(
    "Versions",
    "Newest first. A training run names a version by its id; open one for its events, datasets, volumes, consumers and usage.",
    pagedTable("buffer-versions:" + subject.key, {
      source: pageOf("/buffers", "buffers", () => ({ spec: subject.number })),
      search: false,
      empty: "No version yet.",
      cols: [
        { key: "buffer", label: "id", cell: b => bufferLink(b.id) },
        {
          key: "updated",
          label: "state",
          cell: b =>
            b.latest_event
              ? el("span", {}, statePill(stageOf(b.latest_event)), " ", when(b.latest_event.at))
              : "no events",
        },
        { key: "volumes", label: "volumes", num: true, cell: b => num(b.volumes) },
        { label: "servable", cell: b => servableText(b.servable) },
      ],
    }),
  );

// ---- the steps. Each draws its pane; `turnOf()` tells a step that loaded something whether it is still wanted.
// the footer of a step: on the left the way back, on the right a line on where things stand and the way forward
function foot(back, hint, forward) {
  $("bufferFoot").replaceChildren(
    back || el("span"),
    el("div", { class: "right" }, hint ? el("span", { class: "small" }, hint) : null, forward),
  );
}
const savedHint = subject =>
  lockReason(subject) ||
  problem() ||
  (!subject ? "Not saved yet." : changed() ? "Changed: it is saved when you go on." : "Saved.");
function defineFoot(subject) {
  const fitting = planSummary()?.fitting;
  foot(
    subject
      ? null
      : el(
          "button",
          { type: "button", class: "btn", title: BUTTON_HELP.startOver, onclick: () => startOver() },
          "Start over",
        ),
    savedHint(subject),
    el(
      "button",
      { type: "button", class: "btn primary", disabled: !!problem(), onclick: () => goTo(subject, "datasets") },
      fitting == null ? "Next: the datasets" : `Next: the ${num(fitting)} ${fitting === 1 ? "dataset" : "datasets"}`,
    ),
  );
}
function datasetsFoot(subject) {
  const saves = !subject || (changed() && !lockReason(subject));
  foot(
    el(
      "button",
      { type: "button", class: "btn", onclick: () => goTo(subject, "define") },
      lockReason(subject) ? "← The definition" : "← Change the definition",
    ),
    subject ? savedHint(subject) : problem() || "Saving creates the buffer; nothing runs yet.",
    el(
      "button",
      {
        type: "button",
        class: "btn primary",
        ...(!subject ? { title: BUTTON_HELP.save } : saves ? { title: BUTTON_HELP.saveChanges } : {}),
        disabled: !!problem(),
        onclick: e => leave(subject, "prepare", e.target),
      },
      !subject ? "Save buffer and continue" : saves ? "Save changes and continue" : "Next: prepare",
    ),
  );
}
// Steps 1 and 2 are two views of one thing, the definition and its plan: both make sure the controls hold this
// buffer's definition, say whether it can change, and draw the footer again whenever it or its plan changes.
const designed = footer => async (_pane, subject, _redraw, wanted) => {
  await ensureDefinition(subject);
  if (!wanted()) return;
  setLocked(!!lockReason(subject));
  setOnDesign(() => {
    if (wanted()) footer(subject);
  });
  showDesigner();
  footer(subject);
};
// The go that plans a buffer. It always stops at its plan: the Run there starts the jobs.
function planRound(subject, redraw) {
  const go = { action: "go", plan_first: true, revision: subject.revision, key: newKey() };
  sendAction(subject, go, redraw);
}
// On a step the buffer has passed: the way on is the step of its stage.
const nextLink = subject =>
  el(
    "a",
    { class: "btn primary", href: SUBJECT_HASH.buffer(subject.key) },
    `Next: ${STEP_TITLES[stepOf(subject.stage)].toLowerCase()}`,
  );
const backLink = (subject, step) => el("a", { class: "btn", href: stepHash(subject, step) }, `← ${STEP_TITLES[step]}`);
// The way back from a round in progress: it asks, then cancels the round, and the buffer rests where it can change.
const cancelBack = (subject, redraw) =>
  el(
    "button",
    {
      type: "button",
      class: "btn",
      title: BUTTON_HELP.back,
      disabled: !subject.actions.includes("cancel") || !!subject.asked,
      onclick: e => {
        if (
          !confirm(
            "Go back to change the buffer? The round in progress is cancelled: its cluster jobs are killed and the work so far is not kept.",
          )
        )
          return;
        e.target.disabled = true;
        sendAction(subject, { action: "cancel", key: newKey() }, redraw);
      },
    },
    "← Change the buffer (cancels this round)",
  );
const approval = subject => ({ action: "approve", revision: subject.revision, key: newKey() });

// ---- step 3: the fitting datasets that are not prepared, the requests this buffer filed, and the way on.
// The buffer waits here for as long as the person likes; the way on takes what is prepared at that moment.
const ticked = { key: null, chosen: new Set() }; // the checks of the buffer shown: they outlast the page's redraws
// Why the planner would refuse this plan (`plan`: what POST /plan answered), or null when a round of it would run.
function planRefusal(plan) {
  const errors = plan.errors || [],
    waiting = plan.summary?.not_prepared || 0;
  if (!errors.length) return null;
  if (errors.includes("no dataset admitted") && waiting)
    return `no prepared dataset fits yet; ${num(waiting)} fit but are not prepared`;
  return "the plan has errors: " + errors.join("; ");
}
// `plan`: the plan of the definition as the step last read it; null while it is being read.
function prepareFoot(subject, plan, redraw) {
  const prepared = plan?.summary?.prepared || 0,
    resting = stepOf(subject.stage) === "prepare";
  const blocker = !plan
    ? "planning…"
    : !subject.actions.includes("go")
      ? lockReason(subject) || "Nothing can be planned at this stage."
      : planRefusal(plan);
  foot(
    el("button", { type: "button", class: "btn", onclick: () => goTo(subject, "datasets") }, "← Datasets"),
    resting ? blocker || "You can come back and prepare more later." : null,
    resting
      ? el(
          "button",
          {
            type: "button",
            class: "btn primary",
            title: BUTTON_HELP.plan,
            disabled: !!blocker || !!subject.asked,
            onclick: e => {
              e.target.disabled = true;
              planRound(subject, redraw);
            },
          },
          subject.asked?.action === "go"
            ? "Planning…"
            : `Continue with ${num(prepared)} prepared ${prepared === 1 ? "dataset" : "datasets"}`,
        )
      : nextLink(subject),
  );
}
function prepareStep(pane, subject, redraw, wanted) {
  if (ticked.key !== subject.key) Object.assign(ticked, { key: subject.key, chosen: new Set() });
  const chosen = ticked.chosen;
  let startable = new Set(); // what a request could start: it fits, is not prepared, is in no round, its record can run
  const counts = el("p", {}, "planning…"),
    bar = el("i"),
    prepare = el("span", { class: "actions" });
  const offer = () => {
    prepareButton(
      prepare,
      [...chosen],
      chosen.size ? null : "check the datasets to prepare",
      () => {
        chosen.clear();
        redraw();
      },
      undefined,
      chosen.size > 1 ? `Prepare the ${num(chosen.size)} checked datasets` : "Prepare the checked dataset",
      subject.number,
    );
  };
  const left = async state => {
    // the plan of the stored definition, which is what a round runs, narrowed to what is left to prepare; its
    // summary is over the whole plan
    let out = null;
    const page = {
      limit: state.limit,
      offset: state.offset,
      dir: state.dir,
      q: state.q,
      ...(state.sort ? { sort: state.sort } : {}),
      to_prepare: true,
    };
    try {
      out = await api("/plan", {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify({ spec: subject.data.spec, page }),
      });
    } catch (e) {
      banner(`The plan of this definition failed (${e.status || "network error"}): ${e.message}`);
    }
    return { rows: out?.rows || [], total: out?.total || 0, out };
  };
  const seen = page => {
    const m = page.out?.summary;
    startable = new Set(m?.to_prepare || []);
    for (const id of chosen) if (!startable.has(id)) chosen.delete(id); // prepared, or started, since it was checked
    if (m) {
      counts.replaceChildren(
        el("b", {}, `${num(m.prepared || 0)} of ${num(m.fitting || 0)}`),
        ` datasets that fit are prepared; ${num(m.in_progress || 0)} are being prepared now.`,
      );
      bar.style.width = `${m.fitting ? (100 * (m.prepared || 0)) / m.fitting : 0}%`;
    }
    offer();
    if (wanted() && page.out) prepareFoot(subject, page.out, redraw);
  };
  const table = pagedTable("to-prepare:" + subject.key, {
    source: left,
    onPage: seen,
    sort: "state",
    search: "search id, title, modality, licence",
    empty: "Every dataset that fits is prepared.",
    cols: [
      {
        label: "prepare",
        title:
          "check a dataset to prepare it; one that is in a round already, or whose record cannot run, has no checkbox",
        cell: x => {
          if (!startable.has(x.id)) return "";
          const input = el("input", { type: "checkbox", "aria-label": "prepare " + x.id });
          input.checked = chosen.has(x.id);
          input.addEventListener("change", () => {
            if (input.checked) chosen.add(x.id);
            else chosen.delete(x.id);
            offer();
          });
          return input;
        },
      },
      { key: "dataset", label: "dataset", cell: x => datasetLink(x.id) },
      { label: "issue", cell: x => ghIcon(S.meta.datasets_repo, x.issue) },
      { key: "modality", label: "modality", cell: x => x.modality_group || "" },
      { key: "dims", label: "dims", cell: x => x.dims || "" },
      { key: "state", label: "state", cell: x => statePill(x.prepared) },
      { label: "where it is", cell: x => x.stage || "" },
      { key: "bytes", label: "size", num: true, cell: x => (x.bytes ? fmtBytes(x.bytes) : "") },
    ],
  });
  pane.replaceChildren(
    unsavedFor(subject.key)
      ? el(
          "div",
          { class: "notice" },
          "Steps 1 and 2 hold a change of this buffer that is not saved. What is shown here, and what Continue plans, is the saved definition. ",
          el("a", { href: stepHash(subject, "define") }, "Go to the change"),
        )
      : "",
    counts,
    el("div", { class: "progress" }, bar),
    card(
      "Left to prepare",
      "The datasets that fit this definition and are not prepared. Preparing them is one request, filed for this buffer; the buffer waits here meanwhile.",
      el(
        "div",
        { class: "actions" },
        el(
          "button",
          {
            type: "button",
            class: "btn",
            title: "Check every dataset that a request could start now.",
            onclick: () => {
              for (const id of startable) chosen.add(id);
              table.reload();
            },
          },
          "Check all",
        ),
        prepare,
      ),
      table,
    ),
    card(
      "Requests of this buffer",
      "The preparation requests filed from this step, newest first. Open one for its datasets.",
      pagedTable("buffer-requests:" + subject.key, {
        source: pageOf("/subjects/prep", "subjects", () => ({ buffer: subject.number })),
        sort: "updated",
        dir: "desc",
        search: false,
        empty: "This buffer has asked for no preparation yet.",
        cols: [
          { key: "key", label: "request", cell: x => el("a", { href: SUBJECT_HASH.prep(x.key) }, x.key) },
          { key: "status", label: "stage", cell: x => stagePill(x.stage) },
          { key: "updated", label: "changed", cell: x => when(x.updated) },
          { label: "", wrap: true, cell: x => actionBar(x, redraw) },
        ],
      }),
    ),
  );
  offer();
  prepareFoot(subject, null, redraw);
}
// ---- step 4: the plan as the round wrote it, and the Run that starts the jobs
function planStep(pane, subject, redraw, _wanted, entries) {
  const here = stepOf(subject.stage) === "plan",
    planning = subject.stage === "planning";
  const plan = planning ? null : [...entries].reverse().find(entry => /^# Plan for /.test(entry.body));
  pane.replaceChildren(
    planning
      ? el(
          "div",
          { class: "notice" },
          "The definition is being resolved against the catalog and the prepared datasets. The plan appears here when it is ready.",
        )
      : plan
        ? card(
            "The plan",
            "As the round wrote it: the datasets in this version, their weights and crops.",
            el("div", { class: "md" }, ...markdown(plan.body, cropsOf(plan.body, {}))),
          )
        : el("div", { class: "empty" }, "This round's plan is not in the timeline."),
  );
  let checking = false; // a click while the host check or its dialog is open starts nothing
  foot(
    here ? cancelBack(subject, redraw) : backLink(subject, "prepare"),
    here ? (planning ? "Planning…" : "Nothing is fetched until you press Run.") : null,
    here
      ? el(
          "button",
          {
            type: "button",
            class: "btn primary",
            title: BUTTON_HELP.run,
            disabled: subject.stage !== "plan-review" || !!subject.asked,
            onclick: e => {
              if (checking) return;
              checking = true;
              askHosts({ spec: subject.key }, hosts => {
                e.target.disabled = true;
                const run = { ...approval(subject), ...(hosts.length ? { approve_hosts: hosts } : {}) };
                sendAction(subject, run, redraw);
              }).finally(() => {
                checking = false;
              });
            },
          },
          subject.asked?.action === "approve" ? "Starting…" : "Run: fetch and stage",
        )
      : nextLink(subject),
  );
}
// ---- step 5: the datasets of this version as they are fetched and staged, then the approve that serves it
async function sourceStep(pane, subject, redraw, wanted) {
  const { version, board } = await newestVersion(subject);
  if (!wanted()) return;
  const here = stepOf(subject.stage) === "source",
    onDisk = board.filter(d => d.status === "servable").length,
    cfg = version?.config || {},
    bar = el("i");
  bar.style.width = `${board.length ? (100 * onDisk) / board.length : 0}%`;
  pane.replaceChildren(
    el(
      "p",
      {},
      el("b", {}, `${num(onDisk)} of ${num(board.length)}`),
      " datasets of this version are on disk.",
      subject.stage === "staged" ? " No job is left to run: it waits for your approve." : "",
    ),
    el("div", { class: "progress" }, bar),
    ...droppedNotices(subject, board, redraw),
    version
      ? card(
          "The datasets of this version",
          "One row per dataset of the plan: on disk (servable), fetching with its job, staging, blocked by quota, waiting for its files, or failed with the reason.",
          poolTable(version.id, cfg.patch && cfg.levels?.length ? `p${cfg.patch}-l${cfg.levels.join("-")}` : null),
        )
      : el("div", { class: "empty" }, "Sourcing has not started."),
  );
  foot(
    here ? cancelBack(subject, redraw) : backLink(subject, "plan"),
    here ? (subject.stage === "staged" ? "The approve serves what is on disk now." : "Jobs are running.") : null,
    here
      ? el(
          "button",
          {
            type: "button",
            class: "btn primary",
            title: BUTTON_HELP.serve,
            disabled: subject.stage !== "staged" || !!subject.asked,
            onclick: e => {
              e.target.disabled = true;
              sendAction(subject, approval(subject), redraw);
            },
          },
          subject.asked?.action === "approve" ? "Approving…" : "Approve and serve",
        )
      : nextLink(subject),
  );
}
// ---- step 6: the version being served, by the id a training run names
async function serveStep(pane, subject, redraw, wanted) {
  const { version, board } = await newestVersion(subject);
  if (!wanted()) return;
  const url = S.meta.service_url || location.origin;
  const parts = [
    subject.stage === "idle"
      ? el(
          "div",
          { class: "notice" },
          "No consumer has drawn for a while. Keep, under Actions, returns it to ready; otherwise the keeper retires it.",
        )
      : null,
    version
      ? card(
          "The version being served",
          "A training run names this id. The buffer's name is a label: nothing looks a buffer up by it.",
          el(
            "p",
            {},
            "id ",
            el("code", {}, version.id),
            " · ",
            el(
              "a",
              { href: "#buffers/" + encodeURIComponent(version.id) },
              "its events, datasets, volumes, consumers and usage",
            ),
          ),
          el(
            "pre",
            { class: "snippet" },
            `from mia_buffers.client import BufferDataset\ntrain = BufferDataset("${url}", buffer="${version.id}", split="train")`,
          ),
        )
      : null,
    subject.actions.includes("revoke") ? revokeCard(subject, board, redraw) : null,
  ];
  pane.replaceChildren(...parts.filter(Boolean));
  foot(
    backLink(subject, "source"),
    "Serving.",
    subject.actions.includes("go")
      ? el(
          "button",
          {
            type: "button",
            class: "btn",
            title: BUTTON_HELP.planAgain,
            disabled: !!subject.asked,
            onclick: e => {
              if (!confirm("Plan a new version of this buffer over the datasets that are prepared now?")) return;
              e.target.disabled = true;
              planRound(subject, redraw);
            },
          },
          "Plan a new version",
        )
      : null,
  );
}
const STEP_VIEWS = {
  define: designed(defineFoot),
  datasets: designed(datasetsFoot),
  prepare: prepareStep,
  plan: planStep,
  source: sourceStep,
  serve: serveStep,
};

function draw(subject, step, entries, run, actions, redraw) {
  head(subject, redraw, ["go", "revoke", "approve"]); // each has its own button on a step
  strip(subject, step);
  if (subject) notes(subject, entries, run, actions, redraw);
  else $("bufferNotes").replaceChildren();
  for (const pane of document.querySelectorAll('[data-view="buffer"] [data-step]'))
    pane.hidden = pane.dataset.step !== step;
  $("bufferFoot").replaceChildren();
  const drawn = turn; // a step that loads something paints only while this draw is the newest
  STEP_VIEWS[step](
    document.querySelector(`[data-view="buffer"] [data-step="${step}"]`),
    subject,
    redraw,
    () => drawn === turn,
    entries,
  );
  // under every step: the buffer's versions (also when it rests after a round that failed) and its timeline
  $("bufferTimeline").replaceChildren(...(subject ? [versionsCard(subject), timelineCard(entries)] : []));
}

// `key`: the buffer's, or null for one that is not saved yet. `asked`: the step the address names, if any.
export async function viewFlow(key, asked, refresh) {
  const hash = location.hash,
    mine = ++turn,
    redraw = () => viewFlow(key, asked, true);
  onScreen.redraw = redraw;
  if (!key) {
    onScreen.subject = null;
    return draw(null, stepToShow(null, asked), [], null, [], redraw);
  }
  // what the stream refetches: this buffer's changes
  const known = onScreen.subject?.kind === "buffer" && onScreen.subject.key === key ? onScreen.subject.number : null;
  onScreen.subject = { kind: "buffer", key, number: known };
  if (!refresh) $("bufferNotes").replaceChildren(el("div", { class: "empty" }, "loading…"));
  const subject = await load(`/subjects/buffer/${encodeURIComponent(key)}`, true);
  if (location.hash !== hash || mine !== turn) return; // the reader moved on, or a later load started
  if (!subject) {
    $("bufferHead").replaceChildren(el("h2", {}, key));
    $("bufferSteps").replaceChildren();
    $("bufferNotes").replaceChildren(el("div", { class: "empty" }, "No such buffer."));
    return;
  }
  onScreen.subject.number = subject.number;
  const [timeline, run] = await Promise.all([
    load(`/timeline/buffer/${subject.number}`),
    api(`/run/buffer/${subject.number}`).catch(() => null),
  ]);
  if (location.hash !== hash || mine !== turn) return;
  draw(subject, stepToShow(subject.stage, asked), timeline?.entries || [], run, timeline?.actions || [], redraw);
}
