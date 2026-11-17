// A buffer's six steps: which step a stage is, which step a buffer may show, and the words the list says. No DOM.
export const STEPS = ["define", "datasets", "prepare", "plan", "source", "serve"];
export const STEP_TITLES = {
  define: "Define",
  datasets: "Datasets",
  prepare: "Prepare",
  plan: "Plan",
  source: "Source",
  serve: "Serve",
};
// a stage a round works or waits at is the step of that work (mia_buffers.config.STAGE_ACTIONS)
const STEP_OF = {
  planning: "plan",
  "plan-review": "plan",
  sourcing: "source",
  staged: "source",
  ready: "serve",
  idle: "serve",
};
// The step a saved buffer is on. Every stage it rests at is Prepare: that is where it waits for the person.
export const stepOf = stage => STEP_OF[stage] || "prepare";
// The furthest step a buffer may show: its stage's, or Datasets while it is not saved (it has no stage yet).
const reach = stage => (stage == null ? "datasets" : stepOf(stage));
// The step to show when the address asks for one: a step already passed, never one ahead. Asked for none, or for
// one ahead, a saved buffer shows the step of its stage and an unsaved one its first.
export function stepToShow(stage, asked) {
  const home = stage == null ? "define" : stepOf(stage);
  return STEPS.includes(asked) && STEPS.indexOf(asked) <= STEPS.indexOf(reach(stage)) ? asked : home;
}
// The strip: each step as the one shown (`here`), one that can be opened (`done`) or one not reached (`ahead`).
export const stripOf = (stage, shown) =>
  STEPS.map((step, index) => ({
    step,
    state: step === shown ? "here" : index <= STEPS.indexOf(reach(stage)) ? "done" : "ahead",
  }));
// Whether the definition can change: an unsaved buffer (no subject), or a stage that takes `edit`.
export const editable = subject => !subject || subject.actions.includes("edit");
const STATUS_WORDS = {
  waiting: "waits for you",
  working: "working",
  stuck: "needs you",
  "not started": "saved, not started",
};
// What the Buffers list says a buffer is doing.
export const statusWord = subject =>
  subject.shelved
    ? "shelved"
    : stepOf(subject.stage) === "serve"
      ? "serving"
      : STATUS_WORDS[subject.status] || subject.stage;
