// A peek at the version being served: one crop as the tensorizer returns it, every level side by side. The
// service draws it apart from its consumers, so nothing here registers one or counts a draw.
import { datasetLink, el } from "./dom.js";
import { api } from "./api.js";

const SPLITS = ["train", "val", "test"];

// a length given in nm, in the largest unit that keeps the number at 1 or more, three digits at most
export const length = nm => {
  const far = Math.abs(nm),
    [size, unit] = far >= 1e6 ? [nm / 1e6, "mm"] : far >= 1e3 ? [nm / 1e3, "µm"] : [nm, "nm"];
  return `${Number(size.toPrecision(3))} ${unit}`;
};

const picture = (data, alt) => el("img", { src: "data:image/png;base64," + data, alt });

// what a peek answered: a row of the image's levels, a row per label layer, then what the crop is of
export function peekView(peek) {
  const { descriptor, dataset, levels, labels } = peek,
    plane = Object.entries(descriptor.plane || {}).map(([axis, index]) => `plane ${axis} ${index}`),
    centre = (descriptor.centre_nm || []).map(length).join(", ");
  return el(
    "div",
    { class: "peekrows" },
    el(
      "div",
      { class: "levels" },
      ...levels.map(level =>
        el(
          "figure",
          {},
          el("figcaption", {}, `×${level.factor}`),
          picture(level.img, `the crop at ×${level.factor}`),
          el(
            "figcaption",
            { class: "sub" },
            level.pixel_nm
              ? `${length(level.pixel_nm)}/px · ${length(level.pixel_nm * descriptor.patch)} across`
              : `${descriptor.patch} px across`,
          ),
        ),
      ),
    ),
    ...labels.flatMap(layer => [
      el("div", { class: "layer" }, "labels ", el("code", {}, layer.key)),
      el(
        "div",
        { class: "levels" },
        ...layer.levels.map((data, index) => picture(data, `${layer.key} at ×${levels[index]?.factor}`)),
      ),
    ]),
    el(
      "p",
      { class: "facts" },
      "dataset ",
      datasetLink(dataset.id),
      [dataset.title, dataset.modality, dataset.dims, ...(dataset.organisms || []), ...(dataset.annotation_types || [])]
        .filter(Boolean)
        .map(fact => ` · ${fact}`)
        .join(""),
    ),
    el(
      "p",
      { class: "facts" },
      [
        `volume ${String(descriptor.volume).split("/").pop()}`,
        peek.samples != null && descriptor.candidate != null
          ? `sample ${descriptor.candidate + 1} of ${peek.samples}`
          : null,
        ...plane,
        centre ? `centre ${centre} (y, x)` : null,
        `${peek.split} split`,
      ]
        .filter(Boolean)
        .join(" · "),
    ),
  );
}

const KEPT = 50; // the crops a panel keeps to go back to

// the split to draw from, Previous crop, Next crop, and the crop on screen. One panel per buffer, kept while the
// page redraws the step around it, so a crop stays on screen until another is asked for.
let kept = { buffer: null, node: null };
export function peekPanel(buffer) {
  if (kept.buffer === buffer) return kept.node;
  // a peek costs the service a process of about a second, so the crop after the one on screen is asked for as soon
  // as that one is drawn: the next press shows it at once. `ahead` is that answer and the split it is of.
  let ahead = null;
  // the service keeps no trace of a peek, so going back is the page's: `seen` is the crops this panel showed,
  // oldest first, and `at` the place of what is on screen (a message that is no crop stands after the last one)
  const seen = [];
  let at = 0;
  const ask = wanted => api(`/buffers/${encodeURIComponent(buffer)}/peek?split=${wanted}`);
  const shown = el("div", {}),
    split = el(
      "select",
      { "aria-label": "split", title: "The split the crop is drawn from." },
      ...SPLITS.map(name => el("option", { value: name }, name)),
    ),
    // what is on screen: the crop at `at`, or `message` after the last one
    show = message => {
      if (message) at = seen.length;
      shown.replaceChildren(message || peekView(seen[at]));
      back.disabled = at <= 0;
    },
    back = el(
      "button",
      {
        type: "button",
        class: "btn",
        title: `Show the crop that was on screen before this one. The page keeps the last ${KEPT} it showed; the service keeps none.`,
        onclick: () => {
          at -= 1;
          show();
        },
      },
      "Previous crop",
    ),
    button = el(
      "button",
      {
        type: "button",
        class: "btn",
        title:
          "Draw a crop as a training run would and show what the tensorizer returns for it. After Previous crop, it steps forward through the crops already shown first.",
        onclick: async () => {
          const wanted = SPLITS.includes(split.value) ? split.value : SPLITS[0];
          if (seen[at + 1]?.split === wanted) {
            at += 1;
            return show();
          }
          button.disabled = back.disabled = true;
          button.replaceChildren("Reading…");
          const answer = ahead?.split === wanted ? ahead.answer : ask(wanted);
          ahead = null;
          try {
            const peek = await answer;
            if (peek) {
              seen.push(peek);
              if (seen.length > KEPT) seen.shift();
              at = seen.length - 1;
              show();
              const next = ask(wanted);
              ahead = { split: wanted, answer: next };
              // one that failed is dropped, so the press asks again
              next.catch(() => {
                if (ahead?.answer === next) ahead = null;
              });
            } else show(el("div", { class: "empty" }, `Nothing is servable in ${wanted} yet.`));
          } catch (e) {
            show(el("div", { class: "notice" }, `No crop: ${e.message}`));
          }
          button.replaceChildren("Next crop");
          button.disabled = false;
        },
      },
      "Next crop",
    );
  back.disabled = true; // nothing shown yet
  const node = el(
    "div",
    { class: "peek" },
    el(
      "div",
      { class: "peekbar" },
      el("span", {}, "Simulated buffer drawing:"),
      el("label", {}, "split ", split),
      back,
      button,
    ),
    shown,
  );
  kept = { buffer, node };
  return node;
}
