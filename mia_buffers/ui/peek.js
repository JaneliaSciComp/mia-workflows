// A peek at the version being served: one crop as the tensorizer returns it, every level side by side. The
// service draws it apart from its consumers, so nothing here registers one or counts a draw.
import { datasetLink, el } from "./dom.js";
import { api } from "./api.js";

const SPLITS = ["train", "val", "test"];

// a length given in nm, in the largest unit that keeps the number at 1 or more, three digits at most
export const length = nm => {
  const [size, unit] = nm >= 1e6 ? [nm / 1e6, "mm"] : nm >= 1e3 ? [nm / 1e3, "µm"] : [nm, "nm"];
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

// the split to draw from, Next crop, and the crop it brought. One panel per buffer, kept while the page redraws
// the step around it, so a crop stays on screen until the next one is asked for.
let kept = { buffer: null, node: null };
export function peekPanel(buffer) {
  if (kept.buffer === buffer) return kept.node;
  const shown = el("div", {}),
    split = el(
      "select",
      { "aria-label": "split", title: "The split the crop is drawn from." },
      ...SPLITS.map(name => el("option", { value: name }, name)),
    ),
    button = el(
      "button",
      {
        type: "button",
        class: "btn",
        title: "Draw a crop as a training run would and show what the tensorizer returns for it.",
        onclick: async () => {
          button.disabled = true;
          button.replaceChildren("Reading…");
          const wanted = SPLITS.includes(split.value) ? split.value : SPLITS[0];
          try {
            const peek = await api(`/buffers/${encodeURIComponent(buffer)}/peek?split=${wanted}`);
            shown.replaceChildren(
              peek ? peekView(peek) : el("div", { class: "empty" }, `Nothing is servable in ${wanted} yet.`),
            );
          } catch (e) {
            shown.replaceChildren(el("div", { class: "notice" }, `No crop: ${e.message}`));
          }
          button.replaceChildren("Next crop");
          button.disabled = false;
        },
      },
      "Next crop",
    );
  const node = el(
    "div",
    { class: "peek" },
    el(
      "div",
      { class: "peekbar" },
      el("label", {}, "split ", split),
      button,
      el("span", { class: "sub" }, "A peek: no consumer is registered and nothing is counted as drawn."),
    ),
    shown,
  );
  kept = { buffer, node };
  return node;
}
