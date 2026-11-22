// The YAML the page writes: a buffer spec from the designer's controls, and a preparation request. No DOM.

// ---- what the designer's controls offer
export const DIMS = ["2D", "2D+t", "3D", "3D+t"];
export const LIC = [
  "CC0-1.0",
  "CC-BY-4.0",
  "CC-BY-3.0",
  "MIT",
  "BSD-3-Clause",
  "Apache-2.0",
  "CC-BY-SA-4.0",
  "CC-BY-NC-4.0",
  "CC-BY-NC-SA-4.0",
  "CC-BY-NC-ND-4.0",
  "GPL-3.0-or-later",
];
export const SRC = ["manual", "proofread", "automatic", "mixed", "unknown"];
const LEVELS = [1, 2, 4, 8, 16, 32];
export const levelsOf = coarsest => LEVELS.filter(l => l <= coarsest);
export const levelsKey = c => `p${c.patch}-l${levelsOf(c.level).join("-")}`; // mia_buffers.jobspec.levels_key

// A YAML scalar: plain when YAML reads it back as the same string, double-quoted (JSON is valid YAML) otherwise.
function yq(value) {
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  const s = String(value ?? "");
  const risky =
    s === "" ||
    /[:#,[\]{}&!|>'"%@`\u0000-\u001f\u007f]/.test(s) ||
    /^[\s*?-]/.test(s) ||
    /\s$/.test(s) ||
    /^(true|false|yes|no|on|off|y|n|null|~|<<|=)$/i.test(s) || // a word YAML gives a meaning of its own
    /^[-+]?(\d[\d_]*(\.\d*)?|\.\d+)([eE][-+]?\d+)?$/.test(s) ||
    /^0[xob]/i.test(s) ||
    /^[-+]?\.(inf|nan)$/i.test(s) ||
    /^\d{4}-\d\d-\d\d$/.test(s); // a date
  return risky ? JSON.stringify(s) : s;
}
const flow = xs => `[${xs.map(yq).join(", ")}]`;
export function toSpecYaml(c, ref, repo) {
  const p = c.patch,
    z = c.patchZ,
    sources = SRC.filter(s => c.sources.has(s)),
    // in name order, as the service keeps them; a percentage is a fraction of four places at most
    filters = [
      ...(c.filterContent ? ["{name: content}"] : []),
      ...(c.filterLabelled ? [`{name: labelled, min_fraction: ${+(c.labelledPercent / 100).toFixed(4)}}`] : []),
    ];
  return [
    "schema_version: 1",
    `name: ${yq(c.name.trim() || "buffer")}`,
    "",
    "catalog:",
    `  repo: ${yq(repo || "")}`,
    `  ref: ${yq(ref || "")}`,
    "",
    "select:",
    `  paths: ${flow(c.paths.split(/[\s,]+/).filter(Boolean))}`,
    "  where:",
    `    dims: {in: ${flow(DIMS.filter(d => c.dims.has(d)))}}`,
    "    license:",
    `      in: ${flow(LIC.filter(l => c.lic.has(l)))}`,
    `      allow_unknown: ${c.allowUnknown}`,
    "    annotations:",
    `      required: ${c.annRequired}`,
    `      source: ${sources.length === SRC.length ? "any" : `{in: ${flow(sources)}}`}`,
    `    min_size_voxels: ${c.minVox}`,
    // a dataset unchecked on the Datasets step
    ...(c.exclude?.size ? ["  exclude:", `    ids: ${flow([...c.exclude].sort())}`] : []),
    "  dedup:",
    `    by: ${flow(c.dedup ? ["doi"] : [])}`,
    "",
    "sampling:",
    "  patch:",
    `    2D: {y: ${p}, x: ${p}}`,
    ...(c.dims.has("2D+t") ? [`    2D+t: {y: ${p}, x: ${p}}`] : []),
    ...(c.dims.has("3D") ? [`    3D: {z: ${z}, y: ${p}, x: ${p}}`] : []),
    ...(c.dims.has("3D+t") ? [`    3D+t: {z: ${z}, y: ${p}, x: ${p}}`] : []),
    "    c: 1",
    `  levels: ${flow(levelsOf(c.level))}`,
    `  crops: {train: ${c.cropsTrain}, val: ${c.cropsVal}, test: ${c.cropsTest}}`,
    `  max_crops_per_dataset: ${c.maxCrops}`,
    ...(filters.length ? [`  filters: [${filters.join(", ")}]`] : []),
    "  weighting:",
    "    scheme: temperature",
    "    over: [modality, dataset]",
    `    temperature: ${c.T}`,
    "",
    "splits:",
    "  fractions: {train: 0.8, val: 0.1, test: 0.1}",
    `  holdout_ids: ${flow(c.holdout.split(/[\s,]+/).filter(Boolean))}`,
    "",
    "plan:",
    "  auto_approve: false",
    "",
  ].join("\n");
}
export function toPrepYaml(records, ref, repo) {
  return [
    "schema_version: 1",
    "kind: preparation",
    `catalog: {repo: ${yq(repo || "")}, ref: ${yq(ref || "")}}`,
    "records:",
    ...records.map(r => `  - ${yq(r)}`),
    "",
  ].join("\n");
}

// ---- a definition read back into the controls
// The controls' values for a spec as the service parsed it (POST /specs/parse).
export function fromSpec(s) {
  const where = s.select.where,
    patch = s.sampling.patch,
    crops = s.sampling.crops,
    filters = s.sampling.filters || [],
    labelled = filters.find(f => f.name === "labelled");
  return {
    name: s.name,
    paths: s.select.paths.join(" "),
    dims: new Set(where.dims.in || DIMS), // `any`, or a filter the chips cannot say: every one
    lic: new Set(where.license.in),
    allowUnknown: where.license.allow_unknown,
    annRequired: where.annotations.required,
    sources: new Set(where.annotations.source.in || SRC),
    minVox: where.min_size_voxels,
    patch: patch["2D"].y,
    patchZ: (patch["3D"] || patch["3D+t"])?.z || 32,
    level: Math.max(...s.sampling.levels),
    dedup: s.select.dedup.by.includes("doi"),
    T: s.sampling.weighting.temperature,
    cropsTrain: crops.train,
    cropsVal: crops.val,
    cropsTest: crops.test,
    maxCrops: s.sampling.max_crops_per_dataset,
    filterContent: filters.some(f => f.name === "content"),
    filterLabelled: !!labelled,
    labelledPercent: labelled ? Math.round(labelled.min_fraction * 1e4) / 100 : 5,
    holdout: s.splits.holdout_ids.join(" "),
    exclude: new Set(s.select.exclude.ids),
  };
}
// Where two parsed specs differ, as dotted paths: a spec against the one the controls write for it names what the
// controls cannot hold. A list of words is compared as a set, except `over`, whose order is the weighting's.
export function specDiff(a, b, path = "") {
  const plain = v => v && typeof v === "object" && !Array.isArray(v);
  if (plain(a) && plain(b))
    return [...new Set([...Object.keys(a), ...Object.keys(b)])].flatMap(key =>
      specDiff(a[key], b[key], path ? `${path}.${key}` : key),
    );
  const words = v =>
    Array.isArray(v) && !path.endsWith(".over") && v.every(x => typeof x === "string") ? [...v].sort() : v;
  return JSON.stringify(words(a)) === JSON.stringify(words(b)) ? [] : [path];
}
