// The YAML the page writes: a buffer spec from the designer's controls, and a preparation request. No DOM.

// ---- what the designer's controls offer
export const DIMS = ["2D","2D+t","3D","3D+t"];
export const LIC = ["CC0-1.0","CC-BY-4.0","CC-BY-3.0","MIT","BSD-3-Clause","Apache-2.0","CC-BY-SA-4.0","CC-BY-NC-4.0","CC-BY-NC-SA-4.0","CC-BY-NC-ND-4.0","GPL-3.0-or-later"];
export const SRC = ["manual","proofread","automatic","mixed","unknown"];
const LEVELS = [1,2,4,8,16,32];
export const levelsOf = coarsest => LEVELS.filter(l => l <= coarsest);
export const levelsKey = c => `p${c.patch}-l${levelsOf(c.level).join("-")}`;  // mia_buffers.jobspec.levels_key

// A YAML scalar: plain when YAML reads it back as the same string, double-quoted (JSON is valid YAML) otherwise.
export function yq(value) {
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  const s = String(value ?? "");
  const risky = s === "" || /[:#,[\]{}&!|>'"%@`\u0000-\u001f\u007f]/.test(s) || /^[\s*?-]/.test(s) || /\s$/.test(s)
    || /^(true|false|yes|no|on|off|y|n|null|~)$/i.test(s)
    || /^[-+]?(\d[\d_]*(\.\d*)?|\.\d+)([eE][-+]?\d+)?$/.test(s) || /^0[xob]/i.test(s) || /^[-+]?\.(inf|nan)$/i.test(s);
  return risky ? JSON.stringify(s) : s;
}
const flow = xs => `[${xs.map(yq).join(", ")}]`;
export function toSpecYaml(c, ref, repo) {
  const p = c.patch, z = c.patchZ, sources = SRC.filter(s => c.sources.has(s));
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
    "  weighting:",
    "    scheme: temperature",
    "    over: [modality, dataset]",
    `    temperature: ${c.T}`,
    "",
    "splits:",
    "  fractions: {train: 0.8, val: 0.1, test: 0.1}",
    "",
    "plan:",
    "  auto_approve: true",
    "",
  ].join("\n");
}
export function toPrepYaml(records, ref, repo) {
  return ["schema_version: 1", "kind: preparation", `catalog: {repo: ${yq(repo || "")}, ref: ${yq(ref || "")}}`, "records:", ...records.map(r => `  - ${yq(r)}`), ""].join("\n");
}
