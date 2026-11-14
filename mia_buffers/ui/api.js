// Talking to the service: the page's one fetch, the banner its errors go to, and what the service said at
// boot (`S`).
import { $, el } from "./dom.js";

// ---- fetch: errors go to the banner, the caller gets null
export class HttpError extends Error { constructor(status, message) { super(message); this.status = status; } }
export async function send(path, options) {  // the page's one fetch: the response and its text
  let res;
  try { res = await fetch(path, options); } catch (e) { throw new HttpError(0, e.message); }
  return [res, await res.text()];
}
export async function api(path, options) {
  const [res, text] = await send(path, options); let data = null;
  if (text) { try { data = JSON.parse(text); } catch { data = { error: text.slice(0, 300) }; } }
  if (!res.ok) throw new HttpError(res.status, (data && data.error) || res.statusText);
  return data;
}
export function banner(message) {  // at most 3 lines, a repeat bumps its count instead of adding one, and each fades after 20 s
  const box = $("banner"), same = [...box.children].find(d => d.dataset.message === message);
  if (same) { same.count = (same.count || 1) + 1; same.firstChild.textContent = `${message} (×${same.count})`; clearTimeout(same.timer); }
  const line = same || el("div", { "data-message": message }, el("span", {}, message), el("button", { type: "button", "aria-label": "dismiss", onclick: () => line.remove() }, "×"));
  line.timer = setTimeout(() => line.remove(), 20000);
  if (!same) { box.append(line); while (box.children.length > 3) box.firstChild.remove(); }
}
export async function load(path, quiet404) {  // quiet404: a 404 is an expected answer (a buffer not sourced yet), not an error
  try { return await api(path); }
  catch (e) { if (!(quiet404 && e.status === 404)) banner(`GET ${path} → ${e.status || "network error"}: ${e.message}`); return null; }
}

// ---- shared state: the service's meta, the newest catalog, and the registry rows the views join
export const S = { meta: {}, catalogs: [], ref: null, plan: null, planError: null };

// ---- what an action route takes: JSON, and one key per click
export const JSON_HEADERS = { "Content-Type": "application/json" };
export const newKey = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 10);  // one per click; Retry sends it again
