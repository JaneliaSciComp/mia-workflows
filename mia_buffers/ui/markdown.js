// A timeline entry's markdown, built as DOM nodes.
import { el, link, tipOn } from "./dom.js";

// ---- a timeline entry's markdown: the subset a round writes, built as DOM nodes. Nothing here is ever parsed as
// HTML (a body can carry text from a sandboxed job), and only an http(s) URL becomes a link.
// A round's crops table names its images in the gallery repo on GitHub, which a browser cannot read. buffer-api
// serves the same files from the round's workspace (`GET /gallery/…`), so such an image is a picture from this
// origin, small in its table and at its own size in the tooltip while the pointer is on it. Any other image is a
// link: the page loads nothing from a host a timeline body names.
const GALLERY_IMAGE =
  /^https:\/\/github\.com\/[^/]+\/[^/]+\/blob\/[^/]+\/galleries\/[^/]+\/(i\d+-e\d+)\/([0-9a-f]{40})\/([A-Za-z0-9_-][A-Za-z0-9._-]*)\/(raw|overlay)\.png\?raw=true$/;
function cropImage(round, sha, crop, kind, look = "crop") {
  // kind: raw or overlay; look: the picture's classes
  const src = `/gallery/${round}/${sha}/${crop}/${kind}.png`;
  return tipOn(
    el(
      "a",
      { href: src, target: "_blank", rel: "noopener" },
      el("img", { class: look, src, loading: "lazy", alt: `${kind} of crop ${crop}` }),
    ),
    [{ t: `${kind} of crop ${crop} · click to open it` }, { full: src }],
  );
}
function mdImage(alt, url) {
  const m = GALLERY_IMAGE.exec(url);
  return m ? cropImage(m[1], m[2], m[3], m[4]) : link(url, alt || url);
}
// A judge's findings table (`item | severity | note`) names the crops it judged by id and carries no picture. The
// crops tables of a timeline say where each crop's pictures are: `cropsOf` adds a body's crops to a map of crop id
// -> {round, sha}, and `markdown` given that map shows a finding's overlay beside it. The timeline reads its entries
// oldest first, so a finding gets the picture of the newest crops table before it. A `suspect` or `bad` finding
// has its severity in that severity's colour, and its picture a border of the same; `ok` is plain.
const FLAGGED = ["suspect", "bad"];
const GALLERY_CROP =
  /\/galleries\/[^/\s]+\/(i\d+-e\d+)\/([0-9a-f]{40})\/([A-Za-z0-9_-][A-Za-z0-9._-]*)\/raw\.png\?raw=true/g;
export function cropsOf(text, crops) {
  for (const m of text.matchAll(GALLERY_CROP)) crops[m[3]] = { round: m[1], sha: m[2] };
  return crops;
}
export function mdInline(text) {
  // `code`, **bold**, *italic*, [text](url), ![alt](url) and bare URLs
  const out = [],
    token =
      /(`+)([\s\S]+?)\1|\*\*(.+?)\*\*|\*(?=\S)(.+?)(?<=\S)\*|\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)|!\[([^\]]*)\]\((https?:\/\/[^\s)]+)\)|https?:\/\/[^\s)>\]"'`]+/g;
  let last = 0,
    m;
  while ((m = token.exec(text))) {
    out.push(text.slice(last, m.index));
    last = m.index + m[0].length;
    if (m[1]) out.push(el("code", {}, m[2].trim()));
    else if (m[3] != null) out.push(el("strong", {}, ...mdInline(m[3])));
    else if (m[4] != null) out.push(el("em", {}, ...mdInline(m[4])));
    else if (m[5] != null) out.push(link(m[6], m[5].includes("://") ? m[5] : mdInline(m[5])));
    else if (m[8] != null) out.push(mdImage(m[7], m[8]));
    else out.push(link(m[0], m[0]));
  }
  out.push(text.slice(last));
  return out.filter(part => part !== "");
}
// paragraphs, headings, fenced code, pipe tables, lists, quotes and a <details> fold; comments (markers) are dropped.
// A fold is built closed: whoever shows the nodes says which one is open. `crops` (see cropsOf) is optional.
export function markdown(text, crops) {
  // ponytail: a nested list is flattened into its parent and `---` is a paragraph; a real parser when a round writes more
  const lines = text.replace(/<!--[\s\S]*?-->/g, "").split("\n"),
    out = [];
  const fence = /^\s*(`{3,}|~{3,})/,
    heading = /^(#{1,6})\s+(.*)$/,
    item = /^\s*(?:[-*+]|(\d+)[.)])\s+(.*)$/,
    fold = /^<details><summary>(.*)<\/summary>\s*$/,
    rule = /^\s*\|?\s*:?-+:?\s*(\|\s*:?-+:?\s*)*\|?\s*$/;
  const tableAt = i => lines[i].includes("|") && (lines[i + 1] || "").includes("|") && rule.test(lines[i + 1]);
  const starts = i =>
    !lines[i].trim() ||
    fence.test(lines[i]) ||
    heading.test(lines[i]) ||
    lines[i].startsWith(">") ||
    item.test(lines[i]) ||
    tableAt(i) ||
    fold.test(lines[i]);
  const texts = row =>
    row
      .trim()
      .replace(/^\||\|$/g, "")
      .split(/(?<!\\)\|/)
      .map(cell => cell.trim().replace(/\\\|/g, "|"));
  const cells = row => texts(row).map(mdInline);
  const broken = rows => rows.flatMap((row, n) => (n ? [el("br"), ...mdInline(row)] : mdInline(row))); // a newline is a line break, as in an issue comment
  for (let i = 0; i < lines.length; ) {
    const line = lines[i];
    let m;
    if (!line.trim()) i++;
    else if ((m = fence.exec(line))) {
      const code = [],
        closes = row => row.trim().startsWith(m[1]) && /^[`~]+$/.test(row.trim());
      for (i++; i < lines.length && !closes(lines[i]); i++) code.push(lines[i]);
      i++;
      out.push(el("pre", {}, el("code", {}, code.join("\n"))));
    } else if ((m = heading.exec(line))) {
      out.push(el("h" + Math.min(6, m[1].length + 2), {}, ...mdInline(m[2])));
      i++;
    } else if ((m = fold.exec(line))) {
      // the one tag a round writes (a long crops table)
      const folded = [];
      for (i++; i < lines.length && lines[i].trim() !== "</details>"; i++) folded.push(lines[i]);
      i++;
      out.push(el("details", {}, el("summary", {}, ...mdInline(m[1])), ...markdown(folded.join("\n"), crops)));
    } else if (tableAt(i)) {
      const rows = [];
      for (i += 2; i < lines.length && lines[i].includes("|"); i++) rows.push(lines[i]);
      // a judge's findings: folded like a crops table, each finding with the overlay of the crop it names
      const judged = texts(line).join("|") === "item|severity|note",
        at = row => crops?.[texts(row)[0]],
        flag = row => (judged && FLAGGED.includes(texts(row)[1]) ? `sev-${texts(row)[1]}` : "");
      const pictured = judged && rows.some(at);
      const picture = row =>
        at(row) ? [cropImage(at(row).round, at(row).sha, texts(row)[0], "overlay", `crop ${flag(row)}`.trim())] : [];
      const marked = row =>
        cells(row).map((cell, n) => (n === 1 && flag(row) ? [el("span", { class: flag(row) }, ...cell)] : cell));
      const head = [...(pictured ? [["overlay"]] : []), ...cells(line)];
      const body = rows.map(row => [...(pictured ? [picture(row)] : []), ...marked(row)]);
      const table = el(
        "div",
        { class: "tablewrap" },
        el(
          "table",
          { class: "plain" },
          el("thead", {}, el("tr", {}, ...head.map(cell => el("th", {}, ...cell)))),
          el("tbody", {}, ...body.map(row => el("tr", {}, ...row.map(cell => el("td", {}, ...cell))))),
        ),
      );
      out.push(
        judged
          ? el("details", {}, el("summary", {}, `${rows.length} ${rows.length === 1 ? "finding" : "findings"}`), table)
          : table,
      );
    } else if (line.startsWith(">")) {
      const quoted = [];
      for (; i < lines.length && lines[i].startsWith(">"); i++) quoted.push(lines[i].replace(/^> ?/, ""));
      out.push(el("blockquote", {}, ...markdown(quoted.join("\n"), crops)));
    } else if ((m = item.exec(line))) {
      const list = el(m[1] ? "ol" : "ul", {}),
        items = [];
      for (; i < lines.length && lines[i].trim(); i++) {
        if ((m = item.exec(lines[i]))) items.push([m[2]]);
        else if (/^\s/.test(lines[i]))
          items.at(-1).push(lines[i].trim()); // an indented line continues its item
        else break;
      }
      list.append(...items.map(rows => el("li", {}, ...broken(rows))));
      out.push(list);
    } else {
      const rows = [line];
      for (i++; i < lines.length && !starts(i); i++) rows.push(lines[i]);
      out.push(el("p", {}, ...broken(rows)));
    }
  }
  return out;
}
