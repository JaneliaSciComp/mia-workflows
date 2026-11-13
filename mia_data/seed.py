"""How a dataset's GitHub issue reads, and the markers its body carries.

The mirror opens a dataset's issue (`mia_data.mirror.dataset_issue`) with the body built here: it starts with a
hidden marker naming the record id, so an issue opened just before a crash is found again, and carries the
record's facts. An issue appears when its dataset has something to say; nothing creates issues in bulk.
"""

from __future__ import annotations

import re

from .status import safe_text

MARKER = "<!-- mia-data:record={} -->"
MARKER_RE = re.compile(r"<!-- mia-data:record=(\S+) -->")  # matched only at the start of a body

ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,120}")


def render_issue(
    record: dict, entry: dict, repo: str = "", path: str = "", record_id: str | None = None
) -> tuple[str, str, list[str]]:
    """Build (title, body, labels) for a dataset issue.

    `record` is the catalog's YAML record, `entry` its downloads-state entry (dataset_dir, record_commit,
    bytes_on_disk, downloaders) or just `{"record_commit": sha}` for a record nobody downloaded (a buffer asks
    for it; its jobs fetch on demand), `repo`/`path` locate the record on GitHub for links."""
    record_id = record_id or record["id"]
    imaging, spdx = record.get("imaging") or {}, (record.get("license") or {}).get("spdx")
    arrays = (record.get("technical") or {}).get("arrays") or []
    family = (imaging.get("modality") or ["unknown"])[0]
    # no licence or size verdict here: who may use a dataset is each buffer spec's decision (spec §12)
    labels = ["data:new", f"modality:{family}"]
    if any(array["role"] == "label" for array in arrays):
        labels.append("has-labels")
    base_url = f"https://github.com/{repo}/blob"
    voxel = imaging.get("voxel_size_nm") or {}
    data = record.get("data") or {}
    urls = data.get("download_url") or []
    lines = [
        MARKER.format(record_id),
        "",
        f"- Record: [main]({base_url}/main/{path}), "
        f"[at `{entry['record_commit'][:12]}`]({base_url}/{entry['record_commit']}/{path})",
        f"- Licence: {safe_text(spdx)}",
        f"- Modality: {safe_text(', '.join(imaging.get('modality') or []))}",
        f"- Organism: {safe_text(', '.join(imaging.get('organism') or []))}",
        f"- Voxel size (nm): {safe_text(f'x={voxel.get("x")} y={voxel.get("y")} z={voxel.get("z")}')}",
        f"- Data size: {entry.get('bytes_on_disk', 'unknown')} bytes",
        f"- Source: {safe_text(', '.join([urls] if isinstance(urls, str) else urls))}",
        f"- Downloaded by: {', '.join(entry.get('downloaders') or []) or 'none (jobs fetch on demand)'}",
        *([f"- Dataset dir: `{safe_text(entry['dataset_dir'])}`"] if entry.get("dataset_dir") else []),
        "",
        "| role | axes | shape | dtype |",
        "|---|---|---|---|",
        *(
            f"| {safe_text(array['role'])} | {safe_text(array.get('axes'))} | {safe_text(array.get('shape'))} | "
            f"{safe_text(array.get('dtype'))} |"
            for array in arrays
        ),
    ]
    title = f"{record_id}: {safe_text(record.get('short_name') or record.get('title'))}"
    return title, "\n".join(lines) + "\n", labels
