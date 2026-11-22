"""`mia-buffers register`: a plan plus staged volumes → one buffer in buffer-api (spec v0.5 §5).

The hand-registration path: the worker calls the same functions after staging.
"""

from __future__ import annotations

import json
import logging
import math
import random
import re
from pathlib import Path
from urllib.parse import quote

from mia_core.registry import Registry, RegistryError
from mia_core.untrusted import text

from .plan import split_counts
from .spec import Spec

log = logging.getLogger(__name__)


def _tensorizer_params(spec: Spec) -> dict:
    normalize = spec.sampling.normalize
    return {
        "normalize": normalize.method,
        "range": list(normalize.range),
        "clip": normalize.clip,
        "scope": normalize.scope,
    }


def recipe_for(spec: Spec, *, tensorizer: str, harness_commit: str, env_digest: str) -> dict:
    """What a buffer's crops are made with: the tensorizer, its params and the harness that staged them."""
    return {
        "tensorizer": tensorizer,
        "params": _tensorizer_params(spec),
        "harness_commit": harness_commit,
        "env_digest": env_digest,
    }


def buffer_config(
    plan: dict,
    spec: Spec,
    *,
    tensorizer: str,
    recipe: dict,
    plan_hash: str,
) -> dict:
    """The `PUT buffers/<id>` body from a plan and its spec; a frozen list is as long as the spec's crops of its split."""
    patch = spec.patch_for("2D") or {}
    if patch.get("y") != patch.get("x"):
        raise ValueError(f"MuViT crops are square; the 2D patch is {patch}")
    return {
        "spec_name": plan["spec_name"],
        "spec_hash": plan["spec_hash"],
        "catalog_ref": plan.get("catalog_ref"),
        "levels": list(spec.sampling.levels),
        "patch": int(patch["y"]),
        "channels": spec.sampling.patch.c,
        "seed": spec.sampling.seed,
        "tensorizer": tensorizer,
        "tensorizer_params": _tensorizer_params(spec),
        "splits": dict(spec.splits.fractions),
        "split_sizes": {split: total for split, total in spec.sampling.crops.items() if split != "train"},
        "recipe": recipe,
        "plan_hash": plan_hash,
        "idle_hours": spec.idle.hours,
        "retire_after_days": spec.retire_after_days,
        "auto_approve": spec.plan.auto_approve,
        "max_empty_fraction": spec.health.max_empty_fraction,
    }


def _split_planes(volume_id: str, planes: int, split: dict) -> dict[str, list[int]]:
    """The plan row's split policy applied to a volume's planes: `holdout` puts every plane in test; `fractions` is a
    seeded shuffle cut by the row's fractions (largest remainder); split keys are (volume, plane)."""
    fractions = split["fractions"]
    if split["policy"] == "holdout":
        return {name: list(range(planes)) if name == "test" else [] for name in fractions}
    rng = random.Random(f"{split['seed']}:{volume_id}")
    order = list(range(planes))
    rng.shuffle(order)
    out: dict[str, list[int]] = {name: [] for name in fractions}
    if planes < len(fractions):  # too few planes to apportion: each plane picks its split by fraction, so a dataset
        for plane in order:  # of single-image volumes still splits by fraction overall (review C1)
            out[rng.choices(list(fractions), weights=list(fractions.values()))[0]].append(plane)
        return {name: sorted(chosen) for name, chosen in out.items()}
    for name, count in split_counts(planes, fractions).items():
        out[name], order = sorted(order[:count]), order[count:]
    return out


VOLUME_PATH_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")  # a plain directory name under the record's staged dir


def normalize_bounds(samples: list[list[float]], range_: tuple[float, float]) -> dict:
    """`{"lo": [per channel], "hi": [per channel]}`: the range's percentiles of each channel's sorted sample
    (nearest rank), what the tensorizer uses instead of per-patch percentiles for scope volume|dataset."""

    def rank(values: list[float], percentile: float) -> float:
        return values[min(len(values) - 1, max(0, math.ceil(percentile / 100 * len(values)) - 1))]

    return {
        "lo": [rank(values, range_[0]) for values in samples],
        "hi": [rank(values, range_[1]) for values in samples],
    }


def check_sample(value: object, channels: int) -> str | None:
    """Why a stage.json `intensity_sample` is unusable (not `channels` non-empty lists of numbers), or None."""
    if not (isinstance(value, list) and len(value) == channels):
        return f"intensity_sample is not a list of {channels} channel sample(s)"
    for sample in value:
        if not (isinstance(sample, list) and sample):
            return "a channel sample is empty or not a list"
        if not all(type(number) in (int, float) and math.isfinite(number) for number in sample):
            return "a channel sample holds a non-number"
    return None


SELECTION_NAMES = 8  # the most splits a selection, and reasons a split, may name
SHORT_SHOWN = 20  # the most volume splits `shortfall` names; the rest are counted
NAME_RE = re.compile(r"[a-z][a-z0-9_]{0,31}")  # a split, or why a draw was rejected: "repeat" or a filter's name


def _selection(entry: object, key: str) -> tuple[str, dict[str, tuple[int, int, dict[str, int]]]] | None:
    """A stage.json entry's volume directory (under `key`) and its `selection` as {split: (wanted, staged,
    {reason: rejected draws})}, or None when the entry has no selection or any of it is not what the harness
    writes. It is a job's file: the names become timeline text and the counts a weight."""
    name = entry.get(key) if isinstance(entry, dict) else None
    selection = entry.get("selection") if isinstance(entry, dict) else None
    if not (isinstance(name, str) and len(name) <= 64 and VOLUME_PATH_RE.fullmatch(name)):
        return None
    if not (isinstance(selection, dict) and len(selection) <= SELECTION_NAMES):
        return None
    counts: dict[str, tuple[int, int, dict[str, int]]] = {}
    for split, found in selection.items():
        if not (isinstance(split, str) and NAME_RE.fullmatch(split) and isinstance(found, dict)):
            return None
        wanted, staged, rejected = found.get("wanted"), found.get("staged"), found.get("rejected")
        if not (type(wanted) is int and type(staged) is int and 0 <= staged <= wanted and isinstance(rejected, dict)):
            return None
        if len(rejected) > SELECTION_NAMES or not all(
            isinstance(why, str) and NAME_RE.fullmatch(why) and type(n) is int and n >= 0 for why, n in rejected.items()
        ):
            return None
        counts[split] = (wanted, staged, dict(rejected))
    return name, counts


def staged_share(volume: object) -> float:
    """The part of its planned crops a volume staged, over its splits together: 1.0 for a volume whose rule had no
    filters (no `selection`), and for a selection that cannot be read."""
    found = _selection(volume, "path")
    wanted = sum(counts[0] for counts in found[1].values()) if found else 0
    return sum(counts[1] for counts in found[1].values()) / wanted if found and wanted else 1.0


def shortfall(report: dict) -> str:
    """` Short of its plan: v0.zarr val 0 of 4 (labelled 2000); …`, to follow the line that says a record is
    servable: each volume and split of the record's stage.json that staged fewer crops than its rule asked for,
    the volumes that kept none (`unstaged`) included, with the rejected draws by reason. "" when none is short."""
    found = []
    for key, name in (("volume", "unstaged"), ("path", "volumes")):
        listed = report.get(name)
        found += [_selection(entry, key) for entry in (listed if isinstance(listed, list) else [])]
    short = []
    for volume, counts in sorted((entry for entry in found if entry), key=lambda entry: entry[0]):
        for split, (wanted, staged, rejected) in counts.items():
            if staged < wanted:
                why = ", ".join(f"{reason} {count}" for reason, count in rejected.items())
                short.append(f"{volume} {split} {staged} of {wanted}" + (f" ({why})" if why else ""))
    more = [f"and {len(short) - SHORT_SHOWN} more"] if len(short) > SHORT_SHOWN else []
    return text(f" Short of its plan: {'; '.join(short[:SHORT_SHOWN] + more)}.", 2000) if short else ""


def check_volume(volume: object) -> str | None:
    """Why a stage.json volume entry cannot be registered, or None. The stage job is reviewed code, but its output
    is still a job's file: the path must stay inside the staged directory and the keys the body needs must exist."""
    if not isinstance(volume, dict):
        return "volume entry is not an object"
    path = volume.get("path")
    if not (isinstance(path, str) and VOLUME_PATH_RE.fullmatch(path) and path != ".."):
        return f"path {path!r} is not a plain directory name"
    try:
        axes, shape = volume["axes"], volume["shape"]
        if not (isinstance(axes, str) and isinstance(shape, list) and len(axes) == len(shape)):
            return "axes and shape disagree"
        for level in volume["levels"]:
            for key in ("factor", "shape", "scale_nm", "translation_nm"):
                level[key]
    except (KeyError, TypeError) as error:
        return f"missing or malformed field: {type(error).__name__}: {error}"
    samples = volume.get("samples", {})
    if not (
        isinstance(samples, dict)
        and all(
            isinstance(candidates, list) and all(_is_candidate(candidate) for candidate in candidates)
            for candidates in samples.values()
        )
    ):
        return "samples is not {split: [descriptor]}"
    return None


def _is_candidate(candidate: object) -> bool:
    """A planned sample as the harness writes it: a plane dict, a finite (y, x) centre in nm, int levels and patch,
    and the per-level starts dict; served to consumers verbatim, so typed here."""
    if not isinstance(candidate, dict):
        return False
    centre, levels = candidate.get("centre_nm"), candidate.get("levels")
    return (
        isinstance(candidate.get("plane"), dict)
        and isinstance(centre, list)
        and len(centre) == 2
        and all(type(value) in (int, float) and math.isfinite(value) for value in centre)
        and isinstance(levels, list)
        and all(type(level) is int for level in levels)
        and type(candidate.get("patch")) is int
        and isinstance(candidate.get("starts"), dict)
    )


def _sparse(volume: dict) -> bool:
    """A sparse OME-Zarr 0.6 volume: its levels say the staged box (`origin`, `extent`), a 0.5 one's never do."""
    levels = volume["levels"]
    return bool(levels) and ("origin" in levels[0] or "extent" in levels[0])


def volume_bodies(
    row: dict,
    staged: Path,
    spec: Spec,
    report: dict | None = None,
    refused: dict[str, str] | None = None,
    *,
    artifact: str | None = None,
) -> list[tuple[str, dict]]:
    """(volume id, `PUT volumes/<vol>` body) for every well-formed raw volume in the record's stage.json (or in
    `report`, the same JSON already read by the caller, e.g. through read_untrusted). Malformed entries go to
    `refused` (volume id -> reason) when a dict is given, else raise ValueError. With `normalize.scope` volume
    or dataset, each body carries `normalize` bounds from the stage job's `intensity_sample` (a volume without a
    usable sample is refused: it must be staged again with a harness that records one). Each body names the staged
    `artifact` its volume lives in (default: the staged directory's name, the hand-registration layout)."""
    split = row.get("split")
    if split is None:  # a plan row without a split policy: the spec's fractions, as the plan would decide
        log.warning("%s: plan row has no split policy: using the spec's fractions", row["id"])
        split = {"policy": "fractions", "fractions": dict(spec.splits.fractions), "seed": spec.splits.seed}
    if report is None:
        report = json.loads((staged / "stage.json").read_text())
    scope = spec.sampling.normalize.scope
    volumes: list[dict] = []

    def refuse(index: int, volume: object, why: str) -> None:
        if refused is None:
            raise ValueError(f"{row['id']}: volume {index}: {why}")
        path = volume.get("path") if isinstance(volume, dict) else None
        refused[f"{row['id']}/{path if isinstance(path, str) else index}"] = why

    for index, volume in enumerate(report.get("volumes") or []):
        if (why := check_volume(volume)) is not None:
            refuse(index, volume, why)
        elif _sparse(volume) and not any(volume.get("samples", {}).values()):
            # a harness regression must not turn a staged box into uniform centres over the whole volume
            refuse(index, volume, "0.6 volume without samples")
        elif scope != "patch" and (why := check_sample(volume.get("intensity_sample"), _channels(volume))):
            refuse(index, volume, f"{why}; re-stage with the current harness for normalize.scope {scope}")
        elif scope == "dataset" and volumes and _channels(volume) != _channels(volumes[0]):
            refuse(
                index,
                volume,
                f"{_channels(volume)} channel(s) where the record's first volume has {_channels(volumes[0])}: "
                "dataset-scope bounds need one channel count",
            )
        else:
            volumes.append(volume)
    # a raw whose filters kept nothing is not written, and its planes still count: its part of the record's
    # weight goes to no other volume
    unstaged = report.get("unstaged")
    kept_nothing = sum(
        entry["planes"]
        for entry in (unstaged if isinstance(unstaged, list) else [])
        if isinstance(entry, dict) and type(entry.get("planes")) is int and entry["planes"] > 0
    )
    total_planes = sum(_planes(volume) for volume in volumes) + kept_nothing or 1
    bounds: dict | None = None
    if scope == "dataset" and volumes:
        # ponytail: dataset bounds pool each volume's sample with equal weight, not by plane count; weight by
        # planes if a dataset of unequal volumes shows it matters
        channels = _channels(volumes[0])
        pooled = [
            sorted(value for volume in volumes for value in volume["intensity_sample"][c]) for c in range(channels)
        ]
        bounds = normalize_bounds(pooled, spec.sampling.normalize.range)
    out = []
    for volume in volumes:
        volume_id = f"{row['id']}/{volume['path']}"
        planes = _planes(volume)
        if scope == "volume":
            bounds = normalize_bounds(volume["intensity_sample"], spec.sampling.normalize.range)
        out.append(
            (
                volume_id,
                {
                    "path": str(staged / volume["path"]),
                    "record": row["id"],
                    "axes": volume["axes"],
                    "shape": list(volume["shape"]),
                    "levels": [
                        {key: level[key] for key in ("factor", "shape", "scale_nm", "translation_nm")}
                        for level in volume["levels"]
                    ],
                    "split_planes": _split_planes(volume_id, planes, split),
                    "labels": dict(volume.get("labels") or {}),
                    # a volume whose filters kept fewer crops than planned serves each about as often as any
                    "weight": float(row["weight"]) * planes / total_planes * staged_share(volume),
                    "channels_stored": _channels(volume),
                    "recipe": row.get("channel_recipe"),
                    "voxel_size_nm": {k: v for k, v in (volume.get("voxel_size_nm") or {}).items() if v is not None},
                    "normalize": bounds,
                    "artifact": artifact or staged.name,
                    "samples": volume.get("samples") or {},  # a sparse artifact's planned crops, the only ones staged
                },
            )
        )
    return out


def _planes(volume: dict) -> int:
    axes, shape = volume["axes"], volume["shape"]
    return int(shape[axes.index("z")]) if "z" in axes else 1


def _channels(volume: dict) -> int:
    axes, shape = volume["axes"], volume["shape"]
    return int(shape[axes.index("c")]) if "c" in axes else 1


def register(
    url: str,
    buffer: str,
    plan: dict,
    spec: Spec,
    staged_root: Path,
    *,
    tensorizer: str,
    recipe: dict,
    plan_hash: str,
    token: str | None = None,
) -> dict:
    """Create the buffer, register every pooled, staged volume (with the worker `token`, when the service wants
    one) and authorize it to serve by writing the `approved` and `ready` events; returns registered / skipped /
    refused / events. A volume the service refuses is listed; the service down, the token refused or a fault of
    its own raises RegistryError."""
    out: dict = {"registered": [], "skipped": {}, "refused": {}}
    address = f"/buffers/{quote(buffer, safe='')}"
    with Registry(url, token) as registry:
        registry.request(
            "PUT",
            address,
            json=buffer_config(plan, spec, tensorizer=tensorizer, recipe=recipe, plan_hash=plan_hash),
        )
        for row in plan["rows"]:
            if not (row.get("admitted") and (row.get("fit") or {}).get("ok")):
                continue
            staged = staged_root / row["id"]
            if not (staged / "stage.json").is_file():
                out["skipped"][row["id"]] = "not staged"
                continue
            for volume_id, body in volume_bodies(row, staged, spec, refused=out["refused"]):
                try:
                    registry.request("PUT", f"{address}/volumes/{quote(volume_id, safe='')}", json=body)
                except RegistryError as error:
                    if error.status is None or error.status == 401 or error.status >= 500:
                        raise  # not a verdict on this volume
                    out["refused"][volume_id] = error.message[:500]
                else:
                    out["registered"].append(volume_id)
        for kind in ("approved", "ready"):  # buffer-api serves only under an approved, ready or idle event
            registry.event(buffer, kind, detail="mia-buffers register")
        out["events"] = ["approved", "ready"]
    return out
