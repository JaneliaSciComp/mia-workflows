"""Reading the catalog repository (mia-agentic-search).

Its `main` branch holds one YAML record per dataset under datasets/; its `state` branch holds the
downloaded-state files that say which datasets are on disk, where, and at which record commit. The
dataset folder naming at the bottom mirrors the catalog's own layout tool, so crops land next to the data.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from autodev.github import GitHub

FILES = {"state/downloads.json": "tensorswitch", "state/downloads-native.json": "native"}  # path -> downloader name


async def downloaded(gh_catalog: GitHub) -> dict[str, dict]:
    """record id -> its downloads entry, merging both downloaders' state files.

    When both have an id, the entry with the newest `at` wins; `downloaders` lists every downloader that has it."""
    merged: dict[str, dict] = {}
    for path, downloader in FILES.items():
        raw = await gh_catalog.get_file(path, "state")
        for record_id, entry in (json.loads(raw)["downloaded"] if raw else {}).items():
            previous = merged.get(record_id)
            downloaders = [*(previous["downloaders"] if previous else []), downloader]
            newest = entry if not previous or entry.get("at", "") > previous.get("at", "") else previous
            merged[record_id] = {**newest, "downloaders": downloaders}
    return merged


async def record_paths(gh_catalog: GitHub) -> dict[str, str]:
    """record id -> path of its YAML record on main."""
    tree = (await gh_catalog.request("GET", f"/repos/{gh_catalog.repo}/git/trees/main", params={"recursive": 1})).json()
    return {
        path.rsplit("/", 1)[-1][:-5]: path
        for node in tree["tree"]
        if node["type"] == "blob" and (path := node["path"]).startswith("datasets/") and path.endswith(".yaml")
    }


async def record(gh_catalog: GitHub, path: str, ref: str) -> dict:
    """The parsed YAML record at `path` as of `ref`."""
    raw = await gh_catalog.get_file(path, ref)
    if raw is None:
        raise FileNotFoundError(f"{gh_catalog.repo}:{path}@{ref}")
    return yaml.safe_load(raw)


# Folder names as mia-agentic-search's tools/miao_layout.py@c25d216 builds them (FAMILIES from
# tools/readiness.py@8e9d14f). ponytail: copied tables; re-sync by hand when upstream adds an organism or modality.
FAMILIES = {
    "em": {"FIB-SEM", "SBF-SEM", "ssTEM", "ssSEM", "TEM", "cryo-EM", "cryo-ET"},
    "lm": {
        "confocal",
        "spinning-disk",
        "widefield",
        "light-sheet",
        "two-photon",
        "super-resolution",
        "brightfield",
        "histology-WSI",
        "phase-contrast",
        "DIC",
        "fluorescence-other",
    },
    "xray": {"X-ray"},
    "exm": {"expansion"},
}
ORGANISM_SHORT = {
    "Homo sapiens": "human",
    "Mus musculus": "mouse",
    "Rattus norvegicus": "rat",
    "Drosophila melanogaster": "fly",
    "Caenorhabditis elegans": "worm",
    "Danio rerio": "zebrafish",
    "Platynereis dumerilii": "platynereis",
    "Arabidopsis thaliana": "arabidopsis",
    "Gallus gallus": "chicken",
    "Macaca mulatta": "macaque",
    "Saccharomyces cerevisiae": "yeast",
    "Escherichia coli": "ecoli",
    "Trypanosoma brucei": "trypanosoma",
    "Xenopus laevis": "xenopus",
    "Sus scrofa": "pig",
    "Bos taurus": "cow",
    "Chlamydomonas reinhardtii": "chlamydomonas",
    "Tribolium castaneum": "tribolium",
    "Schmidtea mediterranea": "planaria",
    "Vibrio cholerae": "vibrio",
}


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


UNKNOWN_ORGANISM = "unknown"
OTHER_FAMILY = "other"  # the catalog's modality `other`, and the family of a record nothing else places


def organism_of(organisms: list) -> str:
    """The one organism a record names; `unknown` when it names none, several, or one with no letters or digits."""
    return str(organisms[0]) if len(organisms) == 1 and _slug(str(organisms[0])) else UNKNOWN_ORGANISM


def dataset_folder(record: dict, record_id: str) -> str:
    """The dataset's folder name under storage_root: `{modality family}-{organism}-{name}`.

    A record with no single organism is filed under `unknown`; one whose modalities settle on no family but
    include `other` is filed under the family `other`. Raises ValueError when the record names no modality the
    tables know. `record_id` is the trusted id (the record's own `id` field is not, see seed)."""
    imaging = record.get("imaging") or {}
    modalities, organisms = imaging.get("modality") or [], imaging.get("organism") or []
    # The family of the first modality if it is unambiguous, else the family all modalities share.
    family_sets = [
        {family for modality in subset for family, members in FAMILIES.items() if modality in members}
        for subset in (modalities[:1], modalities)
    ]
    family = next(
        (next(iter(candidates)) for candidates in family_sets if len(candidates) == 1),
        OTHER_FAMILY if OTHER_FAMILY in modalities else None,
    )
    if family is None:
        raise ValueError(f"{record_id}: no dataset folder: modality {modalities} unclear (one family needed)")
    organism = organism_of(organisms)
    words = record_id.split("-")
    if len(words) > 2 and re.fullmatch(r"\d+", words[1]):  # accession prefix: zenodo-7142003-...
        words = words[2:]
    modality_words = {_slug(modality).replace("-", "") for members in FAMILIES.values() for modality in members} | {
        "em",
        "lm",
        "sem",
        "tem",
        "cryoet",
    }
    name = _slug("-".join(word for word in words if word not in modality_words) or record_id)
    return f"{family}-{ORGANISM_SHORT.get(organism) or _slug(organism)}-{name}"
