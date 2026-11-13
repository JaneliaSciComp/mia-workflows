import json
from pathlib import Path

import pytest
import respx
import yaml

from autodev.github import GitHub
from mia_data import catalog
from mia_data.seed import render_issue

API = "https://api.github.com"
FIX = Path(__file__).parent / "fixtures" / "mia"
PATHS = {
    "bbbc034-hipsc-nuclei-3d": "datasets/3D/fluorescence-other",
    "czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet": "datasets/3D/cryo-ET",
    "hf-pytc-zebrafinch-j0126-ffn-em": "datasets/3D/SBF-SEM",
}


def _gh(repo):
    async def nosleep(s): ...

    return GitHub(repo, lambda: "tok", writes_per_second=1000, sleep=nosleep)


@pytest.fixture
def gh_catalog():
    return _gh("o/catalog")


def mock_catalog(ids=None):
    """Mock the catalog repo: its state files, its tree and its records."""
    paths = {i: f"{d}/{i}.yaml" for i, d in PATHS.items() if ids is None or i in ids}
    paths["synthetic-gpl"] = "datasets/3D/EM/synthetic-gpl.yaml"
    if ids is not None and "synthetic-gpl" not in ids:
        del paths["synthetic-gpl"]
    for f in ("downloads", "downloads-native"):
        respx.get(f"{API}/repos/o/catalog/contents/state/{f}.json", params={"ref": "state"}).respond(
            200, content=(FIX / f"{f}.json").read_bytes()
        )
    respx.get(f"{API}/repos/o/catalog/git/trees/main").respond(
        200,
        json={"tree": [{"type": "blob", "path": p} for p in paths.values()] + [{"type": "tree", "path": "datasets"}]},
    )
    for i, p in paths.items():
        respx.get(f"{API}/repos/o/catalog/contents/{p}", params={"ref": "main"}).respond(
            200, content=(FIX / "records" / f"{i}.yaml").read_bytes()
        )


@respx.mock
@respx.mock
@respx.mock
@respx.mock
@respx.mock
@respx.mock
@respx.mock
async def test_downloaded_union_keeps_newest_and_both_downloaders(gh_catalog):
    mock_catalog()
    d = await catalog.downloaded(gh_catalog)
    e = d["bbbc034-hipsc-nuclei-3d"]
    assert e["downloaders"] == ["tensorswitch", "native"]
    ats = [
        json.loads((FIX / f).read_text())["downloaded"]["bbbc034-hipsc-nuclei-3d"]["at"]
        for f in ("downloads.json", "downloads-native.json")
    ]
    assert e["at"] == max(ats)
    assert d["czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet"]["downloaders"] == ["tensorswitch"]


@respx.mock
@respx.mock
@pytest.mark.parametrize(
    "rid, folder",
    [
        ("bbbc034-hipsc-nuclei-3d", "lm-human-bbbc034-hipsc-nuclei-3d"),  # as demo-native's dataset_dir
        ("hf-pytc-zebrafinch-j0126-ffn-em", "em-taeniopygia-guttata-hf-pytc-zebrafinch-j0126-ffn"),
        (
            "czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet",
            "em-mycoplasmoides-pneumoniae-mpneumoniae-chloramphenicol-ribosomes",
        ),
    ],
)
def test_dataset_folder_follows_miao_layout(rid, folder):
    rec = yaml.safe_load((FIX / "records" / f"{rid}.yaml").read_text())
    assert catalog.dataset_folder(rec, rid) == folder
    with pytest.raises(ValueError):
        catalog.dataset_folder({**rec, "imaging": {**rec["imaging"], "organism": []}}, rid)


def test_render_issue_with_record_commit_only():
    rec = yaml.safe_load((FIX / "records" / "bbbc034-hipsc-nuclei-3d.yaml").read_text())
    title, body, labels = render_issue(rec, {"record_commit": "a" * 40}, "o/catalog", "datasets/x.yaml")
    assert body.startswith("<!-- mia-data:record=bbbc034-hipsc-nuclei-3d -->")
    assert "Downloaded by: none" in body and "Dataset dir" not in body and "data:new" in labels
    assert "- Licence: " in body and "mia-data:root" not in body  # who is charged travels with the action
