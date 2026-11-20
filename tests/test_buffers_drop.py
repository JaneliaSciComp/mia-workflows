"""mia_buffers.drop: names for a manual drop, the harness cache key, the files.json manifest."""

import hashlib
import json

from mia_buffers.cli import main
from mia_buffers.drop import cache_key, cached_files, drop_names, merge_manifest, read_manifest


def test_drop_names_prefer_unique_basenames():
    urls = ["https://h/a/x.zip::m.png", "https://drive.google.com/uc?export=download&id=1", "manual://rec/y%20z.zip"]
    assert drop_names(urls) == [
        ("https://h/a/x.zip", "x.zip"),
        ("https://drive.google.com/uc?export=download&id=1", "file-1"),
        ("manual://rec/y%20z.zip", "y z.zip"),
    ]


def test_drop_names_fall_back_on_collision_and_dedupe_urls():
    assert drop_names(["https://h/a/f.tif", "https://h/b/f.tif", "https://h/a/f.tif::m"]) == [
        ("https://h/a/f.tif", "file-0"),
        ("https://h/b/f.tif", "file-1"),
    ]


def test_drop_names_refuse_path_tricks():
    assert [name for _, name in drop_names(["https://h/a/..", "https://h/.hidden"])] == ["file-0", "file-1"]


def test_cache_key_matches_harness_formula():
    assert cache_key("u", {"size": 1, "sha256": "ab"}) == hashlib.sha256(b'u\n{"sha256": "ab", "size": 1}').hexdigest()


def test_manifest_merge_is_atomic_and_additive(tmp_path):
    assert read_manifest(tmp_path) == {}
    merge_manifest(tmp_path, {"u1": {"validators": {"size": 1}, "source": "manual"}})
    merge_manifest(tmp_path, {"u2": {"validators": {"size": 2}, "source": "download"}})
    assert set(read_manifest(tmp_path)) == {"u1", "u2"}
    assert json.loads((tmp_path / "files.json").read_text())["u1"]["source"] == "manual"
    assert [p.name for p in tmp_path.iterdir()] == ["files.json"]


def test_cached_files_are_named_as_upstream_and_linked(tmp_path):
    cache, out = tmp_path / "rec", tmp_path / "out"
    zipped, plain = {"size": 9, "etag": None}, {"size": 3}
    merge_manifest(
        cache,
        {
            "https://h/data.zip": {"validators": zipped, "source": "download"},
            "https://h/a/readme.txt": {"validators": plain, "source": "download"},
        },
    )
    members = ["data/s1/wsi.ome.tif", "data/s1/not-fetched.tif", "../../escape.tif"]
    (cache / cache_key("https://h/data.zip::", zipped)).write_text(json.dumps(dict.fromkeys(members, [])))
    for member in ("data/s1/wsi.ome.tif", "../../escape.tif"):
        (cache / cache_key(f"https://h/data.zip::{member}", zipped)).write_bytes(b"II*")
    (cache / cache_key("https://h/a/readme.txt", plain)).write_text("hi\n")

    assert [name for name, _ in cached_files(cache)] == ["readme.txt", "data.zip/data/s1/wsi.ome.tif"]
    assert main(["raw", str(cache), "--to", str(out)]) == 0
    assert main(["raw", str(cache), "--to", str(out)]) == 0  # again: the links are replaced
    assert (out / "data.zip/data/s1/wsi.ome.tif").read_bytes() == b"II*"
    assert (out / "readme.txt").read_text() == "hi\n"
    assert main(["raw", str(cache), "--to", str(out), "--copy"]) == 0  # over the links, and again over the copies
    assert main(["raw", str(cache), "--to", str(out), "--copy"]) == 0
    copied = out / "data.zip/data/s1/wsi.ome.tif"
    assert not copied.is_symlink() and copied.read_bytes() == b"II*"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out", "rec"]
    assert main(["raw", str(tmp_path / "none")]) == 1
