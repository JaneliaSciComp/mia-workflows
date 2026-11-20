import json

from mia_core.assets import ASSET_STATES, cache_key, cache_missing, raw_asset, read_files_json


def test_raw_asset_is_ready_when_every_binding_url_is_cached(tmp_path):
    (tmp_path / "files.json").write_text(
        json.dumps(
            {
                "https://h/a.zip": {"validators": {"size": 7}, "source": "download"},
                "https://h/a.zip::m.png": {"validators": {"size": 7}, "source": "members"},
                "https://h/b.tif": {"validators": {"size": 3}, "source": "drop"},
            }
        )
    )
    (tmp_path / cache_key("https://h/b.tif", {"size": 3})).write_bytes(b"1234567")
    ready = raw_asset(tmp_path, ["https://h/a.zip::m.png", "https://h/b.tif"])
    assert (
        ready["state"] == "ready"
        and ready["bytes"] == 7 + (tmp_path / "files.json").stat().st_size
        and ready["files"]["https://h/b.tif"]["source"] == "drop"
    )
    assert raw_asset(tmp_path, ["https://h/a.zip", "https://h/c.tif"])["state"] == "partial"
    # the zip's entry (a HEAD) does not vouch for members a fetch never cached
    assert cache_missing(tmp_path, read_files_json(tmp_path), ["https://h/a.zip::x/*.png"]) == (
        False,
        ["https://h/a.zip::x/*.png"],
    )
    assert raw_asset(tmp_path / "none", [])["state"] == "partial" and read_files_json(tmp_path / "none") == {}
    stream = ["https://h/c.zarr"]  # read in place by the harness: never in files.json, still ready
    assert raw_asset(tmp_path, ["https://h/b.tif", *stream], stream)["state"] == "ready"
    assert cache_missing(tmp_path, {}, stream, stream) == (True, [])
    assert cache_missing(tmp_path, {}, stream) == (False, stream)
    assert "evicted" in ASSET_STATES


def test_a_file_listed_but_not_on_disk_is_not_cached_buffer_27716818(tmp_path):
    """Buffer 27716818: a crop job read a dataset's files by range, so files.json listed each url (the harness
    lists a url at its first HEAD) and the cache held no file. The buffer took the cache for complete, staged
    with no hosts, and the stage job was refused the download."""
    url = "https://dandiarchive.s3.amazonaws.com/blobs/c08/b90/c08b9039"
    validators = {"etag": '"c6312cff"', "last_modified": "Tue, 06 May 2025 22:09:38 GMT", "size": 5629770}
    (tmp_path / "files.json").write_text(json.dumps({url: {"validators": validators, "source": "download"}}))
    assert raw_asset(tmp_path, [url])["state"] == "partial"
    entry = tmp_path / cache_key(url, validators)
    entry.symlink_to(tmp_path / "files.json")  # a link is no entry
    assert raw_asset(tmp_path, [url])["state"] == "partial"
    entry.unlink()
    entry.write_bytes(b"png")
    assert raw_asset(tmp_path, [url])["state"] == "ready"


def test_untrusted_files_json_is_reduced(tmp_path):
    (tmp_path / "files.json").write_text(
        json.dumps({"u1": {"validators": "x", "source": "s" * 99}, "u2": {"validators": {"size": "big"}}, "u3": 5})
    )
    files = raw_asset(tmp_path, [])["files"]
    assert files == {"u1": {"size": None, "source": "s" * 49 + "…"}, "u2": {"size": None, "source": None}}
    (tmp_path / "files.json").write_text("[1]")
    assert read_files_json(tmp_path) == {}
