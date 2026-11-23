import json

from mia_core.assets import ASSET_STATES, cache_key, cache_missing, raw_asset, read_files_json


def test_a_raw_cache_on_disk_is_ready_whatever_it_holds(tmp_path):
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
    row = raw_asset(tmp_path)
    assert (
        row["state"] == "ready"  # at rest: nothing completes a raw cache, so none is partial
        and row["bytes"] == 7 + (tmp_path / "files.json").stat().st_size
        and row["files"]["https://h/b.tif"]["source"] == "drop"
    )
    assert raw_asset(tmp_path / "none")["state"] == "ready" and read_files_json(tmp_path / "none") == {}
    assert "evicted" in ASSET_STATES


def test_cache_missing_lists_the_urls_the_cache_does_not_hold(tmp_path):
    manifest = {
        "https://h/a.zip": {"validators": {"size": 7}, "source": "download"},
        "https://h/a.zip::m.png": {"validators": {"size": 7}, "source": "members"},
        "https://h/b.tif": {"validators": {"size": 3}, "source": "drop"},
    }
    (tmp_path / cache_key("https://h/b.tif", {"size": 3})).write_bytes(b"tif")
    assert cache_missing(tmp_path, manifest, ["https://h/a.zip::m.png", "https://h/b.tif"]) == (True, [])
    assert cache_missing(tmp_path, manifest, ["https://h/a.zip", "https://h/c.tif"]) == (
        False,
        ["https://h/a.zip", "https://h/c.tif"],  # a.zip is listed by a HEAD alone: no entry on disk
    )
    # the zip's entry (a HEAD) does not vouch for members no job cached
    assert cache_missing(tmp_path, manifest, ["https://h/a.zip::x/*.png"]) == (False, ["https://h/a.zip::x/*.png"])
    assert cache_missing(tmp_path, {}, []) == (False, []) and cache_missing(tmp_path, manifest, []) == (True, [])


def test_a_file_listed_but_not_on_disk_is_not_cached_buffer_27716818(tmp_path):
    """Buffer 27716818: a crop job read a dataset's files by range, so files.json listed each url (the harness
    lists a url at its first HEAD) and the cache held no file. Such a cache does not hold the url."""
    url = "https://dandiarchive.s3.amazonaws.com/blobs/c08/b90/c08b9039"
    validators = {"etag": '"c6312cff"', "last_modified": "Tue, 06 May 2025 22:09:38 GMT", "size": 5629770}
    manifest = {url: {"validators": validators, "source": "download"}}
    assert cache_missing(tmp_path, manifest, [url]) == (False, [url])
    entry = tmp_path / cache_key(url, validators)
    entry.symlink_to(tmp_path / "files.json")  # a link is no entry
    assert cache_missing(tmp_path, manifest, [url]) == (False, [url])
    entry.unlink()
    entry.write_bytes(b"png")
    assert cache_missing(tmp_path, manifest, [url]) == (True, [])


def test_untrusted_files_json_is_reduced(tmp_path):
    (tmp_path / "files.json").write_text(
        json.dumps({"u1": {"validators": "x", "source": "s" * 99}, "u2": {"validators": {"size": "big"}}, "u3": 5})
    )
    files = raw_asset(tmp_path)["files"]
    assert files == {"u1": {"size": None, "source": "s" * 49 + "…"}, "u2": {"size": None, "source": None}}
    (tmp_path / "files.json").write_text("[1]")
    assert read_files_json(tmp_path) == {}
