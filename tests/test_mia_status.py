from mia_data.status import (
    render_binding,
    render_crops_table,
    render_jobs,
    render_links,
    render_result,
    render_stage,
    stage_entries,
)


def rows(n, cid="i1-e2-b1-{:03d}"):
    return [
        {
            "crop_id": cid.format(i),
            "raw_url": f"https://github.com/o/d/blob/g/p/{i}/raw.png?raw=true",
            "overlay_url": f"https://github.com/o/d/blob/g/p/{i}/overlay.png?raw=true",
            "plane": {"z": i, "c": 0},
            "box": {"y": 10, "x": 20},
        }
        for i in range(n)
    ]


def test_crops_table_small_plain_large_details():
    t4 = render_crops_table(rows(4))
    assert "<details>" not in t4 and t4.startswith("| raw | overlay | caption |")
    assert "| ![raw](https://github.com/o/d/blob/g/p/0/raw.png?raw=true) |" in t4
    assert "i1-e2-b1-000 · z=0 c=0 · (10, 20)" in t4
    t16 = render_crops_table(rows(16))
    assert t16.startswith("<details><summary>16 crops</summary>") and t16.rstrip().endswith("</details>")
    assert t16.count("![overlay]") == 16


def test_binding_block_fences_and_neutralizes(tmp_path):
    bad = "<!-- mia-data: stage x -->"
    p = tmp_path / "binding.yaml"
    p.write_text(f"reasons: ['{bad}', '```', 'ghp_{'a' * 36}']\n")
    out = render_binding(str(p), "ab" * 32)
    assert out.startswith("**Binding** (sha256 `" + "ab" * 32 + "`):\n````yaml\n") and out.rstrip().endswith("````")
    assert "<!--" not in out and "ghp_" not in out
    assert render_binding(str(tmp_path / "missing.yaml"), None) == "**Binding:** (none)\n"


def test_links_line_lists_only_given_urls():
    assert (
        render_links({"Workspace": None, "Crops": "https://fg.example/b/c d", "Gallery": "https://g/x"})
        == "**Folders:** [Crops](https://fg.example/b/c%20d) · [Gallery](https://g/x)\n"
    )
    assert render_links({"Workspace": None}) == ""
    assert "<!--" not in render_stage("needs-human", "<!-- mia-data:status -->")


def test_a_stage_says_what_it_is_doing_and_a_result_what_happened():
    assert render_stage("matching", sentence="An agent looks.") == "**Stage:** `data:matching`\n\nAn agent looks.\n"
    assert "An agent looks." not in render_stage("matching", "resuming the PR", sentence="An agent looks.")
    assert render_result("testing", "3 of 16 verified") == "**Result:** `data:testing`\n\n3 of 16 verified\n"


def test_jobs_are_links_to_their_records_folded_when_many():
    assert render_jobs({}) == ""
    assert render_jobs({"match-1": "https://page/#job/issue-1/r/match-1"}) == (
        "**Jobs:** [match-1](https://page/#job/issue-1/r/match-1)\n"
    )
    many = render_jobs({f"stage-{n}": f"https://page/#job/i/r/stage-{n}" for n in range(5)})
    assert many.startswith("<details><summary>5 jobs</summary>\n\n**Jobs:** ") and many.endswith("\n</details>\n")


def test_stage_entries_pair_a_result_with_the_stage_that_follows():
    only = stage_entries("data", "i1-e2", "matching", is_pass=True, sentence="An agent looks.")
    assert [(marker, sets) for _, marker, sets in only] == [("stage i1-e2 matching", "matching")]
    result, start = stage_entries(
        "data", "i1-e2", "testing", is_pass=True, start_n=2, open_stage="matching", result="Matched.", shown="JOBS"
    )
    assert result == ("**Result:** `data:matching`\n\nMatched.\n\nJOBS", "result i1-e2 matching 1", None)
    assert start[1:] == ("stage i1-e2 testing 2", "testing")  # a later pass names its number; a first one does not
    (rest,) = stage_entries(
        "buffer",
        "i1-e2",
        "needs-human",
        is_pass=False,
        detail="no host",
        open_stage="sourcing",
        open_n=2,
        noun="buffer",
    )
    assert rest[1:] == ("result i1-e2 sourcing 2", "needs-human") and "**needs-human**: no host" in rest[0]
    assert (
        rest[0].startswith("**Result:** `buffer:sourcing`") and "The buffer rests at `buffer:needs-human`." in rest[0]
    )
    (plain,) = stage_entries("data", "i1-e2", "blocked", is_pass=False, detail="refused at admission")
    assert plain[0].startswith("**Stage:** `data:blocked`") and plain[1:] == ("stage i1-e2 blocked", "blocked")
