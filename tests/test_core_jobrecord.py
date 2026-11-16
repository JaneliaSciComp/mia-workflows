import gzip
import json
import subprocess

import pytest

from mia_core import jobrecord
from mia_core.jobrecord import (
    TRANSCRIPT_FILE,
    address,
    keep_result,
    record_path,
    reduce_stream,
    write_finished,
    write_submitted,
)

SECRET = "sk-ant-api03-abcdefghij"


def line(kind, **fields):
    return json.dumps({"type": kind, **fields})


def assistant(*blocks, parent=None):
    return line("assistant", message={"content": list(blocks)}, parent_tool_use_id=parent)


STREAM = "\n".join(
    [
        line("system", subtype="hook_started", hook_name="x"),
        line("system", subtype="init", model="claude-opus"),
        assistant({"type": "thinking", "thinking": "look at the record first"}),
        assistant({"type": "tool_use", "id": "t1", "name": "Read", "input": {"file_path": "record.yaml"}}),
        line("user", message={"content": [{"type": "tool_result", "tool_use_id": "t1", "content": "TOOL-OUTPUT-1"}]}),
        assistant({"type": "tool_use", "id": "t2", "name": "Write", "input": {"content": "x" * 5000}}),
        line(
            "user",
            message={"content": [{"type": "tool_result", "tool_use_id": "t2", "is_error": True, "content": "OUT-2"}]},
        ),
        line("rate_limit_event", rate_limit_info={}),
        "not json at all",
        '["a list"]',
        assistant({"type": "text", "text": f"the key is {SECRET}"}, parent="t9"),
        line("result", subtype="success", result="done", total_cost_usd=0.25, num_turns=3, duration_ms=1200),
    ]
)


def test_reduce_stream_keeps_the_conversation_and_drops_tool_output():
    reduced = reduce_stream(STREAM)
    assert (reduced["model"], reduced["cost_usd"], reduced["num_turns"]) == ("claude-opus", 0.25, 3)
    roles = [turn["role"] for turn in reduced["turns"]]
    assert roles == ["thinking", "tool", "tool", "assistant"]
    read, write, said = reduced["turns"][1:]
    assert read == {"role": "tool", "name": "Read", "input": {"file_path": "record.yaml"}, "error": False}
    assert write["error"] is True
    assert write["input"]["content"] == "x" * 2000 + "… (3000 more characters)"
    assert said["sub"] is True
    assert "TOOL-OUTPUT-1" not in json.dumps(reduced) and "OUT-2" not in json.dumps(reduced)


@pytest.mark.parametrize("text", ["", "{", '{"type": "assistant", "message": 3}\n{"type": "user"}', "\x00\x01"])
def test_reduce_stream_survives_a_broken_stream(text):
    assert reduce_stream(text)["turns"] == []


def test_address_is_the_workspace_under_work_root(tmp_path):
    root = tmp_path / "work"
    assert address(root, root / "issue-3" / "i3-e4" / "match-1") == "issue-3/i3-e4/match-1"
    assert address(root, tmp_path / "elsewhere" / "match-1") is None
    assert address(root, root) is None and address(root, root / "_jobs" / "x") is None


def _read(path):
    return json.loads(gzip.decompress(path.read_bytes()))


def test_both_halves_make_one_record(tmp_path, monkeypatch):
    monkeypatch.setattr(jobrecord, "lsf_usage", lambda job_id: {"max_mem": "2 Gbytes"})
    root, workspace = tmp_path / "work", tmp_path / "work" / "issue-3" / "i3-e4" / "match-1"
    (workspace / ".csub" / "jobs" / "777").mkdir(parents=True)
    (workspace / ".csub" / "jobs" / "777" / "stdout").write_text("out-tail")
    (workspace / "prompt.md").write_text("Match this dataset.")
    (workspace / "claude.jsonl").write_text(STREAM)
    request = {"command": "claude -p …", "cpus": 2, "env": {"ANTHROPIC_API_KEY": SECRET}, "shell": True}
    write_submitted(
        root,
        workspace,
        service="mia-data",
        key="k1",
        job_id="777",
        request=request,
        submit={"queue": "local", "estimated_max_cost_usd": 0.1},
        prompt_file="prompt.md",
        transcript_file="claude.jsonl",
    )
    path = record_path(root, "issue-3/i3-e4/match-1")
    first = _read(path)
    assert first["schema"] == 1 and first["request"]["env"] == ["ANTHROPIC_API_KEY"] and "outcome" not in first
    assert first["submit"]["queue"] == "local" and first["request"]["command"] == "claude -p …"
    write_finished(root, workspace, job_id="777", outcome={"state": "DONE", "exit_code": 0})
    record = _read(path)
    assert record["outcome"] == {"state": "DONE", "exit_code": 0} and record["usage"] == {"max_mem": "2 Gbytes"}
    assert record["logs"] == {"stdout": "out-tail", "stderr": ""}
    assert record["agent"]["prompt"] == "Match this dataset." and len(record["agent"]["turns"]) == 4
    assert record["agent"]["turns"][-1]["text"] == "the key is [redacted]"
    assert SECRET.encode() not in gzip.decompress(path.read_bytes())


def test_a_cut_off_stream_still_gets_its_outcome(tmp_path, monkeypatch):
    monkeypatch.setattr(jobrecord, "lsf_usage", lambda job_id: None)
    root, workspace = tmp_path / "work", tmp_path / "work" / "j"
    workspace.mkdir(parents=True)
    (workspace / "claude.jsonl").write_text(STREAM[: STREAM.index("rate_limit_event")])
    write_submitted(root, workspace, service="s", key="k", job_id="5", request={}, transcript_file="claude.jsonl")
    write_finished(root, workspace, job_id="5", outcome={"state": "EXIT", "exit_code": 137})
    record = _read(record_path(root, "j"))
    assert record["outcome"]["state"] == "EXIT" and "usage" not in record
    assert [turn["role"] for turn in record["agent"]["turns"]] == ["thinking", "tool", "tool"]


def test_the_end_without_a_first_half_writes_what_it_has(tmp_path, monkeypatch):
    monkeypatch.setattr(jobrecord, "lsf_usage", lambda job_id: None)
    root, workspace = tmp_path / "work", tmp_path / "work" / "j"
    workspace.mkdir(parents=True)
    write_finished(root, workspace, job_id="5", outcome={"state": "DONE", "exit_code": 0})
    record = _read(record_path(root, "j"))
    assert record["outcome"]["state"] == "DONE" and "agent" not in record and "request" not in record


def test_nothing_is_written_outside_work_root_and_nothing_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(jobrecord, "lsf_usage", lambda job_id: None)
    root = tmp_path / "work"
    write_submitted(root, tmp_path / "elsewhere", service="s", key="k", job_id="5", request={})
    write_finished(root, tmp_path / "elsewhere", job_id="5", outcome={})
    write_submitted(None, tmp_path / "x", service="s", key="k", job_id="5", request={})
    assert not (root / "_jobs").exists()
    (root).mkdir()
    (root / "_jobs").write_text("a file where the directory should be")
    write_submitted(root, root / "j", service="s", key="k", job_id="5", request={})  # logged, not raised
    write_finished(root, root / "j", job_id="5", outcome={})


def test_lsf_usage_takes_only_a_numeric_job_id():
    assert jobrecord.lsf_usage("--help") is None and jobrecord.lsf_usage("job-k1") is None


def test_keep_result_copies_the_last_line_and_keeps_the_exit_status(tmp_path):
    agent = f"(printf 'first\\nsecond\\n{{\"result\": 1}}\\n'; exit 7) > {TRANSCRIPT_FILE}"
    script = f'{agent}\n{keep_result("claude.json")}\nrc=$?\necho "$rc" > exit_code\n'
    subprocess.run(["bash", "-c", script], cwd=tmp_path, check=True)
    assert (tmp_path / "exit_code").read_text().strip() == "7"
    assert json.loads((tmp_path / "claude.json").read_text()) == {"result": 1}
