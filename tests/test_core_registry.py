import json
import logging

import httpx
import pytest

from mia_core import jobrecord
from mia_core.registry import Registry, RegistryError, best_effort, put_transfers_again, read_token, report_worker


def transport(seen, status=200, body=None):
    def handler(request):
        seen.append(request)
        return httpx.Response(status, json=body if body is not None else {"ok": True})

    return httpx.MockTransport(handler)


def test_bearer_header_and_quoting():
    seen = []
    reg = Registry("http://svc", "tok", transport=transport(seen))
    reg.put_asset("staged", "a/b c", {"x": 1})
    assert seen[0].headers["authorization"] == "Bearer tok" and seen[0].url.raw_path == b"/assets/staged/a%2Fb%20c"


def test_no_token_sends_no_header():
    seen = []
    rows = Registry("http://svc", None, transport=transport(seen, body={"datasets": [{"id": "a"}]})).datasets(
        "prepared"
    )
    assert "authorization" not in seen[0].headers and seen[0].url.params["state"] == "prepared"
    assert rows == [{"id": "a"}]


def test_catalogs_is_the_list():
    reg = Registry("http://svc", "t", transport=transport([], body={"catalogs": [{"ref": "r"}]}))
    assert reg.catalogs() == [{"ref": "r"}]


def test_errors_carry_status_and_message():
    reg = Registry("http://svc", "t", transport=transport([], 401, {"error": "unauthorized"}))
    with pytest.raises(RegistryError) as err:
        reg.put_dataset("rec", state="prepared")
    assert err.value.status == 401 and err.value.message == "unauthorized" and str(err.value) == "unauthorized"


def test_404_is_none_for_lookups():
    reg = Registry("http://svc", "t", transport=transport([], 404, {"error": "no"}))
    assert reg.dataset("rec") is None and reg.asset("staged", "k") is None


def test_asset_returns_the_data_field_and_204_is_none():
    assert Registry("http://svc", "t", transport=transport([], 200, {"data": {"a": 1}})).asset("k", "v") == {"a": 1}
    reg = Registry("http://svc", "t", transport=httpx.MockTransport(lambda request: httpx.Response(204)))
    assert reg.sync_datasets() is None


def test_put_records_body():
    seen = []
    Registry("http://svc", "t", transport=transport(seen)).put_records("r/1", [("a", {"x": 1})])
    assert seen[0].url.raw_path == b"/records/r%2F1"
    assert seen[0].read() == b'{"records":[{"path":"a","record":{"x":1}}]}'


def test_transport_error_is_registry_error():
    def boom(request):
        raise httpx.ConnectError("down")

    with pytest.raises(RegistryError) as err:
        Registry("http://svc", "t", transport=httpx.MockTransport(boom)).catalogs()
    assert err.value.status is None


def test_a_non_json_success_is_registry_error():
    html = httpx.MockTransport(lambda request: httpx.Response(200, text="<html>proxy login</html>"))
    with pytest.raises(RegistryError) as err:
        Registry("http://svc", "t", transport=html).dataset("rec")
    assert err.value.status == 200


def test_an_invalid_url_is_registry_error():
    with pytest.raises(RegistryError) as err:
        Registry("http://svc", "t", transport=transport([])).request("GET", "/x\x00")
    assert err.value.status is None


def test_best_effort_logs_and_continues(caplog):
    def fail():
        raise RegistryError(None, "down")

    assert best_effort(fail, "dataset rec", logging.getLogger("t")) is False
    assert "dataset rec: registry write skipped: down" in caplog.text
    assert best_effort(lambda: None, "x", logging.getLogger("t")) is True


def test_read_token(tmp_path):
    (tmp_path / "t").write_text("abc\n")
    assert read_token(tmp_path / "t") == "abc"
    (tmp_path / "empty").write_text("\n")
    for name in ("missing", "empty"):
        with pytest.raises(RegistryError):
            read_token(tmp_path / name)


def test_assets_sync_assets_and_locks():
    seen = []
    reg = Registry("http://svc", "t", transport=transport(seen, body={"kind": "staged", "assets": [], "bytes": 0}))
    assert reg.assets("staged", state="ready") == {"kind": "staged", "assets": [], "bytes": 0}
    assert (
        seen[0].method == "GET"
        and seen[0].url.path == "/assets/staged"
        and dict(seen[0].url.params) == {"state": "ready"}
    )
    reg.assets("raw", record="rec")
    assert dict(seen[1].url.params) == {"record": "rec"}
    reg.sync_assets()
    assert seen[2].method == "POST" and seen[2].url.path == "/sync/assets"
    assert seen[2].extensions["timeout"]["read"] == 600  # a sync walks a whole root: minutes, not 30 s
    reg.sync_datasets()
    assert seen[3].url.path == "/sync/datasets" and seen[3].extensions["timeout"]["read"] == 600
    reg.assets("raw")
    assert seen[4].extensions["timeout"]["read"] == 30  # every other call keeps the client's


def test_buffer_lifecycle_calls():
    seen = []
    reply = {"records": [{"path": "a.yaml", "record": {"id": "a"}}], "buffers": [{"id": "b"}], "datasets": [{"r": 1}]}
    reg = Registry("http://svc", "t", transport=transport(seen, body=reply))
    assert reg.records("f" * 40) == [("a.yaml", {"id": "a"})]
    assert seen[0].method == "GET" and seen[0].url.path == "/records" and seen[0].url.params["ref"] == "f" * 40
    reg.put_buffer_dataset("b 1", "rec/x", "fetching", job="j1")
    assert seen[1].method == "PUT" and seen[1].url.raw_path == b"/buffers/b%201/datasets/rec%2Fx"
    assert json.loads(seen[1].read()) == {"status": "fetching", "reason": "", "job": "j1"}
    reg.event("b", "ready", issue=2, detail="2 of 2")
    assert seen[2].method == "POST" and seen[2].url.path == "/buffers/b/events"
    assert json.loads(seen[2].read()) == {"kind": "ready", "issue": 2, "detail": "2 of 2"}
    assert reg.buffers() == [{"id": "b"}] and seen[3].url.path == "/buffers"
    assert reg.buffer_datasets("b") == [{"r": 1}] and seen[4].url.path == "/buffers/b/datasets"
    assert reg.retire("b") == reply and seen[5].method == "DELETE" and seen[5].url.path == "/buffers/b/refs"
    assert reg.status("b") == reply and seen[6].method == "GET" and seen[6].url.path == "/buffers/b/status"
    assert all(request.headers["authorization"] == "Bearer t" for request in seen)


def test_put_keeper():
    seen = []
    reg = Registry("http://svc", "t", transport=transport(seen, body={"evicted": [], "at": 1.0}))
    assert reg.put_keeper({"evicted": []}) == {"evicted": [], "at": 1.0}
    assert seen[0].method == "PUT" and seen[0].url.path == "/keeper" and json.loads(seen[0].read()) == {"evicted": []}
    assert seen[0].headers["authorization"] == "Bearer t"


def test_assessment_budget_and_volume_paths():
    seen = []
    reg = Registry("http://svc", "t", transport=transport(seen, body={"ok": True, "assessments": [], "volumes": []}))
    reg.put_assessment({"id": "a1", "gate": "crops"})
    reg.assessments(subject_kind="artifact", subject_ref="art1")
    reg.assessments(by="judge", before="a0", limit=5)
    reg.reserve("o/r#7", "job-1", "match", 5.0, {"max_usd": 10})
    reg.settle("o/r#7", "job-1", usd=1.0, fetched_bytes=3)
    reg.delete_volume("b", "rec/v0.zarr")
    reg.volumes("b")
    assert [(r.method, r.url.raw_path.decode()) for r in seen] == [
        ("PUT", "/assessments/a1"),
        ("GET", "/assessments?subject_kind=artifact&subject_ref=art1"),
        ("GET", "/assessments?by=judge&before=a0&limit=5"),
        ("POST", "/budgets/o%2Fr%237/reserve"),
        ("POST", "/budgets/o%2Fr%237/settle"),
        ("DELETE", "/buffers/b/volumes/rec%2Fv0.zarr"),
        ("GET", "/buffers/b/volumes"),
    ]
    assert json.loads(seen[4].content) == {"reservation_id": "job-1", "usd": 1.0, "fetched_bytes": 3, "staged_bytes": 0}


def test_subject_calls():
    seen = []
    reg = Registry("http://svc", "tok", transport=transport(seen))
    reg.adopt_subject("buffer", 8, "EM pool/2", stage="ready", data={"spec": "s"})
    reg.subject("buffer", "EM pool/2")
    assert (seen[0].method, seen[0].url.raw_path) == ("PUT", b"/subjects/buffer/8")
    assert json.loads(seen[0].content) == {"key": "EM pool/2", "stage": "ready", "data": {"spec": "s"}}
    assert (seen[1].method, seen[1].url.raw_path) == ("GET", b"/subjects/buffer/EM%20pool%2F2")
    assert Registry("http://svc", "t", transport=transport([], 404, {"error": "no"})).subject("buffer", "x") is None


def test_timeline_calls():
    seen = []
    reg = Registry("http://svc", "tok", transport=transport(seen))
    reg.append_entry("dataset", 7, body="text", marker="m", round_id="i7-e1", stage="testing")
    reg.append_entry("prep", 9, stage="done")
    reg.timeline("buffer", 8)
    reg.mark_mirrored("buffer", 8, 3, "https://x/c")
    assert [(request.method, request.url.path) for request in seen] == [
        ("POST", "/timeline/dataset/7"),
        ("POST", "/timeline/prep/9"),
        ("GET", "/timeline/buffer/8"),
        ("PUT", "/timeline/buffer/8/3"),
    ]
    assert json.loads(seen[0].content) == {"body": "text", "marker": "m", "round": "i7-e1", "stage": "testing"}
    assert json.loads(seen[1].content) == {"body": "", "marker": "", "round": None, "stage": "done"}
    assert json.loads(seen[3].content) == {"comment_url": "https://x/c"}
    assert Registry("http://svc", "t", transport=transport([], 404, {"error": "no"})).timeline("buffer", 1) is None


def test_action_calls():
    seen = []
    reg = Registry("http://svc", "tok", transport=transport(seen))
    reg.action(1800000000250)
    reg.refuse_action(1800000000250, "a round is running")
    reg.set_subject_issue("dataset", 28000000, 240)
    reg.send_action("/datasets/rec a/actions", "go", key="p9-e1/rec a", actor="prep:9", root="o/b#9")
    assert [(request.method, request.url.raw_path) for request in seen] == [
        ("GET", b"/actions/1800000000250"),
        ("PUT", b"/actions/1800000000250"),
        ("PUT", b"/subjects/dataset/28000000/issue"),
        ("POST", b"/datasets/rec%20a/actions"),
    ]
    assert json.loads(seen[1].content) == {"state": "refused", "detail": "a round is running"}
    assert json.loads(seen[2].content) == {"issue": 240}
    assert json.loads(seen[3].content) == {"action": "go", "key": "p9-e1/rec a", "actor": "prep:9", "root": "o/b#9"}
    assert seen[3].headers["content-type"] == "application/json"
    assert Registry("http://svc", "t", transport=transport([], 404, {"error": "no"})).action(1) is None


def test_the_code_digest_changes_with_the_packages_alone(tmp_path):
    import subprocess

    from mia_core.checkout import PACKAGES, code_digest, head_commit

    assert head_commit(tmp_path) is None and code_digest(tmp_path) is None  # no checkout
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    git = ["git", "-C", str(tmp_path), "-c", "user.name=t", "-c", "user.email=t@example.org"]

    def commit(file, text):
        (tmp_path / file).parent.mkdir(exist_ok=True)
        (tmp_path / file).write_text(text)
        subprocess.run([*git, "add", file], check=True)
        subprocess.run([*git, "commit", "-q", "-m", file], check=True)
        return head_commit(tmp_path), code_digest(tmp_path)

    for package in PACKAGES:
        first = commit(f"{package}/code.py", "one")
    assert first[0] is not None and len(first[0]) == 40 and first[1] is not None
    docs = commit("docs/notes.md", "words")
    assert docs[0] != first[0] and docs[1] == first[1]  # a commit that touches no package: the same code
    assert commit("mia_buffers/ui/app.js", "page")[1] == first[1]  # the page is served from disk: no worker loads it
    assert commit("mia_data/code.py", "two")[1] != first[1]


def test_a_worker_reports_its_code_until_the_registry_takes_it():
    # buffer-api may come up after the worker (a restart of both): the report is sent again, with the code read
    # when the worker started
    import asyncio

    from mia_core.registry import report_worker

    calls = []

    class Down:
        def put_worker(self, service, commit, code):
            calls.append((service, commit, code))
            if len(calls) < 3:
                raise RegistryError(None, "connection refused")

    asyncio.run(report_worker(Down(), "mia-data", logging.getLogger("test"), retry_s=0))
    assert len(calls) == 3 and len(set(calls)) == 1 and calls[0][0] == "mia-data"


ROW = {
    "minute": 100,
    "host": "a.org",
    "requests": 2,
    "connection_seconds": 3.5,
    "peak": 1,
    "bytes": 10,
    "answers": {"206": 2},
    "recovered": {},
    "final": {},
    "asked_wait_seconds": 0.0,
    "backoff_seconds": 0.0,
}


def test_put_transfers():
    seen = []
    reg = Registry("http://svc", "t", transport=transport(seen, body={"job": "r1/stage-1", "rows": 1}))
    assert reg.put_transfers("r1/stage-1", [ROW], "buffers") == {"job": "r1/stage-1", "rows": 1}
    assert seen[0].method == "PUT" and seen[0].url.path == "/transfers/r1/stage-1"
    assert json.loads(seen[0].read()) == {"service": "buffers", "rows": [ROW]}


def _records_with_transfers(root):
    jobrecord._store(jobrecord.record_path(root, "r1/a"), {"service": "dep", "finished_at": "x", "transfers": [ROW]})
    jobrecord._store(jobrecord.record_path(root, "r1/b"), {"service": "dep", "finished_at": "x"})


async def test_a_starting_worker_puts_its_records_transfers_again(tmp_path):
    root, seen = tmp_path / "work", []
    _records_with_transfers(root)
    reg = Registry("http://svc", "t", transport=transport(seen, body={}))
    assert await put_transfers_again(reg, root, logging.getLogger("t")) == 1
    assert seen[0].url.path == "/transfers/r1/a" and json.loads(seen[0].read()) == {"service": "dep", "rows": [ROW]}
    down = Registry("http://svc", "t", transport=transport([], status=503))
    assert await put_transfers_again(down, root, logging.getLogger("t")) == 0  # logged, not raised


async def test_the_start_up_report_puts_transfers_again_once_the_commit_is_written(tmp_path):
    root, seen = tmp_path / "work", []
    _records_with_transfers(root)
    reg = Registry("http://svc", "t", transport=transport(seen, body={}))
    await report_worker(reg, "dep", logging.getLogger("t"), work_root=root)
    assert [request.url.path for request in seen][-1] == "/transfers/r1/a"
    assert seen[0].url.path.startswith("/workers") and len(seen) == 2
