"""buffer-api's gateway to Temporal, against a stand-in client: what is started or signalled where, and how a
failure reaches the caller."""

import asyncio
import logging
from types import SimpleNamespace

import pytest
from temporalio.service import RPCError, RPCStatusCode

from mia_buffers.dispatch import Dispatcher, DispatchError, Target, dispatcher_for
from mia_buffers.models import BufferState, PrepState
from mia_data.models import DatasetState, LabelEvent
from tests.test_buffers_config import make_cfg

TARGETS = {
    "dataset": Target("data-ns", "data-q", "MiaDataDataset", lambda number: f"data/{number}", DatasetState),
    "buffer": Target("buf-ns", "buf-q", "MiaBuffer", lambda number: f"buffer/{number}", BufferState),
    "prep": Target("buf-ns", "buf-q", "MiaPrep", lambda number: f"prep/{number}", PrepState),
}
EVENT = LabelEvent("go", "1800000000250", "page", "2027-01-15T08:00:00+00:00")


class FakeClient:
    """Records starts and signals; `open` holds the ids of the workflows that are running."""

    def __init__(self, namespace, calls, open_runs=(), pollers=("worker",)):
        self.namespace, self.calls, self.open, self.pollers = namespace, calls, set(open_runs), pollers
        self.workflow_service = SimpleNamespace(describe_task_queue=self._describe_task_queue)

    async def start_workflow(self, workflow, *one, args=(), id, task_queue, start_signal, start_signal_args=()):
        started = (workflow, [*one, *args], id, task_queue, start_signal, list(start_signal_args))
        self.calls.append((self.namespace, *started))

    def get_workflow_handle(self, workflow_id):
        return SimpleNamespace(
            signal=lambda name, *args: self._signal(workflow_id, name, args),
            terminate=lambda reason: self._signal(workflow_id, "terminate", (reason,)),
            describe=lambda: self._describe(workflow_id),
        )

    def _missing(self):
        return RPCError("workflow not found", RPCStatusCode.NOT_FOUND, b"")

    async def _signal(self, workflow_id, name, args):
        if workflow_id not in self.open:
            raise self._missing()
        self.calls.append((self.namespace, "signal", workflow_id, name, list(args)))

    async def _describe(self, workflow_id):
        if workflow_id not in self.open:
            raise self._missing()
        return SimpleNamespace(status=SimpleNamespace(name="RUNNING"))

    async def _describe_task_queue(self, request):
        return SimpleNamespace(pollers=list(self.pollers))


def gateway(calls, **client_kwargs):
    connected = []

    async def connect(namespace):
        connected.append(namespace)
        return FakeClient(namespace, calls, **client_kwargs)

    return Dispatcher(connect, TARGETS), connected


def test_kick_mirror_starts_or_signals_the_mirror_on_its_pipelines_queue():
    calls = []
    dispatcher, connected = gateway(calls)
    for kind, number in (("dataset", 234), ("buffer", 8), ("prep", 9)):
        dispatcher.kick_mirror(kind, number).result(5)
    assert calls == [
        ("data-ns", "MiaMirror", ["dataset", 234], "mirror/dataset/234", "data-q", "kick", []),
        ("buf-ns", "MiaMirror", ["buffer", 8], "mirror/buffer/8", "buf-q", "kick", []),
        ("buf-ns", "MiaMirror", ["prep", 9], "mirror/prep/9", "buf-q", "kick", []),
    ]
    assert connected == ["data-ns", "buf-ns"]  # one client per namespace, made on first use


def test_a_kick_that_fails_is_logged_and_the_next_one_connects_again(caplog):
    attempts = []

    async def connect(namespace):
        attempts.append(namespace)
        raise OSError("temporal is away")

    dispatcher = Dispatcher(connect, TARGETS)
    with caplog.at_level(logging.WARNING):
        assert dispatcher.kick_mirror("dataset", 1).result(5) is None
        dispatcher.kick_mirror("dataset", 1).result(5)
    assert "temporal is away" in caplog.text and attempts == ["data-ns", "data-ns"]


def test_send_starts_the_kinds_workflow_with_the_action_as_its_signal():
    calls = []
    dispatcher, _ = gateway(calls)
    dispatcher.send("dataset", 234, "go", EVENT)
    dispatcher.send("prep", 9, "go", EVENT)
    assert calls == [
        ("data-ns", "MiaDataDataset", [DatasetState(234)], "data/234", "data-q", "go", [EVENT]),
        ("buf-ns", "MiaPrep", [PrepState(9)], "prep/9", "buf-q", "go", [EVENT]),
    ]


def test_a_send_that_fails_or_takes_too_long_raises(monkeypatch):
    async def away(namespace):
        raise OSError("temporal is away")

    with pytest.raises(DispatchError, match="temporal is away"):
        Dispatcher(away, TARGETS).send("dataset", 1, "go", EVENT)

    async def slow(namespace):
        await asyncio.sleep(5)

    monkeypatch.setattr("mia_buffers.dispatch.SEND_LIMIT_S", 0.05)
    with pytest.raises(DispatchError, match="did not answer"):
        Dispatcher(slow, TARGETS).send("dataset", 1, "go", EVENT)


def test_a_plain_signal_reaches_a_running_workflow_and_says_when_there_is_none():
    calls = []
    dispatcher, _ = gateway(calls, open_runs={"data/234"})
    assert dispatcher.signal_running("dataset", 234, "cancel", EVENT) is True
    assert dispatcher.signal_running("dataset", 235, "cancel", EVENT) is False
    assert calls == [("data-ns", "signal", "data/234", "cancel", [EVENT])]
    assert dispatcher.run_status("dataset", 234) == "RUNNING" and dispatcher.run_status("dataset", 235) is None


def test_terminate_ends_a_running_workflow_and_says_when_there_is_none():
    calls = []
    dispatcher, _ = gateway(calls, open_runs={"buffer/8"})
    assert dispatcher.terminate("buffer", 8, "stopped from the page") is True
    assert dispatcher.terminate("prep", 9, "stopped from the page") is False
    assert calls == [("buf-ns", "signal", "buffer/8", "terminate", ["stopped from the page"])]


def test_health_names_each_queue_and_whether_a_worker_polls_it():
    dispatcher, _ = gateway([])
    assert dispatcher.health() == {"temporal": True, "queues": {"data-q": True, "buf-q": True}}
    dispatcher, _ = gateway([], pollers=())
    assert dispatcher.health() == {"temporal": True, "queues": {"data-q": False, "buf-q": False}}

    async def away(namespace):
        raise OSError("temporal is away")

    assert Dispatcher(away, TARGETS).health() == {"temporal": False, "queues": {}}


def test_health_watches_the_worker_of_every_service_it_is_told_of():
    async def connect(namespace):
        return FakeClient(namespace, [], pollers=("worker",) if namespace == "buf-ns" else ())

    watched = Dispatcher(connect, TARGETS, {"snippets": "snip-ns", "buf-q": "buf-ns"}).health()["queues"]
    assert watched == {"data-q": False, "buf-q": True, "snippets": False}  # a queue no workflow of ours runs on, too


def test_targets_come_from_the_config(tmp_path):
    cfg = make_cfg(tmp_path, datasets_pipeline={"namespace": "data-ns", "task_queue": "data-q"})
    targets = dispatcher_for(cfg).targets
    assert [(kind, target.namespace, target.task_queue, target.workflow) for kind, target in targets.items()] == [
        ("dataset", "data-ns", "data-q", "MiaDataDataset"),
        ("buffer", "mia-buffers", "mia-buffers", "MiaBuffer"),
        ("prep", "mia-buffers", "mia-buffers", "MiaPrep"),
    ]
    assert targets["dataset"].workflow_id(7) == "mia-data/o/data/issue-7"  # mia-data's own id for the dataset
    assert targets["buffer"].workflow_id(7) == cfg.workflow_id(7) and targets["prep"].make_state(7) == PrepState(7)
    assert make_cfg(tmp_path).datasets_pipeline.task_queue == "mia-data"  # the default: mia-data's own names
    assert dispatcher_for(make_cfg(tmp_path, workers={"snippets": "snip-ns"})).queues["snippets"] == "snip-ns"
