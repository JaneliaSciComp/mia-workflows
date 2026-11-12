"""buffer-api's gateway to Temporal, against a stand-in client: what is started where, and that a failure stays
a log line."""

import logging

from mia_buffers.dispatch import Dispatcher, dispatcher_for
from tests.test_buffers_config import make_cfg

TARGETS = {"dataset": ("data-ns", "data-q"), "buffer": ("buf-ns", "buf-q"), "prep": ("buf-ns", "buf-q")}


class FakeClient:
    def __init__(self, namespace, started):
        self.namespace, self.started = namespace, started

    async def start_workflow(self, workflow, *, args, id, task_queue, start_signal):
        self.started.append((self.namespace, workflow, args, id, task_queue, start_signal))


def test_kick_mirror_starts_or_signals_the_mirror_on_its_pipelines_queue():
    started, connected = [], []

    async def connect(namespace):
        connected.append(namespace)
        return FakeClient(namespace, started)

    dispatcher = Dispatcher(connect, TARGETS)
    for kind, number in (("dataset", 234), ("buffer", 8), ("prep", 9)):
        dispatcher.kick_mirror(kind, number).result(5)
    assert started == [
        ("data-ns", "MiaMirror", ["dataset", 234], "mirror/dataset/234", "data-q", "kick"),
        ("buf-ns", "MiaMirror", ["buffer", 8], "mirror/buffer/8", "buf-q", "kick"),
        ("buf-ns", "MiaMirror", ["prep", 9], "mirror/prep/9", "buf-q", "kick"),
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


def test_targets_come_from_the_config(tmp_path):
    cfg = make_cfg(tmp_path, datasets_pipeline={"namespace": "data-ns", "task_queue": "data-q"})
    assert dispatcher_for(cfg).targets == {
        "dataset": ("data-ns", "data-q"),
        "buffer": ("mia-buffers", "mia-buffers"),
        "prep": ("mia-buffers", "mia-buffers"),
    }
    assert make_cfg(tmp_path).datasets_pipeline.task_queue == "mia-data"  # the default: mia-data's own names
