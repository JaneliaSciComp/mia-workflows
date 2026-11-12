"""buffer-api's gateway to Temporal (spec webapp-actions §5, §7). The service is threads and blocking calls; the
Temporal client is asyncio. So one daemon thread runs an event loop, and each call here hands a coroutine to it.
Clients are made on first use, one per namespace: the service starts and serves reads with Temporal away."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import Future
from typing import Any

from .config import Config

log = logging.getLogger(__name__)
MIRROR_WORKFLOW = "MiaMirror"  # mia_data.mirror.MiaMirror, by name: the service imports no workflow code


def mirror_id(kind: str, number: int) -> str:
    return f"mirror/{kind}/{number}"


class Dispatcher:
    """Starts and signals workflows for the service. `targets` maps a subject kind to the (namespace, task queue)
    its workflows run on; `connect(namespace)` returns a Temporal client."""

    def __init__(self, connect: Callable[[str], Awaitable[Any]], targets: dict[str, tuple[str, str]]) -> None:
        self._connect, self.targets = connect, targets
        self._clients: dict[str, Any] = {}
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True, name="temporal-dispatcher").start()

    async def _client(self, namespace: str) -> Any:
        if namespace not in self._clients:
            self._clients[namespace] = await self._connect(namespace)
        return self._clients[namespace]

    def _submit[Result](self, work: Coroutine[Any, Any, Result]) -> Future[Result]:
        return asyncio.run_coroutine_threadsafe(work, self._loop)

    def kick_mirror(self, kind: str, number: int) -> Future[None]:
        """Ask the subject's mirror to copy what is new to its issue, starting it if it is not running. Returns
        at once; a kick that fails is logged, and the service kicks again when it next starts."""
        return self._submit(self._kick(kind, number))

    async def _kick(self, kind: str, number: int) -> None:
        namespace, task_queue = self.targets[kind]
        try:
            client = await self._client(namespace)
            await client.start_workflow(
                MIRROR_WORKFLOW, args=[kind, number], id=mirror_id(kind, number), task_queue=task_queue, start_signal="kick"
            )
        except Exception as error:  # never the caller's problem: the entry is written, the issue catches up later
            log.warning("mirror of %s #%d not kicked: %s", kind, number, error)


def dispatcher_for(cfg: Config) -> Dispatcher:
    """The gateway of a configured service: datasets on mia-data's namespace and queue, buffers and requests on
    this service's own."""
    from autodev.cli import _client

    async def connect(namespace: str) -> Any:
        temporal = cfg.temporal.model_copy(update={"namespace": namespace})
        return await _client(cfg.model_copy(update={"temporal": temporal}))

    own = (cfg.temporal.namespace, cfg.task_queue)
    datasets = (cfg.datasets_pipeline.namespace, cfg.datasets_pipeline.task_queue)
    return Dispatcher(connect, {"dataset": datasets, "buffer": own, "prep": own})
