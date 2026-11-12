"""buffer-api's gateway to Temporal (spec webapp-actions §5, §7). The service is threads and blocking calls; the
Temporal client is asyncio. So one daemon thread runs an event loop, and each call here hands a coroutine to it.
Clients are made on first use, one per namespace: the service starts and serves reads with Temporal away."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass
from typing import Any

from mia_data.models import DatasetState, LabelEvent

from .config import Config
from .models import BufferState, PrepState

log = logging.getLogger(__name__)
MIRROR_WORKFLOW = "MiaMirror"  # mia_data.workflows.MiaMirror, by name: the service imports no workflow code
SEND_LIMIT_S = 10.0  # how long a click waits for Temporal before its action is marked failed


class DispatchError(RuntimeError):
    """Temporal did not take what the service sent: unreachable, too slow, or it refused."""


@dataclass(frozen=True)
class Target:
    """Where the workflows of one subject kind run and how one is started."""

    namespace: str
    task_queue: str
    workflow: str  # the workflow type, by name
    workflow_id: Callable[[int], str]
    make_state: Callable[[int], Any]  # the workflow's first argument for a subject number


def mirror_id(kind: str, number: int) -> str:
    return f"mirror/{kind}/{number}"


class Dispatcher:
    """Starts and signals workflows for the service. `targets` maps a subject kind to its `Target`;
    `connect(namespace)` returns a Temporal client."""

    def __init__(self, connect: Callable[[str], Awaitable[Any]], targets: dict[str, Target]) -> None:
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

    def _wait[Result](self, work: Coroutine[Any, Any, Result]) -> Result:
        """Run `work` on the loop and wait for it, at most SEND_LIMIT_S; any failure is a DispatchError."""
        future = self._submit(work)
        try:
            return future.result(SEND_LIMIT_S)
        except FutureTimeout:
            future.cancel()
            raise DispatchError(f"Temporal did not answer within {SEND_LIMIT_S:g} seconds") from None
        except Exception as error:
            raise DispatchError(f"{type(error).__name__}: {error}") from error

    # --- the mirror -----------------------------------------------------------------------------

    def kick_mirror(self, kind: str, number: int) -> Future[None]:
        """Ask the subject's mirror to copy what is new to its issue, starting it if it is not running. Returns
        at once; a kick that fails is logged, and the service kicks again when it next starts."""
        return self._submit(self._kick(kind, number))

    async def _kick(self, kind: str, number: int) -> None:
        target = self.targets[kind]
        try:
            client = await self._client(target.namespace)
            await client.start_workflow(
                MIRROR_WORKFLOW,
                args=[kind, number],
                id=mirror_id(kind, number),
                task_queue=target.task_queue,
                start_signal="kick",
            )
        except Exception as error:  # never the caller's problem: the entry is written, the issue catches up later
            log.warning("mirror of %s #%d not kicked: %s", kind, number, error)

    # --- actions --------------------------------------------------------------------------------

    def send(self, kind: str, number: int, signal: str, event: LabelEvent) -> None:
        """Deliver an action as the workflow's signal of that name, starting the subject's workflow if it is not
        running: what the poller's label delivery did. Blocks until Temporal has it; DispatchError if not."""
        self._wait(self._send(kind, number, signal, event))

    async def _send(self, kind: str, number: int, signal: str, event: LabelEvent) -> None:
        target = self.targets[kind]
        client = await self._client(target.namespace)
        await client.start_workflow(
            target.workflow,
            target.make_state(number),
            id=target.workflow_id(number),
            task_queue=target.task_queue,
            start_signal=signal,
            start_signal_args=[event],
        )

    def signal_running(self, kind: str, number: int, signal: str, *args: Any) -> bool:
        """Signal the subject's workflow only if it is running; False when no run is open (nothing is started)."""
        return self._wait(self._signal_running(kind, number, signal, args))

    async def _signal_running(self, kind: str, number: int, signal: str, args: tuple) -> bool:
        from temporalio.service import RPCError, RPCStatusCode

        target = self.targets[kind]
        client = await self._client(target.namespace)
        try:
            await client.get_workflow_handle(target.workflow_id(number)).signal(signal, *args)
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:  # never started, or its last run is over
                return False
            raise
        return True

    def run_status(self, kind: str, number: int) -> str | None:
        """The Temporal status of the subject's latest run (`RUNNING`, `COMPLETED`, `FAILED`, `TERMINATED`, ...);
        None when it never ran."""
        return self._wait(self._run_status(kind, number))

    async def _run_status(self, kind: str, number: int) -> str | None:
        from temporalio.service import RPCError, RPCStatusCode

        target = self.targets[kind]
        client = await self._client(target.namespace)
        try:
            description = await client.get_workflow_handle(target.workflow_id(number)).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                return None
            raise
        return description.status.name if description.status else None

    def health(self) -> dict:
        """{temporal: whether it answers, queues: {task queue: whether a worker polls it}} for the page's banner."""
        try:
            return {"temporal": True, "queues": self._wait(self._polled())}
        except DispatchError as error:
            log.warning("health: %s", error)
            return {"temporal": False, "queues": {}}

    async def _polled(self) -> dict[str, bool]:
        from temporalio.api.enums.v1 import TaskQueueType
        from temporalio.api.taskqueue.v1 import TaskQueue
        from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

        polled: dict[str, bool] = {}
        for target in self.targets.values():
            if target.task_queue in polled:
                continue
            client = await self._client(target.namespace)
            answer = await client.workflow_service.describe_task_queue(
                DescribeTaskQueueRequest(
                    namespace=target.namespace,
                    task_queue=TaskQueue(name=target.task_queue),
                    task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                )
            )
            polled[target.task_queue] = bool(answer.pollers)
        return polled


def dispatcher_for(cfg: Config) -> Dispatcher:
    """The gateway of a configured service: datasets on mia-data's namespace and queue under mia-data's own
    workflow ids, buffers and requests on this service's."""
    from autodev.cli import _client

    async def connect(namespace: str) -> Any:
        temporal = cfg.temporal.model_copy(update={"namespace": namespace})
        return await _client(cfg.model_copy(update={"temporal": temporal}))

    data, own = cfg.datasets_pipeline, (cfg.temporal.namespace, cfg.task_queue)
    datasets_repo = cfg.github.datasets_repo
    return Dispatcher(
        connect,
        {
            "dataset": Target(
                data.namespace,
                data.task_queue,
                "MiaDataDataset",
                lambda number: f"mia-data/{datasets_repo}/issue-{number}",  # mia_data.config.Config.workflow_id
                DatasetState,
            ),
            "buffer": Target(*own, "MiaBuffer", cfg.workflow_id, BufferState),
            "prep": Target(*own, "MiaPrep", cfg.prep_workflow_id, PrepState),
        },
    )
