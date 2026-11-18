"""What Temporal knows of the workers on a task queue, and how a worker stops and says it is alive."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

# What a worker that is told to stop (SIGTERM, `mia down`) gives its running activities to finish. One still
# running then is cancelled and handed back to Temporal, which gives it to the next worker at once; a worker
# that is killed leaves each to its timeout.
DRAIN = timedelta(seconds=10)
BEAT_S = 20.0  # how often an activity under `heartbeats` says it is alive
BEAT_TIMEOUT = timedelta(
    minutes=2
)  # the `heartbeat_timeout` to give such an activity: a dead worker is noticed in this


async def until_stopped(*workers: Any) -> None:
    """Wait until this task is cancelled (the worker process's SIGTERM), then stop the workers together: all
    take no new task from that moment and share one DRAIN. Left to `async with`, each would drain after the
    other, and the last would go on starting activities meanwhile."""
    try:
        await asyncio.Event().wait()
    finally:
        await asyncio.gather(*(worker.shutdown() for worker in workers))


def heartbeats[**Args, Result](run: Callable[Args, Awaitable[Result]]) -> Callable[Args, Awaitable[Result]]:
    """An activity that tells Temporal it is alive while it runs. With `heartbeat_timeout=BEAT_TIMEOUT` where the
    workflow calls it, a worker that died mid-activity is noticed in that time and not at the activity's own
    timeout. Called outside an activity (a test, a command line) it runs as it is."""

    @functools.wraps(run)
    async def wrapper(*args: Args.args, **kwargs: Args.kwargs) -> Result:
        from temporalio import activity

        if not activity.in_activity():
            return await run(*args, **kwargs)

        async def beat() -> None:
            while True:
                activity.heartbeat()
                await asyncio.sleep(BEAT_S)

        beating = asyncio.create_task(beat())
        try:
            return await run(*args, **kwargs)
        finally:
            beating.cancel()

    wrapper.__dict__["beats"] = True  # the tests hold each such activity against the `beats` of its call
    return wrapper


async def last_poll(client: Any, namespace: str, task_queue: str) -> float | None:
    """When a worker last polled the workflow task queue, in Unix seconds by Temporal's clock; None when Temporal
    remembers no poller (it forgets one about five minutes after its last poll)."""
    from temporalio.api.enums.v1 import TaskQueueType
    from temporalio.api.taskqueue.v1 import TaskQueue
    from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

    answer = await client.workflow_service.describe_task_queue(
        DescribeTaskQueueRequest(
            namespace=namespace,
            task_queue=TaskQueue(name=task_queue),
            task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
        )
    )
    polls = [poller.last_access_time.seconds + poller.last_access_time.nanos / 1e9 for poller in answer.pollers]
    return max(polls, default=None)
