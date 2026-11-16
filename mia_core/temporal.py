"""What Temporal knows of the workers on a task queue."""

from __future__ import annotations

from typing import Any


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
