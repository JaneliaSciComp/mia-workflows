"""How an activity says it is alive, and what a worker gives its activities when it is told to stop."""

import asyncio
import re
from datetime import timedelta
from pathlib import Path

from temporalio.testing import ActivityEnvironment

from mia_core import temporal
from mia_core.temporal import BEAT_TIMEOUT, DRAIN, heartbeats


async def test_an_activity_under_heartbeats_beats_while_it_runs_and_stops_when_it_ends(monkeypatch):
    """A worker that dies under a job submit is noticed at the heartbeat timeout, not at the submit's own ten
    minutes (2026-11-18: four datasets waited those out). The beats cover the whole activity and end with it."""
    monkeypatch.setattr(temporal, "BEAT_S", 0.01)

    @heartbeats
    async def submit(key: str) -> str:
        await asyncio.sleep(0.05)
        return f"job of {key}"

    env, beats = ActivityEnvironment(), []
    env.on_heartbeat = lambda *details: beats.append(details)
    assert await env.run(submit, "crop-1") == "job of crop-1"
    during = len(beats)
    assert during >= 3
    await asyncio.sleep(0.03)
    assert len(beats) == during  # no beat outlives the activity


async def test_an_activity_that_fails_under_heartbeats_stops_beating_and_raises():
    @heartbeats
    async def submit() -> str:
        raise ValueError("refused")

    env, beats = ActivityEnvironment(), []
    env.on_heartbeat = lambda *details: beats.append(details)
    try:
        await env.run(submit)
    except ValueError as error:
        assert str(error) == "refused"
    else:
        raise AssertionError("the failure was swallowed")
    await asyncio.sleep(0.01)
    assert len(beats) <= 1  # it failed before the first beat, or right after it


async def test_heartbeats_outside_an_activity_runs_the_function_as_it_is():
    @heartbeats
    async def submit(key: str) -> str:
        return key

    assert await submit("k") == "k"


def test_the_beat_is_well_inside_its_timeout_and_the_drain_inside_a_restart():
    assert timedelta(seconds=temporal.BEAT_S) * 3 <= BEAT_TIMEOUT
    assert timedelta(0) < DRAIN <= timedelta(seconds=30)


def test_an_activity_heartbeats_exactly_when_its_workflow_gives_it_a_heartbeat_timeout():
    """The two go together: a heartbeat timeout on an activity that does not beat fails it every two minutes,
    and beats without a timeout notice nothing."""
    from mia_buffers.activities import Activities as BufferActivities
    from mia_data.activities import Activities as DataActivities

    root = Path(__file__).parent.parent
    for package, activities in (("mia_data", DataActivities), ("mia_buffers", BufferActivities)):
        source = (root / package / "workflows.py").read_text()
        called = set(re.findall(r'_act\(\s*"(\w+)",(?:(?!_act\().)*?beats=True', source, re.S))
        beating = {name for name in dir(activities) if getattr(getattr(activities, name), "beats", False)}
        assert called == beating and called, package
