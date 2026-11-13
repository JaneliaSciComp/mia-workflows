"""What changed, as a feed the page follows: each write to a subject is announced here, and every open
`GET /stream` response is one reader of it (spec webapp-actions §8.3). The feed names what changed, never the
content: the page fetches the subject again. Nothing is replayed to a new reader, who fetches on connect."""

from __future__ import annotations

import json
import threading
from collections import deque
from collections.abc import Iterator


class Changes:
    """The last `KEEP` changes, in order; readers wait on a condition and are never able to block a writer."""

    KEEP = 1000  # a reader further behind than this misses the oldest and still hears the rest

    def __init__(self) -> None:
        self._changed = threading.Condition()
        self._recent: deque[tuple[int, str, int]] = deque(maxlen=self.KEEP)
        self._sequence = 0

    def announce(self, kind: str, number: int) -> None:
        with self._changed:
            self._sequence += 1
            self._recent.append((self._sequence, kind, number))
            self._changed.notify_all()

    def stream(self, keepalive: float = 15.0) -> Iterator[bytes]:
        """Server-sent events for every change from now on, a comment line every `keepalive` idle seconds.
        The cursor is read here, not at the first `next`: a change made while the headers go out is not missed."""
        with self._changed:
            cursor = self._sequence
        return self._events(cursor, keepalive)

    def _events(self, cursor: int, keepalive: float) -> Iterator[bytes]:
        while True:
            with self._changed:
                if self._sequence == cursor:
                    self._changed.wait(keepalive)
                fresh = [change for change in self._recent if change[0] > cursor]
                cursor = self._sequence
            if not fresh:
                yield b": keep-alive\n\n"
            for _, kind, number in fresh:
                yield b"data: " + json.dumps({"kind": kind, "number": number}).encode() + b"\n\n"
