"""The page's change feed: what a reader sees, and that it stays bounded."""

import json

from mia_buffers.stream import Changes


def test_stream_yields_changes_made_after_it_was_opened_then_keep_alives():
    changes = Changes()
    changes.announce("dataset", 1)  # before the stream was opened: not replayed, the page fetches on connect
    events = changes.stream(keepalive=0.05)
    changes.announce("buffer", 8)
    assert next(events) == b'data: {"kind": "buffer", "number": 8}\n\n'
    assert next(events) == b": keep-alive\n\n"


def test_a_reader_that_fell_behind_gets_what_is_still_held():
    changes = Changes()
    events = changes.stream(keepalive=0.05)
    for number in range(Changes.KEEP + 10):
        changes.announce("dataset", number)
    assert json.loads(next(events)[len(b"data: ") :])["number"] == 10
