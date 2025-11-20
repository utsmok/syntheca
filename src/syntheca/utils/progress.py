from __future__ import annotations

import itertools
from threading import Lock

_counter = itertools.count(0)
_lock = Lock()


def get_next_position() -> int:
    with _lock:
        return next(_counter)


def reset_positions() -> None:
    # Reset the global counter for tests or re-runs where deterministic bar positions are required
    # Not intended for concurrent use; acquire lock to be safe
    global _counter
    with _lock:
        _counter = itertools.count(0)
