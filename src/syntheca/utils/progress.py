"""Utilities for allocating unique console progress bar positions.

Provides a simple thread-safe counter used to assign deterministic `position`
values to concurrent `tqdm` progress bars so they do not overlap in the terminal.
"""

from __future__ import annotations

import itertools
from threading import Lock

_counter = itertools.count(0)
_lock = Lock()


def get_next_position() -> int:
    """Return a next integer position for assigning to a progress bar.

    This function is thread-safe and will increment a global counter to provide
    a unique, deterministic position for `tqdm` instances.

    Returns:
        int: Next available position value.

    """
    with _lock:
        return next(_counter)


def reset_positions() -> None:
    """Reset the progress bar position counter.

    Intended for tests and deterministic runs only; not safe to call concurrently
    while progress bars are actively being used.
    """
    global _counter
    with _lock:
        _counter = itertools.count(0)
