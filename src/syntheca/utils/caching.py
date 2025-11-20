"""Utilities for simple file-based caching of function results.

This module provides a `file_cache` decorator suitable for synchronous and
asynchronous functions, saving results to a project cache directory.
"""

from __future__ import annotations

import functools
import inspect
import pathlib
import pickle
from hashlib import blake2b

from syntheca.config import settings


def _make_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """Create a stable cache key for function arguments.

    Args:
        func_name (str): Qualname of the function.
        args (tuple): Positional arguments supplied to the function.
        kwargs (dict): Keyword arguments supplied to the function.

    Returns:
        str: A stable hex digest string to use as cache key.

    """
    # Use repr-based hashing; stable for basic types and safe for caching across runs
    m = blake2b(digest_size=20)
    m.update(func_name.encode())
    m.update(repr(args).encode())
    m.update(repr(sorted(kwargs.items())).encode())
    return m.hexdigest()


def file_cache(prefix: str | None = None):
    """Create a file-based cache decorator for functions.

    Args:
        prefix (str | None): Optional prefix for the cache files.

    Returns:
        Callable: Decorator function suitable for both sync and async functions.

    """

    def decorator(func):
        cache_dir = settings.cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)

        @functools.wraps(func)
        def _sync_wrapper(*args, **kwargs):
            """Cache the function result to disk synchronously.

            Args:
                *args: Positional arguments passed to the wrapped function.
                **kwargs: Keyword arguments passed to the wrapped function.

            Returns:
                Any: The result of the wrapped function, possibly loaded from cache.

            """
            key = _make_key(func.__qualname__, args, kwargs)
            filename = cache_dir / f"{prefix or func.__name__}_{key}.pkl"
            if filename.exists():
                with pathlib.Path(filename).open("rb") as fh:
                    return pickle.load(fh)
            result = func(*args, **kwargs)
            with pathlib.Path(filename).open("wb") as fh:
                pickle.dump(result, fh)
            return result

        @functools.wraps(func)
        async def _async_wrapper(*args, **kwargs):
            """Cache the coroutine function result to disk asynchronously.

            Args:
                *args: Positional arguments passed to the wrapped coroutine.
                **kwargs: Keyword arguments passed to the wrapped coroutine.

            Returns:
                Any: The result of the coroutine, possibly loaded from cache.

            """
            key = _make_key(func.__qualname__, args, kwargs)
            filename = cache_dir / f"{prefix or func.__name__}_{key}.pkl"
            if filename.exists():
                with pathlib.Path(filename).open("rb") as fh:
                    return pickle.load(fh)
            result = await func(*args, **kwargs)
            # Ensure parent exists
            filename.parent.mkdir(parents=True, exist_ok=True)
            with pathlib.Path(filename).open("wb") as fh:
                pickle.dump(result, fh)
            return result

        if inspect.iscoroutinefunction(func):
            return _async_wrapper
        return _sync_wrapper

    return decorator
