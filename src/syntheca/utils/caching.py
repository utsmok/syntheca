from __future__ import annotations

import functools
import inspect
import pathlib
import pickle
from hashlib import blake2b

from syntheca.config import settings


def _make_key(func_name: str, args: tuple, kwargs: dict) -> str:
    # Use repr-based hashing; stable for basic types and safe for caching across runs
    m = blake2b(digest_size=20)
    m.update(func_name.encode())
    m.update(repr(args).encode())
    m.update(repr(sorted(kwargs.items())).encode())
    return m.hexdigest()


def file_cache(prefix: str | None = None):
    """A simple file-based cache decorator that supports async functions.

    Args:
        prefix: Optional prefix for the cache files.
    """

    def decorator(func):
        cache_dir = settings.cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)

        @functools.wraps(func)
        def _sync_wrapper(*args, **kwargs):
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
