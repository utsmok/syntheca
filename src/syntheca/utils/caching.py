"""Utilities for file-based function and raw HTTP-response caching.

This module provides a `file_cache` decorator suitable for synchronous and
asynchronous functions, plus helpers for the shared on-disk raw-response cache
used by retrieval HTTP clients.
"""

from __future__ import annotations

import base64
import functools
import gzip
import inspect
import json
import pathlib
import pickle
from hashlib import blake2b
from typing import Any
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import httpx

from syntheca.config import settings

RAW_RESPONSE_CACHE_DIRNAME = "raw_responses"
RAW_RESPONSE_CACHE_VERSION = 1
_RAW_RESPONSE_HEADERS_TO_STRIP = {
    "content-encoding",
    "content-length",
    "transfer-encoding",
}


def _canonicalize_for_cache(value: Any) -> Any:
    """Return a JSON-serializable representation for cache-key generation.

    Args:
        value (Any): Value to normalize.

    Returns:
        Any: A stable, JSON-serializable representation.

    """
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, bytes):
        return {
            "__type__": "bytes",
            "value": base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, pathlib.Path):
        return str(value)
    if isinstance(value, httpx.QueryParams):
        return [
            [str(key), str(val)]
            for key, val in sorted(
                value.multi_items(),
                key=lambda item: (str(item[0]), str(item[1])),
            )
        ]
    if isinstance(value, httpx.Headers):
        return [
            [str(key).lower(), str(val)]
            for key, val in sorted(
                value.multi_items(),
                key=lambda item: (str(item[0]).lower(), str(item[1])),
            )
        ]
    if isinstance(value, dict):
        return {
            str(key): _canonicalize_for_cache(val)
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list | tuple):
        return [_canonicalize_for_cache(item) for item in value]
    if isinstance(value, set):
        normalized_items = [_canonicalize_for_cache(item) for item in value]
        return sorted(normalized_items, key=lambda item: json.dumps(item, sort_keys=True))
    return repr(value)


def _normalize_url_and_query(url: str, params: Any = None) -> tuple[str, list[list[str]]]:
    """Normalize a URL plus query params into a stable cache-key representation.

    Args:
        url (str): Request URL, possibly including query parameters.
        params (Any): Additional request params passed separately.

    Returns:
        tuple[str, list[list[str]]]: Base URL without query, plus sorted query items.

    """
    parsed = urlsplit(str(url))
    query_items: list[tuple[str, str]] = [
        (str(key), str(val)) for key, val in parse_qsl(parsed.query, keep_blank_values=True)
    ]
    if params is not None:
        query_items.extend(
            (str(key), str(val)) for key, val in httpx.QueryParams(params).multi_items()
        )
    base_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    normalized_query = sorted(query_items, key=lambda item: (item[0], item[1]))
    return base_url, [[key, val] for key, val in normalized_query]


def build_request_cache_key(method: str, url: str, **kwargs: Any) -> str:
    """Build a stable cache key derived from request semantics.

    Args:
        method (str): HTTP method.
        url (str): Request URL.
        **kwargs: Request keyword arguments such as ``params``, ``json``, and ``data``.

    Returns:
        str: Stable BLAKE2 hex digest.

    """
    base_url, query_items = _normalize_url_and_query(url, kwargs.get("params"))
    semantics: dict[str, Any] = {
        "version": RAW_RESPONSE_CACHE_VERSION,
        "method": method.upper(),
        "url": base_url,
        "query": query_items,
    }
    for field in ("json", "data", "content", "files", "headers"):
        value = kwargs.get(field)
        if value is not None:
            semantics[field] = _canonicalize_for_cache(value)

    digest = blake2b(digest_size=32)
    digest.update(
        json.dumps(
            semantics,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
    return digest.hexdigest()


def get_raw_response_cache_path(cache_dir: pathlib.Path, cache_key: str) -> pathlib.Path:
    """Return the on-disk path for a raw-response cache entry.

    Args:
        cache_dir (pathlib.Path): Cache root directory.
        cache_key (str): Stable cache digest.

    Returns:
        pathlib.Path: Path to the compressed cache artifact.

    """
    cache_root = pathlib.Path(cache_dir) / RAW_RESPONSE_CACHE_DIRNAME / cache_key[:2]
    return cache_root / f"{cache_key}.httpx.gz"


def _normalize_cached_response_headers(headers: Any) -> list[tuple[str, str]]:
    """Return cache-safe response headers for reconstructed ``httpx.Response`` objects.

    Cached response bodies are stored as decoded bytes via ``response.content``.
    Transport-level headers that describe the encoded wire representation must be
    stripped, otherwise httpx will attempt to decode the already-decoded payload
    again when the cached response is reconstructed.

    Args:
        headers (Any): Original response headers in any representation accepted by ``httpx.Headers``.

    Returns:
        list[tuple[str, str]]: Header items safe to persist and reuse for cached responses.

    """
    normalized_headers = httpx.Headers(headers or [])
    return [
        (key, value)
        for key, value in normalized_headers.multi_items()
        if key.lower() not in _RAW_RESPONSE_HEADERS_TO_STRIP
    ]


def save_raw_response(
    cache_dir: pathlib.Path, cache_key: str, response: httpx.Response
) -> pathlib.Path:
    """Persist an HTTP response to the shared raw-response cache.

    Args:
        cache_dir (pathlib.Path): Cache root directory.
        cache_key (str): Stable request cache key.
        response (httpx.Response): Response to persist.

    Returns:
        pathlib.Path: Path to the written cache artifact.

    """
    path = get_raw_response_cache_path(cache_dir, cache_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    request = getattr(response, "request", None)
    payload = {
        "version": RAW_RESPONSE_CACHE_VERSION,
        "request": {
            "method": request.method if request is not None else "GET",
            "url": str(request.url) if request is not None else "https://cache.invalid/",
        },
        "response": {
            "status_code": response.status_code,
            "headers": _normalize_cached_response_headers(response.headers),
            "content": response.content,
        },
    }
    with gzip.open(path, "wb", compresslevel=6) as handle:
        pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return path


def load_raw_response(cache_dir: pathlib.Path, cache_key: str) -> httpx.Response | None:
    """Load a cached raw HTTP response if present and readable.

    Args:
        cache_dir (pathlib.Path): Cache root directory.
        cache_key (str): Stable request cache key.

    Returns:
        httpx.Response | None: Reconstructed response, or ``None`` on cache miss.

    """
    path = get_raw_response_cache_path(cache_dir, cache_key)
    if not path.exists():
        return None

    try:
        with gzip.open(path, "rb") as handle:
            payload = pickle.load(handle)
    except EOFError, OSError, pickle.PickleError:
        path.unlink(missing_ok=True)
        return None

    if payload.get("version") != RAW_RESPONSE_CACHE_VERSION:
        return None

    request_data = payload.get("request") or {}
    response_data = payload.get("response") or {}
    content = response_data.get("content") or b""
    if isinstance(content, bytearray):
        content = bytes(content)
    request = httpx.Request(
        request_data.get("method") or "GET",
        request_data.get("url") or "https://cache.invalid/",
    )
    response = httpx.Response(
        int(response_data.get("status_code") or 200),
        headers=_normalize_cached_response_headers(response_data.get("headers")),
        content=content,
        request=request,
    )
    response.extensions["syntheca.from_cache"] = True
    response.extensions["syntheca.cache_key"] = cache_key
    response.extensions["syntheca.cache_path"] = str(path)
    return response


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
