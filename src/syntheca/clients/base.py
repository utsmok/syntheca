from __future__ import annotations

import logging
from typing import Any

import httpx
from loguru import logger
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from syntheca.config import settings


def _is_retriable_exception(exc: Exception) -> bool:
    # Retry on network errors and HTTP 429/5xx
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        status = exc.response.status_code
        return status == 429 or 500 <= status < 600
    return False


class BaseClient:
    """Base async client with retries, logging, and caching hooks.

    Subclasses should use `await self.request("GET", url, params=params)` to ensure
    retry logic is applied uniformly.
    """

    def __init__(self, *, headers: dict[str, str] | None = None, timeout: float | None = None):
        self.headers = headers or {"User-Agent": settings.user_agent}
        self.timeout = timeout or settings.default_timeout
        self.client = httpx.AsyncClient(headers=self.headers, timeout=self.timeout)
        # loguru logger is configured by syntheca.utils.logging
        self.logger = logger.bind(client=self.__class__.__name__)

    async def __aenter__(self) -> BaseClient:
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # ensure we close the underlying httpx client pool
        try:
            await self.client.aclose()
        except Exception:
            self.logger.exception("Error closing httpx AsyncClient")

    @retry(
        retry=retry_if_exception(_is_retriable_exception),
        wait=wait_exponential(min=1, max=20),
        stop=stop_after_attempt(4),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Perform an HTTP request with retries on transient errors.

        This method raises on non-2xx responses via `response.raise_for_status()`.
        """

        # ensure we time out per call if provided
        if "timeout" not in kwargs and self.timeout is not None:
            kwargs["timeout"] = self.timeout

        try:
            response = await self.client.request(method, url, **kwargs)
            # httpx does not raise for 4xx/5xx unless we ask
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            # re-raise so tenacity can inspect and possibly retry
            self.logger.warning("HTTPStatus error on %s %s: %s", method, url, e)
            raise
        except httpx.RequestError as e:
            self.logger.warning("HTTP Request error on %s %s: %s", method, url, e)
            raise
