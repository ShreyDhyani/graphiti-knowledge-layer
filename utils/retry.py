# utils/retry.py
"""
Retry utilities: a reusable async retry decorator with exponential backoff + jitter.

Provides:
- is_retryable_exception(exc) -> bool
- retry_async(...) -> decorator
- default_retry decorator instance
"""
from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)


def is_retryable_exception(exc: Exception | None) -> bool:
    """Heuristic to detect retryable rate-limit/quota errors."""
    if exc is None:
        return False
    msg = str(exc).lower()
    return any(
        k in msg
        for k in (
            "rate limit",
            "rate_limited",
            "429",
            "quota",
            "resource_exhausted",
            "insufficient_quota",
            "too many requests",
        )
    )


def retry_async(
    *,
    max_retries: int = 6,
    initial_delay: float = 0.5,
    max_delay: float = 30.0,
    factor: float = 2.0,
    jitter: float = 0.3,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """
    Async decorator factory for exponential backoff with jitter.

    Usage:
        @retry_async(max_retries=5)
        async def call_api(...): ...
    """
    def decorator(fn: Callable[..., Awaitable[Any]]):
        async def _wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    # If not retryable or we've exhausted attempts, raise
                    if attempt > max_retries or not is_retryable_exception(e):
                        log.debug("No retry: attempt=%s max_retries=%s exc=%s", attempt, max_retries, e)
                        raise
                    # compute exponential backoff with jitter
                    base = initial_delay * (factor ** (attempt - 1))
                    sleep_time = min(base, max_delay)
                    jitter_amount = sleep_time * jitter
                    sleep_time = max(0.0, sleep_time + random.uniform(-jitter_amount, jitter_amount))
                    sleep_time = min(sleep_time, max_delay)
                    log.warning(
                        "Retryable error (attempt %d/%d): %s — sleeping %.2fs then retrying...",
                        attempt,
                        max_retries,
                        e,
                        sleep_time,
                    )
                    await asyncio.sleep(sleep_time)
        return _wrapper
    return decorator


# default decorator instance (import if you want module defaults)
default_retry = retry_async(max_retries=6, initial_delay=0.5, max_delay=30.0)
