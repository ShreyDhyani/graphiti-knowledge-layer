"""
ingest_utils.py

Contains a robust, reusable ingestion helper and retry decorator for Graphiti episode ingestion.
Intended to be imported and used by graphiti_ingest_mapper.py:

from ingest_utils import ingest_models_as_episodes, retry_async

Features:
- async retry_async decorator with exponential backoff + jitter
- ingest_models_as_episodes(graphiti, circular, clauses) function
  * concurrency throttling via asyncio.Semaphore
  * per-episode retry via decorator (configurable)
  * failure persistence into `failed/`
  * circuit-break pause on repeated failures

Adjust SEMAPHORE_MAX / MAX_CONSECUTIVE_FAILURES / LONG_BACKOFF_SECONDS to tune behavior.
"""
from __future__ import annotations

import asyncio
import random
import logging
import os
import json
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, List
from graphiti_core.nodes import EpisodeType


log = logging.getLogger(__name__)

# ---------------------------------
# Retry decorator
# ---------------------------------

def _is_retryable_exception(exc: Exception) -> bool:
    """Naive heuristic to detect retryable rate-limit/quota errors."""
    if exc is None:
        return False
    msg = str(exc).lower()
    if any(k in msg for k in ("rate limit", "rate_limited", "429", "quota", "resource_exhausted", "insufficient_quota")):
        return True
    return False


def retry_async(
    max_retries: int = 5,
    initial_delay: float = 0.5,
    max_delay: float = 60.0,
    factor: float = 2.0,
    jitter: float = 0.3,
):
    """Async decorator for exponential backoff with jitter.

    Usage:
        @retry_async(max_retries=6)
        async def call_api(...):
            ...
    """
    def decorator(fn: Callable[..., Awaitable[Any]]):
        async def _wrapper(*args, **kwargs):
            attempt = 0
            last_exc = None
            while True:
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    attempt += 1
                    if attempt > max_retries or not _is_retryable_exception(e):
                        log.debug("No retry: attempt=%s max_retries=%s exc=%s", attempt, max_retries, e)
                        raise
                    base = initial_delay * (factor ** (attempt - 1))
                    sleep_time = min(base, max_delay)
                    jitter_amount = sleep_time * jitter
                    sleep_time = max(0.0, sleep_time + random.uniform(-jitter_amount, jitter_amount))
                    sleep_time = min(max(0.0, sleep_time), max_delay)
                    log.warning("Retryable error (attempt %d/%d): %s — sleeping %.2fs then retrying...",
                                attempt, max_retries, e, sleep_time)
                    await asyncio.sleep(sleep_time)
        return _wrapper
    return decorator


# ---------------------------------
# Ingest helper
# ---------------------------------

# Tunables — adjust for your provider/quota
SEMAPHORE_MAX = int(os.getenv("INGEST_SEMAPHORE_MAX", "1"))
MAX_CONSECUTIVE_FAILURES = int(os.getenv("INGEST_MAX_CONSECUTIVE_FAILURES", "3"))
LONG_BACKOFF_SECONDS = int(os.getenv("INGEST_LONG_BACKOFF_SECONDS", str(60 * 5)))

_sem = asyncio.Semaphore(SEMAPHORE_MAX)

# Default per-episode retry decorator instance
_retry_add_episode = retry_async(max_retries=6, initial_delay=0.5, max_delay=30.0)

async def _default_persist_failure(circular: Any, clauses: List[Any], reason: str) -> None:
    os.makedirs("failed", exist_ok=True)
    out_path = os.path.join("failed", f"{getattr(circular, 'id', 'unknown')}.failed.json")
    payload = {
        "reason": reason,
        "circular": circular.to_dict() if hasattr(circular, "to_dict") else dict(circular.__dict__),
        "clauses": [c.to_dict() if hasattr(c, "to_dict") else dict(c.__dict__) for c in clauses],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
    log.info("Persisted failed payload to %s", out_path)


async def ingest_models_as_episodes(graphiti: Any, circular: Any, clauses: List[Any],
                                   persist_failure_fn: Callable[[Any, List[Any], str], Awaitable[None]] = None):
    """Ingest circular metadata + clauses as Graphiti episodes.

    Parameters
    - graphiti: Graphiti client instance
    - circular: Pydantic Circular model instance (has .to_dict())
    - clauses: list of Clause model instances
    - persist_failure_fn: optional async function to persist failures (defaults to writing into failed/)
    """
    if persist_failure_fn is None:
        persist_failure_fn = _default_persist_failure

    meta_text = (
        f"CIRCULAR METADATA:\n"
        f"Title: {getattr(circular, 'title', None)}\n"
        f"Source File: {getattr(circular, 'source_file', None)}\n"
        f"Pages: {getattr(circular, 'pages', None)}\n\n"
        f"Full text (first 2000 chars):\n{(getattr(circular, 'full_text', '') or '')[:2000]}\n"
    )

    consecutive_failures = 0

    # add meta episode
    @_retry_add_episode
    async def _add_meta():
        async with _sem:
            await graphiti.add_episode(
                name=f"circular_meta_{getattr(circular, 'id', 'unknown')}",
                episode_body=meta_text,
                source=EpisodeType.text,
                source_description=f"circular metadata {getattr(circular, 'source_file', None)}",
                reference_time=datetime.now(timezone.utc),
            )


    try:
        await _add_meta()
        consecutive_failures = 0
    except Exception as e:
        log.error("Meta episode ingestion failed: %s", e)
        consecutive_failures += 1
        await persist_failure_fn(circular, clauses, f"meta_episode_failed: {e}")

    # ingest clauses
    for i, cl in enumerate(clauses):
        # capture loop vars in defaults
        @_retry_add_episode
        async def _add_clause(ci=i, clause=cl):
            async with _sem:
                print(f"Adding Episode using _add_clause {getattr(circular, 'id', 'unknown')}_clause_{ci}")
                await graphiti.add_episode(
                    name=f"{getattr(circular, 'id', 'unknown')}_clause_{ci}",
                    episode_body=getattr(clause, 'text', '') or "",
                    source=EpisodeType.text,
                    source_description=f"{getattr(circular, 'source_file', None)} chunk {ci}",
                    reference_time=datetime.now(timezone.utc),
                )

        try:
            await _add_clause()
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            log.error("Ingest failed for clause %d of circular %s: %s", i, getattr(circular, 'id', 'unknown'), e)
            await persist_failure_fn(circular, clauses, f"clause_{i}_failed: {e}")

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log.warning("Detected %d consecutive failures — pausing for %ds", consecutive_failures, LONG_BACKOFF_SECONDS)
                await asyncio.sleep(LONG_BACKOFF_SECONDS)
                consecutive_failures = 0

    log.info("Ingestion attempted for circular %s complete.", getattr(circular, 'id', 'unknown'))
