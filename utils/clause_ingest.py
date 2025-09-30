# clause_ingest.py
"""
Helper to add single clause episodes and a bulk helper.

- add_clause_episode(...) : add a single clause (uses retry + semaphore)
- add_clause_episode_in_bulk(...) : attempt bulk ingestion (preferred when you want throughput)

This module intentionally does not import ingest_utils to avoid circular imports.
It uses utils.retry for retry logic so retry behavior is shared across modules.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, List, Optional

# Graphiti node types may be available in graphiti_core.nodes
try:
    from graphiti_core.nodes import EpisodeType, RawEpisode  # RawEpisode optional
    _HAS_RAW_EPISODE = True
except Exception:
    try:
        from graphiti_core.nodes import EpisodeType
    except Exception:
        EpisodeType = None
    RawEpisode = None
    _HAS_RAW_EPISODE = False

from utils.retry import default_retry

log = logging.getLogger(__name__)

async def add_clause_episode(
    graphiti: Any,
    circular: Any,
    clause: Any,
    index: int,
    sem: asyncio.Semaphore,
    retry_decorator: Optional[Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]] = None,
) -> None:
    """
    Add a single clause as an episode to Graphiti.

    Uses a retry decorator (module default if not provided) and respects the provided semaphore.
    """
    if retry_decorator is None:
        retry_decorator = default_retry

    @retry_decorator
    async def _do_add():
        async with sem:
            name = f"{getattr(circular, 'id', 'unknown')}_clause_{index}"
            body = getattr(clause, "text", "") or ""
            # Use the client API expected by your Graphiti client; we assume add_episode exists
            await graphiti.add_episode(
                name=name,
                episode_body=body,
                source=(EpisodeType.text if EpisodeType is not None else "text"),
                source_description=f"{getattr(circular, 'source_file', None)} chunk {index}",
                reference_time=datetime.now(timezone.utc),
            )
            log.debug("Added episode %s", name)

    await _do_add()


async def add_clause_episode_in_bulk(
    graphiti: Any,
    circular: Any,
    clauses: Iterable[Any],
    sem: asyncio.Semaphore,
    retry_decorator: Optional[Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]] = None,
) -> None:
    """
    Attempt to ingest all clauses in bulk using graphiti.add_episode_bulk(...).

    Behavior:
    - If RawEpisode is available, builds RawEpisode objects; otherwise falls back to dict payloads.
    - Wraps the bulk call with `retry_decorator` if provided (otherwise uses module default).
    - Uses the provided semaphore to limit concurrent Graphiti usage (entire bulk call is inside the semaphore).
    - Raises any exception that occurs (caller may catch and fall back to per-clause ingestion).
    """
    if retry_decorator is None:
        retry_decorator = default_retry

    # Build payloads
    payloads: List[Any] = []
    for i, clause in enumerate(clauses):
        payload = {
            "name": f"{getattr(circular, 'id', 'unknown')}_clause_{i}",
            "source_description": f"{getattr(circular, 'source_file', None)} chunk {i}",
            "reference_time": datetime.now(timezone.utc),
        }
        if hasattr(clause, "to_dict"):
            payload_content = clause.to_dict()
            payload["content"] = json.dumps(payload_content, default=str, ensure_ascii=False)
            payload["source"] = (EpisodeType.json if hasattr(EpisodeType, "json") else "json")
        else:
            payload["content"] = getattr(clause, "text", "") or ""
            payload["source"] = (EpisodeType.text if EpisodeType is not None else "text")

        if _HAS_RAW_EPISODE and RawEpisode is not None:
            # Create SDK RawEpisode instance
            ep = RawEpisode(
                name=payload["name"],
                content=payload["content"],
                source=payload["source"],
                source_description=payload["source_description"],
                reference_time=payload["reference_time"],
            )
            payloads.append(ep)
        else:
            # Ensure reference_time serializable
            payload["reference_time"] = payload["reference_time"].isoformat() if hasattr(payload["reference_time"], "isoformat") else str(payload["reference_time"])
            payloads.append(payload)

    # The bulk call itself should be retried if it's a retryable error
    @retry_decorator
    async def _do_bulk_call():
        async with sem:
            if not hasattr(graphiti, "add_episode_bulk"):
                raise AttributeError("Graphiti client does not implement add_episode_bulk")
            await graphiti.add_episode_bulk(payloads)
            log.debug("Bulk add_episode_bulk called for circular %s (count=%d)", getattr(circular, "id", "unknown"), len(payloads))

    await _do_bulk_call()
