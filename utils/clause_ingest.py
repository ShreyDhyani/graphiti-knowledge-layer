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
import logging
from datetime import datetime, timezone
from typing import Any, Iterable
from utils.retry import default_retry

# Graphiti node types may be available in graphiti_core.nodes
try:
    from graphiti_core.nodes import EpisodeType  # RawEpisode optional
    from graphiti_core.utils.bulk_utils import RawEpisode
    _HAS_RAW_EPISODE = True
except Exception:
    try:
        from graphiti_core.nodes import EpisodeType
    except Exception:
        EpisodeType = None
    RawEpisode = None
    _HAS_RAW_EPISODE = False


log = logging.getLogger(__name__)

async def add_clause_episode(
    graphiti: Any,
    circular: Any,
    clause: Any,
    index: int,
    sem: asyncio.Semaphore,
) -> None:
    """
    Add a single clause as an episode to Graphiti.

    Uses a retry decorator (module default if not provided) and respects the provided semaphore.
    Prints ingestion progress and timing for visibility.
    """
    retry_decorator = default_retry

    @retry_decorator
    async def _do_add():
        async with sem:
            name = f"{getattr(circular, 'id', 'unknown')}_clause_{index}"
            body = getattr(clause, "text", "") or ""

            start_time = datetime.now()

            await graphiti.add_episode(
                name=name,
                episode_body=body,
                source=(EpisodeType.text if EpisodeType is not None else "text"),
                source_description=f"{getattr(circular, 'source_file', None)} chunk {index}",
                reference_time=datetime.now(timezone.utc),
            )

            end_time = datetime.now()
            elapsed = (end_time - start_time).total_seconds()

            # Print instead of log.debug so it always shows up
            print(f"✅ Added Episode {name} (took {elapsed:.2f}s)")

    await _do_add()


async def add_clause_episode_in_bulk(
    graphiti: Any,
    circular: Any,
    clauses: Iterable[Any],
    sem: asyncio.Semaphore,
) -> None:
    """
    Attempt to ingest clauses as text-only episodes in batches using graphiti.add_episode_bulk(...).

    - Treat every clause as TEXT only.
    - Send payloads in batches (default batch size = 50).
    - Print compact messages showing ranges like "Adding 1-50 out of 200" and timing per batch.
    - Each batch call is wrapped with the provided retry_decorator (or module default).
    """
    retry_decorator = default_retry

    BATCH_SIZE = 50

    clause_list = list(clauses)
    total = len(clause_list)
    circular_id = getattr(circular, "id", "unknown")

    # Helper to build a single payload entry (text-only)
    def _build_payload(name: str, clause_obj: Any, idx: int) -> Any:
        content = getattr(clause_obj, "text", "") or str(clause_obj)
        source_val = (EpisodeType.text if EpisodeType is not None else "text")
        if _HAS_RAW_EPISODE and RawEpisode is not None:
            return RawEpisode(
                name=name,
                content=content,
                source=source_val,
                source_description=f"{getattr(circular, 'source_file', None)} chunk {idx}",
                reference_time=datetime.now(timezone.utc),
            )
        else:
            return {
                "name": name,
                "content": content,
                "source": (source_val if isinstance(source_val, str) else getattr(source_val, "name", "text")),
                "source_description": f"{getattr(circular, 'source_file', None)} chunk {idx}",
                "reference_time": datetime.now(timezone.utc).isoformat(),
            }

    # Process in batches
    for start in range(0, total, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total)
        batch = [_build_payload(f"{getattr(circular, 'id', 'unknown')}_clause_{i}", clause_list[i], i) for i in range(start, end)]

        # Print brief range info before sending
        print(f"Adding {start + 1}-{end} out of {total} for circular {circular_id}")

        # Wrapped bulk call for this batch
        @retry_decorator
        async def _do_batch_call():
            async with sem:
                if not hasattr(graphiti, "add_episode_bulk"):
                    raise AttributeError("Graphiti client does not implement add_episode_bulk")
                await graphiti.add_episode_bulk(batch)

        # Execute the batch call with timing
        start_time = datetime.now()
        await _do_batch_call()
        elapsed = (datetime.now() - start_time).total_seconds()

        # Print completion for this batch with timing
        print(f"Added {start + 1}-{end} out of {total} for circular {circular_id} (took {elapsed:.2f}s)")