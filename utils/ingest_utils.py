# ingest_utils.py
from __future__ import annotations

import asyncio
import logging
import os
import json
from datetime import datetime, timezone
from typing import Any, List
from graphiti_core.nodes import EpisodeType
from dotenv import load_dotenv

load_dotenv()

# clause helpers
from utils.clause_ingest import add_clause_episode
import utils.clause_ingest as _clause_ingest
from utils._default_persist_failure import default_persist_failure as _imported_default_persist_failure

# retry utilities
try:
    from utils.retry import default_retry
except Exception:
    # fallback: simple local no-op decorator if utils.retry missing
    def default_retry(fn=None, *_, **__):
        if fn is None:
            def _wrap(f): return f
            return _wrap
        return fn

log = logging.getLogger(__name__)

# Tunables — environment-configurable
SEMAPHORE_MAX = int(os.getenv("INGEST_SEMAPHORE_MAX", "10"))
MAX_CONSECUTIVE_FAILURES = int(os.getenv("INGEST_MAX_CONSECUTIVE_FAILURES", "2"))
LONG_BACKOFF_SECONDS = int(os.getenv("INGEST_LONG_BACKOFF_SECONDS", str(60 * 1)))

_sem = asyncio.Semaphore(SEMAPHORE_MAX)

async def ingest_models_as_episodes(
    graphiti: Any,
    circular: Any,
    clauses: List[Any],
    bulk: bool = False,
):
    """
    Ingest circular metadata + clauses as Graphiti episodes.

    Parameters
    - graphiti: Graphiti client instance
    - circular: Pydantic-like Circular model instance
    - clauses: list of Clause model instances
    - bulk: whether to attempt a bulk ingestion path (preferred when available)
    - persist_failure_fn: async function to persist failures (defaults to module/_default_persist_failure)
    - semaphore: optional asyncio.Semaphore to throttle Graphiti calls (defaults to module semaphore)
    - retry_decorator: optional retry decorator (defaults to utils.retry.default_retry)
    """
    persist_failure_fn = _imported_default_persist_failure
    semaphore = _sem
    retry_decorator = default_retry

    meta_text = (
        f"CIRCULAR METADATA:\n"
        f"Title: {getattr(circular, 'title', None)}\n"
        f"Source File: {getattr(circular, 'source_file', None)}\n"
        f"Pages: {getattr(circular, 'pages', None)}\n\n"
        f"Full text (first 2000 chars):\n{(getattr(circular, 'full_text', '') or '')[:2000]}\n"
    )

    consecutive_failures = 0
    n_total = len(clauses)
    n_ok = 0
    n_fail = 0

    # Add meta episode (retry via provided decorator)
    try:
        async def _add_meta():
            async with semaphore:
                await graphiti.add_episode(
                    name=f"circular_meta_{getattr(circular, 'id', 'unknown')}",
                    episode_body=meta_text,
                    source=EpisodeType.text,
                    source_description=f"circular metadata {getattr(circular, 'source_file', None)}",
                    reference_time=datetime.now(timezone.utc),
                )

        wrapped_meta = retry_decorator(_add_meta)
        await wrapped_meta()
        consecutive_failures = 0
    except Exception as e:
        log.error("Meta episode ingestion failed: %s", e)
        consecutive_failures += 1
        try:
            await persist_failure_fn(circular, clauses, f"clause_{i}_failed: {e}")
        except Exception as pf_exc:
            log.exception("persist_failure_fn failed while handling clause %d error: %s", i, pf_exc)
        

    # Bulk path: prefer clause_ingest.add_clause_episode_in_bulk if available
    bulk_done = False
    if bulk:
        bulk_helper = getattr(_clause_ingest, "add_clause_episode_in_bulk", None)
        if callable(bulk_helper):
            try:
                await bulk_helper(graphiti=graphiti, circular=circular, clauses=clauses, sem=semaphore)
                n_ok = n_total
                bulk_done = True
                log.info("Bulk helper succeeded for circular %s", getattr(circular, "id", "unknown"))
            except Exception as e:
                log.error("Bulk helper failed: %s", e)
                try:
                    await persist_failure_fn(circular, clauses, f"clause_{i}_failed: {e}")
                except Exception as pf_exc:
                    log.exception("persist_failure_fn failed while handling clause %d error: %s", i, pf_exc)
                # fall through to fallback behavior
        else:
            # try graphiti.add_episode_bulk (build payloads then call)
            if hasattr(graphiti, "add_episode_bulk"):
                try:
                    # Build payloads compatible with many clients: prefer to send dicts; clause_ingest will create RawEpisode if SDK available.
                    payloads = []
                    for i, clause in enumerate(clauses):
                        name = f"{getattr(circular, 'id', 'unknown')}_clause_{i}"
                        if hasattr(clause, "to_dict"):
                            content = json.dumps(clause.to_dict(), default=str, ensure_ascii=False)
                            source = "json"
                        else:
                            content = getattr(clause, "text", "") or ""
                            source = "text"
                        payloads.append({
                            "name": name,
                            "content": content,
                            "source": source,
                            "source_description": f"{getattr(circular, 'source_file', None)} chunk {i}",
                            "reference_time": datetime.now(timezone.utc).isoformat(),
                        })

                    async def _do_bulk():
                        async with semaphore:
                            await graphiti.add_episode_bulk(payloads)

                    wrapped_bulk = retry_decorator(_do_bulk)
                    await wrapped_bulk()
                    n_ok = n_total
                    bulk_done = True
                    log.info("Bulk ingestion via graphiti.add_episode_bulk succeeded for circular %s", getattr(circular, "id", "unknown"))
                except Exception as e:
                    log.error("graphiti.add_episode_bulk failed: %s", e)
                    try:
                        await persist_failure_fn(circular, clauses, f"clause_{i}_failed: {e}")
                    except Exception as pf_exc:
                        log.exception("persist_failure_fn failed while handling clause %d error: %s", i, pf_exc)
                    # fall through to sequential ingestion
            else:
                log.warning("Bulk requested but no bulk helper and graphiti.add_episode_bulk missing — will use per-clause ingestion.")

    # If bulk not performed or not fully successful, do per-clause sequential ingestion (to preserve circuit-break semantics)
    if not bulk_done:
        for i, cl in enumerate(clauses):
            try:
                await add_clause_episode(graphiti, circular, cl, i, semaphore)
                consecutive_failures = 0
                n_ok += 1
                print(f"Ingested {n_ok}/{n_total}")
                log.info("Ingested %d/%d", n_ok, n_total)
            except Exception as e:
                consecutive_failures += 1
                n_fail += 1
                print(f"Ingest failed for clause %d of circular %s: %s", i, getattr(circular, "id", "unknown"), e)
                log.error("Ingest failed for clause %d of circular %s: %s", i, getattr(circular, "id", "unknown"), e)
                try:
                    await persist_failure_fn(circular, clauses, f"clause_{i}_failed: {e}")
                except Exception as pf_exc:
                    log.exception("persist_failure_fn failed while handling clause %d error: %s", i, pf_exc)

                # if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                #     log.warning("Detected %d consecutive failures — pausing for %ds", consecutive_failures, LONG_BACKOFF_SECONDS)
                #     await asyncio.sleep(LONG_BACKOFF_SECONDS)
                #     consecutive_failures = 0

    log.info("Done. %d/%d ingested, %d failed.", n_ok, n_total, n_fail)
    log.info("Ingestion attempted for circular %s complete.", getattr(circular, "id", "unknown"))
