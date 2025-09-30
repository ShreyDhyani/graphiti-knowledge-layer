from __future__ import annotations
import logging
import os
import json
import re
import uuid
import asyncio
from typing import Any, List
from datetime import datetime, timezone

log = logging.getLogger(__name__)

async def default_persist_failure(circular: Any, clauses: List[Any], reason: str) -> None:
    """
    Persist a failed *clause* into a single per-circular failed file:
      failed/<circular_id>.failed.json

    File structure:
    {
      "circular_id": "<id>",
      "failed_clauses": {
        "<clause_key>": [
          { "reason": "...", "failed_clause": {...}, "timestamp": "..." },
          ...
        ],
        ...
      },
      "updated_at": "..."
    }

    Behavior:
    - Attempts to extract clause index from reason using "clause_<N>_failed".
    - If found, uses clause id (if present) or "clause_<N>" as the clause key.
    - If file doesn't exist, creates it. If exists, appends this failure event.
    - Writes atomically (tmp file -> replace) and runs I/O in a thread.
    - Never raises (exceptions are logged).
    """
    try:
        os.makedirs("failed", exist_ok=True)
        circ_id = getattr(circular, "id", "unknown")
        out_path = os.path.join("failed", f"{circ_id}.failed.json")
        tmp_path = out_path + ".tmp"

        # Try to determine failed clause index from reason (common format used elsewhere)
        m = re.search(r"clause_(\d+)_failed", reason or "")
        failed_index = None
        clause_obj = None
        if m:
            idx = int(m.group(1))
            if 0 <= idx < len(clauses):
                failed_index = idx
                clause_obj = clauses[idx]
        elif len(clauses) == 1:
            # If only one clause was supplied, assume that one failed
            failed_index = 0
            clause_obj = clauses[0]

        # Clause key: prefer clause.id if present, else clause_<index>, else random uuid
        if clause_obj is not None:
            clause_key = getattr(clause_obj, "id", None) or f"clause_{failed_index}"
            # Serialise clause
            failed_clause = clause_obj.to_dict() if hasattr(clause_obj, "to_dict") else dict(getattr(clause_obj, "__dict__", {}))
        else:
            clause_key = f"clause_unknown_{uuid.uuid4().hex[:8]}"
            failed_clause = None

        entry = {
            "reason": reason,
            "failed_clause": failed_clause,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        def _read_update_write():
            # load existing file if present
            data = {}
            if os.path.exists(out_path):
                try:
                    with open(out_path, "r", encoding="utf-8") as fh:
                        data = json.load(fh) or {}
                except Exception:
                    # if the existing file is corrupted, back it up and start fresh
                    try:
                        backup = out_path + f".corrupt.{uuid.uuid4().hex[:6]}"
                        os.replace(out_path, backup)
                        logging.getLogger(__name__).warning("Backed up corrupt failed file to %s", backup)
                    except Exception:
                        logging.getLogger(__name__).exception("Failed to back up corrupt failed file %s", out_path)
                    data = {}

            # ensure structure
            if not isinstance(data, dict):
                data = {}

            if "circular_id" not in data:
                data["circular_id"] = circ_id
            if "failed_clauses" not in data or not isinstance(data["failed_clauses"], dict):
                data["failed_clauses"] = {}

            # append entry under clause_key
            lst = data["failed_clauses"].setdefault(clause_key, [])
            lst.append(entry)

            data["updated_at"] = datetime.now(timezone.utc).isoformat()

            # atomic write
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2, default=str)
            os.replace(tmp_path, out_path)

        # Run the blocking file I/O in a thread so we don't block the event loop
        await asyncio.to_thread(_read_update_write)

        log.info("Persisted failed clause %s for circular %s to %s", clause_key, circ_id, out_path)

    except Exception:
        # Never raise to ingestion flow; just log the issue
        log.exception("Failed to persist failure for circular %s", getattr(circular, "id", "unknown"))
