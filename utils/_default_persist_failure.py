from __future__ import annotations
import logging
import os
import json
from typing import Any, List
from datetime import datetime, timezone

log = logging.getLogger(__name__)


async def default_persist_failure(circular: Any, clauses: List[Any], reason: str) -> None:
    os.makedirs("failed", exist_ok=True)
    out_path = os.path.join("failed", f"{getattr(circular, 'id', 'unknown')}.failed.json")
    payload = {
        "reason": reason,
        "circular": circular.to_dict() if hasattr(circular, "to_dict") else dict(getattr(circular, '__dict__', {})),
        "clauses": [c.to_dict() if hasattr(c, "to_dict") else dict(getattr(c, '__dict__', {})) for c in clauses],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
    log.info("Persisted failed payload to %s", out_path)