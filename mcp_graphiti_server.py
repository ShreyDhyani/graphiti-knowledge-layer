# mcp_graphiti_server.py
from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, Body, HTTPException
from fastapi_mcp import FastApiMCP

from graphiti_core import Graphiti

# Gemini clients from graphiti-core
from graphiti_core.llm_client.gemini_client import GeminiClient, LLMConfig as GeminiLLMConfig
from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
from graphiti_core.cross_encoder.gemini_reranker_client import GeminiRerankerClient

# --------- FastAPI app + MCP wrapper ---------
app = FastAPI(title="Graphiti MCP Server")
mcp = FastApiMCP(app)
mcp.mount_http()  # exposes MCP at /mcp

# --------- Globals ---------
_graphiti: Optional[Graphiti] = None

async def _init_graphiti() -> Graphiti:
    """Build a Graphiti client using Gemini LLM + embedder + cross encoder."""
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd = os.getenv("NEO4J_PASSWORD", "neo4j")
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        raise RuntimeError("GOOGLE_API_KEY is required for Gemini provider.")

    llm_client = GeminiClient(
        config=GeminiLLMConfig(
            api_key=google_api_key,
            model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
        )
    )
    embedder = GeminiEmbedder(
        config=GeminiEmbedderConfig(
            api_key=google_api_key,
            embedding_model=os.getenv("GEMINI_EMBED_MODEL", "text-embedding-004"),
        )
    )
    cross_encoder = GeminiRerankerClient(
        config=GeminiLLMConfig(
            api_key=google_api_key,
            model=os.getenv("GEMINI_RERANKER", "gemini-2.5-flash-lite-preview-06-17"),
        )
    )

    return Graphiti(uri, user, pwd, llm_client=llm_client, embedder=embedder, cross_encoder=cross_encoder)


def to_dict(obj):
    # works for pydantic, dataclasses, or plain objects
    if hasattr(obj, "model_dump"):   # pydantic v2
        return obj.model_dump()
    if hasattr(obj, "dict"):         # pydantic v1
        return obj.dict()
    return getattr(obj, "__dict__", {}) or {}

def normalize_results(results):
    out = []
    for r in results or []:
        d = to_dict(r)

        # Heuristic: if it has a `fact`, treat as edge-fact result
        if "fact" in d:
            out.append({
                "type": "fact",
                "uuid": d.get("uuid"),
                "group_id": d.get("group_id"),
                "name": d.get("name"),                       # relation label (e.g., HAS_ACCESS_TO)
                "fact": d.get("fact"),
                "source_node_uuid": d.get("source_node_uuid"),
                "target_node_uuid": d.get("target_node_uuid"),
                "episodes": d.get("episodes") or [],
                "created_at": d.get("created_at"),
                "valid_at": d.get("valid_at"),
                "invalid_at": d.get("invalid_at"),
                "expired_at": d.get("expired_at"),
                "attributes": d.get("attributes") or {},
                "score": d.get("score"),                     # present on some paths
            })
            continue

        # If you also want episodes/nodes, keep them too (optional)
        t = d.get("type") or getattr(r, "type", None)
        if t in ("node", "episode", "edge"):
            out.append({**d, "type": t})
    return out



# --------- Lifecycle ---------
@app.on_event("startup")
async def _startup():
    global _graphiti
    _graphiti = await _init_graphiti()


@app.on_event("shutdown")
async def _shutdown():
    global _graphiti
    if _graphiti is None:
        return
    aclose = getattr(_graphiti, "aclose", None)
    if callable(aclose):
        await aclose()
    else:
        close = getattr(_graphiti, "close", None)
        if callable(close):
            await close()
    _graphiti = None


# --------- Health (non-MCP) ----------
@app.get("/health")
async def health():
    return {"ok": True}


# --------- MCP Tools (each route with operation_id becomes a tool) ----------

@app.post("/graphiti/search", operation_id="graphiti_search")
async def graphiti_search(
    body: Dict[str, Any] = Body(
        ...,
        example={
            "query": "What is NDS-OM?",
        },
    )
):
    """
    MCP Tool: graphiti_search
    Search Graphiti for nodes/edges/episodes. Returns a list of results with scores and metadata.
    """
    global _graphiti
    if _graphiti is None:
        raise HTTPException(503, "Graphiti not initialized")

    q: str = (body.get("query") or "").strip()
    if not q:
        raise HTTPException(400, "query is required")

    # Note: graphiti.search signature in 0.20.x doesn't take top_k/min_score directly.
    # It internally runs hybrid searches; we keep it simple here.
    try:
        results = await _graphiti.search(
            q,
        )
        facts_first = normalize_results(results)
        facts_only = [item for item in facts_first if item["type"] == "fact"]

    except Exception as e:
        raise HTTPException(500, f"Graphiti search failed: {e}")

    return {"results": [facts_only]}
