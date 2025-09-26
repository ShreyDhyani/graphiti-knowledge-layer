# console_qa.py
import os
import asyncio
import textwrap
from datetime import datetime, timezone

from graphiti_core import Graphiti
from graphiti_core.nodes import EpisodeType

# Gemini provider bits (shipped with graphiti-core)
from graphiti_core.llm_client.gemini_client import GeminiClient, LLMConfig as GeminiLLMConfig
from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
from graphiti_core.cross_encoder.gemini_reranker_client import GeminiRerankerClient


def _env_bool(name: str, default=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y")


def fmt(s: str | None, width: int = 100) -> str:
    if not s:
        return ""
    s = s.replace("\n", " ").strip()
    return textwrap.shorten(s, width=width, placeholder="…")


async def build_graphiti():
    # --- Required env ---
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd  = os.getenv("NEO4J_PASSWORD", "neo4j")
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        raise RuntimeError("Missing GOOGLE_API_KEY")

    # --- Gemini clients ---
    llm_client = GeminiClient(
        config=GeminiLLMConfig(
            api_key=google_api_key,
            model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
        )
    )
    embedder = GeminiEmbedder(
        config=GeminiEmbedderConfig(
            api_key=google_api_key,
            embedding_model=os.getenv("GEMINI_EMBED_MODEL", "text-embedding-004")
        )
    )
    cross_encoder = GeminiRerankerClient(
        config=GeminiLLMConfig(
            api_key=google_api_key,
            model=os.getenv("GEMINI_RERANKER", "gemini-2.5-flash-lite-preview-06-17")
        )
    )

    print("✅ Graphiti (Gemini) initialised")
    return Graphiti(uri, user, pwd, llm_client=llm_client, embedder=embedder, cross_encoder=cross_encoder)


async def one_query(graphiti: Graphiti, q: str, *, top_k=8, min_score=0.2,
                    include_nodes=True, include_edges=True, include_episodes=True):
    results = await graphiti.search(
        q,
        # top_k=top_k,
        # min_score=min_score,
        # include_nodes=include_nodes,
        # include_edges=include_edges,
        # include_episodes=include_episodes,
        # # You can scope searches if you used grouping or want only text chunks, e.g.:
        # # node_labels=["Organization","Policy"],
        # # episode_sources=[EpisodeType.text],
        # reference_time=datetime.now(timezone.utc),
    )
    if not results:
        print("— no results —")
        return

    # Nicely print, grouped by type
    def header(t): print(f"\n=== {t.upper()} ===")
    printed_any = False

    # Nodes
    node_res = [r for r in results if getattr(r, "type", "") == "node"]
    if node_res:
        header("nodes")
        for r in node_res:
            printed_any = True
            print(f"• {fmt(getattr(r, 'content', None), 120)}  (score={getattr(r,'score',0):.3f})")
            md = getattr(r, "metadata", {}) or {}
            name = md.get("name") or md.get("title")
            labels = md.get("labels")
            print("  name:", name, "| labels:", labels, "| uuid:", md.get("uuid"))

    # Edges
    edge_res = [r for r in results if getattr(r, "type", "") == "edge"]
    if edge_res:
        header("edges")
        for r in edge_res:
            printed_any = True
            fact = fmt(getattr(r, "content", None), 140)
            md = getattr(r, "metadata", {}) or {}
            print(f"• {fact}  (score={getattr(r,'score',0):.3f})")
            print("  src:", md.get("source_node_name"), "| rel:", md.get("name"), "| dst:", md.get("target_node_name"))

    # Episodes (text chunks)
    ep_res = [r for r in results if getattr(r, "type", "") == "episode"]
    if ep_res:
        header("episodes")
        for r in ep_res:
            printed_any = True
            print(f"• {fmt(getattr(r,'content',None), 160)}  (score={getattr(r,'score',0):.3f})")
            md = getattr(r, "metadata", {}) or {}
            print("  uuid:", md.get("uuid"), "| valid_at:", md.get("valid_at"))

    if not printed_any:
        for r in results:
            print("\n---")
            print("type:", getattr(r, "type", None))
            print("content:", getattr(r, "content", None))
            print("metadata:", getattr(r, "metadata", None))
            print("fact:", getattr(r, "fact", None))
        print("— results contained unknown types; try different include_* flags —")


async def main():
    graphiti = await build_graphiti()
    try:
        print("\nAsk me anything about your ingested circulars. Type 'exit' to quit.")
        while True:
            q = input("\nQ > ").strip()
            if not q or q.lower() in ("exit", "quit"):
                break
            await one_query(graphiti, q)
    finally:
        # clean shutdown
        aclose = getattr(graphiti, "aclose", None)
        if callable(aclose):
            await aclose()
        else:
            close = getattr(graphiti, "close", None)
            if callable(close):
                await close()


if __name__ == "__main__":
    asyncio.run(main())
