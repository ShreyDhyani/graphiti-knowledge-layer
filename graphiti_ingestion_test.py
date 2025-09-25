# graphiti_ingestion_test.py
import os
import asyncio
from datetime import datetime, timezone
from graphiti_core import Graphiti
from graphiti_core.nodes import EpisodeType

# remote provider imports (Gemini) - keep these around if you want remote LLM/embedder
try:
    from graphiti_core.llm_client.gemini_client import GeminiClient, LLMConfig as GeminiLLMConfig
    from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
    from graphiti_core.cross_encoder.gemini_reranker_client import GeminiRerankerClient
except Exception:
    GeminiClient = GeminiLLMConfig = GeminiEmbedder = GeminiEmbedderConfig = GeminiRerankerClient = None

# local embedder adapter (must subclass Graphiti's EmbedderClient)
try:
    from utils.local_embedder import LocalEmbedderClient
except Exception:
    LocalEmbedderClient = None


async def main():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    pwd = os.getenv("NEO4J_PASSWORD")

    use_local = os.getenv("USE_LOCAL_EMBEDDER", "false").lower() in ("1", "true", "yes")
    disable_llm = os.getenv("DISABLE_LLM", "false").lower() in ("1", "true", "yes")
    google_api_key = os.getenv("GOOGLE_API_KEY")


    if use_local:
        if LocalEmbedderClient is None:
            raise RuntimeError("LocalEmbedderClient not found. Ensure utils/local_embedder.py exists and sentence-transformers is installed.")
        embedder = LocalEmbedderClient(model_name=os.getenv("LOCAL_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))
        # When testing local embedder you probably want to avoid remote LLM calls:
        # llm_client = None if disable_llm else None  # keep None unless you wire a local LLM
        llm_client = GeminiClient(config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_MODEL","gemini-2.0-flash")))
        cross_encoder = None
        print("Using LocalEmbedderClient:", embedder)
    else:
        # default: wire Gemini (remote)
        # google_api_key = os.getenv("GOOGLE_API_KEY")
        if not google_api_key:
            raise RuntimeError("GOOGLE_API_KEY required for remote mode")
        llm_client = GeminiClient(config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_MODEL","gemini-2.0-flash")))
        embedder = GeminiEmbedder(config=GeminiEmbedderConfig(api_key=google_api_key, embedding_model=os.getenv("GEMINI_EMBED_MODEL","text-embedding-004")))
        cross_encoder = GeminiRerankerClient(config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_RERANKER","gemini-2.5-flash-lite-preview-06-17")))
        print("Using Gemini provider for embedder+LLM")

    print("✅ Initialising Graphiti client")
    graphiti = Graphiti(uri, user, pwd, llm_client=llm_client, embedder=embedder, cross_encoder=cross_encoder)
    try:
        # 1) Add a text episode
        print("✅ Adding Episode")
        await graphiti.add_episode(
            name="dummy_test_1",
            episode_body="Alice works at Acme Corp since 2021.",
            source=EpisodeType.text,
            source_description="unit test",
            reference_time=datetime.now(timezone.utc),
        )
        print("✅ Episode added")

        # 2) Run a simple search (hybrid search)
        results = await graphiti.search("Who works at Acme?")
        print(f"Found {len(results)} results")
        for r in results:
            print("----")
            print("content:", getattr(r, "content", None))
            print("meta:", getattr(r, "metadata", None))

    finally:
        # close Graphiti to avoid unclosed session warnings
        aclose = getattr(graphiti, "aclose", None)
        if callable(aclose):
            await aclose()
        else:
            close = getattr(graphiti, "close", None)
            if callable(close):
                close()


if __name__ == "__main__":
    asyncio.run(main())
