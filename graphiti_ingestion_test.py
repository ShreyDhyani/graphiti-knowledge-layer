# graphiti_ingestion_test.py
import os
import asyncio
from datetime import datetime, timezone
from graphiti_core.llm_client.gemini_client import GeminiClient, LLMConfig
from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
from graphiti_core.cross_encoder.gemini_reranker_client import GeminiRerankerClient
from graphiti_core import Graphiti
from graphiti_core.nodes import EpisodeType

async def main():
    # config from env (or hardcode for testing)
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd = os.getenv("NEO4J_PASSWORD", "your_password_here")
    api_key = os.getenv("GOOGLE_API_KEY")

    # initialize Graphiti (simple constructor)
    graphiti = Graphiti(
        uri,
        user,
        pwd,
        llm_client=GeminiClient(
        config=LLMConfig(
            api_key=api_key,
            model="gemini-2.0-flash"
        )
    ),
    embedder=GeminiEmbedder(
        config=GeminiEmbedderConfig(
            api_key=api_key,
            embedding_model="text-embedding-004"
        )
    ),
    cross_encoder=GeminiRerankerClient(
        config=LLMConfig(
            api_key=api_key,
            model="gemini-2.5-flash-lite-preview-06-17"
        )
    )
    )

    # 1) Add a text episode
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
        # result objects contain .content and .metadata
        print("----")
        print("content:", getattr(r, "content", None))
        print("meta:", getattr(r, "metadata", None))

if __name__ == "__main__":
    # ensure your OPENAI_API_KEY is set (or other embedder configured)
    if not os.getenv("OPENAI_API_KEY"):
        print("Warning: OPENAI_API_KEY not set — ingestion may fail if Graphiti needs embeddings/LLM.")
    asyncio.run(main())
