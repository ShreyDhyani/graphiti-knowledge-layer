# graphiti_client.py
import os
from graphiti_core import Graphiti

# Optional: import different clients/embedder depending on provider
from graphiti_core.llm_client.gemini_client import GeminiClient, LLMConfig as GeminiLLMConfig
from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
from graphiti_core.cross_encoder.gemini_reranker_client import GeminiRerankerClient
# You can add OpenAI / huggingface initializers here later.

def get_graphiti():
    """
    Initialize and return a Graphiti instance configured from environment variables.
    Caller is responsible for reusing the returned instance (do not call repeatedly).
    """
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd = os.getenv("NEO4J_PASSWORD", "your_password_here")

    # Which provider to use (gemini | openai | local). Default: gemini
    provider = os.getenv("GRAPHITI_PROVIDER", "gemini").lower()

    if provider == "gemini":
        google_api_key = os.getenv("GOOGLE_API_KEY")
        if not google_api_key:
            raise RuntimeError("GOOGLE_API_KEY not set in environment for Gemini provider")

        llm_client = GeminiClient(
            config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"))
        )
        embedder = GeminiEmbedder(
            config=GeminiEmbedderConfig(api_key=google_api_key, embedding_model=os.getenv("GEMINI_EMBED_MODEL", "text-embedding-004"))
        )
        cross_encoder = GeminiRerankerClient(
            config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_RERANKER", "gemini-2.5-flash-lite-preview-06-17"))
        )

        graphiti = Graphiti(
            uri,
            user,
            pwd,
            llm_client=llm_client,
            embedder=embedder,
            cross_encoder=cross_encoder,
        )
        return graphiti

    # Example: OpenAI branch (keeps Graphiti defaults if set)
    if provider == "openai":
        # Graphiti will pick up OPENAI_API_KEY from env by default; just init Graphiti
        return Graphiti(uri, user, pwd)

    # Add other providers (huggingface/local) here as needed
    raise RuntimeError(f"Unsupported GRAPHITI_PROVIDER={provider}")
