# # graphiti_client.py
# import os
# from graphiti_core import Graphiti

# # Optional: import different clients/embedder depending on provider
# from graphiti_core.llm_client.gemini_client import GeminiClient, LLMConfig as GeminiLLMConfig
# from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
# from graphiti_core.cross_encoder.gemini_reranker_client import GeminiRerankerClient
# # You can add OpenAI / huggingface initializers here later.

# def get_graphiti():
#     """
#     Initialize and return a Graphiti instance configured from environment variables.
#     Caller is responsible for reusing the returned instance (do not call repeatedly).
#     """
#     uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
#     user = os.getenv("NEO4J_USER", "neo4j")
#     pwd = os.getenv("NEO4J_PASSWORD", "your_password_here")

#     # Which provider to use (gemini | openai | local). Default: gemini
#     provider = os.getenv("GRAPHITI_PROVIDER", "gemini").lower()

#     if provider == "gemini":
#         google_api_key = os.getenv("GOOGLE_API_KEY")
#         if not google_api_key:
#             raise RuntimeError("GOOGLE_API_KEY not set in environment for Gemini provider")

#         llm_client = GeminiClient(
#             config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"))
#         )
#         embedder = GeminiEmbedder(
#             config=GeminiEmbedderConfig(api_key=google_api_key, embedding_model=os.getenv("GEMINI_EMBED_MODEL", "text-embedding-004"))
#         )
#         cross_encoder = GeminiRerankerClient(
#             config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_RERANKER", "gemini-2.5-flash-lite-preview-06-17"))
#         )

#         graphiti = Graphiti(
#             uri,
#             user,
#             pwd,
#             llm_client=llm_client,
#             embedder=embedder,
#             cross_encoder=cross_encoder,
#         )
#         return graphiti

#     # Example: OpenAI branch (keeps Graphiti defaults if set)
#     if provider == "openai":
#         # Graphiti will pick up OPENAI_API_KEY from env by default; just init Graphiti
#         return Graphiti(uri, user, pwd)

#     # Add other providers (huggingface/local) here as needed
#     raise RuntimeError(f"Unsupported GRAPHITI_PROVIDER={provider}")

# graphiti_client.py
from __future__ import annotations
import os
from typing import Optional

from graphiti_core import Graphiti

# Optional provider imports (Gemini)
try:
    from graphiti_core.llm_client.gemini_client import GeminiClient, LLMConfig as GeminiLLMConfig
    from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
    from graphiti_core.cross_encoder.gemini_reranker_client import GeminiRerankerClient
except Exception:
    GeminiClient = GeminiLLMConfig = GeminiEmbedder = GeminiEmbedderConfig = GeminiRerankerClient = None

# Optional local embedder
try:
    from utils.local_embedder import LocalEmbedder
except Exception:
    LocalEmbedder = None


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y")


def get_graphiti(
    uri: Optional[str] = None,
    user: Optional[str] = None,
    pwd: Optional[str] = None,
    use_local_embedder: Optional[bool] = None,
    disable_llm: Optional[bool] = None,
):
    """
    Create and return a configured Graphiti instance.

    Controls via env:
      - GRAPHITI_PROVIDER (gemini | openai | local)  default: gemini
      - USE_LOCAL_EMBEDDER (true/false)               default: false
      - DISABLE_LLM (true/false)                      default: false
      - GOOGLE_API_KEY / OPENAI_API_KEY                as needed by provider
      - LOCAL_EMBED_MODEL (optional)                   model name for local embedder
    """
    uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = user or os.getenv("NEO4J_USER", "neo4j")
    pwd = pwd or os.getenv("NEO4J_PASSWORD", "neo4j")

    provider = os.getenv("GRAPHITI_PROVIDER", "gemini").lower()
    if use_local_embedder is None:
        use_local_embedder = _bool_env("USE_LOCAL_EMBEDDER", False)
    if disable_llm is None:
        disable_llm = _bool_env("DISABLE_LLM", False)

    llm_client = None
    embedder = None
    cross_encoder = None

    # Local embedder branch (explicit or via provider=local)
    if use_local_embedder or provider == "local":
        if LocalEmbedder is None:
            raise RuntimeError("LocalEmbedder not available. Install sentence-transformers and ensure utils/local_embedder.py exists.")
        model_name = os.getenv("LOCAL_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
        embedder = LocalEmbedder(model_name=model_name)

        # If LLM not disabled, try wiring provider LLM (Gemini preferred)
        if not disable_llm:
            google_api_key = os.getenv("GOOGLE_API_KEY")
            if google_api_key and GeminiClient is not None:
                llm_client = GeminiClient(config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash")))
                try:
                    cross_encoder = GeminiRerankerClient(config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_RERANKER", "gemini-2.5-flash-lite-preview-06-17")))
                except Exception:
                    cross_encoder = None
            else:
                # no remote LLM configured; keep llm_client None (extraction disabled)
                llm_client = None

    # Non-local provider branch (Gemini by default)
    elif provider == "gemini":
        google_api_key = os.getenv("GOOGLE_API_KEY")
        if not google_api_key:
            raise RuntimeError("GOOGLE_API_KEY not set in environment for Gemini provider")
        if not disable_llm:
            llm_client = GeminiClient(config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash")))
        embedder = GeminiEmbedder(config=GeminiEmbedderConfig(api_key=google_api_key, embedding_model=os.getenv("GEMINI_EMBED_MODEL", "text-embedding-004")))
        try:
            cross_encoder = GeminiRerankerClient(config=GeminiLLMConfig(api_key=google_api_key, model=os.getenv("GEMINI_RERANKER", "gemini-2.5-flash-lite-preview-06-17")))
        except Exception:
            cross_encoder = None

    # OpenAI branch (basic)
    elif provider == "openai":
        # Graphiti will pick OPENAI_API_KEY from env by default; do not explicitly construct here.
        # Pass None so Graphiti will configure its default OpenAI client if supported.
        llm_client = None
        embedder = None

    else:
        raise RuntimeError(f"Unsupported GRAPHITI_PROVIDER={provider}")

    graphiti = Graphiti(uri, user, pwd, llm_client=llm_client, embedder=embedder, cross_encoder=cross_encoder)
    return graphiti
