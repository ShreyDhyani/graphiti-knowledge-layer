# utils/local_embedder.py
"""
Local embedder for Graphiti ingestion (sentence-transformers).

Provides:
  - async create(input_data: List[str]) -> List[List[float]]
  - async embed(input_data: List[str]) -> List[List[float]]  (alias)
  - create_embeddings_sync(texts, model_name) -> List[List[float]] (convenience)

Usage:
  from utils.local_embedder import LocalEmbedder
  embedder = LocalEmbedder(model_name="sentence-transformers/all-MiniLM-L6-v2")
  vectors = await embedder.create(["hello world", "another text"])
"""
from __future__ import annotations
import asyncio
from typing import List, Optional
import os

# Lazy import to avoid heavy startup if module imported but not used
try:
    from sentence_transformers import SentenceTransformer
except Exception:  # pragma: no cover
    SentenceTransformer = None  # will raise on use

_DEFAULT_MODEL = os.getenv("LOCAL_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")


class LocalEmbedder:
    def __init__(self, model_name: str = _DEFAULT_MODEL):
        if SentenceTransformer is None:
            raise RuntimeError("sentence-transformers not installed. Run: pip install sentence-transformers")
        self.model_name = model_name
        # load synchronously once (may take a while); callers should construct once and reuse
        self.model = SentenceTransformer(model_name)

    async def create(self, input_data: List[str]) -> List[List[float]]:
        """
        Async wrapper around SentenceTransformer.encode using a threadpool.
        Returns a list of lists (float).
        """
        loop = asyncio.get_running_loop()
        # SentenceTransformer.encode signature varies; we pass show_progress_bar=False to avoid stdout spam
        def _encode(texts):
            # convert_to_numpy True for newer versions gives numpy array; we convert to lists below
            return self.model.encode(texts, show_progress_bar=False, convert_to_numpy=True)

        embs = await loop.run_in_executor(None, _encode, input_data)
        # convert numpy arrays to lists safely
        try:
            return embs.tolist()
        except Exception:
            return [list(e) for e in embs]

    # alias some Graphiti variants expect
    async def embed(self, input_data: List[str]) -> List[List[float]]:
        return await self.create(input_data)


# convenience sync function (useful for quick local tests)
def create_embeddings_sync(texts: List[str], model_name: Optional[str] = None) -> List[List[float]]:
    if SentenceTransformer is None:
        raise RuntimeError("sentence-transformers not installed. Run: pip install sentence-transformers")
    model_name = model_name or _DEFAULT_MODEL
    model = SentenceTransformer(model_name)
    embs = model.encode(texts, show_progress_bar=False, convert_to_numpy=True)
    try:
        return embs.tolist()
    except Exception:
        return [list(e) for e in embs]
