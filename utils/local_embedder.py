# utils/local_embedder.py
from __future__ import annotations
import asyncio
from typing import List, Optional
import os

# sentence-transformers (lazy)
try:
    from sentence_transformers import SentenceTransformer
except Exception:
    SentenceTransformer = None

# Import Graphiti's EmbedderClient base class (v0.20.4 uses this path)
try:
    from graphiti_core.embedder.client import EmbedderClient
except Exception:
    EmbedderClient = object  # fallback (shouldn't occur with graphiti-core installed)

_DEFAULT_MODEL = os.getenv("LOCAL_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

class LocalEmbedder:
    """Helper wrapper around sentence-transformers encode (sync model used via threadpool)."""
    def __init__(self, model_name: str = _DEFAULT_MODEL):
        if SentenceTransformer is None:
            raise RuntimeError("sentence-transformers not installed. Run: pip install sentence-transformers")
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)

    async def create(self, input_data: List[str]) -> List[List[float]]:
        loop = asyncio.get_running_loop()
        def _encode(texts):
            # convert_to_numpy True returns np.array; we'll convert to list below
            return self.model.encode(texts, show_progress_bar=False, convert_to_numpy=True)
        embs = await loop.run_in_executor(None, _encode, input_data)
        # Always return list-of-lists of plain Python floats
        return [[float(x) for x in v] for v in (embs.tolist() if hasattr(embs, "tolist") else embs)]

    async def embed(self, input_data: List[str]) -> List[List[float]]:
        return await self.create(input_data)


class LocalEmbedderClient(EmbedderClient if EmbedderClient is not object else object):
    """
    Adapter implementing the Graphiti EmbedderClient interface by delegating
    to LocalEmbedder. Pass an instance of this class into Graphiti(...)
    as the embedder argument.
    """
    def __init__(self, model_name: Optional[str] = None):
        model_name = model_name or _DEFAULT_MODEL
        self._impl = LocalEmbedder(model_name=model_name)

    async def create(self, input_data: List[str]):
        """
        Graphiti's search expects a *flat* vector (1-D list of floats) when
        only one text is passed. For multiple texts, return a list of vectors.
        """
        vecs = await self._impl.create(input_data)
        if len(vecs) == 1:
            return vecs[0]  # flatten to [d] for Neo4j cosine() queries
        return vecs

    async def create_batch(self, texts: List[str]):
        # Always return list-of-lists for batch operations
        return await self._impl.create(texts)

    async def embed(self, input_data: List[str]):
        # Mirror create() behavior for consistency
        return await self.create(input_data)

    def __repr__(self):
        return f"<LocalEmbedderClient model={self._impl.model_name}>"
