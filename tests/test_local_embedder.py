# tests/test_local_embedder.py

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


import asyncio
from utils.local_embedder import create_embeddings_sync, LocalEmbedder

def test_sync():
    texts = ["hello world", "RBI circular about NDS-OM"]
    vecs = create_embeddings_sync(texts)
    assert isinstance(vecs, list)
    assert len(vecs) == 2
    assert all(isinstance(v, list) for v in vecs)
    print("sync OK -> dims:", [len(v) for v in vecs])

async def test_async():
    e = LocalEmbedder()
    res = await e.create(["first text", "second text"])
    assert isinstance(res, list)
    assert len(res) == 2
    print("async OK -> dims:", [len(v) for v in res])

if __name__ == "__main__":
    test_sync()
    asyncio.run(test_async())
