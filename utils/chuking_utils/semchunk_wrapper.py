# utils/semchunk_wrapper.py
from __future__ import annotations
from typing import List, Optional, Union, Callable

# semchunk is required
import semchunk

# tokenizers are optional; we try them in order and fall back cleanly
def _resolve_token_counter(
    tokenizer: Optional[Union[str, Callable[[str], int]]] = None
) -> Callable[[str], int] | object:
    """
    Return either a semchunk-compatible tokenizer/encoding object or a callable
    that counts tokens. If `tokenizer` is None, try in this order:
      - 'isaacus/kanon-tokenizer' (HF)
      - 'cl100k_base' (tiktoken)
      - simple word-count fallback
    """
    # 1) explicit custom
    if callable(tokenizer):
        return tokenizer
    if isinstance(tokenizer, str):
        return tokenizer  # semchunk.chunkerify accepts model/encoding names

    # 2) try HF tokenizer (kanon)
    try:
        return "isaacus/kanon-tokenizer"
    except Exception:
        pass

    # 3) try tiktoken cl100k_base
    try:
        import tiktoken  # noqa: F401
        return "cl100k_base"
    except Exception:
        pass

    # 4) last resort: word counter callable
    return (lambda text: len(text.split()))


def smart_chunk_text(
    text: str,
    *,
    chunk_size_tokens: int = 300,
    overlap: Union[int, float] = 0,  # int = tokens, float in (0,1) = ratio
    tokenizer: Optional[Union[str, Callable[[str], int]]] = None,
    processes: int = 1,
    return_offsets: bool = False,
) -> List[str] | tuple[List[str], List[tuple[int, int]]]:
    """
    Chunk `text` into semantically sensible spans using semchunk.chunkerify.
    - `chunk_size_tokens`: target token size per chunk
    - `overlap`: absolute token overlap (>=1) or ratio (0<r<1)
    - `tokenizer`: name/counter accepted by semchunk (e.g. 'cl100k_base') or callable
    - `processes`: multiprocessing workers (1 = off)
    - `return_offsets`: also return (start,end) character offsets
    """
    text = (text or "").strip()
    if not text:
        return [] if not return_offsets else ([], [])

    token_counter = _resolve_token_counter(tokenizer)
    chunker = semchunk.chunkerify(token_counter, chunk_size_tokens)
    if chunker is None:
        # Fallback: word counter
        chunker = semchunk.chunkerify(lambda s: len(s.split()), chunk_size_tokens)

    if return_offsets:
        chunks, offsets = chunker(text, overlap=overlap, offsets=True, processes=processes)
        # normalize whitespace
        chunks = [c.strip() for c in chunks if c and c.strip()]
        return chunks, offsets

    chunks = chunker(text, overlap=overlap, processes=processes)
    return [c.strip() for c in chunks if c and c.strip()]
