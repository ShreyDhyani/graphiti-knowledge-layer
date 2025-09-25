# utils/chunker.py
from typing import List, Optional
import re

# Default character sizes (tweak these when you tune)
DEFAULT_CHUNK_CHARS = 3000
DEFAULT_OVERLAP_CHARS = 300
SENTENCE_LOOKAHEAD = 200     # when trying to extend to sentence boundary
WORD_BACKOFF = 40            # max chars to back off to avoid splitting a word

_SENTENCE_END_RE = re.compile(r'[\.!?]\s+|\n+')  # treat newline groups as sentence breaks


def sliding_chunk_text(
    text: str,
    chunk_chars: int = DEFAULT_CHUNK_CHARS,
    overlap_chars: int = DEFAULT_OVERLAP_CHARS,
    preserve_sentences: bool = True,
    min_chunk_size: Optional[int] = None,
) -> List[str]:
    """
    Sliding-window chunker with overlap.

    Parameters
    ----------
    text:
        Input string.
    chunk_chars:
        Target size (chars) of each chunk window.
    overlap_chars:
        How many characters to overlap between adjacent chunks.
    preserve_sentences:
        If True, prefer to end a chunk at a sentence boundary within a
        small lookahead window. Otherwise it will split roughly at chunk_chars.
    min_chunk_size:
        Minimum allowed chunk length. If None, defaults to chunk_chars // 4.

    Behavior
    --------
    - Produces chunks by moving a sliding window of size `chunk_chars`.
    - After yielding a chunk, the next window starts at `end - overlap_chars`.
    - Tries not to split sentences (if preserve_sentences) by extending `end` into
      a small lookahead to the next sentence end (period/question/exclamation or newline).
    - Tries not to split words by backing off a little when cutting in the middle of a word.
    """
    text = (text or "").strip()
    if not text:
        return []

    L = len(text)
    min_chunk_size = min_chunk_size or max(1, chunk_chars // 4)

    chunks: List[str] = []
    start = 0
    seen = 0  # safety counter

    while start < L:
        # Compute naive end
        end = min(start + chunk_chars, L)

        # If preserving sentences, try to extend to the next sentence end within lookahead
        if preserve_sentences and end < L:
            lookahead_end = min(end + SENTENCE_LOOKAHEAD, L)
            lookahead = text[end:lookahead_end]
            m = _SENTENCE_END_RE.search(lookahead)
            if m:
                # extend to just after the matched sentence boundary
                end = end + m.end()

        # Avoid splitting in middle of a word: back off up to WORD_BACKOFF chars
        if end < L and not text[end].isspace():
            backoff_limit = max(start, end - WORD_BACKOFF)
            last_space = text.rfind(" ", backoff_limit, end)
            last_nl = text.rfind("\n", backoff_limit, end)
            cut_pos = max(last_space, last_nl)
            if cut_pos > start:
                end = cut_pos

        # Ensure chunk is not too small; if too small and not at the end, extend
        if (end - start) < min_chunk_size and end < L:
            # extend to at least min_chunk_size (but not past L)
            end = min(start + max(min_chunk_size, chunk_chars), L)

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)

        # Advance start by sliding window (overlap)
        next_start = end - overlap_chars
        # Safety: ensure progress; if next_start <= start, force an advance
        if next_start <= start:
            next_start = end
        start = max(0, next_start)

        # Safety break to avoid infinite loop (very defensive)
        seen += 1
        if seen > 10000:
            # Extremely large document or logic error — stop to avoid hang
            break

    return chunks
