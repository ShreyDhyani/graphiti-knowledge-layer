from typing import List

# Default chunk size used when splitting large text blocks
CHUNK_SIZE = 3000

def chunk_text(text: str, chunk_chars: int = CHUNK_SIZE) -> List[str]:
    """
    Split text into approximately chunk_chars-sized pieces, attempting to avoid
    splitting mid-sentence by extending to the next newline or period (within
    a small lookahead window).
    """
    text = (text or '').strip()
    if not text:
        return []
    chunks: List[str] = []
    i = 0
    L = len(text)
    while i < L:
        end = min(i + chunk_chars, L)
        # try to not split mid-sentence: extend to next newline/period within 200 chars
        lookahead = text[end:min(end + 200, L)]
        if lookahead:
            import re
            m = re.search(r'[\n\.]\s', lookahead)
            if m:
                end += m.end()
        chunks.append(text[i:end].strip())
        i = end
    return chunks


