from __future__ import annotations
from typing import List, Tuple
import traceback
from ..model import Circular, Clause
from ..chuking_utils.semchunk_wrapper import smart_chunk_text
from ..chuking_utils.num_tokens_from_string import num_tokens_from_string

def _fallback_chunk_text(text: str, chunk_chars: int = 3000) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    out, i, L = [], 0, len(text)
    while i < L:
        end = min(i + chunk_chars, L)
        look = text[end:min(end+200, L)]
        # try to end at next sentence break/newline within a small window
        j = -1
        for k, ch in enumerate(look):
            if ch in ".\n":
                j = k + 1
                break
        if j > 0:
            end += j
        out.append(text[i:end].strip())
        i = end
    return out

def map_normalized_to_models_func(normalized: dict) -> Tuple[Circular, List[Clause]]:
    """
    Convert a normalized PDF JSON record into a Circular model and a list of Clause models.

    Parameters
    ----------
    normalized : dict
        Output of `normalize_pdfs.py` for a single PDF. Must contain:
        - "metadata": basic document info (title, filename, page_count, etc.)
        - "normalized_text" or "full_text": the cleaned text of the document
        - Optional "chunks": pre-chunked text segments (if not provided, text will be
          token-chunked automatically).

    Returns
    -------
    Tuple[Circular, List[Clause]]
        * Circular: a Pydantic model representing the entire circular,
          including metadata and full_text.
        * List[Clause]: a list of Pydantic Clause models, one per text chunk,
          each carrying chunk text and indexing metadata for ingestion.

    Notes
    -----
    - If `normalized["chunks"]` is absent, this function falls back to
      `smart_chunk_text` to create overlapping semantic chunks.
    - The returned models are ready for ingestion into Graphiti with
      `ingest_models_as_episodes`.
    """
    meta = normalized.get('metadata', {}) or {}
    full_text = normalized.get('normalized_text') or normalized.get('full_text') or ''
    segments = normalized.get('segments') or []

    circ = Circular(
        circular_number=meta.get('title') or meta.get('filename'),
        title=meta.get('title'),
        summary=None,
        full_text=full_text,
        issued_date=meta.get('date'),  # keep if your normalizer sets this
        issued_by=None,
        source_file=meta.get('filename'),
        pages=meta.get('page_count'),
        metadata={
            'normalized_at': meta.get('normalized_at'),
            'original_filename': meta.get('filename'),
            'has_segments': bool(segments),
            'has_chunks': bool(normalized.get('chunks')),
        }
    )

    clauses: List[Clause] = []

    # 1) Prefer structured segments (keeps page/type)
    if segments:
        print(f"[mapper] Using {len(segments)} segments")
        for i, seg in enumerate(segments):
            text = (seg.get('text') or '').strip()
            if not text:
                continue
            clauses.append(
                Clause(
                    circular_id=circ.id,
                    clause_number=str(i),
                    text=text,
                    page_ref=seg.get('page'),
                    metadata={
                        'source_file': meta.get('filename'),
                        'chunk_index': i,
                        'block_type': seg.get('type'),
                        'format': seg.get('format'),
                        'markdown_preview': seg.get('markdown_preview'),
                    }
                )
            )
        return circ, clauses

    # 2) Fallback to existing chunks if present
    if normalized.get('chunks'):
        chunks = [ (c or '').strip() for c in normalized['chunks'] if (c or '').strip() ]
        print(f"[mapper] Using existing chunks: {len(chunks)}")
        for i, ch in enumerate(chunks):
            clauses.append(
                Clause(
                    circular_id=circ.id,
                    clause_number=str(i),
                    text=ch,
                    page_ref=None,
                    metadata={
                        'source_file': meta.get('filename'),
                        'chunk_index': i,
                        'block_type': 'paragraph',
                    }
                )
            )
        return circ, clauses

    # 3) Last resort: smart chunking → naive fallback
    chunks: List[str] = []
    if full_text:
        try:
            print("[mapper] Chunking with semchunk...")
            chunks = smart_chunk_text(
                full_text,
                chunk_size_tokens=90,   # tune for your pipeline
                overlap=0.15,           # ~15% overlap
                tokenizer="cl100k_base"
            ) or []
            token_length = num_tokens_from_string("tiktoken is great!", "cl100k_base")
            print(f"[mapper] semchunk produced {len(chunks)} chunks  token_length => ${token_length}")
        except Exception:
            print("[mapper] semchunk failed; falling back to naive chunker")
            traceback.print_exc()
            chunks = _fallback_chunk_text(full_text, chunk_chars=3000)

    for i, ch in enumerate(chunks):
        clauses.append(
            Clause(
                circular_id=circ.id,
                clause_number=str(i),
                text=(ch or '').strip(),
                page_ref=None,
                metadata={
                    'source_file': meta.get('filename'),
                    'chunk_index': i,
                    'block_type': 'paragraph',
                }
            )
        )

    print(f"[mapper] Final clauses: {len(clauses)}")
    return circ, clauses
