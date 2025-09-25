"""
graphiti_ingest_mapper.py

Map normalized PDF JSON -> Pydantic models (model.py) -> ingest as Graphiti episodes.

This script DOES NOT attempt to call Graphiti's internal node upsert APIs. Instead it
creates one Episode per Clause/Chunk so Graphiti's extraction pipeline can run (LLM/embedder)
and extract entities/relations according to your Graphiti prompts/schema.

Usage:
  1) Put normalized JSON files in the `normalized/` directory (output from normalize_pdfs.py).
  2) Configure NEO4J_* and optional LLM env vars (OPENAI_API_KEY or GOOGLE_API_KEY) as needed.
  3) Run: python3 graphiti_ingest_mapper.py --ingest

The file contains three main functions:
 - load_normalized_json(path)
 - map_normalized_to_models(data) -> (Circular, List[Clause])
 - ingest_models_as_episodes(graphiti, circular, clauses)

"""
from __future__ import annotations
import os
import json
import argparse
from typing import List, Tuple
from utils.graphiti_client import get_graphiti
from utils.ingest_utils import ingest_models_as_episodes
from utils.model import Circular, Clause
from utils.chuking_utils.semchunk_wrapper import smart_chunk_text
# from utils.chuking_utils.chunk_text import chunk_text
# from utils.chuking_utils.sliding_chunk_text import sliding_chunk_text

# Graphiti imports
from graphiti_core import Graphiti


def load_normalized_json(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as fh:
        return json.load(fh)


def map_normalized_to_models(normalized: dict) -> Tuple[Circular, List[Clause]]:
    meta = normalized.get('metadata', {})
    full_text = normalized.get('normalized_text') or normalized.get('full_text') or ''

    circ = Circular(
        circular_number=meta.get('title') or meta.get('filename'),
        title=meta.get('title'),
        summary=None,
        full_text=full_text,
        issued_date=None,
        issued_by=None,
        source_file=meta.get('filename'),
        pages=meta.get('page_count'),
        metadata={
            'normalized_at': meta.get('normalized_at'),
            'original_filename': meta.get('filename')
        }
    )
    # TODO(B): run heuristic/regex extractors here to pre-populate
    # Organization / Person entities (e.g. extract issuer, signatory)
    # Example: TODO: extract_orgs_and_people(normalized) -> List[Organization], List[Person]

    # If the normalized JSON already includes explicit chunks, use those
    
    # this is normal token based chunking
    # chunks = normalized.get('chunks') or chunk_text(full_text)
    # This is for sliding window based chunking
    # chunks = normalized.get('chunks') or sliding_chunk_text(full_text)

    chunks = normalized.get('chunks') or smart_chunk_text(
        full_text,
        chunk_size_tokens=90,   # tune
        overlap=0.15,            # ~15% overlap for context carry-over
        tokenizer="cl100k_base"           # None / "cl100k_base" / "isaacus/kanon-tokenizer"
    )

    clauses: List[Clause] = []
    for i, ch in enumerate(chunks):
        clause = Clause(
            circular_id=circ.id,
            clause_number=str(i),
            text=ch,
            page_ref=None,
            metadata={
                'source_file': meta.get('filename'),
                'chunk_index': i,
            }
        )
        clauses.append(clause)

    return circ, clauses

async def main(ingest: bool = False):
    # Only process normalized source files, avoid mapped outputs
    files = sorted(
        [
            f
            for f in os.listdir('normalized')
            if f.lower().endswith('.normalized.json') and ".mapped." not in f
        ]
    )
    if not files:
        print("No normalized JSON files found in 'normalized/' — run normalize_pdfs.py first.")
        return

    # If ingest flag set, initialize Graphiti using env creds
    graphiti = None
    if ingest:
        # Create Graphiti instance (this uses your graphiti_client pattern or direct init)
        try:
            graphiti = get_graphiti()
        except Exception:
            # Fallback to direct init if graphiti_client missing
            uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
            user = os.getenv('NEO4J_USER', 'neo4j')
            pwd = os.getenv('NEO4J_PASSWORD', 'password')
            graphiti = Graphiti(uri, user, pwd)

    for fname in files:
        path = os.path.join('normalized', fname)
        print('Processing', fname)

        # load normalized JSON and validate it's the expected mapping
        try:
            norm = load_normalized_json(path)
            if not isinstance(norm, dict):
                print(f" Skipping {fname}: expected JSON object, got {type(norm).__name__}")
                continue
        except Exception as e:
            print(f" Skipping {fname}: failed to load JSON ({e})")
            continue

        # map to models; catch mapping errors and continue with next file
        try:
            circ, clauses = map_normalized_to_models(norm)
        except Exception as e:
            print(f" Error mapping {fname}: {e} — skipping file")
            continue

        # write out mapped JSON for inspection (to a separate folder to avoid re-processing)
        out_mapped_dir = os.path.join('normalized', 'mapped_outputs')
        os.makedirs(out_mapped_dir, exist_ok=True)
        base = os.path.splitext(fname)[0]
        with open(os.path.join(out_mapped_dir, base + '.circular.json'), 'w', encoding='utf-8') as fh:
            json.dump(circ.to_dict(), fh, ensure_ascii=False, indent=2, default=str)
        with open(os.path.join(out_mapped_dir, base + '.clauses.json'), 'w', encoding='utf-8') as fh:
            json.dump([c.to_dict() for c in clauses], fh, ensure_ascii=False, indent=2, default=str)

        print(' Wrote mapped circular + clauses for', fname)

        # ingest only if requested and mapping succeeded
        if ingest and graphiti:
            try:
                print(' Ingesting into Graphiti...')
                await ingest_models_as_episodes(graphiti, circ, clauses)
                print(' Ingest complete for', fname)
            except Exception as e:
                print(f" Ingest failed for {fname}: {e}")
                # don't raise — continue with next file
                continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ingest', action='store_true', help='Also ingest mapped content into Graphiti')
    args = parser.parse_args()
    import asyncio
    asyncio.run(main(ingest=args.ingest))
