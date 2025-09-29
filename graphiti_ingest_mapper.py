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
from utils.graphiti_client import get_graphiti
from utils.ingest_utils import ingest_models_as_episodes
from utils.normalisation_utils.map_normalized_to_models import map_normalized_to_models_func
# Graphiti imports
from graphiti_core import Graphiti

def load_normalized_json(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as fh:
        return json.load(fh)

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
            print(f" Custom Graphiti client created !!");
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
            circ, clauses = map_normalized_to_models_func(norm)
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
                print('Ingesting into Graphiti...')
                await ingest_models_as_episodes(graphiti, circ, clauses)
                print('Ingest complete for', fname)
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
