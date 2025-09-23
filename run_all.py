# run_all.py
import argparse
import asyncio
import os
import shutil
import sys

# ensure project folder is on path if running from elsewhere
PROJECT_DIR = os.path.dirname(__file__)
sys.path.insert(0, PROJECT_DIR)

# import the functions from the other scripts
from pdf_extract import process_all as pdf_extract_all
from normalize_pdfs import main as normalize_main
from graphiti_ingest_mapper import main as mapper_main  # async

# Optional: pre-check Graphiti client if doing ingestion
try:
    from graphiti_client import get_graphiti
except Exception:
    get_graphiti = None


def clean_dirs():
    """Remove previous pipeline output directories to start fresh."""
    to_remove = ["extracted_text", "normalized", os.path.join("normalized", "mapped_outputs")]
    for p in to_remove:
        if os.path.exists(p):
            try:
                if os.path.isdir(p):
                    shutil.rmtree(p)
                else:
                    os.remove(p)
                print(f"Removed: {p}")
            except Exception as e:
                print(f"Warning: failed to remove {p}: {e}")


def run_extraction():
    print("1/4 — Running PDF extraction...")
    pdf_extract_all()
    print(" -> extraction complete\n")


def run_normalization():
    print("2/4 — Running normalization...")
    normalize_main()
    print(" -> normalization complete\n")


async def run_mapping(ingest: bool):
    print("3/4 — Running mapping and optional ingestion...")

    # If ingest requested, do a quick config validation by initializing Graphiti here.
    if ingest and get_graphiti is not None:
        print("  -> Validating Graphiti configuration...")
        try:
            client = get_graphiti()
            print("  -> Graphiti client validated.")
        except Exception as e:
            print("ERROR: Graphiti client validation failed:", str(e))
            print("Fix your environment variables (NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD and LLM keys) and retry.")
            raise

    await mapper_main(ingest=ingest)
    print(" -> mapping/ingest complete\n")


def main():
    parser = argparse.ArgumentParser(description="Run full pipeline: extract -> normalize -> map -> (optional) ingest")
    parser.add_argument("--ingest", action="store_true", help="Also ingest mapped data into Graphiti (needs env vars)")
    parser.add_argument("--clean", action="store_true", help="Remove previous extracted/normalized data before running")
    args = parser.parse_args()

    if args.clean:
        print("Cleaning previous outputs...")
        clean_dirs()
        print("Clean complete.\n")
        exit()

    run_extraction()
    run_normalization()
    asyncio.run(run_mapping(ingest=args.ingest))
    print("4/4 — All done ✅")


if __name__ == "__main__":
    main()
