# run_all.py
import argparse
import asyncio
import os
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
    # If graphiti_client isn't present yet, we'll skip pre-check and let mapper fail later.
    get_graphiti = None


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
    # This helps surface missing env vars (GOOGLE_API_KEY / OPENAI_API_KEY / NEO4J_*) early.
    if ingest and get_graphiti is not None:
        print("  -> Validating Graphiti configuration...")
        try:
            # get_graphiti will raise if required env vars are missing or invalid.
            client = get_graphiti()
            # If it returns an instance, we can optionally close it or let mapper create its own.
            # We won't reuse this instance here; this is just a validation step.
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
    args = parser.parse_args()

    run_extraction()
    run_normalization()
    asyncio.run(run_mapping(ingest=args.ingest))
    print("4/4 — All done ✅")


if __name__ == "__main__":
    main()
