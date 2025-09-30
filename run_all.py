# run_all.py
import argparse
import asyncio
import os
import shutil
import sys
import time
from datetime import timedelta

# ensure project folder is on path if running from elsewhere
PROJECT_DIR = os.path.dirname(__file__)
sys.path.insert(0, PROJECT_DIR)

# import the functions from the other scripts
from utils.normalize_pdfs import main as normalize_main
from graphiti_ingest_mapper import main as mapper_main  # async



def clean_dirs():
    """Remove previous pipeline output directories to start fresh."""
    to_remove = ["normalized", os.path.join("normalized", "mapped_outputs")]
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


def _format_elapsed(seconds: float) -> str:
    """Return H:MM:SS.mmm from seconds."""
    td = timedelta(seconds=int(seconds))
    ms = int((seconds - int(seconds)) * 1000)
    return f"{str(td)}.{ms:03d}s"


def run_normalization():
    print("1/3 — Running normalization...")
    start = time.perf_counter()
    normalize_main()
    elapsed = time.perf_counter() - start
    print(f" -> normalization complete ({_format_elapsed(elapsed)})\n")
    return elapsed


async def run_mapping(ingest: bool, bulk: bool):
    print("2/3 — Running mapping and optional ingestion...")
    print(f"  -> ingest flag: {ingest}, bulk flag: {bulk}")

    start = time.perf_counter()
    # Delegate to mapper_main; pass both flags so the mapper can choose the bulk or per-clause path.
    await mapper_main(ingest=ingest, bulk=bulk)
    elapsed = time.perf_counter() - start
    print(f" -> mapping/ingest complete ({_format_elapsed(elapsed)})\n")
    return elapsed


def main():
    parser = argparse.ArgumentParser(description="Run full pipeline: normalize -> map -> (optional) ingest")
    parser.add_argument("--ingest", action="store_true", help="Also ingest mapped data into Graphiti (needs env vars)")
    parser.add_argument("--bulk", action="store_true", help="Use bulk ingestion path inside mapper")
    parser.add_argument("--clean", action="store_true", help="Remove previous normalized data before running")
    args = parser.parse_args()

    if args.clean:
        print("Cleaning previous outputs...")
        clean_dirs()
        print("Clean complete.\n")
        exit()

    overall_start = time.perf_counter()

    norm_time = run_normalization()
    mapping_time = asyncio.run(run_mapping(ingest=args.ingest, bulk=args.bulk))

    total_elapsed = time.perf_counter() - overall_start

    print("3/3 — All done ✅")
    print(f"Timing summary:")
    print(f"  - Normalization: {_format_elapsed(norm_time)}")
    print(f"  - Mapping/ingest: {_format_elapsed(mapping_time)}")
    print(f"  - Total run time: {_format_elapsed(total_elapsed)} ({total_elapsed:.2f}s)")


if __name__ == "__main__":
    main()
