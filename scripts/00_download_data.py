"""
Download Microsoft Building Footprints GeoJSON files for all US states.

Each state is downloaded as a .zip from Microsoft's CDN and extracted
into SOURCE_DATA_DIR.

Usage:
    # Download all states
    python scripts/00_download_data.py

    # Download only the small test states (Delaware, RhodeIsland, Vermont)
    python scripts/00_download_data.py --test

    # Download specific states
    python scripts/00_download_data.py --state Delaware California Texas
"""

import argparse
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import (
    ALL_STATES,
    TEST_STATES,
    SOURCE_DATA_DIR,
    DOWNLOAD_BASE_URL,
)


def download_state(state_name: str, dest_dir: Path) -> bool:
    """
    Download and extract a single state's GeoJSON zip file.

    Returns True on success, False on failure.
    """
    geojson_path = dest_dir / f"{state_name}.geojson"
    if geojson_path.exists():
        size_mb = geojson_path.stat().st_size / (1024 * 1024)
        print(f"  [SKIP] {state_name}.geojson already exists ({size_mb:.1f} MB)")
        return True

    zip_url = f"{DOWNLOAD_BASE_URL}/{state_name}.geojson.zip"
    zip_path = dest_dir / f"{state_name}.geojson.zip"

    try:
        # Download
        print(f"  Downloading {state_name}.geojson.zip ...", end=" ", flush=True)
        t0 = time.time()
        urllib.request.urlretrieve(zip_url, zip_path)
        dl_time = time.time() - t0
        size_mb = zip_path.stat().st_size / (1024 * 1024)
        print(f"({size_mb:.1f} MB in {dl_time:.0f}s)", flush=True)

        # Extract
        print(f"  Extracting {state_name}.geojson.zip ...", end=" ", flush=True)
        t0 = time.time()
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(dest_dir)
        ex_time = time.time() - t0
        print(f"done ({ex_time:.0f}s)")

        # Clean up zip
        zip_path.unlink(missing_ok=True)

        # Verify extracted file exists
        if geojson_path.exists():
            final_mb = geojson_path.stat().st_size / (1024 * 1024)
            print(f"  OK: {state_name}.geojson ({final_mb:.1f} MB)")
            return True
        else:
            print(f"  WARNING: {state_name}.geojson not found after extraction")
            return False

    except Exception as e:
        print(f"\n  ERROR downloading {state_name}: {e}")
        zip_path.unlink(missing_ok=True)
        return False


def run(states: list[str] | None = None, test_only: bool = False):
    dest_dir = Path(SOURCE_DATA_DIR)
    dest_dir.mkdir(parents=True, exist_ok=True)

    if states:
        target_states = states
    elif test_only:
        target_states = TEST_STATES
    else:
        target_states = ALL_STATES

    print("\n" + "=" * 60)
    print("DOWNLOADING MICROSOFT BUILDING FOOTPRINTS")
    print("=" * 60)
    print(f"  States to download : {len(target_states)}")
    print(f"  Destination        : {dest_dir}\n")

    results = {}
    t0_all = time.time()

    for idx, state in enumerate(target_states, 1):
        print(f"\n[{idx}/{len(target_states)}] {state}")
        ok = download_state(state, dest_dir)
        results[state] = ok

    elapsed = time.time() - t0_all
    succeeded = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)

    print("\n" + "=" * 60)
    print(f"DOWNLOAD COMPLETE  ({elapsed / 60:.1f} min)")
    print(f"  Succeeded: {succeeded}/{len(target_states)}")
    if failed:
        print(f"  Failed   : {failed}")
        for state, ok in results.items():
            if not ok:
                print(f"    - {state}")
    print("=" * 60 + "\n")


def parse_args():
    p = argparse.ArgumentParser(description="Download MS Building Footprints")
    p.add_argument("--state", nargs="+", help="Specific state(s) to download")
    p.add_argument("--test", action="store_true", help="Only download test states")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(states=args.state, test_only=args.test)
