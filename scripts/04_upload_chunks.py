"""
Upload pre-built chunk files from disk into PostGIS.

Reads chunk GeoJSON files created by 03_create_chunks.py from:
    data/chunks/{state}/chunk_{N}.geojson

and loads them into the `buildings` table using geopandas + psycopg2
with ST_GeomFromText (WKT).

Loading approach
----------------
1. Read each chunk with geopandas (gives us Shapely geometries).
2. Run integrity checks — count invalid, non-polygon, zero-area geometries.
3. Convert valid geometries to WKT.
4. Batch-insert via psycopg2 execute_values with ST_GeomFromText.

Key design decisions
--------------------
* Each chunk is a self-contained GeoJSON FeatureCollection that already
  has state / source_file / chunk_number in its properties.
* Each chunk is logged to the `load_progress` table for auditability.
* A ``--resume`` flag skips chunks that already completed.
* If a chunk fails, the error is recorded and re-raised so the operator
  can inspect and resume from the failed chunk.

Usage:
    # Upload all chunks found in data/chunks/
    python scripts/04_upload_chunks.py

    # Upload chunks for a single state
    python scripts/04_upload_chunks.py --state California

    # Resume after a crash (skip completed chunks)
    python scripts/04_upload_chunks.py --resume

    # Only upload the small test states (Delaware, Rhode Island, Vermont)
    python scripts/04_upload_chunks.py --test
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import warnings
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
from psycopg2.extras import execute_values
from config import (
    DB_CONNECTION,
    CHUNKS_DIR,
    LOGS_DIR,
    BATCH_SIZE,
    TEST_STATES,
)

SRID = 4326  # MS Building Footprints CRS


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(LOGS_DIR) / "upload_chunks.log"),
    ],
)
log = logging.getLogger("upload_chunks")


# ─────────────────────────────────────────────────────────────
# Database helpers
# ─────────────────────────────────────────────────────────────

def get_connection():
    """Return a new psycopg2 connection."""
    return psycopg2.connect(DB_CONNECTION)


def log_progress(conn, state, chunk_num, features, status, error=None):
    """Insert or update a row in load_progress for monitoring."""
    with conn.cursor() as cur:
        if status == "loading":
            cur.execute(
                """
                INSERT INTO load_progress
                    (state, chunk_number, features_in_chunk, status)
                VALUES (%s, %s, %s, %s)
                """,
                (state, chunk_num, features, status),
            )
        else:
            cur.execute(
                """
                UPDATE load_progress
                SET status = %s,
                    error_message = %s,
                    completed_at = NOW()
                WHERE state = %s
                  AND chunk_number = %s
                  AND status = 'loading'
                """,
                (status, error, state, chunk_num),
            )
    conn.commit()


def get_completed_chunks(conn, state):
    """Return set of chunk numbers already marked completed."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT chunk_number
            FROM load_progress
            WHERE state = %s AND status = 'completed'
            """,
            (state,),
        )
        return {row[0] for row in cur.fetchall()}


def get_feature_count_from_db(conn, state):
    """Return how many features are in `buildings` for the given state."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM buildings WHERE state = %s",
            (state,),
        )
        return cur.fetchone()[0]


# ─────────────────────────────────────────────────────────────
# Integrity checks  (from reference implementation)
# ─────────────────────────────────────────────────────────────

def check_integrity(gdf):
    """
    Run quick sanity checks on a GeoDataFrame before uploading.

    Returns (total_rows, invalid_count, non_polygon_count, bad_area_count).
    """
    count_rows = len(gdf)
    count_invalid = int((~gdf.geometry.is_valid).sum())
    count_nonpolygons = int(
        gdf.geometry.map(lambda geom: not isinstance(geom, Polygon)).sum()
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)  # suppress CRS area warning
        areas = gdf.geometry.area
    count_bad_area = int((pd.isnull(areas) | (areas == 0)).sum())
    return count_rows, count_invalid, count_nonpolygons, count_bad_area


# ─────────────────────────────────────────────────────────────
# Core loader  (geopandas → WKT → ST_GeomFromText)
# ─────────────────────────────────────────────────────────────

def _load_chunk(chunk_file: Path, state_name: str, chunk_number: int, conn):
    """
    Load a single chunk GeoJSON into PostGIS.

    1. Read with geopandas.
    2. Check integrity and log warnings.
    3. Extract WKT from each geometry.
    4. Batch-insert via execute_values + ST_GeomFromText.
    """
    # ── 1. Read with geopandas ───────────────────────────────
    gdf = gpd.read_file(chunk_file)
    source_file = f"{state_name}.geojson"

    # ── 2. Integrity checks ──────────────────────────────────
    total, invalid, non_poly, bad_area = check_integrity(gdf)
    if invalid > 0:
        log.warning("    %d invalid geometries found", invalid)
    if non_poly > 0:
        log.warning("    %d non-polygon geometries found", non_poly)
    if bad_area > 0:
        log.warning("    %d zero/null-area geometries found", bad_area)

    # ── 3. Extract WKT and build row tuples ──────────────────
    rows = []
    for geom in gdf.geometry:
        wkt = geom.wkt
        rows.append((state_name, wkt, SRID, source_file, chunk_number))

    # ── 4. Batch-insert via execute_values ───────────────────
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO buildings (state, geom, source_file, chunk_number)
            VALUES %s
            """,
            rows,
            template=(
                "(%s, ST_GeomFromText(%s, %s), %s, %s)"
            ),
            page_size=BATCH_SIZE,
        )
    conn.commit()

    return total


# ─────────────────────────────────────────────────────────────
# Per-state uploader
# ─────────────────────────────────────────────────────────────

def upload_state_chunks(
    state_name: str,
    conn,
    resume: bool = False,
) -> bool:
    """
    Upload all chunk files for a state into PostGIS.

    Reads the manifest to know how many chunks to expect, then loads
    each chunk file sequentially.

    Returns True if validation passes (manifest count == db count).
    """
    state_chunks_dir = Path(CHUNKS_DIR) / state_name
    manifest_path = state_chunks_dir / "manifest.json"

    # ── Read manifest ────────────────────────────────────────
    if not manifest_path.exists():
        log.error(
            "No manifest found for %s at %s -- run 03_create_chunks.py first.",
            state_name, manifest_path,
        )
        return False

    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    total_features = manifest["total_features"]
    total_chunks = manifest["total_chunks"]

    log.info("=" * 60)
    log.info("Uploading: %s", state_name)
    log.info("  %s features across %d chunks", f"{total_features:,}", total_chunks)
    log.info("=" * 60)

    if total_features == 0:
        log.warning("  No features -- skipping %s", state_name)
        return True

    # ── Completed-chunk set (for --resume) ──────────────────
    completed = get_completed_chunks(conn, state_name) if resume else set()
    if completed:
        log.info("  Resuming -- %d chunks already completed", len(completed))

    # ── Upload each chunk ────────────────────────────────────
    t0_state = time.time()

    for chunk_meta in manifest["chunks"]:
        chunk_num = chunk_meta["chunk_number"]
        chunk_features = chunk_meta["features"]
        chunk_file = state_chunks_dir / chunk_meta["filename"]

        if chunk_num in completed:
            log.info(
                "  Chunk %d/%d -- skipped (already completed)",
                chunk_num, total_chunks,
            )
            continue

        if not chunk_file.exists():
            log.error(
                "  Chunk file missing: %s -- skipping", chunk_file,
            )
            continue

        log.info(
            "  Chunk %d/%d (%s features) ...",
            chunk_num, total_chunks, f"{chunk_features:,}",
        )

        # Mark as loading
        log_progress(conn, state_name, chunk_num, chunk_features, "loading")
        t0_chunk = time.time()

        try:
            loaded = _load_chunk(chunk_file, state_name, chunk_num, conn)

            elapsed_chunk = time.time() - t0_chunk
            log_progress(conn, state_name, chunk_num, chunk_features, "completed")
            log.info(
                "    Chunk %d done in %.1f s (%s feat/s)",
                chunk_num,
                elapsed_chunk,
                f"{loaded / max(elapsed_chunk, 0.1):,.0f}",
            )

        except Exception as exc:
            log_progress(
                conn, state_name, chunk_num, chunk_features, "failed", str(exc),
            )
            log.error("    Chunk %d FAILED: %s", chunk_num, exc)
            raise

    # ── Per-state validation ────────────────────────────────
    db_count = get_feature_count_from_db(conn, state_name)
    elapsed_state = time.time() - t0_state

    log.info("-" * 60)
    log.info("Validation for %s:", state_name)
    log.info("  Source : %s", f"{total_features:,}")
    log.info("  Loaded : %s", f"{db_count:,}")
    log.info("  Time   : %.1f min", elapsed_state / 60)

    if db_count == total_features:
        log.info("  MATCH")
        return True
    else:
        log.warning(
            "  MISMATCH -- missing %s features",
            f"{total_features - db_count:,}",
        )
        return False


# ─────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────

def run(states=None, resume=False, test_only=False):
    """
    Main entry point.

    Parameters
    ----------
    states : list[str] | None
        Specific state names to upload. If None, uploads all states
        with chunk directories in data/chunks/.
    resume : bool
        Skip chunks already marked completed in load_progress.
    test_only : bool
        Only upload TEST_STATES (small states for dry-run).
    """
    chunks_dir = Path(CHUNKS_DIR)

    # Determine which states to process
    if states:
        state_dirs = []
        for s in states:
            d = chunks_dir / s
            if d.exists() and (d / "manifest.json").exists():
                state_dirs.append(d)
            else:
                log.warning(
                    "No chunks found for %s -- run 03_create_chunks.py first.", s,
                )
    elif test_only:
        state_dirs = [
            chunks_dir / s
            for s in TEST_STATES
            if (chunks_dir / s / "manifest.json").exists()
        ]
    else:
        state_dirs = sorted([
            d for d in chunks_dir.iterdir()
            if d.is_dir() and (d / "manifest.json").exists()
        ])

    if not state_dirs:
        log.error(
            "No chunk directories to upload in %s -- "
            "run 03_create_chunks.py first.",
            chunks_dir,
        )
        return

    log.info("Loading method: geopandas + WKT + ST_GeomFromText")
    log.info("Batch size: %d rows per execute_values page", BATCH_SIZE)

    conn = get_connection()
    results = {}
    t0_all = time.time()

    for idx, state_dir in enumerate(state_dirs, 1):
        state_name = state_dir.name
        log.info("\n[%d/%d] Starting %s", idx, len(state_dirs), state_name)

        try:
            ok = upload_state_chunks(state_name, conn, resume=resume)
            results[state_name] = "OK -- MATCH" if ok else "WARNING -- MISMATCH"
        except Exception as exc:
            results[state_name] = f"FAILED: {exc}"
            log.error("State %s failed: %s", state_name, exc)

    conn.close()

    # ── Summary ─────────────────────────────────────────────
    elapsed_all = time.time() - t0_all
    log.info("\n" + "=" * 60)
    log.info("UPLOAD SUMMARY  (%.1f min total)", elapsed_all / 60)
    log.info("=" * 60)
    for state, status in results.items():
        log.info("  %-30s %s", state, status)
    log.info("=" * 60 + "\n")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Upload chunk files to PostGIS"
    )
    p.add_argument("--state", nargs="+", help="One or more state names to upload")
    p.add_argument("--resume", action="store_true", help="Skip chunks already completed")
    p.add_argument("--test", action="store_true", help="Only upload small test states")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        states=args.state,
        resume=args.resume,
        test_only=args.test,
    )
