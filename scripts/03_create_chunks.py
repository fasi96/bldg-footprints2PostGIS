"""
Stream large GeoJSON files and split them into manageable chunk files on disk.

Uses **ijson** to stream GeoJSON features so that only one chunk
(default 50 000 features) is ever held in memory — even when source files
are multi-GB (e.g. Texas, California).

Each chunk is written as a standalone GeoJSON FeatureCollection to:
    data/chunks/{state}/chunk_{N}.geojson

A per-state **manifest.json** is written alongside the chunks with metadata
(total features, chunk count, etc.) so the upload step knows what to expect.

Key design decisions
--------------------
* Chunk creation is pure I/O — no database connection required.
* Feature enrichment (state, source_file, chunk_number) happens here so that
  the upload step can treat each file as a self-contained unit.
* If chunks already exist for a state, they are skipped unless ``--overwrite``
  is passed.

Usage:
    # Create chunks for all states found in data/source/
    python scripts/03_create_chunks.py

    # Create chunks for a single state
    python scripts/03_create_chunks.py --state California

    # Only chunk the small test states (Delaware, Rhode Island, Vermont)
    python scripts/03_create_chunks.py --test

    # Overwrite existing chunks
    python scripts/03_create_chunks.py --overwrite
"""

import argparse
import decimal
import json
import logging
import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ijson
from config import (
    SOURCE_DATA_DIR,
    CHUNKS_DIR,
    LOGS_DIR,
    CHUNK_SIZE,
    TEST_STATES,
)

# ─────────────────────────────────────────────────────────────
# JSON encoder that handles Decimal (produced by ijson)
# ─────────────────────────────────────────────────────────────

class _DecimalEncoder(json.JSONEncoder):
    """json.dumps() encoder that converts Decimal → float."""
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(LOGS_DIR) / "create_chunks.log"),
    ],
)
log = logging.getLogger("create_chunks")


# ─────────────────────────────────────────────────────────────
# Streaming helpers  (memory-efficient — never loads full file)
# ─────────────────────────────────────────────────────────────

def _count_features(filepath: Path) -> int:
    """
    Count features in a GeoJSON file by streaming with ijson.

    Each feature is parsed and immediately discarded, so memory stays flat
    regardless of file size.
    """
    count = 0
    with open(filepath, "rb") as f:
        for _ in ijson.items(f, "features.item"):
            count += 1
    return count


def _iter_chunks(filepath: Path, chunk_size: int):
    """
    Yield successive lists of *chunk_size* features streamed from *filepath*.

    Only one chunk (~chunk_size features) is held in memory at any time.
    The final chunk may contain fewer than *chunk_size* features.
    """
    chunk = []
    with open(filepath, "rb") as f:
        for feature in ijson.items(f, "features.item"):
            chunk.append(feature)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
    if chunk:
        yield chunk


# ─────────────────────────────────────────────────────────────
# Chunk creation
# ─────────────────────────────────────────────────────────────

def _prepare_chunk_geojson(features, state_name, chunk_number):
    """
    Build a GeoJSON FeatureCollection with state / source_file /
    chunk_number injected into each feature's properties so that
    ogr2ogr maps them to the corresponding table columns.
    """
    enriched = []
    source_file = f"{state_name}.geojson"
    for feat in features:
        enriched.append(
            {
                "type": "Feature",
                "geometry": feat["geometry"],
                "properties": {
                    "state": state_name,
                    "source_file": source_file,
                    "chunk_number": chunk_number,
                },
            }
        )
    return {"type": "FeatureCollection", "features": enriched}


def create_chunks_for_state(
    filepath: Path,
    state_name: str,
    overwrite: bool = False,
) -> dict:
    """
    Stream a GeoJSON state file and write chunk files to disk.

    Returns a manifest dict with metadata about the chunks created.
    """
    state_chunks_dir = Path(CHUNKS_DIR) / state_name
    manifest_path = state_chunks_dir / "manifest.json"

    # ── Skip if chunks already exist ─────────────────────────
    if manifest_path.exists() and not overwrite:
        with open(manifest_path, "r") as f:
            existing = json.load(f)
        log.info(
            "  Chunks already exist for %s (%d chunks, %s features) — skipping. "
            "Use --overwrite to recreate.",
            state_name,
            existing["total_chunks"],
            f"{existing['total_features']:,}",
        )
        return existing

    # ── Create output directory ──────────────────────────────
    state_chunks_dir.mkdir(parents=True, exist_ok=True)

    # ── Count features (streaming — memory stays flat) ───────
    log.info("=" * 60)
    log.info("Processing: %s", state_name)
    log.info("=" * 60)

    t0_count = time.time()
    log.info("Counting features in %s (streaming) ...", filepath.name)

    total_features = _count_features(filepath)
    total_chunks = math.ceil(total_features / CHUNK_SIZE) if total_features else 0
    log.info(
        "  %s features in %d chunks (counted in %.1f s)",
        f"{total_features:,}",
        total_chunks,
        time.time() - t0_count,
    )

    if total_features == 0:
        log.warning("  No features found — skipping %s", state_name)
        manifest = {
            "state": state_name,
            "source_file": filepath.name,
            "total_features": 0,
            "total_chunks": 0,
            "chunk_size": CHUNK_SIZE,
            "chunks": [],
        }
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        return manifest

    # ── Stream and write each chunk ──────────────────────────
    t0_state = time.time()
    chunks_meta = []

    for chunk_idx, chunk in enumerate(_iter_chunks(filepath, CHUNK_SIZE), 1):
        log.info(
            "  Chunk %d/%d (%s features) ...",
            chunk_idx, total_chunks, f"{len(chunk):,}",
        )

        t0_chunk = time.time()

        # Build enriched GeoJSON
        chunk_geojson = _prepare_chunk_geojson(chunk, state_name, chunk_idx)

        # Write to disk
        chunk_file = state_chunks_dir / f"chunk_{chunk_idx}.geojson"
        with open(chunk_file, "w", encoding="utf-8") as f:
            json.dump(chunk_geojson, f, cls=_DecimalEncoder)

        elapsed_chunk = time.time() - t0_chunk
        file_size_mb = chunk_file.stat().st_size / (1024 * 1024)

        chunks_meta.append({
            "chunk_number": chunk_idx,
            "filename": chunk_file.name,
            "features": len(chunk),
            "size_mb": round(file_size_mb, 2),
        })

        log.info(
            "    Saved %s (%.1f MB) in %.1f s",
            chunk_file.name,
            file_size_mb,
            elapsed_chunk,
        )

    elapsed_state = time.time() - t0_state

    # ── Write manifest ───────────────────────────────────────
    manifest = {
        "state": state_name,
        "source_file": filepath.name,
        "total_features": total_features,
        "total_chunks": total_chunks,
        "chunk_size": CHUNK_SIZE,
        "created_in_seconds": round(elapsed_state, 1),
        "chunks": chunks_meta,
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    log.info("-" * 60)
    log.info("Chunking complete for %s:", state_name)
    log.info("  Features  : %s", f"{total_features:,}")
    log.info("  Chunks    : %d", total_chunks)
    log.info("  Time      : %.1f s", elapsed_state)
    log.info(
        "  Output    : %s",
        state_chunks_dir,
    )

    return manifest


# ─────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────

def run(states=None, test_only=False, overwrite=False):
    """
    Main entry point.

    Parameters
    ----------
    states : list[str] | None
        Specific state names to chunk. If None, chunks all *.geojson found.
    test_only : bool
        Only chunk TEST_STATES (small states for dry-run).
    overwrite : bool
        Re-create chunks even if they already exist.
    """
    source_dir = Path(SOURCE_DATA_DIR)

    # Determine which state files to process
    if states:
        files = []
        for s in states:
            p = source_dir / f"{s}.geojson"
            if p.exists():
                files.append(p)
            else:
                log.warning("File not found: %s", p)
    elif test_only:
        files = [
            source_dir / f"{s}.geojson"
            for s in TEST_STATES
            if (source_dir / f"{s}.geojson").exists()
        ]
    else:
        files = sorted(source_dir.glob("*.geojson"))

    if not files:
        log.error("No GeoJSON files to process in %s", source_dir)
        return

    log.info("Found %d state file(s) to chunk", len(files))

    results = {}
    t0_all = time.time()

    for idx, filepath in enumerate(files, 1):
        state_name = filepath.stem
        log.info("\n[%d/%d] Starting %s", idx, len(files), state_name)

        try:
            manifest = create_chunks_for_state(
                filepath, state_name, overwrite=overwrite,
            )
            results[state_name] = (
                f"OK — {manifest['total_chunks']} chunks, "
                f"{manifest['total_features']:,} features"
            )
        except Exception as exc:
            results[state_name] = f"FAILED: {exc}"
            log.error("State %s failed: %s", state_name, exc)

    # ── Summary ─────────────────────────────────────────────
    elapsed_all = time.time() - t0_all
    log.info("\n" + "=" * 60)
    log.info("CHUNK CREATION SUMMARY  (%.1f min total)", elapsed_all / 60)
    log.info("=" * 60)
    for state, status in results.items():
        log.info("  %-30s %s", state, status)
    log.info("=" * 60 + "\n")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Stream GeoJSON → chunk files on disk"
    )
    p.add_argument("--state", nargs="+", help="One or more state names to chunk")
    p.add_argument("--test", action="store_true", help="Only chunk small test states")
    p.add_argument(
        "--overwrite", action="store_true",
        help="Re-create chunks even if they already exist",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(states=args.state, test_only=args.test, overwrite=args.overwrite)
