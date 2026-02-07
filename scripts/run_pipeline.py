"""
Master pipeline orchestrator for loading all US states into PostGIS.

Processes each state through the full pipeline:
    download -> chunk -> upload -> validate -> cleanup raw file

State-level progress is tracked in data/pipeline_status.json so the script
can be stopped and restarted at any time.  Only states marked "completed"
are skipped.  Any other status (including "failed" or missing) triggers
a clean redo for that state -- DB rows and load_progress are wiped first
to prevent duplicates.

Usage:
    # Process all 51 states
    python scripts/run_pipeline.py

    # Process specific state(s)
    python scripts/run_pipeline.py --state California Texas

    # Process only the small test states
    python scripts/run_pipeline.py --test

    # Preview what would run without changing anything
    python scripts/run_pipeline.py --dry-run
"""

import argparse
import importlib.util
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
from config import (
    ALL_STATES,
    TEST_STATES,
    DB_CONNECTION,
    SOURCE_DATA_DIR,
    CHUNKS_DIR,
    LOGS_DIR,
    PIPELINE_STATUS_FILE,
)

# ─────────────────────────────────────────────────────────────
# Import functions from numbered scripts (can't use normal import)
# ─────────────────────────────────────────────────────────────

_SCRIPTS_DIR = Path(__file__).resolve().parent


def _import_script(module_name: str, filename: str):
    """Import a script file that has a numeric prefix in its name."""
    filepath = _SCRIPTS_DIR / filename
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_mod_download = _import_script("download_data", "00_download_data.py")
_mod_chunks = _import_script("create_chunks", "03_create_chunks.py")
_mod_upload = _import_script("upload_chunks", "04_upload_chunks.py")
_mod_inventory = _import_script("generate_inventory", "02_generate_inventory.py")

download_state = _mod_download.download_state
create_chunks_for_state = _mod_chunks.create_chunks_for_state
upload_state_chunks = _mod_upload.upload_state_chunks
get_connection = _mod_upload.get_connection
get_feature_count_from_db = _mod_upload.get_feature_count_from_db
update_inventory_row = _mod_inventory.update_inventory_row


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(LOGS_DIR) / "pipeline.log"),
    ],
)
log = logging.getLogger("pipeline")


# ─────────────────────────────────────────────────────────────
# Status file helpers
# ─────────────────────────────────────────────────────────────

def _load_status() -> dict:
    """Load pipeline_status.json, returning {} if it doesn't exist."""
    path = Path(PIPELINE_STATUS_FILE)
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return {}


def _save_status(status: dict):
    """Write pipeline_status.json atomically."""
    path = Path(PIPELINE_STATUS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(status, f, indent=2)


def _mark_state(status_dict: dict, state: str, **fields):
    """Update a state's entry in the status dict and persist."""
    status_dict[state] = {**status_dict.get(state, {}), **fields}
    _save_status(status_dict)


# ─────────────────────────────────────────────────────────────
# Clean-up helper
# ─────────────────────────────────────────────────────────────

def _clean_state_from_db(conn, state_name: str):
    """
    Remove all rows for a state from buildings and load_progress.

    This ensures a clean slate before re-uploading, preventing duplicates.
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM buildings WHERE state = %s", (state_name,))
        buildings_deleted = cur.rowcount
        cur.execute("DELETE FROM load_progress WHERE state = %s", (state_name,))
        progress_deleted = cur.rowcount
    conn.commit()

    if buildings_deleted > 0 or progress_deleted > 0:
        log.info(
            "  Cleaned DB: %d rows from buildings, %d from load_progress",
            buildings_deleted, progress_deleted,
        )


# ─────────────────────────────────────────────────────────────
# Per-state pipeline
# ─────────────────────────────────────────────────────────────

def process_state(state_name: str, conn, status: dict) -> bool:
    """
    Run the full pipeline for a single state.

    Steps: clean DB -> download -> chunk -> upload -> validate -> cleanup raw.
    Returns True if the state completed successfully.
    """
    source_dir = Path(SOURCE_DATA_DIR)
    source_file = source_dir / f"{state_name}.geojson"

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # ── Step 1: Clean slate ──────────────────────────────────
    log.info("  Step 1/6: Cleaning DB for %s ...", state_name)
    _clean_state_from_db(conn, state_name)
    _mark_state(status, state_name, status="cleaning", started_at=now)

    # ── Step 2: Download ─────────────────────────────────────
    log.info("  Step 2/6: Downloading %s ...", state_name)
    _mark_state(status, state_name, status="downloading")

    ok = download_state(state_name, source_dir)
    if not ok:
        raise RuntimeError(f"Download failed for {state_name}")

    if not source_file.exists():
        raise RuntimeError(f"Source file not found after download: {source_file}")

    file_size_mb = source_file.stat().st_size / (1024 * 1024)

    # ── Step 3: Create chunks ────────────────────────────────
    log.info("  Step 3/6: Creating chunks for %s ...", state_name)
    _mark_state(status, state_name, status="chunking")

    manifest = create_chunks_for_state(
        filepath=source_file,
        state_name=state_name,
        overwrite=True,  # always overwrite to handle interrupted chunking
    )

    total_features = manifest["total_features"]
    total_chunks = manifest["total_chunks"]

    if total_features == 0:
        log.warning("  No features found in %s -- marking completed", state_name)
        _mark_state(
            status, state_name,
            status="completed", features=0, chunks=0, completed_at=now,
        )
        return True

    # ── Step 4: Upload chunks ────────────────────────────────
    log.info("  Step 4/6: Uploading %d chunks for %s ...", total_chunks, state_name)
    _mark_state(status, state_name, status="uploading")

    validated = upload_state_chunks(state_name, conn, resume=False)

    # ── Step 5: Validate ─────────────────────────────────────
    log.info("  Step 5/6: Validating %s ...", state_name)
    _mark_state(status, state_name, status="validating")

    db_count = get_feature_count_from_db(conn, state_name)

    if db_count != total_features:
        raise RuntimeError(
            f"Validation failed: expected {total_features:,} but DB has {db_count:,} "
            f"(missing {total_features - db_count:,})"
        )

    log.info(
        "  Validated: %s features match (%s)",
        f"{db_count:,}", state_name,
    )

    # ── Step 6: Cleanup raw file + update inventory ──────────
    log.info("  Step 6/6: Cleaning up raw file for %s ...", state_name)

    # Update inventory CSV from manifest data
    update_inventory_row(state_name, total_features, file_size_mb)
    log.info("  Updated source_inventory.csv")

    # Delete raw source file (multi-GB)
    if source_file.exists():
        source_file.unlink()
        log.info("  Deleted raw file: %s (%.1f MB freed)", source_file.name, file_size_mb)

    # Mark completed
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _mark_state(
        status, state_name,
        status="completed",
        features=total_features,
        chunks=total_chunks,
        file_size_mb=round(file_size_mb, 2),
        completed_at=now,
    )

    return True


# ─────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────

def run(states=None, test_only=False, dry_run=False):
    """
    Main entry point.

    Parameters
    ----------
    states : list[str] | None
        Specific state names to process. If None, processes all states.
    test_only : bool
        Only process TEST_STATES.
    dry_run : bool
        Show what would run without changing anything.
    """
    # Determine target states
    if states:
        target_states = states
    elif test_only:
        target_states = TEST_STATES
    else:
        target_states = ALL_STATES

    status = _load_status()

    # Categorize states
    completed = [s for s in target_states if status.get(s, {}).get("status") == "completed"]
    pending = [s for s in target_states if s not in completed]

    log.info("\n" + "=" * 60)
    log.info("PIPELINE ORCHESTRATOR")
    log.info("=" * 60)
    log.info("  Target states : %d", len(target_states))
    log.info("  Completed     : %d (will skip)", len(completed))
    log.info("  To process    : %d", len(pending))

    if completed:
        log.info("  Skipping: %s", ", ".join(completed))

    if dry_run:
        log.info("\n-- DRY RUN -- No changes will be made.\n")
        if pending:
            log.info("Would process these states:")
            for i, s in enumerate(pending, 1):
                prev = status.get(s, {}).get("status", "new")
                log.info("  %2d. %-25s (current status: %s)", i, s, prev)
        else:
            log.info("Nothing to do -- all states completed.")
        log.info("=" * 60 + "\n")
        return

    if not pending:
        log.info("\nAll states completed. Nothing to do.")
        log.info("Reminder: run  python scripts/06_create_indexes.py  if not done yet.")
        log.info("=" * 60 + "\n")
        return

    # Open DB connection
    conn = get_connection()
    results = {}
    t0_all = time.time()

    for idx, state_name in enumerate(pending, 1):
        log.info("\n" + "#" * 60)
        log.info("# [%d/%d] %s", idx, len(pending), state_name)
        log.info("#" * 60)

        t0_state = time.time()

        try:
            ok = process_state(state_name, conn, status)
            elapsed = time.time() - t0_state
            results[state_name] = f"OK ({elapsed / 60:.1f} min)"
            log.info(
                "  >> %s COMPLETED in %.1f min",
                state_name, elapsed / 60,
            )

        except Exception as exc:
            elapsed = time.time() - t0_state
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            _mark_state(
                status, state_name,
                status="failed",
                error=str(exc),
                failed_at=now,
            )
            results[state_name] = f"FAILED: {exc}"
            log.error(
                "  >> %s FAILED after %.1f min: %s",
                state_name, elapsed / 60, exc,
            )

    conn.close()

    # ── Summary ─────────────────────────────────────────────
    elapsed_all = time.time() - t0_all
    status = _load_status()  # reload for final counts
    total_completed = sum(1 for s in ALL_STATES if status.get(s, {}).get("status") == "completed")

    log.info("\n" + "=" * 60)
    log.info("PIPELINE SUMMARY  (%.1f min this run)", elapsed_all / 60)
    log.info("=" * 60)
    for state, result in results.items():
        log.info("  %-30s %s", state, result)
    log.info("-" * 60)
    log.info("  Overall progress: %d / %d states completed", total_completed, len(ALL_STATES))

    if total_completed == len(ALL_STATES):
        log.info("\n  ALL STATES LOADED.")
        log.info("  Next step: python scripts/06_create_indexes.py")
    else:
        remaining = len(ALL_STATES) - total_completed
        log.info("\n  %d states remaining. Re-run this script to continue.", remaining)

    log.info("=" * 60 + "\n")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Run the full state-by-state loading pipeline"
    )
    p.add_argument("--state", nargs="+", help="Specific state(s) to process")
    p.add_argument("--test", action="store_true", help="Only process small test states")
    p.add_argument(
        "--dry-run", action="store_true",
        help="Show what would run without making changes",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(states=args.state, test_only=args.test, dry_run=args.dry_run)
