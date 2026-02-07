"""
Three-level validation of loaded building footprints.

Level 1 — Per-chunk  : already handled inside 04_upload_chunks.py
Level 2 — Per-state  : compare database counts to source_inventory.csv
Level 3 — Final roll-up: full report with discrepancies highlighted

Outputs:
    reports/validation_report.csv
    (also prints a human-readable summary to stdout)

Usage:
    python scripts/05_validate_counts.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import psycopg2
from config import DB_CONNECTION, REPORTS_DIR


def generate_validation_report() -> pd.DataFrame:
    """
    Build and save a CSV that compares source feature counts with
    the number of features actually loaded into PostGIS.
    """
    conn = psycopg2.connect(DB_CONNECTION)
    cur = conn.cursor()

    # ── 1. Load source inventory ────────────────────────────
    inventory_path = Path(REPORTS_DIR) / "source_inventory.csv"
    if not inventory_path.exists():
        print(f"❌ Source inventory not found: {inventory_path}")
        print("   Run  python scripts/02_generate_inventory.py  first.")
        return pd.DataFrame()

    source_df = pd.read_csv(inventory_path)

    # ── 2. Query loaded counts from database ────────────────
    cur.execute("""
        SELECT
            state,
            COUNT(*)           AS loaded_count,
            MIN(loaded_at)     AS first_loaded,
            MAX(loaded_at)     AS last_loaded
        FROM buildings
        GROUP BY state
        ORDER BY state;
    """)
    rows = cur.fetchall()
    db_df = pd.DataFrame(rows, columns=["state", "loaded_count", "first_loaded", "last_loaded"])

    # ── 3. Query load_progress for chunk-level stats ────────
    cur.execute("""
        SELECT
            state,
            COUNT(*) FILTER (WHERE status = 'completed') AS chunks_ok,
            COUNT(*) FILTER (WHERE status = 'failed')    AS chunks_failed
        FROM load_progress
        GROUP BY state
        ORDER BY state;
    """)
    progress_rows = cur.fetchall()
    progress_df = pd.DataFrame(progress_rows, columns=["state", "chunks_ok", "chunks_failed"])

    cur.close()
    conn.close()

    # ── 4. Merge everything ─────────────────────────────────
    report = source_df.merge(db_df, on="state", how="outer")
    report = report.merge(progress_df, on="state", how="outer")

    # Fill NaN for states not yet loaded
    report["loaded_count"] = report["loaded_count"].fillna(0).astype(int)
    report["chunks_ok"] = report["chunks_ok"].fillna(0).astype(int)
    report["chunks_failed"] = report["chunks_failed"].fillna(0).astype(int)

    # Compute comparison fields
    report["match"] = report["feature_count"] == report["loaded_count"]
    report["missing"] = report["feature_count"] - report["loaded_count"]
    report["percent_complete"] = (
        (report["loaded_count"] / report["feature_count"]) * 100
    ).round(2)

    # Sort: problems first, then alphabetical
    report = report.sort_values(["match", "state"], ascending=[True, True]).reset_index(drop=True)

    # ── 5. Save CSV ─────────────────────────────────────────
    output_path = Path(REPORTS_DIR) / "validation_report.csv"
    report.to_csv(output_path, index=False)

    # ── 6. Print human-readable summary ─────────────────────
    total_source = report["feature_count"].sum()
    total_loaded = report["loaded_count"].sum()
    states_matched = report["match"].sum()
    states_total = len(report)

    print("\n" + "=" * 70)
    print("VALIDATION REPORT SUMMARY")
    print("=" * 70)
    print(f"  States in source inventory : {states_total}")
    print(f"  States loaded into DB      : {len(db_df)}")
    print(f"  States with 100% match     : {states_matched}")
    print(f"  States with discrepancies  : {states_total - states_matched}")
    print()
    print(f"  Total source features      : {total_source:>15,}")
    print(f"  Total loaded features      : {total_loaded:>15,}")

    if total_source > 0:
        pct = (total_loaded / total_source) * 100
        print(f"  Overall completion         : {pct:>14.2f}%")

    # Show problems
    problems = report[~report["match"]]
    if not problems.empty:
        print()
        print("⚠️   States with discrepancies:")
        cols = ["state", "feature_count", "loaded_count", "missing", "percent_complete", "chunks_failed"]
        print(problems[cols].to_string(index=False))

    print(f"\n✅ Full report saved to: {output_path}")
    print("=" * 70 + "\n")

    return report


# ─────────────────────────────────────────────────────────────
# Data-integrity spot checks (run after validation report)
# ─────────────────────────────────────────────────────────────

def run_integrity_checks():
    """Quick sanity checks on the loaded data."""
    conn = psycopg2.connect(DB_CONNECTION)
    cur = conn.cursor()

    print("\n" + "=" * 70)
    print("DATA INTEGRITY CHECKS")
    print("=" * 70)

    # 1. NULL geometries
    cur.execute("SELECT COUNT(*) FROM buildings WHERE geom IS NULL;")
    null_geoms = cur.fetchone()[0]
    status = "✅" if null_geoms == 0 else "❌"
    print(f"  {status} NULL geometries       : {null_geoms:,}")

    # 2. Invalid geometries
    cur.execute("SELECT COUNT(*) FROM buildings WHERE NOT ST_IsValid(geom);")
    invalid = cur.fetchone()[0]
    status = "✅" if invalid == 0 else "⚠️ "
    print(f"  {status} Invalid geometries    : {invalid:,}")

    # 3. Empty state field
    cur.execute("SELECT COUNT(*) FROM buildings WHERE state IS NULL OR state = '';")
    empty_state = cur.fetchone()[0]
    status = "✅" if empty_state == 0 else "❌"
    print(f"  {status} Empty state values    : {empty_state:,}")

    # 4. SRID check (sample)
    cur.execute("SELECT DISTINCT ST_SRID(geom) FROM buildings LIMIT 5;")
    srids = [row[0] for row in cur.fetchall()]
    status = "✅" if srids == [4326] else "⚠️ "
    print(f"  {status} SRIDs found           : {srids}")

    # 5. Geometry types
    cur.execute("SELECT DISTINCT GeometryType(geom) FROM buildings LIMIT 5;")
    types = [row[0] for row in cur.fetchall()]
    print(f"  📊 Geometry types found  : {types}")

    # 6. Total row count
    cur.execute("SELECT COUNT(*) FROM buildings;")
    total = cur.fetchone()[0]
    print(f"  📊 Total rows            : {total:,}")

    cur.close()
    conn.close()
    print("=" * 70 + "\n")


if __name__ == "__main__":
    generate_validation_report()
    run_integrity_checks()
