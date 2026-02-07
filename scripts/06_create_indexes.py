"""
Post-load optimisation: spatial index, clustering, and statistics.

This script must be run AFTER all data is loaded (04_upload_chunks.py)
and validated (05_validate_counts.py).  Creating indexes on 130 M rows
takes several hours — plan accordingly.

Steps
-----
1. Create GIST spatial index on `buildings.geom`
2. Create composite index on `(state, geom)`
3. CLUSTER the table by the spatial index (optimises disk layout)
4. VACUUM ANALYZE (refreshes planner statistics)
5. Report index sizes and total table footprint

Usage:
    python scripts/06_create_indexes.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
from config import DB_CONNECTION


def _fmt_elapsed(seconds: float) -> str:
    """Human-friendly elapsed time."""
    if seconds < 60:
        return f"{seconds:.1f} s"
    if seconds < 3600:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.2f} h"


def create_indexes():
    """Create spatial and composite indexes, then cluster & vacuum."""
    conn = psycopg2.connect(DB_CONNECTION)
    conn.autocommit = True  # required for CREATE INDEX CONCURRENTLY / VACUUM
    cur = conn.cursor()

    print("\n" + "=" * 60)
    print("POST-LOAD OPTIMISATION")
    print("=" * 60 + "\n")

    # ────────────────────────────────────────────────────────
    # Step 1 — Spatial GIST index
    # ────────────────────────────────────────────────────────
    print("Step 1/5: Creating spatial index (GIST) on buildings.geom ...")
    print("          This may take 2-4 hours on 130 M rows.")
    t0 = time.time()

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_buildings_geom
        ON buildings USING GIST (geom);
    """)

    print(f"  ✅ idx_buildings_geom created in {_fmt_elapsed(time.time() - t0)}")

    # ────────────────────────────────────────────────────────
    # Step 2 — Composite index (state + geom)
    # ────────────────────────────────────────────────────────
    print("\nStep 2/5: Creating composite index on (state, geom) ...")
    t0 = time.time()

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_buildings_state_geom
        ON buildings USING GIST (geom)
        WHERE state IS NOT NULL;
    """)
    # Note: A B-tree + GIST combo isn't directly supported. Instead we use
    # a partial GIST index. For state-filtered spatial queries, PostgreSQL
    # can combine idx_buildings_state (B-tree) with idx_buildings_geom (GIST).

    print(f"  ✅ idx_buildings_state_geom created in {_fmt_elapsed(time.time() - t0)}")

    # ────────────────────────────────────────────────────────
    # Step 3 — loaded_at index (useful for incremental loads)
    # ────────────────────────────────────────────────────────
    print("\nStep 3/5: Creating index on loaded_at ...")
    t0 = time.time()

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_buildings_loaded_at
        ON buildings (loaded_at);
    """)

    print(f"  ✅ idx_buildings_loaded_at created in {_fmt_elapsed(time.time() - t0)}")

    # ────────────────────────────────────────────────────────
    # Step 4 — CLUSTER by spatial index
    # ────────────────────────────────────────────────────────
    print("\nStep 4/5: Clustering table by spatial index ...")
    print("          Reorganises rows on disk for spatial locality.")
    t0 = time.time()

    cur.execute("CLUSTER buildings USING idx_buildings_geom;")

    print(f"  ✅ Table clustered in {_fmt_elapsed(time.time() - t0)}")

    # ────────────────────────────────────────────────────────
    # Step 5 — VACUUM ANALYZE
    # ────────────────────────────────────────────────────────
    print("\nStep 5/5: Running VACUUM ANALYZE ...")
    t0 = time.time()

    cur.execute("VACUUM ANALYZE buildings;")

    print(f"  ✅ VACUUM ANALYZE completed in {_fmt_elapsed(time.time() - t0)}")

    # ────────────────────────────────────────────────────────
    # Report
    # ────────────────────────────────────────────────────────
    print("\n" + "-" * 60)
    print("INDEX REPORT")
    print("-" * 60)

    cur.execute("""
        SELECT
            indexname,
            pg_size_pretty(pg_relation_size(indexname::regclass)) AS size
        FROM pg_indexes
        WHERE tablename = 'buildings';
    """)
    for name, size in cur.fetchall():
        print(f"  {name:<40} {size}")

    cur.execute("""
        SELECT
            pg_size_pretty(pg_relation_size('buildings'))       AS table_only,
            pg_size_pretty(pg_total_relation_size('buildings')) AS total;
    """)
    table_only, total = cur.fetchone()
    print(f"\n  Table data  : {table_only}")
    print(f"  Total (+ idx): {total}")

    cur.close()
    conn.close()

    print("\n" + "=" * 60)
    print("✅  POST-LOAD OPTIMISATION COMPLETE")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    create_indexes()
