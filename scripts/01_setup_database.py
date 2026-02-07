"""
Create the required database tables for the MS Building Footprints load.

Tables created:
  - buildings       : Main table holding all 130M+ building polygons.
  - load_progress   : Tracking table for chunk-level load monitoring.

NOTE: Spatial index on `buildings.geom` is NOT created here.
      It is created AFTER all data is loaded (see 05_create_indexes.py).

Usage:
    python scripts/01_setup_database.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
from config import DB_CONNECTION


def setup_database():
    """Create tables and lightweight indexes for loading."""
    conn = psycopg2.connect(DB_CONNECTION)
    conn.autocommit = True  # DDL requires autocommit for some operations
    cur = conn.cursor()

    print("\n" + "=" * 60)
    print("DATABASE SETUP")
    print("=" * 60 + "\n")

    # ── 1. Ensure PostGIS extension ─────────────────────────────
    print("Enabling PostGIS extension...")
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    print("  ✅ PostGIS enabled")

    # ── 2. Create buildings table ───────────────────────────────
    print("\nCreating buildings table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS buildings (
            id          BIGSERIAL PRIMARY KEY,
            state       VARCHAR(50) NOT NULL,
            geom        GEOMETRY(Polygon, 4326) NOT NULL,
            source_file VARCHAR(100),
            chunk_number INT,
            loaded_at   TIMESTAMP DEFAULT NOW()
        );
    """)
    print("  ✅ buildings table ready")

    # ── 3. Create state index (lightweight, helps validation) ───
    print("\nCreating index on buildings(state)...")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_buildings_state
        ON buildings(state);
    """)
    print("  ✅ idx_buildings_state ready")

    # ── 4. Create load_progress table ───────────────────────────
    print("\nCreating load_progress table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS load_progress (
            id               SERIAL PRIMARY KEY,
            state            VARCHAR(50),
            chunk_number     INT,
            features_in_chunk INT,
            status           VARCHAR(20),
            error_message    TEXT,
            started_at       TIMESTAMP DEFAULT NOW(),
            completed_at     TIMESTAMP
        );
    """)
    print("  ✅ load_progress table ready")

    # ── 5. Verify ───────────────────────────────────────────────
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = [row[0] for row in cur.fetchall()]
    print(f"\nPublic tables: {tables}")

    cur.close()
    conn.close()

    print("\n" + "=" * 60)
    print("✅  DATABASE SETUP COMPLETE")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    setup_database()
