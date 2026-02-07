"""
Test database connectivity and verify PostGIS is available.

Run this FIRST before any other script to confirm the Crunchy Bridge
instance is reachable and PostGIS is installed.

Usage:
    python scripts/test_connection.py
"""

import sys
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
from config import DB_CONNECTION


def test_connection():
    """Verify database connection, PostGIS, and available disk space."""
    print("\n" + "=" * 60)
    print("DATABASE CONNECTION TEST")
    print("=" * 60 + "\n")

    try:
        conn = psycopg2.connect(DB_CONNECTION)
        cur = conn.cursor()
        print("✅ Connected to database successfully")

        # ── PostgreSQL version ──────────────────────────────────
        cur.execute("SELECT version();")
        pg_version = cur.fetchone()[0]
        print(f"✅ PostgreSQL: {pg_version.split(',')[0]}")

        # ── PostGIS version ─────────────────────────────────────
        cur.execute("SELECT PostGIS_Full_Version();")
        postgis_version = cur.fetchone()[0]
        print(f"✅ PostGIS:    {postgis_version}")

        # ── Current database size ───────────────────────────────
        cur.execute(
            "SELECT pg_size_pretty(pg_database_size(current_database()));"
        )
        db_size = cur.fetchone()[0]
        print(f"📊 Current database size: {db_size}")

        # ── Max connections ─────────────────────────────────────
        cur.execute("SHOW max_connections;")
        max_conn = cur.fetchone()[0]
        print(f"📊 Max connections: {max_conn}")

        # ── Shared buffers ──────────────────────────────────────
        cur.execute("SHOW shared_buffers;")
        shared_buf = cur.fetchone()[0]
        print(f"📊 Shared buffers: {shared_buf}")

        # ── Work mem ────────────────────────────────────────────
        cur.execute("SHOW work_mem;")
        work_mem = cur.fetchone()[0]
        print(f"📊 Work memory: {work_mem}")

        # ── Maintenance work mem ────────────────────────────────
        cur.execute("SHOW maintenance_work_mem;")
        maint_mem = cur.fetchone()[0]
        print(f"📊 Maintenance work memory: {maint_mem}")

        # ── Test spatial functionality ──────────────────────────
        cur.execute("""
            SELECT ST_AsText(
                ST_GeomFromGeoJSON(
                    '{"type":"Polygon","coordinates":[[[-122.4,37.7],[-122.4,37.8],[-122.3,37.8],[-122.3,37.7],[-122.4,37.7]]]}'
                )
            );
        """)
        geom_test = cur.fetchone()[0]
        print(f"✅ Spatial query test passed: {geom_test[:50]}...")

        cur.close()
        conn.close()

        print("\n" + "=" * 60)
        print("✅  ALL CHECKS PASSED — Ready to proceed")
        print("=" * 60 + "\n")
        return True

    except psycopg2.OperationalError as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Verify the connection string in config.py / .env")
        print("  2. Ensure the database host is reachable")
        print("  3. Check that sslmode=require is correct for your setup")
        return False

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
