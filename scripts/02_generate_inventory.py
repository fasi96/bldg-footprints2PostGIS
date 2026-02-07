"""
Generate a source-data inventory of all downloaded GeoJSON files.

Counts features in every *.geojson file under SOURCE_DATA_DIR and writes
a CSV baseline that later scripts compare against to validate completeness.

Two counting strategies are supported (auto-selected):
  1. ogrinfo  – fast, memory-safe, requires GDAL CLI tools.
  2. Python   – loads the file via json.load(); needs enough RAM for the
                largest state file (~5 GB for California).

Output:
    reports/source_inventory.csv

Usage:
    python scripts/02_generate_inventory.py
"""

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from config import SOURCE_DATA_DIR, REPORTS_DIR


# ─────────────────────────────────────────────────────────────
# Feature-counting helpers
# ─────────────────────────────────────────────────────────────

def _count_with_ogrinfo(filepath: Path) -> int:
    """Use ogrinfo (GDAL) to count features without loading into Python."""
    result = subprocess.run(
        ["ogrinfo", "-al", "-so", str(filepath)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    for line in result.stdout.splitlines():
        if "Feature Count" in line:
            return int(line.split(":")[-1].strip())

    raise RuntimeError("ogrinfo did not report Feature Count")


def _count_with_python(filepath: Path) -> int:
    """Count features by loading the full GeoJSON into memory."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return len(data.get("features", []))


def count_features(filepath: Path) -> int:
    """
    Return the number of features in a GeoJSON file.

    Prefers ogrinfo for memory safety; falls back to Python.
    """
    if shutil.which("ogrinfo"):
        return _count_with_ogrinfo(filepath)
    return _count_with_python(filepath)


# ─────────────────────────────────────────────────────────────
# Main inventory generator
# ─────────────────────────────────────────────────────────────

def generate_inventory(source_dir: str | None = None) -> pd.DataFrame:
    """
    Scan source_dir for *.geojson files, count features, and save CSV.
    """
    source_dir = Path(source_dir or SOURCE_DATA_DIR)

    geojson_files = sorted(source_dir.glob("*.geojson"))
    if not geojson_files:
        print(f"⚠️  No .geojson files found in {source_dir}")
        print("   Download state files from:")
        print("   https://github.com/microsoft/USBuildingFootprints")
        return pd.DataFrame()

    print("\n" + "=" * 60)
    print("GENERATING SOURCE INVENTORY")
    print("=" * 60)
    print(f"\nSource directory : {source_dir}")
    print(f"Files found      : {len(geojson_files)}")
    backend = "ogrinfo" if shutil.which("ogrinfo") else "Python json.load"
    print(f"Counting backend : {backend}\n")

    inventory = []

    for idx, filepath in enumerate(geojson_files, 1):
        state_name = filepath.stem
        file_size_mb = filepath.stat().st_size / (1024 * 1024)

        print(f"  [{idx:>2}/{len(geojson_files)}] {state_name:.<30}", end=" ", flush=True)

        t0 = time.time()
        try:
            feature_count = count_features(filepath)
            elapsed = time.time() - t0
            print(f"{feature_count:>12,} features  ({elapsed:>6.1f}s)")
        except Exception as e:
            print(f"  ❌ ERROR: {e}")
            feature_count = -1

        inventory.append(
            {
                "state": state_name,
                "file_path": str(filepath),
                "feature_count": feature_count,
                "file_size_mb": round(file_size_mb, 2),
            }
        )

    df = pd.DataFrame(inventory)

    # Sort by feature count descending (largest states first)
    df = df.sort_values("feature_count", ascending=False).reset_index(drop=True)

    output_path = Path(REPORTS_DIR) / "source_inventory.csv"
    df.to_csv(output_path, index=False)

    # ── Summary ─────────────────────────────────────────────
    total = df.loc[df["feature_count"] > 0, "feature_count"].sum()
    total_gb = df["file_size_mb"].sum() / 1024

    print("\n" + "-" * 60)
    print(f"Total features : {total:>15,}")
    print(f"Total file size: {total_gb:>14.2f} GB")
    print(f"States counted : {len(df):>15}")
    print(f"\nInventory saved to: {output_path}")
    print("=" * 60 + "\n")

    return df


# ─────────────────────────────────────────────────────────────
# Single-state inventory upsert  (used by run_pipeline.py)
# ─────────────────────────────────────────────────────────────

def update_inventory_row(state: str, feature_count: int, file_size_mb: float):
    """
    Insert or update a single state's row in source_inventory.csv.

    This allows the pipeline orchestrator to update the inventory
    from the chunk manifest without needing the raw source file on disk.
    """
    output_path = Path(REPORTS_DIR) / "source_inventory.csv"

    if output_path.exists():
        df = pd.read_csv(output_path)
    else:
        df = pd.DataFrame(columns=["state", "file_path", "feature_count", "file_size_mb"])

    file_path = str(Path(SOURCE_DATA_DIR) / f"{state}.geojson")

    if state in df["state"].values:
        # Update existing row
        mask = df["state"] == state
        df.loc[mask, "feature_count"] = feature_count
        df.loc[mask, "file_size_mb"] = round(file_size_mb, 2)
        df.loc[mask, "file_path"] = file_path
    else:
        # Append new row
        new_row = pd.DataFrame([{
            "state": state,
            "file_path": file_path,
            "feature_count": feature_count,
            "file_size_mb": round(file_size_mb, 2),
        }])
        df = pd.concat([df, new_row], ignore_index=True)

    # Keep sorted by feature count descending
    df = df.sort_values("feature_count", ascending=False).reset_index(drop=True)
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    generate_inventory()
