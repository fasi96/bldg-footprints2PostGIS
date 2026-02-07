"""
Unit tests for the chunk loader logic.

These tests exercise pure-Python helpers (streaming chunking, GeoJSON
preparation) without requiring a database connection.

The streaming helpers (_count_features, _iter_chunks) are mirrored here
from 03_chunk_loader.py because the script's numeric filename prefix and
database-dependent top-level imports make direct import impractical in a
test environment.

Run:
    python -m pytest tests/ -v
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import ijson
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ─────────────────────────────────────────────────────────────
# Helpers under test
# (Mirrors core logic from 03_chunk_loader.py so tests can run
#  without a database connection.)
# ─────────────────────────────────────────────────────────────

def _count_features(filepath: Path) -> int:
    """
    Count features in a GeoJSON file by streaming with ijson.

    Mirror of _count_features from 03_chunk_loader.py.
    """
    count = 0
    with open(filepath, "rb") as f:
        for _ in ijson.items(f, "features.item"):
            count += 1
    return count


def _iter_chunks(filepath: Path, chunk_size: int):
    """
    Yield successive lists of *chunk_size* features streamed from *filepath*.

    Mirror of _iter_chunks from 03_chunk_loader.py.
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


def prepare_chunk_geojson(features, state_name, chunk_number):
    """Mirror of _prepare_chunk_geojson from 03_chunk_loader.py."""
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


def validate_counts(source_count, db_count):
    """Return True if counts match exactly."""
    return source_count == db_count


# ─────────────────────────────────────────────────────────────
# Test fixtures
# ─────────────────────────────────────────────────────────────

def _make_feature(idx=0):
    """Return a single minimal GeoJSON Feature."""
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[idx, 0], [idx + 1, 0], [idx + 1, 1], [idx, 1], [idx, 0]]],
        },
        "properties": {},
    }


def _write_geojson(filepath, n_features):
    """Write a GeoJSON FeatureCollection with *n_features* to *filepath*."""
    fc = {
        "type": "FeatureCollection",
        "features": [_make_feature(i) for i in range(n_features)],
    }
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(fc, f)


# ─────────────────────────────────────────────────────────────
# Tests — streaming _count_features
# ─────────────────────────────────────────────────────────────

class TestCountFeatures:
    def test_counts_all_features(self, tmp_path):
        fp = tmp_path / "test.geojson"
        _write_geojson(fp, 150)
        assert _count_features(fp) == 150

    def test_empty_feature_collection(self, tmp_path):
        fp = tmp_path / "empty.geojson"
        _write_geojson(fp, 0)
        assert _count_features(fp) == 0

    def test_single_feature(self, tmp_path):
        fp = tmp_path / "one.geojson"
        _write_geojson(fp, 1)
        assert _count_features(fp) == 1


# ─────────────────────────────────────────────────────────────
# Tests — streaming _iter_chunks
# ─────────────────────────────────────────────────────────────

class TestIterChunks:
    def test_exact_multiple(self, tmp_path):
        """Features divisible evenly by chunk size."""
        fp = tmp_path / "test.geojson"
        _write_geojson(fp, 100)
        chunks = list(_iter_chunks(fp, chunk_size=50))
        assert len(chunks) == 2
        assert all(len(c) == 50 for c in chunks)

    def test_remainder(self, tmp_path):
        """Last chunk is smaller when features aren't evenly divisible."""
        fp = tmp_path / "test.geojson"
        _write_geojson(fp, 130)
        chunks = list(_iter_chunks(fp, chunk_size=50))
        assert len(chunks) == 3
        assert len(chunks[0]) == 50
        assert len(chunks[1]) == 50
        assert len(chunks[2]) == 30

    def test_fewer_than_chunk_size(self, tmp_path):
        """Single chunk when features < chunk_size."""
        fp = tmp_path / "test.geojson"
        _write_geojson(fp, 10)
        chunks = list(_iter_chunks(fp, chunk_size=50))
        assert len(chunks) == 1
        assert len(chunks[0]) == 10

    def test_empty_feature_collection(self, tmp_path):
        fp = tmp_path / "empty.geojson"
        _write_geojson(fp, 0)
        chunks = list(_iter_chunks(fp, chunk_size=50))
        assert chunks == []

    def test_total_features_preserved(self, tmp_path):
        """Sum of all chunk lengths == original feature count."""
        fp = tmp_path / "test.geojson"
        n = 275
        _write_geojson(fp, n)
        chunks = list(_iter_chunks(fp, chunk_size=50))
        assert sum(len(c) for c in chunks) == n

    def test_features_are_valid_dicts(self, tmp_path):
        """Each yielded feature is a parsed dict with geometry."""
        fp = tmp_path / "test.geojson"
        _write_geojson(fp, 5)
        chunks = list(_iter_chunks(fp, chunk_size=100))
        assert len(chunks) == 1
        for feat in chunks[0]:
            assert feat["type"] == "Feature"
            assert "geometry" in feat
            assert feat["geometry"]["type"] == "Polygon"

    def test_chunk_size_of_one(self, tmp_path):
        """Edge case: chunk_size=1 yields one feature per chunk."""
        fp = tmp_path / "test.geojson"
        _write_geojson(fp, 3)
        chunks = list(_iter_chunks(fp, chunk_size=1))
        assert len(chunks) == 3
        assert all(len(c) == 1 for c in chunks)


# ─────────────────────────────────────────────────────────────
# Tests — prepare_chunk_geojson  (unchanged from original)
# ─────────────────────────────────────────────────────────────

class TestPrepareChunkGeojson:
    def test_structure(self):
        feat = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
            },
            "properties": {"foo": "bar"},
        }
        result = prepare_chunk_geojson([feat], "Texas", 3)

        assert result["type"] == "FeatureCollection"
        assert len(result["features"]) == 1

        props = result["features"][0]["properties"]
        assert props["state"] == "Texas"
        assert props["source_file"] == "Texas.geojson"
        assert props["chunk_number"] == 3

    def test_geometry_preserved(self):
        coords = [[[10, 20], [30, 20], [30, 40], [10, 40], [10, 20]]]
        feat = {
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": coords},
            "properties": {},
        }
        result = prepare_chunk_geojson([feat], "Ohio", 1)
        assert result["features"][0]["geometry"]["coordinates"] == coords


# ─────────────────────────────────────────────────────────────
# Tests — validate_counts  (unchanged from original)
# ─────────────────────────────────────────────────────────────

class TestValidateCounts:
    def test_match(self):
        assert validate_counts(100_000, 100_000) is True

    def test_mismatch(self):
        assert validate_counts(100_000, 99_999) is False

    def test_zero(self):
        assert validate_counts(0, 0) is True

    def test_over_count(self):
        """DB has more than source (shouldn't happen, but still a mismatch)."""
        assert validate_counts(100_000, 100_001) is False
