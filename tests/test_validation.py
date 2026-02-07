"""
Tests for validation report generation.

These tests mock the database layer so they can run without
a live PostgreSQL connection.

Run:
    python -m pytest tests/ -v
"""

import sys
from pathlib import Path
from io import StringIO
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest


# ─────────────────────────────────────────────────────────────
# Validation helpers (duplicated here to avoid DB imports)
# ─────────────────────────────────────────────────────────────

def build_validation_df(source_df: pd.DataFrame, db_df: pd.DataFrame) -> pd.DataFrame:
    """
    Core merge & comparison logic extracted from 04_validate_counts.py
    so it can be tested without touching the database.
    """
    report = source_df.merge(db_df, on="state", how="outer")

    report["loaded_count"] = report["loaded_count"].fillna(0).astype(int)
    report["match"] = report["feature_count"] == report["loaded_count"]
    report["missing"] = report["feature_count"] - report["loaded_count"]
    report["percent_complete"] = (
        (report["loaded_count"] / report["feature_count"]) * 100
    ).round(2)

    return report


# ─────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────


class TestBuildValidationDf:
    def test_all_match(self):
        source = pd.DataFrame(
            {
                "state": ["Delaware", "Vermont"],
                "feature_count": [300_000, 200_000],
                "file_size_mb": [120.5, 80.3],
            }
        )
        loaded = pd.DataFrame(
            {"state": ["Delaware", "Vermont"], "loaded_count": [300_000, 200_000]}
        )
        report = build_validation_df(source, loaded)

        assert report["match"].all()
        assert (report["missing"] == 0).all()
        assert (report["percent_complete"] == 100.0).all()

    def test_partial_load(self):
        source = pd.DataFrame(
            {"state": ["California"], "feature_count": [10_000_000], "file_size_mb": [4500.0]}
        )
        loaded = pd.DataFrame(
            {"state": ["California"], "loaded_count": [9_500_000]}
        )
        report = build_validation_df(source, loaded)

        assert not report["match"].iloc[0]
        assert report["missing"].iloc[0] == 500_000
        assert report["percent_complete"].iloc[0] == 95.0

    def test_state_not_loaded(self):
        """State exists in source but has no rows in DB."""
        source = pd.DataFrame(
            {"state": ["Hawaii"], "feature_count": [500_000], "file_size_mb": [200.0]}
        )
        loaded = pd.DataFrame(columns=["state", "loaded_count"])

        report = build_validation_df(source, loaded)

        assert report["loaded_count"].iloc[0] == 0
        assert not report["match"].iloc[0]
        assert report["missing"].iloc[0] == 500_000
        assert report["percent_complete"].iloc[0] == 0.0

    def test_multiple_states_mixed(self):
        source = pd.DataFrame(
            {
                "state": ["Alabama", "Alaska", "Arizona"],
                "feature_count": [2_000_000, 200_000, 3_000_000],
                "file_size_mb": [800, 80, 1200],
            }
        )
        loaded = pd.DataFrame(
            {
                "state": ["Alabama", "Alaska", "Arizona"],
                "loaded_count": [2_000_000, 200_000, 2_999_990],
            }
        )
        report = build_validation_df(source, loaded)

        assert report.loc[report["state"] == "Alabama", "match"].iloc[0]
        assert report.loc[report["state"] == "Alaska", "match"].iloc[0]
        assert not report.loc[report["state"] == "Arizona", "match"].iloc[0]
        assert report.loc[report["state"] == "Arizona", "missing"].iloc[0] == 10
