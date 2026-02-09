"""
Configuration for MS Building Footprints PostGIS Loader.

Loads settings from .env file if present, otherwise uses defaults.
Sensitive values (DB_CONNECTION) should always be set via .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env")

# ──────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────
DB_CONNECTION = os.getenv(
    "DB_CONNECTION"
)

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
SOURCE_DATA_DIR = os.getenv("SOURCE_DATA_DIR", str(PROJECT_ROOT / "data" / "source"))
CHUNKS_DIR = os.getenv("CHUNKS_DIR", str(PROJECT_ROOT / "data" / "chunks"))
TEMP_DIR = os.getenv("TEMP_DIR", str(PROJECT_ROOT / "data" / "temp"))
LOGS_DIR = os.getenv("LOGS_DIR", str(PROJECT_ROOT / "logs"))
REPORTS_DIR = os.getenv("REPORTS_DIR", str(PROJECT_ROOT / "reports"))
PIPELINE_STATUS_FILE = os.getenv(
    "PIPELINE_STATUS_FILE", str(PROJECT_ROOT / "data" / "pipeline_status.json")
)

# Ensure directories exist
for d in (SOURCE_DATA_DIR, CHUNKS_DIR, TEMP_DIR, LOGS_DIR, REPORTS_DIR):
    Path(d).mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────
# Loading parameters
# ──────────────────────────────────────────────
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))   # Features per chunk
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))     # Insert batch size (psycopg2 fallback)

# ──────────────────────────────────────────────
# State ordering
# ──────────────────────────────────────────────
# Small states used for initial testing / dry-run
TEST_STATES = ["Delaware", "RhodeIsland", "Vermont"]

# All US states + DC — names must match the Microsoft download filenames exactly
ALL_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California",
    "Colorado", "Connecticut", "Delaware", "DistrictofColumbia",
    "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana",
    "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "NewHampshire", "NewJersey",
    "NewMexico", "NewYork", "NorthCarolina", "NorthDakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "RhodeIsland", "SouthCarolina",
    "SouthDakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia",
    "Washington", "WestVirginia", "Wisconsin", "Wyoming",
]

# Base URL for Microsoft Building Footprints downloads
DOWNLOAD_BASE_URL = "https://minedbuildings.z5.web.core.windows.net/legacy/usbuildings-v2"
