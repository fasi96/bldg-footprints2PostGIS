# MS Building Footprints — PostGIS Loader

Load ~130 million Microsoft building footprint polygons into a PostGIS
database with chunked loading, real-time progress tracking, and three-level
validation.

## Quick Start

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Install system dependencies (GDAL for ogr2ogr)
#    Ubuntu/Debian:  sudo apt-get install gdal-bin
#    macOS:          brew install gdal
#    Windows:        conda install -c conda-forge gdal

# 3. Configure database connection
cp .env.example .env
# Edit .env with your Crunchy Bridge connection string

# 4. Test connection
python scripts/test_connection.py

# 5. Create tables
python scripts/01_setup_database.py

# 6. Download data → data/source/
#    See "Data Download" section below

# 7. Generate inventory
python scripts/02_generate_inventory.py

# 8. Load data (small test states first)
python scripts/03_chunk_loader.py --test

# 9. Load all states
python scripts/03_chunk_loader.py

# 10. Validate
python scripts/04_validate_counts.py

# 11. Create indexes (after ALL data is loaded)
python scripts/05_create_indexes.py
```

## Data Download

Source: <https://github.com/microsoft/USBuildingFootprints>

Download each state's GeoJSON file and place it under `data/source/`:

```
data/source/
├── Alabama.geojson
├── Alaska.geojson
├── Arizona.geojson
└── ... (all 50 states + DC)
```

## Project Structure

```
├── config.py                    # Configuration (paths, DB, chunk size)
├── requirements.txt             # Python dependencies
├── .env.example                 # Template for secrets
│
├── scripts/
│   ├── test_connection.py       # Verify DB access & PostGIS
│   ├── 01_setup_database.py     # Create tables & lightweight indexes
│   ├── 02_generate_inventory.py # Count features per state file
│   ├── 03_chunk_loader.py       # Chunked GeoJSON → PostGIS loader
│   ├── 04_validate_counts.py    # Validation report generator
│   └── 05_create_indexes.py     # Post-load spatial index + CLUSTER
│
├── data/
│   ├── source/                  # Downloaded GeoJSON state files
│   └── temp/                    # Temporary chunk files (auto-cleaned)
│
├── logs/                        # Application logs
├── reports/                     # source_inventory.csv, validation_report.csv
├── deliverables/                # sample_queries.sql, documentation
└── tests/                       # Unit tests (pytest)
```

## Key Design Decisions

| Decision                   | Rationale                                    |
| -------------------------- | -------------------------------------------- |
| **50k features per chunk** | Balances throughput vs. failure blast radius |
| **ogr2ogr with COPY mode** | Fastest bulk-load path into PostgreSQL       |
| **psycopg2 fallback**      | Works without GDAL CLI installed             |
| **Indexes created LAST**   | Avoids 10–100× insert slowdown               |
| **Per-state validation**   | Catches partial loads before proceeding      |
| **`load_progress` table**  | Enables resume after crash                   |

## Resuming After a Failure

If the loader crashes mid-state:

```bash
python scripts/03_chunk_loader.py --resume
```

This checks `load_progress` for each chunk and skips those already marked
`completed`.

## Running Tests

```bash
python -m pytest tests/ -v
```

## License

Internal project — not for public distribution.
