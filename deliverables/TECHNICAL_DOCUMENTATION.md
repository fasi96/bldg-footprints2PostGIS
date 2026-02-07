# Technical Documentation

## MS Building Footprints — PostGIS Database

---

## 1. Database Schema

### 1.1 `buildings` Table

Primary table containing all US building footprint polygons.

| Column         | Type                      | Description                                |
| -------------- | ------------------------- | ------------------------------------------ |
| `id`           | `BIGSERIAL` (PK)          | Auto-incrementing unique identifier        |
| `state`        | `VARCHAR(50)`             | US state name (e.g. `California`, `Texas`) |
| `geom`         | `GEOMETRY(Polygon, 4326)` | Building polygon in WGS 84                 |
| `source_file`  | `VARCHAR(100)`            | Original GeoJSON filename                  |
| `chunk_number` | `INT`                     | Loading chunk identifier                   |
| `loaded_at`    | `TIMESTAMP`               | When the row was inserted                  |

### 1.2 `load_progress` Table

Audit / monitoring table tracking every chunk load operation.

| Column              | Type          | Description                         |
| ------------------- | ------------- | ----------------------------------- |
| `id`                | `SERIAL` (PK) | Row identifier                      |
| `state`             | `VARCHAR(50)` | State being loaded                  |
| `chunk_number`      | `INT`         | Chunk sequence within state         |
| `features_in_chunk` | `INT`         | Feature count in this chunk         |
| `status`            | `VARCHAR(20)` | `loading`, `completed`, or `failed` |
| `error_message`     | `TEXT`        | Error detail (NULL if successful)   |
| `started_at`        | `TIMESTAMP`   | Chunk load start                    |
| `completed_at`      | `TIMESTAMP`   | Chunk load finish                   |

---

## 2. Indexes

Indexes are created **after** all data is loaded to maximise insert throughput.

| Index                      | Type           | Column(s)                      | Purpose                                  |
| -------------------------- | -------------- | ------------------------------ | ---------------------------------------- |
| `buildings_pkey`           | B-tree         | `id`                           | Primary key lookups                      |
| `idx_buildings_state`      | B-tree         | `state`                        | Filtering / validation by state          |
| `idx_buildings_geom`       | GIST           | `geom`                         | Spatial queries (intersect, within, KNN) |
| `idx_buildings_state_geom` | GIST (partial) | `geom WHERE state IS NOT NULL` | Combined state + spatial                 |
| `idx_buildings_loaded_at`  | B-tree         | `loaded_at`                    | Incremental load auditing                |

After indexing, the table is **CLUSTERed** by `idx_buildings_geom` so that
spatially adjacent buildings are stored on the same disk pages, dramatically
improving range-query performance.

---

## 3. Loading Methodology

### 3.1 Chunked Loading Pipeline

```
Source GeoJSON  ──►  Python splits into 50k-feature chunks
                            │
                            ▼
                     Temp GeoJSON file
                            │
                            ▼
                     ogr2ogr -append -f PostgreSQL
                     (COPY mode, no spatial index)
                            │
                            ▼
                     load_progress row updated
                            │
                            ▼
                     Temp file deleted
```

### 3.2 Why 50,000 Features Per Chunk?

| Chunk Size | Pros             | Cons                                  |
| ---------- | ---------------- | ------------------------------------- |
| 1,000      | Very safe        | Too many temp files, overhead         |
| 50,000     | Good balance     | Requires ~200 MB RAM per chunk        |
| 500,000    | Fewer iterations | Risk of OOM, long recovery on failure |

### 3.3 ogr2ogr Configuration

Key flags used during loading:

```
ogr2ogr
  -f PostgreSQL              # Output format
  PG:<connection_string>     # Target database
  <chunk.geojson>            # Source file
  -nln buildings             # Target table name
  -append                    # Add to existing table
  -lco GEOMETRY_NAME=geom    # Match our schema
  -lco SPATIAL_INDEX=NO      # Critical: don't create index yet
  -nlt POLYGON               # Force polygon type
  -a_srs EPSG:4326           # Assign coordinate system
  --config PG_USE_COPY YES   # Use fast COPY protocol
```

### 3.4 Fallback (psycopg2)

When ogr2ogr / GDAL is not available, the loader uses `psycopg2` with
`ST_GeomFromGeoJSON()` to insert features directly. This is 5–10×
slower but works in any Python environment.

---

## 4. Validation

### Three-Level Approach

| Level | Scope        | When                  | Method                         |
| ----- | ------------ | --------------------- | ------------------------------ |
| 1     | Per-chunk    | During load           | ogr2ogr exit code + stderr     |
| 2     | Per-state    | After state completes | `COUNT(*)` vs source inventory |
| 3     | Final report | After all states      | CSV comparing all states       |

### Data Integrity Checks

Run automatically by `04_validate_counts.py`:

- No NULL geometries
- All geometries are valid (`ST_IsValid`)
- No empty state values
- All SRIDs = 4326
- Only POLYGON geometry types

---

## 5. Performance Benchmarks

Expected performance on a typical managed PostgreSQL instance:

| Metric                             | Value                         |
| ---------------------------------- | ----------------------------- |
| Insert rate (ogr2ogr + COPY)       | 100,000–300,000 features/hour |
| Insert rate (psycopg2 fallback)    | 20,000–50,000 features/hour   |
| Spatial index creation (130M rows) | 2–4 hours                     |
| CLUSTER operation                  | 1–3 hours                     |
| Point-radius query (10 km)         | < 1 second                    |
| Bounding-box query (city block)    | < 100 ms                      |
| KNN (nearest 10)                   | < 500 ms                      |

---

## 6. Maintenance Recommendations

### Weekly

```sql
VACUUM ANALYZE buildings;
```

### Monthly

```sql
-- Check index health
SELECT indexname,
       pg_size_pretty(pg_relation_size(indexname::regclass)) AS size
FROM pg_indexes
WHERE tablename = 'buildings';
```

### As Needed

```sql
-- Rebuild fragmented indexes
REINDEX TABLE CONCURRENTLY buildings;

-- Re-cluster if spatial query performance degrades
CLUSTER buildings USING idx_buildings_geom;
```

---

## 7. Known Limitations

1. **Memory requirement** — The Python loader reads each state's full
   GeoJSON into memory before chunking. California (~5 GB) requires at
   least 16 GB of available RAM. Machines with < 16 GB should reduce
   `CHUNK_SIZE` or use the ogr2ogr-only path.

2. **MultiPolygon handling** — Microsoft's dataset contains only simple
   Polygons. If future updates include MultiPolygons, the table's
   geometry constraint would need updating.

3. **No incremental updates** — This loader is designed for a one-time
   bulk load. Adding incremental update support would require change
   detection against the source data.

---

## 8. Connection Details

| Parameter             | Value                               |
| --------------------- | ----------------------------------- |
| **Host**              | Crunchy Bridge (managed PostgreSQL) |
| **Database**          | `ms_buildings_load`                 |
| **SSL**               | Required (`sslmode=require`)        |
| **Coordinate System** | WGS 84 (EPSG:4326)                  |
| **Table**             | `buildings`                         |
| **Expected Rows**     | ~130,000,000                        |

---

_Document version 1.0 — February 6, 2026_
