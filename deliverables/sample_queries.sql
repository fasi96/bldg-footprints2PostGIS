-- ============================================================
-- Sample Spatial Queries — MS Building Footprints (PostGIS)
-- ============================================================
-- Table : buildings
-- Columns: id, state, geom (Polygon, SRID 4326),
--          source_file, chunk_number, loaded_at
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1.  BASIC COUNTS
-- ────────────────────────────────────────────────────────────

-- Total buildings in the database
SELECT COUNT(*) AS total_buildings FROM buildings;

-- Building count by state (descending)
SELECT state, COUNT(*) AS building_count
FROM buildings
GROUP BY state
ORDER BY building_count DESC;


-- ────────────────────────────────────────────────────────────
-- 2.  POINT-RADIUS SEARCH
-- ────────────────────────────────────────────────────────────
-- Find buildings within 10 km of San Francisco City Hall
-- Uses geography cast for accurate metre-based distance.

SELECT id, state, ST_AsGeoJSON(geom) AS geojson
FROM buildings
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(-122.4194, 37.7749), 4326)::geography,
    10000  -- radius in metres
)
LIMIT 100;


-- ────────────────────────────────────────────────────────────
-- 3.  BOUNDING-BOX (VIEWPORT) QUERY
-- ────────────────────────────────────────────────────────────
-- Efficient for map tile rendering: returns buildings whose
-- bounding box overlaps the given envelope.

SELECT id, state, geom
FROM buildings
WHERE geom && ST_MakeEnvelope(
    -122.5, 37.7,   -- min longitude, min latitude
    -122.3, 37.8,   -- max longitude, max latitude
    4326
);


-- ────────────────────────────────────────────────────────────
-- 4.  POLYGON INTERSECTION
-- ────────────────────────────────────────────────────────────
-- Find buildings that intersect with a custom polygon
-- (e.g. a project boundary or administrative area).

WITH search_area AS (
    SELECT ST_GeomFromText(
        'POLYGON((-122.45 37.75, -122.45 37.78, -122.40 37.78, -122.40 37.75, -122.45 37.75))',
        4326
    ) AS geom
)
SELECT b.id, b.state, ST_AsGeoJSON(b.geom) AS geojson
FROM buildings b
JOIN search_area sa ON ST_Intersects(b.geom, sa.geom);


-- ────────────────────────────────────────────────────────────
-- 5.  NEAREST-N BUILDINGS (KNN)
-- ────────────────────────────────────────────────────────────
-- Uses the GIST index <-> operator for index-assisted KNN.

SELECT
    id,
    state,
    ST_Distance(
        geom::geography,
        ST_SetSRID(ST_MakePoint(-122.4194, 37.7749), 4326)::geography
    ) AS distance_metres
FROM buildings
ORDER BY geom <-> ST_SetSRID(ST_MakePoint(-122.4194, 37.7749), 4326)
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- 6.  BUILDING AREA BY STATE (sq metres)
-- ────────────────────────────────────────────────────────────
-- Geography cast gives area in square metres.

SELECT
    state,
    COUNT(*)                                AS building_count,
    ROUND(SUM(ST_Area(geom::geography))::numeric, 2) AS total_area_sqm,
    ROUND(AVG(ST_Area(geom::geography))::numeric, 2) AS avg_area_sqm
FROM buildings
GROUP BY state
ORDER BY total_area_sqm DESC;


-- ────────────────────────────────────────────────────────────
-- 7.  BUILDING DENSITY (per sq km) FOR A STATE
-- ────────────────────────────────────────────────────────────
-- Approximate: uses the convex hull of all buildings in the state.

WITH state_hull AS (
    SELECT
        state,
        COUNT(*)                           AS cnt,
        ST_ConvexHull(ST_Collect(geom))    AS hull
    FROM buildings
    WHERE state = 'California'
    GROUP BY state
)
SELECT
    state,
    cnt AS building_count,
    ROUND(ST_Area(hull::geography) / 1e6, 2)          AS hull_area_sqkm,
    ROUND(cnt / (ST_Area(hull::geography) / 1e6), 2)  AS buildings_per_sqkm
FROM state_hull;


-- ────────────────────────────────────────────────────────────
-- 8.  EXPORT BUILDINGS AS GEOJSON FEATURE COLLECTION
-- ────────────────────────────────────────────────────────────
-- Useful for piping into mapping libraries (Mapbox, Leaflet).
-- Limit to a small area to avoid huge result sets.

SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(
        json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geom)::json,
            'properties', json_build_object(
                'id', id,
                'state', state
            )
        )
    )
) AS geojson
FROM buildings
WHERE geom && ST_MakeEnvelope(-122.42, 37.77, -122.41, 37.78, 4326);


-- ────────────────────────────────────────────────────────────
-- 9.  MONITORING / ADMIN QUERIES
-- ────────────────────────────────────────────────────────────

-- Current load progress
SELECT
    state,
    COUNT(*) AS chunks_done,
    SUM(features_in_chunk) AS features_loaded,
    MAX(completed_at) AS last_activity
FROM load_progress
WHERE status = 'completed'
GROUP BY state
ORDER BY last_activity DESC;

-- Failed chunks
SELECT state, chunk_number, error_message, started_at
FROM load_progress
WHERE status = 'failed'
ORDER BY started_at DESC;

-- Estimated completion
WITH stats AS (
    SELECT
        COUNT(DISTINCT state)      AS states_done,
        SUM(features_in_chunk)     AS features_done,
        EXTRACT(EPOCH FROM (MAX(completed_at) - MIN(started_at))) / 3600
                                   AS hours_elapsed
    FROM load_progress
    WHERE status = 'completed'
)
SELECT
    states_done,
    features_done,
    ROUND(hours_elapsed::numeric, 2)                       AS hours_elapsed,
    ROUND((features_done / NULLIF(hours_elapsed, 0))::numeric, 0)
                                                           AS features_per_hour,
    ROUND(
        ((130000000 - features_done)
         / NULLIF(features_done / NULLIF(hours_elapsed, 0), 0))::numeric, 1
    )                                                      AS est_hours_remaining
FROM stats;

-- Database and index sizes
SELECT
    pg_size_pretty(pg_relation_size('buildings'))       AS table_data,
    pg_size_pretty(pg_total_relation_size('buildings')) AS total_with_indexes;

SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS size
FROM pg_indexes
WHERE tablename = 'buildings';
