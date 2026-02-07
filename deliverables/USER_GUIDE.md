# User Guide — Building Footprints Database

## What's In The Database?

Your PostGIS database contains approximately **130 million** building
footprint polygons covering every US state, sourced from
[Microsoft Building Footprints](https://github.com/microsoft/USBuildingFootprints).

Each record includes:

- **A polygon** representing the building's outline
- **The state** the building belongs to
- **A timestamp** of when it was loaded

---

## Connecting

Use any PostgreSQL client (pgAdmin, DBeaver, psql, QGIS, etc.) with
the connection string provided to you. Ensure **SSL is enabled**
(`sslmode=require`).

---

## Common Queries

### How many buildings total?

```sql
SELECT COUNT(*) FROM buildings;
```

### Buildings per state

```sql
SELECT state, COUNT(*) AS count
FROM buildings
GROUP BY state
ORDER BY count DESC;
```

### Buildings near a location (within 5 km)

Replace the longitude/latitude with your point of interest:

```sql
SELECT id, state, ST_AsGeoJSON(geom) AS geometry
FROM buildings
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(-122.4194, 37.7749), 4326)::geography,
    5000  -- metres
)
LIMIT 100;
```

### Buildings in a map viewport (bounding box)

```sql
SELECT id, state, geom
FROM buildings
WHERE geom && ST_MakeEnvelope(
    -122.5, 37.7,   -- bottom-left  (lon, lat)
    -122.3, 37.8,   -- top-right    (lon, lat)
    4326
);
```

### 10 nearest buildings to a point

```sql
SELECT id, state,
       ST_Distance(geom::geography,
                   ST_SetSRID(ST_MakePoint(-122.4194, 37.7749), 4326)::geography
       ) AS distance_m
FROM buildings
ORDER BY geom <-> ST_SetSRID(ST_MakePoint(-122.4194, 37.7749), 4326)
LIMIT 10;
```

### Export as GeoJSON (for Mapbox / Leaflet)

```sql
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(
        json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geom)::json,
            'properties', json_build_object('id', id, 'state', state)
        )
    )
) AS geojson
FROM buildings
WHERE geom && ST_MakeEnvelope(-122.42, 37.77, -122.41, 37.78, 4326);
```

---

## Performance Tips

| Do                                                | Don't                                                |
| ------------------------------------------------- | ---------------------------------------------------- |
| Use spatial operators (`&&`, `ST_DWithin`, `<->`) | Run `ST_Distance` on the whole table without a WHERE |
| Filter by `state` when you only need one state    | Use `SELECT *` on millions of rows                   |
| Add `LIMIT` to large queries                      | Call `ST_AsGeoJSON` on unbounded result sets         |
| Cast to `::geography` for metre-based distance    | Assume coordinates are in metres (they're degrees)   |

---

## Maintenance

The database is production-ready as delivered. If you make heavy use of
it, consider running this once a week:

```sql
VACUUM ANALYZE buildings;
```

This keeps the query planner's statistics up to date.

---

_Questions? Contact the project team._
