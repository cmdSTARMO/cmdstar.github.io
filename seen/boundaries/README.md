# Seen boundary data

`seen/index.html` tries boundary sources in this order:

1. Optional local files in this folder.
2. Runtime OSM boundary providers configured on each area (`osmRelationId` or `boundaryQuery`).
3. The inline fallback soft polygon from `bounds`.

The runtime provider is cached in browser `localStorage` under `seen.boundaryGeoJson.v1` when the returned GeoJSON is small enough. Large country boundaries may be loaded without caching to avoid filling localStorage.

Optional local files:

- `countries.geojson`
- `provinces.geojson`
- `cities.geojson`

Each GeoJSON feature should expose one of these identifiers so the page can match it to `SEEN_DATA.boundaries`:

- `feature.id`
- `properties.id`
- `properties.seen_id`
- `properties.slug`
- `properties.name`
- `properties.NAME`

For example, a city feature for Guangzhou can use:

```json
{
  "type": "Feature",
  "id": "guangzhou",
  "properties": { "id": "guangzhou", "name": "Guangzhou" },
  "geometry": { "type": "Polygon", "coordinates": [] }
}
```

If you want fully deterministic production rendering, commit simplified GeoJSON files here and keep `properties.id` aligned with `SEEN_DATA.boundaries`.
