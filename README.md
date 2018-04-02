`osm` was written to extract administrative boundaries from OpenStreetMap dataset and turn them into pseudo-geojson compatible with Elasticsearch format. The command is used once or twice a year and is tuned to target OSM dataset, expect some breakage/weirdness with randomly picked data.

The generation process looks like:

- Download the "planet.pbf" dataset from one of the providers listed in <https://wiki.openstreetmap.org/wiki/Planet.osm>
- Convert it to o5m format using osmconvert tool (<https://gitlab.com/osm-c-tools/osmctools>)
```
osmconvert planet.pbf -o=planet.o5m
```
- Filter the o5m file to retain a superset of administrative boundaries
```
osmfilter --keep="place=country =state =region =province =district =county =municipality =postcode =city =town =village =hamlet boundary=" planet.o5m -o=admin.o5m
```
- Install geos 3.8 library and headers, build the `osm` tool.
- Reconstruct ways from nodes
```
osm indexways admin.o5m admin.db
```
- Reconstruct intermediate relations. These are relations used to build other relations. In theory they do not exist. In practice, France and Germany boundaries are defined that way.
```
osm indexrelations admin.o5m admin.db
```
- Reconstruct polygons
```
osm indexlocations admin.o5m admin.db
```
- Extract/compute polygons centroids
```
osm indexcenters admin.o5m admin.db
```
- Extract JSONL
```
osm geojson admin.o5m admin.db admin.jsonl
```

The process is fairly intensive both in disk space and cpu usage. The filtered admin.o5m is around 1.7G, the final planet.db around 8.5G and the output jsonl around 3.5G. Processing took a bit less than 7h on an MBP.

