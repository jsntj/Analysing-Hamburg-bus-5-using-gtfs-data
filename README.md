# GTFS Bus 5 Workflow (Hamburg)

This workspace is dedicated to one topic only: extracting and analyzing bus line 5 from a GTFS feed, focused on Hamburg.

The workflow has four stages:

1. Filter national GTFS to Hamburg.
2. Filter Hamburg GTFS to line 5 (bus).
3. Build path and stop density analytics with maps and hourly charts.
4. Quantify schedule-based bunching risk by direction and key stop.

## Project Goal

The goal is to produce a clean, reproducible pipeline that answers:

- Where bus 5 runs most frequently in Hamburg.
- How service intensity changes by hour.
- Whether schedule spacing shows bunching-like patterns.

## Scripts and Responsibilities

- filter_hamburg_gtfs.py
	- Input: source GTFS folder.
	- Output: Hamburg-only GTFS subset.
	- Modes:
		- strict: keep only stop_times rows at Hamburg stops.
		- connected: keep full trips that touch Hamburg.

- filter_bus5_gtfs.py
	- Input: Hamburg GTFS subset.
	- Output: line-specific GTFS subset (default line 5, route_type 3).
	- Keeps GTFS integrity across routes, trips, stop_times, stops, agency, calendar, and calendar_dates.

- analyze_bus5_density.py
	- Input: bus 5 GTFS subset.
	- Output: map and chart artifacts for density analysis.
	- Includes:
		- OSM basemap overlays.
		- Direction split maps.
		- Hourly density charts.

- bus5bunching.py
	- Input: bus 5 GTFS subset.
	- Output: bunching report CSVs and charts.
	- Includes:
		- Hourly bunching index by direction.
		- Hourly bunching index by key stops.

- run_bus5_pipeline.py
	- Orchestrates end-to-end run for stages 1 to 3.
	- Bunching can be run as an additional command after pipeline completion.

## Data Flow

1. Source GTFS
	 - input/gtfs_de_2026-03-30

2. Hamburg subset
	 - output/gtfs_hamburg

3. Bus 5 subset
	 - output/gtfs_5bus

4. Analytics outputs
	 - output/bus5_density

This layered design is intentional:

- Each stage can be validated independently.
- Re-running downstream stages does not require repeating expensive upstream work.
- Intermediate outputs are auditable and reusable.

## Methodology (Detailed)

### A) Hamburg filtering approach

The Hamburg filter identifies stops within a configured geographic bounding box and keeps related records according to selected mode.

- strict mode:
	- Keeps only stop_times rows whose stop_id is in Hamburg.
	- Produces a geographically strict subset.

- connected mode:
	- Selects trips that visit at least one Hamburg stop.
	- Keeps all stop_times for those trips, including out-of-bounds sections.
	- Produces an operationally connected subset.

### B) Bus 5 filtering approach

The line filter starts from routes where:

- route_short_name equals target line (default 5), and
- route_type equals 3 (bus) unless route_type is set to all.

Then it cascades through dependent GTFS tables:

1. trips linked to selected routes.
2. stop_times linked to selected trips.
3. stops referenced by selected stop_times.
4. agency rows referenced by selected routes.
5. calendar and calendar_dates rows referenced by selected trips.

This preserves a valid GTFS subset for line-specific analysis.

### C) Density analysis approach

For every selected trip:

1. Stop_times are ordered by stop_sequence.
2. Consecutive stop pairs define directed segments.
3. Segment frequency counts how often each edge is traversed.
4. Stop frequency counts how often each stop is visited.

Direction split is inferred from first-stop to last-stop pairs:

- The two most common origin-destination patterns are treated as opposite directions.

Hourly analysis:

- Segment traversals are bucketed by hour of departure time.
- Aggregated for all trips and by inferred direction.

Map rendering:

- OSM raster tiles are fetched at runtime.
- Line and stop densities are drawn over tile basemap.
- Color and line thickness encode intensity.

### D) Bunching analysis approach

Important: this is schedule-based bunching risk, not realtime observed bunching.

Peak date selection:

- The script computes scheduled trips per service day from calendar plus calendar_dates.
- It chooses the day with highest scheduled trip volume.

Headway construction:

- Direction-level metrics use trip-start departure times.
- Key-stop metrics use departure times at that stop.
- Hourly headways are consecutive gaps within each hour.

Metrics per hour:

- departures
- headways_n
- mean_headway_min
- median_headway_min
- cv_headway (coefficient of variation)
- short_ratio (fraction of headways below threshold, default 3 minutes)

Bunching index:

- Combines short headway ratio and headway variability.
- Formula:
	- cv_component = min(cv_headway, 2.0) / 2.0
	- bunching_index = 0.6 * short_ratio + 0.4 * cv_component
- Index range is 0 to 1.

Interpretation guide:

- 0.00 to 0.20: low bunching risk.
- 0.20 to 0.50: moderate bunching risk.
- 0.50 to 1.00: elevated bunching risk.

## Install

```bash
pip install -r requirements.txt
```

## Run Commands

### Full core pipeline (stages 1 to 3)

```bash
python run_bus5_pipeline.py
```

### Full core pipeline with explicit options

```bash
python run_bus5_pipeline.py --hamburg-mode strict --line 5 --route-type 3 --zoom 13
```

### Run bunching analysis (stage 4)

```bash
python bus5bunching.py --gtfs-dir output/gtfs_5bus --output-dir output/bus5_density --line 5 --route-type 3 --short-headway-min 3 --key-stops-per-direction 4
```

## Output Artifacts

### Density outputs

- bus5_path_density.png
- bus5_stop_density.png
- bus5_path_density_direction_1.png
- bus5_path_density_direction_2.png
- bus5_stop_density_direction_1.png
- bus5_stop_density_direction_2.png
- bus5_hourly_activity.png
- bus5_hourly_by_direction.png
- bus5_top_stops.csv
- bus5_top_segments.csv
- bus5_hourly_activity.csv
- bus5_density_summary.txt

### Bunching outputs

- bus5_bunching_hourly_direction.csv
- bus5_bunching_hourly_keystops.csv
- bus5_bunching_hourly_direction.png
- bus5_bunching_keystops_heatmap.png
- bus5_bunching_summary.txt

## Practical Notes

- GTFS times may exceed 24:00:00 for post-midnight service on same service day.
- Basemap tiles are fetched from OpenStreetMap at runtime and require network access.
- For observed bunching validation, add realtime AVL data and compare scheduled vs actual headways.

## GitHub Publishing Setup

This repository is configured to publish code only.

- Large local data folders are ignored by git:
	- input/gtfs_de_2026-03-30
	- output
- Python environment and cache folders are ignored:
	- .venv
	- __pycache__

If you want to share sample data, add a small anonymized sample folder and update commands to point to that sample path.

### Suggested First Push

Run these commands in this folder:

```bash
git init
git add .
git commit -m "Initial commit: GTFS bus 5 Hamburg workflow"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
```
