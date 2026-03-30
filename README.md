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

## Analysis Results (output/bus5_density)

All findings below are derived from the GTFS schedule dated 2026-03-30. Results reflect scheduled service, not realtime observations.

### Route Overview

- Operator: Hamburger Verkehrsverbund
- Route id: 14007 (route_short_name 5, bus)
- Total scheduled trips in dataset: 1,243
- Unique stops served: 62
- Unique path segments: 64
- Detected directions: 2 (A Burgwedel → Hauptbahnhof/ZOB and Hamburg Hbf ZOB → A Burgwedel)

### Direction Asymmetry

The two directions differ significantly in both coverage and volume:

| Direction | Segment traversals | Unique segments |
|---|---|---|
| Direction 1 — A Burgwedel → Hauptbahnhof/ZOB | 22,337 | 63 |
| Direction 2 — Hamburg Hbf ZOB → A Burgwedel | 8,010 | 30 |

Direction 1 covers more than twice as many unique segments and generates nearly three times the traversal volume. This indicates direction 1 is the longer variant, running additional stops in the Nedderfeld corridor that direction 2 does not serve.

### Stop and Segment Density

Three stops appear on virtually every trip in the dataset:

- U Hoheluftbrücke — 1,242 trip occurrences (rank 1)
- Bf. Dammtor — 1,242 trip occurrences (rank 2)
- Rathausmarkt — 1,242 trip occurrences (rank 3)

These three form the shared trunk of the route and are present in both directions.

Stops from rank 4 onward (Nedderfeld, Siemersplatz, Brunsberg, etc.) appear in 643 trips each — roughly half the total. This half corresponds exclusively to direction 1 trips that extend through the Nedderfeld branch before reaching the city centre.

The Nedderfeld → Siemersplatz segment is the single most traversed edge with 643 trip occurrences, shared by five consecutive segments of equal count in the same corridor.

### Hourly Service Intensity

Peak hour for segment activity: 09:00 with 1,991 traversals across all directions.

Service profile by time window:

- Night (00:00-04:00): Low activity. Direction 1 maintains limited inbound service (6-7 trips/hour). Direction 2 is absent from 01:00 to 04:00.
- Early morning (05:00-06:00): Ramp-up begins. Direction 2 rejoins at 05:00.
- Morning peak (07:00-09:00): Rapid increase. Segment activity rises from 1,366 to 1,991.
- Daytime plateau (09:00-20:00): Sustained high activity, all hours above 1,469 traversals. Direction 1 consistently runs 18-30 trips/hour. Direction 2 holds a steady 6 trips/hour throughout.
- Evening taper (21:00-23:00): Activity declines from 1,319 to 792 traversals per hour.

The asymmetry in daytime frequency is notable. Direction 1 operates at roughly 4-5x the frequency of direction 2 during peak hours.

### Bunching Risk (peak date 2026-04-15, 533 scheduled trips)

#### Direction 1 — A Burgwedel → Hauptbahnhof/ZOB

Direction 1 shows sustained moderate bunching risk throughout the service day:

- Morning peak (07:00-09:00): Bunching index 0.44-0.50. Mean headway drops to ~2 minutes. Short headway ratio reaches 68% at 07:00, meaning more than two-thirds of consecutive departures are within 3 minutes of each other.
- Midday (10:00-12:00): Index retreats to 0.30-0.32 as frequency reduces slightly (18 trips/hour). Still moderate risk.
- Afternoon (13:00-17:00): Index rises again to 0.41-0.48. Mean headway is ~2 minutes with median of 1-2 minutes.
- Early evening (18:00-20:00): Index 0.43-0.47. Elevated short headway ratio persists.
- Off-peak (21:00-23:00): Low risk. Mean headways 8-9 minutes.

Hour 07 is the single worst direction-level hour (index 0.4967, just below the elevated threshold of 0.50).

Key stops for direction 1 exceed the elevated threshold during peak hours:

| Stop | Peak hour | Bunching index | Short ratio |
|---|---|---|---|
| U Hoheluftbrücke | 14:00 | 0.540 | 0.793 |
| Rathausmarkt | 13:00 | 0.520 | 0.692 |
| Bf. Dammtor | 08:00 | 0.516 | 0.724 |
| Nedderfeld | 04:00 | 0.271 | 0.000 (high CV) |

The Nedderfeld anomaly at 04:00 is driven by high headway variability (CV 1.355) rather than short gaps, suggesting erratic early-morning departure spacing rather than classical bunching.

#### Direction 2 — Hamburg Hbf ZOB → A Burgwedel

Direction 2 shows no meaningful bunching risk at any hour:

- Daytime (05:00-20:00): Headways are uniformly 10 minutes (CV near 0.00, bunching index 0.00-0.04).
- Maximum index across all 24 hours: 0.0747 at 21:00 — well within the low risk range.

This direction operates as a fixed-interval, clock-face service with predictable, evenly spaced departures.

#### Summary

| Metric | Direction 1 | Direction 2 |
|---|---|---|
| Peak bunching index | 0.497 (07:00) | 0.075 (21:00) |
| Risk classification | Moderate to near-elevated | Low |
| Mean peak headway | ~2 min | 10 min |
| Hours with index > 0.40 | 10 of 24 | 0 of 24 |
| Service gap (no trips) | None | 01:00-04:00 |

The core finding is that direction 1's high frequency during peak hours generates schedule compression where bunching becomes structurally likely. Direction 2's fixed low-frequency pattern eliminates this risk entirely.

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

This repository is configured to publish code plus selected generated figures.

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

