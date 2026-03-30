from __future__ import annotations

import argparse
import csv
import math
from collections import Counter, defaultdict
from io import BytesIO
from pathlib import Path

import matplotlib.pyplot as plt
import requests
from matplotlib.collections import LineCollection
from matplotlib.colors import Normalize
from PIL import Image


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build path-density analysis and graphs for a GTFS bus line in Hamburg."
    )
    parser.add_argument(
        "--gtfs-dir",
        default="output/gtfs_hamburg",
        help="GTFS directory containing routes, trips, stops, and stop_times.",
    )
    parser.add_argument(
        "--output-dir",
        default="output/bus5_density",
        help="Directory to write analysis graphs and CSV outputs.",
    )
    parser.add_argument(
        "--line",
        default="5",
        help="Route short name to analyze.",
    )
    parser.add_argument(
        "--route-type",
        default="3",
        help="GTFS route_type to analyze. Default 3 = bus.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=15,
        help="Number of top stops and segments to export.",
    )
    parser.add_argument(
        "--zoom",
        type=int,
        default=13,
        help="OpenStreetMap tile zoom level for the basemap.",
    )
    return parser.parse_args()


def require_files(gtfs_dir: Path) -> None:
    required = ["routes.txt", "trips.txt", "stops.txt", "stop_times.txt", "agency.txt"]
    missing = [name for name in required if not (gtfs_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing files in {gtfs_dir}: {', '.join(missing)}")


def parse_time_to_seconds(value: str) -> int | None:
    parts = (value or "").strip().split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = map(int, parts)
    except ValueError:
        return None
    if not (0 <= minutes <= 59 and 0 <= seconds <= 59 and 0 <= hours <= 99):
        return None
    return hours * 3600 + minutes * 60 + seconds


def load_agency_names(path: Path) -> dict[str, str]:
    agency_names: dict[str, str] = {}
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            agency_id = (row.get("agency_id") or "").strip()
            if agency_id:
                agency_names[agency_id] = (row.get("agency_name") or "").strip()
    return agency_names


def load_selected_routes(
    routes_path: Path,
    agency_names: dict[str, str],
    *,
    line: str,
    route_type: str,
) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    line_l = line.strip().lower()
    route_type_filter = route_type.strip()

    with routes_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            short_name = (row.get("route_short_name") or "").strip()
            current_route_type = (row.get("route_type") or "").strip()
            if short_name.lower() != line_l:
                continue
            if route_type_filter.lower() != "all" and current_route_type != route_type_filter:
                continue

            agency_id = (row.get("agency_id") or "").strip()
            route_id = (row.get("route_id") or "").strip()
            if not route_id:
                continue
            selected.append(
                {
                    "route_id": route_id,
                    "route_short_name": short_name,
                    "route_long_name": (row.get("route_long_name") or "").strip(),
                    "route_type": current_route_type,
                    "agency_name": agency_names.get(agency_id, agency_id),
                }
            )

    return selected


def load_selected_trips(trips_path: Path, route_ids: set[str]) -> set[str]:
    trip_ids: set[str] = set()
    with trips_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            route_id = (row.get("route_id") or "").strip()
            if route_id in route_ids:
                trip_id = (row.get("trip_id") or "").strip()
                if trip_id:
                    trip_ids.add(trip_id)
    return trip_ids


def load_stops(stops_path: Path) -> dict[str, dict[str, float | str]]:
    stops: dict[str, dict[str, float | str]] = {}
    with stops_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            stop_id = (row.get("stop_id") or "").strip()
            if not stop_id:
                continue
            lat_raw = (row.get("stop_lat") or "").strip()
            lon_raw = (row.get("stop_lon") or "").strip()
            try:
                lat = float(lat_raw)
                lon = float(lon_raw)
            except ValueError:
                continue
            stops[stop_id] = {
                "stop_name": (row.get("stop_name") or "").strip(),
                "lat": lat,
                "lon": lon,
            }
    return stops


def collect_trip_rows(
    stop_times_path: Path,
    trip_ids: set[str],
) -> dict[str, list[tuple[int, str, int | None, int | None]]]:
    rows_by_trip: dict[str, list[tuple[int, str, int | None, int | None]]] = defaultdict(list)
    with stop_times_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            trip_id = (row.get("trip_id") or "").strip()
            if trip_id not in trip_ids:
                continue
            stop_id = (row.get("stop_id") or "").strip()
            seq_raw = (row.get("stop_sequence") or "").strip()
            if not stop_id or not seq_raw:
                continue
            try:
                sequence = int(seq_raw)
            except ValueError:
                continue

            arrival = parse_time_to_seconds((row.get("arrival_time") or "").strip())
            departure = parse_time_to_seconds((row.get("departure_time") or "").strip())
            rows_by_trip[trip_id].append((sequence, stop_id, arrival, departure))
    return rows_by_trip


def build_direction_labels(
    trip_rows: dict[str, list[tuple[int, str, int | None, int | None]]],
    stops: dict[str, dict[str, float | str]],
) -> dict[str, str]:
    pair_counts: Counter[tuple[str, str]] = Counter()
    first_last_by_trip: dict[str, tuple[str, str]] = {}

    for trip_id, rows in trip_rows.items():
        ordered = sorted(rows, key=lambda item: item[0])
        if not ordered:
            continue
        first_stop = ordered[0][1]
        last_stop = ordered[-1][1]
        pair = (first_stop, last_stop)
        first_last_by_trip[trip_id] = pair
        pair_counts[pair] += 1

    top_pairs = pair_counts.most_common(2)
    if len(top_pairs) < 2:
        return {trip_id: "all_trips" for trip_id in trip_rows}

    labels: dict[tuple[str, str], str] = {}
    for index, (pair, _) in enumerate(top_pairs, start=1):
        origin = stops.get(pair[0], {}).get("stop_name", pair[0])
        destination = stops.get(pair[1], {}).get("stop_name", pair[1])
        labels[pair] = f"direction_{index}: {origin} -> {destination}"

    fallback = labels[top_pairs[0][0]]
    return {trip_id: labels.get(first_last_by_trip.get(trip_id, ("", "")), fallback) for trip_id in trip_rows}


def build_density_metrics(
    trip_rows: dict[str, list[tuple[int, str, int | None, int | None]]],
    direction_by_trip: dict[str, str],
) -> tuple[
    Counter[str],
    Counter[tuple[str, str]],
    dict[str, Counter[str]],
    dict[str, Counter[tuple[str, str]]],
    Counter[int],
    dict[str, Counter[int]],
]:
    stop_counts: Counter[str] = Counter()
    segment_counts: Counter[tuple[str, str]] = Counter()
    directional_stop_counts: dict[str, Counter[str]] = defaultdict(Counter)
    directional_segment_counts: dict[str, Counter[tuple[str, str]]] = defaultdict(Counter)
    hourly_counts: Counter[int] = Counter()
    hourly_by_direction: dict[str, Counter[int]] = defaultdict(Counter)

    for trip_id, rows in trip_rows.items():
        direction = direction_by_trip.get(trip_id, "all_trips")
        ordered = sorted(rows, key=lambda item: item[0])
        for _, stop_id, _, _ in ordered:
            stop_counts[stop_id] += 1
            directional_stop_counts[direction][stop_id] += 1

        for current_row, next_row in zip(ordered, ordered[1:]):
            _, from_stop, _, from_departure = current_row
            _, to_stop, next_arrival, _ = next_row
            if from_stop == to_stop:
                continue
            segment = (from_stop, to_stop)
            segment_counts[segment] += 1
            directional_segment_counts[direction][segment] += 1

            time_source = from_departure if from_departure is not None else next_arrival
            if time_source is not None:
                hour = (time_source // 3600) % 24
                hourly_counts[hour] += 1
                hourly_by_direction[direction][hour] += 1

    return (
        stop_counts,
        segment_counts,
        directional_stop_counts,
        directional_segment_counts,
        hourly_counts,
        hourly_by_direction,
    )


def write_top_stops_csv(
    output_path: Path,
    stop_counts: Counter[str],
    stops: dict[str, dict[str, float | str]],
    top_n: int,
) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "stop_id", "stop_name", "lat", "lon", "trip_occurrences"])
        for rank, (stop_id, count) in enumerate(stop_counts.most_common(top_n), start=1):
            stop = stops.get(stop_id, {})
            writer.writerow([rank, stop_id, stop.get("stop_name", ""), stop.get("lat", ""), stop.get("lon", ""), count])


def write_top_segments_csv(
    output_path: Path,
    segment_counts: Counter[tuple[str, str]],
    stops: dict[str, dict[str, float | str]],
    top_n: int,
) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "from_stop_id", "from_stop_name", "to_stop_id", "to_stop_name", "trip_occurrences"])
        for rank, ((from_stop, to_stop), count) in enumerate(segment_counts.most_common(top_n), start=1):
            writer.writerow([
                rank,
                from_stop,
                stops.get(from_stop, {}).get("stop_name", ""),
                to_stop,
                stops.get(to_stop, {}).get("stop_name", ""),
                count,
            ])


def write_hourly_csv(output_path: Path, hourly_counts: Counter[int], hourly_by_direction: dict[str, Counter[int]]) -> None:
    directions = sorted(hourly_by_direction)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["hour", "all_segments", *directions])
        for hour in range(24):
            writer.writerow([hour, hourly_counts.get(hour, 0), *[hourly_by_direction[direction].get(hour, 0) for direction in directions]])


def deg2num(lat_deg: float, lon_deg: float, zoom: int) -> tuple[float, float]:
    lat_rad = math.radians(lat_deg)
    n = 2.0**zoom
    xtile = (lon_deg + 180.0) / 360.0 * n
    ytile = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n
    return xtile, ytile


def num2deg(xtile: float, ytile: float, zoom: int) -> tuple[float, float]:
    n = 2.0**zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return lat_deg, lon_deg


def fetch_basemap(bounds: tuple[float, float, float, float], zoom: int) -> tuple[Image.Image, tuple[float, float, float, float]] | None:
    min_lon, max_lon, min_lat, max_lat = bounds
    x0, y1 = deg2num(min_lat, min_lon, zoom)
    x1, y0 = deg2num(max_lat, max_lon, zoom)
    min_x = math.floor(min(x0, x1))
    max_x = math.floor(max(x0, x1))
    min_y = math.floor(min(y0, y1))
    max_y = math.floor(max(y0, y1))

    tile_count = (max_x - min_x + 1) * (max_y - min_y + 1)
    if tile_count > 25:
        raise RuntimeError("Basemap request would fetch too many tiles; reduce zoom.")

    canvas = Image.new("RGB", ((max_x - min_x + 1) * 256, (max_y - min_y + 1) * 256))
    session = requests.Session()
    session.headers.update({"User-Agent": "question-list-gtfs-analysis/1.0"})

    for x in range(min_x, max_x + 1):
        for y in range(min_y, max_y + 1):
            url = f"https://tile.openstreetmap.org/{zoom}/{x}/{y}.png"
            response = session.get(url, timeout=30)
            response.raise_for_status()
            tile = Image.open(BytesIO(response.content)).convert("RGB")
            canvas.paste(tile, ((x - min_x) * 256, (y - min_y) * 256))

    top_lat, left_lon = num2deg(min_x, min_y, zoom)
    bottom_lat, right_lon = num2deg(max_x + 1, max_y + 1, zoom)
    extent = (left_lon, right_lon, bottom_lat, top_lat)
    return canvas, extent


def bounds_from_stops(stops: dict[str, dict[str, float | str]], stop_ids: list[str]) -> tuple[float, float, float, float]:
    lons = [float(stops[stop_id]["lon"]) for stop_id in stop_ids if stop_id in stops]
    lats = [float(stops[stop_id]["lat"]) for stop_id in stop_ids if stop_id in stops]
    min_lon = min(lons)
    max_lon = max(lons)
    min_lat = min(lats)
    max_lat = max(lats)
    lon_pad = max(0.01, (max_lon - min_lon) * 0.08)
    lat_pad = max(0.01, (max_lat - min_lat) * 0.08)
    return (min_lon - lon_pad, max_lon + lon_pad, min_lat - lat_pad, max_lat + lat_pad)


def prepare_segments(
    segment_counts: Counter[tuple[str, str]],
    stops: dict[str, dict[str, float | str]],
) -> tuple[list[list[tuple[float, float]]], list[int]]:
    segments: list[list[tuple[float, float]]] = []
    weights: list[int] = []
    for (from_stop, to_stop), count in segment_counts.items():
        from_info = stops.get(from_stop)
        to_info = stops.get(to_stop)
        if not from_info or not to_info:
            continue
        segments.append([
            (float(from_info["lon"]), float(from_info["lat"])),
            (float(to_info["lon"]), float(to_info["lat"])),
        ])
        weights.append(count)
    return segments, weights


def add_basemap(ax: plt.Axes, bounds: tuple[float, float, float, float], zoom: int) -> None:
    try:
        basemap = fetch_basemap(bounds, zoom)
    except Exception as exc:
        ax.text(0.02, 0.02, f"Basemap unavailable: {exc}", transform=ax.transAxes, fontsize=8, color="#7f1d1d")
        ax.set_facecolor("#f7f4ef")
        return

    image, extent = basemap
    ax.imshow(image, extent=extent, origin="upper", alpha=0.78, zorder=0)


def plot_segment_density(
    output_path: Path,
    segment_counts: Counter[tuple[str, str]],
    stops: dict[str, dict[str, float | str]],
    bounds: tuple[float, float, float, float],
    *,
    title: str,
    zoom: int,
) -> None:
    segments, weights = prepare_segments(segment_counts, stops)
    if not segments:
        raise RuntimeError("No path segments available for plotting.")

    fig, ax = plt.subplots(figsize=(10, 10))
    add_basemap(ax, bounds, zoom)
    norm = Normalize(vmin=min(weights), vmax=max(weights))
    scale = max(weights)
    line_collection = LineCollection(
        segments,
        cmap="inferno",
        norm=norm,
        linewidths=[0.8 + 4.5 * (value / scale) for value in weights],
        alpha=0.9,
        zorder=2,
    )
    line_collection.set_array(weights)
    ax.add_collection(line_collection)
    ax.set_xlim(bounds[0], bounds[1])
    ax.set_ylim(bounds[2], bounds[3])
    ax.set_aspect("equal", adjustable="box")
    fig.colorbar(line_collection, ax=ax, label="Trip occurrences per segment")
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    fig.tight_layout()
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def plot_stop_density(
    output_path: Path,
    stop_counts: Counter[str],
    stops: dict[str, dict[str, float | str]],
    bounds: tuple[float, float, float, float],
    *,
    title: str,
    zoom: int,
) -> None:
    points: list[tuple[float, float, int]] = []
    for stop_id, count in stop_counts.items():
        stop = stops.get(stop_id)
        if not stop:
            continue
        points.append((float(stop["lon"]), float(stop["lat"]), count))

    if not points:
        raise RuntimeError("No stop density points available for plotting.")

    lons = [p[0] for p in points]
    lats = [p[1] for p in points]
    counts = [p[2] for p in points]
    sizes = [18 + 220 * (value / max(counts)) for value in counts]

    fig, ax = plt.subplots(figsize=(10, 10))
    add_basemap(ax, bounds, zoom)
    scatter = ax.scatter(
        lons,
        lats,
        c=counts,
        s=sizes,
        cmap="viridis",
        alpha=0.78,
        edgecolors="#0f172a",
        linewidths=0.35,
        zorder=2,
    )
    ax.set_xlim(bounds[0], bounds[1])
    ax.set_ylim(bounds[2], bounds[3])
    ax.set_aspect("equal", adjustable="box")
    fig.colorbar(scatter, ax=ax, label="Trip occurrences per stop")
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    fig.tight_layout()
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def plot_hourly_activity(output_path: Path, hourly_counts: Counter[int], *, title: str) -> None:
    hours = list(range(24))
    values = [hourly_counts.get(hour, 0) for hour in hours]
    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.bar(hours, values, color="#0f766e", edgecolor="#134e4a")
    if values:
        peak_hour = max(hours, key=lambda hour: values[hour])
        bars[peak_hour].set_color("#f97316")
    ax.set_xticks(hours)
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Segment traversals")
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def plot_hourly_by_direction(output_path: Path, hourly_by_direction: dict[str, Counter[int]], *, title: str) -> None:
    hours = list(range(24))
    colors = ["#2563eb", "#dc2626", "#7c3aed", "#059669"]
    fig, ax = plt.subplots(figsize=(12, 5))
    for index, direction in enumerate(sorted(hourly_by_direction)):
        values = [hourly_by_direction[direction].get(hour, 0) for hour in hours]
        ax.plot(hours, values, marker="o", linewidth=2.2, label=direction, color=colors[index % len(colors)])
    ax.set_xticks(hours)
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Segment traversals")
    ax.set_title(title)
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def write_summary(
    output_path: Path,
    routes: list[dict[str, str]],
    trip_count: int,
    stop_counts: Counter[str],
    segment_counts: Counter[tuple[str, str]],
    directional_segment_counts: dict[str, Counter[tuple[str, str]]],
    hourly_counts: Counter[int],
    stops: dict[str, dict[str, float | str]],
) -> None:
    top_stop = stop_counts.most_common(1)
    top_segment = segment_counts.most_common(1)
    peak_hour = hourly_counts.most_common(1)

    with output_path.open("w", encoding="utf-8") as f:
        f.write("Bus line density analysis\n")
        f.write("=" * 60 + "\n")
        f.write(f"Matched routes: {len(routes)}\n")
        f.write(f"Matched trips: {trip_count}\n")
        f.write(f"Unique active stops: {len(stop_counts)}\n")
        f.write(f"Unique path segments: {len(segment_counts)}\n")
        f.write(f"Detected directions: {len(directional_segment_counts)}\n")
        if routes:
            f.write("Routes:\n")
            for route in routes:
                f.write(
                    f"- route_id={route['route_id']}, agency={route['agency_name']}, long_name={route['route_long_name']}\n"
                )
        if top_stop:
            stop_id, count = top_stop[0]
            f.write(
                f"Top stop: {stops.get(stop_id, {}).get('stop_name', stop_id)} ({stop_id}) with {count} trip occurrences\n"
            )
        if top_segment:
            (from_stop, to_stop), count = top_segment[0]
            f.write(
                "Top segment: "
                f"{stops.get(from_stop, {}).get('stop_name', from_stop)} -> "
                f"{stops.get(to_stop, {}).get('stop_name', to_stop)} with {count} trip occurrences\n"
            )
        if peak_hour:
            hour, count = peak_hour[0]
            f.write(f"Peak hour: {hour:02d}:00 with {count} segment traversals\n")
        f.write("Directions:\n")
        for direction, counts in sorted(directional_segment_counts.items()):
            total = sum(counts.values())
            f.write(f"- {direction}: {total} segment traversals across {len(counts)} unique segments\n")


def main() -> None:
    args = parse_args()
    gtfs_dir = Path(args.gtfs_dir)
    output_dir = Path(args.output_dir)
    require_files(gtfs_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    agency_names = load_agency_names(gtfs_dir / "agency.txt")
    routes = load_selected_routes(
        gtfs_dir / "routes.txt",
        agency_names,
        line=args.line,
        route_type=args.route_type,
    )
    if not routes:
        raise RuntimeError("No matching routes found for the requested line and route type.")

    route_ids = {route["route_id"] for route in routes}
    trip_ids = load_selected_trips(gtfs_dir / "trips.txt", route_ids)
    if not trip_ids:
        raise RuntimeError("No trips found for the selected routes.")

    stops = load_stops(gtfs_dir / "stops.txt")
    trip_rows = collect_trip_rows(gtfs_dir / "stop_times.txt", trip_ids)
    direction_by_trip = build_direction_labels(trip_rows, stops)
    (
        stop_counts,
        segment_counts,
        directional_stop_counts,
        directional_segment_counts,
        hourly_counts,
        hourly_by_direction,
    ) = build_density_metrics(trip_rows, direction_by_trip)

    bounds = bounds_from_stops(stops, list(stop_counts.keys()))

    top_stops_csv = output_dir / "bus5_top_stops.csv"
    top_segments_csv = output_dir / "bus5_top_segments.csv"
    hourly_csv = output_dir / "bus5_hourly_activity.csv"
    summary_txt = output_dir / "bus5_density_summary.txt"
    segment_plot = output_dir / "bus5_path_density.png"
    stop_plot = output_dir / "bus5_stop_density.png"
    hourly_plot = output_dir / "bus5_hourly_activity.png"
    hourly_direction_plot = output_dir / "bus5_hourly_by_direction.png"

    write_top_stops_csv(top_stops_csv, stop_counts, stops, args.top_n)
    write_top_segments_csv(top_segments_csv, segment_counts, stops, args.top_n)
    write_hourly_csv(hourly_csv, hourly_counts, hourly_by_direction)
    write_summary(
        summary_txt,
        routes,
        len(trip_ids),
        stop_counts,
        segment_counts,
        directional_segment_counts,
        hourly_counts,
        stops,
    )

    plot_segment_density(
        segment_plot,
        segment_counts,
        stops,
        bounds,
        title=f"Bus {args.line} path density in Hamburg",
        zoom=args.zoom,
    )
    plot_stop_density(
        stop_plot,
        stop_counts,
        stops,
        bounds,
        title=f"Bus {args.line} stop density in Hamburg",
        zoom=args.zoom,
    )
    plot_hourly_activity(hourly_plot, hourly_counts, title=f"Bus {args.line} hourly segment density")
    plot_hourly_by_direction(hourly_direction_plot, hourly_by_direction, title=f"Bus {args.line} hourly density by direction")

    for direction in sorted(directional_segment_counts):
        safe_name = direction.split(":", 1)[0].replace("/", "_").replace(" ", "_")
        plot_segment_density(
            output_dir / f"bus5_path_density_{safe_name}.png",
            directional_segment_counts[direction],
            stops,
            bounds,
            title=f"Bus {args.line} {direction}",
            zoom=args.zoom,
        )
        plot_stop_density(
            output_dir / f"bus5_stop_density_{safe_name}.png",
            directional_stop_counts[direction],
            stops,
            bounds,
            title=f"Bus {args.line} stop density {direction}",
            zoom=args.zoom,
        )

    peak_hour_text = "n/a"
    if hourly_counts:
        peak_hour_text = f"{hourly_counts.most_common(1)[0][0]:02d}:00"

    print("Bus density analysis created")
    print(f"Output directory: {output_dir.resolve()}")
    print(f"Matched routes: {len(routes)}")
    print(f"Matched trips: {len(trip_ids)}")
    print(f"Unique active stops: {len(stop_counts)}")
    print(f"Unique path segments: {len(segment_counts)}")
    print(f"Detected directions: {len(directional_segment_counts)}")
    print(f"Peak hour: {peak_hour_text}")
    print(f"Path density graph: {segment_plot.resolve()}")
    print(f"Stop density graph: {stop_plot.resolve()}")
    print(f"Hourly graph: {hourly_plot.resolve()}")
    print(f"Hourly by direction graph: {hourly_direction_plot.resolve()}")
    print(f"Top stops CSV: {top_stops_csv.resolve()}")
    print(f"Top segments CSV: {top_segments_csv.resolve()}")
    print(f"Hourly CSV: {hourly_csv.resolve()}")
    print(f"Summary text: {summary_txt.resolve()}")


if __name__ == "__main__":
    main()