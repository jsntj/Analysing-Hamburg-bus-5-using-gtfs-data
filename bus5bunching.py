from __future__ import annotations

import argparse
import csv
import statistics
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute hourly bus bunching metrics for bus 5 by direction and key stops."
    )
    parser.add_argument("--gtfs-dir", default="output/gtfs_5bus", help="Path to GTFS folder.")
    parser.add_argument(
        "--output-dir",
        default="output/bus5_density",
        help="Directory for bunching CSV and chart outputs.",
    )
    parser.add_argument("--line", default="5", help="Route short name filter.")
    parser.add_argument("--route-type", default="3", help="Route type filter (3=bus).")
    parser.add_argument(
        "--short-headway-min",
        type=float,
        default=3.0,
        help="Headway threshold (minutes) used as short-gap bunching proxy.",
    )
    parser.add_argument(
        "--key-stops-per-direction",
        type=int,
        default=4,
        help="How many highest-departure stops to keep per direction.",
    )
    return parser.parse_args()


def parse_time_to_seconds(value: str) -> int | None:
    parts = (value or "").strip().split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = map(int, parts)
    except ValueError:
        return None
    if not (0 <= h <= 99 and 0 <= m <= 59 and 0 <= s <= 59):
        return None
    return h * 3600 + m * 60 + s


def parse_yyyymmdd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def gtfs_weekday_name(d: date) -> str:
    names = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    return names[d.weekday()]


def bunching_index(cv_headway: float, short_ratio: float) -> float:
    # Keep index on [0, 1] by capping CV contribution.
    cv_component = min(cv_headway, 2.0) / 2.0
    return round(0.6 * short_ratio + 0.4 * cv_component, 4)


def ensure_files(gtfs_dir: Path) -> None:
    needed = [
        "routes.txt",
        "trips.txt",
        "stops.txt",
        "stop_times.txt",
        "calendar.txt",
        "calendar_dates.txt",
    ]
    missing = [name for name in needed if not (gtfs_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing GTFS files in {gtfs_dir}: {', '.join(missing)}")


def load_routes(gtfs_dir: Path, line: str, route_type: str) -> set[str]:
    route_ids: set[str] = set()
    line_l = line.strip().lower()
    rt = route_type.strip()
    with (gtfs_dir / "routes.txt").open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            short_name = (row.get("route_short_name") or "").strip()
            current_rt = (row.get("route_type") or "").strip()
            if short_name.lower() != line_l:
                continue
            if rt.lower() != "all" and current_rt != rt:
                continue
            rid = (row.get("route_id") or "").strip()
            if rid:
                route_ids.add(rid)
    return route_ids


def load_trips(gtfs_dir: Path, route_ids: set[str]) -> tuple[dict[str, str], Counter[str]]:
    trip_service: dict[str, str] = {}
    trips_per_service: Counter[str] = Counter()
    with (gtfs_dir / "trips.txt").open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            rid = (row.get("route_id") or "").strip()
            if rid not in route_ids:
                continue
            tid = (row.get("trip_id") or "").strip()
            sid = (row.get("service_id") or "").strip()
            if tid:
                trip_service[tid] = sid
                if sid:
                    trips_per_service[sid] += 1
    return trip_service, trips_per_service


def load_active_dates_by_service(gtfs_dir: Path, service_ids: set[str]) -> dict[str, set[date]]:
    active: dict[str, set[date]] = {sid: set() for sid in service_ids}

    with (gtfs_dir / "calendar.txt").open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            sid = (row.get("service_id") or "").strip()
            if sid not in service_ids:
                continue
            start_raw = (row.get("start_date") or "").strip()
            end_raw = (row.get("end_date") or "").strip()
            if not start_raw or not end_raw:
                continue
            start = parse_yyyymmdd(start_raw)
            end = parse_yyyymmdd(end_raw)
            flags = {
                "monday": (row.get("monday") or "").strip() == "1",
                "tuesday": (row.get("tuesday") or "").strip() == "1",
                "wednesday": (row.get("wednesday") or "").strip() == "1",
                "thursday": (row.get("thursday") or "").strip() == "1",
                "friday": (row.get("friday") or "").strip() == "1",
                "saturday": (row.get("saturday") or "").strip() == "1",
                "sunday": (row.get("sunday") or "").strip() == "1",
            }
            for d in daterange(start, end):
                if flags[gtfs_weekday_name(d)]:
                    active[sid].add(d)

    with (gtfs_dir / "calendar_dates.txt").open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            sid = (row.get("service_id") or "").strip()
            if sid not in service_ids:
                continue
            date_raw = (row.get("date") or "").strip()
            ex_type = (row.get("exception_type") or "").strip()
            if not date_raw:
                continue
            d = parse_yyyymmdd(date_raw)
            if ex_type == "1":
                active[sid].add(d)
            elif ex_type == "2":
                active[sid].discard(d)

    return active


def pick_peak_date(active_dates_by_service: dict[str, set[date]], trips_per_service: Counter[str]) -> tuple[date, int]:
    trips_by_date: Counter[date] = Counter()
    for sid, days in active_dates_by_service.items():
        trips = trips_per_service.get(sid, 0)
        for d in days:
            trips_by_date[d] += trips
    if not trips_by_date:
        raise RuntimeError("No active service dates found for selected line.")
    peak_date, peak_trips = max(trips_by_date.items(), key=lambda item: item[1])
    return peak_date, int(peak_trips)


def load_stop_names(gtfs_dir: Path) -> dict[str, str]:
    names: dict[str, str] = {}
    with (gtfs_dir / "stops.txt").open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            sid = (row.get("stop_id") or "").strip()
            if sid:
                names[sid] = (row.get("stop_name") or "").strip()
    return names


def load_trip_rows_for_day(
    gtfs_dir: Path,
    active_trip_ids: set[str],
) -> dict[str, list[tuple[int, str, int | None, int | None]]]:
    rows: dict[str, list[tuple[int, str, int | None, int | None]]] = defaultdict(list)
    with (gtfs_dir / "stop_times.txt").open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            tid = (row.get("trip_id") or "").strip()
            if tid not in active_trip_ids:
                continue
            sid = (row.get("stop_id") or "").strip()
            seq_raw = (row.get("stop_sequence") or "").strip()
            if not sid or not seq_raw:
                continue
            try:
                seq = int(seq_raw)
            except ValueError:
                continue
            dep = parse_time_to_seconds((row.get("departure_time") or "").strip())
            arr = parse_time_to_seconds((row.get("arrival_time") or "").strip())
            rows[tid].append((seq, sid, dep, arr))
    return rows


def infer_directions(
    trip_rows: dict[str, list[tuple[int, str, int | None, int | None]]],
    stop_names: dict[str, str],
) -> dict[str, str]:
    pair_counts: Counter[tuple[str, str]] = Counter()
    trip_pair: dict[str, tuple[str, str]] = {}

    for tid, rows in trip_rows.items():
        ordered = sorted(rows, key=lambda item: item[0])
        if not ordered:
            continue
        pair = (ordered[0][1], ordered[-1][1])
        pair_counts[pair] += 1
        trip_pair[tid] = pair

    top_pairs = pair_counts.most_common(2)
    if not top_pairs:
        return {tid: "direction_1" for tid in trip_rows}

    labels: dict[tuple[str, str], str] = {}
    for i, (pair, _) in enumerate(top_pairs, start=1):
        labels[pair] = (
            f"direction_{i}: {stop_names.get(pair[0], pair[0])} -> {stop_names.get(pair[1], pair[1])}"
        )

    default_label = labels[top_pairs[0][0]]
    return {tid: labels.get(trip_pair.get(tid, ("", "")), default_label) for tid in trip_rows}


def compute_hourly_metrics(
    stop_departures: list[int],
    short_threshold: float,
) -> dict[int, dict[str, float | int]]:
    by_hour: dict[int, list[int]] = defaultdict(list)
    for t in sorted(stop_departures):
        hour = (t // 3600) % 24
        by_hour[hour].append(t)

    result: dict[int, dict[str, float | int]] = {}
    for hour in range(24):
        departures = by_hour.get(hour, [])
        if len(departures) < 2:
            result[hour] = {
                "departures": len(departures),
                "headways_n": max(len(departures) - 1, 0),
                "mean_headway_min": 0.0,
                "median_headway_min": 0.0,
                "cv_headway": 0.0,
                "short_ratio": 0.0,
                "bunching_index": 0.0,
            }
            continue

        hw = [(b - a) / 60.0 for a, b in zip(departures, departures[1:]) if b > a]
        if not hw:
            result[hour] = {
                "departures": len(departures),
                "headways_n": 0,
                "mean_headway_min": 0.0,
                "median_headway_min": 0.0,
                "cv_headway": 0.0,
                "short_ratio": 0.0,
                "bunching_index": 0.0,
            }
            continue

        mean_hw = statistics.mean(hw)
        cv_hw = (statistics.pstdev(hw) / mean_hw) if mean_hw > 0 else 0.0
        short_ratio = sum(1 for x in hw if x < short_threshold) / len(hw)

        result[hour] = {
            "departures": len(departures),
            "headways_n": len(hw),
            "mean_headway_min": round(mean_hw, 3),
            "median_headway_min": round(statistics.median(hw), 3),
            "cv_headway": round(cv_hw, 3),
            "short_ratio": round(short_ratio, 3),
            "bunching_index": bunching_index(cv_hw, short_ratio),
        }

    return result


def write_direction_csv(path: Path, metrics: dict[str, dict[int, dict[str, float | int]]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "direction",
                "hour",
                "departures",
                "headways_n",
                "mean_headway_min",
                "median_headway_min",
                "cv_headway",
                "short_ratio",
                "bunching_index",
            ]
        )
        for direction in sorted(metrics):
            for hour in range(24):
                m = metrics[direction][hour]
                writer.writerow(
                    [
                        direction,
                        hour,
                        m["departures"],
                        m["headways_n"],
                        m["mean_headway_min"],
                        m["median_headway_min"],
                        m["cv_headway"],
                        m["short_ratio"],
                        m["bunching_index"],
                    ]
                )


def write_key_stop_csv(
    path: Path,
    metrics: dict[tuple[str, str, str], dict[int, dict[str, float | int]]],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "direction",
                "stop_id",
                "stop_name",
                "hour",
                "departures",
                "headways_n",
                "mean_headway_min",
                "median_headway_min",
                "cv_headway",
                "short_ratio",
                "bunching_index",
            ]
        )
        keys = sorted(metrics.keys(), key=lambda item: (item[0], item[2]))
        for direction, stop_id, stop_name in keys:
            for hour in range(24):
                m = metrics[(direction, stop_id, stop_name)][hour]
                writer.writerow(
                    [
                        direction,
                        stop_id,
                        stop_name,
                        hour,
                        m["departures"],
                        m["headways_n"],
                        m["mean_headway_min"],
                        m["median_headway_min"],
                        m["cv_headway"],
                        m["short_ratio"],
                        m["bunching_index"],
                    ]
                )


def plot_direction_hourly(path: Path, metrics: dict[str, dict[int, dict[str, float | int]]]) -> None:
    hours = list(range(24))
    colors = ["#2563eb", "#dc2626", "#059669", "#7c3aed"]

    fig, ax = plt.subplots(figsize=(12, 5))
    for idx, direction in enumerate(sorted(metrics)):
        y = [float(metrics[direction][h]["bunching_index"]) for h in hours]
        ax.plot(hours, y, marker="o", linewidth=2.0, color=colors[idx % len(colors)], label=direction)

    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Bunching index")
    ax.set_title("Bus 5 hourly bunching index by direction")
    ax.set_xticks(hours)
    ax.set_ylim(0, 1)
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=220)
    plt.close(fig)


def plot_key_stop_heatmap(
    path: Path,
    metrics: dict[tuple[str, str, str], dict[int, dict[str, float | int]]],
) -> None:
    rows = sorted(metrics.keys(), key=lambda item: (item[0], item[2]))
    labels = [f"{direction.split(':')[0]} | {stop_name}" for direction, _, stop_name in rows]
    matrix: list[list[float]] = []

    for key in rows:
        row = [float(metrics[key][h]["bunching_index"]) for h in range(24)]
        matrix.append(row)

    fig, ax = plt.subplots(figsize=(14, max(4, len(rows) * 0.55)))
    image = ax.imshow(matrix, aspect="auto", cmap="magma", vmin=0, vmax=1)
    ax.set_title("Bus 5 bunching index heatmap by key stop and hour")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Direction and key stop")
    ax.set_xticks(list(range(24)))
    ax.set_yticks(list(range(len(labels))))
    ax.set_yticklabels(labels)
    fig.colorbar(image, ax=ax, label="Bunching index")
    fig.tight_layout()
    fig.savefig(path, dpi=220)
    plt.close(fig)


def write_summary(
    path: Path,
    peak_date: date,
    peak_trips: int,
    direction_metrics: dict[str, dict[int, dict[str, float | int]]],
    key_stop_metrics: dict[tuple[str, str, str], dict[int, dict[str, float | int]]],
) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write("Bus 5 bunching report\n")
        f.write("=" * 60 + "\n")
        f.write(f"Peak scheduled date: {peak_date.isoformat()}\n")
        f.write(f"Peak scheduled trips: {peak_trips}\n")

        f.write("\nDirection-level peak bunching by hour\n")
        f.write("-" * 60 + "\n")
        for direction in sorted(direction_metrics):
            best_hour = max(range(24), key=lambda h: float(direction_metrics[direction][h]["bunching_index"]))
            m = direction_metrics[direction][best_hour]
            f.write(
                f"{direction} | hour={best_hour:02d} | bunching_index={m['bunching_index']} | "
                f"short_ratio={m['short_ratio']} | cv={m['cv_headway']}\n"
            )

        f.write("\nKey-stop peak bunching by hour\n")
        f.write("-" * 60 + "\n")
        for direction, stop_id, stop_name in sorted(key_stop_metrics.keys(), key=lambda item: (item[0], item[2])):
            best_hour = max(
                range(24),
                key=lambda h: float(key_stop_metrics[(direction, stop_id, stop_name)][h]["bunching_index"]),
            )
            m = key_stop_metrics[(direction, stop_id, stop_name)][best_hour]
            f.write(
                f"{direction} | {stop_name} ({stop_id}) | hour={best_hour:02d} | "
                f"bunching_index={m['bunching_index']} | short_ratio={m['short_ratio']} | cv={m['cv_headway']}\n"
            )


def main() -> None:
    args = parse_args()
    gtfs_dir = Path(args.gtfs_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ensure_files(gtfs_dir)
    route_ids = load_routes(gtfs_dir, args.line, args.route_type)
    if not route_ids:
        raise RuntimeError("No matching routes found for line and route_type.")

    trip_service, trips_per_service = load_trips(gtfs_dir, route_ids)
    if not trip_service:
        raise RuntimeError("No trips found for selected routes.")

    active_dates_by_service = load_active_dates_by_service(gtfs_dir, set(trips_per_service.keys()))
    peak_date, peak_trips = pick_peak_date(active_dates_by_service, trips_per_service)

    active_services = {sid for sid, days in active_dates_by_service.items() if peak_date in days}
    active_trip_ids = {tid for tid, sid in trip_service.items() if sid in active_services}

    stop_names = load_stop_names(gtfs_dir)
    trip_rows = load_trip_rows_for_day(gtfs_dir, active_trip_ids)
    if not trip_rows:
        raise RuntimeError("No stop_times found for active trips on peak date.")

    direction_by_trip = infer_directions(trip_rows, stop_names)

    # Collect departures by direction and by (direction, stop).
    # Direction-level metrics use trip-start departures only.
    direction_departures: dict[str, list[int]] = defaultdict(list)
    stop_direction_departures: dict[tuple[str, str, str], list[int]] = defaultdict(list)

    for tid, rows in trip_rows.items():
        direction = direction_by_trip.get(tid, "direction_1")
        ordered = sorted(rows, key=lambda item: item[0])
        if ordered:
            first_dep = ordered[0][2] if ordered[0][2] is not None else ordered[0][3]
            if first_dep is not None:
                direction_departures[direction].append(first_dep)

        for _, sid, dep, arr in rows:
            t = dep if dep is not None else arr
            if t is None:
                continue
            stop_direction_departures[(direction, sid, stop_names.get(sid, sid))].append(t)

    # Direction-level hourly metrics.
    direction_hourly: dict[str, dict[int, dict[str, float | int]]] = {}
    for direction, departures in direction_departures.items():
        direction_hourly[direction] = compute_hourly_metrics(departures, args.short_headway_min)

    # Key stops: top N by departures per direction.
    key_stop_hourly: dict[tuple[str, str, str], dict[int, dict[str, float | int]]] = {}
    for direction in sorted(direction_departures):
        candidates = [
            (key, len(values))
            for key, values in stop_direction_departures.items()
            if key[0] == direction
        ]
        top_keys = [key for key, _ in sorted(candidates, key=lambda item: item[1], reverse=True)[: args.key_stops_per_direction]]
        for key in top_keys:
            key_stop_hourly[key] = compute_hourly_metrics(stop_direction_departures[key], args.short_headway_min)

    direction_csv = output_dir / "bus5_bunching_hourly_direction.csv"
    key_stop_csv = output_dir / "bus5_bunching_hourly_keystops.csv"
    direction_png = output_dir / "bus5_bunching_hourly_direction.png"
    key_stop_png = output_dir / "bus5_bunching_keystops_heatmap.png"
    summary_txt = output_dir / "bus5_bunching_summary.txt"

    write_direction_csv(direction_csv, direction_hourly)
    write_key_stop_csv(key_stop_csv, key_stop_hourly)
    plot_direction_hourly(direction_png, direction_hourly)
    plot_key_stop_heatmap(key_stop_png, key_stop_hourly)
    write_summary(summary_txt, peak_date, peak_trips, direction_hourly, key_stop_hourly)

    print("Bus 5 bunching analysis created")
    print(f"Peak date used: {peak_date.isoformat()}")
    print(f"Peak scheduled trips: {peak_trips}")
    print(f"Direction CSV: {direction_csv.resolve()}")
    print(f"Key-stop CSV: {key_stop_csv.resolve()}")
    print(f"Direction chart: {direction_png.resolve()}")
    print(f"Key-stop heatmap: {key_stop_png.resolve()}")
    print(f"Summary: {summary_txt.resolve()}")


if __name__ == "__main__":
    main()
