from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path
from typing import Iterable


GTFS_FILES = [
    "agency.txt",
    "attributions.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "feed_info.txt",
    "routes.txt",
    "stop_times.txt",
    "stops.txt",
    "trips.txt",
]


def default_input_dir() -> str:
    root = Path(__file__).resolve().parent
    candidates = ["input/gtfs5bus", "input/gtfs_de_2026-03-30"]
    for candidate in candidates:
        if (root / candidate).exists():
            return candidate
    return candidates[0]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a Hamburg-focused GTFS subset from a larger feed."
    )
    parser.add_argument(
        "--input-dir",
        default=default_input_dir(),
        help="Directory with source GTFS .txt files.",
    )
    parser.add_argument(
        "--output-dir",
        default="output/gtfs_hamburg",
        help="Directory where filtered GTFS files are written.",
    )
    parser.add_argument(
        "--mode",
        choices=["strict", "connected"],
        default="strict",
        help=(
            "strict: keep only stop_times rows for Hamburg stops. "
            "connected: keep all stops in selected trips if trip touches Hamburg."
        ),
    )
    parser.add_argument("--min-lat", type=float, default=53.35, help="Hamburg bbox min latitude.")
    parser.add_argument("--max-lat", type=float, default=53.75, help="Hamburg bbox max latitude.")
    parser.add_argument("--min-lon", type=float, default=9.70, help="Hamburg bbox min longitude.")
    parser.add_argument("--max-lon", type=float, default=10.35, help="Hamburg bbox max longitude.")
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="CSV file encoding (default: utf-8).",
    )
    return parser.parse_args()


def ensure_input_files(input_dir: Path) -> None:
    missing = [name for name in GTFS_FILES if not (input_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing GTFS files in {input_dir}: {', '.join(missing)}")


def is_in_bbox(lat: str, lon: str, *, min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> bool:
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        return False
    return min_lat <= lat_f <= max_lat and min_lon <= lon_f <= max_lon


def iter_csv_rows(path: Path, encoding: str) -> Iterable[dict[str, str]]:
    with path.open("r", encoding=encoding, errors="ignore", newline="") as f:
        yield from csv.DictReader(f)


def read_hamburg_stop_ids(
    stops_path: Path,
    *,
    encoding: str,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
) -> set[str]:
    selected: set[str] = set()
    for row in iter_csv_rows(stops_path, encoding):
        stop_id = (row.get("stop_id") or "").strip()
        if not stop_id:
            continue
        if is_in_bbox(
            row.get("stop_lat", ""),
            row.get("stop_lon", ""),
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        ):
            selected.add(stop_id)
    return selected


def discover_selected_trip_ids(stop_times_path: Path, selected_stop_ids: set[str], *, encoding: str) -> set[str]:
    trip_ids: set[str] = set()
    for row in iter_csv_rows(stop_times_path, encoding):
        stop_id = (row.get("stop_id") or "").strip()
        if stop_id in selected_stop_ids:
            trip_id = (row.get("trip_id") or "").strip()
            if trip_id:
                trip_ids.add(trip_id)
    return trip_ids


def write_stop_times_strict(
    src_path: Path,
    dst_path: Path,
    selected_stop_ids: set[str],
    *,
    encoding: str,
) -> tuple[set[str], set[str], int]:
    selected_trip_ids: set[str] = set()
    used_stop_ids: set[str] = set()
    written = 0

    with src_path.open("r", encoding=encoding, errors="ignore", newline="") as src, dst_path.open(
        "w", encoding=encoding, newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError("stop_times.txt has no header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            stop_id = (row.get("stop_id") or "").strip()
            if stop_id not in selected_stop_ids:
                continue
            trip_id = (row.get("trip_id") or "").strip()
            if trip_id:
                selected_trip_ids.add(trip_id)
            used_stop_ids.add(stop_id)
            writer.writerow(row)
            written += 1

    return selected_trip_ids, used_stop_ids, written


def write_stop_times_connected(
    src_path: Path,
    dst_path: Path,
    selected_trip_ids: set[str],
    *,
    encoding: str,
) -> tuple[set[str], int]:
    used_stop_ids: set[str] = set()
    written = 0

    with src_path.open("r", encoding=encoding, errors="ignore", newline="") as src, dst_path.open(
        "w", encoding=encoding, newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError("stop_times.txt has no header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            trip_id = (row.get("trip_id") or "").strip()
            if trip_id not in selected_trip_ids:
                continue
            stop_id = (row.get("stop_id") or "").strip()
            if stop_id:
                used_stop_ids.add(stop_id)
            writer.writerow(row)
            written += 1

    return used_stop_ids, written


def filter_trips(
    src_path: Path,
    dst_path: Path,
    selected_trip_ids: set[str],
    *,
    encoding: str,
) -> tuple[set[str], set[str], int]:
    route_ids: set[str] = set()
    service_ids: set[str] = set()
    written = 0

    with src_path.open("r", encoding=encoding, errors="ignore", newline="") as src, dst_path.open(
        "w", encoding=encoding, newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError("trips.txt has no header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            trip_id = (row.get("trip_id") or "").strip()
            if trip_id not in selected_trip_ids:
                continue
            route_id = (row.get("route_id") or "").strip()
            service_id = (row.get("service_id") or "").strip()
            if route_id:
                route_ids.add(route_id)
            if service_id:
                service_ids.add(service_id)
            writer.writerow(row)
            written += 1

    return route_ids, service_ids, written


def filter_routes(
    src_path: Path,
    dst_path: Path,
    selected_route_ids: set[str],
    *,
    encoding: str,
) -> tuple[set[str], int]:
    agency_ids: set[str] = set()
    written = 0

    with src_path.open("r", encoding=encoding, errors="ignore", newline="") as src, dst_path.open(
        "w", encoding=encoding, newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError("routes.txt has no header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            route_id = (row.get("route_id") or "").strip()
            if route_id not in selected_route_ids:
                continue
            agency_id = (row.get("agency_id") or "").strip()
            if agency_id:
                agency_ids.add(agency_id)
            writer.writerow(row)
            written += 1

    return agency_ids, written


def filter_services(
    calendar_src: Path,
    calendar_dst: Path,
    cal_dates_src: Path,
    cal_dates_dst: Path,
    service_ids: set[str],
    *,
    encoding: str,
) -> tuple[int, int]:
    calendar_written = 0
    cal_dates_written = 0

    with calendar_src.open("r", encoding=encoding, errors="ignore", newline="") as src, calendar_dst.open(
        "w", encoding=encoding, newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError("calendar.txt has no header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            service_id = (row.get("service_id") or "").strip()
            if service_id in service_ids:
                writer.writerow(row)
                calendar_written += 1

    with cal_dates_src.open("r", encoding=encoding, errors="ignore", newline="") as src, cal_dates_dst.open(
        "w", encoding=encoding, newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError("calendar_dates.txt has no header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            service_id = (row.get("service_id") or "").strip()
            if service_id in service_ids:
                writer.writerow(row)
                cal_dates_written += 1

    return calendar_written, cal_dates_written


def filter_stops(src_path: Path, dst_path: Path, selected_stop_ids: set[str], *, encoding: str) -> int:
    written = 0
    with src_path.open("r", encoding=encoding, errors="ignore", newline="") as src, dst_path.open(
        "w", encoding=encoding, newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError("stops.txt has no header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            stop_id = (row.get("stop_id") or "").strip()
            if stop_id in selected_stop_ids:
                writer.writerow(row)
                written += 1
    return written


def filter_agency(src_path: Path, dst_path: Path, selected_agency_ids: set[str], *, encoding: str) -> int:
    written = 0
    with src_path.open("r", encoding=encoding, errors="ignore", newline="") as src, dst_path.open(
        "w", encoding=encoding, newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError("agency.txt has no header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            agency_id = (row.get("agency_id") or "").strip()
            if agency_id in selected_agency_ids:
                writer.writerow(row)
                written += 1
    return written


def copy_passthrough(input_dir: Path, output_dir: Path) -> None:
    for name in ("feed_info.txt", "attributions.txt"):
        shutil.copy2(input_dir / name, output_dir / name)


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    ensure_input_files(input_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    hamburg_stop_ids = read_hamburg_stop_ids(
        input_dir / "stops.txt",
        encoding=args.encoding,
        min_lat=args.min_lat,
        max_lat=args.max_lat,
        min_lon=args.min_lon,
        max_lon=args.max_lon,
    )
    if not hamburg_stop_ids:
        raise RuntimeError("No stops found in selected Hamburg bounding box.")

    stop_times_src = input_dir / "stop_times.txt"
    stop_times_dst = output_dir / "stop_times.txt"

    if args.mode == "strict":
        selected_trip_ids, used_stop_ids, stop_times_written = write_stop_times_strict(
            stop_times_src,
            stop_times_dst,
            hamburg_stop_ids,
            encoding=args.encoding,
        )
    else:
        selected_trip_ids = discover_selected_trip_ids(
            stop_times_src,
            hamburg_stop_ids,
            encoding=args.encoding,
        )
        used_stop_ids, stop_times_written = write_stop_times_connected(
            stop_times_src,
            stop_times_dst,
            selected_trip_ids,
            encoding=args.encoding,
        )

    routes_selected, services_selected, trips_written = filter_trips(
        input_dir / "trips.txt",
        output_dir / "trips.txt",
        selected_trip_ids,
        encoding=args.encoding,
    )

    agencies_selected, routes_written = filter_routes(
        input_dir / "routes.txt",
        output_dir / "routes.txt",
        routes_selected,
        encoding=args.encoding,
    )

    stops_written = filter_stops(
        input_dir / "stops.txt",
        output_dir / "stops.txt",
        used_stop_ids,
        encoding=args.encoding,
    )

    agency_written = filter_agency(
        input_dir / "agency.txt",
        output_dir / "agency.txt",
        agencies_selected,
        encoding=args.encoding,
    )

    calendar_written, calendar_dates_written = filter_services(
        input_dir / "calendar.txt",
        output_dir / "calendar.txt",
        input_dir / "calendar_dates.txt",
        output_dir / "calendar_dates.txt",
        services_selected,
        encoding=args.encoding,
    )

    copy_passthrough(input_dir, output_dir)

    print("Hamburg GTFS subset created.")
    print(f"Mode: {args.mode}")
    print(f"Output directory: {output_dir.resolve()}")
    print(f"Hamburg bbox stops (seed): {len(hamburg_stop_ids)}")
    print(f"Trips selected: {len(selected_trip_ids)}")
    print(f"stop_times written: {stop_times_written}")
    print(f"stops written: {stops_written}")
    print(f"trips written: {trips_written}")
    print(f"routes written: {routes_written}")
    print(f"agency written: {agency_written}")
    print(f"calendar written: {calendar_written}")
    print(f"calendar_dates written: {calendar_dates_written}")


if __name__ == "__main__":
    main()
