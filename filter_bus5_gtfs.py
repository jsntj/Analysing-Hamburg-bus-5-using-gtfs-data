from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path


REQUIRED_FILES = [
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter a GTFS dataset to bus line 5 only (or another line if specified)."
    )
    parser.add_argument(
        "--input-dir",
        default="output/gtfs_hamburg",
        help="Input GTFS folder (typically Hamburg subset).",
    )
    parser.add_argument(
        "--output-dir",
        default="output/gtfs_5bus",
        help="Output GTFS folder for filtered bus line dataset.",
    )
    parser.add_argument("--line", default="5", help="Route short name to keep.")
    parser.add_argument(
        "--route-type",
        default="3",
        help="GTFS route_type to keep. Default 3=bus. Use 'all' to disable this filter.",
    )
    parser.add_argument("--encoding", default="utf-8", help="CSV encoding.")
    return parser.parse_args()


def ensure_input_files(input_dir: Path) -> None:
    missing = [name for name in REQUIRED_FILES if not (input_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing GTFS files in {input_dir}: {', '.join(missing)}")


def write_filtered_routes(
    src_path: Path,
    dst_path: Path,
    *,
    line: str,
    route_type: str,
    encoding: str,
) -> tuple[set[str], set[str], int]:
    route_ids: set[str] = set()
    agency_ids: set[str] = set()
    written = 0

    line_filter = line.strip().lower()
    route_type_filter = route_type.strip()

    with src_path.open("r", encoding=encoding, errors="ignore", newline="") as src, dst_path.open(
        "w", encoding=encoding, newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError("routes.txt has no header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            short_name = (row.get("route_short_name") or "").strip()
            current_route_type = (row.get("route_type") or "").strip()
            if short_name.lower() != line_filter:
                continue
            if route_type_filter.lower() != "all" and current_route_type != route_type_filter:
                continue

            route_id = (row.get("route_id") or "").strip()
            if not route_id:
                continue

            agency_id = (row.get("agency_id") or "").strip()
            route_ids.add(route_id)
            if agency_id:
                agency_ids.add(agency_id)
            writer.writerow(row)
            written += 1

    return route_ids, agency_ids, written


def write_filtered_trips(
    src_path: Path,
    dst_path: Path,
    route_ids: set[str],
    *,
    encoding: str,
) -> tuple[set[str], set[str], int]:
    trip_ids: set[str] = set()
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
            route_id = (row.get("route_id") or "").strip()
            if route_id not in route_ids:
                continue

            trip_id = (row.get("trip_id") or "").strip()
            if not trip_id:
                continue

            service_id = (row.get("service_id") or "").strip()
            trip_ids.add(trip_id)
            if service_id:
                service_ids.add(service_id)
            writer.writerow(row)
            written += 1

    return trip_ids, service_ids, written


def write_filtered_stop_times(
    src_path: Path,
    dst_path: Path,
    trip_ids: set[str],
    *,
    encoding: str,
) -> tuple[set[str], int]:
    stop_ids: set[str] = set()
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
            if trip_id not in trip_ids:
                continue
            stop_id = (row.get("stop_id") or "").strip()
            if stop_id:
                stop_ids.add(stop_id)
            writer.writerow(row)
            written += 1

    return stop_ids, written


def write_filtered_stops(src_path: Path, dst_path: Path, stop_ids: set[str], *, encoding: str) -> int:
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
            parent_station = (row.get("parent_station") or "").strip()
            if stop_id in stop_ids or parent_station in stop_ids:
                writer.writerow(row)
                written += 1
    return written


def write_filtered_agency(src_path: Path, dst_path: Path, agency_ids: set[str], *, encoding: str) -> int:
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
            if agency_id in agency_ids:
                writer.writerow(row)
                written += 1
    return written


def write_filtered_services(
    calendar_src: Path,
    calendar_dst: Path,
    calendar_dates_src: Path,
    calendar_dates_dst: Path,
    service_ids: set[str],
    *,
    encoding: str,
) -> tuple[int, int]:
    cal_written = 0
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
                cal_written += 1

    with calendar_dates_src.open("r", encoding=encoding, errors="ignore", newline="") as src, calendar_dates_dst.open(
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

    return cal_written, cal_dates_written


def copy_passthrough(input_dir: Path, output_dir: Path) -> None:
    for name in ("feed_info.txt", "attributions.txt"):
        shutil.copy2(input_dir / name, output_dir / name)


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    ensure_input_files(input_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    route_ids, agency_ids, routes_written = write_filtered_routes(
        input_dir / "routes.txt",
        output_dir / "routes.txt",
        line=args.line,
        route_type=args.route_type,
        encoding=args.encoding,
    )
    if not route_ids:
        raise RuntimeError("No matching routes found for selected line and route_type.")

    trip_ids, service_ids, trips_written = write_filtered_trips(
        input_dir / "trips.txt",
        output_dir / "trips.txt",
        route_ids,
        encoding=args.encoding,
    )

    stop_ids, stop_times_written = write_filtered_stop_times(
        input_dir / "stop_times.txt",
        output_dir / "stop_times.txt",
        trip_ids,
        encoding=args.encoding,
    )

    stops_written = write_filtered_stops(
        input_dir / "stops.txt",
        output_dir / "stops.txt",
        stop_ids,
        encoding=args.encoding,
    )

    agency_written = write_filtered_agency(
        input_dir / "agency.txt",
        output_dir / "agency.txt",
        agency_ids,
        encoding=args.encoding,
    )

    cal_written, cal_dates_written = write_filtered_services(
        input_dir / "calendar.txt",
        output_dir / "calendar.txt",
        input_dir / "calendar_dates.txt",
        output_dir / "calendar_dates.txt",
        service_ids,
        encoding=args.encoding,
    )

    copy_passthrough(input_dir, output_dir)

    print("Bus line GTFS subset created")
    print(f"Output directory: {output_dir.resolve()}")
    print(f"routes: {routes_written}")
    print(f"trips: {trips_written}")
    print(f"stop_times: {stop_times_written}")
    print(f"stops: {stops_written}")
    print(f"agency: {agency_written}")
    print(f"calendar: {cal_written}")
    print(f"calendar_dates: {cal_dates_written}")


if __name__ == "__main__":
    main()
