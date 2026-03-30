from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def default_source_gtfs() -> str:
    root = Path(__file__).resolve().parent
    candidates = ["input/gtfs5bus", "input/gtfs_de_2026-03-30"]
    for candidate in candidates:
        if (root / candidate).exists():
            return candidate
    return candidates[0]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run complete GTFS bus 5 pipeline: Hamburg filter -> bus 5 filter -> density analysis."
    )
    parser.add_argument(
        "--source-gtfs",
        default=default_source_gtfs(),
        help="Original GTFS source folder.",
    )
    parser.add_argument(
        "--hamburg-dir",
        default="output/gtfs_hamburg",
        help="Output folder for Hamburg subset.",
    )
    parser.add_argument(
        "--bus5-dir",
        default="output/gtfs_5bus",
        help="Output folder for bus 5 subset.",
    )
    parser.add_argument(
        "--density-dir",
        default="output/bus5_density",
        help="Output folder for density analysis charts and CSVs.",
    )
    parser.add_argument("--line", default="5", help="Route short name to filter.")
    parser.add_argument("--route-type", default="3", help="GTFS route_type to filter.")
    parser.add_argument("--zoom", type=int, default=13, help="Basemap zoom level.")
    parser.add_argument(
        "--hamburg-mode",
        choices=["strict", "connected"],
        default="strict",
        help="Hamburg filter mode.",
    )
    return parser.parse_args()


def run_step(step_name: str, command: list[str]) -> None:
    print(f"\n[{step_name}] {' '.join(command)}")
    subprocess.run(command, check=True)


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parent
    python = sys.executable

    run_step(
        "1/3 Filter Hamburg",
        [
            python,
            str(root / "filter_hamburg_gtfs.py"),
            "--input-dir",
            args.source_gtfs,
            "--output-dir",
            args.hamburg_dir,
            "--mode",
            args.hamburg_mode,
        ],
    )

    run_step(
        "2/3 Filter Bus Line",
        [
            python,
            str(root / "filter_bus5_gtfs.py"),
            "--input-dir",
            args.hamburg_dir,
            "--output-dir",
            args.bus5_dir,
            "--line",
            args.line,
            "--route-type",
            args.route_type,
        ],
    )

    run_step(
        "3/3 Density Analysis",
        [
            python,
            str(root / "analyze_bus5_density.py"),
            "--gtfs-dir",
            args.bus5_dir,
            "--output-dir",
            args.density_dir,
            "--line",
            args.line,
            "--route-type",
            args.route_type,
            "--zoom",
            str(args.zoom),
        ],
    )

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
