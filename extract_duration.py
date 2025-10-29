"""CLI utility for extracting duration data from CSV files."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys
from typing import Optional, Sequence


def _readable_file(path_str: str) -> Path:
    """Return a Path for ``path_str`` if it is a readable file."""
    path = Path(path_str)
    if not path.exists():
        raise argparse.ArgumentTypeError(f"Input file '{path}' does not exist.")
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"Input file '{path}' is not a file.")
    try:
        with path.open("r", encoding="utf-8"):
            pass
    except OSError as exc:
        raise argparse.ArgumentTypeError(
            f"Input file '{path}' cannot be read: {exc.strerror or exc}"
        ) from exc
    return path


def _output_path(path_str: str) -> Path:
    """Return a Path for ``path_str`` pointing to the output file."""
    return Path(path_str)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract duration information from a CSV file."
    )
    parser.add_argument(
        "input_csv",
        type=_readable_file,
        help="Path to the input CSV file containing source data.",
    )
    parser.add_argument(
        "output_csv",
        type=_output_path,
        help="Path to the output CSV file that will receive the results.",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Text encoding used for reading and writing CSV files (default: UTF-8).",
    )
    return parser.parse_args(argv)


def process_csv(input_path: Path, output_path: Path, encoding: str) -> None:
    """Copy CSV contents from ``input_path`` to ``output_path``."""
    with input_path.open("r", newline="", encoding=encoding) as src:
        reader = csv.reader(src)
        with output_path.open("w", newline="", encoding=encoding) as dst:
            writer = csv.writer(dst)
            writer.writerows(reader)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point for the duration extraction CLI."""
    try:
        args = parse_args(argv)
        process_csv(args.input_csv, args.output_csv, args.encoding)
    except argparse.ArgumentTypeError as exc:
        print(exc, file=sys.stderr)
        return 2
    except (OSError, csv.Error) as exc:
        print(f"Failed to process CSV files: {exc}", file=sys.stderr)
        return 1

    print(f"Successfully processed '{args.input_csv}' into '{args.output_csv}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
