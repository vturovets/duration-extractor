"""CLI utility for extracting duration data from CSV files."""

from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path
import sys
from typing import Iterable, Iterator, Optional, Sequence


logger = logging.getLogger(__name__)


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


def extract_durations(
    input_path: Path, *, encoding: str = "utf-8"
) -> Iterable[str]:
    """Yield normalized duration values from ``input_path`` in milliseconds."""

    def _iterator() -> Iterator[str]:
        missing_count = 0
        malformed_count = 0

        try:
            with input_path.open("r", newline="", encoding=encoding) as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames is None:
                    raise ValueError("Input CSV is missing a header row.")
                if "Duration" not in reader.fieldnames:
                    raise ValueError("Input CSV does not contain a 'Duration' column.")

                for row_number, row in enumerate(reader, start=2):
                    raw_value = row.get("Duration")
                    if raw_value is None or raw_value.strip() == "":
                        missing_count += 1
                        continue

                    normalized = raw_value.strip()
                    normalized_lower = normalized.lower()

                    if normalized_lower.endswith("ms"):
                        magnitude_text = normalized[:-2].strip()
                        multiplier = 1.0
                    elif normalized_lower.endswith("s"):
                        magnitude_text = normalized[:-1].strip()
                        multiplier = 1000.0
                    else:
                        malformed_count += 1
                        logger.warning(
                            "Row %d has malformed Duration '%s': missing units; skipping.",
                            row_number,
                            raw_value,
                        )
                        continue

                    try:
                        magnitude = float(magnitude_text)
                    except ValueError as exc:
                        malformed_count += 1
                        logger.warning(
                            "Row %d has malformed Duration '%s': %s; skipping.",
                            row_number,
                            raw_value,
                            exc,
                        )
                        continue

                    millis = magnitude * multiplier
                    yield format(millis, "g")
        finally:
            if missing_count:
                logger.warning(
                    "Skipped %d row(s) with missing or empty Duration values.",
                    missing_count,
                )
            if malformed_count:
                logger.warning(
                    "Skipped %d row(s) with malformed Duration values.",
                    malformed_count,
                )

    return _iterator()


def write_durations(
    output_path: Path, values: Iterable[str], *, encoding: str = "utf-8"
) -> None:
    """Write ``values`` to ``output_path`` as a single-column CSV."""
    with output_path.open("w", newline="", encoding=encoding) as handle:
        writer = csv.writer(handle)
        for value in values:
            writer.writerow([value])


def process_csv(input_path: Path, output_path: Path, encoding: str) -> None:
    """Extract normalized duration data and write it to ``output_path``."""
    durations = extract_durations(input_path, encoding=encoding)
    write_durations(output_path, durations, encoding=encoding)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point for the duration extraction CLI."""
    try:
        args = parse_args(argv)
        logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
        process_csv(args.input_csv, args.output_csv, args.encoding)
    except argparse.ArgumentTypeError as exc:
        print(exc, file=sys.stderr)
        return 2
    except (OSError, csv.Error, ValueError) as exc:
        print(f"Failed to process CSV files: {exc}", file=sys.stderr)
        return 1

    print(f"Successfully processed '{args.input_csv}' into '{args.output_csv}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
