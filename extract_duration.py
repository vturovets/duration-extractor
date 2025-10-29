"""CLI utility for extracting duration data from CSV files.

Usage:
    python extract_duration.py input.csv output.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Iterable, Iterator, Optional, Sequence


logger = logging.getLogger(__name__)


class DurationExtractionError(RuntimeError):
    """Raised when the input CSV cannot be parsed for duration extraction."""


@dataclass(frozen=True)
class DurationExtractionStats:
    """Simple container for extraction summary statistics."""

    processed: int
    skipped: int
    malformed: int


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


class DurationStream(Iterable[str]):
    """Single-use iterable that streams normalized duration values from a CSV file."""

    def __init__(self, input_path: Path, *, encoding: str) -> None:
        self._input_path = input_path
        self._encoding = encoding
        self.processed_count = 0
        self.skipped_count = 0
        self.malformed_count = 0
        self._consumed = False

    def __iter__(self) -> Iterator[str]:
        if self._consumed:
            raise RuntimeError("DurationStream objects can only be iterated once.")
        self._consumed = True

        with self._input_path.open("r", newline="", encoding=self._encoding) as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise DurationExtractionError("Input CSV is missing a header row.")
            if "Duration" not in reader.fieldnames:
                raise DurationExtractionError(
                    "Input CSV does not contain a 'Duration' column."
                )

            for row_number, row in enumerate(reader, start=2):
                raw_value = row.get("Duration")
                if raw_value is None or raw_value.strip() == "":
                    self.skipped_count += 1
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
                    self.malformed_count += 1
                    logger.warning(
                        "Row %d has malformed Duration '%s': missing units; skipping.",
                        row_number,
                        raw_value,
                    )
                    continue

                try:
                    magnitude = float(magnitude_text)
                except ValueError as exc:
                    self.malformed_count += 1
                    logger.warning(
                        "Row %d has malformed Duration '%s': %s; skipping.",
                        row_number,
                        raw_value,
                        exc,
                    )
                    continue

                millis = magnitude * multiplier
                self.processed_count += 1
                yield format(millis, "g")


def extract_durations(
    input_path: Path, *, encoding: str = "utf-8"
) -> DurationStream:
    """Return a stream of normalized duration values from ``input_path``."""

    return DurationStream(input_path, encoding=encoding)


def write_durations(
    output_path: Path, values: Iterable[str], *, encoding: str = "utf-8"
) -> None:
    """Write ``values`` to ``output_path`` as a single-column CSV."""
    with output_path.open("w", newline="", encoding=encoding) as handle:
        writer = csv.writer(handle)
        for value in values:
            writer.writerow([value])


def process_csv(
    input_path: Path, output_path: Path, encoding: str
) -> DurationExtractionStats:
    """Extract normalized duration data and write it to ``output_path``."""

    logger.info(
        "Starting duration extraction from '%s' to '%s'.", input_path, output_path
    )
    durations = extract_durations(input_path, encoding=encoding)
    try:
        write_durations(output_path, durations, encoding=encoding)
    finally:
        stats = DurationExtractionStats(
            processed=durations.processed_count,
            skipped=durations.skipped_count,
            malformed=durations.malformed_count,
        )
        logger.info(
            "Completed duration extraction for '%s': processed=%d, skipped=%d, malformed=%d.",
            input_path,
            stats.processed,
            stats.skipped,
            stats.malformed,
        )
    return stats


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point for the duration extraction CLI."""
    try:
        args = parse_args(argv)
        logging.basicConfig(level=logging.INFO, stream=sys.stderr)
        process_csv(args.input_csv, args.output_csv, args.encoding)
    except argparse.ArgumentTypeError as exc:
        print(exc, file=sys.stderr)
        return 2
    except (IOError, csv.Error, DurationExtractionError) as exc:
        print(f"Failed to process CSV files: {exc}", file=sys.stderr)
        return 1

    print(f"Successfully processed '{args.input_csv}' into '{args.output_csv}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
