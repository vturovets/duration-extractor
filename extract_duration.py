"""CLI utility for extracting duration data from CSV files.

Usage:
    python extract_duration.py input.csv [output.csv]

If ``output.csv`` is omitted, the tool will create a file alongside ``input.csv``
with the same name prefixed by ``durations_``.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Iterable, Iterator, List, Optional, Sequence


logger = logging.getLogger(__name__)


class DurationExtractionError(RuntimeError):
    """Raised when the input CSV cannot be parsed for duration extraction."""


@dataclass(frozen=True)
class DurationExtractionStats:
    """Simple container for extraction summary statistics."""

    processed: int
    skipped: int
    malformed: int


@dataclass(frozen=True)
class BatchSummaryRow:
    """Summary information computed for a single CSV file in batch mode."""

    date: str
    observations: int
    percentile_95: Optional[float]
    time_of_day: str
    intensity: Optional[float]


def _normalize_duration_to_milliseconds(raw_value: str) -> float:
    """Return ``raw_value`` expressed in milliseconds."""

    normalized = raw_value.strip()
    normalized_lower = normalized.lower()

    if normalized_lower.endswith("ms"):
        magnitude_text = normalized[:-2].strip()
        multiplier = 1.0
    elif normalized_lower.endswith(("μs", "µs", "us")):
        magnitude_text = normalized[:-2].strip()
        multiplier = 0.001
    elif normalized_lower.endswith("s"):
        magnitude_text = normalized[:-1].strip()
        multiplier = 1000.0
    else:
        raise ValueError("missing units")

    try:
        magnitude = float(magnitude_text)
    except ValueError as exc:
        raise ValueError(str(exc)) from exc

    return magnitude * multiplier


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


def _readable_directory(path_str: str) -> Path:
    """Return a Path for ``path_str`` if it is an existing directory."""
    path = Path(path_str)
    if not path.exists():
        raise argparse.ArgumentTypeError(f"Batch directory '{path}' does not exist.")
    if not path.is_dir():
        raise argparse.ArgumentTypeError(
            f"Batch directory '{path}' is not a directory."
        )
    return path


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract duration information from a CSV file."
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        type=_readable_file,
        help="Path to the input CSV file containing source data.",
    )
    parser.add_argument(
        "output_csv",
        nargs="?",
        type=_output_path,
        help=(
            "Path to the output CSV file that will receive the results."
            " Defaults to the input file name prefixed with 'durations_'."
        ),
    )
    parser.add_argument(
        "--batch-dir",
        type=_readable_directory,
        help="Process every CSV file within the specified directory.",
    )
    parser.add_argument(
        "--summary-output",
        type=_output_path,
        help=(
            "Path to the summary CSV generated in batch mode (default: 'summary.csv'"
            " inside the batch directory)."
        ),
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Text encoding used for reading and writing CSV files (default: UTF-8).",
    )
    args = parser.parse_args(argv)

    if bool(args.input_csv) == bool(args.batch_dir):
        raise argparse.ArgumentTypeError(
            "Provide either an input CSV file or --batch-dir, but not both."
        )

    if args.batch_dir is None and args.summary_output is not None:
        raise argparse.ArgumentTypeError(
            "--summary-output can only be used together with --batch-dir."
        )

    if args.batch_dir is not None and args.summary_output is None:
        args.summary_output = args.batch_dir / "summary.csv"

    return args


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

                try:
                    millis = _normalize_duration_to_milliseconds(raw_value)
                except ValueError as exc:
                    self.malformed_count += 1
                    message = str(exc)
                    if message == "missing units":
                        logger.warning(
                            "Row %d has malformed Duration '%s': missing units; skipping.",
                            row_number,
                            raw_value,
                        )
                    else:
                        logger.warning(
                            "Row %d has malformed Duration '%s': %s; skipping.",
                            row_number,
                            raw_value,
                            message,
                        )
                    continue

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


def _parse_iso8601(value: str) -> Optional[datetime]:
    """Parse ``value`` as an ISO-8601 datetime."""

    text = value.strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _time_of_day_label(timestamp: datetime) -> str:
    """Return a human-readable time-of-day label for ``timestamp``."""

    hour = timestamp.astimezone(timezone.utc).hour
    if 5 <= hour < 12:
        return "Morning"
    if 12 <= hour < 18:
        return "Afternoon"
    return "Evening"


def _calculate_percentile(values: Sequence[float], percentile: float) -> float:
    """Return the ``percentile`` (0-1) from ``values`` using the nearest-rank method."""

    if not 0 < percentile <= 1:
        raise ValueError("percentile must be within (0, 1]")
    if not values:
        raise ValueError("values must be non-empty")

    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def summarize_csv(input_path: Path, *, encoding: str) -> BatchSummaryRow:
    """Compute summary statistics for ``input_path``."""

    durations: List[float] = []
    timestamps: List[datetime] = []
    date_counts: Counter[str] = Counter()
    time_of_day_counts: Counter[str] = Counter()

    with input_path.open("r", newline="", encoding=encoding) as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise DurationExtractionError("Input CSV is missing a header row.")
        if "Duration" not in reader.fieldnames:
            raise DurationExtractionError(
                "Input CSV does not contain a 'Duration' column."
            )
        if "Date" not in reader.fieldnames:
            raise DurationExtractionError(
                "Input CSV does not contain a 'Date' column required for summaries."
            )

        for row in reader:
            raw_duration = row.get("Duration")
            if raw_duration is None or raw_duration.strip() == "":
                continue

            try:
                millis = _normalize_duration_to_milliseconds(raw_duration)
            except ValueError:
                continue

            raw_timestamp = row.get("Date")
            timestamp = _parse_iso8601(raw_timestamp or "") if raw_timestamp else None
            if timestamp is None:
                continue

            durations.append(millis)
            timestamps.append(timestamp)

            date_key = timestamp.date().isoformat()
            date_counts[date_key] += 1

            label = _time_of_day_label(timestamp)
            time_of_day_counts[label] += 1

    observations = len(durations)
    date_text = ""
    time_of_day = ""
    intensity: Optional[float] = None
    percentile_95: Optional[float] = None

    if observations:
        percentile_95 = _calculate_percentile(durations, 0.95)

    if timestamps:
        earliest = min(timestamps)
        latest = max(timestamps)
        span_seconds = (latest - earliest).total_seconds()
        if span_seconds > 0:
            intensity = observations / span_seconds

        if date_counts:
            dominant_date, _ = max(
                date_counts.items(), key=lambda item: (item[1], item[0])
            )
            date_text = dominant_date

        if time_of_day_counts:
            order = {"Morning": 0, "Afternoon": 1, "Evening": 2}
            time_of_day = max(
                time_of_day_counts.items(),
                key=lambda item: (item[1], -order.get(item[0], 99)),
            )[0]

    return BatchSummaryRow(
        date=date_text,
        observations=observations,
        percentile_95=percentile_95,
        time_of_day=time_of_day,
        intensity=intensity,
    )


def _write_summary(
    output_path: Path, rows: Sequence[BatchSummaryRow], *, encoding: str
) -> None:
    """Persist ``rows`` to ``output_path`` as a CSV table."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding=encoding) as handle:
        writer = csv.writer(handle)
        writer.writerow(["Date", "n", "P95", "Time of Day", "Intensity"])
        for row in rows:
            writer.writerow(
                [
                    row.date,
                    str(row.observations),
                    "" if row.percentile_95 is None else format(row.percentile_95, "g"),
                    row.time_of_day,
                    "" if row.intensity is None else format(row.intensity, "g"),
                ]
            )


def process_directory(
    directory: Path, *, summary_output: Path, encoding: str
) -> Sequence[BatchSummaryRow]:
    """Process every CSV file within ``directory`` and return summary information."""

    csv_files = sorted(
        path for path in directory.iterdir() if path.is_file() and path.suffix.lower() == ".csv"
    )

    summaries: List[BatchSummaryRow] = []

    for input_path in csv_files:
        output_path = input_path.with_name(f"durations_{input_path.name}")
        process_csv(input_path, output_path, encoding)
        summaries.append(summarize_csv(input_path, encoding=encoding))

    _write_summary(summary_output, summaries, encoding=encoding)
    return summaries


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point for the duration extraction CLI."""
    try:
        args = parse_args(argv)
        logging.basicConfig(level=logging.INFO, stream=sys.stderr)

        if args.batch_dir is not None:
            summaries = process_directory(
                args.batch_dir,
                summary_output=args.summary_output,
                encoding=args.encoding,
            )
            print(
                f"Successfully processed directory '{args.batch_dir}' "
                f"and wrote {len(summaries)} summary row(s) to "
                f"'{args.summary_output}'."
            )
            return 0

        output_path = args.output_csv
        if output_path is None:
            output_path = args.input_csv.with_name(
                f"durations_{args.input_csv.name}"
            )

        process_csv(args.input_csv, output_path, args.encoding)
    except argparse.ArgumentTypeError as exc:
        print(exc, file=sys.stderr)
        return 2
    except (IOError, csv.Error, DurationExtractionError) as exc:
        print(f"Failed to process CSV files: {exc}", file=sys.stderr)
        return 1

    print(f"Successfully processed '{args.input_csv}' into '{output_path}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
