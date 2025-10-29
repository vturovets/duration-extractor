import csv
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from extract_duration import (
    DurationExtractionError,
    extract_durations,
    process_csv,
)


def write_csv(path: Path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Duration"])
        writer.writerows([[value] for value in rows])


def test_extract_durations_converts_units_and_tracks_counts(tmp_path: Path):
    input_path = tmp_path / "input.csv"
    write_csv(
        input_path,
        ["100ms", "5s", "", "invalid"],
    )

    stream = extract_durations(input_path)
    values = list(stream)

    assert values == ["100", "5000"]
    assert stream.processed_count == 2
    assert stream.skipped_count == 1
    assert stream.malformed_count == 1


def test_process_csv_writes_normalized_values(tmp_path: Path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    write_csv(input_path, ["1s", "250ms"])

    stats = process_csv(input_path, output_path, "utf-8")

    with output_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        assert list(reader) == [["1000"], ["250"]]

    assert stats.processed == 2
    assert stats.skipped == 0
    assert stats.malformed == 0


def test_extract_durations_raises_for_missing_header(tmp_path: Path):
    input_path = tmp_path / "input.csv"
    input_path.write_text("no_header\n1s\n", encoding="utf-8")

    stream = extract_durations(input_path)

    with pytest.raises(DurationExtractionError):
        list(stream)
