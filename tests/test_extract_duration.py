import csv
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from extract_duration import (
    DurationExtractionError,
    extract_durations,
    main,
    process_csv,
)


def write_csv(path: Path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Duration"])
        writer.writerows([[value] for value in rows])


def write_csv_with_dates(path: Path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Date", "Duration"])
        for date_value, duration in rows:
            writer.writerow([date_value, duration])


def test_extract_durations_converts_units_and_tracks_counts(tmp_path: Path):
    input_path = tmp_path / "input.csv"
    write_csv(
        input_path,
        ["100ms", "5s", "1721.39μs", "", "invalid"],
    )

    stream = extract_durations(input_path)
    values = list(stream)

    assert values == ["100", "5000", "1.72139"]
    assert stream.processed_count == 3
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


def test_extract_durations_supports_microseconds(tmp_path: Path):
    input_path = tmp_path / "input.csv"
    write_csv(input_path, ["1µs", "2μs", "3us"])

    stream = extract_durations(input_path)
    values = list(stream)

    assert values == ["0.001", "0.002", "0.003"]
    assert stream.processed_count == 3
    assert stream.skipped_count == 0
    assert stream.malformed_count == 0


def test_extract_durations_raises_for_missing_header(tmp_path: Path):
    input_path = tmp_path / "input.csv"
    input_path.write_text("no_header\n1s\n", encoding="utf-8")

    stream = extract_durations(input_path)

    with pytest.raises(DurationExtractionError):
        list(stream)


def test_main_defaults_output_path_when_missing(tmp_path: Path, capsys):
    input_path = tmp_path / "input.csv"
    write_csv(input_path, ["1s"])

    exit_code = main([str(input_path)])

    assert exit_code == 0

    expected_output = tmp_path / "durations_input.csv"
    with expected_output.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        assert list(reader) == [["1000"]]

    captured = capsys.readouterr()
    assert f"Successfully processed '{input_path}' into '{expected_output}'." in captured.out


def test_main_processes_directory_and_writes_summary(tmp_path: Path, capsys):
    batch_dir = tmp_path
    file_a = batch_dir / "alpha.csv"
    file_b = batch_dir / "beta.csv"

    write_csv_with_dates(
        file_a,
        [
            ("2025-10-27T09:00:00Z", "100ms"),
            ("2025-10-27T09:00:01Z", "200ms"),
            ("2025-10-27T09:00:02Z", "3s"),
        ],
    )
    write_csv_with_dates(
        file_b,
        [
            ("2025-10-28T15:00:00Z", "500ms"),
            ("2025-10-28T15:00:05Z", "250ms"),
            ("2025-10-28T15:00:10Z", "1s"),
        ],
    )

    exit_code = main(["--batch-dir", str(batch_dir)])

    assert exit_code == 0

    summary_path = batch_dir / "summary.csv"
    with summary_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    assert rows[0] == ["Date", "n", "P95", "Time of Day", "Intensity"]
    assert rows[1] == ["2025-10-27", "3", "3000", "Morning", "1.5"]
    assert rows[2] == ["2025-10-28", "3", "1000", "Afternoon", "0.3"]

    output_a = batch_dir / "durations_alpha.csv"
    with output_a.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        assert list(reader) == [["100"], ["200"], ["3000"]]

    output_b = batch_dir / "durations_beta.csv"
    with output_b.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        assert list(reader) == [["500"], ["250"], ["1000"]]

    captured = capsys.readouterr()
    assert "summary row(s)" in captured.out
