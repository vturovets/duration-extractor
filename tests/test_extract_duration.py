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
    process_directory,
    summarize_csv,
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


def write_csv_with_mixed_columns(path: Path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Date", "Duration", "Other"])
        for date_value, duration, other in rows:
            writer.writerow([date_value, duration, other])


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
    assert rows[1] == ["2025-10-27", "3", "3000.00", "Morning", "1.50"]
    assert rows[2] == ["2025-10-28", "3", "1000.00", "Afternoon", "0.30"]

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


def test_summarize_csv_aggregates_by_dominant_date_and_time(tmp_path: Path):
    input_path = tmp_path / "summary.csv"
    write_csv_with_mixed_columns(
        input_path,
        [
            ("2025-10-27T06:00:00Z", "100ms", "a"),
            ("2025-10-27T07:30:00Z", "1s", "b"),
            ("2025-10-27T18:00:00Z", "750ms", "c"),
            ("2025-10-28T07:00:00Z", "200ms", "d"),
            ("2025-10-27T08:00:00Z", "", "e"),
        ],
    )

    summary = summarize_csv(input_path, encoding="utf-8")

    assert summary.date == "2025-10-27"
    assert summary.observations == 4
    assert summary.percentile_95 == 1000.0
    assert summary.time_of_day == "Morning"
    assert summary.intensity is not None
    assert summary.intensity == pytest.approx(summary.observations / 90000)


def test_process_directory_returns_ordered_records(tmp_path: Path):
    batch_dir = tmp_path
    file_b = batch_dir / "beta.csv"
    file_a = batch_dir / "alpha.csv"

    write_csv_with_dates(
        file_b,
        [
            ("2025-10-28T15:00:00Z", "500ms"),
            ("2025-10-28T15:00:05Z", "250ms"),
            ("2025-10-28T15:00:10Z", "1s"),
        ],
    )
    write_csv_with_dates(
        file_a,
        [
            ("2025-10-27T09:00:00Z", "100ms"),
            ("2025-10-27T09:00:01Z", "200ms"),
            ("2025-10-27T09:00:02Z", "3s"),
        ],
    )

    summary_path = batch_dir / "summary.csv"
    records = process_directory(
        batch_dir, summary_output=summary_path, encoding="utf-8"
    )

    assert [record["filename"] for record in records] == ["alpha.csv", "beta.csv"]
    alpha, beta = records

    assert alpha["date"] == "2025-10-27"
    assert alpha["observations"] == 3
    assert alpha["percentile_95"] == pytest.approx(3000.0)
    assert alpha["time_of_day"] == "Morning"
    assert alpha["intensity"] == pytest.approx(1.5)

    assert beta["date"] == "2025-10-28"
    assert beta["observations"] == 3
    assert beta["percentile_95"] == pytest.approx(1000.0)
    assert beta["time_of_day"] == "Afternoon"
    assert beta["intensity"] == pytest.approx(0.3)

    with summary_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    assert rows[1] == ["2025-10-27", "3", "3000.00", "Morning", "1.50"]


def test_process_directory_writes_summary_for_multiple_files(tmp_path: Path):
    batch_dir = tmp_path / "batch"
    batch_dir.mkdir()

    file_alpha = batch_dir / "alpha.csv"
    file_beta = batch_dir / "beta.csv"
    file_gamma = batch_dir / "gamma.csv"

    write_csv_with_dates(
        file_alpha,
        [
            ("2025-01-01T09:00:00Z", "100ms"),
            ("2025-01-01T09:00:10Z", "400ms"),
        ],
    )
    write_csv_with_dates(
        file_beta,
        [
            ("2025-01-02T14:00:00Z", "1s"),
            ("2025-01-02T14:00:20Z", "3s"),
        ],
    )
    write_csv_with_dates(
        file_gamma,
        [
            ("2025-01-03T20:00:00Z", "250ms"),
            ("2025-01-03T20:02:00Z", "750ms"),
            ("2025-01-03T20:04:00Z", "1250ms"),
        ],
    )

    summary_path = batch_dir / "summary.csv"
    records = process_directory(
        batch_dir, summary_output=summary_path, encoding="utf-8"
    )

    assert [record["filename"] for record in records] == [
        "alpha.csv",
        "beta.csv",
        "gamma.csv",
    ]

    alpha, beta, gamma = records
    assert alpha["date"] == "2025-01-01"
    assert alpha["observations"] == 2
    assert alpha["percentile_95"] == pytest.approx(400.0)
    assert alpha["time_of_day"] == "Morning"
    assert alpha["intensity"] == pytest.approx(0.2)

    assert beta["date"] == "2025-01-02"
    assert beta["observations"] == 2
    assert beta["percentile_95"] == pytest.approx(3000.0)
    assert beta["time_of_day"] == "Afternoon"
    assert beta["intensity"] == pytest.approx(0.1)

    assert gamma["date"] == "2025-01-03"
    assert gamma["observations"] == 3
    assert gamma["percentile_95"] == pytest.approx(1250.0)
    assert gamma["time_of_day"] == "Evening"
    assert gamma["intensity"] == pytest.approx(0.0125)

    with summary_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    assert rows == [
        ["Date", "n", "P95", "Time of Day", "Intensity"],
        ["2025-01-01", "2", "400.00", "Morning", "0.20"],
        ["2025-01-02", "2", "3000.00", "Afternoon", "0.10"],
        ["2025-01-03", "3", "1250.00", "Evening", "0.01"],
    ]


@pytest.mark.parametrize(
    "header,row,expected_message",
    [
        (
            ["Date", "Other"],
            ["2025-01-01T00:00:00Z", "value"],
            "'Duration'",
        ),
        (
            ["Duration", "Other"],
            ["100ms", "value"],
            "'Date'",
        ),
    ],
)
def test_summarize_csv_raises_for_missing_required_columns(
    tmp_path: Path, header, row, expected_message
):
    input_path = tmp_path / "missing.csv"
    with input_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerow(row)

    with pytest.raises(DurationExtractionError) as excinfo:
        summarize_csv(input_path, encoding="utf-8")

    assert expected_message in str(excinfo.value)


def test_summarize_csv_handles_empty_dataset(tmp_path: Path):
    input_path = tmp_path / "empty.csv"
    with input_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Date", "Duration"])

    summary = summarize_csv(input_path, encoding="utf-8")

    assert summary.date == ""
    assert summary.observations == 0
    assert summary.percentile_95 is None
    assert summary.time_of_day == ""
    assert summary.intensity is None


def test_main_writes_custom_summary_output(tmp_path: Path, capsys):
    batch_dir = tmp_path / "inputs"
    batch_dir.mkdir()

    write_csv_with_dates(
        batch_dir / "first.csv",
        [
            ("2025-05-01T08:00:00Z", "100ms"),
            ("2025-05-01T08:00:05Z", "200ms"),
        ],
    )
    write_csv_with_dates(
        batch_dir / "second.csv",
        [
            ("2025-05-02T13:00:00Z", "1s"),
            ("2025-05-02T13:00:30Z", "500ms"),
        ],
    )

    summary_output = tmp_path / "reports" / "custom_summary.csv"

    exit_code = main(
        [
            "--batch-dir",
            str(batch_dir),
            "--summary-output",
            str(summary_output),
        ]
    )

    assert exit_code == 0
    assert summary_output.exists()

    with summary_output.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    assert rows[0] == ["Date", "n", "P95", "Time of Day", "Intensity"]
    assert len(rows) == 3

    captured = capsys.readouterr()
    assert str(summary_output) in captured.out

    first_output = batch_dir / "durations_first.csv"
    second_output = batch_dir / "durations_second.csv"
    assert first_output.exists()
    assert second_output.exists()
