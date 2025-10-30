# Duration Extractor

Duration Extractor is a lightweight command-line tool for normalizing duration
samples stored in CSV files. It can operate on a single input file or scan an
entire directory of CSVs and produce a summary report with aggregate
statistics.

## Features

- Normalizes duration values expressed in microseconds, milliseconds, or
  seconds into milliseconds.
- Writes extracted duration values to a new one-column CSV per input file.
- Computes a per-file summary during batch runs, including:
  - Observation count (`n`).
  - 95th percentile of the normalized durations (`P95`).
  - Predominant time-of-day bucket (`Morning`, `Afternoon`, `Evening`).
  - Request intensity (observations per second based on the first and last
    timestamps).
- Provides informative logging and robust validation for malformed data.

## Installation

The script targets Python 3.9+ and only depends on the standard library. Clone
this repository and install any development tools you prefer (e.g., `pipx`,
`uv`, or a virtual environment) to run the utility and its tests.

```bash
git clone <repository-url>
cd duration-extractor
python -m venv .venv
source .venv/bin/activate
```

## Usage

Run `extract_duration.py` directly with Python. The CLI exposes both
single-file and batch-processing workflows.

### Single CSV file

```bash
python extract_duration.py path/to/input.csv [path/to/output.csv]
```

- If `output.csv` is omitted, the tool writes to a sibling file prefixed with
  `durations_`.
- The resulting CSV contains one normalized duration per row expressed in
  milliseconds.

### Batch directory mode

```bash
python extract_duration.py --batch-dir path/to/folder [--summary-output path/to/summary.csv]
```

- Every `.csv` file inside the directory is processed individually.
- Per-file duration data is written to `durations_<original-name>.csv`.
- A summary table (default `summary.csv` inside the directory) captures the
  date, observation count, 95th percentile, dominant time of day, and intensity
  for each source file.
- Use `--summary-output` to change where the summary CSV is saved.
- Provide `--encoding` if your data uses a character set other than UTF-8.

### Exit codes

- `0` – Success.
- `1` – I/O, CSV parsing, or data validation failure.
- `2` – Argument validation error.

## CSV requirements

Input CSV files must contain the following columns:

- `Duration` – Duration values with explicit units (`ms`, `µs`/`us`, or `s`).
- `Date` – ISO-8601 timestamps (e.g., `2024-02-01T09:30:00Z`) for batch
  summaries.

Rows lacking either column, blank duration values, or unparseable timestamps
are skipped when computing statistics.

## Development

### Running the automated tests

```bash
python -m pytest
```

The test suite covers duration normalization, CSV validation, and the batch
processing workflow.

### Logging

By default, the CLI configures logging to `INFO` level and writes messages to
stderr. Adjust the global logging configuration in `extract_duration.py` if you
need more verbose output during development.

## Sample data

Example inputs and outputs are provided in `docs/` to help you explore the
batch-processing feature.

## License

This project is distributed under the terms specified in the repository. See
`LICENSE` (if present) or contact the maintainers for details.
