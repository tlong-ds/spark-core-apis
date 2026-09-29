# Spark Storage Benchmark

This project is a small PySpark benchmark for comparing storage formats and
Parquet partitioning strategies using the Kaggle
[`usdot/flight-delays`](https://www.kaggle.com/datasets/usdot/flight-delays)
dataset.

The benchmark answers two questions:

1. How do CSV, JSON, Parquet, and ORC compare for write time, storage size,
   and reading data for an aggregation query?
2. How do `repartition(20)`, `coalesce(2)`, and
   `partitionBy("YEAR", "MONTH")` affect Parquet output?

The main program is [`format_benchmark.py`](format_benchmark.py). It is
designed for local execution or Google Colab.

## Repository contents

```text
spark-storage-benchmark/
├── format_benchmark.py          # Main PySpark benchmark
├── sample.py                    # Creates a small 1 MiB input sample
├── requirements.txt             # Python dependency versions
├── data/
│   ├── raw/                     # Put flights.csv here; ignored by Git
│   └── generated/               # Spark output datasets; ignored by Git
├── results/                     # Checked-in benchmark results
├── logs/                        # Runtime logs; ignored by Git
└── Report/
    ├── REPORT.md                # Written analysis of the recorded run
    └── Report_format_benchmark.pdf
```

The raw Kaggle dataset and generated Spark directories are intentionally not
committed because they are large. The empty `.gitkeep` files preserve the
expected directory structure.

## What the benchmark does

`format_benchmark.py` runs the following workflow:

1. Reads a CSV file with a header and inferred schema.
2. Optionally samples the input with a fixed seed (`42`).
3. Casts `ARRIVAL_DELAY` to `double`.
4. Writes the DataFrame as CSV, JSON, Parquet, and ORC.
5. Measures write time, output size, data-file count, and the time to read the
   data and execute this aggregation:

   ```python
   df.select("AIRLINE", "ARRIVAL_DELAY") \
     .groupBy("AIRLINE") \
     .avg("ARRIVAL_DELAY")
   ```

6. Writes the same data as Parquet using three partitioning strategies:

   - `repartition(20)`
   - `coalesce(2)`
   - `partitionBy("YEAR", "MONTH")`

7. Saves result files and writes progress to `logs/benchmark.log`.

The script expects the source columns `AIRLINE`, `ARRIVAL_DELAY`, `YEAR`, and
`MONTH`. These names are defined near the top of the script as
`CARRIER_COL`, `DELAY_COL`, and `PARTITION_COLS`. Change those constants when
using a different dataset.

## Requirements

- Python 3.9 or newer
- Java 8 or newer for Spark
- PySpark 3.5.1, installed from `requirements.txt`
- The Kaggle `flights.csv` file, or another CSV with the expected columns

Install the Python dependency in a virtual environment:

```bash
python -m venv .venv
```

Activate it on Linux/macOS:

```bash
source .venv/bin/activate
```

Activate it in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Then install dependencies:

```bash
pip install -r requirements.txt
```

## Prepare the input data

Download `flights.csv` from the Kaggle dataset and place it here:

```text
data/raw/flights.csv
```

For a quick, smaller test input, run:

```bash
python sample.py
```

This creates `data/raw/flights_1mb.csv`. It copies complete rows from the
beginning of `flights.csv` until the file is at least 1 MiB; it does not run
the benchmark itself.

## Run the benchmark

From the repository root, use explicit paths so the outputs are written to
this checkout:

```bash
python format_benchmark.py \
  --input data/raw/flights.csv \
  --data-dir data/generated \
  --results-dir results \
  --logs-dir logs
```

The equivalent one-line command is:

```bash
python format_benchmark.py --input data/raw/flights.csv --data-dir data/generated --results-dir results --logs-dir logs
```

For a faster test using the sample file:

```bash
python format_benchmark.py --input data/raw/flights_1mb.csv --data-dir data/generated --results-dir results --logs-dir logs
```

The script uses Spark in local mode with `local[2]` and sets
`spark.sql.shuffle.partitions` to `8`.

### Command-line options

| Option | Default in the script | Purpose |
|---|---|---|
| `--input` | A legacy nested path | Source CSV file |
| `--data-dir` | A legacy nested path | Generated CSV/JSON/Parquet/ORC directories |
| `--results-dir` | A legacy nested path | Benchmark result files |
| `--logs-dir` | A legacy nested path | Log directory |
| `--sample-fraction` | `1.0` | Fraction of rows to sample, in `(0, 1]` |
| `--clean-generated` | Off | Delete generated datasets after the run |

The script currently contains legacy nested defaults for the four directory
options. Passing the paths explicitly, as shown above, avoids writing to an
unexpected location. The sample-fraction option is useful for iteration, for
example:

```bash
python format_benchmark.py --input data/raw/flights.csv --sample-fraction 0.1 --data-dir data/generated --results-dir results --logs-dir logs
```

## Output files

After a successful run, the results directory contains:

- `benchmark_results.csv`: one row per format with write time, size, file
  count, and read-query time.
- `benchmark_results.json`: the same format results plus run metadata such as
  row count, input size, Spark version, Python version, and timestamp.
- `partition_results.csv`: one row per Parquet partitioning strategy.
- `file_layout.txt`: a readable listing of the partition experiment output.

The generated datasets are written below `data/generated/` with names such as
`flights_parquet` and `partition_repartition_20`. Use `--clean-generated` to
remove them after the benchmark completes. Logs are written to
`logs/benchmark.log`; both generated data and logs are ignored by Git.

## Windows setup

On Windows, Spark may need Hadoop's native filesystem helpers when writing to
the local filesystem. If the run fails with
`UnsatisfiedLinkError: NativeIO$Windows.access0`, install a compatible
`winutils.exe` and `hadoop.dll`, set `HADOOP_HOME` to the Hadoop directory,
and ensure the relevant `bin` directory is on `PATH`. The script emits a
warning when it detects Windows without `HADOOP_HOME`, but it does not install
these files automatically.

## Recorded results

The checked-in files under `results/` and the analysis in
[`Report/REPORT.md`](Report/REPORT.md) come from one full-dataset run. The
recorded run processed 5,819,079 rows from a 592.4 MB input file.

| Format | Output size | Write time | Read + query time |
|---|---:|---:|---:|
| CSV | 605.8 MB | 9.39 s | 14.26 s |
| JSON | 2,736.4 MB | 17.05 s | 29.01 s |
| Parquet | 142.6 MB | 8.75 s | 0.82 s |
| ORC | 177.7 MB | 10.13 s | 1.70 s |

In this run, Parquet had the smallest output and fastest read-query time. The
results are a single local benchmark snapshot; timings depend on hardware,
Spark, Java, Python, and input-file location.

## Google Colab

The same workflow can run in Colab after cloning the repository, installing
the requirements, uploading or downloading `flights.csv` into `data/raw/`, and
running the explicit-path command above. Kaggle downloads require a configured
Kaggle API credential or manual upload through the Colab file browser.

## Reproducibility notes

- Spark writes directories containing `part-*` files and `_SUCCESS` markers,
  rather than one conventional output file.
- Output-size measurements include all files under each output directory.
- Data-file counts exclude `_SUCCESS` and `.crc` marker files.
- `partitionBy("YEAR", "MONTH")` creates Hive-style directories such as
  `YEAR=2015/MONTH=1`.
- The benchmark measures a write operation and one aggregation query; it does
  not measure filtered reads that take advantage of year/month partition
  pruning.
