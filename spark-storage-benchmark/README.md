# Spark Storage Benchmark

This project measures how storage representation, output layout, and query
shape affect Spark reads for the Kaggle `usdot/flight-delays` `flights.csv`
dataset. The primary implementation is [format_benchmark.py](format_benchmark.py).

## Run

Install the dependency and place the source file at `data/raw/flights.csv`:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python format_benchmark.py --config config/benchmark_config.json
```

Useful overrides are `--input`, `--data-dir`, `--results-dir`, and
`--logs-dir`. When `--config` is omitted, the script uses its Python defaults.
The JSON file is intentionally small and controls run counts, sampling, Spark
settings, compression, workload filters, and layout partition counts.

On Windows, local Spark writes may require Hadoop's `winutils.exe` and
`hadoop.dll`. Set `HADOOP_HOME` if Spark reports
`UnsatisfiedLinkError: NativeIO$Windows.access0`.

## Workloads and experiments

The raw Kaggle CSV is parsed with an explicit full-column `StructType`, then
the selected six-column logical dataset is written and read in every format
with one explicit benchmark `StructType`. No measured read uses schema
inference.

- **W1, broad aggregation:** average arrival delay, average departure delay,
  average distance, and row count grouped by `AIRLINE`.
- **W2, selective aggregation:** W1 after filtering the configured `YEAR` and
  `MONTH`.
- **W3, narrow projection:** only `AIRLINE` and `ARRIVAL_DELAY`, with count,
  minimum and maximum carrier, and average arrival delay.

The representation experiment runs all four formats against a fixed baseline
layout and all three workloads. The layout experiment uses Parquet and W1 to
compare a baseline, `repartition(N)` for many output files, and `coalesce(N)`
for few output files. Its values come from `layout` in the config.

A separate Parquet dataset is written with `.partitionBy("YEAR", "MONTH")`
for the W2 partition-pruning experiment. It is not combined with the
small-file layout experiment.

## Timing and evidence

Every benchmark cell has one warm-up and five measured actions. The warm-up is
discarded. Each measured runtime includes a real Spark action (`collect()`),
while DataFrame construction and `explain()` are not used as benchmark times.
Spark catalog cache is cleared before every run and benchmark DataFrames are
never persisted. Filesystem/OS caching may still affect sequential runs and is
not flushed.

Cells are shuffled deterministically for each round using `random_seed`; the
actual measured execution order is recorded in the raw CSV files and metadata.

The plan files under `results/plans/` come from `df.explain("formatted")` for:

- W2 on partitioned Parquet, to inspect `PartitionFilters`;
- a selective `ARRIVAL_DELAY` predicate, to inspect `PushedFilters`;
- W3 on baseline Parquet, to inspect `ReadSchema` for the two required columns.

`results/plan_evidence.csv` records the fields found verbatim in those plans,
or `ABSENT` when a field is not present. A `partitionBy()` call alone is not
treated as proof of pruning or pushdown.

## Results

The script writes:

```text
results/
├── representation_raw.csv
├── representation_summary.csv
├── layout_raw.csv
├── layout_summary.csv
├── plan_evidence.csv
├── benchmark_metadata.json
└── plans/
```

Raw files contain each measured runtime, run number, execution order, result
row count, layout, and storage metadata. Summary files report median, minimum,
maximum, standard deviation, and measured-run count. The metadata records the
effective configuration, schema, Spark/Python settings, row count, compression,
cache policy, platform information, and generated storage statistics.

For a quick input sample, run `python sample.py` after placing the full
`flights.csv` in `data/raw/`, then pass the generated sample path with
`--input`. For a small smoke test, make a temporary JSON config based on
`config/benchmark_config.json` with a low `sample_fraction`, one measured run,
and smaller partition counts. The production configuration is intentionally
set to five measured runs as required by the assignment.
