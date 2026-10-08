"""Run Task 1 and the updated Task 2 in one Spark application."""

import argparse
import json
import os
import sys
import time
from pathlib import Path

# Keep the two original task modules unchanged and import Task 2 from its
# existing directory. --py-files in submit_job.sh also distributes it.
PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR / "spark-storage-benchmark"))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import format_benchmark as benchmark
from helper import load_ip_country_map
from rdd_processing import execute_rdd_job, write_result_output


def parse_args():
    parser = argparse.ArgumentParser(
        description="Task 3: run logistics RDD processing and the updated file benchmark"
    )
    parser.add_argument("--input-path", default="./data/raw_logs.txt")
    parser.add_argument("--lookup-path", default="./data/ip_geolocation.csv")
    parser.add_argument("--output-path", default="./data/output")
    parser.add_argument(
        "--format-input",
        default="./spark-storage-benchmark/data/raw/flights.csv",
    )
    parser.add_argument(
        "--format-data-dir",
        default="./spark-storage-benchmark/data/generated",
    )
    parser.add_argument(
        "--format-results-dir",
        default="./spark-storage-benchmark/results",
    )
    parser.add_argument(
        "--format-logs-dir",
        default="./spark-storage-benchmark/logs",
    )
    parser.add_argument(
        "--format-config",
        default="./spark-storage-benchmark/config/benchmark_config.json",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=None,
        help="Override Task 2 JSON sample fraction; otherwise use the JSON value",
    )
    parser.add_argument("--clean-generated", action="store_true")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Wait for Enter after both tasks, so the Spark UI stays available",
    )
    return parser.parse_args()


def run_updated_task2(spark, args):
    """Reuse Task 2's benchmark functions without calling its standalone main().

    The standalone main() creates and stops its own SparkSession. Here the
    Task 3 SparkSession must survive through the classroom UI demonstration.
    """
    benchmark.setup_logging(args.format_logs_dir)
    config, config_source = benchmark.load_config(args.format_config)
    if args.sample_fraction is not None:
        config["benchmark"]["sample_fraction"] = args.sample_fraction
    sample_fraction = float(config["benchmark"]["sample_fraction"])
    if not 0 < sample_fraction <= 1:
        raise ValueError("benchmark.sample_fraction must be in (0, 1]")

    # Report the actual submit configuration, without editing Task 2's JSON.
    config["spark"]["master"] = spark.sparkContext.master
    config["spark"]["shuffle_partitions"] = int(
        spark.conf.get("spark.sql.shuffle.partitions")
    )
    config["spark"]["adaptive_enabled"] = (
        spark.conf.get("spark.sql.adaptive.enabled").lower() == "true"
    )

    os.makedirs(args.format_results_dir, exist_ok=True)
    benchmark.clean_dir(os.path.join(args.format_results_dir, "plans"))
    benchmark.warn_if_windows_without_hadoop()

    try:
        source_df = benchmark.load_source_data(
            spark,
            args.format_input,
            sample_fraction,
            config["benchmark"]["random_seed"],
        )
        spark.catalog.clearCache()
        row_count = source_df.count()
        benchmark.logger.info("Source row count: %d", row_count)
        generated_dir = args.format_data_dir
        os.makedirs(generated_dir, exist_ok=True)

        representation_cells = []
        format_write_rows = []
        for fmt in config["representation"]["formats"]:
            if fmt not in benchmark.FORMATS:
                raise ValueError(f"Unsupported representation format: {fmt}")
            path = os.path.join(generated_dir, f"representation_{fmt}")
            spark.catalog.clearCache()
            write_time = benchmark.write_dataset(
                source_df, path, fmt, config["compression"][fmt]
            )
            storage = benchmark.storage_metadata(path)
            format_write_rows.append(
                {"format": fmt, "layout": "baseline", "write_time_sec": write_time, **storage}
            )
            for workload_name in config["representation"]["workloads"]:
                if workload_name not in benchmark.WORKLOADS:
                    raise ValueError(f"Unsupported workload: {workload_name}")
                representation_cells.append(
                    {
                        "format": fmt,
                        "layout": "baseline",
                        "workload": workload_name,
                        "path": path,
                        "storage": storage,
                    }
                )

        layout_cells = []
        layout_write_rows = []
        layout_dir = os.path.join(generated_dir, "layout")
        layout_specs = [
            (
                "baseline",
                lambda df: df.repartition(int(config["layout"]["baseline_partitions"])),
            ),
            (
                "many_files_repartition",
                lambda df: df.repartition(int(config["layout"]["many_files_partitions"])),
            ),
            (
                "few_files_coalesce",
                lambda df: df.coalesce(int(config["layout"]["few_files_partitions"])),
            ),
        ]
        for layout_name, transform in layout_specs:
            path = os.path.join(layout_dir, layout_name)
            spark.catalog.clearCache()
            write_time = benchmark.write_dataset(
                source_df,
                path,
                "parquet",
                config["compression"]["parquet"],
                transform=transform,
            )
            storage = benchmark.storage_metadata(path)
            layout_write_rows.append(
                {
                    "format": "parquet",
                    "layout": layout_name,
                    "write_time_sec": write_time,
                    **storage,
                }
            )
            layout_cells.append(
                {
                    "format": "parquet",
                    "layout": layout_name,
                    "workload": "W1",
                    "path": path,
                    "storage": storage,
                }
            )

        partitioned_path = os.path.join(
            generated_dir, "partitioned_parquet_year_month"
        )
        spark.catalog.clearCache()
        partitioned_write_time = benchmark.write_dataset(
            source_df,
            partitioned_path,
            "parquet",
            config["compression"]["parquet"],
            partition_by=benchmark.PARTITION_COLS,
        )
        partitioned_storage = benchmark.storage_metadata(partitioned_path)

        representation_raw = benchmark.run_cells(spark, representation_cells, config)
        layout_raw = benchmark.run_cells(spark, layout_cells, config)
        representation_summary = benchmark.summarize_rows(representation_raw)
        layout_summary = benchmark.summarize_rows(layout_raw)

        evidence_rows = []
        partitioned_df = benchmark.reader_for(spark, "parquet", partitioned_path)
        spark.catalog.clearCache()
        pruning_start = time.perf_counter()
        pruning_result = benchmark.workload(partitioned_df, "W2", config).collect()
        pruning_runtime = time.perf_counter() - pruning_start
        benchmark.save_plan(
            args.format_results_dir,
            "partition_pruning_w2",
            benchmark.workload(partitioned_df, "W2", config),
            "partition_pruning",
            "parquet",
            "partitioned_year_month",
            "W2",
            evidence_rows,
        )
        parquet_df = benchmark.reader_for(
            spark, "parquet", os.path.join(generated_dir, "representation_parquet")
        )
        threshold = config["workloads"]["W2_pushdown"]["arrival_delay_gt"]
        pushdown_query = (
            parquet_df.filter(F.col(benchmark.ARRIVAL_DELAY_COL) > threshold)
            .select(benchmark.ARRIVAL_DELAY_COL)
            .agg(F.count(F.lit(1)))
        )
        benchmark.save_plan(
            args.format_results_dir,
            "predicate_pushdown_arrival_delay",
            pushdown_query,
            "predicate_pushdown",
            "parquet",
            "baseline",
            "W2_pushdown",
            evidence_rows,
        )
        benchmark.save_plan(
            args.format_results_dir,
            "column_pruning_w3",
            benchmark.workload(parquet_df, "W3", config),
            "column_pruning",
            "parquet",
            "baseline",
            "W3",
            evidence_rows,
        )

        raw_fields = [
            "format",
            "layout",
            "workload",
            "run_number",
            "execution_order",
            "runtime_sec",
            "result_rows",
            "path",
            "num_data_files",
            "output_size_bytes",
        ]
        benchmark.write_csv(
            os.path.join(args.format_results_dir, "representation_raw.csv"),
            representation_raw,
            raw_fields,
        )
        benchmark.write_csv(
            os.path.join(args.format_results_dir, "representation_summary.csv"),
            representation_summary,
            list(representation_summary[0].keys()),
        )
        benchmark.write_csv(
            os.path.join(args.format_results_dir, "layout_raw.csv"),
            layout_raw,
            raw_fields,
        )
        benchmark.write_csv(
            os.path.join(args.format_results_dir, "layout_summary.csv"),
            layout_summary,
            list(layout_summary[0].keys()),
        )
        benchmark.write_csv(
            os.path.join(args.format_results_dir, "plan_evidence.csv"),
            evidence_rows,
            [
                "experiment",
                "format",
                "layout",
                "workload",
                "partition_filters",
                "pushed_filters",
                "read_schema",
                "plan_file",
            ],
        )

        execution_order = {
            "representation_raw": [
                {
                    key: row[key]
                    for key in (
                        "format",
                        "layout",
                        "workload",
                        "run_number",
                        "execution_order",
                    )
                }
                for row in representation_raw
            ],
            "layout_raw": [
                {
                    key: row[key]
                    for key in (
                        "format",
                        "layout",
                        "workload",
                        "run_number",
                        "execution_order",
                    )
                }
                for row in layout_raw
            ],
        }
        metadata = benchmark.collect_metadata(
            spark,
            args.format_input,
            generated_dir,
            config,
            config_source,
            row_count,
            execution_order,
        )
        metadata["generated_datasets"] = {
            "representation": format_write_rows,
            "layout": layout_write_rows,
            "partitioned_parquet": {
                "write_time_sec": partitioned_write_time,
                **partitioned_storage,
            },
        }
        metadata["partition_pruning_probe"] = {
            "workload": "W2",
            "year": config["workloads"]["W2"]["year"],
            "month": config["workloads"]["W2"]["month"],
            "runtime_sec": pruning_runtime,
            "result_rows": len(pruning_result),
        }
        metadata["task3_integration"] = {
            "shared_spark_session": True,
            "source_config_unmodified": True,
            "spark_settings_from_submit": True,
            "sample_fraction_overridden": args.sample_fraction is not None,
        }
        with open(
            os.path.join(args.format_results_dir, "benchmark_metadata.json"),
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(metadata, handle, indent=2)
        benchmark.logger.info(
            "Benchmark finished successfully. Results written to %s",
            args.format_results_dir,
        )
        return metadata
    finally:
        if args.clean_generated:
            benchmark.clean_dir(args.format_data_dir)


def main():
    args = parse_args()
    for label, path in (
        ("Task 1 raw logs", args.input_path),
        ("Task 1 lookup", args.lookup_path),
        ("Task 2 flights.csv", args.format_input),
        ("Task 2 config", args.format_config),
    ):
        if not os.path.isfile(path):
            raise FileNotFoundError(f"{label} is missing: {path}")

    # spark-submit supplies master, name, memory and SQL settings.
    spark = (
        SparkSession.builder.appName("LowLevel_FileFormat_Job")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        print("\n" + "=" * 70)
        print(" [TASK 1] PROCESSING LOGS (RDD + BROADCAST CIDR + ACCUMULATOR)")
        print("=" * 70)
        ip_country_map = load_ip_country_map(args.lookup_path)
        metrics = execute_rdd_job(
            spark.sparkContext, spark, args.input_path, ip_country_map
        )
        write_result_output(
            spark.sparkContext,
            spark,
            args.output_path,
            "rdd",
            args.input_path,
            ip_country_map,
        )
        print(f"-> Execution Latency : {metrics['time']:.2f}s")
        print(f"-> Semantic Counts    : {metrics['semantic_counts']}")
        print(
            "-> Accumulator Telemetry (not correctness): "
            f"{metrics['telemetry_invalid_accumulator']:,d}"
        )

        print("\n" + "=" * 70)
        print(" [TASK 2] FORMAT, LAYOUT & QUERY BENCHMARK (W1/W2/W3)")
        print("=" * 70)
        run_updated_task2(spark, args)
        print(f"-> Benchmark metrics saved to: {args.format_results_dir}")

        ui_url = spark.sparkContext.uiWebUrl or "(Spark UI unavailable)"
        print("\n" + "=" * 70)
        print(f" SPARK WEB UI: {ui_url}")
        print(" Inspect Jobs, Stages, SQL / DataFrame and benchmark plans")
        print("=" * 70)
        if args.interactive:
            input("\n[LIVE DEMO] Press [Enter] to terminate Spark UI...")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
