"""Reproducible Spark storage-format, layout, and read-behavior benchmark."""

import argparse
import csv
import io
import itertools
import json
import logging
import os
import platform
import random
import re
import shutil
import statistics
import sys
import time
from copy import deepcopy
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


CARRIER_COL = "AIRLINE"
ARRIVAL_DELAY_COL = "ARRIVAL_DELAY"
DEPARTURE_DELAY_COL = "DEPARTURE_DELAY"
DISTANCE_COL = "DISTANCE"
PARTITION_COLS = ["YEAR", "MONTH"]
FORMATS = ["csv", "json", "parquet", "orc"]
WORKLOADS = ["W1", "W2", "W3"]

BENCHMARK_SCHEMA = StructType(
    [
        StructField(CARRIER_COL, StringType(), True),
        StructField(ARRIVAL_DELAY_COL, DoubleType(), True),
        StructField(DEPARTURE_DELAY_COL, DoubleType(), True),
        StructField(DISTANCE_COL, DoubleType(), True),
        StructField("YEAR", IntegerType(), True),
        StructField("MONTH", IntegerType(), True),
    ]
)

# The raw Kaggle CSV has these columns in this order. It is parsed explicitly
# before the six-column logical benchmark dataset is selected.
SOURCE_SCHEMA = StructType(
    [
        StructField("YEAR", IntegerType(), True),
        StructField("MONTH", IntegerType(), True),
        StructField("DAY", IntegerType(), True),
        StructField("DAY_OF_WEEK", IntegerType(), True),
        StructField(CARRIER_COL, StringType(), True),
        StructField("FLIGHT_NUMBER", IntegerType(), True),
        StructField("TAIL_NUMBER", StringType(), True),
        StructField("ORIGIN_AIRPORT", StringType(), True),
        StructField("DESTINATION_AIRPORT", StringType(), True),
        StructField("SCHEDULED_DEPARTURE", IntegerType(), True),
        StructField("DEPARTURE_TIME", IntegerType(), True),
        StructField(DEPARTURE_DELAY_COL, DoubleType(), True),
        StructField("TAXI_OUT", DoubleType(), True),
        StructField("WHEELS_OFF", IntegerType(), True),
        StructField("SCHEDULED_TIME", DoubleType(), True),
        StructField("ELAPSED_TIME", DoubleType(), True),
        StructField("AIR_TIME", DoubleType(), True),
        StructField(DISTANCE_COL, DoubleType(), True),
        StructField("WHEELS_ON", IntegerType(), True),
        StructField("TAXI_IN", DoubleType(), True),
        StructField("SCHEDULED_ARRIVAL", IntegerType(), True),
        StructField("ARRIVAL_TIME", IntegerType(), True),
        StructField(ARRIVAL_DELAY_COL, DoubleType(), True),
        StructField("DIVERTED", IntegerType(), True),
        StructField("CANCELLED", IntegerType(), True),
        StructField("CANCELLATION_REASON", StringType(), True),
        StructField("AIR_SYSTEM_DELAY", DoubleType(), True),
        StructField("SECURITY_DELAY", DoubleType(), True),
        StructField("AIRLINE_DELAY", DoubleType(), True),
        StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),
        StructField("WEATHER_DELAY", DoubleType(), True),
    ]
)

DEFAULT_CONFIG = {
    "benchmark": {
        "warmup_runs": 1,
        "measured_runs": 5,
        "random_seed": 42,
        "sample_fraction": 1.0,
    },
    "spark": {
        "master": "local[2]",
        "shuffle_partitions": 8,
        "adaptive_enabled": True,
    },
    "compression": {"csv": "none", "json": "none", "parquet": "snappy", "orc": "zlib"},
    "representation": {"formats": FORMATS, "workloads": WORKLOADS},
    "layout": {"baseline_partitions": 4, "many_files_partitions": 20, "few_files_partitions": 2},
    "workloads": {"W2": {"year": 2015, "month": 1}, "W2_pushdown": {"arrival_delay_gt": 60}},
}

logger = logging.getLogger("format_benchmark")


def setup_logging(logs_dir):
    os.makedirs(logs_dir, exist_ok=True)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(os.path.join(logs_dir, "benchmark.log"), mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def load_config(config_path):
    config = deepcopy(DEFAULT_CONFIG)
    if not config_path:
        return config, "python_defaults"
    with open(config_path, encoding="utf-8") as handle:
        supplied = json.load(handle)
    for section, values in supplied.items():
        if isinstance(values, dict) and isinstance(config.get(section), dict):
            config[section].update(values)
        else:
            config[section] = values
    return config, os.path.abspath(config_path)


def get_dir_size_bytes(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, names in os.walk(path):
        for name in names:
            total += os.path.getsize(os.path.join(root, name))
    return total


def data_files(path):
    files = []
    for root, _, names in os.walk(path):
        for name in names:
            if name.startswith("_") or name.startswith(".") or name.endswith(".crc"):
                continue
            file_path = os.path.join(root, name)
            files.append((file_path, os.path.getsize(file_path)))
    return files


def storage_metadata(path):
    sizes = [size for _, size in data_files(path)]
    return {
        "output_size_bytes": sum(sizes),
        "num_data_files": len(sizes),
        "min_file_size_bytes": min(sizes) if sizes else 0,
        "median_file_size_bytes": statistics.median(sizes) if sizes else 0,
        "max_file_size_bytes": max(sizes) if sizes else 0,
    }


def clean_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)


def warn_if_windows_without_hadoop():
    if platform.system() == "Windows" and not os.environ.get("HADOOP_HOME"):
        logger.warning(
            "Windows is running without HADOOP_HOME. Local Spark writes may require "
            "winutils.exe and hadoop.dll; see README.md if NativeIO access fails."
        )


def load_source_data(spark, input_path, sample_fraction, random_seed):
    logger.info("Reading source dataset with the explicit benchmark schema: %s", input_path)
    raw_df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .schema(SOURCE_SCHEMA)
        .csv(input_path)
    )
    return_df = raw_df.select(*BENCHMARK_SCHEMA.fieldNames())
    if sample_fraction < 1.0:
        return_df = return_df.sample(withReplacement=False, fraction=sample_fraction, seed=random_seed)
    return return_df


def aggregate_by_carrier(df):
    return df.groupBy(CARRIER_COL).agg(
        F.avg(ARRIVAL_DELAY_COL).alias("avg_arrival_delay"),
        F.avg(DEPARTURE_DELAY_COL).alias("avg_departure_delay"),
        F.avg(DISTANCE_COL).alias("avg_distance"),
        F.count(F.lit(1)).alias("row_count"),
    )


def workload(df, workload_name, config):
    if workload_name == "W1":
        selected = df.select(CARRIER_COL, ARRIVAL_DELAY_COL, DEPARTURE_DELAY_COL, DISTANCE_COL)
        return aggregate_by_carrier(selected)
    if workload_name == "W2":
        year = config["workloads"]["W2"]["year"]
        month = config["workloads"]["W2"]["month"]
        filtered = df.filter((F.col("YEAR") == year) & (F.col("MONTH") == month))
        selected = filtered.select(CARRIER_COL, ARRIVAL_DELAY_COL, DEPARTURE_DELAY_COL, DISTANCE_COL)
        return aggregate_by_carrier(selected)
    if workload_name == "W3":
        return df.select(CARRIER_COL, ARRIVAL_DELAY_COL).agg(
            F.count(F.lit(1)).alias("row_count"),
            F.min(CARRIER_COL).alias("min_carrier"),
            F.max(CARRIER_COL).alias("max_carrier"),
            F.avg(ARRIVAL_DELAY_COL).alias("avg_arrival_delay"),
        )
    raise ValueError(f"Unknown workload: {workload_name}")


def reader_for(spark, fmt, path):
    reader = spark.read.schema(BENCHMARK_SCHEMA)
    if fmt == "csv":
        return reader.option("header", True).option("inferSchema", False).csv(path)
    return reader.format(fmt).load(path)


def write_dataset(df, path, fmt, compression, transform=None, partition_by=None):
    clean_dir(path)
    transformed = transform(df) if transform else df
    writer = transformed.write.mode("overwrite").option("compression", compression)
    if fmt == "csv":
        writer = writer.option("header", True)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    start = time.perf_counter()
    writer.format(fmt).save(path)
    return time.perf_counter() - start


def summary_row(rows):
    runtimes = [float(row["runtime_sec"]) for row in rows]
    return {
        "format": rows[0]["format"],
        "layout": rows[0]["layout"],
        "workload": rows[0]["workload"],
        "measured_runs": len(runtimes),
        "median_sec": statistics.median(runtimes),
        "min_sec": min(runtimes),
        "max_sec": max(runtimes),
        "stddev_sec": statistics.stdev(runtimes) if len(runtimes) > 1 else 0.0,
    }


def summarize_rows(raw_rows):
    ordered = sorted(raw_rows, key=lambda row: (row["format"], row["layout"], row["workload"]))
    return [summary_row(list(group)) for _, group in itertools.groupby(ordered, key=lambda row: (row["format"], row["layout"], row["workload"]))]


def run_cells(spark, cells, config):
    """
    Thực thi ma trận các bài test đo thời gian đọc dữ liệu.
    Đã được cập nhật để gán tên Job chi tiết tới từng cell/lượt chạy trên Spark Web UI.
    """
    benchmark_config = config["benchmark"]
    if benchmark_config["warmup_runs"] != 1:
        raise ValueError("This benchmark requires benchmark.warmup_runs to be exactly 1")
    rng = random.Random(benchmark_config["random_seed"])
    rounds = [("warmup", 0)] + [("measured", run_number) for run_number in range(1, benchmark_config["measured_runs"] + 1)]
    result_rows = []
    execution_order = 0
    for phase, run_number in rounds:
        round_cells = list(cells)
        rng.shuffle(round_cells)
        for cell in round_cells:
            fmt = cell["format"].upper()
            layout = cell["layout"]
            workload_name = cell["workload"]
            run_label = "Warmup" if phase == "warmup" else f"Run #{run_number}"

            # Gán mô tả chi tiết cho từng Job trên Spark UI
            job_desc = f"Task 2: [{fmt}] Layout: {layout} | Workload: {workload_name} | {run_label}"
            spark.sparkContext.setJobDescription(job_desc)

            try:
                spark.catalog.clearCache()
                read_df = reader_for(spark, cell["format"], cell["path"])
                start = time.perf_counter()
                result = workload(read_df, cell["workload"], config).collect()
                elapsed = time.perf_counter() - start
                execution_order += 1
                if phase == "measured":
                    result_rows.append(
                        {
                            "format": cell["format"],
                            "layout": cell["layout"],
                            "workload": cell["workload"],
                            "run_number": run_number,
                            "execution_order": execution_order,
                            "runtime_sec": elapsed,
                            "result_rows": len(result),
                            "path": cell["path"],
                            "num_data_files": cell["storage"]["num_data_files"],
                            "output_size_bytes": cell["storage"]["output_size_bytes"],
                        }
                    )
            finally:
                # Reset lại tên job sau khi chạy xong cell này
                spark.sparkContext.setJobDescription(None)

    return result_rows


def capture_formatted_plan(query_df):
    output = io.StringIO()
    previous_stdout = sys.stdout
    try:
        sys.stdout = output
        query_df.explain("formatted")
    finally:
        sys.stdout = previous_stdout
    return output.getvalue().strip()


def plan_fields(plan_text):
    fields = {"partition_filters": "ABSENT", "pushed_filters": "ABSENT", "read_schema": "ABSENT"}
    labels = {"PartitionFilters": "partition_filters", "PushedFilters": "pushed_filters", "ReadSchema": "read_schema"}
    for line in plan_text.splitlines():
        match = re.match(r"^\s*(PartitionFilters|PushedFilters|ReadSchema):\s*(.*)$", line)
        if match:
            fields[labels[match.group(1)]] = match.group(2).strip() or "EMPTY"
    return fields


def save_plan(results_dir, name, query_df, experiment, fmt, layout, workload_name, evidence_rows):
    plan_text = capture_formatted_plan(query_df)
    plans_dir = os.path.join(results_dir, "plans")
    os.makedirs(plans_dir, exist_ok=True)
    plan_path = os.path.join(plans_dir, f"{name}.txt")
    with open(plan_path, "w", encoding="utf-8") as handle:
        handle.write(plan_text + "\n")
    evidence_rows.append(
        {
            "experiment": experiment,
            "format": fmt,
            "layout": layout,
            "workload": workload_name,
            **plan_fields(plan_text),
            "plan_file": os.path.relpath(plan_path, results_dir),
        }
    )


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def collect_metadata(spark, input_path, data_dir, config, config_source, row_count, execution_order):
    input_size = get_dir_size_bytes(input_path) if os.path.exists(input_path) else None
    disk = shutil.disk_usage(os.path.dirname(os.path.abspath(data_dir)))
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "name": "usdot/flight-delays flights.csv",
            "input_path": os.path.abspath(input_path),
            "input_size_bytes": input_size,
            "row_count": row_count,
            "sample_fraction": config["benchmark"]["sample_fraction"],
            "schema": [{"name": field.name, "type": field.dataType.simpleString()} for field in BENCHMARK_SCHEMA.fields],
        },
        "spark": {
            "version": spark.version,
            "master": spark.sparkContext.master,
            "configured_master": config["spark"]["master"],
            "default_parallelism": spark.sparkContext.defaultParallelism,
            "available_system_cores": os.cpu_count(),
            "spark.sql.shuffle.partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "spark.sql.adaptive.enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        },
        "python_version": platform.python_version(),
        "platform": {"system": platform.system(), "release": platform.release(), "machine": platform.machine()},
        "disk": {"free_bytes_at_start": disk.free, "total_bytes": disk.total},
        "timing": {
            "warmup_runs": config["benchmark"]["warmup_runs"],
            "measured_runs": config["benchmark"]["measured_runs"],
            "random_seed": config["benchmark"]["random_seed"],
            "execution_order": execution_order,
        },
        "compression": config["compression"],
        "cache_state": {
            "benchmark_dataframes_cached": False,
            "clear_cache_before_each_run": True,
            "os_filesystem_cache_flushed": False,
            "limitation": "Operating-system/filesystem caching may affect sequential runs; it is not flushed by this benchmark.",
        },
        "config_source": config_source,
        "effective_configuration": config,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Spark storage representation and layout benchmark")
    parser.add_argument("--config", default=None, help="Optional JSON configuration path")
    parser.add_argument("--input", default=os.path.join("data", "raw", "flights.csv"), help="Source CSV path")
    parser.add_argument("--data-dir", default=os.path.join("data", "generated"), help="Generated datasets directory")
    parser.add_argument("--results-dir", default="results", help="Results directory")
    parser.add_argument("--logs-dir", default="logs", help="Logs directory")
    parser.add_argument("--clean-generated", action="store_true", help="Remove generated datasets after the run")
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.logs_dir)
    config, config_source = load_config(args.config)
    sample_fraction = float(config["benchmark"]["sample_fraction"])
    if not 0 < sample_fraction <= 1:
        raise ValueError("benchmark.sample_fraction must be in (0, 1]")
    os.makedirs(args.results_dir, exist_ok=True)
    clean_dir(os.path.join(args.results_dir, "plans"))
    warn_if_windows_without_hadoop()

    spark = (
        SparkSession.builder.appName("SparkStorageBenchmark")
        .master(config["spark"]["master"])
        .config("spark.sql.shuffle.partitions", str(config["spark"]["shuffle_partitions"]))
        .config("spark.sql.adaptive.enabled", str(config["spark"]["adaptive_enabled"]).lower())
        .getOrCreate()
    )

    try:
        spark.sparkContext.setJobGroup("TASK_2", "Task 2: Storage Format & Layout Benchmark")

        spark.sparkContext.setJobDescription("Task 2: Load Source Flights Dataset")
        source_df = load_source_data(spark, args.input, sample_fraction, config["benchmark"]["random_seed"])
        spark.catalog.clearCache()
        row_count = source_df.count()
        logger.info("Source row count: %d", row_count)
        generated_dir = args.data_dir
        os.makedirs(generated_dir, exist_ok=True)

        representation_cells = []
        format_write_rows = []
        for fmt in config["representation"]["formats"]:
            if fmt not in FORMATS:
                raise ValueError(f"Unsupported representation format: {fmt}")
            path = os.path.join(generated_dir, f"representation_{fmt}")
            spark.catalog.clearCache()

            spark.sparkContext.setJobDescription(f"Task 2: Write Format [{fmt.upper()}] Dataset")
            write_time = write_dataset(source_df, path, fmt, config["compression"][fmt])
            storage = storage_metadata(path)
            format_write_rows.append({"format": fmt, "layout": "baseline", "write_time_sec": write_time, **storage})
            for workload_name in config["representation"]["workloads"]:
                if workload_name not in WORKLOADS:
                    raise ValueError(f"Unsupported workload: {workload_name}")
                representation_cells.append({"format": fmt, "layout": "baseline", "workload": workload_name, "path": path, "storage": storage})

        layout_cells = []
        layout_write_rows = []
        layout_dir = os.path.join(generated_dir, "layout")
        layout_specs = [
            ("baseline", lambda df: df.repartition(int(config["layout"]["baseline_partitions"]))),
            ("many_files_repartition", lambda df: df.repartition(int(config["layout"]["many_files_partitions"]))),
            ("few_files_coalesce", lambda df: df.coalesce(int(config["layout"]["few_files_partitions"]))),
        ]
        for layout_name, transform in layout_specs:
            path = os.path.join(layout_dir, layout_name)
            spark.catalog.clearCache()

            spark.sparkContext.setJobDescription(f"Task 2: Write Parquet Layout [{layout_name}]")
            write_time = write_dataset(source_df, path, "parquet", config["compression"]["parquet"], transform=transform)
            storage = storage_metadata(path)
            layout_write_rows.append({"format": "parquet", "layout": layout_name, "write_time_sec": write_time, **storage})
            layout_cells.append({"format": "parquet", "layout": layout_name, "workload": "W1", "path": path, "storage": storage})

        partitioned_path = os.path.join(generated_dir, "partitioned_parquet_year_month")
        spark.catalog.clearCache()

        spark.sparkContext.setJobDescription("Task 2: Write Partitioned Parquet (Year/Month)")
        partitioned_write_time = write_dataset(source_df, partitioned_path, "parquet", config["compression"]["parquet"], partition_by=PARTITION_COLS)
        partitioned_storage = storage_metadata(partitioned_path)

        # Chạy ma trận benchmark đọc dữ liệu (tên các job được xử lý tự động bên trong run_cells)
        representation_raw = run_cells(spark, representation_cells, config)
        layout_raw = run_cells(spark, layout_cells, config)
        representation_summary = summarize_rows(representation_raw)
        layout_summary = summarize_rows(layout_raw)

        evidence_rows = []
        partitioned_df = reader_for(spark, "parquet", partitioned_path)
        spark.catalog.clearCache()

        spark.sparkContext.setJobDescription("Task 2: Probe - Partition Pruning (W2 Query)")
        pruning_start = time.perf_counter()
        pruning_result = workload(partitioned_df, "W2", config).collect()
        pruning_runtime = time.perf_counter() - pruning_start
        save_plan(args.results_dir, "partition_pruning_w2", workload(partitioned_df, "W2", config), "partition_pruning", "parquet", "partitioned_year_month", "W2", evidence_rows)

        parquet_df = reader_for(spark, "parquet", os.path.join(generated_dir, "representation_parquet"))
        threshold = config["workloads"]["W2_pushdown"]["arrival_delay_gt"]
        pushdown_query = parquet_df.filter(F.col(ARRIVAL_DELAY_COL) > threshold).select(ARRIVAL_DELAY_COL).agg(F.count(F.lit(1)))

        spark.sparkContext.setJobDescription("Task 2: Probe - Predicate Pushdown Query")
        save_plan(args.results_dir, "predicate_pushdown_arrival_delay", pushdown_query, "predicate_pushdown", "parquet", "baseline", "W2_pushdown", evidence_rows)
        save_plan(args.results_dir, "column_pruning_w3", workload(parquet_df, "W3", config), "column_pruning", "parquet", "baseline", "W3", evidence_rows)

        raw_fields = ["format", "layout", "workload", "run_number", "execution_order", "runtime_sec", "result_rows", "path", "num_data_files", "output_size_bytes"]
        write_csv(os.path.join(args.results_dir, "representation_raw.csv"), representation_raw, raw_fields)
        write_csv(os.path.join(args.results_dir, "representation_summary.csv"), representation_summary, list(representation_summary[0].keys()))
        write_csv(os.path.join(args.results_dir, "layout_raw.csv"), layout_raw, raw_fields)
        write_csv(os.path.join(args.results_dir, "layout_summary.csv"), layout_summary, list(layout_summary[0].keys()))
        write_csv(os.path.join(args.results_dir, "plan_evidence.csv"), evidence_rows, ["experiment", "format", "layout", "workload", "partition_filters", "pushed_filters", "read_schema", "plan_file"])

        execution_order = {
            "representation_raw": [{key: row[key] for key in ("format", "layout", "workload", "run_number", "execution_order")} for row in representation_raw],
            "layout_raw": [{key: row[key] for key in ("format", "layout", "workload", "run_number", "execution_order")} for row in layout_raw],
        }
        metadata = collect_metadata(spark, args.input, generated_dir, config, config_source, row_count, execution_order)
        metadata["generated_datasets"] = {
            "representation": format_write_rows,
            "layout": layout_write_rows,
            "partitioned_parquet": {"write_time_sec": partitioned_write_time, **partitioned_storage},
        }
        metadata["partition_pruning_probe"] = {
            "workload": "W2",
            "year": config["workloads"]["W2"]["year"],
            "month": config["workloads"]["W2"]["month"],
            "runtime_sec": pruning_runtime,
            "result_rows": len(pruning_result),
        }
        with open(os.path.join(args.results_dir, "benchmark_metadata.json"), "w", encoding="utf-8") as handle:
            json.dump(metadata, handle, indent=2)
        logger.info("Benchmark finished successfully. Results written to %s", args.results_dir)
    finally:
        spark.sparkContext.setJobDescription(None)
        spark.stop()
        if args.clean_generated:
            clean_dir(args.data_dir)


if __name__ == "__main__":
    main()