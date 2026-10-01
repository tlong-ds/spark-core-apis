"""Profile the Airline Delays input for Task 2 and write a JSON result."""

import argparse
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from airline_schema import AIRLINE_SCHEMA

DELAY_COLUMNS = ("ARRIVAL_DELAY", "DEPARTURE_DELAY", "DISTANCE")
NULL_COLUMNS = (
    "AIRLINE",
    "YEAR",
    "MONTH",
    "DISTANCE",
    "ARRIVAL_DELAY",
    "DEPARTURE_DELAY",
    "CANCELLATION_REASON",
    "AIR_SYSTEM_DELAY",
    "SECURITY_DELAY",
    "AIRLINE_DELAY",
    "LATE_AIRCRAFT_DELAY",
    "WEATHER_DELAY",
)
QUANTILES = (0.5, 0.9, 0.95, 0.99)
QUANTILE_RELATIVE_ERROR = 0.0001
DELAY_CAUSE_COLUMNS = (
    "AIR_SYSTEM_DELAY",
    "SECURITY_DELAY",
    "AIRLINE_DELAY",
    "LATE_AIRCRAFT_DELAY",
    "WEATHER_DELAY",
)


def parse_args():
    project_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Profile the Airline Delays CSV for Spark Task 2."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=project_dir.parent / "flights.csv",
        help="Path to flights.csv (default: ../flights.csv)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=project_dir / "results" / "eda_task2.json",
        help="Path for the JSON profile result",
    )
    parser.add_argument(
        "--master",
        default="local[2]",
        help="Spark master; local[2] limits concurrent memory-intensive dedup tasks",
    )
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument(
        "--aqe",
        choices=("true", "false"),
        default="true",
        help="Effective spark.sql.adaptive.enabled value",
    )
    return parser.parse_args()


def rows_as_dicts(dataframe):
    return [row.asDict(recursive=True) for row in dataframe.collect()]


def profile(input_path, output_path, master, shuffle_partitions, aqe_enabled):
    if not input_path.is_file():
        raise FileNotFoundError(f"Input CSV was not found: {input_path}")

    spark = (
        SparkSession.builder.appName("AirlineTask2EDA")
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", aqe_enabled)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        flights = (
            spark.read.option("header", True)
            .schema(AIRLINE_SCHEMA)
            .csv(str(input_path.resolve()))
        )
        csv_read_partition_count = flights.rdd.getNumPartitions()
        flights = flights.cache()
        row_count = flights.count()
        distinct_row_count = flights.dropDuplicates().count()
        duplicate_row_count = row_count - distinct_row_count

        null_counts = flights.agg(
            *[
                F.sum(F.col(column).isNull().cast("long")).alias(column)
                for column in NULL_COLUMNS
            ]
        ).first().asDict()
        null_counts = {key: int(value or 0) for key, value in null_counts.items()}

        all_cause_null = F.lit(True)
        all_cause_non_null = F.lit(True)
        for column in DELAY_CAUSE_COLUMNS:
            all_cause_null = all_cause_null & F.col(column).isNull()
            all_cause_non_null = all_cause_non_null & F.col(column).isNotNull()
        cause_null_pattern = flights.agg(
            F.sum(all_cause_null.cast("long")).alias("all_null"),
            F.sum(all_cause_non_null.cast("long")).alias("all_non_null"),
        ).first().asDict()
        cause_null_pattern = {
            key: int(value or 0) for key, value in cause_null_pattern.items()
        }
        cause_null_pattern["partially_null"] = (
            row_count
            - cause_null_pattern["all_null"]
            - cause_null_pattern["all_non_null"]
        )

        carrier_counts = rows_as_dicts(
            flights.groupBy("AIRLINE")
            .count()
            .orderBy(F.desc("count"), F.asc("AIRLINE"))
        )
        monthly_counts = rows_as_dicts(
            flights.groupBy("YEAR", "MONTH")
            .count()
            .orderBy("YEAR", "MONTH")
        )
        status_counts = rows_as_dicts(
            flights.groupBy("CANCELLED", "DIVERTED")
            .count()
            .orderBy("CANCELLED", "DIVERTED")
        )

        null_by_status = {}
        for column in (
            "ARRIVAL_DELAY",
            "DEPARTURE_DELAY",
            "CANCELLATION_REASON",
        ):
            null_by_status[column] = rows_as_dicts(
                flights.groupBy("CANCELLED", "DIVERTED")
                .agg(
                    F.count(F.lit(1)).alias("rows"),
                    F.sum(F.col(column).isNull().cast("long")).alias("nulls"),
                )
                .orderBy("CANCELLED", "DIVERTED")
            )

        delay_profiles = {}
        for column in DELAY_COLUMNS:
            summary = flights.agg(
                F.count(column).alias("non_null_count"),
                F.min(column).alias("min"),
                F.avg(column).alias("mean"),
                F.max(column).alias("max"),
            ).first().asDict()
            quantile_values = flights.approxQuantile(
                column, list(QUANTILES), QUANTILE_RELATIVE_ERROR
            )
            delay_profiles[column] = {
                **summary,
                "quantiles": {
                    str(quantile): value
                    for quantile, value in zip(QUANTILES, quantile_values)
                },
            }

        result = {
            "metadata": {
                "dataset": "Kaggle usdot/flight-delays flights.csv",
                "input_path": str(input_path.resolve()),
                "input_size_bytes": input_path.stat().st_size,
                "row_count": row_count,
                "column_count": len(AIRLINE_SCHEMA.fields),
                "schema": [
                    {"name": field.name, "type": field.dataType.simpleString()}
                    for field in AIRLINE_SCHEMA.fields
                ],
                "csv_read_partition_count": csv_read_partition_count,
                "spark_sql_files_max_partition_bytes": spark.conf.get(
                    "spark.sql.files.maxPartitionBytes"
                ),
                "spark_sql_files_open_cost_in_bytes": spark.conf.get(
                    "spark.sql.files.openCostInBytes"
                ),
                "spark_version": spark.version,
                "python_version": platform.python_version(),
                "master": spark.sparkContext.master,
                "spark_sql_adaptive_enabled": spark.conf.get(
                    "spark.sql.adaptive.enabled"
                ),
                "spark_sql_shuffle_partitions": spark.conf.get(
                    "spark.sql.shuffle.partitions"
                ),
                "quantile_relative_error": QUANTILE_RELATIVE_ERROR,
                "profiled_at_utc": datetime.now(timezone.utc).isoformat(),
            },
            "distinct_row_count": distinct_row_count,
            "duplicate_row_count": duplicate_row_count,
            "null_counts": null_counts,
            "delay_cause_columns": list(DELAY_CAUSE_COLUMNS),
            "delay_cause_null_pattern": cause_null_pattern,
            "carrier_counts": carrier_counts,
            "year_month_counts": monthly_counts,
            "flight_status_counts": status_counts,
            "nulls_by_flight_status": null_by_status,
            "delay_profiles": delay_profiles,
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        print(f"Profiled {row_count:,} rows from {input_path}")
        print(f"Spark {spark.version}; AQE={result['metadata']['spark_sql_adaptive_enabled']}")
        print(f"Wrote EDA results to {output_path}")
    finally:
        spark.stop()


def main():
    args = parse_args()
    profile(
        args.input,
        args.output,
        args.master,
        args.shuffle_partitions,
        args.aqe,
    )


if __name__ == "__main__":
    main()