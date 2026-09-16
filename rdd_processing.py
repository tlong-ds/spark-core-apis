import re
import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, regexp_extract, col, trim, broadcast

from helper import *


# ==============================================================================
# 1. REUSABLE LOW-LEVEL RDD TRANSFORMATIONS (CORE INTUITION)
# ==============================================================================

def unpack_compound_logs(rdd):
    """flatMap [Narrow]: 1-to-N compound/batch request expansion."""
    return rdd.flatMap(lambda line: [entry.strip() for entry in line.split(";") if entry.strip()])


def parse_and_audit_logs(rdd, invalid_log_counter):
    """map [Narrow]: 1-to-1 parsing with in-flight fault-tolerant error accumulation."""
    def parse(entry):
        m = LOG_REGEX.match(entry)
        if not m:
            invalid_log_counter.add(1)
            return None
        return (m.group(1), int(m.group(2)))

    return rdd.map(parse)


def filter_successful_requests(rdd):
    """filter [Narrow]: Discard corrupt rows and non-200 HTTP statuses."""
    return rdd.filter(lambda record: record is not None and record[1] == 200)


def enrich_with_country(rdd, broadcast_ip_map):
    """map [Narrow]: O(1) in-memory CIDR lookup via Broadcast Variable (zero shuffle)."""
    return rdd.map(lambda record: (resolve_ip(record[0], broadcast_ip_map.value), 1))


def aggregate_access_counts(rdd):
    """reduceByKey [Wide]: Map-side combine and partition shuffle aggregation."""
    return rdd.reduceByKey(lambda count_a, count_b: count_a + count_b)


# ==============================================================================
# 2. JOB EXECUTION & METRICS COLLECTION
# ==============================================================================

def execute_rdd_job(sc, spark, input_path: str, ip_country_map: dict):
    broadcast_ip_map = sc.broadcast(ip_country_map)
    invalid_log_counter = sc.accumulator(0)

    t0 = time.perf_counter()

    # Transformation
    raw_rdd = sc.textFile(input_path)
    unpacked_rdd = unpack_compound_logs(raw_rdd)
    parsed_rdd   = parse_and_audit_logs(unpacked_rdd, invalid_log_counter)
    filtered_rdd = filter_successful_requests(parsed_rdd)
    enriched_rdd = enrich_with_country(filtered_rdd, broadcast_ip_map)
    rdd_counts  = aggregate_access_counts(enriched_rdd)

    # Schema wrapper and Transformation
    country_df = rdd_counts.toDF(["country", "access_count"]).orderBy(col("access_count").desc())

    # Action
    results = country_df.take(10)
    elapsed = time.perf_counter() - t0

    return {
        "time": elapsed,
        "corrupt": invalid_log_counter.value,
        "actions": 1,
        "scans": 1,
        "operator": "In-Memory Bitwise O(1)",
        "results": {row["country"]: row["access_count"] for row in results},
        "df": country_df,
    }


def execute_dataframe_job(spark, input_path: str, ip_country_map: dict):
    range_data = []
    for (net, mask), country in ip_country_map.items():
        range_data.append((net, net | (~mask & 0xFFFFFFFF), country))
    df_rules = spark.createDataFrame(range_data, ["start_ip", "end_ip", "country"])

    t0 = time.perf_counter()
    df_raw = spark.read.text(input_path)
    df_entries = df_raw.select(explode(split(col("value"), ";")).alias("entry"))
    df_parsed = df_entries.select(
        regexp_extract(trim(col("entry")), LOG_REGEX.pattern, 1).alias("ip"),
        regexp_extract(trim(col("entry")), LOG_REGEX.pattern, 2).alias("status"),
    )

    # Action 1: Count malformed records (extra file scan)
    corrupt_count = df_parsed.filter(col("ip") == "").count()

    # Action 2: Non-equi join on CIDR ranges + aggregation
    parts = split(col("ip"), r"\.")
    ip_int_col = (
        parts[0].cast("bigint") * 16777216
        + parts[1].cast("bigint") * 65536
        + parts[2].cast("bigint") * 256
        + parts[3].cast("bigint")
    )
    valid_df = df_parsed.filter((col("ip") != "") & (col("status") == "200")).withColumn("ip_int", ip_int_col)
    joined = valid_df.join(
        broadcast(df_rules),
        (col("ip_int") >= col("start_ip")) & (col("ip_int") <= col("end_ip")),
        "left",
    )
    df_results = joined.groupBy("country").count().orderBy(col("count").desc()).take(10)
    elapsed = time.perf_counter() - t0

    return {
        "time": elapsed,
        "corrupt": corrupt_count,
        "actions": 2,
        "scans": 2,
        "operator": "BroadcastNestedLoopJoin",
        "results": {row["country"]: row["count"] for row in df_results},
    }


# ==============================================================================
# 3. CLI ENTRY POINT & OUTPUT DISPATCHER
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="PySpark Low-Level RDD Logistics Processor")
    parser.add_argument("--input-path", default="data/raw_logs.txt", help="Path to raw logs text file")
    parser.add_argument("--lookup-path", default="data/ip_geolocation.csv", help="Path to master CIDR lookup CSV")
    parser.add_argument("--output-path", default=None, help="Optional output directory for results")
    parser.add_argument("--compare", action="store_true", help="Run quantitative RDD vs DataFrame benchmark")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("RDD_Logistics_Processing").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    ip_country_map = load_ip_country_map(args.lookup_path)

    # Execute RDD pipeline
    rdd_metrics = execute_rdd_job(sc, spark, args.input_path, ip_country_map)

    print("\n" + "=" * 55)
    print("        TOP COUNTRIES BY LOGISTICS ACCESS COUNT        ")
    print("=" * 55)
    rdd_metrics["df"].show(10, truncate=False)

    comparison_df = None
    if args.compare:
        df_metrics = execute_dataframe_job(spark, args.input_path, ip_country_map)
        comparison_df = create_comparison_df(spark, rdd_metrics, df_metrics)
        print("\n" + "=" * 60)
        print("          RDD VS DATAFRAME BENCHMARK MATRIX          ")
        print("=" * 60)
        comparison_df.show(truncate=False)
    else:
        print("=" * 55)
        print(f"  Execution Time:    {rdd_metrics['time']:.2f}s")
        print(f"  Corrupt Lines:     {rdd_metrics['corrupt']:,d}")
        print("=" * 55 + "\n")

    if args.output_path:
        country_output = f"{args.output_path}/country_counts"
        rdd_metrics["df"].coalesce(1).write.mode("overwrite").csv(country_output, header=True)
        print(f"Aggregated results saved to: {country_output}")
        if comparison_df is not None:
            comparison_output = f"{args.output_path}/comparison_metrics"
            comparison_df.coalesce(1).write.mode("overwrite").csv(comparison_output, header=True)
            print(f"Benchmark metrics saved to:  {comparison_output}")

    spark.stop()


if __name__ == "__main__":
    main()
