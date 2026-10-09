"""Task 1: comparable RDD, DataFrame, and Hybrid access-log pipelines."""

import argparse
import statistics
import time
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from helper import (
    SPARK_LOG_PATTERN,
    create_comparison_df,
    load_ip_country_map,
    parse_log_entry,
    resolve_ip,
)


OUTPUT_SCHEMA = StructType([
    StructField("record_state", StringType(), False),
    StructField("reason", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("http_status", StringType(), True),
    StructField("country", StringType(), True),
    StructField("raw", StringType(), False),
])


class ExecutionTrace:
    """Attach human-readable action labels to Spark jobs and retain their IDs.

    Spark tasks themselves are generic execution units.  A job group and job
    description make the pipeline/action that created each Job and Stage visible
    in both the History Server and the CLI result.
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.entries = []

    def run(self, sc, action, thunk, ui_action=None):
        group_id = f"task1:{self.pipeline.lower()}:{uuid4().hex}"
        # A concise UI label keeps the Jobs page readable.  ``action`` remains
        # the precise, machine-readable name persisted in the execution trace.
        label = f"Task1/{self.pipeline} | {ui_action or action}"
        sc.setJobGroup(group_id, label, interruptOnCancel=False)
        sc.setJobDescription(label)
        try:
            result = thunk()
            self._record(sc, group_id, action)
            return result
        except Exception:
            # Failed jobs are evidence too.  Retain their Spark job/stage IDs
            # so a simulator report can point to the failed Stage Task List.
            self._record(sc, group_id, action)
            raise
        finally:
            # PySpark 4.2 exposes setJobGroup but not clearJobGroup.  Clear the
            # same thread-local properties so a later action cannot inherit it.
            sc.setLocalProperty("spark.jobGroup.id", None)
            sc.setLocalProperty("spark.job.description", None)
            sc.setLocalProperty("spark.job.interruptOnCancel", None)

    def _record(self, sc, group_id, action):
        """Persist the Spark mapping for both successful and failed actions."""
        tracker = sc.statusTracker()
        job_ids = sorted(int(job_id) for job_id in tracker.getJobIdsForGroup(group_id))
        stage_ids = sorted({
            int(stage_id)
            for job_id in job_ids
            for info in [tracker.getJobInfo(job_id)]
            if info is not None
            for stage_id in info.stageIds
        })
        self.entries.append({"action": action, "job_ids": job_ids, "stage_ids": stage_ids})


def unpack_compound_logs(rdd):
    """Expand physical lines to entries; one line may contain many requests."""
    return rdd.flatMap(lambda line: [item.strip() for item in line.split(";") if item.strip()])


def _classify_parsed(parsed, ip_country_map, invalid_log_counter=None):
    if parsed.state == "malformed":
        # Telemetry only.  A task retry/recomputation may execute this add again,
        # so semantic_counts remains the sole correctness/audit result.
        if invalid_log_counter is not None:
            invalid_log_counter.add(1)
        return ("malformed", parsed.reason, None, None, None, parsed.raw)
    country = resolve_ip(parsed.ip, ip_country_map)
    if country == "Unknown":
        return ("unknown", "unmapped_ip", parsed.ip, str(parsed.http_status), country, parsed.raw)
    return ("valid", None, parsed.ip, str(parsed.http_status), country, parsed.raw)


def _rdd_classified_rows(sc, input_path, ip_country_map, invalid_log_counter=None):
    """Build the canonical RDD record-state stream for RDD-derived paths."""
    lookup = sc.broadcast(ip_country_map)
    entries = unpack_compound_logs(sc.textFile(input_path))
    return entries.map(parse_log_entry).map(
        lambda parsed: _classify_parsed(parsed, lookup.value, invalid_log_counter)
    )


def _summarize_rows(sc, rows, trace):
    """Common result contract for an RDD of OUTPUT_SCHEMA-compatible tuples."""
    states = dict(trace.run(
        sc,
        "semantic counts (reduceByKey -> collect)",
        lambda: rows.map(lambda row: (row[0], 1)).reduceByKey(lambda a, b: a + b).collect(),
    ))
    top = trace.run(
        sc,
        "top countries for valid HTTP 200 (reduceByKey -> takeOrdered)",
        lambda: (
            rows.filter(lambda row: row[0] == "valid" and row[3] == "200")
            .map(lambda row: (row[4], 1))
            .reduceByKey(lambda a, b: a + b)
            .takeOrdered(10, key=lambda item: (-item[1], item[0]))
        ),
    )
    http_200 = trace.run(
        sc,
        "HTTP 200 count",
        lambda: rows.filter(lambda row: row[3] == "200").count(),
    )
    return {
        "semantic_counts": {
            "valid": states.get("valid", 0),
            "malformed": states.get("malformed", 0),
            "unknown": states.get("unknown", 0),
            "http_200": http_200,
        },
        "results": dict(top),
        "execution_trace": trace.entries,
    }


def collect_rdd_skew_signals(sc, input_path, ip_country_map):
    """Collect an opt-in skew diagnostic for the RDD aggregation key.

    This is observability, not automatic mitigation. A high key share is a
    reason to inspect the shuffle Stage before adding salting or repartitioning.
    It runs outside timed comparisons and cannot contaminate benchmark medians.
    """
    rows = _rdd_classified_rows(sc, input_path, ip_country_map)
    partition_counts = dict(
        rows.mapPartitionsWithIndex(
            lambda partition_id, items: [(partition_id, sum(1 for _ in items))]
        ).collect()
    )
    country_counts = dict(
        rows.filter(lambda row: row[0] == "valid" and row[3] == "200")
        .map(lambda row: (row[4], 1))
        .reduceByKey(lambda left, right: left + right)
        .collect()
    )
    counts = list(partition_counts.values())
    total_country_records = sum(country_counts.values())
    hottest_country, hottest_count = max(
        country_counts.items(), key=lambda item: (item[1], item[0])
    ) if country_counts else (None, 0)
    return {
        "input_partition_records": partition_counts,
        "input_partition_max_to_min": (
            max(counts) / min(counts) if counts and min(counts) else None
        ),
        "valid_http_200_records": total_country_records,
        "hottest_aggregation_key": hottest_country,
        "hottest_key_records": hottest_count,
        "hottest_key_share": (
            hottest_count / total_country_records if total_country_records else 0.0
        ),
        "interpretation": (
            "Diagnostic only: inspect the shuffle stage before choosing salting, "
            "repartitioning, or another mitigation."
        ),
    }


def _summarize_dataframe(sc, rows, trace):
    """Native DataFrame aggregation counterpart; preserves the same contract."""
    states = {row["record_state"]: row["count"] for row in trace.run(
        sc,
        "semantic counts (groupBy -> collect)",
        lambda: rows.groupBy("record_state").count().collect(),
    )}
    top = [(row["country"], row["access_count"]) for row in trace.run(
        sc,
        "top countries for valid HTTP 200 (groupBy -> orderBy -> collect)",
        lambda: (
            rows.filter((F.col("record_state") == "valid") & (F.col("http_status") == "200"))
            .groupBy("country").count().withColumnRenamed("count", "access_count")
            .orderBy(F.desc("access_count"), F.asc("country")).limit(10).collect()
        ),
    )]
    http_200 = trace.run(
        sc,
        "HTTP 200 count",
        lambda: rows.filter(F.col("http_status") == "200").count(),
    )
    return {
        "semantic_counts": {
            "valid": states.get("valid", 0), "malformed": states.get("malformed", 0),
            "unknown": states.get("unknown", 0),
            "http_200": http_200,
        },
        "results": dict(top),
        "execution_trace": trace.entries,
    }


def execute_rdd_job(sc, spark, input_path, ip_country_map, cache_rows=False):
    """Pure RDD: parsing, CIDR lookup, and aggregation remain in RDDs."""
    invalid_log_counter = sc.accumulator(0)
    trace = ExecutionTrace("RDD")
    started = time.perf_counter()
    rows = _rdd_classified_rows(sc, input_path, ip_country_map, invalid_log_counter)
    if cache_rows:
        # Persisting is lazy: Job 0 populates this cache, then Jobs 1 and 2
        # reuse the classified partitions instead of parsing the input again.
        rows.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        summary = _summarize_rows(sc, rows, trace)
    finally:
        if cache_rows:
            rows.unpersist(blocking=False)
    summary.update(time=time.perf_counter() - started,
                   operator="RDD parser + broadcast CIDR + reduceByKey",
                   input_partitions=sc.textFile(input_path).getNumPartitions(),
                   telemetry_invalid_accumulator=invalid_log_counter.value)
    return summary


def _dataframe_classified(spark, input_path, ip_country_map):
    """DataFrame path using built-in expressions only; no Python UDF."""
    rules = [(net, net | (~mask & 0xFFFFFFFF), country)
             for (net, mask), country in ip_country_map.items()]
    rules_df = F.broadcast(spark.createDataFrame(rules, ["start_ip", "end_ip", "rule_country"]))
    entries = spark.read.text(input_path).select(F.explode(F.split("value", ";")).alias("entry"))
    parsed = (entries.select(F.trim("entry").alias("entry"))
              .withColumn("ip", F.regexp_extract("entry", SPARK_LOG_PATTERN, 1))
              .withColumn("http_status", F.regexp_extract("entry", SPARK_LOG_PATTERN, 6)))
    octets = F.split(F.col("ip"), r"\.")
    # Guard casts explicitly: malformed rows have an empty regex result and
    # must be classified, not crash under Spark's ANSI casting mode.
    valid_ip = F.col("ip") != ""
    ip_int = (F.when(valid_ip, octets.getItem(0).cast("bigint")) * 16777216
              + F.when(valid_ip, octets.getItem(1).cast("bigint")) * 65536
              + F.when(valid_ip, octets.getItem(2).cast("bigint")) * 256
              + F.when(valid_ip, octets.getItem(3).cast("bigint")))
    parsed = parsed.withColumn("ip_int", ip_int)
    joined = parsed.join(rules_df, (F.col("ip_int") >= F.col("start_ip")) &
                          (F.col("ip_int") <= F.col("end_ip")), "left")
    return joined.select(
        F.when(F.col("ip") == "", "malformed").when(F.col("rule_country").isNull(), "unknown")
        .otherwise("valid").alias("record_state"),
        F.when(F.col("ip") == "", "invalid_format").when(F.col("rule_country").isNull(), "unmapped_ip")
        .otherwise(F.lit(None).cast("string")).alias("reason"),
        F.when(F.col("ip") == "", F.lit(None).cast("string")).otherwise(F.col("ip")).alias("ip"),
        F.when(F.col("ip") == "", F.lit(None).cast("string")).otherwise(F.col("http_status")).alias("http_status"),
        F.coalesce(F.col("rule_country"), F.lit("Unknown")).alias("country"),
        F.col("entry").alias("raw"),
    )


def execute_dataframe_job(spark, input_path, ip_country_map):
    """Pure DataFrame counterpart with the same output contract as the RDD path."""
    started = time.perf_counter()
    trace = ExecutionTrace("DataFrame")
    summary = _summarize_dataframe(
        spark.sparkContext, _dataframe_classified(spark, input_path, ip_country_map), trace
    )
    summary.update(time=time.perf_counter() - started,
                   operator="native DataFrame expressions + broadcast range join",
                   input_partitions=spark.read.text(input_path).rdd.getNumPartitions(),
                   telemetry_invalid_accumulator=None)
    return summary


def execute_hybrid_job(sc, spark, input_path, ip_country_map, cache_rows=False):
    """Hybrid: RDD parser plus explicit-schema DataFrame aggregation."""
    invalid_log_counter = sc.accumulator(0)
    trace = ExecutionTrace("Hybrid")
    started = time.perf_counter()
    rows = _rdd_classified_rows(sc, input_path, ip_country_map, invalid_log_counter)
    if cache_rows:
        rows.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        classified_df = spark.createDataFrame(rows, OUTPUT_SCHEMA)
        summary = _summarize_dataframe(sc, classified_df, trace)
    finally:
        if cache_rows:
            rows.unpersist(blocking=False)
    summary.update(
        time=time.perf_counter() - started,
        operator="RDD parser + explicit StructType + DataFrame aggregation",
        input_partitions=sc.textFile(input_path).getNumPartitions(),
        telemetry_invalid_accumulator=invalid_log_counter.value,
    )
    return summary


RUNNERS = {"rdd": execute_rdd_job, "dataframe": execute_dataframe_job, "hybrid": execute_hybrid_job}


def _run(name, sc, spark, args, lookup):
    return (RUNNERS[name](spark, args.input_path, lookup) if name == "dataframe"
            else RUNNERS[name](sc, spark, args.input_path, lookup, cache_rows=args.cache_rows))


def _assert_equivalent(results):
    baseline = results["rdd"]
    bad = [name for name, result in results.items()
           if result["semantic_counts"] != baseline["semantic_counts"] or result["results"] != baseline["results"]]
    if bad:
        raise RuntimeError(f"Semantic contract mismatch: {', '.join(bad)}")


def apply_bad_record_policy(sc, input_path, policy, quarantine_path=None):
    """Apply an explicit post-run policy outside the timed benchmark region.

    Source/file-read failures are not records and are intentionally not handled
    here; Spark source-read errors remain a separate operational failure class.
    """
    malformed = unpack_compound_logs(sc.textFile(input_path)).map(parse_log_entry).filter(
        lambda parsed: parsed.state == "malformed"
    )
    if policy == "skip":
        return {"policy": "skip", "affected_records": None, "quarantine_path": None}
    malformed_count = malformed.count()
    if policy == "fail":
        if malformed_count:
            raise RuntimeError(f"Bad-record policy 'fail': found {malformed_count} malformed entries")
        return {"policy": "fail", "affected_records": 0, "quarantine_path": None}
    if not quarantine_path:
        raise ValueError("--quarantine-path is required when --bad-record-policy=quarantine")
    malformed.map(lambda parsed: f"{parsed.reason}\t{parsed.raw}").saveAsTextFile(quarantine_path)
    return {"policy": "quarantine", "affected_records": malformed_count, "quarantine_path": quarantine_path}


def run_comparison(sc, spark, args, lookup):
    """One warm-up, then five rotated measurement runs by default."""
    for name in RUNNERS:
        _run(name, sc, spark, args, lookup)
    timings, results, order = {name: [] for name in RUNNERS}, {}, list(RUNNERS)
    for index in range(args.benchmark_runs):
        rotated = order[index % len(order):] + order[:index % len(order)]
        for name in rotated:
            results[name] = _run(name, sc, spark, args, lookup)
            timings[name].append(results[name]["time"])
    _assert_equivalent(results)
    return results, {name: statistics.median(values) for name, values in timings.items()}, timings


def print_execution_trace(trace):
    """Print the History-Server mapping in a form convenient for a report."""
    print("Execution trace (action -> Spark jobs -> stages):")
    for entry in trace:
        print(f"  - {entry['action']}: jobs={entry['job_ids']} stages={entry['stage_ids']}")


def build_country_counts_dataframe(sc, spark, pipeline, input_path, ip_country_map):
    """Build the complete valid-HTTP-200 country aggregation for output.

    This is deliberately separate from the timed run.  It uses the selected
    pipeline style, does not use an accumulator, and returns all countries
    rather than only the console top-10.
    """
    if pipeline == "rdd":
        rows = _rdd_classified_rows(sc, input_path, ip_country_map)
        counts = (rows.filter(lambda row: row[0] == "valid" and row[3] == "200")
                  .map(lambda row: (row[4], 1))
                  .reduceByKey(lambda left, right: left + right))
        return counts.toDF(["country", "access_count"]).orderBy(
            F.desc("access_count"), F.asc("country")
        )

    if pipeline == "dataframe":
        rows = _dataframe_classified(spark, input_path, ip_country_map)
    elif pipeline == "hybrid":
        rows = spark.createDataFrame(
            _rdd_classified_rows(sc, input_path, ip_country_map), OUTPUT_SCHEMA
        )
    else:
        raise ValueError(f"Unknown pipeline '{pipeline}'")
    return (rows.filter((F.col("record_state") == "valid") & (F.col("http_status") == "200"))
            .groupBy("country").count().withColumnRenamed("count", "access_count")
            .orderBy(F.desc("access_count"), F.asc("country")))


def write_result_output(sc, spark, output_path, pipeline, input_path, ip_country_map):
    """Materialize the complete country aggregation at the legacy path."""
    country_output = f"{output_path}/country_counts"
    build_country_counts_dataframe(sc, spark, pipeline, input_path, ip_country_map).coalesce(1).write.mode(
        "overwrite"
    ).csv(country_output, header=True)
    print(f"Country counts saved to: {country_output}")


def write_comparison_output(comparison_df, output_path):
    comparison_output = f"{output_path}/comparison_metrics"
    comparison_df.coalesce(1).write.mode("overwrite").csv(comparison_output, header=True)
    print(f"Comparison metrics saved to: {comparison_output}")


def _rdd_lineage_text(rdd):
    lineage = rdd.toDebugString()
    return lineage.decode("utf-8") if isinstance(lineage, bytes) else str(lineage)


def _query_execution_text(dataframe):
    """Return Spark SQL's parsed, optimized, and planned query representation."""
    return dataframe._jdf.queryExecution().toString()


def write_execution_plans(sc, spark, input_path, ip_country_map, pipelines, output_path):
    """Persist planning evidence without including it in benchmark timing.

    The DataFrame files are pre-execution plans; they document Catalyst's
    intended physical strategy rather than claiming an AQE-finalized runtime
    plan. The directory must not already exist to avoid overwriting evidence.
    """
    evidence_dir = Path(output_path)
    evidence_dir.mkdir(parents=True, exist_ok=False)
    for pipeline in pipelines:
        if pipeline == "rdd":
            rows = _rdd_classified_rows(sc, input_path, ip_country_map)
            (evidence_dir / "rdd_lineage.txt").write_text(
                _rdd_lineage_text(rows), encoding="utf-8"
            )
        elif pipeline == "dataframe":
            rows = _dataframe_classified(spark, input_path, ip_country_map)
            (evidence_dir / "dataframe_query_execution.txt").write_text(
                _query_execution_text(rows), encoding="utf-8"
            )
        elif pipeline == "hybrid":
            rows = _rdd_classified_rows(sc, input_path, ip_country_map)
            (evidence_dir / "hybrid_rdd_lineage.txt").write_text(
                _rdd_lineage_text(rows), encoding="utf-8"
            )
            dataframe = spark.createDataFrame(rows, OUTPUT_SCHEMA)
            (evidence_dir / "hybrid_query_execution.txt").write_text(
                _query_execution_text(dataframe), encoding="utf-8"
            )
        else:
            raise ValueError(f"Unknown pipeline '{pipeline}'")
    print(f"Execution-plan evidence saved to: {evidence_dir}")


def create_spark_session(app_name, master=None):
    """Create or reuse the session selected by the Spark launcher.

    In normal use, ``spark-submit --master ...`` owns deployment selection.
    Core pipeline functions receive the resulting ``SparkSession``/``SparkContext``
    as arguments and therefore remain independent of Docker, Standalone, YARN,
    or Kubernetes. ``master`` exists only as an explicit local-development
    fallback when this module is run directly.
    """
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="Comparable Task-1 Spark pipelines")
    parser.add_argument("--input-path", default="data/raw_logs.txt")
    parser.add_argument("--lookup-path", default="data/ip_geolocation.csv")
    parser.add_argument(
        "--master",
        help=("Local-development fallback only. Prefer `spark-submit --master ...` "
              "so the launcher owns deployment selection."),
    )
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--pipeline", choices=RUNNERS, default="rdd")
    parser.add_argument(
        "--cache-rows",
        action="store_true",
        help=("Persist classified RDD rows with MEMORY_AND_DISK for the RDD and Hybrid "
              "pipelines. The first action populates the cache; later actions reuse it."),
    )
    parser.add_argument("--benchmark-runs", type=int, default=5)
    parser.add_argument(
        "--output-path",
        help="Compatibility output directory for top-country and comparison CSV files.",
    )
    parser.add_argument(
        "--explain-output",
        help="New directory for RDD lineage and DataFrame query-plan evidence.",
    )
    parser.add_argument(
        "--rdd-skew-profile",
        action="store_true",
        help=("Run an untimed RDD-only key/partition skew diagnostic after the selected "
              "pipeline. It reports signals; it does not apply an automatic mitigation."),
    )
    parser.add_argument("--bad-record-policy", choices=("skip", "quarantine", "fail"), default="skip")
    parser.add_argument("--quarantine-path", help="New output directory required by the quarantine policy")
    args = parser.parse_args()
    app_name = "Task1-Comparison" if args.compare else f"Task1-{args.pipeline.upper()}"
    spark = create_spark_session(app_name, args.master)
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try:
        lookup = load_ip_country_map(args.lookup_path)
        print(f"Spark {spark.version}; AQE={spark.conf.get('spark.sql.adaptive.enabled')}; input={args.input_path}")
        if args.compare:
            results, medians, timings = run_comparison(sc, spark, args, lookup)
            comparison_df = create_comparison_df(
                spark, results["rdd"], results["dataframe"], results["hybrid"]
            )
            print("Semantic contract: PASS")
            for name in RUNNERS:
                print(f"{name}: median={medians[name]:.3f}s runs={timings[name]} partitions={results[name]['input_partitions']}")
            print("Counts:", results["rdd"]["semantic_counts"])
            print("Top countries:", results["rdd"]["results"])
            for name in RUNNERS:
                print(f"{name}:")
                print_execution_trace(results[name]["execution_trace"])
            print("Accumulator telemetry (not correctness):", {
                name: results[name]["telemetry_invalid_accumulator"] for name in RUNNERS
            })
            print("Bad-record policy:", apply_bad_record_policy(
                sc, args.input_path, args.bad_record_policy, args.quarantine_path
            ))
            if args.output_path:
                write_result_output(
                    sc, spark, args.output_path, "rdd", args.input_path, lookup
                )
                write_comparison_output(comparison_df, args.output_path)
            if args.explain_output:
                write_execution_plans(
                    sc, spark, args.input_path, lookup, RUNNERS, args.explain_output
                )
            if args.rdd_skew_profile:
                print("RDD skew signals:", collect_rdd_skew_signals(sc, args.input_path, lookup))
        else:
            result = _run(args.pipeline, sc, spark, args, lookup)
            print("Counts:", result["semantic_counts"])
            print("Top countries:", result["results"])
            print_execution_trace(result["execution_trace"])
            print("Accumulator telemetry (not correctness):", result["telemetry_invalid_accumulator"])
            print("Bad-record policy:", apply_bad_record_policy(
                sc, args.input_path, args.bad_record_policy, args.quarantine_path
            ))
            if args.output_path:
                write_result_output(
                    sc, spark, args.output_path, args.pipeline, args.input_path, lookup
                )
            if args.explain_output:
                write_execution_plans(
                    sc, spark, args.input_path, lookup, (args.pipeline,), args.explain_output
                )
            if args.rdd_skew_profile:
                print("RDD skew signals:", collect_rdd_skew_signals(sc, args.input_path, lookup))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
