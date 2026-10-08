"""Execute a scenario without mixing simulator evidence into pipeline timings."""

from __future__ import annotations

from datetime import datetime, timezone
from functools import partial
import json
from pathlib import Path
import traceback
from typing import Any
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F

from helper import load_ip_country_map
from rdd_processing import ExecutionTrace, apply_bad_record_policy, execute_rdd_job, unpack_compound_logs
from simulator.failure_injectors import INJECTORS
from simulator.scenarios import Scenario


def create_run_directory(output_root: str | Path, scenario_name: str) -> Path:
    """Create one non-overwriting evidence directory for a simulator run."""
    root = Path(output_root)
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{scenario_name}_{uuid4().hex[:8]}"
    run_dir = root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def _assert_expected(scenario: Scenario, result: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if result["semantic_counts"] != scenario.expected_counts:
        failures.append(
            f"semantic_counts expected {scenario.expected_counts}, got {result['semantic_counts']}"
        )
    if result["results"] != scenario.expected_top_countries:
        failures.append(
            f"top countries expected {scenario.expected_top_countries}, got {result['results']}"
        )
    telemetry = result["telemetry_invalid_accumulator"]
    if scenario.require_telemetry_overcount and telemetry <= scenario.expected_counts["malformed"]:
        failures.append(
            "expected accumulator telemetry to exceed the malformed semantic count after recomputation"
        )
    return failures


def _run_rdd_fault_probe(
    sc,
    input_path: str,
    injector_name: str,
    retry_observation_seconds: int,
) -> dict[str, Any]:
    """Run an RDD fault probe before the clean Task-1 pipeline.

    This makes the injected exception appear once in Spark History while keeping
    the production RDD pipeline and its three semantic actions fault-free.
    """
    trace = ExecutionTrace("Simulator/RDD")
    injector = partial(
        INJECTORS[injector_name],
        retry_observation_seconds=retry_observation_seconds,
    )
    entries = unpack_compound_logs(sc.textFile(input_path)).mapPartitions(injector)
    is_retry_demo = injector_name == "fail_partition_zero_on_first_attempt"
    processed_entries = trace.run(
        sc,
        (
            "task retry: partition 0 attempt 0 failed; partition 0 attempt 1 retried successfully"
            if is_retry_demo
            else "RDD task failure: partition 0 mapPartitions raises a Python exception"
        ),
        lambda: entries.count(),
        ui_action=(
            "RETRY: P0/A0 failed -> P0/A1 retry"
            if is_retry_demo else "RDD: P0 mapPartitions failure"
        ),
    )
    return {"processed_entries": processed_entries, "execution_trace": trace.entries}


def _run_dataframe_ansi_cast_probe(spark, input_path: str) -> dict[str, Any]:
    """Fail a native DataFrame task, without a Python UDF or RDD injector.

    Raw access-log rows cannot be cast to INT under ANSI mode. ``collect`` is
    deliberate: it forces the expression to execute in executor tasks, so the
    Spark UI records a failed task and its JVM-side CAST_INVALID_INPUT error.
    """
    trace = ExecutionTrace("Simulator/DataFrame")
    raw_logs = spark.read.text(input_path).repartition(2)
    trace.run(
        spark.sparkContext,
        "DataFrame ANSI cast failure: raw log text cannot become INT",
        lambda: raw_logs.select(F.col("value").cast("int").alias("invalid_integer")).collect(),
        ui_action="DATAFRAME: ANSI cast failure",
    )
    raise AssertionError("DataFrame ANSI cast probe unexpectedly succeeded")


def _run_expected_failure_probe(spark, input_path: str, scenario: Scenario, observation_seconds: int):
    if scenario.probe == "rdd_terminal_failure":
        return _run_rdd_fault_probe(
            spark.sparkContext,
            input_path,
            scenario.fault_injector,
            observation_seconds,
        )
    if scenario.probe == "dataframe_ansi_cast_failure":
        return _run_dataframe_ansi_cast_probe(spark, input_path)
    raise ValueError(f"Scenario '{scenario.name}' has no expected-failure probe")


def run_scenario(
    scenario: Scenario,
    lookup_path: str,
    output_root: str | Path = "runs",
    master: str = "local[2]",
    retry_observation_seconds: int | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Run a controlled RDD or DataFrame failure fixture and persist evidence.

    The regular scenarios exercise the RDD pipeline because its accumulator
    behaviour is part of the experiment. Dedicated probes may instead exercise
    an RDD task or a native DataFrame expression. It is not a performance
    benchmark and does not claim executor-loss behaviour for local mode.
    """
    run_dir = create_run_directory(output_root, scenario.name)
    input_path = scenario.write_input(run_dir)
    observation_seconds = (
        scenario.retry_observation_seconds
        if retry_observation_seconds is None
        else retry_observation_seconds
    )
    config = {
        "scenario": scenario.name,
        "description": scenario.description,
        "capability": scenario.capability,
        "master": master,
        "deployment": "standalone_cluster" if master.startswith("spark://") else "local",
        "pipeline": "rdd",
        "lookup_path": str(Path(lookup_path).resolve()),
        "input_path": str(input_path.resolve()),
        "expected_counts": scenario.expected_counts,
        "expected_top_countries": scenario.expected_top_countries,
        "fault_injector": scenario.fault_injector,
        "retry_observation_seconds": observation_seconds,
        "spark_conf": dict(scenario.spark_conf),
    }
    (run_dir / "config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")

    spark = None
    try:
        builder = SparkSession.builder.appName(f"Task1Simulator-{scenario.name}").master(master)
        for key, value in scenario.spark_conf:
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        lookup = load_ip_country_map(lookup_path)
        if scenario.expected_job_failure:
            try:
                _run_expected_failure_probe(spark, str(input_path), scenario, observation_seconds)
            except Exception as exc:
                report = {
                    "status": "EXPECTED_FAILURE",
                    "scenario": scenario.name,
                    "deployment": config["deployment"],
                    "failure_surface": "RDD task" if scenario.probe == "rdd_terminal_failure" else "DataFrame executor expression",
                    "expected_error_hint": (
                        "SIMULATED_RDD_TASK_FAILURE"
                        if scenario.probe == "rdd_terminal_failure"
                        else "CAST_INVALID_INPUT"
                    ),
                    "observed_error": str(exc),
                    "fault_injection": {
                        "injector": scenario.fault_injector,
                        "spark_conf": dict(scenario.spark_conf),
                        "evidence": "Spark Job and Stage are expected to be FAILED; inspect the failed task error in Spark UI.",
                    },
                }
            else:
                report = {
                    "status": "FAIL",
                    "scenario": scenario.name,
                    "deployment": config["deployment"],
                    "assertion_failures": ["expected Spark job failure, but the probe succeeded"],
                }
            return _persist_report(run_dir, scenario.name, report)

        fault_probe = (
            _run_rdd_fault_probe(
                spark.sparkContext,
                str(input_path),
                scenario.fault_injector,
                observation_seconds,
            )
            if scenario.fault_injector else None
        )
        result = execute_rdd_job(spark.sparkContext, spark, str(input_path), lookup)
        assertions = _assert_expected(scenario, result)
        policy = {"requested": scenario.bad_record_policy, "outcome": "not_run"}
        if scenario.expect_policy_failure:
            try:
                apply_bad_record_policy(spark.sparkContext, str(input_path), scenario.bad_record_policy)
            except RuntimeError as exc:
                policy = {"requested": scenario.bad_record_policy, "outcome": "rejected", "message": str(exc)}
            else:
                assertions.append("bad-record fail policy unexpectedly accepted malformed entries")
                policy = {"requested": scenario.bad_record_policy, "outcome": "accepted"}
        else:
            policy_result = apply_bad_record_policy(spark.sparkContext, str(input_path), scenario.bad_record_policy)
            policy = {"requested": scenario.bad_record_policy, "outcome": "completed", "result": policy_result}
        report = {
            "status": "PASS" if not assertions else "FAIL",
            "scenario": scenario.name,
            "deployment": config["deployment"],
            "cluster_failure_modes_not_tested": [
                mode for mode in ("task retry", "executor loss", "speculation")
                if not (mode == "task retry" and scenario.fault_injector)
            ],
            "result": result,
            "fault_probe": fault_probe,
            "policy": policy,
            "fault_injection": {
                "injector": scenario.fault_injector,
                "spark_conf": dict(scenario.spark_conf),
                "evidence": (
                    "attempt 0 fails deterministically; attempt 1 stays active for the configured "
                    "observation window, then succeeds"
                    if scenario.fault_injector else "not applicable"
                ),
            },
            "assertion_failures": assertions,
        }
    except Exception as exc:
        report = {
            "status": "ERROR",
            "scenario": scenario.name,
            "deployment": config["deployment"],
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
    finally:
        if spark is not None:
            spark.stop()

    return _persist_report(run_dir, scenario.name, report)


def _persist_report(run_dir: Path, scenario_name: str, report: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    (run_dir / "result.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (run_dir / "stdout.log").write_text(
        f"scenario={scenario_name}\nstatus={report['status']}\n", encoding="utf-8"
    )
    (run_dir / "stderr.log").write_text("", encoding="utf-8")
    return run_dir, report
