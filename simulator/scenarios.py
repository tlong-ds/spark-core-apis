"""Deterministic scenario fixtures for the Task-1 production simulator."""

from dataclasses import dataclass
from pathlib import Path


def _log(ip: str, status: int = 200) -> str:
    return (
        f'{ip} - - [16/Sep/2026:10:00:00 +0000] '
        f'"GET /api/v1/shipments/track?tx_id=SIM-1 HTTP/1.1" '
        f'{status} 512 15ms'
    )


@dataclass(frozen=True)
class Scenario:
    """A deterministic workload and its expected semantic observations."""

    name: str
    description: str
    capability: str
    lines: tuple[str, ...]
    expected_counts: dict[str, int]
    expected_top_countries: dict[str, int]
    bad_record_policy: str = "skip"
    expect_policy_failure: bool = False
    require_telemetry_overcount: bool = False
    fault_injector: str | None = None
    probe: str | None = None
    expected_job_failure: bool = False
    retry_observation_seconds: int = 0
    spark_conf: tuple[tuple[str, str], ...] = ()

    def write_input(self, run_dir: Path) -> Path:
        path = run_dir / "scenario_input.txt"
        path.write_text("\n".join(self.lines) + "\n", encoding="utf-8")
        return path


SCENARIOS = {
    "malformed_burst": Scenario(
        name="malformed_burst",
        description=(
            "A burst of malformed entries, including one compound physical line; "
            "checks that semantic accounting stays visible and the fail policy rejects it."
        ),
        capability="local deterministic record-quality simulation",
        lines=(
            _log("192.168.0.1"),
            "CORRUPTED_PACKET_HEADER_01",
            f"{_log('192.168.0.2')}; TRUNCATED_PAYLOAD",
            "NOT_AN_ACCESS_LOG",
        ),
        expected_counts={"valid": 2, "malformed": 3, "unknown": 0, "http_200": 2},
        expected_top_countries={"United States": 2},
        bad_record_policy="fail",
        expect_policy_failure=True,
    ),
    "unknown_cidr": Scenario(
        name="unknown_cidr",
        description=(
            "Syntactically valid requests from addresses outside the broadcast CIDR table; "
            "checks that enrichment gaps are not relabelled as malformed input."
        ),
        capability="local deterministic enrichment-gap simulation",
        lines=(_log("192.168.0.1"), _log("8.8.8.8"), _log("1.1.1.1", 404)),
        # http_200 is an HTTP-status metric for every parsed record, including
        # syntactically valid entries whose CIDR cannot be enriched.
        expected_counts={"valid": 1, "malformed": 0, "unknown": 2, "http_200": 2},
        expected_top_countries={"United States": 1},
    ),
    "accumulator_recompute": Scenario(
        name="accumulator_recompute",
        description=(
            "A malformed record with multiple result actions.  It demonstrates that an RDD "
            "accumulator can exceed the semantic malformed count after recomputation."
        ),
        capability="local deterministic observability simulation; not a task-retry simulation",
        lines=(_log("192.168.0.1"), "CORRUPTED_PACKET_HEADER_RECOMPUTE"),
        expected_counts={"valid": 1, "malformed": 1, "unknown": 0, "http_200": 1},
        expected_top_countries={"United States": 1},
        require_telemetry_overcount=True,
    ),
    "task_retry_once": Scenario(
        name="task_retry_once",
        description=(
            "Partition 0 raises a controlled exception only on attempt 0. Spark must retry "
            "it as attempt 1; History Server retains the failed task attempt and its error."
        ),
        capability="Spark task-retry simulation; run with Docker Standalone for UI evidence",
        lines=(_log("192.168.0.1"), _log("192.168.0.2"), _log("192.168.0.3"), _log("192.168.0.4")),
        expected_counts={"valid": 4, "malformed": 0, "unknown": 0, "http_200": 4},
        expected_top_countries={"United States": 4},
        fault_injector="fail_partition_zero_on_first_attempt",
        probe="rdd_retry_once",
        # Keep the retry attempt running long enough for the transient driver
        # UI to show both the failed attempt and the active retry together.
        retry_observation_seconds=25,
        spark_conf=(("spark.task.maxFailures", "2"),),
    ),
    "rdd_task_failure": Scenario(
        name="rdd_task_failure",
        description=(
            "An RDD mapPartitions transformation raises a deterministic Python exception. "
            "With one permitted attempt, the Spark job and stage must fail."
        ),
        capability="Spark RDD task-failure simulation; run with Docker Standalone for UI evidence",
        lines=(_log("192.168.0.1"), _log("192.168.0.2"), _log("192.168.0.3"), _log("192.168.0.4")),
        expected_counts={"valid": 4, "malformed": 0, "unknown": 0, "http_200": 4},
        expected_top_countries={"United States": 4},
        fault_injector="fail_partition_zero_every_attempt",
        probe="rdd_terminal_failure",
        expected_job_failure=True,
        spark_conf=(("spark.task.maxFailures", "1"),),
    ),
    "dataframe_ansi_cast_failure": Scenario(
        name="dataframe_ansi_cast_failure",
        description=(
            "A native DataFrame cast tries to turn raw access-log text into INT while ANSI mode is on. "
            "The executor task must fail with CAST_INVALID_INPUT."
        ),
        capability="Spark DataFrame task-failure simulation; run with Docker Standalone for UI evidence",
        lines=(_log("192.168.0.1"), _log("192.168.0.2"), _log("192.168.0.3"), _log("192.168.0.4")),
        expected_counts={"valid": 4, "malformed": 0, "unknown": 0, "http_200": 4},
        expected_top_countries={"United States": 4},
        probe="dataframe_ansi_cast_failure",
        expected_job_failure=True,
        spark_conf=(("spark.task.maxFailures", "1"), ("spark.sql.ansi.enabled", "true")),
    ),
}


def get_scenario(name: str) -> Scenario:
    try:
        return SCENARIOS[name]
    except KeyError as exc:
        available = ", ".join(sorted(SCENARIOS))
        raise ValueError(f"Unknown scenario '{name}'. Choose one of: {available}") from exc
