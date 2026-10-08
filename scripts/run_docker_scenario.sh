#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 || "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'EOF'
Usage:
  scripts/run_docker_scenario.sh <scenario> [retry-observation-seconds]

Scenarios:
  malformed_burst
      Controlled malformed records. Verifies semantic malformed counts and the
      bad-record fail policy.

  unknown_cidr
      Valid log records whose IPs do not match the CIDR lookup. They must be
      classified as unknown, not malformed.

  accumulator_recompute
      Demonstrates why an RDD accumulator is telemetry: repeated actions can
      increment it beyond the semantic malformed-record count.

  task_retry_once [seconds]
      Partition 0 / attempt 0 fails, then Spark retries attempt 1 successfully.
      Default observation window: 25 seconds. During that window, inspect
      http://localhost:14040/jobs/ then Stage 0 -> Task List.

  rdd_task_failure
      A Python exception in RDD mapPartitions exhausts the configured task
      attempts. Spark UI shows a red Failed Job and SIMULATED_RDD_TASK_FAILURE.

  dataframe_ansi_cast_failure
      A native DataFrame ANSI cast fails in an executor task. Spark UI shows a
      red Failed Job and CAST_INVALID_INPUT.

After a run:
  Live Driver UI:  http://localhost:14040
  History Server:  http://localhost:18080
EOF
  exit 0
fi

scenario="$1"
retry_observation_seconds="${2:-}"
compose_file="docker-compose.failure-sim.yml"
master="spark://spark-master:7077"
submit_args=(
  --scenario "$scenario"
  --master "$master"
  --output-root /opt/task1/runs/docker
)
if [[ -n "$retry_observation_seconds" ]]; then
  submit_args+=(--retry-observation-seconds "$retry_observation_seconds")
fi

docker compose -f "$compose_file" up -d spark-master spark-worker-a spark-worker-b driver
if [[ "$scenario" == "task_retry_once" ]]; then
  echo "Live Spark evidence: http://localhost:14040/jobs/"
  echo "Stage 0 will show one failed attempt while retry attempt 1 remains active for ${retry_observation_seconds:-25}s."
elif [[ "$scenario" == "rdd_task_failure" || "$scenario" == "dataframe_ansi_cast_failure" ]]; then
  echo "Spark failure evidence: http://localhost:14040/jobs/"
  echo "This scenario is expected to end with a red Failed Job; open its failed Stage and Task List for the error."
fi
docker compose -f "$compose_file" exec -T driver /opt/spark/bin/spark-submit \
  --master "$master" \
  --conf spark.driver.host=driver \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///opt/spark-events \
  /opt/task1/production_simulator.py \
  "${submit_args[@]}"
