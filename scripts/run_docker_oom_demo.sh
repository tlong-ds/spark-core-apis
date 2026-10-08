#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm-oom" || "$#" -ne 1 ]]; then
  cat <<'EOF'
Usage: scripts/run_docker_oom_demo.sh --confirm-oom

Lab-only destructive test. It caps each worker container at 768 MiB, then
asks two executor tasks to touch 700 MiB Python buffers. Spark is expected to
lose executors; cgroup memory.events must show oom_kill > 0 for confirmation.
Workers are restored to the normal compose configuration after the run.
EOF
  exit 2
fi

base_compose="docker-compose.failure-sim.yml"
cap_compose="docker-compose.memory-cap-768m.yml"

restore_workers() {
  docker compose -f "$base_compose" up -d --force-recreate spark-worker-a spark-worker-b >/dev/null
}
trap restore_workers EXIT

docker compose -f "$base_compose" -f "$cap_compose" up -d --force-recreate \
  spark-master spark-worker-a spark-worker-b driver spark-history

set +e
docker compose -f "$base_compose" -f "$cap_compose" exec -T driver /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --conf spark.executor.memoryOverhead=128m \
  --conf spark.driver.host=driver \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///opt/spark-events \
  /opt/task1/production_simulator.py \
  --scenario container_oom_expected \
  --allow-destructive-capacity-test \
  --master spark://spark-master:7077 \
  --output-root /opt/task1/runs/docker
submit_status=$?
set -e

echo "cgroup OOM counters (must show oom_kill > 0):"
oom_kills=0
for worker in spark-worker-a spark-worker-b; do
  echo "[$worker]"
  events="$(docker compose -f "$base_compose" -f "$cap_compose" exec -T "$worker" \
    sh -c 'cat /sys/fs/cgroup/memory.events')"
  printf '%s\n' "$events"
  worker_oom_kills="$(printf '%s\n' "$events" | awk '$1 == "oom_kill" {print $2}')"
  oom_kills=$((oom_kills + worker_oom_kills))
done

if [[ "$submit_status" -ne 0 || "$oom_kills" -eq 0 ]]; then
  echo "OOM demonstration did not meet its acceptance condition." >&2
  exit 1
fi
