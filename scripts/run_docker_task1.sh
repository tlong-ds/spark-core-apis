#!/usr/bin/env bash
set -euo pipefail

pipeline="${1:-rdd}"
if [[ "$#" -gt 0 ]]; then
  shift
fi

case "$pipeline" in
  rdd|dataframe|hybrid) ;;
  *)
    echo "usage: scripts/run_docker_task1.sh [rdd|dataframe|hybrid] [Task-1 arguments...]" >&2
    exit 2
    ;;
esac

compose_file="docker-compose.failure-sim.yml"
master="spark://spark-master:7077"

docker compose -f "$compose_file" up -d spark-master spark-worker-a spark-worker-b driver spark-history
docker compose -f "$compose_file" exec -T driver /opt/spark/bin/spark-submit \
  --master "$master" \
  --conf spark.driver.host=driver \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///opt/spark-events \
  /opt/task1/rdd_processing.py \
  --pipeline "$pipeline" \
  --input-path /opt/task1/data/raw_logs.txt \
  --lookup-path /opt/task1/data/ip_geolocation.csv \
  "$@"
