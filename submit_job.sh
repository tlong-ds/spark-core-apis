#!/usr/bin/env bash
set -euo pipefail

# Run from any working directory; keep all relative data/config paths stable.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate the Python virtual environment before invoking this script.
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYSPARK_PYTHON}"

# Git Bash on Windows invokes the Windows wrapper. Linux/macOS/WSL use the
# native shell executable; never try to interpret a .cmd file as Bash.
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) SPARK_BIN="${SPARK_BIN:-spark-submit.cmd}" ;;
  *) SPARK_BIN="${SPARK_BIN:-spark-submit}" ;;
esac
if ! command -v "$SPARK_BIN" >/dev/null 2>&1; then
  echo "Cannot find $SPARK_BIN. Activate the PySpark virtual environment." >&2
  exit 1
fi

# Forward optional --task all|task1|task2 and other main.py flags via "$@".
"$SPARK_BIN" \
  --master "${SPARK_MASTER:-local[*]}" \
  --name "${SPARK_APP_NAME:-LowLevel_FileFormat_Job}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-10}" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.pyspark.python=$PYSPARK_PYTHON" \
  --conf "spark.pyspark.driver.python=$PYSPARK_DRIVER_PYTHON" \
  --py-files "helper.py,rdd_processing.py,spark-storage-benchmark/format_benchmark.py" \
  main.py \
  --input-path ./data/raw_logs.txt \
  --lookup-path ./data/ip_geolocation.csv \
  --output-path ./data/output \
  --format-input ./spark-storage-benchmark/data/raw/flights.csv \
  --format-data-dir ./spark-storage-benchmark/data/generated \
  --format-results-dir ./spark-storage-benchmark/results \
  --format-logs-dir ./spark-storage-benchmark/logs \
  --format-config ./spark-storage-benchmark/config/benchmark_config.json \
  "$@"
