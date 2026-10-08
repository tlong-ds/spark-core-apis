# Spark Core APIs: Low-Level RDD Processing & Storage Formats

This repository contains practical implementations and benchmarks for Spark Low-Level Core APIs (RDDs, Distributed Shared Variables), Application Execution, and Storage Formats based on *Spark: The Definitive Guide*.

---

## Project Structure

```text
spark-core-apis/
├── data/
│   ├── ip_geolocation.csv      # Master CIDR lookup table (15 subnet rules)
│   ├── raw_logs.txt            # Big Data logistics access logs (1,000,000 records, ~113.2 MB)
│   └── output/
│       ├── country_counts/     # Aggregated access counts by country (CSV)
│       └── comparison_metrics/ # Quantitative benchmark matrix DataFrame (CSV)
├── generate_logs.py            # High-volume logistics log generator (buffered I/O)
├── helper.py                   # Helper functions (regex, CIDR loader, IP resolver)
├── rdd_processing.py           # Task 1: Low-level RDD pipeline & quantitative benchmark
├── production_simulator.py      # Controlled local production-behaviour scenarios
├── simulator/                   # Scenario fixtures, runner, and evidence collection
├── requirements.txt            # Python dependencies (pyspark>=3.5.0, psutil>=5.9.0)
├── RDD_FINDINGS.md             # In-depth architectural findings and benchmark report
└── README.md                   # Documentation and execution guide
```

---

## Task 1: Low-Level RDD Processing & Benchmark (`rdd_processing.py`)

### Dataset Scale
- **Records**: 1,000,000 server access log rows (~113.2 MB)
- **Malformed-record handling**: parser status/reason is the correctness source; the Accumulator is not used for correctness because retries can over-count it.
- **Master Geolocation Table**: 15 CIDR subnet rules (`data/ip_geolocation.csv`)

### How to Run

```bash
# 1. Activate virtual environment
source .venv/bin/activate

# 2. Run core RDD pipeline through Spark's launcher (default)
spark-submit --master 'local[*]' rdd_processing.py \
  --input-path data/raw_logs.txt --lookup-path data/ip_geolocation.csv

# 3. Run Hybrid parsing + explicit-schema DataFrame aggregation.
spark-submit --master 'local[*]' rdd_processing.py --pipeline hybrid \
  --input-path data/raw_logs.txt --lookup-path data/ip_geolocation.csv

# 4. Run the 3-way comparison: one warm-up, then five rotated measurement runs
spark-submit --master 'local[*]' rdd_processing.py \
  --input-path data/raw_logs.txt --lookup-path data/ip_geolocation.csv --compare

# 5. Use a different number of measured runs when needed
spark-submit --master 'local[*]' rdd_processing.py \
  --input-path data/raw_logs.txt --lookup-path data/ip_geolocation.csv --compare --benchmark-runs 7

# 6. Handle malformed entries explicitly.  Quarantine paths must not exist yet.
spark-submit --master 'local[*]' rdd_processing.py --pipeline rdd --bad-record-policy quarantine \
  --quarantine-path data/output/task1_quarantine

# 7. Preserve the legacy CSV output convention. Writes the complete semantic
# country aggregation and, when --compare is used, the comparison matrix.
spark-submit --master 'local[*]' rdd_processing.py --compare --output-path data/output

# 8. Persist evidence for the report: RDD lineage plus DataFrame/Hybrid query
# plans. The target directory must not already exist.
spark-submit --master 'local[*]' rdd_processing.py --compare \
  --explain-output runs/task1_plan_evidence

# 9. Optional, untimed RDD skew diagnostic. It reports partition/key signals
# and never applies a mitigation automatically.
spark-submit --master 'local[*]' rdd_processing.py --pipeline rdd \
  --rdd-skew-profile
```

The three implementations must agree on `valid`, `malformed`, `unknown`, HTTP-200,
and country top-10 results before benchmark medians are interpreted. Use
`pytest -q` to run the compact fixture/equivalence tests.

`invalid_log_counter` is printed only as RDD/Hybrid telemetry. Task retries or
recomputation can increment it again, so the semantic `malformed` aggregation
is the correctness count. `--bad-record-policy` controls malformed entries only;
Spark source/file-read failures are diagnosed separately and are not row errors.

Every action also receives a pipeline-aware Spark Job Group. The CLI prints an
`action -> jobs -> stages` trace, and the same label is visible in the Spark
History Server. This makes an RDD/DataFrame/Hybrid run distinguishable even when
they share a comparison application.

For detailed architectural analysis and runtime trade-offs, see [`RDD_FINDINGS.md`](file:///Users/bunnypro/Projects/spark-core-apis/RDD_FINDINGS.md).

### Production-behaviour simulator

`production_simulator.py` creates an isolated, non-overwriting run folder under
`runs/`. It does not touch `data/raw_logs.txt`. Each run stores its generated
input, `config.json`, `result.json`, and stdout/stderr logs.

```bash
# Malformed-record burst: semantic counts remain auditable; fail policy rejects it.
python3 production_simulator.py --scenario malformed_burst

# Valid-but-unmapped IPs stay `unknown`, not `malformed`.
python3 production_simulator.py --scenario unknown_cidr

# Demonstrate why the RDD accumulator is telemetry, not a correctness counter.
python3 production_simulator.py --scenario accumulator_recompute

# Spark task failure, then retry. For 25 seconds, the live Driver UI keeps the
# failed attempt and the running retry in the same active Stage.
scripts/run_docker_scenario.sh task_retry_once

# Terminal RDD failure: mapPartitions raises a Python exception. The Job and
# Stage turn red; its Task List contains SIMULATED_RDD_TASK_FAILURE.
scripts/run_docker_scenario.sh rdd_task_failure

# Terminal DataFrame failure: native ANSI cast of access-log text to INT. The
# Job and Stage turn red; its Task List contains CAST_INVALID_INPUT.
scripts/run_docker_scenario.sh dataframe_ansi_cast_failure

# Optional: use a 60-second observation window instead.
scripts/run_docker_scenario.sh task_retry_once 60

# Controlled workload exercises: inspect their Stage metrics in Spark UI.
scripts/run_docker_scenario.sh skewed_key
scripts/run_docker_scenario.sh slow_shuffle_join
scripts/run_docker_scenario.sh memory_pressure

# Destructive capacity test: requires an explicit acknowledgement. Spark UI
# normally shows executor loss; cgroup oom_kill is the authoritative proof.
scripts/run_docker_oom_demo.sh --confirm-oom
```

These are deterministic `local[2]` simulations. They test record quality,
enrichment gaps, and recomputation observability; they do **not** prove task
retry, executor loss, or speculative execution. Those scenarios require a real
local-cluster, Standalone, YARN, or Kubernetes deployment and separate Spark UI
or event-log evidence.

### Docker Standalone lab

The Docker lab starts a Spark Standalone master, two one-core workers, and a
driver container. It is deliberately a local lab, but the workers are separate
processes so it is the right basis for executor-loss and speculation exercises.

```bash
# Start the cluster and submit one existing simulator scenario through it.
scripts/run_docker_scenario.sh malformed_burst

# Submit the Task-1 pipeline through spark-submit; the pipeline does not choose
# the Docker master itself.
scripts/run_docker_task1.sh rdd

# Inspect cluster registration and the two workers.
open http://localhost:8080
# Worker UIs: http://localhost:8081 (worker-a), http://localhost:8082 (worker-b)

# Stop the lab when finished. This does not remove source code or run evidence.
docker compose -f docker-compose.failure-sim.yml down
```

The driver stores Spark event logs under `runs/docker-eventlog/` and scenario
evidence under `runs/docker/`. Do not kill a worker during the initial semantic
scenario: the next step is to add a controlled long-running failure injector so
that task rescheduling can be verified from the event log rather than guessed
from final output.

Open http://localhost:18080 for the Spark History Server. Unlike the transient
driver UI, it reconstructs completed applications from the event logs, including
the Jobs, Stages, Executors, and task-attempt views.

For a running Docker submission, the transient Driver UI is published at
http://localhost:14040. With `task_retry_once`, open
http://localhost:14040/jobs/ immediately. The Job description is deliberately
short: `RETRY: P0/A0 failed -> P0/A1 retry`. The Jobs table will show
`Tasks: 1/2 (1 failed)` while the retry is held active. Select the active
stage, then its **Task List**, for the source-of-truth sequence:

1. Partition 0, attempt 0 — **FAILED**, with `SIMULATED_TASK_RETRY` and the
   Python stack trace.
2. Partition 0, attempt 1 — **RUNNING** during the observation window, then
   **SUCCESS**.

The Jobs timeline remains green because the *job* has not failed; a Spark task
attempt failed and Spark is recovering it. After completion, History Server
preserves the same Stage Task List and failed-attempt error details.

`rdd_task_failure` and `dataframe_ansi_cast_failure` are intentionally
different from that retry demo: their Job is expected to fail. In the Driver UI
or History Server, use **Jobs → failed Job → failed Stage → Task List**. The RDD
scenario exposes a Python `mapPartitions` exception; the DataFrame scenario
exposes the native Spark ANSI `CAST_INVALID_INPUT` error. The simulator reports
`EXPECTED_FAILURE` and exits successfully only because that red Spark Job is the
scenario's expected evidence.
