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
├── requirements.txt            # Python dependencies (pyspark>=3.5.0, psutil>=5.9.0)
└── README.md                   # Documentation and execution guide
```

---

## Task 1: Low-Level RDD Processing & Benchmark (`rdd_processing.py`)

### Dataset Scale
- **Records**: 1,000,000 server access log rows (~113.2 MB)
- **Deliberately Corrupted Records**: 13,866 rows (audited in-flight by Accumulator)
- **Master Geolocation Table**: 15 CIDR subnet rules (`data/ip_geolocation.csv`)

### How to Run

```bash
# 1. Activate virtual environment
source .venv/bin/activate

# 2. Run core RDD pipeline
python3 rdd_processing.py --input-path data/raw_logs.txt --lookup-path data/ip_geolocation.csv

# 3. Run with Quantitative Benchmark Mode (displays comparison DataFrame)
python3 rdd_processing.py --input-path data/raw_logs.txt --lookup-path data/ip_geolocation.csv --compare

# 4. Run and persist both country rankings & benchmark DataFrame to CSV
python3 rdd_processing.py --input-path data/raw_logs.txt --lookup-path data/ip_geolocation.csv --compare --output-path data/output
```
