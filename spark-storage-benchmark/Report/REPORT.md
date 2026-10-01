# Task 2 — Spark Storage Benchmark

## 1. Objective

This experiment examines how physical file representation, physical layout, and selective reading affect Spark workload performance. The representation comparison covers CSV, JSON, Parquet, and ORC. A separate experiment compares Parquet layouts produced with different numbers of execution partitions, and physical plans are inspected for partition pruning, predicate pushdown, and column projection.

## 2. Experimental Setup

The benchmark used the complete `usdot/flight-delays flights.csv` dataset with 5,819,079 rows and a six-column schema. The recorded environment and effective settings were:

| Parameter | Value |
| --- | --- |
| Spark version | 3.5.9 |
| Python version | 3.11.7 |
| Master | `local[2]` |
| Available system cores | 12 |
| Default parallelism | 2 |
| Shuffle partitions | 8 |
| AQE | enabled |
| Warm-up runs | 1, discarded |
| Measured runs | 5 |
| Compression | CSV: none; JSON: none; Parquet: Snappy; ORC: Zlib |
| Dataset | `usdot/flight-delays flights.csv` |
| Row count | 5,819,079 |
| Sample fraction | 1.0 |
| Cache state | Benchmark DataFrames were not persisted; Spark catalog cache was cleared before each run; OS/filesystem cache was not flushed |

Each measured cell executed a real Spark action and reported its runtime. The warm-up action was excluded from the summaries. Sequential runs may still have benefited from operating-system or filesystem caching.

## 3. Benchmark Design

### 3.1 Representation Experiments

Each of CSV, JSON, Parquet, and ORC was written using the baseline layout and then read with the same explicit six-column schema. Three workloads were measured:

- **W1 — broad aggregation:** group by carrier and calculate average arrival delay, average departure delay, average distance, and count.
- **W2 — selective aggregation:** filter to `YEAR = 2015` and `MONTH = 1`, then perform the W1 aggregation.
- **W3 — narrow projection:** read only carrier and arrival delay, then calculate count, minimum carrier, maximum carrier, and average arrival delay.

### 3.2 Layout Experiments

The layout experiment used Parquet and W1. It compared a baseline written after `repartition(4)`, a many-file layout written after `repartition(20)`, and a few-file layout written after `coalesce(2)`.

These are execution partitions: they affect how Spark distributes data when writing. They are different from directory partitions, which were created separately with `partitionBy("YEAR", "MONTH")` for the W2 pruning probe.

### 3.3 Read-Behavior Evidence

Formatted physical plans were saved for three separate Parquet probes:

1. W2 over Parquet partitioned by `YEAR` and `MONTH`, to inspect `PartitionFilters`.
2. A selective `ARRIVAL_DELAY > 60` query, to inspect `PushedFilters`.
3. W3 over baseline Parquet, to inspect `ReadSchema` and the columns read by the scan.

## 4. Results

### 4.1 Representation Results

The table reports the median runtime across five measured runs. Variability is shown as the minimum–maximum range and standard deviation, all in seconds.

| Format | W1 median | W1 min–max | W1 SD | W2 median | W2 min–max | W2 SD | W3 median | W3 min–max | W3 SD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CSV | 3.573 | 3.457–4.107 | 0.260 | 3.053 | 2.995–3.380 | 0.170 | 3.599 | 2.872–5.626 | 1.082 |
| JSON | 6.970 | 6.739–12.474 | 2.684 | 5.816 | 5.334–6.951 | 0.661 | 6.032 | 5.593–10.507 | 2.317 |
| Parquet | 0.461 | 0.409–0.765 | 0.143 | 0.306 | 0.176–0.514 | 0.132 | 0.750 | 0.597–1.337 | 0.286 |
| ORC | 0.759 | 0.663–0.930 | 0.106 | 0.255 | 0.218–0.422 | 0.080 | 0.733 | 0.621–1.221 | 0.238 |

Under this configuration, Parquet had the lowest median for W1, while ORC had the lowest median for W2 and W3. JSON was slower than the other formats in all three workloads and had the largest standard deviation for W1 and W3. Parquet's W3 median was slightly higher than ORC's, despite being lower for W1 and W2.

### 4.2 Representation Storage Results

The generated representation datasets each contained five data files. Sizes below are rounded to MiB; the codec is included because compression was part of the tested representation.

| Format | Codec | Total bytes | Approx. size | Data files | File-size range |
| --- | --- | ---: | ---: | ---: | ---: |
| CSV | none | 159,958,207 | 152.55 MiB | 5 | 14.61–34.75 MiB |
| JSON | none | 581,327,565 | 554.45 MiB | 5 | 52.05–126.15 MiB |
| Parquet | Snappy | 24,930,817 | 23.78 MiB | 5 | 2.19–5.46 MiB |
| ORC | Zlib | 32,840,782 | 31.32 MiB | 5 | 2.94–7.20 MiB |

The row-oriented CSV and JSON outputs were substantially larger than the compressed columnar outputs. JSON was the largest representation in this run. The runtime comparison therefore reflects both storage format and the tested codec choices; it does not isolate the format structure from compression.

### 4.3 Layout Results

The layout summary contains a separate Parquet experiment, so these W1 medians should not be substituted for the representation table's baseline-Parquet medians.

| Layout | Data files / write partitions | Median runtime (s) | Min–max (s) | SD (s) |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 4 / `repartition(4)` | 0.566 | 0.477–1.170 | 0.284 |
| Many files | 20 / `repartition(20)` | 0.722 | 0.525–0.922 | 0.149 |
| Few files | 2 / `coalesce(2)` | 0.758 | 0.442–0.822 | 0.172 |

The baseline had the lowest median W1 runtime among the three tested layouts. The many-file layout produced 20 files and the few-file layout produced 2 files; their total output sizes were similar to the baseline (27,680,682, 25,164,456, and 27,820,410 bytes respectively), so the runtime differences were not simply a consequence of a large size difference.

### 4.4 Partition Pruning, Predicate Pushdown and Column Pruning

The physical-plan evidence reports the following:

| Probe | Evidence | Interpretation |
| --- | --- | --- |
| Partitioned Parquet W2 | `PartitionFilters: [isnotnull(YEAR#5364), isnotnull(MONTH#5365), (YEAR#5364 = 2015), (MONTH#5365 = 1)]` | The scan plan contains filters on both directory-partition columns, consistent with partition pruning for the requested year and month. |
| `ARRIVAL_DELAY > 60` | `PushedFilters: [IsNotNull(ARRIVAL_DELAY), GreaterThan(ARRIVAL_DELAY,60.0)]` | The Parquet scan reports the selective predicate as pushed to the data source. |
| W3 Parquet | `ReadSchema: struct<AIRLINE:string,ARRIVAL_DELAY:double>` | The scan reads only the two logical columns required by W3. |

The W2 partition-pruning plan has a `ReadSchema` containing the non-partition data columns while `YEAR` and `MONTH` appear in `PartitionFilters`. The plan evidence, rather than the `partitionBy()` call alone, is the basis for the pruning conclusion.

## 5. Discussion

### 5.1 Representation

The measured medians show a consistent separation between the text formats and the columnar formats for W1 and W2. For example, Parquet's W1 median was 0.461 seconds, compared with 3.573 seconds for CSV and 6.970 seconds for JSON under the same measured configuration. For W2, ORC recorded 0.255 seconds and Parquet 0.306 seconds, while CSV and JSON recorded 3.053 and 5.816 seconds.

These observations are consistent with Spark reading compressed columnar data more efficiently for these aggregations, especially when the scan can use column-oriented storage. However, the experiment used Snappy for Parquet, Zlib for ORC, and no compression for CSV or JSON. The results therefore measure the combined effect of format, encoding, and codec. They should be treated as results for this tested configuration, not as a universal ordering of formats.

W3 narrows the difference between the two columnar formats: ORC's median was 0.733 seconds and Parquet's was 0.750 seconds. The result shows that the narrow workload did not produce a lower Parquet median than ORC in this run, even though the physical plan confirms that Parquet read only the requested columns.

### 5.2 Layout

The four-file Parquet baseline had a lower W1 median than both alternatives. Increasing the write partition count to 20 produced more, smaller files and increased the median from 0.566 to 0.722 seconds. This is consistent with additional file and task overhead in the tested local run. The two-file `coalesce(2)` layout was also slower by median at 0.758 seconds, so the measurements do not show a monotonic relationship in which fewer files always improve runtime.

The result supports a narrower conclusion: for this dataset, machine, and W1 workload, the tested four-file layout had the lowest median. Excessive small-file output can add overhead, but the few-file result shows that reducing file count alone is not sufficient to predict the fastest layout.

### 5.3 Read Behavior

The plans distinguish query intent from the scan behavior Spark selected. The W2 query requested a year/month filter, and the partitioned Parquet scan reported matching `PartitionFilters`, providing direct evidence of directory-partition pruning. The separate arrival-delay probe reported `PushedFilters`, showing that the predicate was passed to the Parquet scan. Finally, the W3 Parquet scan reported a `ReadSchema` containing only `AIRLINE` and `ARRIVAL_DELAY`, which is evidence of column projection at the data-source scan.

## 6. Limitations

- The experiment used one dataset and a single-machine local Spark environment (`local[2]`).
- Only five measured runs were used for each cell, so individual runtimes can vary.
- Spark catalog caching was cleared and benchmark DataFrames were not persisted, but operating-system/filesystem caching was not flushed between sequential runs.
- Compression choices differed by format, so format structure and codec effects are not separated.
- The workload matrix was limited to the three defined workloads and the tested Parquet layouts.
- Results may change with dataset size, hardware, Spark settings, codecs, or file-system behavior.

## 7. Conclusion

For the tested Airline dataset and local Spark configuration, Parquet and ORC had substantially lower median runtimes than CSV and JSON for the broad and selective aggregations. ORC had the lowest median for W2 and W3, while Parquet had the lowest median for W1. The storage results also showed much smaller compressed columnar outputs than the uncompressed text outputs, although codec choices were part of the comparison.

For the separate Parquet layout experiment, the four-file baseline had the lowest median W1 runtime. Both the 20-file and two-file layouts were slower by median, so the evidence supports considering file count and task overhead together rather than treating fewer files as automatically faster. The physical plans directly showed `PartitionFilters` for the partitioned W2 query, `PushedFilters` for `ARRIVAL_DELAY > 60`, and a W3 `ReadSchema` containing only the required carrier and arrival-delay columns. These observations support workload-specific use of columnar storage and selective-read features, subject to the experiment's environment and compression choices.
