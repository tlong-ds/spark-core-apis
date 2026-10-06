# Spark Storage Benchmark

How file format, write layout and selective reads change disk size and query speed. 5,819,079 flight records, Spark 3.5.9 on `local[2]`.

**23.78 MiB**Parquet on disk, 84% smaller than CSV

**7.8× / 15×**W1 speed-up of Parquet over CSV / JSON

**2 → 20 → 16**data files from coalesce, repartition, partitionBy

**1 of 16**files read for `MONTH=1` after pruning

## Method

One raw CSV (`data/raw/flights.csv`) is loaded with a fixed six-column schema, written in each format, then read back and timed. Each cell: 1 warm-up run (discarded) and 5 measured runs; medians reported.

| Setting | Value |
| --- | --- |
| Parallelism / shuffle partitions | 2 / 8, AQE on |
| Caching | Catalog cache cleared per run; OS cache not flushed |

## 1. Disk storage size

Row-oriented text is far larger than compressed columnar output. JSON repeats every field name in every record.

| Format | Codec | Bytes | Files | vs Parquet |
| --- | --- | --- | --- | --- |
| CSV | none | 159,958,207 | 5 | 6.42× |
| JSON | none | 581,327,565 | 5 | 23.32× |
| Parquet | Snappy | 24,930,817 | 5 | 1.00× |
| ORC | Zlib | 32,840,782 | 5 | 1.32× |

## 2. Read query time

Median seconds over 5 runs. W3 is the required `SELECT carrier, avg(arr_delay)`.

CSVJSONParquetORC

- W1: Parquet fastest (0.461 s). CSV is 7.75× and JSON 15.1× slower.
- W2 and W3: ORC leads, but the W3 gap to Parquet (0.017 s) is smaller than the run-to-run SD (0.24–0.29 s), so treat it as a tie.
- JSON has the highest variance: W1 ranges 6.74–12.47 s.

## 3. Write execution time

Write timings were not present in the supplied results. Fill this table from the benchmark log before submission.

| Format | Median (s) | Min–max (s) | SD (s) |
| --- | --- | --- | --- |
| CSV | TBD | TBD | TBD |
| JSON | TBD | TBD | TBD |
| Parquet | TBD | TBD | TBD |
| ORC | TBD | TBD | TBD |

## 4. Partition control experiment

`repartition` and `coalesce` decide how many files are written. `partitionBy` decides the folder structure. Block width below is proportional to file size.

### 4.1 W1 runtime by layout

### 4.2 What lands on disk

**`repartition(20)`** — 20 data files, 158,137,675 B

Round-robin shuffle gives near-equal files (7.53–7.55 MiB). 42 files on disk including 20 `.crc`, `_SUCCESS` and `._SUCCESS.crc`.

**`coalesce(2)`** — 2 data files, 141,923,392 B

No shuffle, so files are large and uneven (74.70 and 60.65 MiB). 6 files on disk.

**`partitionBy("YEAR","MONTH")`** — 16 data files in 12 folders, 142,686,303 B

main filesplit file from a second write task

Months 3, 6, 9 and 11 were split across write tasks and have two files. Bar width is scaled to the largest month.

```
partition_partitionBy_year_month/
├── _SUCCESS
├── YEAR=2015/
│   ├── MONTH=1/   part-00000-….snappy.parquet  (11.6 MB)
│   ├── MONTH=2/   part-00000-….snappy.parquet  (10.8 MB)
│   ├── MONTH=3/   part-00000-… (10.7 MB), part-00001-… (1.8 MB)
│   ├── …
│   └── MONTH=12/  part-00004-….snappy.parquet  (11.7 MB)
```

| Strategy | Data files | Files on disk | Data MiB | Smallest–largest file |
| --- | --- | --- | --- | --- |
| repartition(20) | 20 | 42 | 150.81 | 7.53–7.55 MiB |
| coalesce(2) | 2 | 6 | 135.35 | 60.65–74.70 MiB |
| partitionBy | 16 | 34 | 136.08 | 1.37–12.23 MiB |

- `repartition(20)` data is 11.4% larger than `coalesce(2)`. This is consistent with the shuffle breaking row order, which weakens Parquet's encoding and compression.
- `partitionBy` is about the size of `coalesce(2)` despite 16 files: `YEAR` and `MONTH` live in the folder names, not in the files.
- Fewest files was not fastest: the 4-file baseline (0.566 s) beat both 20 files (0.722 s) and 2 files (0.758 s).

## 5. What Spark skipped

### Partition pruning

The W2 filter `YEAR = 2015 AND MONTH = 1` reads one folder.

```
PartitionFilters: [isnotnull(YEAR#5364), isnotnull(MONTH#5365),
                   (YEAR#5364 = 2015), (MONTH#5365 = 1)]
```

### Predicate pushdown

```
PushedFilters: [IsNotNull(ARRIVAL_DELAY), GreaterThan(ARRIVAL_DELAY,60.0)]
```

### Column pruning

```
ReadSchema: struct<AIRLINE:string,ARRIVAL_DELAY:double>
```

## Conclusion

- **Size:** Parquet 23.78 MiB \< ORC 31.32 \< CSV 152.55 \< JSON 554.45.
- **Read speed:** Parquet and ORC are 4–15× faster than text formats; Parquet wins W1, ORC wins W2 and W3 by margins within noise.
- **Layout:** balance file count against 2 local cores; neither 2 nor 20 files beat the 4-file baseline.
- **Partitioning:** `partitionBy` turns a filter into a folder lookup, at the cost of more files and occasional small split files.

### Limitations

- One dataset, one machine, 5 runs per cell; OS cache not flushed.
- Codecs differ per format, so format and compression effects are combined.
- Write times missing (section 3); `partitionBy` was not timed on W1.