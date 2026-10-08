import re
import csv
import ipaddress
from dataclasses import dataclass


# A full-record contract is deliberately stricter than the old Task-1 regex.
# Every implementation must classify input according to this exact contract.
LOG_REGEX = re.compile(
    r'^(?P<ip>\S+) - - \[(?P<timestamp>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<url>\S+) HTTP/(?P<protocol>\S+)" '
    r'(?P<status>\d{3}) (?P<response_bytes>\d+) (?P<latency_ms>\d+)ms$'
)
# Spark's Java regex engine does not support Python's ``(?P<name>...)`` groups.
SPARK_LOG_PATTERN = (
    r'^(\S+) - - \[([^\]]+)\] "(\S+) (\S+) HTTP/(\S+)" '
    r'(\d{3}) (\d+) (\d+)ms$'
)


@dataclass(frozen=True)
class ParsedLog:
    """Shared parser result used by the RDD and Hybrid paths."""

    state: str
    reason: str | None
    raw: str
    ip: str | None = None
    http_status: int | None = None


def parse_log_entry(entry: str) -> ParsedLog:
    """Parse one already-expanded entry without silently discarding failures."""
    raw = entry.strip()
    match = LOG_REGEX.match(raw)
    if not match:
        return ParsedLog("malformed", "invalid_format", raw)
    return ParsedLog(
        "valid", None, raw, match.group("ip"), int(match.group("status"))
    )



def load_ip_country_map(csv_path: str) -> dict:
    """Driver-side: Load master IP geolocation rules into an IP-to-Country dictionary."""
    ip_country_map = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            net = ipaddress.IPv4Network(row["cidr_block"])
            # Map (network_int, netmask_int) -> country_name
            ip_country_map[(int(net.network_address), int(net.netmask))] = row["country_name"]
    return ip_country_map


def resolve_ip(ip_str: str, ip_country_map: dict) -> str:
    """Worker-side in-memory bitwise lookup without a distributed shuffle.

    The current implementation scans the CIDR rules, so lookup is O(R) for R
    rules.  R is 15 in this assignment; use a prefix trie or range index only
    when the lookup table becomes materially larger.
    """
    try:
        ip_int = int(ipaddress.IPv4Address(ip_str))
        for (net, mask), country in ip_country_map.items():
            if (ip_int & mask) == net:
                return country
    except Exception:
        pass
    return "Unknown"


def create_comparison_df(spark, rdd_metrics: dict, df_metrics: dict, hybrid_metrics: dict = None):
    """Build an exportable comparison matrix from the semantic result contract.

    Accumulator values are intentionally not included: they are telemetry and
    may over-count after task retries or recomputation.
    """
    metrics = [("Execution latency", lambda item: f"{item['time']:.2f}s"),
               ("Semantic counts", lambda item: str(item["semantic_counts"])),
               ("Top-10 parity", lambda item: "PASS" if item["results"] == rdd_metrics["results"] else "FAIL"),
               ("Spark actions", lambda item: str(len(item["execution_trace"]))),
               ("CIDR operator", lambda item: item["operator"])]
    result_sets = [("low_level_rdd", rdd_metrics), ("high_level_dataframe", df_metrics)]
    if hybrid_metrics is not None:
        result_sets.append(("hybrid_rdd_dataframe", hybrid_metrics))
    columns = ["metric"] + [name for name, _ in result_sets]
    rows = [(metric,) + tuple(render(result) for _, result in result_sets) for metric, render in metrics]
    return spark.createDataFrame(rows, columns)
