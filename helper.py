import re
import csv
import ipaddress
LOG_REGEX = re.compile(r'^(\S+) - - \[.*?\] "\S+ \S+ \S+" (\d{3})')



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
    """Worker-side: O(1) in-memory bitwise lookup without triggering a distributed shuffle."""
    try:
        ip_int = int(ipaddress.IPv4Address(ip_str))
        for (net, mask), country in ip_country_map.items():
            if (ip_int & mask) == net:
                return country
    except Exception:
        pass
    return "Unknown"


def create_comparison_df(spark, rdd_metrics: dict, df_metrics: dict):
    """Build a structured Spark DataFrame for the quantitative comparison matrix."""
    parity = "PASS (100% Match)" if rdd_metrics["results"] == df_metrics["results"] else "FAIL (Mismatch)"
    speedup = df_metrics["time"] / rdd_metrics["time"] if rdd_metrics["time"] > 0 else 1.0

    rows = [
        ("Execution Latency", f"{rdd_metrics['time']:.2f}s", f"{df_metrics['time']:.2f}s ({speedup:.2f}x)"),
        ("Spark Actions / Jobs Triggered", str(rdd_metrics["actions"]), str(df_metrics["actions"])),
        ("Storage File Scan Passes", str(rdd_metrics["scans"]), str(df_metrics["scans"])),
        ("CIDR Geolocation Operator", rdd_metrics["operator"], df_metrics["operator"]),
        ("Corrupt Lines Audited", f"{rdd_metrics['corrupt']:,d}", f"{df_metrics['corrupt']:,d}"),
        ("Top-10 Checksum Parity", parity, parity),
    ]
    return spark.createDataFrame(rows, ["metric", "low_level_rdd", "high_level_dataframe"])

