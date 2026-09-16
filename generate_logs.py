#!/usr/bin/env python3
"""
generate_logs.py - High-Volume Logistics Big Data Generator
Generates:
  1. data/ip_geolocation.csv: Master CIDR subnet rules
  2. data/raw_logs.txt: High-volume unstructured server access logs (default 1,000,000 records, ~105 MB)
"""

import os
import random
import csv
import time
import argparse
import ipaddress
from datetime import datetime, timedelta

CIDR_RULES = [
    ("103.21.240.0/20", "VN", "Vietnam"),
    ("103.7.32.0/19", "SG", "Singapore"),
    ("192.168.0.0/16", "US", "United States"),
    ("198.51.100.0/24", "US", "United States"),
    ("203.0.112.0/20", "JP", "Japan"),
    ("185.220.96.0/19", "DE", "Germany"),
    ("51.15.0.0/16", "FR", "France"),
    ("151.101.0.0/16", "GB", "United Kingdom"),
    ("210.140.0.0/16", "KR", "South Korea"),
    ("139.130.0.0/16", "AU", "Australia"),
    ("145.220.0.0/16", "NL", "Netherlands"),
    ("200.160.0.0/16", "BR", "Brazil"),
    ("106.51.0.0/16", "IN", "India"),
    ("116.206.0.0/16", "TW", "Taiwan"),
    ("194.67.192.0/18", "SE", "Sweden"),
]

LOGISTICS_ENDPOINTS = [
    ("GET", "/api/v1/shipments/track?tx_id=TXN-{}", 200, 0.45),
    ("POST", "/api/v1/tracking/update?tx_id=TXN-{}", 200, 0.25),
    ("GET", "/api/v1/manifests/wh-{}-manifest", 200, 0.10),
    ("POST", "/api/v1/dispatch/schedule?order_id=ORD-{}", 200, 0.08),
    ("GET", "/api/v1/customs/clearance?entry_no=CUST-{}", 200, 0.05),
    ("GET", "/api/v1/rates/calculator", 200, 0.03),
    ("GET", "/api/v1/shipments/track?tx_id=INVALID-{}", 404, 0.02),
    ("POST", "/api/v1/orders/create", 500, 0.01),
    ("POST", "/api/v1/customs/declaration", 400, 0.01),
]


def generate_master_lookup(output_path: str):
    """Generate master CIDR geolocation lookup CSV file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cidr_block", "country_code", "country_name"])
        for cidr, code, name in CIDR_RULES:
            writer.writerow([cidr, code, name])


def generate_raw_logs(output_path: str, num_lines: int = 1000000, corrupt_rate: float = 0.014):
    """Fast buffered generation of 1M+ realistic logistics access logs."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    t0 = time.perf_counter()

    # Precompute host IPs per subnet
    subnet_pools = []
    for cidr, _, _ in CIDR_RULES:
        net = ipaddress.IPv4Network(cidr)
        base = int(net.network_address)
        ips = [str(ipaddress.IPv4Address(base + off)) for off in range(1, min(net.num_addresses - 2, 250))]
        subnet_pools.append(ips)

    methods, endpoints, statuses, weights = zip(*LOGISTICS_ENDPOINTS)

    CHUNK_SIZE = 50000
    corrupt_count = 0
    total_written = 0

    with open(output_path, "w", encoding="utf-8", buffering=1024 * 1024) as f:
        for chunk_idx in range(0, num_lines, CHUNK_SIZE):
            current_chunk = min(CHUNK_SIZE, num_lines - chunk_idx)
            buffer = []
            for i in range(current_chunk):
                # 1.4% chance of corrupt row
                if random.random() < corrupt_rate:
                    buffer.append(f"CORRUPTED_PACKET_HEADER_0x{random.randint(1000, 9999):X}_ERR\n")
                    corrupt_count += 1
                    continue

                pool = random.choice(subnet_pools)
                ip = random.choice(pool)
                idx = random.choices(range(len(endpoints)), weights=weights)[0]
                m, ep, st = methods[idx], endpoints[idx], statuses[idx]
                req = ep.format(random.randint(10000, 99999))
                line = f'{ip} - - [16/Sep/2026:10:00:00 +0000] "{m} {req} HTTP/1.1" {st} {random.randint(128, 4096)} {random.randint(10, 200)}ms'

                # 3.5% chance of compound/batch request (semicolon-delimited)
                if random.random() < 0.035:
                    second_ip = random.choice(random.choice(subnet_pools))
                    line += f'; {second_ip} - - [16/Sep/2026:10:00:00 +0000] "GET /api/v1/shipments/track?tx_id=TXN-{random.randint(10000, 99999)} HTTP/1.1" 200 512 15ms'

                buffer.append(line + "\n")

            f.writelines(buffer)
            total_written += current_chunk

    elapsed = time.perf_counter() - t0
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Generated {total_written:,} logs -> {output_path} ({file_size_mb:.2f} MB in {elapsed:.2f}s, {corrupt_count:,} corrupt)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Logistics Access Log Generator")
    parser.add_argument("--lines", type=int, default=1000000, help="Number of log records to generate")
    parser.add_argument("--logs-path", default="data/raw_logs.txt", help="Path for generated raw logs")
    parser.add_argument("--lookup-path", default="data/ip_geolocation.csv", help="Path for master CIDR lookup")
    args = parser.parse_args()

    generate_master_lookup(args.lookup_path)
    generate_raw_logs(args.logs_path, num_lines=args.lines)
