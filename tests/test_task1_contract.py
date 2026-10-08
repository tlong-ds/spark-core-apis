from pathlib import Path
import ipaddress

import pytest

from helper import parse_log_entry


def log(ip, status=200):
    return (
        f'{ip} - - [16/Sep/2026:10:00:00 +0000] '
        f'"GET /api/v1/shipments/track?tx_id=TXN-1 HTTP/1.1" '
        f'{status} 512 15ms'
    )


def test_parser_returns_a_visible_malformed_reason():
    valid = parse_log_entry(log("192.168.0.1"))
    malformed = parse_log_entry("CORRUPTED_PACKET_HEADER_0xD50_ERR")

    assert (valid.state, valid.http_status, valid.reason) == ("valid", 200, None)
    assert (malformed.state, malformed.reason) == ("malformed", "invalid_format")


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName("task1-contract-test").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_all_pipelines_share_the_same_contract(tmp_path: Path, spark):
    from rdd_processing import execute_dataframe_job, execute_hybrid_job, execute_rdd_job

    source = tmp_path / "logs.txt"
    source.write_text(
        "\n".join([
            log("192.168.0.1"),
            "CORRUPTED_PACKET_HEADER_0xD50_ERR",
            log("8.8.8.8"),
            f"{log('103.21.240.1')}; {log('192.168.0.2', 500)}",
        ]) + "\n",
        encoding="utf-8",
    )
    us = ipaddress.IPv4Network("192.168.0.0/16")
    vn = ipaddress.IPv4Network("103.21.240.0/20")
    lookup = {
        (int(us.network_address), int(us.netmask)): "United States",
        (int(vn.network_address), int(vn.netmask)): "Vietnam",
    }

    rdd = execute_rdd_job(spark.sparkContext, spark, str(source), lookup)
    dataframe = execute_dataframe_job(spark, str(source), lookup)
    hybrid = execute_hybrid_job(spark.sparkContext, spark, str(source), lookup)

    expected_counts = {"valid": 3, "malformed": 1, "unknown": 1, "http_200": 3}
    expected_top = {"United States": 1, "Vietnam": 1}
    for result in (rdd, dataframe, hybrid):
        assert result["semantic_counts"] == expected_counts
        assert result["results"] == expected_top
