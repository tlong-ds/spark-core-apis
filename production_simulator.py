"""CLI for deterministic Task-1 production-behaviour simulations."""

import argparse
import json
import sys

from simulator.runner import run_scenario
from simulator.scenarios import SCENARIOS, get_scenario


def main() -> int:
    parser = argparse.ArgumentParser(description="Controlled Task-1 Spark production-behaviour simulator")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), required=True)
    parser.add_argument("--lookup-path", default="data/ip_geolocation.csv")
    parser.add_argument("--output-root", default="runs")
    parser.add_argument("--master", default="local[2]", help="Default is local-only; do not use it as executor-loss proof")
    parser.add_argument(
        "--retry-observation-seconds",
        type=int,
        default=None,
        help="Hold retry attempt 1 active for this many seconds so it is visible in the live Spark UI",
    )
    args = parser.parse_args()

    run_dir, report = run_scenario(
        get_scenario(args.scenario),
        args.lookup_path,
        args.output_root,
        args.master,
        args.retry_observation_seconds,
    )
    print(f"Evidence: {run_dir}")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] in {"PASS", "EXPECTED_FAILURE"} else 1


if __name__ == "__main__":
    sys.exit(main())
