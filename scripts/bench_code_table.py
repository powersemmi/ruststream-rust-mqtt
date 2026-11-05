#!/usr/bin/env python3
"""Print the per-message cost of a code-cost benchmark run.

Reads the summary `cargo bench -- --output-format=json` writes for the benches under
`crates/ruststream-rust-mqtt-bench/benches` (one JSON object per benchmark, gungraun's summary
layout 7), and prints instructions, allocations and allocated bytes per message for each scenario
by the core's method: every scenario is measured over one delivery, over MESSAGES and over twice
MESSAGES, the slope between the last two is the steady state and the one-delivery run is the cold
start. A publish is the reply scenario less the consume scenario.

    python3 scripts/bench_code_table.py --messages 500 target/bench-code.json
"""

import argparse
import json
import sys
from pathlib import Path

SUMMARY_VERSION = "7"
METRICS = (("Callgrind", "Ir", "instructions"), ("DHAT", "TotalBlocks", "allocations"),
           ("DHAT", "TotalBytes", "bytes"))


def metric(summary: dict, tool: str, name: str) -> int:
    for profile in summary["profiles"]:
        if profile["tool"] == tool:
            values = profile["data"]["total"]["metrics"][name]["values"]
            return int(values.get("new", values.get("old")))
    sys.exit(f"{summary['id']} carries no {tool} profile")


def runs(path: Path) -> dict[str, dict[str, int]]:
    found = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        summary = json.loads(line)
        if summary.get("version") != SUMMARY_VERSION:
            sys.exit(f"summary layout {summary.get('version')}, this script reads {SUMMARY_VERSION}")
        key = f"{Path(summary['benchmark_file']).stem}/{summary['id']}"
        found[key] = {name: metric(summary, tool, raw) for tool, raw, name in METRICS}
    return found


def slope(found: dict, scenario: str, messages: int) -> dict[str, float]:
    base, twice = found[f"{scenario}/base"], found[f"{scenario}/twice"]
    return {name: (twice[name] - base[name]) / messages for _, _, name in METRICS}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("summary", type=Path)
    parser.add_argument("--messages", type=int, default=500)
    args = parser.parse_args()
    found = runs(args.summary)
    consume = slope(found, "consume", args.messages)
    reply = slope(found, "reply", args.messages)
    rows = {
        "delivery (consume, QoS 1, ack)": consume,
        "reply (delivery + publish)": reply,
        "publish (reply - consume)": {name: reply[name] - consume[name] for name in consume},
    }
    print(f"{'per message':34} {'instructions':>14} {'allocations':>12} {'bytes':>10}")
    for label, row in rows.items():
        print(f"{label:34} {row['instructions']:14.1f} {row['allocations']:12.2f} {row['bytes']:10.1f}")
    print()
    for scenario in ("consume", "reply"):
        first = found[f"{scenario}/first"]
        print(f"cold start, {scenario}: {first['instructions']} instructions, "
              f"{first['allocations']} allocations, {first['bytes']} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
