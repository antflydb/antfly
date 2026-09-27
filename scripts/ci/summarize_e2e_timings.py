# Copyright 2026 Antfly, Inc.
# Licensed under the Elastic License 2.0 (ELv2).
"""Show fixture costs and the slowest isolation groups in the Actions summary."""

import argparse
import json
from collections import defaultdict
from pathlib import Path

from zig_e2e_shard import canonical_nodeid


def summarize(plan, phases):
    groups = defaultdict(lambda: defaultdict(float))
    totals = defaultdict(float)
    for node, values in phases["tests"].items():
        group = plan["groups"][canonical_nodeid(node)]
        for phase, seconds in values.items():
            groups[group][phase] += seconds
            totals[phase] += seconds
    lines = ["### E2E measured durations", "", "| Phase | Seconds |", "| --- | ---: |"]
    for phase in ("setup", "call", "teardown"):
        lines.append(f"| {phase} | {totals[phase]:.2f} |")
    lines += [
        "",
        "Totals are test phase time; concurrent work can overlap.",
        "",
        "| Slowest isolation groups | Setup | Execution | Teardown |",
        "| --- | ---: | ---: | ---: |",
    ]
    for group, values in sorted(
        groups.items(), key=lambda pair: -sum(pair[1].values())
    )[:15]:
        lines.append(
            f"| `{group}` | {values['setup']:.2f} | {values['call']:.2f} | {values['teardown']:.2f} |"
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", required=True)
    parser.add_argument("--reports", required=True)
    args = parser.parse_args()
    path = Path(args.reports) / "phase-durations.json"
    if path.exists():
        print(
            summarize(
                json.loads(Path(args.plan).read_text()), json.loads(path.read_text())
            )
        )
    else:
        print(
            "No Antfly phase durations available; consult the retained JUnit results and job steps."
        )
