"""Screen native treatments at 50K before qualifying passing experiments at 1M.

Uses fresh public-API arms, batch 100, reversed pairs, and one pinned binary.
Does not remove databases, modify defaults, or select winners automatically.
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from native_preparation_experiments import REFINEMENTS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--experiments", nargs="+", choices=list(REFINEMENTS), default=list(REFINEMENTS)
    )
    parser.add_argument("--include-1m", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--port", type=int, default=19560)
    parser.add_argument("--health-port", type=int, default=19561)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    args.root.mkdir(parents=True, exist_ok=args.resume)
    root = args.root.resolve()
    runner = Path(__file__).resolve().with_name("run_posting_locality_ab.py")
    outcomes_path = root / "matrix-runs.json"
    outcomes = (
        json.loads(outcomes_path.read_text())
        if args.resume and outcomes_path.exists()
        else []
    )
    passed = []
    for large in (False, True) if args.include_1m else (False,):
        experiments = passed.copy() if large else args.experiments
        for name in experiments:
            command = [
                sys.executable,
                str(runner),
                str(root / name),
                "--binary",
                str(binary),
                "--refinement",
                name,
                "--sample-process",
                "--mixed-write-rows-per-second",
                "1000",
                "--port",
                str(args.port),
                "--health-port",
                str(args.health_port),
            ]
            # Isolate added rebasing from reader staging, and certificate
            # pruning from the pre-existing balanced physical subgroup layout.
            if name == "staged_rebase":
                command += ["--common-refinement", "staged_readers"]
            if name in (
                "deferred_capture",
                "coalesced_deletes",
                "reused_delete_vectors",
                "dense_delete_plan",
            ):
                command += ["--common-refinement", "staged_readers", "--capture-stages"]
            if name == "certified_subgroups":
                command += ["--common-refinement", "subgroups_4"]
            if large:
                command.append("--include-1m")
            if large or (args.resume and (root / name / "ab-runs.json").exists()):
                command.append("--resume")
            row = {
                "experiment": name,
                "through_1m": large,
                "command": command,
                "started_at": time.time(),
            }
            outcomes.append(row)
            outcomes_path.write_text(json.dumps(outcomes, indent=2) + "\n")
            result = subprocess.run(command, check=False)
            row.update(exit_code=result.returncode, finished_at=time.time())
            outcomes_path.write_text(json.dumps(outcomes, indent=2) + "\n")
            if result.returncode == 0 and not large:
                passed.append(name)
    if any(row["exit_code"] != 0 for row in outcomes if "exit_code" in row):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
