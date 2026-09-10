"""Run each source-store refinement and their combination in alternating pairs."""

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

from vector_store_experiment_settings import NEXT, FIRST

worktree = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--binary", type=Path, required=True)
parser.add_argument("--python", default=sys.executable)
parser.add_argument(
    "--root",
    type=Path,
    default=worktree
    / ".benchmark-results"
    / ("vector-refinement-experiments-" + time.strftime("%Y%m%d-%H%M%S")),
)
parser.add_argument("--pairs", type=int, default=2)
parser.add_argument("--suite", choices=["next", "first"], default="next")
parser.add_argument("--port", type=int, default=18118)
args = parser.parse_args()
if args.pairs < 2:
    parser.error("at least two pairs are required for alternating order")
args.root = args.root.absolute()
args.root.mkdir(parents=True, exist_ok=False)
python = args.python
binary = args.binary.resolve(strict=True)
receipt = args.root / "experiments.json"


def binary_digest():
    digest = hashlib.sha256()
    with binary.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


snapshot = args.root / "harness/scripts"
snapshot.mkdir(parents=True)
for source in (worktree / "scripts").glob("*.py"):
    shutil.copy2(source, snapshot / source.name)
results = []
for feature in NEXT if args.suite == "next" else FIRST:
    root = args.root / feature
    command = [
        python,
        str(snapshot / "run_vector_store_enrichment_ab.py"),
        str(root),
        "--binary",
        str(binary),
        "--refinement",
        feature,
        "--pairs",
        str(args.pairs),
        "--port",
        str(args.port),
    ]
    result = {
        "feature": feature,
        "command": command,
        "started_at": time.time(),
        "binary_sha256": binary_digest(),
    }
    results.append(result)
    receipt.write_text(json.dumps(results, indent=2) + "\n")
    with Path(str(root) + ".log").open("w") as log:
        completed = subprocess.run(
            command, cwd=worktree, stdout=log, stderr=subprocess.STDOUT
        )
    result.update(exit_code=completed.returncode, finished_at=time.time())
    receipt.write_text(json.dumps(results, indent=2) + "\n")
    print(feature, completed.returncode, flush=True)
# Try every independent experiment, while preserving each failed gate.
if any(result["exit_code"] for result in results):
    raise SystemExit(
        "one or more comparisons failed qualification; inspect experiments.json"
    )
