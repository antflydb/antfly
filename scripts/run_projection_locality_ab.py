"""Matched source-ownership qualification with explicit vector encoding.

Use one pinned binary, fresh roots, and alternating A/B then B/A ordering.
The 1M arms run only after every 50K arm passes the public API qualification.
This runner deliberately uses the archived qualification harness to avoid
changing measurement code underneath other experiments in this worktree.
"""

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

from projection_locality_inputs import input_paths, receipt_passed


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--port", type=int, default=19440)
    parser.add_argument("--health-port", type=int, default=19441)
    parser.add_argument("--pairs", type=int, default=2)
    parser.add_argument("--include-1m", action="store_true")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue incomplete pairs without rerunning qualified fresh roots.",
    )
    parser.add_argument("--encoding", choices=["float16", "float32"], default="float16")
    parser.add_argument("--memory-budget-mb", type=int)
    parser.add_argument("--query-seconds", type=int, default=30)
    parser.add_argument("--mixed-seconds", type=int, default=30)
    parser.add_argument("--profile-count", type=int, default=1000)
    args = parser.parse_args()
    if args.pairs < 2:
        parser.error("use at least two pairs to reverse run order")
    binary = args.binary.resolve(strict=True)
    scripts = Path(__file__).resolve().parent
    harness = scripts / "run_vdbbench_qualification_snapshot_20260906.sh"
    # These helpers are read by the harness at runtime; detect edits instead
    # of silently combining different measurement implementations.
    inputs = input_paths(binary, Path(__file__).resolve())
    expected = {str(path): digest(path) for path in sorted(inputs)}
    environment = os.environ.copy()
    # Source refinements are a separate experiment. Do not accidentally
    # inherit a refinement or routing/admission treatment from the shell.
    for key in list(environment):
        if key.startswith(("ANTFLY_", "VDBBENCH_")):
            del environment[key]
    environment["ANTFLY_BIN"] = str(binary)
    environment["ANTFLY_VDBBENCH_SYNC_LEVEL"] = "write"
    # This measures source ownership, not posting locality. Keep the original
    # common layout explicit even when the product's fresh-build default changes.
    environment["ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS"] = "1"
    args.root.mkdir(parents=True, exist_ok=args.resume)
    root = args.root.resolve()
    receipts = json.loads((root / "ab-runs.json").read_text()) if args.resume else []

    def save():
        (root / "ab-runs.json").write_text(json.dumps(receipts, indent=2) + "\n")

    def verify():
        for name, checksum in expected.items():
            if digest(Path(name)) != checksum:
                raise RuntimeError(f"qualification input changed: {name}")

    cases = ["Performance1536D50K"]
    if args.include_1m:
        cases.append("Performance768D1M")
    for case in cases:
        for pair in range(args.pairs):
            modes = ["primary_lsm", "vector_store"]
            if pair % 2:
                modes.reverse()
            for mode in modes:
                verify()
                arm = root / f"{case}-{pair + 1}-{mode}"
                command = [
                    str(harness),
                    str(arm),
                    str(args.port),
                    str(args.health_port),
                    "--case",
                    case,
                    "--dense-embeddings",
                    mode,
                    "--native-hbc",
                    "--vector-blocks",
                    "--vector-block-encoding",
                    args.encoding,
                    "--batch",
                    "100",
                    "--workers",
                    "4",
                    "--query-concurrency",
                    "1,10,20,30",
                    "--query-seconds",
                    str(args.query_seconds),
                    "--mixed-seconds",
                    str(args.mixed_seconds),
                    "--profile-count",
                    str(args.profile_count),
                ]
                if args.memory_budget_mb is not None:
                    command.extend(["--memory-budget-mb", str(args.memory_budget_mb)])
                previous = next(
                    (
                        r
                        for r in receipts
                        if r["case"] == case
                        and r["pair"] == pair + 1
                        and r["mode"] == mode
                    ),
                    None,
                )
                if previous is not None:
                    if (
                        previous["command"] != command
                        or previous["inputs_sha256"].get(str(binary))
                        != expected[str(binary)]
                        or not receipt_passed(root, previous)
                    ):
                        raise RuntimeError(
                            f"cannot resume failed, changed, or unaudited arm: {arm.name}"
                        )
                    continue
                receipt = {
                    "case": case,
                    "pair": pair + 1,
                    "mode": mode,
                    "encoding": args.encoding,
                    "command": command,
                    "started_at": time.time(),
                    "inputs_sha256": expected,
                    "environment": {
                        key: value
                        for key, value in environment.items()
                        if key.startswith(("ANTFLY_", "VDBBENCH_"))
                    },
                }
                receipts.append(receipt)
                save()
                print(f"Starting {arm.name}", flush=True)
                with (root / f"{arm.name}.log").open("w") as log:
                    result = subprocess.run(
                        command,
                        env=environment,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=False,
                    )
                receipt.update(exit_code=result.returncode, finished_at=time.time())
                try:
                    verify()
                except RuntimeError as error:
                    receipt["invalid_reason"] = str(error)
                    save()
                    raise
                save()
                if result.returncode:
                    raise RuntimeError(f"{arm.name} failed; later arms are gated")
                print(f"Passed {arm.name}", flush=True)


if __name__ == "__main__":
    main()
