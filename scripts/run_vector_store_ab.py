#!/usr/bin/env python3
"""Run sequential fresh-table source-storage comparisons in alternating order.

Each arm runs the existing public VectorDBBench load, query, mixed-update,
restart, recall, footprint and disk qualification. 1M is gated on all 50K arms
passing. Use the same binary throughout; alternate repeated arms to expose host-load variability.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from vector_store_experiment_settings import ALL_FLAGS, TREATMENTS, configure
from vector_store_capacity import observe as observe_capacity, require_capacity
from vector_store_qualification_errors import inspect_workload_errors


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def verify_50k_gate(root, binary_hash, refinement, pairs, environment, settings):
    receipt_path = root / "ab-runs.json"
    receipts = json.loads(receipt_path.read_text())
    arms = [r for r in receipts if r["case"] == "Performance1536D50K"]
    labels = ["control", "candidate"] if refinement else ["primary_lsm", "vector_store"]
    expected_order = [(pair + 1, mode) for pair in range(pairs)
                      for mode in (labels if pair % 2 == 0 else list(reversed(labels)))]
    if [(r["pair"], r["mode"]) for r in arms] != expected_order:
        raise ValueError("50K gate must contain complete alternating pairs")
    for arm in arms:
        if (arm.get("exit_code") != 0 or arm.get("invalid_reason")
                or arm.get("resource_sampler_exit_code", 0) != 0
                or arm["binary_sha256"] != binary_hash or arm.get("refinement") != refinement):
            raise ValueError("50K gate failed or used different binary/treatment")
        expected_environment = configure(environment, refinement, arm["mode"] == "candidate") if refinement else environment
        if refinement and arm["refinement_environment"] != {k: expected_environment.get(k) for k in ALL_FLAGS}:
            raise ValueError("50K experiment settings do not match")
        for flag, value in settings.items():
            command = arm["command"]
            if flag not in command or command[command.index(flag) + 1] != str(value):
                raise ValueError("50K workload setting does not match: " + flag)
        arm_root = root / f"Performance1536D50K-{arm['pair']}-{arm['mode']}"
        if not inspect_workload_errors(arm_root)["qualified"]:
            raise ValueError("50K workload logs contain errors/retries or are missing")
        if not (arm_root / "qualification-summary.json").is_file():
            raise ValueError("50K qualification summary is missing")
        if not json.loads((arm_root / "source-enrichment.json").read_text()).get("qualified"):
            raise ValueError("50K enrichment lifecycle gate failed")
    return {"root": str(root.resolve()), "receipt_sha256": digest(receipt_path),
            "binary_sha256": binary_hash, "refinement": refinement, "completed_arms": len(arms)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--port", type=int, default=18088)
    parser.add_argument("--health-port", type=int, default=18089)
    parser.add_argument("--pairs", type=int, default=2)
    parser.add_argument("--include-1m", action="store_true")
    parser.add_argument("--only-1m", action="store_true", help="Reuse a matching completed 50K gate before running 1M pairs.")
    parser.add_argument("--qualified-50k-root", type=Path)
    parser.add_argument("--refinement", choices=list(TREATMENTS), help="Compare all source-store refinements against their controls on vector_store tables.")
    parser.add_argument("--query-seconds", type=int, default=30)
    parser.add_argument("--mixed-seconds", type=int, default=30)
    parser.add_argument("--profile-count", type=int, default=1000)
    parser.add_argument("--memory-budget-mb", type=int, default=4096)
    parser.add_argument("--vdbbench-root", type=Path)
    parser.add_argument("--vdbbench-python", type=Path)
    parser.add_argument("--after-arm-hook", type=Path, help="Run a pinned Python lifecycle hook after each successful arm and before starting the next.")
    args = parser.parse_args()
    if args.pairs < 2:
        parser.error("at least two pairs are needed to alternate run order")
    if args.only_1m and (args.include_1m or not args.qualified_50k_root):
        parser.error("--only-1m requires --qualified-50k-root and excludes --include-1m")
    if args.qualified_50k_root and not args.only_1m:
        parser.error("--qualified-50k-root requires --only-1m")
    binary = args.binary.resolve(strict=True)
    expected_hash = digest(binary)
    environment = os.environ.copy()
    environment["ANTFLY_BIN"] = str(binary)
    environment["ANTFLY_VDBBENCH_SYNC_LEVEL"] = "write"
    environment["ANTFLY_BENCH_METRICS"] = "1"
    gate = verify_50k_gate(args.qualified_50k_root, expected_hash, args.refinement,
                          args.pairs, environment, {
                              "--batch": 100, "--workers": 4, "--query-concurrency": "1,10,20,30",
                              "--query-seconds": args.query_seconds, "--mixed-seconds": args.mixed_seconds,
                              "--profile-count": args.profile_count, "--memory-budget-mb": args.memory_budget_mb,
                              "--vector-block-encoding": "float32",
                          }) if args.only_1m else None
    args.root.mkdir(parents=True, exist_ok=False)
    if gate is not None:
        (args.root / "qualification-gate.json").write_text(json.dumps(gate, indent=2) + "\n")
    script = Path(__file__).resolve().with_name("run_vdbbench_qualification.sh")
    # Shared worktrees can change while a long qualification is running.
    # Treat changed measurement code or executable as an invalid arm.
    measurement_scripts = sorted(
        {script, Path(__file__).resolve()}
        | set(script.parent.glob("*vdbbench*.py"))
        | set(script.parent.glob("*vector_store*.py"))
        | {script.with_name("prepare_vdbbench_vector_source.py")}
        | {script.with_name("sample_macos_process_memory.py")}
    )
    expected_scripts = {str(path): digest(path) for path in measurement_scripts}

    def verify_inputs():
        if digest(binary) != expected_hash:
            raise RuntimeError("binary changed during experiment")
        for path in measurement_scripts:
            if digest(path) != expected_scripts[str(path)]:
                raise RuntimeError(
                    f"measurement script changed during experiment: {path}"
                )

    hook = args.after_arm_hook.resolve(strict=True) if args.after_arm_hook else None
    hook_hash = digest(hook) if hook else None
    results = []
    cases = ["Performance768D1M"] if args.only_1m else ["Performance1536D50K"]
    if args.include_1m:
        cases.append("Performance768D1M")
    for case in cases:
        for pair in range(args.pairs):
            labels = ["control", "candidate"] if args.refinement else ["primary_lsm", "vector_store"]
            order = labels if pair % 2 == 0 else list(reversed(labels))
            for mode in order:
                verify_inputs()
                arm_environment = environment.copy()
                table_mode = "vector_store" if args.refinement else mode
                if args.refinement:
                    arm_environment = configure(environment, args.refinement, mode == "candidate")
                run = args.root.resolve() / f"{case}-{pair + 1}-{mode}"
                command = [
                    str(script),
                    str(run),
                    str(args.port),
                    str(args.health_port),
                    "--case",
                    case,
                    "--dense-embeddings",
                    table_mode,
                    "--native-hbc",
                    "--vector-blocks",
                    "--vector-block-encoding",
                    "float32",
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
                    "--memory-budget-mb",
                    str(args.memory_budget_mb),
                ]
                for name in ["vdbbench_root", "vdbbench_python"]:
                    if getattr(args, name):
                        command += [
                            "--" + name.replace("_", "-"),
                            # Resolving a venv's Python symlink selects the
                            # base interpreter and loses its site-packages.
                            str(getattr(args, name).absolute()),
                        ]
                receipt = {
                    "case": case,
                    "pair": pair + 1,
                    "mode": mode,
                    "table_mode": table_mode,
                    "refinement": args.refinement,
                    "refinement_environment": {key: arm_environment.get(key) for key in ALL_FLAGS},
                    "command": command,
                    "binary_sha256": expected_hash,
                    "started_at": time.time(),
                    "measurement_scripts_sha256": expected_scripts,
                    "capacity_preflight": observe_capacity(args.root, case),
                }
                results.append(receipt)
                index = args.root / "ab-runs.json"
                index.write_text(json.dumps(results, indent=2) + "\n")
                try:
                    require_capacity(receipt["capacity_preflight"])
                except RuntimeError as exc:
                    receipt.update(exit_code=1, invalid_reason=str(exc), finished_at=time.time())
                    index.write_text(json.dumps(results, indent=2) + "\n")
                    raise
                with (args.root / f"{run.name}.log").open("w") as log:
                    sampler = None
                    if sys.platform == "darwin":
                        sampler = subprocess.Popen([
                            sys.executable, "-B", str(script.with_name("sample_macos_process_memory.py")),
                            "--pid-file", str(run / "antfly.pid"),
                            "--output", str(args.root / f"{run.name}-resources.jsonl"),
                            "--seconds", "86400", "--interval", "0.5",
                        ], stdout=log, stderr=subprocess.STDOUT)
                    try:
                        completed = subprocess.run(
                            command, env=arm_environment, stdout=log, stderr=subprocess.STDOUT
                        )
                    finally:
                        if sampler is not None:
                            sampler.terminate()
                            sampler.wait(timeout=10)
                            receipt["resource_sampler_exit_code"] = sampler.returncode
                receipt.update(exit_code=completed.returncode, finished_at=time.time())
                if receipt.get("resource_sampler_exit_code", 0) != 0:
                    receipt["invalid_reason"] = "resource sampler failed"
                    index.write_text(json.dumps(results, indent=2) + "\n")
                    raise RuntimeError(f"{run.name}: resource sampler failed")
                try:
                    verify_inputs()
                except RuntimeError as error:
                    receipt["invalid_reason"] = str(error)
                    index.write_text(json.dumps(results, indent=2) + "\n")
                    raise
                index.write_text(json.dumps(results, indent=2) + "\n")
                if completed.returncode:
                    raise RuntimeError(
                        f"{run.name} failed; later arms and scale-up are gated"
                    )
                # The benchmark client retries failed writes and may exit zero
                # after OOM or writer poisoning. Preserve those attempts, but
                # do not promote them into clean A/B or scale qualification.
                workload_errors = inspect_workload_errors(run)
                (run / "workload-errors.json").write_text(
                    json.dumps(workload_errors, indent=2) + "\n"
                )
                receipt["workload_errors"] = workload_errors
                if not workload_errors["qualified"]:
                    receipt["invalid_reason"] = "workload errors/retries or missing logs"
                index.write_text(json.dumps(results, indent=2) + "\n")
                if receipt.get("invalid_reason"):
                    raise RuntimeError(f"{run.name}: workload error gate failed")
                if hook is not None:
                    if digest(hook) != hook_hash:
                        raise RuntimeError("lifecycle hook changed during experiment")
                    hook_command = [sys.executable, "-B", str(hook), str(args.root.resolve()), run.name]
                    receipt["after_arm_hook"] = {"command": hook_command, "sha256": hook_hash}
                    index.write_text(json.dumps(results, indent=2) + "\n")
                    with (args.root / f"{run.name}-lifecycle.log").open("w") as log:
                        followup = subprocess.run(hook_command, stdout=log, stderr=subprocess.STDOUT)
                    receipt["after_arm_hook"]["exit_code"] = followup.returncode
                    if followup.returncode or digest(hook) != hook_hash:
                        receipt["invalid_reason"] = "lifecycle hook failed or changed"
                    index.write_text(json.dumps(results, indent=2) + "\n")
                    if receipt.get("invalid_reason"):
                        raise RuntimeError(f"{run.name}: lifecycle hook failed or changed; later arms are gated")


if __name__ == "__main__":
    main()
