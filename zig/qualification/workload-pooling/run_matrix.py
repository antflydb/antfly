#!/usr/bin/env python3
"""Run independent isolated profiles and preserve all receipts, including failures."""

import argparse
import json
import pathlib
import subprocess
import sys


def variant_order(variants, repetition):
    offset = repetition % len(variants)
    return variants[offset:] + variants[:offset]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=pathlib.Path, required=True)
    parser.add_argument("--baseline", type=pathlib.Path)
    parser.add_argument("--manifest", type=pathlib.Path, action="append", required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--repetitions", type=int, default=1)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        parser.error("output directory is not empty; use a fresh matrix directory")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    root = pathlib.Path(__file__).parent
    failures = []
    profiles = []
    for manifest in args.manifest:
        variants = [
            ("candidate-fixed", args.candidate, "fixed"),
            ("candidate-failfast", args.candidate, "failfast"),
        ]
        if args.baseline:
            variants.insert(0, ("origin-main", args.baseline, "baseline"))
        for repetition in range(args.repetitions):
            stem = manifest.stem + (
                "-r%02d" % (repetition + 1) if args.repetitions > 1 else ""
            )
            outputs = {}
            for name, binary, policy in variant_order(variants, repetition):
                output = args.output_dir / (stem + "-" + name + ".json")
                result = subprocess.run(
                    [
                        sys.executable,
                        str(root / "run_local.py"),
                        "--binary",
                        str(binary),
                        "--manifest",
                        str(manifest),
                        "--output",
                        str(output),
                        "--policy",
                        policy,
                    ]
                )
                outputs[name] = output
                profiles.append(
                    {
                        "manifest": str(manifest),
                        "variant": name,
                        "repetition": repetition + 1,
                        "receipt": str(output),
                        "exit_code": result.returncode,
                    }
                )
                if result.returncode:
                    failures.append(stem + ": " + name)
            if json.loads(manifest.read_text()).get("kind") == "load":
                for name in ("candidate-failfast", "origin-main"):
                    if name not in outputs:
                        continue
                    output = args.output_dir / (stem + "-compare-" + name + ".json")
                    result = subprocess.run(
                        [
                            sys.executable,
                            str(root / "compare_receipts.py"),
                            "--baseline",
                            str(outputs[name]),
                            "--candidate",
                            str(outputs["candidate-fixed"]),
                            "--output",
                            str(output),
                        ]
                    )
                    if result.returncode:
                        failures.append(stem + ": comparison with " + name)
    summary = dict(
        passed=not failures,
        failures=failures,
        profiles=profiles,
        automatic_defaults_qualified=False,
        repetitions=args.repetitions,
        order="rotated each repetition",
    )
    (args.output_dir / "matrix.json").write_text(json.dumps(summary, indent=2) + "\n")
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
