#!/usr/bin/env python3
"""Run independent isolated profiles and preserve all receipts, including failures."""

import argparse
import json
import pathlib
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=pathlib.Path, required=True)
    parser.add_argument("--baseline", type=pathlib.Path)
    parser.add_argument("--manifest", type=pathlib.Path, action="append", required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()
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
        outputs = {}
        for name, binary, policy in variants:
            output = args.output_dir / (manifest.stem + "-" + name + ".json")
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
                    "receipt": str(output),
                    "exit_code": result.returncode,
                }
            )
            if result.returncode:
                failures.append(manifest.stem + ": " + name)
        if json.loads(manifest.read_text()).get("kind") == "load":
            for name in ("candidate-failfast", "origin-main"):
                if name not in outputs:
                    continue
                output = args.output_dir / (
                    manifest.stem + "-compare-" + name + ".json"
                )
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
                    failures.append(manifest.stem + ": comparison with " + name)
    summary = dict(
        passed=not failures,
        failures=failures,
        profiles=profiles,
        automatic_defaults_qualified=False,
    )
    (args.output_dir / "matrix.json").write_text(json.dumps(summary, indent=2) + "\n")
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
