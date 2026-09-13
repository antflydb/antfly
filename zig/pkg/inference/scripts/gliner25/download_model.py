#!/usr/bin/env python3
"""Download one immutable GLiNER2.5 bundle serially with per-file limits."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import oracle


def download(variant: str, destination: Path) -> dict:
    model = oracle.load_manifest()["models"][variant]
    with oracle.atomic_output_directory(destination) as output:
        for name, expected in model["files"].items():
            target = output / name
            target.parent.mkdir(parents=True, exist_ok=True)
            url = f"https://huggingface.co/{model['model_id']}/resolve/{model['revision']}/{name}"
            subprocess.run([
                "curl", "--fail", "--silent", "--show-error", "--location",
                "--proto", "=https", "--proto-redir", "=https",
                "--connect-timeout", "15", "--max-time", "600",
                "--max-filesize", str(expected["size_bytes"]),
                "--output", str(target), url,
            ], check=True, timeout=620)
            oracle.verify_file(target, expected)
            print(json.dumps({"status": "file_verified", "file": name,
                              "size_bytes": expected["size_bytes"]}), file=sys.stderr)
        verified = oracle.verify_model_dir(variant, output)
    verified["directory"] = str(destination.resolve())
    return {"status": "downloaded_and_verified", **verified,
            "real_model_qualified": False, "native_runtime_qualified": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=("small", "base", "multi"), required=True)
    parser.add_argument("--output", type=Path, required=True, help="new bundle directory; never overwritten")
    parser.add_argument("--dry-run", action="store_true", help="report byte budget without network access")
    args = parser.parse_args()
    try:
        if args.dry_run:
            model = oracle.load_manifest()["models"][args.model]
            result = {"status": "planned", "model_id": model["model_id"], "revision": model["revision"],
                      "bytes": sum(item["size_bytes"] for item in model["files"].values()),
                      "files": len(model["files"]), "serial": True}
        else:
            result = download(args.model, args.output)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (oracle.ContractError, OSError, subprocess.SubprocessError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
