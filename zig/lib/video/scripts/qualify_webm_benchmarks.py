#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check every native picture against the independent WebM corpus manifest."""

import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate(stream, rows):
    expected = stream["picture_sha256"]
    mismatches = []
    for index, (row, digest) in enumerate(zip(rows, expected)):
        if (
            row["index"],
            row["width"],
            row["height"],
            row["depth"],
            row["chroma"],
            row["sha256"],
        ) != (
            index,
            stream["width"],
            stream["height"],
            stream.get("depth", 8),
            stream.get("chroma", 1),
            digest,
        ):
            mismatches.append(index)
    return {
        "status": "pass"
        if expected
        and stream["frames"] == len(expected)
        and len(rows) == len(expected)
        and not mismatches
        else "failed",
        "pictures": len(rows),
        "expected_pictures": len(expected),
        "mismatches": mismatches,
    }


def run(binary, corpus, output, codec):
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = corpus / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    report = {
        "version": 1,
        "status": "running",
        "scope": "Every visible native picture; independent normalized-plane SHA-256 reference",
        "codec_filter": codec,
        "platform": platform.platform(),
        "tool_sha256": sha(Path(__file__)),
        "binary_sha256": sha(binary),
        "manifest_sha256": sha(manifest_path),
        "streams": {},
    }
    try:
        for stream in manifest["streams"]:
            if codec != "all" and f"-{codec}-" not in stream["path"]:
                continue
            path = (corpus / stream["path"]).resolve()
            path.relative_to(corpus.resolve())
            if sha(path) != stream["sha256"]:
                raise ValueError("coded fixture changed")
            print("Qualifying", stream["path"], flush=True)
            result = subprocess.check_output(
                [str(binary.resolve()), str(path), "hash", "2", "384", "24"],
                timeout=600,
                text=True,
            )
            artifact = output / (stream["path"] + ".native.jsonl")
            artifact.write_text(result)
            rows = [json.loads(line) for line in result.splitlines()]
            record = validate(stream, rows)
            record["artifact_sha256"] = sha(artifact)
            report["streams"][stream["path"]] = record
            (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
            print(stream["path"], record["status"], record["pictures"], flush=True)
        if not report["streams"] or any(
            row["status"] != "pass" for row in report["streams"].values()
        ):
            raise ValueError("independent picture parity failed; evidence retained")
        report["status"] = "pass"
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--codec", choices=["all", "vp8", "vp9", "av1"], default="all")
    args = parser.parse_args()
    run(args.binary, args.corpus, args.output, args.codec)


if __name__ == "__main__":
    main()
