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

"""Stream the public VectorDBBench Cohere 10M corpus into native FS objects.

The AFOBJ001 envelope matches zig/lib/objectstore/src/filesystem.zig. No second
raw corpus is retained. HTTP ETags fence each download; body SHA-256 and object
versions are recorded before atomically installing each immutable fixture.
"""

import argparse
import hashlib
import json
import os
import shutil
import struct
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--files", type=int, default=10)
    args = parser.parse_args()
    if not 1 <= args.files <= 10:
        parser.error("files must be between 1 and 10")
    objects = args.output / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True, exist_ok=True)
    report_path = args.output / "fixture.json"
    report = (
        json.loads(report_path.read_text())
        if report_path.exists()
        else {
            "format": "AFOBJ001",
            "dataset": "VectorDBBench Cohere Large 10M",
            "files": [],
        }
    )
    for index in range(args.files):
        name = f"train-{index:02d}-of-10.parquet"
        url = "https://assets.zilliz.com/benchmark/cohere_large_10m/" + name
        target = objects / name
        existing = next(
            (item for item in report["files"] if item["name"] == name), None
        )
        if (
            existing
            and target.is_file()
            and target.stat().st_size == existing["bytes"] + 84
        ):
            with target.open("rb") as source:
                header = source.read(84)
                expected = (
                    b"AFOBJ001"
                    + struct.pack("<QI", existing["bytes"], 0)
                    + existing["sha256"].encode("ascii")
                )
                if (
                    header != expected
                    or hashlib.file_digest(source, "sha256").hexdigest()
                    != existing["sha256"]
                ):
                    raise ValueError(f"cached fixture integrity mismatch: {target}")
            continue
        raw = subprocess.check_output(
            ["curl", "-fsSI", "--max-time", "60", url], text=True
        )
        headers = {}
        for line in raw.splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.lower()] = value.strip()
        length = int(headers["content-length"])
        if shutil.disk_usage(objects).free < length + 2 * 1024**3:
            raise RuntimeError("insufficient disk for the next immutable fixture")
        staging = target.with_suffix(".download")
        digest = hashlib.sha256()
        received = 0
        child = subprocess.Popen(
            [
                "curl",
                "-fsSL",
                "--retry",
                "3",
                "--max-time",
                "1800",
                "-H",
                "If-Match: " + headers["etag"],
                url,
            ],
            stdout=subprocess.PIPE,
        )
        try:
            with staging.open("wb") as out:
                out.write(bytes(84))
                while chunk := child.stdout.read(8 * 1024**2):
                    out.write(chunk)
                    digest.update(chunk)
                    received += len(chunk)
                    if received > length:
                        raise RuntimeError("object exceeded pinned Content-Length")
                if child.wait() or received != length:
                    raise RuntimeError(
                        f"incomplete HTTP object {url}: {received}/{length}"
                    )
                checksum = digest.hexdigest()
                out.seek(0)
                out.write(
                    b"AFOBJ001"
                    + struct.pack("<QI", length, 0)
                    + checksum.encode("ascii")
                )
                out.flush()
                os.fsync(out.fileno())
            os.replace(staging, target)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()
        item = dict(
            name=name,
            url=url,
            bytes=length,
            sha256=digest.hexdigest(),
            etag=headers["etag"],
            version=headers.get("x-amz-version-id"),
        )
        report["files"] = [
            record for record in report["files"] if record["name"] != name
        ] + [item]
        report["files"].sort(key=lambda record: record["name"])
        report["source_uri"] = "file://" + str(args.output.resolve())
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(item), flush=True)


if __name__ == "__main__":
    main()
