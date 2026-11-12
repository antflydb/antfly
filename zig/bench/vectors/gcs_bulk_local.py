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

"""Run a hash-bound Parquet bulk qualification locally with GCS source/artifacts.

Uses existing local ADC with refresh, without creating compute, IAM or buckets.
Same-region deployment can follow the existing Hacker News regional runner.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import selectors
import shutil
import signal
import subprocess
import sys
import time
import uuid
from bench_bulk_pipeline import validate_source, validate_truth, write_report


def command(argv, **kwargs):
    kwargs.setdefault("timeout", 60)
    return subprocess.run(argv, check=True, capture_output=True, **kwargs)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def metadata(bucket, prefix, name):
    return json.loads(
        command(
            [
                "gcloud",
                "storage",
                "objects",
                "describe",
                f"gs://{bucket}/{prefix}/{name}",
                "--format=json",
            ],
            text=True,
        ).stdout
    )


def check_object(value, expected):
    if (
        value.get("custom_fields", {}).get("sha256") != expected["sha256"]
        or int(value["size"]) != expected["bytes"]
    ):
        raise ValueError("remote source differs from hash-verified input")


def terminate_group(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    finally:
        # The leader can exit before descendants that ignore SIGTERM. Drain
        # the owned group even when waiting for its leader already succeeded.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def upload(bucket, prefix, name, path, expected, framed, log_path, *, timeout=1800):
    # Never retain another raw corpus just to remove the filesystem envelope.
    argv = [
        "gcloud",
        "storage",
        "cp",
        "-",
        f"gs://{bucket}/{prefix}/{name}",
        "--if-generation-match=0",
        "--custom-metadata=sha256=" + expected["sha256"],
    ]
    with path.open("rb") as stream, log_path.open("w") as upload_log:
        if framed:
            stream.seek(84)
        hashed = hashlib.sha256()
        process = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=upload_log,
            start_new_session=True,
        )
        deadline = time.monotonic() + timeout
        try:
            os.set_blocking(process.stdin.fileno(), False)
            with selectors.DefaultSelector() as ready:
                ready.register(process.stdin, selectors.EVENT_WRITE)
                while chunk := stream.read(8 * 1024 * 1024):
                    hashed.update(chunk)
                    pending = memoryview(chunk)
                    while pending:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or not ready.select(remaining):
                            raise subprocess.TimeoutExpired(argv, timeout)
                        try:
                            written = os.write(process.stdin.fileno(), pending)
                        except BlockingIOError:
                            continue
                        pending = pending[written:]
            process.stdin.close()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(argv, timeout)
            process.wait(timeout=remaining)
            if process.returncode:
                raise RuntimeError("source upload failed")
            if hashed.hexdigest() != expected["sha256"]:
                raise ValueError("source changed during upload")
        finally:
            if not process.stdin.closed:
                process.stdin.close()
            terminate_group(process)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--source-prefix", required=True)
    p.add_argument("--recall", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--bucket", default="colony-import-sources-antfly-dev-01")
    p.add_argument(
        "--variant",
        choices=["incremental", "control", "optimized"],
        default="optimized",
    )
    p.add_argument("--interrupt", choices=[1, 2], type=int)
    p.add_argument(
        "--upload",
        action="store_true",
        help="Upload absent objects into the test source prefix",
    )
    p.add_argument(
        "--reopen-from",
        type=Path,
        help="Verify a retained completed publication without rebuilding",
    )
    p.add_argument("--timeout", type=int, default=7200)
    args = p.parse_args()
    if args.interrupt and args.reopen_from:
        p.error("receipt interruption and verification-only reopen are separate cases")
    if args.interrupt and args.variant != "optimized":
        p.error("construction receipts require the optimized builder")
    if not 60 <= args.timeout <= 10800:
        p.error("timeout must be between one minute and three hours")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    source = args.source.resolve()
    recall = args.recall.resolve()
    truth = json.loads((recall / "prepare.json").read_text())
    binding = validate_source(source, truth)
    validate_truth(recall, truth, 768)
    rows = truth["vectors"]
    if rows not in (100000, 1000000, 10000000):
        raise ValueError("unqualified corpus size")
    if args.variant == "incremental" and rows > 100000:
        raise ValueError(
            "the established insertion path retains its 512 MiB input / 1 GiB output admission; use the bulk control for larger matched comparisons"
        )
    needed = (140 if rows == 10000000 else 12 if rows == 1000000 else 2) * 1024**3
    free = shutil.disk_usage(args.output).free
    if free < needed:
        raise ValueError("insufficient scratch headroom before build")
    if source.is_file():
        expected = [
            dict(
                name="train.parquet",
                bytes=source.stat().st_size,
                sha256=binding["sha256"],
            )
        ]
        local = {"train.parquet": source}
    else:
        expected = json.loads((source / "fixture.json").read_text())["files"]
        local = {
            item["name"]: source / "buckets/antfly/objects" / item["name"]
            for item in expected
        }
    objects = []
    for item in expected:
        try:
            current = metadata(args.bucket, args.source_prefix, item["name"])
        except subprocess.CalledProcessError as error:
            if not args.upload or not any(
                marker in error.stderr.lower()
                for marker in ("not found", "404", "notfound")
            ):
                raise
            upload(
                args.bucket,
                args.source_prefix,
                item["name"],
                local[item["name"]],
                item,
                source.is_dir(),
                args.output / (item["name"] + ".upload.log"),
            )
            current = metadata(args.bucket, args.source_prefix, item["name"])
        check_object(current, item)
        objects.append(current)
    case = dict(
        case=args.variant
        + ("-receipt-" + str(args.interrupt) if args.interrupt else ""),
        binary=str(args.binary.resolve()),
        bucket=args.bucket,
        prefix="bulk-qualification/local-" + uuid.uuid4().hex[:12],
        source_prefix=args.source_prefix,
        rows=rows,
        variant=args.variant,
        interrupt=args.interrupt,
        auth="adc",
        recall=str(recall),
        work=str(args.output / "case"),
        allowance=(40 if rows == 10000000 else 4) * 1024**3,
        identity_allowance=(8 if rows == 10000000 else 1) * 1024**3,
    )
    if args.reopen_from:
        old = json.loads((args.reopen_from / "case.json").read_text())
        if any(
            old[key] != case[key]
            for key in ("bucket", "source_prefix", "rows", "variant")
        ):
            raise ValueError("reopen input does not match the retained publication")
        case["prefix"] = old["prefix"]
        case["reopen_catalog"] = str((args.reopen_from / "case/catalog.json").resolve())
        case["case"] = "reopen-" + case["case"]
    write_report(args.output / "case.json", case)
    worker = Path(__file__).with_name("gcs_bulk_worker.py")
    report = dict(
        version=1,
        qualified=False,
        execution="local machine against remote GCS; WAN affects timings",
        platform=platform.platform(),
        logical_cpus=os.cpu_count(),
        load_before=os.getloadavg(),
        host_free_bytes_before=free,
        source_binding=binding,
        source_objects=objects,
        binary_sha256=digest(args.binary),
        harness_sha256={
            path.name: digest(path)
            for path in (
                Path(__file__),
                worker,
                Path(__file__).with_name("bench_bulk_pipeline.py"),
            )
        },
        base_revision=command(["git", "rev-parse", "HEAD"], text=True).stdout.strip(),
        tracked_diff_sha256=hashlib.sha256(
            command(["git", "diff", "HEAD"]).stdout
        ).hexdigest(),
        case=case,
    )
    write_report(args.output / "local.json", report)
    start = time.monotonic()
    with (args.output / "worker.log").open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-B",
                str(worker.resolve()),
                str(args.output / "case.json"),
            ],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            process.wait(timeout=args.timeout)
        except BaseException:
            terminate_group(process)
            raise
        finally:
            report["wall_seconds"] = time.monotonic() - start
            report["load_after"] = os.getloadavg()
            report["worker_exit_code"] = process.returncode
            write_report(args.output / "local.json", report)
    for previous, item in zip(objects, expected):
        after = metadata(args.bucket, args.source_prefix, item["name"])
        check_object(after, item)
        if after["generation"] != previous["generation"]:
            raise ValueError("remote source changed during build")
    result = json.loads((args.output / "case/result.json").read_text())
    report["qualified"] = process.returncode == 0 and result["qualified"]
    write_report(args.output / "local.json", report)
    if not report["qualified"]:
        raise RuntimeError(
            "local GCS qualification failed; inspect retained worker/build logs"
        )
    print(json.dumps(dict(qualified=True, output=str(args.output))), flush=True)


if __name__ == "__main__":
    main()
