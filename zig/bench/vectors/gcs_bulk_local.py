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
import subprocess
import sys
import time
import uuid
from bench_bulk_pipeline import (
    CancellationScope,
    OwnedProcess,
    check_cancellation,
    gcs_probe_environment,
    sha256_stream,
    terminate_group as terminate_group,
    validate_source,
    validate_truth,
    write_report,
)


def command(argv, **kwargs):
    timeout = kwargs.pop("timeout", 60)
    with OwnedProcess() as owner:
        owner.start(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        stdout, stderr = owner.communicate(timeout)
        result = subprocess.CompletedProcess(argv, owner.returncode, stdout, stderr)
        result.check_returncode()
        return result


def digest(path):
    with path.open("rb") as stream:
        return sha256_stream(stream)


def freeze_inputs(output, binary, recall, *, reopen_from=None):
    inputs = output / "inputs"
    inputs.mkdir(mode=0o700)
    files = {"probe": binary}
    for name in ("gcs_bulk_local.py", "gcs_bulk_worker.py", "bench_bulk_pipeline.py"):
        files[name] = Path(__file__).with_name(name)
    for name in ("prepare.json", "queries.f32", "truth.u64"):
        files["recall/" + name] = recall / name
    if reopen_from is not None:
        files["reopen/case.json"] = reopen_from / "case.json"
        files["reopen/catalog.json"] = reopen_from / "case/catalog.json"
    hashes = {}
    for name, original in files.items():
        target = inputs / name
        target.parent.mkdir(mode=0o700, exist_ok=True)
        shutil.copyfile(original, target)
        target.chmod(0o500 if name == "probe" else 0o400)
        hashes[name] = digest(target)
    manifest = dict(version=1, files=hashes)
    write_report(inputs / "manifest.json", manifest)
    (inputs / "manifest.json").chmod(0o400)
    return inputs


def verify_inputs(inputs):
    manifest = json.loads((inputs / "manifest.json").read_text())
    expected = {
        "probe",
        "gcs_bulk_local.py",
        "gcs_bulk_worker.py",
        "bench_bulk_pipeline.py",
        "recall/prepare.json",
        "recall/queries.f32",
        "recall/truth.u64",
    }
    entries = set(manifest.get("files", {}))
    if manifest.get("version") != 1 or entries not in (
        expected,
        expected | {"reopen/case.json", "reopen/catalog.json"},
    ):
        raise ValueError("invalid frozen input manifest")
    for name, expected_hash in manifest["files"].items():
        if digest(inputs / name) != expected_hash:
            raise ValueError("frozen input changed: " + name)
    return manifest["files"]


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
        with OwnedProcess() as owner:
            process = owner.start(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=upload_log,
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
                            check_cancellation()
                            if remaining <= 0:
                                raise subprocess.TimeoutExpired(argv, timeout)
                            if not ready.select(min(0.2, remaining)):
                                continue
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
                # OwnedProcess drains the group after the stream closes.


def parse_args():
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
    p.add_argument("--frozen-inputs", action="store_true", help=argparse.SUPPRESS)
    args = p.parse_args()
    if args.interrupt and args.reopen_from:
        p.error("receipt interruption and verification-only reopen are separate cases")
    if args.interrupt and args.variant != "optimized":
        p.error("construction receipts require the optimized builder")
    if not 60 <= args.timeout <= 10800:
        p.error("timeout must be between one minute and three hours")
    args.output = args.output.resolve()
    return args


def run(args, report, cancellation):
    cancellation.check()
    gcs_probe_environment("adc")
    if not args.frozen_inputs:
        inputs = freeze_inputs(
            args.output,
            args.binary.resolve(),
            args.recall.resolve(),
            reopen_from=args.reopen_from.resolve() if args.reopen_from else None,
        )
        # Re-exec the copied controller too: its code, late validator imports,
        # executable and recall payloads must all match the recorded hashes.
        cancellation.exec(
            [
                sys.executable,
                "-B",
                str(inputs / "gcs_bulk_local.py"),
                *sys.argv[1:],
                "--frozen-inputs",
            ],
        )
    inputs = args.output / "inputs"
    if Path(__file__).resolve() != inputs / "gcs_bulk_local.py":
        raise ValueError("frozen-inputs is reserved for the owned controller snapshot")
    frozen_hashes = verify_inputs(inputs)
    if bool(args.reopen_from) != ("reopen/catalog.json" in frozen_hashes):
        raise ValueError("frozen reopen inputs do not match the requested case")
    args.binary = inputs / "probe"
    source = args.source.resolve()
    recall = inputs / "recall"
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
                bytes=binding["bytes"],
                sha256=binding["sha256"],
            )
        ]
        local = {"train.parquet": source}
    else:
        expected = binding["fixture"]["files"]
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
        source_objects=[
            dict(
                uri="gs://" + args.bucket + "/" + item["name"],
                bytes=int(item["size"]),
                version=str(item["generation"]),
                etag=item.get("etag", ""),
            )
            for item in objects
        ],
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
        old = json.loads((inputs / "reopen/case.json").read_text())
        if any(
            old[key] != case[key]
            for key in ("bucket", "source_prefix", "rows", "variant")
        ):
            raise ValueError("reopen input does not match the retained publication")
        case["prefix"] = old["prefix"]
        case["reopen_catalog"] = str(inputs / "reopen/catalog.json")
        case["case"] = "reopen-" + case["case"]
    write_report(args.output / "case.json", case)
    (args.output / "case.json").chmod(0o400)
    case_hash = digest(args.output / "case.json")
    worker = Path(__file__).with_name("gcs_bulk_worker.py")
    report.update(
        version=1,
        qualified=False,
        execution="local machine against remote GCS; WAN affects timings",
        platform=platform.platform(),
        python_version=sys.version,
        python_executable=sys.executable,
        logical_cpus=os.cpu_count(),
        load_before=os.getloadavg(),
        host_free_bytes_before=free,
        frozen_inputs_sha256=frozen_hashes,
        frozen_input_bytes=sum(
            (inputs / name).stat().st_size for name in frozen_hashes
        ),
        case_sha256=case_hash,
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
    report["status"] = "running"
    report["phase"] = "worker"
    write_report(args.output / "local.json", report)
    start = time.monotonic()
    owner = OwnedProcess()
    try:
        with (args.output / "worker.log").open("w") as log, owner:
            owner.start(
                [
                    sys.executable,
                    "-B",
                    str(worker.resolve()),
                    str(args.output / "case.json"),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            owner.wait(timeout=args.timeout)
    finally:
        report["wall_seconds"] = time.monotonic() - start
        report["load_after"] = os.getloadavg()
        report["worker_exit_code"] = owner.returncode
        report["worker_started"] = owner.process is not None
        write_report(args.output / "local.json", report)
    if owner.returncode != 0:
        raise RuntimeError(
            "qualification worker failed; inspect retained worker/build logs"
        )
    report["phase"] = "revalidate"
    for previous, item in zip(objects, expected):
        after = metadata(args.bucket, args.source_prefix, item["name"])
        check_object(after, item)
        if after["generation"] != previous["generation"]:
            raise ValueError("remote source changed during build")
    verify_inputs(inputs)
    if digest(args.output / "case.json") != case_hash:
        raise ValueError("case descriptor changed during execution")
    result = json.loads((args.output / "case/result.json").read_text())
    report["qualified"] = owner.returncode == 0 and result["qualified"]
    cancellation.check()
    report["status"] = "qualified" if report["qualified"] else "failed"
    report["phase"] = "complete"
    report["controller_exit_code"] = 0 if report["qualified"] else 1
    write_report(args.output / "local.json", report)
    if not report["qualified"]:
        raise RuntimeError(
            "local GCS qualification failed; inspect retained worker/build logs"
        )
    print(json.dumps(dict(qualified=True, output=str(args.output))), flush=True)


def main():
    with CancellationScope() as cancellation:
        args = parse_args()
        if not args.frozen_inputs:
            args.output.mkdir(parents=True, exist_ok=False)
        report = dict(
            version=1,
            status="preparing",
            phase="admission",
            qualified=False,
            worker_started=False,
            worker_exit_code=None,
        )
        started = time.monotonic()
        try:
            write_report(args.output / "local.json", report)
            run(args, report, cancellation)
        except BaseException as error:
            report["qualified"] = False
            report["status"] = (
                "cancelled"
                if cancellation.signum is not None
                or isinstance(error, KeyboardInterrupt)
                else "failed"
            )
            report["controller_error_type"] = type(error).__name__
            report["controller_exit_code"] = (
                128 + cancellation.signum
                if cancellation.signum is not None
                else error.code
                if isinstance(error, SystemExit) and isinstance(error.code, int)
                else 1
            )
            cancellation.check()
            raise
        finally:
            report["controller_wall_seconds"] = time.monotonic() - started
            write_report(args.output / "local.json", report)


if __name__ == "__main__":
    main()
