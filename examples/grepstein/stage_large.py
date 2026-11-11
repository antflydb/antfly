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

"""Inventory or preserve the selected large archive snapshots; never index them."""

import argparse
import fcntl
import json
import re
import subprocess
import zipfile
from pathlib import Path

IDENTITY_FIELDS = (
    "dataset",
    "item",
    "url",
    "file",
    "official_status",
    "mirror_reported_status",
)


def receipt_identity(receipt):
    return {key: receipt.get(key) for key in IDENTITY_FIELDS}


def plan_identity(plan):
    # Retrieval timestamps are diagnostic; source identities and budgets are pinned.
    return [
        (r["destination"], r["bytes"], receipt_identity(r["receipt"]))
        for r in plan["archives"]
    ]


def run(args):
    root = args.state.resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Serialize the entire orchestration, not just individual CLI subprocesses.
    with (root / ".stage-large.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("another staging workflow owns this state") from error
        return stage_locked(args, root)


def stage_locked(args, root):
    archives = root / "archives"
    binary = str(args.grepstein.resolve())
    common = [
        binary,
        "fetch-archive",
        "--dataset",
        "all-large",
        "--output",
        str(archives),
        "--max-bytes",
        str(args.max_compressed_bytes),
        "--timeout",
        "6h",
    ]
    subprocess.run(common + ["--inventory-only"], check=True)
    plan = json.loads((archives / "acquisition-plan.json").read_text())
    print("Acquisition plan SHA-256:", plan["sha256"], flush=True)
    if not args.execute:
        return
    if args.expected_plan_sha256 != plan["sha256"]:
        raise ValueError("--execute requires the reviewed --expected-plan-sha256")
    # The CLI repeats the metadata check immediately before any downloads.
    subprocess.run(common + ["--expected-plan-sha256", plan["sha256"]], check=True)
    # Share the CLI's acquisition lock while inspecting and consuming cached
    # revisions. A standalone inventory/download cannot replace a receipt midway
    # through staging. A crashed lock needs explicit removal, as for the CLI.
    lock = archives / ".lock"
    try:
        lock.mkdir(mode=0o700)
    except FileExistsError as error:
        raise ValueError("archive acquisition state is locked") from error
    try:
        return stage_acquired(args, root, archives, binary, plan)
    finally:
        lock.rmdir()


def stage_acquired(args, root, archives, binary, plan):
    verified = json.loads((archives / "acquisition-plan.json").read_text())
    if verified.get("sha256") != plan["sha256"] or plan_identity(
        verified
    ) != plan_identity(plan):
        raise ValueError("acquisition plan changed during transfer")
    # Consume the original in-memory plan, never a later mutable inventory.
    stages = []
    total = 0
    for archive in plan["archives"]:
        destination = Path(archive["destination"]).resolve()
        receipt = json.loads(Path(str(destination) + ".json").read_text())
        if receipt_identity(receipt) != receipt_identity(archive["receipt"]):
            raise ValueError("acquisition receipt differs from the pinned plan")
        if not re.fullmatch("[a-f0-9]{64}", receipt.get("sha256", "")):
            raise ValueError("archive has no completed acquisition receipt")
        with zipfile.ZipFile(destination) as source:
            members = [f for f in source.infolist() if not f.is_dir()]
            size = sum(f.file_size for f in members)
            if any(f.file_size > args.max_object_bytes for f in members):
                raise ValueError("archive member exceeds the reviewed object budget")
        total += size
        if total > args.max_raw_bytes:
            raise ValueError("archives exceed the reviewed total uncompressed budget")
        stages.append((receipt["dataset"], receipt["sha256"], destination, size))
    # Each revision keeps its own raw catalog. The CLI verifies archive bytes
    # against acquisition receipts and reuses only checksum-verified GCS objects.
    summaries = []
    for dataset, revision, destination, size in stages:
        state = root / "originals" / dataset / revision
        subprocess.run(
            [
                binary,
                "stage-originals",
                "--state",
                str(state),
                "--bucket",
                args.bucket,
                "--source",
                f"{dataset}={destination}",
                "--max-bytes",
                str(size),
                "--max-object-bytes",
                str(args.max_object_bytes),
            ],
            check=True,
        )
        receipt = json.loads((state / "originals.json").read_text())
        if not receipt.get("uploaded") or receipt.get("antfly_loaded"):
            raise ValueError("unexpected staging outcome")
        summaries.append(
            {"dataset": dataset, "archive_sha256": revision, "receipt": receipt}
        )
    output = root / "staging-complete.json"
    payload = {
        "acquisition_plan_sha256": plan["sha256"],
        "raw_bytes": total,
        "antfly_loaded": False,
        "coverage": "selected archive snapshots only; official/media completeness unverified",
        "stages": summaries,
    }
    # A completion marker is written only after every per-release catalog seals.
    pending = output.with_suffix(".pending")
    pending.write_text(json.dumps(payload, indent=2) + "\n")
    pending.replace(output)
    print("Raw snapshots staged; Antfly tables and OCR were not changed.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grepstein", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--bucket", default="antfly-showcases-originals")
    parser.add_argument("--max-compressed-bytes", type=int, default=270_000_000_000)
    parser.add_argument("--max-raw-bytes", type=int, default=270_000_000_000)
    parser.add_argument("--max-object-bytes", type=int, default=8 << 30)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--expected-plan-sha256")
    arguments = parser.parse_args()
    if (
        min(
            arguments.max_compressed_bytes,
            arguments.max_raw_bytes,
            arguments.max_object_bytes,
        )
        <= 0
    ):
        parser.error("byte budgets must be positive")
    run(arguments)
