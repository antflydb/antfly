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

"""Alternate complete Parquet build variants; validate reopened recall and work.

Build the ReleaseFast runtime probe first. Fixture preparation and query/reopen
are outside its operation timer, but remain included in subprocess wall time.
The child logs retain per-phase times, logical partition I/O, and recall sweeps.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import re
import sys
import subprocess
import struct
import time
from pathlib import Path
from statistics import median


def validate_truth(path, manifest, dims):
    if (
        manifest["k"] != 100
        or manifest["metric"] != "squared_l2"
        or manifest["normalization"] != "none"
        or manifest["accumulation"] != "float64"
        or not 1 <= manifest["queries"] <= 1000
    ):
        raise ValueError("unsupported ground-truth contract")
    for name, size in (
        ("queries.f32", manifest["queries"] * dims * 4),
        ("truth.u64", manifest["queries"] * manifest["k"] * 8),
    ):
        payload = path / name
        with payload.open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        if payload.stat().st_size != size or actual != manifest["outputs"][name]:
            raise ValueError(f"ground-truth payload mismatch: {payload}")


def write_report(path, report):
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    os.replace(temporary, path)


def validate_recall(records, truth):
    if not 5 <= len(records) <= 9 or [r["probes"] for r in records] != [
        512 << i for i in range(len(records))
    ]:
        raise ValueError("missing, duplicate or out-of-order recall widths")
    denominator = truth["queries"] * truth["k"]
    for row in records:
        if (
            row["queries"] != truth["queries"]
            or row["k"] != truth["k"]
            or type(row["hits"]) is not int
            or not 0 <= row["hits"] <= denominator
            or type(row["scored"]) is not int
            or row["scored"] < row["hits"]
            or not math.isfinite(row["recall"])
            or not math.isclose(
                row["recall"], row["hits"] / denominator, rel_tol=0, abs_tol=1e-12
            )
        ):
            raise ValueError("inconsistent recall evidence")
    if records[-1]["recall"] < 0.95:
        raise ValueError("recall target not reached")


def validate_source(source, truth):
    if source.is_file():
        with source.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if digest != truth["source_sha256"]:
            raise ValueError("ground truth is not bound to this Parquet source")
        return dict(bytes=source.stat().st_size, sha256=digest, fixture=None)
    fixture = json.loads((source / "fixture.json").read_text())
    objects = source / "buckets" / "antfly" / "objects"
    files = fixture["files"]
    expected = truth["sources"]
    if (
        fixture["format"] != "AFOBJ001"
        or not files
        or [f["name"] for f in files] != sorted(f["name"] for f in files)
        or [f["name"] for f in files] != [f["name"] for f in expected]
        or len({f["name"] for f in files}) != len(files)
        or sum(f["rows"] for f in expected) != truth["vectors"]
        or {p.name for p in objects.iterdir()} != {f["name"] for f in files}
    ):
        raise ValueError("framed Parquet inventory does not match ground truth")
    for item, authored in zip(files, expected):
        if (
            Path(item["name"]).name != item["name"]
            or item["sha256"] != authored["sha256"]
        ):
            raise ValueError("framed Parquet source identity mismatch")
        payload = objects / item["name"]
        with payload.open("rb") as stream:
            expected_header = (
                b"AFOBJ001"
                + struct.pack("<QI", item["bytes"], 0)
                + item["sha256"].encode("ascii")
            )
            if (
                stream.read(84) != expected_header
                or payload.stat().st_size != item["bytes"] + 84
                or hashlib.file_digest(stream, "sha256").hexdigest() != item["sha256"]
            ):
                raise ValueError(f"framed Parquet payload mismatch: {payload}")
    return dict(bytes=sum(f["bytes"] for f in files), sha256=None, fixture=fixture)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=Path)
    parser.add_argument(
        "parquet", type=Path, help="Parquet file or verified framed fixture directory"
    )
    parser.add_argument("recall", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--logs", type=Path, required=True)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument(
        "--variants",
        nargs="+",
        choices=["full_vectors", "fused_identity", "prebound_parents", "fused_compact"],
        default=["full_vectors", "fused_identity", "prebound_parents"],
    )
    parser.add_argument("--dims", type=int, default=768)
    parser.add_argument("--allowance", type=int, default=4 * 1024**3)
    parser.add_argument("--identity-spill-allowance", type=int)
    parser.add_argument("--algorithm", choices=["lloyd", "emeans"], default="lloyd")
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("trials must be positive")
    if args.allowance <= 0 or (
        args.identity_spill_allowance is not None and args.identity_spill_allowance <= 0
    ):
        parser.error("input and identity spill allowances must be positive")
    args.logs.mkdir(parents=True, exist_ok=True)
    executable = args.executable.resolve()
    available = {
        "full_vectors": ("full_vectors", "0", "0", "0"),
        "fused_identity": ("fused_identity", "1", "0", "0"),
        "prebound_parents": ("prebound_parents", "1", "0", "1"),
        "fused_compact": ("fused_compact", "1", "1", "1"),
    }
    if len(set(args.variants)) != len(args.variants):
        parser.error("variants must be unique")
    configs = [available[name] for name in args.variants]
    rows = []
    source = args.parquet.resolve()
    truth_manifest = json.loads((args.recall / "prepare.json").read_text())
    if truth_manifest["dims"] != args.dims:
        raise ValueError("ground truth is not bound to this dimension")
    source_binding = validate_source(source, truth_manifest)
    validate_truth(args.recall, truth_manifest, args.dims)
    report = dict(
        executable=str(executable),
        executable_sha256=hashlib.file_digest(
            executable.open("rb"), "sha256"
        ).hexdigest(),
        parquet=str(source),
        parquet_bytes=source_binding["bytes"],
        parquet_sha256=source_binding["sha256"],
        source_fixture=source_binding["fixture"],
        platform=platform.platform(),
        logical_cpus=os.cpu_count(),
        algorithm=args.algorithm,
        dims=args.dims,
        input_allowance=args.allowance,
        identity_spill_allowance=args.identity_spill_allowance,
        trials=args.trials,
        variants=args.variants,
        ground_truth=truth_manifest,
        operation_scope="Complete native build call including identity, source/checkpoint work, clustering, and artifact candidate; excludes fixture framing, final metadata CAS, and reopened recall",
        subprocess_scope="Entire probe including fixture framing, source opening, construction, reopen, raw recall, and teardown",
        results=rows,
    )
    for trial in range(args.trials):
        order = configs if trial % 2 == 0 else list(reversed(configs))
        for name, fused, compact, prebind in order:
            with executable.open("rb") as stream:
                if (
                    hashlib.file_digest(stream, "sha256").hexdigest()
                    != report["executable_sha256"]
                ):
                    raise ValueError("benchmark executable changed after preflight")
            validate_truth(args.recall, truth_manifest, args.dims)
            env = dict(
                os.environ,
                ANTFLY_LAKE_BULK_RECALL_DIR=str(args.recall.resolve()),
                ANTFLY_LAKE_BULK_DIMS=str(args.dims),
                ANTFLY_LAKE_BULK_INPUT_BYTES=str(args.allowance),
                ANTFLY_LAKE_BULK_ALGORITHM=args.algorithm,
                ANTFLY_LAKE_BULK_FUSED=fused,
                ANTFLY_LAKE_BULK_COMPACT=compact,
                ANTFLY_LAKE_BULK_PREBIND=prebind,
                ANTFLY_LAKE_BULK_CHECKPOINT="1",
            )
            env.pop("ANTFLY_LAKE_BULK_PARQUET_FILE", None)
            env.pop("ANTFLY_LAKE_BULK_PARQUET", None)
            env.pop("ANTFLY_LAKE_BULK_IDENTITY_SPILL_BYTES", None)
            env[
                "ANTFLY_LAKE_BULK_PARQUET"
                if source.is_dir()
                else "ANTFLY_LAKE_BULK_PARQUET_FILE"
            ] = source.as_uri() if source.is_dir() else str(source)
            if args.identity_spill_allowance is not None:
                env["ANTFLY_LAKE_BULK_IDENTITY_SPILL_BYTES"] = str(
                    args.identity_spill_allowance
                )
            log = args.logs / f"{args.algorithm}-{trial}-{name}.log"
            start = time.monotonic_ns()
            load_before = os.getloadavg()
            with log.open("w") as out:
                with subprocess.Popen(
                    [str(executable)], env=env, stdout=out, stderr=subprocess.STDOUT
                ) as child:
                    try:
                        _, status, usage = os.wait4(child.pid, 0)
                        child.returncode = os.waitstatus_to_exitcode(status)
                    except BaseException:
                        child.terminate()
                        try:
                            child.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            child.kill()
                            child.wait()
                        raise
            wall = time.monotonic_ns() - start
            text = log.read_text()
            # Save process measurements before interpreting or qualifying output.
            # Cache retention is a soft target: pinned handles can exceed it;
            # all cache ownership still belongs to the hard allocator quota.
            row = dict(
                trial=trial,
                variant=name,
                fused=fused == "1",
                compact=compact == "1",
                prebind_parents=prebind == "1",
                process_exit_code=child.returncode,
                qualified=False,
                subprocess_wall_ns=wall,
                subprocess_user_ns=round(usage.ru_utime * 1e9),
                subprocess_system_ns=round(usage.ru_stime * 1e9),
                subprocess_peak_rss_bytes=usage.ru_maxrss
                * (1 if sys.platform == "darwin" else 1024),
                load_before=load_before,
                load_after=os.getloadavg(),
                log=str(log),
            )
            rows.append(row)
            write_report(args.output, report)
            try:
                with executable.open("rb") as stream:
                    if (
                        hashlib.file_digest(stream, "sha256").hexdigest()
                        != report["executable_sha256"]
                    ):
                        raise ValueError(
                            "benchmark executable changed during execution"
                        )
                if validate_source(source, truth_manifest) != source_binding:
                    raise ValueError("benchmark source changed during execution")
                validate_truth(args.recall, truth_manifest, args.dims)
                if child.returncode:
                    raise RuntimeError(f"{name} failed ({child.returncode}): {log}")
                match = re.search(
                    r"^PARQUET_BULK algorithm=(\w+) vectors=(\d+) dims=(\d+) total_ns=(\d+) reopened_hits=(\d+) checkpoints=(\d+) logical_identity=(true|false)$",
                    text,
                    re.M,
                )
                if (
                    not match
                    or match[1] != args.algorithm
                    or int(match[2]) != truth_manifest["vectors"]
                    or int(match[3]) != args.dims
                    or int(match[5]) != 10
                    or int(match[6]) < 2
                    or match[7] != "true"
                ):
                    raise RuntimeError(f"incomplete build verification: {log}")
                records = {
                    prefix: [
                        json.loads(line[len(prefix) + 1 :])
                        for line in text.splitlines()
                        if line.startswith(prefix + " ")
                    ]
                    for prefix in (
                        "PARQUET_BULK_PHASES",
                        "PARQUET_BULK_WORK",
                        "PARQUET_BULK_RECALL",
                        "PARQUET_BULK_SOURCE",
                        "PARQUET_BULK_PROJECTION",
                        "PARQUET_BULK_MEMORY",
                        "PARQUET_BULK_BUILD_MODE",
                    )
                }
                modes = records["PARQUET_BULK_BUILD_MODE"]
                if (
                    len(modes) != 1
                    or modes[0]["caller"] != "fast"
                    or modes[0]["provider"] != modes[0]["caller_value"]
                    or modes[0]["provider_is_test"]
                ):
                    raise RuntimeError(
                        f"mixed or non-release benchmark compilation: {log}"
                    )
                if (
                    len(records["PARQUET_BULK_PROJECTION"]) != 1
                    or len(records["PARQUET_BULK_MEMORY"]) != 1
                ):
                    raise RuntimeError(f"missing storage/memory evidence: {log}")
                if len(records["PARQUET_BULK_WORK"]) != 1:
                    raise RuntimeError(f"missing construction work evidence: {log}")
                validate_recall(records["PARQUET_BULK_RECALL"], truth_manifest)
                phases = records["PARQUET_BULK_PHASES"]
                if (
                    len(phases) != 1
                    or not {"identity", "dense_cluster", "dense_checkpoint"}.issubset(
                        phases[0]["names"]
                    )
                    or len(phases[0]["names"]) != len(set(phases[0]["names"]))
                    or len(phases[0]["names"]) != len(phases[0]["ns"])
                    or len(phases[0]["names"]) != len(phases[0]["calls"])
                    or any(v < 0 for v in phases[0]["ns"] + phases[0]["calls"])
                ):
                    raise RuntimeError(f"incomplete phase attribution: {log}")
                if (
                    len(records["PARQUET_BULK_SOURCE"]) != 1
                    or records["PARQUET_BULK_SOURCE"][0]["new_bytes"] != 0
                    or records["PARQUET_BULK_SOURCE"][0]["reused_runs"]
                    != records["PARQUET_BULK_SOURCE"][0]["runs"]
                    or records["PARQUET_BULK_SOURCE"][0]["runs"] <= 0
                    or records["PARQUET_BULK_SOURCE"][0]["reused_bytes"] <= 0
                ):
                    raise RuntimeError(f"exact source was not fully reused: {log}")
                memory = records["PARQUET_BULK_MEMORY"][0]
                if (
                    memory["dense_allocator_peak_bytes"] > 256 * 1024**2
                    or memory["source_allocator_peak_bytes"] > 256 * 1024**2
                ):
                    raise RuntimeError(f"private allocation quota exceeded: {log}")
                work = records["PARQUET_BULK_WORK"][0]
                if (
                    not work.get("identity_census_verified")
                    or work["owned_index_count"] != 0
                    or work["identity_merge_inputs"] > 32
                    or work["projection_cache_peak_bytes"]
                    > memory["dense_allocator_peak_bytes"]
                ):
                    raise RuntimeError(
                        f"unbounded or unverified identity construction: {log}"
                    )
                if (
                    prebind == "1"
                    and records["PARQUET_BULK_WORK"][0]["update_parent_calls"] != 0
                ):
                    raise RuntimeError(f"prebound build rewrote parent headers: {log}")
                row.update(
                    trial=trial,
                    fused=fused == "1",
                    compact=compact == "1",
                    prebind_parents=prebind == "1",
                    variant=name,
                    vectors=int(match[2]),
                    dims=int(match[3]),
                    algorithm=match[1],
                    total_ns=int(match[4]),
                    reopened_hits=int(match[5]),
                    checkpoints=int(match[6]),
                    logical_identity=match[7] == "true",
                    subprocess_wall_ns=wall,
                    subprocess_user_ns=round(usage.ru_utime * 1e9),
                    subprocess_system_ns=round(usage.ru_stime * 1e9),
                    subprocess_peak_rss_bytes=usage.ru_maxrss
                    * (1 if sys.platform == "darwin" else 1024),
                    load_before=load_before,
                    load_after=os.getloadavg(),
                    build_mode=modes[0],
                    phases=records["PARQUET_BULK_PHASES"],
                    work=records["PARQUET_BULK_WORK"],
                    source_reuse=records["PARQUET_BULK_SOURCE"],
                    projection=records["PARQUET_BULK_PROJECTION"],
                    memory=records["PARQUET_BULK_MEMORY"],
                    recall=records["PARQUET_BULK_RECALL"],
                    log=str(log),
                )
                row["qualified"] = True
                row["cache_retention_target_bytes"] = 16 * 1024**2
                row["cache_target_exceeded"] = (
                    work["projection_cache_peak_bytes"] > 16 * 1024**2
                )
                write_report(args.output, report)
            except Exception as error:
                row["qualified"] = False
                row["qualification_error"] = f"{type(error).__name__}: {error}"
                write_report(args.output, report)
                raise
            print(
                json.dumps(
                    {
                        k: row[k]
                        for k in (
                            "trial",
                            "variant",
                            "vectors",
                            "total_ns",
                            "subprocess_wall_ns",
                        )
                    }
                ),
                flush=True,
            )
    report["median_total_ns"] = {
        name: median(row["total_ns"] for row in rows if row["variant"] == name)
        for name, *_ in configs
    }
    report["median_subprocess_cpu_ns"] = {
        name: median(
            row["subprocess_user_ns"] + row["subprocess_system_ns"]
            for row in rows
            if row["variant"] == name
        )
        for name, *_ in configs
    }
    write_report(args.output, report)
    print(json.dumps(report["median_total_ns"]), flush=True)


if __name__ == "__main__":
    main()
