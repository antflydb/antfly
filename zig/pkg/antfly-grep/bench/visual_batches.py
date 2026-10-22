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

"""Paired local visual-unit transport fixture; no live models or token-cost claim."""

import argparse
import base64
import copy
import hashlib
import importlib.util
import json
import platform
import statistics
import struct
import subprocess
import sys
import time
import zlib
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--fixture-library", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--units", type=int, default=8)
    parser.add_argument("--parents", type=int, default=1)
    parser.add_argument("--page-size", type=int, default=16)
    parser.add_argument("--image-bytes", type=int, default=0)
    parser.add_argument(
        "--stream",
        action="store_true",
        help="disable ranking to measure first streaming output",
    )
    parser.add_argument(
        "--source-projection",
        action="store_true",
        help="enable conditional source identities on the fixture server",
    )
    parser.add_argument("--delay-ms", type=float, default=20)
    parser.add_argument("--slow-first-ms", type=float, default=0)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (
        args.rounds < 1
        or not 1 <= args.units <= 16
        or not 1 <= args.parents <= 64
        or not 1 <= args.page_size <= 1000
        or not 0 <= args.image_bytes <= 500000
        or args.delay_ms < 0
        or args.slow_first_ms < 0
        or not 1 <= args.batch_size <= 128
    ):
        parser.error(
            "positive rounds, 1..16 units, 1..64 parents, 1..1000 page size, nonnegative delay required"
        )
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.binary.resolve()}
    spec = importlib.util.spec_from_file_location(
        "visual_fixture", Path(__file__).resolve().parents[1] / "tests/test_cli.py"
    )
    fixture = importlib.util.module_from_spec(spec)
    argv = sys.argv
    try:
        sys.argv = [
            "fixture",
            str(binaries["candidate"]),
            str(args.fixture_library.resolve()),
        ]
        spec.loader.exec_module(fixture)
    finally:
        sys.argv = argv
    samples = []
    canonical = None
    fixture.CliTests.setUpClass()
    try:
        for round_id in range(args.rounds):
            order = (
                ("baseline", "candidate")
                if round_id % 2 == 0
                else ("candidate", "baseline")
            )
            for variant in order:
                fixture.BINARY = str(binaries[variant])
                case = fixture.CliTests("runTest")
                case.setUp()
                try:
                    for key in ("OPENAI_API_KEY", "ANTFLY_API_KEY"):
                        case.env.pop(key, None)
                    case.env["ANTFLY_GREP_TOKEN"] = "visual-fixture"
                    case.server.allow_asset_order = variant == "baseline"
                    config = case.visual_document(units=args.units)
                    template = case.server.index_documents[0]
                    case.server.index_documents = []
                    for parent in range(args.parents):
                        entry = copy.deepcopy(template)
                        entry["_id"] = f"{parent:04}"
                        entry["_sort"] = [entry["_id"]]
                        entry["_source"]["grep"]["path"] = (
                            f"Drive/file-{parent:04}.slides"
                        )
                        # Distinct valid images for every parent and unit;
                        # equal projection shapes do not imply equal content.
                        for i, unit in enumerate(entry["_source"]["grep"]["evidence"]):
                            image = case.image_fixture(
                                width=i + 2 + parent * 17, height=3
                            )
                            if args.image_bytes > len(image) + 20:
                                payload = b"padding\0" + b"x" * (
                                    args.image_bytes - len(image) - 20
                                )
                                chunk = (
                                    struct.pack(">I", len(payload))
                                    + b"tEXt"
                                    + payload
                                    + struct.pack(">I", zlib.crc32(b"tEXt" + payload))
                                )
                                image = image[:-12] + chunk + image[-12:]
                            field = f"preview{i}"
                            entry["_source"]["payloads"][field] = (
                                "data:image/png;base64,"
                                + base64.b64encode(image).decode()
                            )
                            unit["asset"].update(
                                field=f"payloads.{field}",
                                sha256=hashlib.sha256(image).hexdigest(),
                                byte_length=len(image),
                            )
                        case.server.index_documents.append(entry)
                    settings = json.loads(Path(config).read_text())
                    settings["page_size"] = args.page_size
                    Path(config).write_text(json.dumps(settings))
                    case.server.delay = args.delay_ms / 1000
                    case.server.slow_first_ms = args.slow_first_ms
                    case.server.source_projection_mode = (
                        "enabled" if args.source_projection else None
                    )
                    for phase in ("cold", "warm"):
                        case.server.requests.clear()
                        case.server.index_requests.clear()
                        case.server.projected_asset_bytes = 0
                        case.server.index_response_bytes = 0
                        start = time.monotonic_ns()
                        process_report = case.root / "child-usage.json"
                        command = [
                            *case.decision_args(),
                            "--media=images",
                            "--json",
                            "--table-config",
                            config,
                            f"--batch-size={args.batch_size}",
                            "--decision-concurrency=8",
                            "--decision-batch-wait-ms=20",
                        ]
                        if args.stream:
                            command.append("--no-rank")
                        result = subprocess.run(
                            [
                                sys.executable,
                                str(
                                    Path(__file__)
                                    .with_name("measure_process.py")
                                    .resolve()
                                ),
                                "--report",
                                str(process_report),
                                "--",
                                str(binaries[variant]),
                                *command,
                            ],
                            cwd=case.root,
                            env=case.env,
                            capture_output=True,
                            timeout=30,
                        )
                        if result.returncode != 0:
                            raise AssertionError(result.stderr.decode())
                        usage = json.loads(process_report.read_text())
                        harness_elapsed = time.monotonic_ns() - start
                        events = [
                            json.loads(line) for line in result.stdout.splitlines()
                        ]
                        evidence = sorted(
                            [
                                (r["data"], r["probability"], r["source"]["revision"])
                                for r in events
                                if r["type"] == "evidence"
                            ],
                            key=lambda r: (r[0]["path"], r[0]["unit"]),
                        )
                        if canonical is None:
                            canonical = evidence
                        if (
                            evidence != canonical
                            or len(evidence) != args.units * args.parents
                        ):
                            raise AssertionError("baseline/candidate output differs")
                        requests = case.server.requests
                        samples.append(
                            {
                                "variant": variant,
                                "round": round_id,
                                "phase": phase,
                                **usage,
                                "harness_elapsed_ns": harness_elapsed,
                                "provider_requests": len(requests),
                                "index_requests": len(case.server.index_requests),
                                "index_response_bytes": case.server.index_response_bytes,
                                "projected_asset_bytes": case.server.projected_asset_bytes,
                                "logical_provider_json_bytes": sum(
                                    len(
                                        json.dumps(
                                            body,
                                            separators=(",", ":"),
                                            ensure_ascii=False,
                                        ).encode()
                                    )
                                    for _, _, body in requests
                                ),
                                "evaluated_units": len(evidence),
                            }
                        )
                finally:
                    case.server.delay = 0
                    case.tearDown()
    finally:
        fixture.CliTests.tearDownClass()
    medians = []
    for variant in binaries:
        for phase in ("cold", "warm"):
            selected = [
                r for r in samples if r["variant"] == variant and r["phase"] == phase
            ]
            medians.append(
                {
                    "variant": variant,
                    "phase": phase,
                    **{
                        key: statistics.median(r[key] for r in selected)
                        for key in (
                            "elapsed_ns",
                            "first_stdout_ns",
                            "user_cpu_ns",
                            "system_cpu_ns",
                            "peak_rss_bytes",
                            "provider_requests",
                            "index_requests",
                            "index_response_bytes",
                            "projected_asset_bytes",
                            "logical_provider_json_bytes",
                            "evaluated_units",
                        )
                    },
                }
            )
    report = {
        "schema_version": 1,
        "scope": "constant-score loopback image fixture with artificial delay per HTTP request; no live relevance, model-token or universal latency claim",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "units": args.units,
        "parents": args.parents,
        "page_size": args.page_size,
        "requested_image_bytes": args.image_bytes,
        "ranked": not args.stream,
        "rounds": args.rounds,
        "per_request_delay_ms": args.delay_ms,
        "slow_first_ms": args.slow_first_ms,
        "batch_size": args.batch_size,
        "conditional_source_projection": args.source_projection,
        "binaries": {
            name: {
                "path": str(path),
                "sha256": digest(path),
                "bytes": path.stat().st_size,
            }
            for name, path in binaries.items()
        },
        "output_sha256": hashlib.sha256(
            json.dumps(canonical, sort_keys=True).encode()
        ).hexdigest(),
        "medians": medians,
        "samples": samples,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(medians, indent=2))


if __name__ == "__main__":
    main()
