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
import hashlib
import importlib.util
import json
import platform
import statistics
import sys
import time
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
    parser.add_argument("--delay-ms", type=float, default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 1 or not 1 <= args.units <= 16 or args.delay_ms < 0:
        parser.error("positive rounds, 1..16 units, nonnegative delay required")
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
                    row = case.server.index_documents[0]["_source"]
                    # Distinct images/fields, rather than measuring only dedup
                    # of repeated references to one small image.
                    for i, unit in enumerate(row["grep"]["evidence"]):
                        image = case.image_fixture(width=i + 2, height=3)
                        field = f"preview{i}"
                        row["payloads"][field] = (
                            "data:image/png;base64," + base64.b64encode(image).decode()
                        )
                        unit["asset"].update(
                            field=f"payloads.{field}",
                            sha256=hashlib.sha256(image).hexdigest(),
                            byte_length=len(image),
                        )
                    case.server.delay = args.delay_ms / 1000
                    for phase in ("cold", "warm"):
                        case.server.requests.clear()
                        case.server.index_requests.clear()
                        start = time.monotonic_ns()
                        result = case.raw_cli(
                            *case.decision_args(),
                            "--media=images",
                            "--json",
                            "--table-config",
                            config,
                            "--batch-size=8",
                            "--decision-concurrency=8",
                        )
                        elapsed = time.monotonic_ns() - start
                        events = [
                            json.loads(line) for line in result.stdout.splitlines()
                        ]
                        evidence = sorted(
                            [
                                (r["data"], r["probability"], r["source"]["revision"])
                                for r in events
                                if r["type"] == "evidence"
                            ],
                            key=lambda r: r[0]["unit"],
                        )
                        if canonical is None:
                            canonical = evidence
                        if evidence != canonical or len(evidence) != args.units:
                            raise AssertionError("baseline/candidate output differs")
                        requests = case.server.requests
                        samples.append(
                            {
                                "variant": variant,
                                "round": round_id,
                                "phase": phase,
                                "elapsed_ns": elapsed,
                                "provider_requests": len(requests),
                                "index_requests": len(case.server.index_requests),
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
                            "provider_requests",
                            "index_requests",
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
        "rounds": args.rounds,
        "per_request_delay_ms": args.delay_ms,
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
