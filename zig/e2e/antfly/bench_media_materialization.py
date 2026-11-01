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

"""Paired authorized media materialization, stopping before model execution."""

import argparse
import base64
import hashlib
import json
import os
import platform
import statistics
import re
from benchmark_pairing import summarize, validate_rounds
import time
from pathlib import Path

import requests
from bench_source_projection import file_info, measure
from conftest import StandaloneAntflyServer, find_free_port
from test_grep_indexed import logical_process_reader
from media_fixture import png


WORKLOADS = [
    ("tiny_shared_duplicates", 1, True, True, 4),
    ("tiny_distinct_unique", 1, False, False, 4),
    ("medium_shared_duplicates", 64, True, True, 4),
    ("medium_distinct_unique", 64, False, False, 4),
    ("crossover_shared_duplicates", 128, True, True, 4),
    ("crossover_distinct_unique", 128, False, False, 4),
    ("large_shared_duplicates", 256, True, True, 4),
    ("concurrent_shared_duplicates", 256, True, True, 4),
    ("large_distinct_unique", 256, False, False, 4),
    ("large_distinct_duplicates", 256, False, True, 4),
    ("large_distinct_unique_eight", 256, False, False, 8),
    ("skewed_distinct_eight", 512, False, False, 8),
    ("large_512_distinct_unique_eight", 512, False, False, 8),
    ("large_distinct_repeated_refs", 256, False, False, 4),
    ("large_512_distinct_repeated_refs", 512, False, False, 4),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=8)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--queries-per-phase", type=int, default=5)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--workload",
        action="append",
        choices=[case[0] for case in WORKLOADS],
        help="repeat to select cases; default: all",
    )
    parser.add_argument(
        "--profile-source-reuse",
        action="store_true",
        help="separate untimed attribution of native verification receipts",
    )
    parser.add_argument(
        "--native-source-path",
        action="store_true",
        help="configured native decider preparation; stops at evidence/question validation before model execution",
    )
    args = parser.parse_args()
    try:
        validate_rounds(args.rounds, attribution=args.profile_source_reuse)
    except ValueError as error:
        parser.error(str(error))
    if args.rounds < 1 or not 1 <= args.queries_per_phase <= 100:
        parser.error("positive rounds and 1..100 queries required")
    if not 1 <= args.shards <= 16:
        parser.error("shards must be in 1..16")
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.binary.resolve()}
    samples = []
    reuse_counts = []
    previous_reuse_profile = os.environ.get("ANTFLY_BENCH_SOURCE_REUSE")
    if args.profile_source_reuse:
        os.environ["ANTFLY_BENCH_SOURCE_REUSE"] = "1"
    else:
        os.environ.pop("ANTFLY_BENCH_SOURCE_REUSE", None)
    corpora = {}
    config = args.output.with_suffix(".config.json")
    config.parent.mkdir(parents=True, exist_ok=True)
    fixture_config = {"public_inference": {"enabled": True}}
    if args.native_source_path:
        fixture_config["deciders"] = {
            "source_bench": {
                "decision_method": "generated",
                "generator": {
                    "provider": "antfly",
                    "model": "unused-source-materialization-fixture",
                    "url": "",
                },
                "probabilities": "verdict_only",
            }
        }
    config.write_text(json.dumps(fixture_config))
    try:
        for name, size, shared_mask, duplicates, count in WORKLOADS:
            if args.workload and name not in args.workload:
                continue
            documents = {}
            references = []
            for i in range(count):
                image_size = 64 if name == "skewed_distinct_eight" and i != 0 else size
                data = png(image_size, 100 if duplicates else 100 + i)
                field = "preview" if shared_mask else f"image{i}"
                doc_id = f"doc-{i}"
                documents[doc_id] = {
                    "revision": "r1",
                    "permissions": {"public": True},
                    "images": {
                        field: "data:image/png;base64,"
                        + base64.b64encode(data).decode("ascii")
                    },
                }
                references.append(
                    {
                        "type": "input_image",
                        "source": {
                            "table": "media_bench",
                            "document_id": doc_id,
                            "field": "images." + field,
                            "revision_field": "revision",
                            "revision": "r1",
                            "permissions_field": "permissions",
                            "sha256": hashlib.sha256(data).hexdigest(),
                            "mime_type": "image/png",
                            "byte_length": len(data),
                            "max_pixels": image_size * image_size,
                        },
                    }
                )
            if name == "concurrent_shared_duplicates":
                # Separate admission groups with equivalent per-row ID filters;
                # query/ACL work stays independent while codec work can join.
                for part in references:
                    part["source"]["filter_query"] = {
                        "ids": [part["source"]["document_id"]]
                    }
            if name.endswith("repeated_refs"):
                # Same row/field, independently checked limits: canonical source
                # dedup must retain each claim, while storage/output can share.
                references = [
                    {
                        **part,
                        "source": {**part["source"], "max_pixels": size * size + j},
                    }
                    for part in references
                    for j in range(2 if size == 512 else 4)
                ]
            request = {"input": [{"role": "user", "content": references}]}
            if args.native_source_path:
                request["model"] = "deciders/source_bench"
            expected_error = "ModelRequired"
            if args.native_source_path:
                expanded_parts = []
                for part in references:
                    reference = part["source"]
                    value = documents[reference["document_id"]]
                    for component in reference["field"].split("."):
                        value = value[component]
                    expanded_parts.append({"type": "input_image", "image_url": value})
                evidence_bytes = len(
                    json.dumps(
                        [{"role": "user", "content": expanded_parts}],
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode()
                )
                expected_error = (
                    "DecisionLimitExceeded"
                    if evidence_bytes > 1024 * 1024
                    else "InvalidDecisionSpecification"
                )
            corpora[name] = hashlib.sha256(
                json.dumps(documents, sort_keys=True).encode()
            ).hexdigest()
            for round_id in range(args.rounds):
                order = (
                    ("baseline", "candidate")
                    if round_id % 2 == 0
                    else ("candidate", "baseline")
                )
                for variant in order:
                    server = StandaloneAntflyServer(
                        str(binaries[variant]),
                        "127.0.0.1",
                        find_free_port(),
                        config_path=config.resolve(),
                    )
                    try:
                        with requests.Session() as session:
                            table = server.api_url + "/tables/media_bench"
                            session.post(
                                table, json={"num_shards": args.shards}, timeout=30
                            ).raise_for_status()
                            deadline = time.monotonic() + 30
                            while session.get(table, timeout=5).status_code != 200:
                                assert time.monotonic() < deadline
                                time.sleep(0.05)
                            session.post(
                                table + "/batch",
                                json={"inserts": documents, "sync_level": "full_index"},
                                timeout=60,
                            ).raise_for_status()
                            read = logical_process_reader(server.proc.pid)
                            for query_id in range(args.queries_per_phase):
                                response, usage = measure(
                                    read,
                                    lambda: session.post(
                                        server.url + "/ai/v1/decisions",
                                        json=request,
                                        timeout=60,
                                    ),
                                    expected_status=400,
                                )
                                # Both gates follow materialization; the native
                                # fixture omits questions, so decideRequest fails
                                # before constructing a model/provider runtime.
                                assert (
                                    response.json()["error"]["code"] == expected_error
                                ), response.text
                                samples.append(
                                    {
                                        "variant": variant,
                                        "workload": name,
                                        "round": round_id,
                                        "query_id": query_id,
                                        **usage,
                                    }
                                )
                    except BaseException:
                        args.output.with_suffix(
                            f".{name}.{variant}.failure.log"
                        ).write_text(server.debug_logs())
                        raise
                    finally:
                        if args.profile_source_reuse:
                            counts = [
                                dict(
                                    zip(
                                        (
                                            "verifications",
                                            "byte_hits",
                                            "proof_hits",
                                            "native_results",
                                            "native_views",
                                            "native_witnesses",
                                            "flight_waits",
                                        ),
                                        (int(value or 0) for value in match),
                                    )
                                )
                                for match in re.findall(
                                    r"antfly_bench_source_reuse verifications=(\d+) byte_hits=(\d+) proof_hits=(\d+)(?: native_results=(\d+) native_views=(\d+) native_witnesses=(\d+))?(?: flight_waits=(\d+))?",
                                    server.debug_logs(),
                                )
                            ]
                            reuse_counts.append(
                                {
                                    "variant": variant,
                                    "round": round_id,
                                    "workload": name,
                                    "counters": counts,
                                    "field_views": [
                                        dict(
                                            zip(
                                                (
                                                    "rows",
                                                    "requested",
                                                    "authority",
                                                    "full",
                                                    "bytes_match",
                                                    "selected",
                                                    "members",
                                                    "witnesses",
                                                ),
                                                map(int, values),
                                            )
                                        )
                                        for values in re.findall(
                                            r"antfly_bench_field_view rows=(\d+) requested=(\d+) authority=(\d+) full=(\d+) bytes_match=(\d+) selected=(\d+) members=(\d+) witnesses=(\d+)",
                                            server.debug_logs(),
                                        )
                                    ],
                                }
                            )
                        server.stop()
    finally:
        config.unlink(missing_ok=True)
        if previous_reuse_profile is None:
            os.environ.pop("ANTFLY_BENCH_SOURCE_REUSE", None)
        else:
            os.environ["ANTFLY_BENCH_SOURCE_REUSE"] = previous_reuse_profile
    medians = []
    for workload in corpora:
        for variant in binaries:
            rows = [
                r
                for r in samples
                if r["workload"] == workload and r["variant"] == variant
            ]
            medians.append(
                {
                    "workload": workload,
                    "variant": variant,
                    **{
                        k: statistics.median(r[k] for r in rows)
                        for k in (
                            "elapsed_ns",
                            "server_cpu_ns",
                            "peak_rss_bytes",
                            "response_bytes",
                        )
                    },
                }
            )
    report = {
        "schema_version": 2,
        "paired": summarize(
            samples, ("workload",), attribution=args.profile_source_reuse
        ),
        "profile_source_reuse": args.profile_source_reuse,
        "native_source_path": args.native_source_path,
        "source_reuse_counts": reuse_counts,
        "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": (
            "Configured native decider materialization ends at evidence limits or missing-question validation; no model/provider runtime is constructed. "
            if args.native_source_path
            else ""
        )
        + "Four/eight authorized table images per request (skewed: one 512x512, seven 64x64), warm storage after ingestion, shared host. Legacy materialization ends at ModelRequired; no model/provider execution or billing measured. Whole-process CPU and sampled RSS include background work.",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "rounds": args.rounds,
        "shards": args.shards,
        "queries_per_phase": args.queries_per_phase,
        "binaries": {k: file_info(v) for k, v in binaries.items()},
        "corpus_sha256": corpora,
        "medians": medians,
        "samples": samples,
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(medians, indent=2))


if __name__ == "__main__":
    main()
