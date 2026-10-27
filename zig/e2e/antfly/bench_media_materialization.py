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
import random
import statistics
import struct
import time
import zlib
from pathlib import Path

import requests
from bench_source_projection import file_info, measure
from conftest import StandaloneAntflyServer, find_free_port
from test_grep_indexed import logical_process_reader


def png(size, seed):
    pixels = random.Random(seed).randbytes(size * size * 4)
    rows = b"".join(
        b"\0" + pixels[i * size * 4 : (i + 1) * size * 4] for i in range(size)
    )

    def chunk(kind, data):
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data))
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--queries-per-phase", type=int, default=5)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.rounds < 1 or not 1 <= args.queries_per_phase <= 100:
        parser.error("positive rounds and 1..100 queries required")
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.binary.resolve()}
    samples = []
    corpora = {}
    config = args.output.with_suffix(".config.json")
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(json.dumps({"public_inference": {"enabled": True}}))
    try:
        for name, size, shared_mask, duplicates in [
            ("tiny_shared_duplicates", 1, True, True),
            ("tiny_distinct_unique", 1, False, False),
            ("large_shared_duplicates", 256, True, True),
            ("large_distinct_unique", 256, False, False),
            ("large_distinct_duplicates", 256, False, True),
        ]:
            documents = {}
            references = []
            for i in range(4):
                data = png(size, 100 if duplicates else 100 + i)
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
                            "max_pixels": size * size,
                        },
                    }
                )
            request = {"input": [{"role": "user", "content": references}]}
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
                                table, json={"num_shards": 1}, timeout=30
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
                                # This gate follows all source authorization,
                                # revision, digest, size, MIME and pixel checks.
                                # No model is resolved and no provider is called.
                                assert (
                                    response.json()["error"]["code"] == "ModelRequired"
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
                        server.stop()
    finally:
        config.unlink(missing_ok=True)
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
        "schema_version": 1,
        "scope": "Four authorized table images per request, warm storage after ingestion, shared host. Materialization and adaptation end at ModelRequired; no model/provider execution or billing measured. Whole-process CPU and sampled RSS include background work.",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "rounds": args.rounds,
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
