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

"""Paired conditional-hit reads without large-value response transfer."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
from benchmark_pairing import summarize, validate_rounds
import time
import requests
from bench_source_projection import file_info, measure
from conftest import StandaloneAntflyServer, find_free_port
from test_grep_indexed import conditional_select, logical_process_reader


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("binary", type=Path)
    p.add_argument("--baseline", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--documents", type=int, default=64)
    p.add_argument("--value-bytes", type=int, default=1048576)
    p.add_argument("--rounds", type=int, default=8)
    p.add_argument(
        "--metadata-shape",
        choices=("scalar", "nested"),
        default="scalar",
        help="nested projects an object with arrays in addition to scalar receipt fields",
    )
    p.add_argument("--queries-per-phase", type=int, default=5)
    args = p.parse_args()
    try:
        validate_rounds(args.rounds, attribution=False)
    except ValueError as error:
        p.error(str(error))
    if not (
        1 <= args.documents <= 64
        and 16384 <= args.value_bytes <= 1048576
        and args.rounds >= 1
        and 1 <= args.queries_per_phase <= 100
    ):
        p.error(
            "1..64 documents, 16 KiB..1 MiB values, positive rounds and 1..100 reads required"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    text = ("payload " * (args.value_bytes // 8 + 1))[: args.value_bytes]
    documents = {
        f"doc-{i:04}": {
            "metadata": {"revision": "r1", "allowed": True},
            "payload": {"large": text + str(i)},
        }
        for i in range(args.documents)
    }
    if args.metadata_shape == "nested":
        for i, document in enumerate(documents.values()):
            document["metadata"]["details"] = {
                "owners": [{"id": j, "role": "viewer"} for j in range(8)],
                "labels": ["code", "docs", f"document-{i}"],
            }
    corpus = hashlib.sha256()
    for key, document in documents.items():
        corpus.update(json.dumps([key, document], sort_keys=True).encode())
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.binary.resolve()}
    samples = []
    expected_hash = None
    for round_id in range(args.rounds):
        order = (
            ("baseline", "candidate")
            if round_id % 2 == 0
            else ("candidate", "baseline")
        )
        for variant in order:
            server = StandaloneAntflyServer(
                str(binaries[variant]), "127.0.0.1", find_free_port()
            )
            try:
                with requests.Session() as session:
                    table = server.api_url + "/tables/narrow_batches"
                    session.post(
                        table, json={"num_shards": 1}, timeout=60
                    ).raise_for_status()
                    end = time.monotonic() + 60
                    while session.get(table, timeout=5).status_code != 200:
                        assert time.monotonic() < end
                        time.sleep(0.05)
                    items = list(documents.items())
                    # Keep ingress bounded independently of corpus size.
                    for start in range(0, len(items), 8):
                        session.post(
                            table + "/batch",
                            json={
                                "inserts": dict(items[start : start + 8]),
                                "sync_level": "full_index",
                            },
                            timeout=120,
                        ).raise_for_status()
                    query = {
                        "query": {"bool": {"filter": {"ids": list(documents)}}},
                        "fields": (
                            ["metadata"]
                            if args.metadata_shape == "nested"
                            else ["metadata.revision", "metadata.allowed"]
                        ),
                        "limit": args.documents,
                    }
                    seed = {
                        **query,
                        "select": {
                            "identity": {
                                "call": "value_identity",
                                "args": [{"field": "payload.large"}],
                            }
                        },
                    }
                    response = session.post(table + "/query", json=seed, timeout=60)
                    response.raise_for_status()
                    result = response.json()["responses"][0]
                    assert result["status"] == 200, result
                    known = {
                        h["_id"]: h["_computed"]["identity"]
                        for h in result["hits"]["hits"]
                    }
                    assert set(known) == set(documents)
                    request = {
                        **query,
                        "select": conditional_select("payload.large"),
                        "params": {"known": known},
                    }
                    read = logical_process_reader(server.proc.pid)
                    for query_id in range(args.queries_per_phase):
                        response, usage = measure(
                            read,
                            lambda: session.post(
                                table + "/query", json=request, timeout=60
                            ),
                        )
                        result = response.json()["responses"][0]
                        assert result["status"] == 200, result
                        hits = result["hits"]["hits"]
                        assert {h["_id"] for h in hits} == set(documents)
                        for hit in hits:
                            assert hit["_source"] == {
                                "metadata": documents[hit["_id"]]["metadata"]
                            }
                            values = hit["_computed"]
                            assert (
                                values["identity"] == known[hit["_id"]]
                                and values["changed"] is False
                                and values["value"] is None
                            )
                        digest = hashlib.sha256(
                            json.dumps(
                                {
                                    "hits": sorted(
                                        (
                                            h["_id"],
                                            h["_source"],
                                            h["_computed"],
                                            h.get("_score"),
                                        )
                                        for h in hits
                                    ),
                                    "total": result["hits"].get("total"),
                                },
                                sort_keys=True,
                            ).encode()
                        ).hexdigest()
                        if expected_hash is None:
                            expected_hash = digest
                        assert expected_hash == digest, (
                            "baseline/candidate conditional output differs"
                        )
                        samples.append(
                            {
                                "variant": variant,
                                "round": round_id,
                                "query_id": query_id,
                                **usage,
                            }
                        )
            except BaseException:
                args.output.with_suffix(
                    f".{variant}.{round_id}.failure.log"
                ).write_text(server.debug_logs())
                raise
            finally:
                server.stop()
    medians = []
    for variant in binaries:
        values = [s for s in samples if s["variant"] == variant]
        medians.append(
            {
                "variant": variant,
                **{
                    k: statistics.median(s[k] for s in values)
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
        "paired": summarize(samples, (), attribution=False),
        "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": "Identity-only warmup then conditional hits; metadata shape distinguishes scalar receipts from nested object/array reconstruction. No large payload output or model; one shard; shared host; CPU whole process, RSS sampled",
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "rounds": args.rounds,
        "queries_per_phase": args.queries_per_phase,
        "documents": args.documents,
        "value_bytes": args.value_bytes,
        "metadata_shape": args.metadata_shape,
        "corpus_sha256": corpus.hexdigest(),
        "output_sha256": expected_hash,
        "binaries": {k: file_info(v) for k, v in binaries.items()},
        "medians": medians,
        "samples": samples,
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(medians, indent=2))


if __name__ == "__main__":
    main()
