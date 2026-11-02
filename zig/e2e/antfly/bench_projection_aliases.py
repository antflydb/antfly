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

"""Paired source/alias identities with identical outputs and bounded copy chains."""

import argparse
import hashlib
import json
import os
import platform
import statistics
import time
from pathlib import Path

import requests
from bench_source_projection import file_info, measure
from conftest import StandaloneAntflyServer, find_free_port
from test_grep_indexed import logical_process_reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--queries-per-phase", type=int, default=5)
    parser.add_argument("--documents", type=int, default=4)
    parser.add_argument("--value-bytes", type=int, default=262144)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not (
        args.rounds > 0
        and 1 <= args.queries_per_phase <= 100
        and 1 <= args.documents <= 8
        and 16384 <= args.value_bytes <= 1048576
    ):
        parser.error(
            "positive rounds, 1..100 reads, 1..8 documents, 16 KiB..1 MiB values required"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.binary.resolve()}
    samples = []
    hashes = {}
    corpora = {}
    for workload, unit in (
        ("escaped_opaque", 'line\nquote"\\'),
        ("unicode_string", "café λ 🚀"),
    ):
        text = unit * (args.value_bytes // len(unit.encode()))
        documents = {
            f"doc-{i}": {
                "payload": [{"text": text + str(i), "n": i}]
                if workload == "escaped_opaque"
                else text + str(i)
            }
            for i in range(args.documents)
        }
        corpora[workload] = hashlib.sha256(
            json.dumps(documents, sort_keys=True).encode()
        ).hexdigest()
        for copies in (0, 3):
            case = f"{workload}_copies{copies}"
            for trial in range(args.rounds):
                for variant in (
                    ("baseline", "candidate")
                    if trial % 2 == 0
                    else ("candidate", "baseline")
                ):
                    server = StandaloneAntflyServer(
                        str(binaries[variant]), "127.0.0.1", find_free_port()
                    )
                    try:
                        with requests.Session() as session:
                            table = server.api_url + "/tables/alias_bench"
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
                            for mode in (
                                ("field", "alias")
                                if trial % 2 == 0
                                else ("alias", "field")
                            ):
                                selected = {"value": {"field": "payload"}}
                                last = "value"
                                for i in range(copies):
                                    selected[f"copy{i}"] = {"ref": last}
                                    last = f"copy{i}"
                                selected["identity"] = {
                                    "call": "value_identity",
                                    "args": [
                                        {"field": "payload"}
                                        if mode == "field"
                                        else {"ref": last}
                                    ],
                                }
                                selected["missing"] = {
                                    "call": "is_missing",
                                    "args": [
                                        {"field": "payload"}
                                        if mode == "field"
                                        else {"ref": last}
                                    ],
                                }
                                body = {
                                    "query": {"match_all": {}},
                                    "limit": args.documents,
                                    "fields": [],
                                    "select": selected,
                                }
                                for repeat in range(args.queries_per_phase):
                                    response, usage = measure(
                                        read,
                                        lambda: session.post(
                                            table + "/query", json=body, timeout=60
                                        ),
                                    )
                                    hits = response.json()["responses"][0]["hits"][
                                        "hits"
                                    ]
                                    assert len(hits) == len(documents)
                                    values = sorted(
                                        (h["_id"], h["_computed"]) for h in hits
                                    )
                                    digest = hashlib.sha256(
                                        json.dumps(values, sort_keys=True).encode()
                                    ).hexdigest()
                                    if case in hashes:
                                        assert hashes[case] == digest, (
                                            case,
                                            mode,
                                            variant,
                                        )
                                    hashes[case] = digest
                                    samples.append(
                                        {
                                            "workload": case,
                                            "variant": variant,
                                            "mode": mode,
                                            "round": trial,
                                            "query_id": repeat,
                                            **usage,
                                        }
                                    )
                    except BaseException:
                        args.output.with_suffix(
                            f".{case}.{variant}.{trial}.failure.log"
                        ).write_text(server.debug_logs())
                        raise
                    finally:
                        server.stop()
    medians = []
    for case in hashes:
        for variant in binaries:
            for mode in ("field", "alias"):
                values = [
                    s
                    for s in samples
                    if s["workload"] == case
                    and s["variant"] == variant
                    and s["mode"] == mode
                ]
                medians.append(
                    {
                        "workload": case,
                        "variant": variant,
                        "mode": mode,
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
        "schema_version": 1,
        "scope": "Paired field/alias identities and missing checks with identical outputs, warm synthetic storage, whole-process CPU and sampled RSS on shared host; no inference",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "rounds": args.rounds,
        "queries_per_phase": args.queries_per_phase,
        "documents": args.documents,
        "value_bytes": args.value_bytes,
        "binaries": {k: file_info(v) for k, v in binaries.items()},
        "corpus_sha256": corpora,
        "output_sha256": hashes,
        "medians": medians,
        "samples": samples,
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(medians, indent=2))


if __name__ == "__main__":
    main()
