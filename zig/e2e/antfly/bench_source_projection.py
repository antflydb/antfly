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

"""Paired native-server source delivery qualification; no model or live data."""

import argparse
import hashlib
import json
import os
import platform
import re
import statistics
import threading
import time
from pathlib import Path

import requests

from conftest import StandaloneAntflyServer, find_free_port
from test_grep_indexed import logical_process_reader


def file_info(path):
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def measure(read, operation):
    before = read()
    peaks = dict(before)
    stop = threading.Event()

    def sample():
        while not stop.wait(0.005):
            current = read()
            for key in ("rss_bytes", "footprint_bytes"):
                if current[key] is not None:
                    peaks[key] = max(peaks[key] or 0, current[key])

    sampler = threading.Thread(target=sample)
    sampler.start()
    started = time.monotonic_ns()
    try:
        response = operation()
        elapsed = time.monotonic_ns() - started
        after = read()
    finally:
        stop.set()
        sampler.join()
    assert response.ok, (response.status_code, response.text[:4096])
    for key in ("rss_bytes", "footprint_bytes"):
        if after[key] is not None:
            peaks[key] = max(peaks[key] or 0, after[key])
    return response, {
        "elapsed_ns": elapsed,
        "server_cpu_ns": after["cpu_ns"] - before["cpu_ns"],
        "peak_rss_bytes": peaks["rss_bytes"],
        "peak_footprint_bytes": peaks["footprint_bytes"],
        "response_bytes": len(response.content),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument(
        "--queries-per-phase",
        type=int,
        default=1,
        help="repeat reads on each fresh server; keep parsing outside timings",
    )
    parser.add_argument("--documents", type=int, default=8)
    parser.add_argument("--value-bytes", type=int, default=262144)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--mask", choices=("exact", "parent", "wildcard", "exclude"), default="exact"
    )
    parser.add_argument(
        "--workload",
        choices=(
            "ascii",
            "escaped",
            "unicode",
            "numeric",
            "wide",
            "opaque",
            "small",
            "mixed",
        ),
        default="ascii",
    )
    args = parser.parse_args()
    if (
        not 1 <= args.documents <= 64
        or not 16384 <= args.value_bytes <= 1048576
        or args.rounds < 1
        or not 1 <= args.queries_per_phase <= 100
    ):
        parser.error(
            "1..64 documents, 16 KiB..1 MiB values and positive rounds required"
        )
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.binary.resolve()}

    def document(i):
        workload = args.workload
        if workload == "mixed":
            workload = (
                "ascii",
                "escaped",
                "unicode",
                "numeric",
                "wide",
                "opaque",
                "small",
            )[i % 7]
        unit = {"ascii": "x", "escaped": 'line\nquote"\\', "unicode": "café λ 🚀"}.get(
            workload, "x"
        )
        large = (unit * (args.value_bytes // len(unit.encode()) + 1))[
            : args.value_bytes
        ] + str(i)
        if workload == "numeric":
            large = 9007199254740993 + i
        elif workload == "opaque":
            large = [{"text": large, "number": i}]
        elif workload == "small":
            large = f"small-{i}"
        payload = {"large": large}
        if workload == "wide":
            payload.update({f"sibling{j:03}": j for j in range(128)})
        out = {"metadata": {"revision": "r1", "allowed": True}, "payload": payload}
        if workload == "numeric":
            out["padding"] = "p" * args.value_bytes
        return out

    documents = {f"doc-{i:04}": document(i) for i in range(args.documents)}
    corpus = hashlib.sha256(json.dumps(documents, sort_keys=True).encode()).hexdigest()
    samples = []
    output_hashes = {}
    certificate_counts = []
    previous_profile = os.environ.get("ANTFLY_BENCH_QUERY_PROFILE")
    os.environ["ANTFLY_BENCH_QUERY_PROFILE"] = "1"
    try:
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
                    session = requests.Session()
                    base = server.api_url + "/tables/identity_bench"
                    created = session.post(base, json={"num_shards": 1}, timeout=60)
                    created.raise_for_status()
                    deadline = time.monotonic() + 30
                    while session.get(base, timeout=5).status_code != 200:
                        if time.monotonic() >= deadline:
                            raise AssertionError(
                                "committed table did not become visible"
                            )
                        time.sleep(0.05)
                    read = logical_process_reader(server.proc.pid)
                    _, ingestion = measure(
                        read,
                        lambda: session.post(
                            base + "/batch",
                            json={"inserts": documents, "sync_level": "full_index"},
                            timeout=60,
                        ),
                    )
                    samples.append(
                        {
                            "variant": variant,
                            "round": round_id,
                            "phase": "ingest",
                            **ingestion,
                        }
                    )
                    request = {
                        "query": {"bool": {"filter": {"ids": list(documents)}}},
                        "fields": {
                            "exact": [
                                "metadata.revision",
                                "metadata.allowed",
                                "payload.large",
                            ],
                            "parent": ["metadata", "payload"],
                            "wildcard": ["metadata.*", "payload.*"],
                            "exclude": ["-padding"],
                        }[args.mask],
                        "limit": args.documents,
                        "source_projection": {"fields": [{"field": "payload.large"}]},
                    }
                    for phase in ("ordinary", "full", "conditional"):
                        if phase == "ordinary":
                            phase_request = {
                                k: v
                                for k, v in request.items()
                                if k != "source_projection"
                            }
                        else:
                            phase_request = request
                        for query_id in range(args.queries_per_phase):
                            response, usage = measure(
                                read,
                                lambda: session.post(
                                    base + "/query", json=phase_request, timeout=60
                                ),
                            )
                            result = response.json()["responses"][0]
                            assert result["status"] == 200, result
                            hits = result["hits"]["hits"]
                            assert {h["_id"] for h in hits} == set(documents), result
                            evidence = sorted(
                                (h["_id"], h["_source"], h.get("_source_projection"))
                                for h in hits
                            )
                            digest = hashlib.sha256(
                                json.dumps(evidence, sort_keys=True).encode()
                            ).hexdigest()
                            assert output_hashes.setdefault(phase, digest) == digest, (
                                "baseline/candidate source delivery differs"
                            )
                            for hit in hits:
                                if phase != "ordinary":
                                    state = hit["_source_projection"]["payload.large"]
                                    assert state["state"] == (
                                        "value" if phase == "full" else "unchanged"
                                    ), hit
                                assert hit["_source"]["metadata"] == {
                                    "revision": "r1",
                                    "allowed": True,
                                }
                                assert (
                                    "large" in hit["_source"].get("payload", {})
                                ) == (phase != "conditional")
                            samples.append(
                                {
                                    "variant": variant,
                                    "round": round_id,
                                    "phase": phase,
                                    "query_id": query_id,
                                    **usage,
                                }
                            )
                        if phase != "full":
                            continue
                        request["source_projection"]["fields"][0]["if_match"] = [
                            {
                                "id": h["_id"],
                                "identity": h["_source_projection"]["payload.large"][
                                    "identity"
                                ],
                            }
                            for h in hits
                        ]
                    logs = server.log_path.read_text(errors="replace")
                    counts = [
                        (int(a), int(b))
                        for a, b in re.findall(
                            r"antfly_bench_conditional_source certificate_hits=(\d+) full_fallbacks=(\d+)",
                            logs,
                        )
                    ]
                    if (
                        variant == "candidate"
                        and args.mask == "exact"
                        and args.workload in ("ascii", "escaped", "unicode", "numeric")
                        and sum(a for a, _ in counts)
                        < 2 * args.documents * args.queries_per_phase
                    ):
                        failure = args.output.with_suffix(".server-failure.log")
                        failure.parent.mkdir(parents=True, exist_ok=True)
                        failure.write_text(logs)
                        raise AssertionError(
                            f"Missing certificate-hit evidence: {counts}; see {failure}"
                        )
                    if (
                        variant == "candidate"
                        and args.mask == "exact"
                        and args.workload in ("small", "opaque")
                    ):
                        assert sum(a for a, _ in counts) == 0, counts
                        assert (
                            sum(b for _, b in counts)
                            >= 2 * args.documents * args.queries_per_phase
                        ), counts
                    certificate_counts.append(
                        {
                            "variant": variant,
                            "round": round_id,
                            "certificate_hits": sum(a for a, _ in counts),
                            "full_fallbacks": sum(b for _, b in counts),
                        }
                    )
                    session.close()
                except BaseException:
                    failure = args.output.with_suffix(f".{variant}-failure.log")
                    failure.parent.mkdir(parents=True, exist_ok=True)
                    failure.write_text(server.log_path.read_text(errors="replace"))
                    raise
                finally:
                    server.stop()
    finally:
        if previous_profile is None:
            os.environ.pop("ANTFLY_BENCH_QUERY_PROFILE", None)
        else:
            os.environ["ANTFLY_BENCH_QUERY_PROFILE"] = previous_profile
    medians = []
    for variant in binaries:
        for phase in ("ingest", "ordinary", "full", "conditional"):
            selected = [
                s for s in samples if s["variant"] == variant and s["phase"] == phase
            ]
            medians.append(
                {
                    "variant": variant,
                    "phase": phase,
                    **{
                        key: statistics.median(s[key] for s in selected)
                        for key in (
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
        "workload": args.workload,
        "mask": args.mask,
        "scope": "Native single-shard JSON tables, synthetic varied source values, warm filesystem/storage caches after ingestion; whole server process CPU includes background work; RSS sampled every 5 ms and is not an allocation bound; shared host; no inference",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "rounds": args.rounds,
        "queries_per_phase": args.queries_per_phase,
        "documents": args.documents,
        "value_bytes": args.value_bytes,
        "corpus_sha256": corpus,
        "binaries": {k: file_info(v) for k, v in binaries.items()},
        "output_sha256": output_hashes,
        "certificate_counts": certificate_counts,
        "medians": medians,
        "samples": samples,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(medians, indent=2))


if __name__ == "__main__":
    main()
