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
import copy
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
from test_grep_indexed import logical_process_reader, conditional_select


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
    parser.add_argument(
        "--baseline",
        type=Path,
        help="Optional comparison binary supporting select; defaults to candidate",
    )
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument(
        "--metadata-outputs",
        type=int,
        default=0,
        help="add 0..60 distinct small scalar outputs to qualify wide rows",
    )
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
        "--metadata-updates",
        type=int,
        default=0,
        help="measure primary rewrites with unchanged large payloads and verify conditional identities",
    )
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
        or not 0 <= args.metadata_updates <= 100
        or not 0 <= args.metadata_outputs <= 60
        or args.rounds < 1
        or not 1 <= args.queries_per_phase <= 100
    ):
        parser.error(
            "1..64 documents, 16 KiB..1 MiB values and positive rounds required"
        )
    binaries = {
        "baseline": (args.baseline or args.binary).resolve(),
        "candidate": args.binary.resolve(),
    }

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
        out["metadata"].update(
            {f"extra_{j}": f"value-{j}" for j in range(args.metadata_outputs)}
        )
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
                        "select": conditional_select("payload.large"),
                        "params": {"known": {}},
                    }
                    request["select"].update(
                        {
                            f"extra_{j}": {"field": f"metadata.extra_{j}"}
                            for j in range(args.metadata_outputs)
                        }
                    )
                    for phase in (
                        "ordinary",
                        "reference_full",
                        "full",
                        "conditional",
                        "reference",
                    ):
                        if phase == "ordinary":
                            phase_request = {
                                k: v
                                for k, v in request.items()
                                if k not in ("select", "params")
                            }
                        else:
                            phase_request = {
                                **request,
                                "fields": ["metadata.revision", "metadata.allowed"],
                            }
                        if phase in ("reference", "reference_full"):
                            phase_request = copy.deepcopy(phase_request)
                            phase_request["select"]["identity"]["args"][0] = {
                                "call": "jsonb_extract_path",
                                "args": [
                                    {
                                        "call": "jsonb_build_object",
                                        "args": [
                                            {"literal": "v"},
                                            {"field": "payload.large"},
                                        ],
                                    },
                                    {"literal": "v"},
                                ],
                            }
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
                                (h["_id"], h["_source"], h.get("_computed"))
                                for h in hits
                            )
                            digest = hashlib.sha256(
                                json.dumps(evidence, sort_keys=True).encode()
                            ).hexdigest()
                            assert (
                                output_hashes.setdefault(
                                    "conditional"
                                    if phase == "reference"
                                    else "full"
                                    if phase == "reference_full"
                                    else phase,
                                    digest,
                                )
                                == digest
                            ), "baseline/candidate source delivery differs"
                            for hit in hits:
                                if phase != "ordinary":
                                    values = hit["_computed"]
                                    for j in range(args.metadata_outputs):
                                        assert values[f"extra_{j}"] == f"value-{j}"
                                    assert values["changed"] == (
                                        phase in ("full", "reference_full")
                                    ), (phase, hit["_id"])
                                    assert (values["value"] is not None) == (
                                        phase in ("full", "reference_full")
                                    ), (phase, hit["_id"])
                                assert hit["_source"]["metadata"] == {
                                    "revision": "r1",
                                    "allowed": True,
                                }
                                assert (
                                    "large" in hit["_source"].get("payload", {})
                                ) == (phase == "ordinary")
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
                        request["params"]["known"] = {
                            h["_id"]: h["_computed"]["identity"] for h in hits
                        }
                    request["fields"] = ["metadata.revision", "metadata.allowed"]
                    # Keep metadata edits separate from the cold/ordinary/full
                    # controls. Reuse the same payload and vary only metadata;
                    # validate new revision delivery plus unchanged identities.
                    for update in range(args.metadata_updates):
                        updated = copy.deepcopy(documents)
                        for doc in updated.values():
                            doc["metadata"] = {
                                "revision": f"r{update + 2}",
                                "allowed": update % 2 == 0,
                            }
                        _, usage = measure(
                            read,
                            lambda: session.post(
                                base + "/batch",
                                json={"inserts": updated, "sync_level": "full_index"},
                                timeout=60,
                            ),
                        )
                        samples.append(
                            {
                                "variant": variant,
                                "round": round_id,
                                "phase": "metadata_update",
                                "update": update,
                                **usage,
                            }
                        )
                        verified = session.post(
                            base + "/query", json=request, timeout=60
                        )
                        verified.raise_for_status()
                        changed_hits = verified.json()["responses"][0]["hits"]["hits"]
                        assert {h["_id"] for h in changed_hits} == set(documents)
                        for hit in changed_hits:
                            assert (
                                hit["_source"]["metadata"]
                                == updated[hit["_id"]]["metadata"]
                            ), hit["_id"]
                            assert hit["_computed"]["changed"] is False, hit
                        digest = hashlib.sha256(
                            json.dumps(
                                sorted(
                                    (h["_id"], h["_source"], h["_computed"])
                                    for h in changed_hits
                                ),
                                sort_keys=True,
                            ).encode()
                        ).hexdigest()
                        key = f"metadata_update_{update}"
                        assert output_hashes.setdefault(key, digest) == digest, (
                            "metadata update delivery differs"
                        )
                    logs = server.log_path.read_text(errors="replace")
                    counts = [
                        (int(a), int(b))
                        for a, b in re.findall(
                            r"antfly_bench_projection metadata_rows=(\d+) fallback_rows=(\d+)",
                            logs,
                        )
                    ]
                    identity_rows = sum(
                        int(n) for n in re.findall(r"certificate_rows=(\d+)", logs)
                    )
                    if (
                        variant == "candidate"
                        and args.mask == "exact"
                        and args.workload
                        in ("ascii", "escaped", "unicode", "numeric", "opaque")
                        and identity_rows < 2 * args.documents * args.queries_per_phase
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
                        and args.workload == "small"
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
                            "certificate_hits": identity_rows,
                            "certificate_counter_available": "certificate_rows="
                            in logs,
                            "metadata_rows": sum(a for a, _ in counts),
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
        for phase in (
            "ingest",
            "ordinary",
            "full",
            "reference_full",
            "conditional",
            "reference",
            *(("metadata_update",) if args.metadata_updates else ()),
        ):
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
        "schema_version": 2,
        "workload": args.workload,
        "mask": args.mask,
        "scope": "Native single-shard JSON tables, synthetic varied source values, warm filesystem/storage caches after ingestion; whole server process CPU includes background work; RSS sampled every 5 ms and is not an allocation bound; shared host; no inference",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "rounds": args.rounds,
        "queries_per_phase": args.queries_per_phase,
        "metadata_updates": args.metadata_updates,
        "metadata_outputs": args.metadata_outputs,
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
