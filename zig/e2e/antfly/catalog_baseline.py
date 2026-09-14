# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
"""Cross-language client for the versioned internal report-baseline protocol.

Diagnostic report objects already carry every schema default in wire field order.
Retaining number lexemes lets this client hash that canonical Zig representation
without changing floating-point spelling during a Python JSON round trip.
"""

import base64
import hashlib
import json
import time

import requests
from conftest import internal_service_headers


class JsonNumber(str):
    """A JSON number lexeme, not a string value."""


def encode(value):
    if isinstance(value, JsonNumber):
        return str(value)
    if isinstance(value, dict):
        return (
            "{" + ",".join(encode(k) + ":" + encode(v) for k, v in value.items()) + "}"
        )
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(encode(v) for v in value) + "]"
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def canonical_report(header, groups, runtimes):
    # Version 1 hashes the complete StoreStatusReport wire schema. The golden
    # end-to-end test checks this independently against the production encoder.
    report = {
        "store_id": header["store_id"],
        "runtime_reference": False,
        "embedding_activity_protocol_version": 0,
        "embedding_activity_sequence": 0,
        "reporter_incarnation": 0,
        "status_generation": 0,
        "artifact_sources_protocol_version": 0,
        "dense_native_storage_protocol_version": 0,
        "live": True,
        "health_class": "healthy",
        "capacity_bytes": 0,
        "available_bytes": 0,
        "lease_pressure": 0,
        "read_load": 0,
        "write_load": 0,
        "active_backfills": 0,
        "backfill_progress_millis": 1000,
        "group_statuses": groups,
        "runtime_statuses": runtimes,
    }
    for key in report:
        if key not in ("group_statuses", "runtime_statuses") and key in header:
            report[key] = header[key]
    return report


def plan_baseline(header, stored, sequence):
    groups = {}
    runtimes = {}
    for report in stored["group_statuses"]:
        groups.setdefault(report["group_id"], []).append(report)
    for report in stored["runtime_statuses"]:
        runtimes.setdefault(report["group_id"], []).append(report)
    ids = sorted(groups.keys() | runtimes.keys())
    chunks = []
    chain = bytes(32)
    total_bytes = 0
    start = 0
    while start < len(ids):
        count = min(64, len(ids) - start)
        while True:
            selected = ids[start : start + count]
            report = canonical_report(
                header,
                [item for key in selected for item in groups.get(key, [])],
                [item for key in selected for item in runtimes.get(key, [])],
            )
            body = encode(report).encode()
            if len(body) <= 1024 * 1024:
                break
            if count == 1:
                break
            count = max(1, count // 2)
        total_bytes += len(body)
        if len(body) > 1024 * 1024:
            digest = list(hashlib.sha256(body).digest())
            for offset in range(0, len(body), 512 * 1024):
                fragment = {
                    "group_id": selected[0],
                    "offset": offset,
                    "total_bytes": len(body),
                    "digest": digest,
                    "data": base64.b64encode(
                        body[offset : offset + 512 * 1024]
                    ).decode(),
                }
                chain = hashlib.sha256(
                    chain + hashlib.sha256(encode(fragment).encode()).digest()
                ).digest()
                chunks.append({"$fragment": fragment})
        else:
            chain = hashlib.sha256(chain + hashlib.sha256(body).digest()).digest()
            chunks.append(report)
        start += count
    request = {
        "version": 1,
        "action": "prepare",
        "cursor": {
            "reporter_incarnation": header["reporter_incarnation"],
            "sequence": sequence,
            "digest": list(chain),
        },
        "chunk_count": len(chunks),
        "chunk_index": 0,
        "total_bytes": total_bytes,
        "report": canonical_report(header, [], []),
    }
    return request, chunks


def next_baseline_request(manifest, chunks, next_chunk, *, batch_size=8):
    if next_chunk == len(chunks):
        return {**manifest, "action": "activate"}
    chunk = chunks[next_chunk]
    if "$fragment" in chunk:
        return {
            **manifest,
            "action": "fragment",
            "chunk_index": next_chunk,
            "fragment": chunk["$fragment"],
        }
    requests = []
    size = 0
    for index in range(next_chunk, min(next_chunk + batch_size, len(chunks))):
        chunk = chunks[index]
        if "$fragment" in chunk:
            break
        encoded_size = len(encode(chunk).encode())
        if requests and size + encoded_size > 1536 * 1024:
            break
        requests.append(
            {**manifest, "action": "chunk", "chunk_index": index, "report": chunk}
        )
        size += encoded_size
    if len(requests) == 1:
        return requests[0]
    return {**manifest, "action": "batch", "chunk_index": next_chunk, "batch": requests}


def post_baseline(cluster, request, attempts=None):
    body = encode(request).encode()
    assert len(body) <= 2 * 1024 * 1024
    failures = []
    for index, url in enumerate(cluster.metadata_urls):
        if cluster.metadata_procs[index].poll() is not None:
            continue
        started = time.perf_counter_ns()
        response = requests.post(
            url + f"/internal/v1/nodes/{request['report']['store_id']}/status/baseline",
            data=body,
            headers={**internal_service_headers(), "Content-Type": "application/json"},
            timeout=15,
        )
        if attempts is not None:
            attempts.append(
                {
                    "endpoint": index,
                    "action": request["action"],
                    "request_bytes": len(body),
                    "chunk_index": request["chunk_index"],
                    "status": response.status_code,
                    "elapsed_ms": (time.perf_counter_ns() - started) / 1e6,
                    "body": response.text[:300] if not response.ok else "",
                }
            )
        if response.ok:
            return index, response.json(), len(body)
        failures.append((index, response.status_code, response.text[:300]))
    raise AssertionError(failures)
