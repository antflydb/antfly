#!/usr/bin/env python3
"""Time managed embedding production plus full-text with a deterministic provider.

This isolates the storage/enrichment pipeline from remote model latency. It is
not a measurement of a real model's inference throughput or semantic quality.
"""

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time

import httpx


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--mode", choices=["primary_lsm", "vector_store"], required=True
    )
    parser.add_argument("--rows", type=int, default=4000)
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    dimensions = 128
    requests = []

    class Provider(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            inputs = payload.get("input", [])
            if isinstance(inputs, str):
                inputs = [inputs]
            rows = []
            for i, text in enumerate(inputs):
                seed = hashlib.shake_256(str(text).encode()).digest(dimensions)
                vector = [(v - 127.5) / 128 for v in seed]
                rows.append({"object": "embedding", "index": i, "embedding": vector})
            requests.append(len(inputs))
            body = json.dumps(
                {
                    "object": "list",
                    "data": rows,
                    "model": "deterministic-storage-test",
                    "usage": {
                        "prompt_tokens": len(inputs),
                        "total_tokens": len(inputs),
                    },
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    worker = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    worker.start()
    base = f"http://127.0.0.1:{args.port}/db/v1/tables/source_enrichment"
    result = {
        "mode": args.mode,
        "rows": args.rows,
        "dimensions": dimensions,
        "provider": "local deterministic SHAKE-256, not model inference",
        "phases": [],
    }
    try:
        with httpx.Client(timeout=180) as client:

            def post(path, value):
                response = client.post(base + path, json=value)
                response.raise_for_status()
                return response.json()

            post("", {"num_shards": 1, "storage": {"dense_embeddings": args.mode}})
            post(
                "/indexes/semantic",
                {
                    "name": "semantic",
                    "type": "embeddings",
                    "field": "body",
                    "dimension": dimensions,
                    "embedder": {
                        "provider": "openai",
                        "model": "deterministic-storage-test",
                        "url": f"http://127.0.0.1:{server.server_port}",
                        "dimensions": dimensions,
                    },
                },
            )
            # Index creation is asynchronous. Qualify ingestion against the
            # same fully provisioned configuration in both storage modes.
            provision_started = time.perf_counter()
            deadline = time.monotonic() + 180
            while True:
                statuses = {}
                for name in ("semantic", "full_text_index_v0"):
                    response = client.get(base + "/indexes/" + name)
                    if response.status_code == 404:
                        statuses[name] = {}
                        continue
                    response.raise_for_status()
                    statuses[name] = response.json().get("status", {})
                if all(
                    status.get("readiness", {}).get("complete")
                    and not status.get("backfill_active", True)
                    for status in statuses.values()
                ):
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError(f"index provisioning did not settle: {statuses}")
                time.sleep(0.1)
            result["provision_s"] = time.perf_counter() - provision_started
            result["provision_repair_issues"] = post(
                "/repair/issues", {"target": "index"}
            )
            args.output.write_text(json.dumps(result, indent=2) + "\n")
            for version in range(2):
                started = time.perf_counter()
                for offset in range(0, args.rows, args.batch):
                    docs = {
                        str(i): {
                            "title": f"Document {i}",
                            "body": f"Retrieval storage qualification document {i} revision {version}. "
                            + "Chunks, embeddings, and full text provide independent document views. "
                            * 8,
                        }
                        for i in range(offset, min(offset + args.batch, args.rows))
                    }
                    post(
                        "/batch",
                        {
                            "inserts": docs,
                            "sync_level": (
                                "full_index"
                                if offset + args.batch >= args.rows
                                else "write"
                            ),
                        },
                    )
                    result["last_accepted_batch"] = {
                        "version": version,
                        "through_row": min(offset + args.batch, args.rows),
                    }
                    args.output.write_text(json.dumps(result, indent=2) + "\n")
                deadline = time.monotonic() + 180
                while True:
                    response = client.get(base + "/indexes/semantic")
                    response.raise_for_status()
                    status = response.json().get("status", {})
                    text_response = client.get(base + "/indexes/full_text_index_v0")
                    text_response.raise_for_status()
                    text_status = text_response.json().get("status", {})
                    if (
                        status.get("query_visible_doc_count") == args.rows
                        and status.get("total_indexed") == args.rows
                        and text_status.get("total_indexed") == args.rows
                        and all(
                            current.get("readiness", {}).get("complete")
                            and not current.get("backfill_active", True)
                            and not current.get("repair")
                            for current in (status, text_status)
                        )
                    ):
                        break
                    if time.monotonic() > deadline:
                        raise RuntimeError(f"enrichment did not settle: {status}")
                    time.sleep(0.1)
                result["phases"].append(
                    {
                        "phase": f"version-{version}",
                        "ready_s": time.perf_counter() - started,
                        "semantic_status": status,
                        "full_text_status": text_status,
                    }
                )
                result["phases"][-1]["repair_issues"] = post(
                    "/repair/issues", {"target": "index"}
                )
            for kind, query in [
                (
                    "full_text",
                    {
                        "full_text_search": {
                            "match": {"field": "body", "text": "retrieval"}
                        },
                        "indexes": ["full_text_index_v0"],
                        "limit": 10,
                    },
                ),
                (
                    "semantic",
                    {
                        "semantic_search": "Retrieval storage qualification",
                        "indexes": ["semantic"],
                        "limit": 10,
                    },
                ),
            ]:
                latencies = []
                failures = []
                result[kind] = {"failures": failures, "in_progress": True}
                query_started = time.perf_counter()
                for _ in range(args.queries):
                    started = time.perf_counter()
                    try:
                        response = post("/query", query)
                    except httpx.HTTPStatusError as error:
                        if error.response.status_code != 503:
                            raise
                        failures.append(
                            {
                                "status": 503,
                                "elapsed_ms": (time.perf_counter() - started) * 1000,
                                "body": error.response.text,
                            }
                        )
                        # Failed arms are already disqualified. Capture the
                        # admission state instead of retrying away the failure.
                        if len(failures) <= 3:
                            for label, path in (
                                ("index", "/indexes/semantic"),
                                ("table", ""),
                            ):
                                diagnostic = client.get(base + path)
                                failures[-1][label] = (
                                    diagnostic.json()
                                    if diagnostic.status_code == 200
                                    else diagnostic.text
                                )
                            failures[-1]["repair_issues"] = post(
                                "/repair/issues", {"target": "index"}
                            )
                        args.output.write_text(json.dumps(result, indent=2) + "\n")
                        continue
                    latencies.append((time.perf_counter() - started) * 1000)
                    if not response["responses"][0]["hits"]["hits"]:
                        raise RuntimeError(f"empty {kind} result: {response}")
                ordered = sorted(latencies)
                elapsed = time.perf_counter() - query_started
                result[kind] = {
                    "queries": args.queries,
                    "successful_queries": len(ordered),
                    "failed_queries": len(failures),
                    "failures": failures,
                    "elapsed_s": elapsed,
                    "qps": len(ordered) / elapsed,
                    "p50_ms": ordered[max(0, int(len(ordered) * 0.50) - 1)]
                    if ordered
                    else None,
                    "p95_ms": ordered[max(0, int(len(ordered) * 0.95) - 1)]
                    if ordered
                    else None,
                    "p99_ms": ordered[max(0, int(len(ordered) * 0.99) - 1)]
                    if ordered
                    else None,
                }
            result["qualified"] = not any(
                result[kind]["failed_queries"] for kind in ("full_text", "semantic")
            )
            response = client.get(base)
            response.raise_for_status()
            result["table"] = response.json()
            result["provider_requests"] = len(requests)
            result["provider_inputs"] = sum(requests)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
    except Exception as error:
        result["error"] = str(error)
        result["provider_requests"] = len(requests)
        result["provider_inputs"] = sum(requests)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        raise
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


if __name__ == "__main__":
    main()
