#!/usr/bin/env python3
"""Load the canonical search corpus into Quickwit through its REST API.

The loader preserves the kernel benchmark's zero-based corpus ordinal and
keeps every HTTP payload below Quickwit's 10 MiB ingest limit. Intermediate
batches use normal queued ingestion; the final batch requests a forced commit
and does not return until the whole load is searchable.
"""

from __future__ import annotations

import argparse
import http.client
import json
import time
from pathlib import Path
from typing import BinaryIO, Iterator
from urllib.parse import quote, urlsplit


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--base-url", default="http://127.0.0.1:7280")
    parser.add_argument("--index", default="antfly-benchmark")
    parser.add_argument("--batch-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--timeout-seconds", type=float, default=300)
    parser.add_argument("--max-documents", type=int)
    args = parser.parse_args()
    if args.batch_bytes <= 0 or args.batch_bytes >= 10 * 1024 * 1024:
        parser.error("--batch-bytes must be between 1 and 10 MiB (exclusive)")
    if args.max_documents is not None and args.max_documents <= 0:
        parser.error("--max-documents must be positive")
    return args


def encode_document(raw_line: bytes, ordinal: int) -> bytes:
    try:
        value = json.loads(raw_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON at corpus ordinal {ordinal}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("text"), str):
        raise ValueError(f"missing string text at corpus ordinal {ordinal}")
    return (
        json.dumps(
            {"corpus_ordinal": ordinal, "body": value["text"]},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def batches(stream: BinaryIO, target_bytes: int, max_documents: int | None = None) -> Iterator[tuple[bytes, int]]:
    payload = bytearray()
    documents = 0
    for raw_line in stream:
        if not raw_line.strip():
            continue
        if max_documents is not None and documents >= max_documents:
            break
        encoded = encode_document(raw_line, documents)
        if len(encoded) >= 10 * 1024 * 1024:
            raise ValueError(f"document {documents} exceeds Quickwit's ingest payload limit")
        if payload and len(payload) + len(encoded) > target_bytes:
            yield bytes(payload), documents
            payload.clear()
        payload.extend(encoded)
        documents += 1
    if payload:
        yield bytes(payload), documents


class QuickwitClient:
    def __init__(self, base_url: str, index: str, timeout: float):
        parsed = urlsplit(base_url)
        if parsed.scheme != "http" or not parsed.hostname:
            raise ValueError("loader currently requires an http base URL")
        self.host = parsed.hostname
        self.port = parsed.port or 80
        self.prefix = parsed.path.rstrip("/")
        self.index = quote(index, safe="")
        self.connection = http.client.HTTPConnection(self.host, self.port, timeout=timeout)

    def ingest(self, payload: bytes, force: bool) -> int:
        commit = "force" if force else "auto"
        path = f"{self.prefix}/api/v1/{self.index}/ingest?commit={commit}&detailed_response=true"
        self.connection.request(
            "POST",
            path,
            body=payload,
            headers={"Content-Type": "application/x-ndjson"},
        )
        response = self.connection.getresponse()
        body = response.read()
        if not 200 <= response.status < 300:
            raise RuntimeError(f"Quickwit ingest failed with HTTP {response.status}: {body[:1000]!r}")
        result = json.loads(body)
        rejected = int(result.get("num_rejected_docs", 0))
        failures = result.get("parse_failures", [])
        if rejected or failures:
            raise RuntimeError(f"Quickwit rejected {rejected} documents: {failures[:3]!r}")
        return int(result.get("num_docs_for_processing", result.get("num_ingested_docs", 0)))

    def close(self) -> None:
        self.connection.close()


def main() -> int:
    args = arguments()
    started = time.perf_counter()
    with args.corpus.open("rb") as corpus:
        prepared = batches(corpus, args.batch_bytes, args.max_documents)
        pending = next(prepared, None)
        if pending is None:
            raise ValueError("corpus contains no documents")
        client = QuickwitClient(args.base_url, args.index, args.timeout_seconds)
        submitted = 0
        batch_count = 0
        try:
            for following in prepared:
                payload, cumulative_documents = pending
                acknowledged = client.ingest(payload, force=False)
                expected = cumulative_documents - submitted
                if acknowledged != expected:
                    raise RuntimeError(f"Quickwit acknowledged {acknowledged} of {expected} documents")
                submitted = cumulative_documents
                batch_count += 1
                pending = following
            payload, cumulative_documents = pending
            acknowledged = client.ingest(payload, force=True)
            expected = cumulative_documents - submitted
            if acknowledged != expected:
                raise RuntimeError(f"Quickwit acknowledged {acknowledged} of {expected} documents")
            submitted = cumulative_documents
            batch_count += 1
        finally:
            client.close()
    elapsed = time.perf_counter() - started
    print(
        json.dumps(
            {
                "batches": batch_count,
                "documents": submitted,
                "elapsed_seconds": elapsed,
                "documents_per_second": submitted / elapsed,
                "final_commit": "force",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
