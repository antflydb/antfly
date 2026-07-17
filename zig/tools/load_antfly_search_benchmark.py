#!/usr/bin/env python3
"""Load the canonical search corpus through Antfly's public batch API."""

from __future__ import annotations

import argparse
import http.client
import json
import sys
import time
from pathlib import Path
from typing import BinaryIO, Iterator
from urllib.parse import quote, urlsplit


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--table", default="antfly-benchmark")
    parser.add_argument(
        "--ensure-table",
        action="store_true",
        help="create the table with its default full-text index if it is absent",
    )
    parser.add_argument("--batch-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--timeout-seconds", type=float, default=300)
    parser.add_argument("--max-documents", type=int)
    parser.add_argument(
        "--start-document",
        type=int,
        default=0,
        help="resume at this zero-based corpus ordinal; deterministic keys make replay idempotent",
    )
    parser.add_argument("--progress-every-batches", type=int, default=25)
    args = parser.parse_args()
    if args.batch_bytes <= 0:
        parser.error("--batch-bytes must be positive")
    if args.max_documents is not None and args.max_documents <= 0:
        parser.error("--max-documents must be positive")
    if args.progress_every_batches < 0:
        parser.error("--progress-every-batches must be non-negative")
    return args


def encode_entry(raw_line: bytes, ordinal: int) -> bytes:
    try:
        value = json.loads(raw_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON at corpus ordinal {ordinal}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("text"), str):
        raise ValueError(f"missing string text at corpus ordinal {ordinal}")
    key = f"doc:{ordinal}"
    document = {"corpus_ordinal": ordinal, "body": value["text"]}
    return json.dumps(key, separators=(",", ":")).encode() + b":" + json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def batches(
    stream: BinaryIO,
    target_bytes: int,
    max_documents: int | None = None,
    start_document: int = 0,
) -> Iterator[tuple[bytes, int]]:
    entries: list[bytes] = []
    size = len(b'{"inserts":{},"sync_level":"full_index"}')
    documents = 0
    for raw_line in stream:
        if not raw_line.strip():
            continue
        if max_documents is not None and documents >= max_documents:
            break
        ordinal = documents
        if ordinal < start_document:
            documents += 1
            continue
        encoded = encode_entry(raw_line, ordinal)
        additional = len(encoded) + (1 if entries else 0)
        if entries and size + additional > target_bytes:
            yield b",".join(entries), documents
            entries.clear()
            size = len(b'{"inserts":{},"sync_level":"full_index"}')
        entries.append(encoded)
        size += additional
        documents += 1
    if entries:
        yield b",".join(entries), documents


def batch_payload(entries: bytes, sync_level: str) -> bytes:
    return b'{"inserts":{' + entries + b'},"sync_level":' + json.dumps(sync_level).encode() + b"}"


class AntflyClient:
    def __init__(self, base_url: str, table: str, timeout: float):
        parsed = urlsplit(base_url)
        if parsed.scheme != "http" or not parsed.hostname:
            raise ValueError("loader currently requires an http base URL")
        self.connection = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=timeout)
        self.base_path = parsed.path.rstrip("/")
        self.table = table
        self.path = f"{parsed.path.rstrip('/')}/tables/{quote(table, safe='')}/batch"
        self.timeout = timeout
        self.backpressure_retries = 0
        self.backpressure_wait_seconds = 0.0

    def ensure_table(self) -> None:
        path = f"{self.base_path}/tables/{quote(self.table, safe='')}"
        payload = b'{"num_shards":1}'
        self.connection.request("POST", path, body=payload, headers={"Content-Type": "application/json"})
        response = self.connection.getresponse()
        body = response.read()
        if not 200 <= response.status < 300 and response.status != 409:
            raise RuntimeError(f"Antfly table creation failed with HTTP {response.status}: {body[:1000]!r}")

    def ingest(self, entries: bytes, sync_level: str) -> int:
        payload = batch_payload(entries, sync_level)
        deadline = time.monotonic() + self.timeout
        delay = 0.01
        while True:
            self.connection.request("POST", self.path, body=payload, headers={"Content-Type": "application/json"})
            response = self.connection.getresponse()
            body = response.read()
            if 200 <= response.status < 300:
                result = json.loads(body)
                return int(result.get("inserted", 0))
            if response.status != 429:
                raise RuntimeError(f"Antfly batch failed with HTTP {response.status}: {body[:1000]!r}")
            now = time.monotonic()
            if now >= deadline:
                raise TimeoutError(f"Antfly batch remained backpressured for {self.timeout:.3f}s")
            sleep_seconds = min(delay, deadline - now)
            time.sleep(sleep_seconds)
            self.backpressure_retries += 1
            self.backpressure_wait_seconds += sleep_seconds
            delay = min(delay * 2, 0.25)

    def close(self) -> None:
        self.connection.close()


def main() -> int:
    args = arguments()
    started = time.perf_counter()
    with args.corpus.open("rb") as corpus:
        prepared = batches(corpus, args.batch_bytes, args.max_documents, args.start_document)
        pending = next(prepared, None)
        if pending is None:
            raise ValueError("corpus contains no documents")
        client = AntflyClient(args.base_url, args.table, args.timeout_seconds)
        submitted = args.start_document
        batch_count = 0
        try:
            if args.ensure_table:
                client.ensure_table()
            for following in prepared:
                entries, cumulative_documents = pending
                expected = cumulative_documents - submitted
                inserted = client.ingest(entries, "write")
                if inserted != expected:
                    raise RuntimeError(f"Antfly inserted {inserted} of {expected} documents")
                submitted = cumulative_documents
                batch_count += 1
                if args.progress_every_batches and batch_count % args.progress_every_batches == 0:
                    elapsed = time.perf_counter() - started
                    print(
                        json.dumps(
                            {
                                "progress_batches": batch_count,
                                "progress_documents": submitted,
                                "progress_documents_per_second": submitted / elapsed,
                                "progress_elapsed_seconds": elapsed,
                            },
                            sort_keys=True,
                        ),
                        file=sys.stderr,
                        flush=True,
                    )
                pending = following
            entries, cumulative_documents = pending
            expected = cumulative_documents - submitted
            inserted = client.ingest(entries, "full_index")
            if inserted != expected:
                raise RuntimeError(f"Antfly inserted {inserted} of {expected} documents")
            submitted = cumulative_documents
            batch_count += 1
        finally:
            client.close()
    elapsed = time.perf_counter() - started
    print(json.dumps({
        "batches": batch_count,
        "documents": submitted,
        "documents_per_second": submitted / elapsed,
        "elapsed_seconds": elapsed,
        "final_sync_level": "full_index",
        "backpressure_retries": client.backpressure_retries,
        "backpressure_wait_seconds": client.backpressure_wait_seconds,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
