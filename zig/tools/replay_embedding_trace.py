#!/usr/bin/env python3
"""Replay opt-in managed embedding captures against the text embedding API.

Captures contain private corpus data. Only loopback destinations are allowed
unless --allow-remote is explicitly supplied. Redirects are never followed.
This compares identical inputs, not identical machine state or concurrent load.
"""
import argparse
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import statistics
import time
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def endpoint_url(url, allow_remote=False):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("expected an http(s) server URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("credentials, query strings, and fragments are not allowed in the URL")
    try:
        loopback = ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        loopback = parsed.hostname == "localhost"
    if not loopback and not allow_remote:
        raise ValueError("captures contain private inputs; use --allow-remote to authorize a non-loopback destination")
    return url.rstrip("/") + "/ai/v1/embeddings"


def load_capture(path):
    if path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError(f"oversized capture: {path.name}")
    value = json.loads(path.read_bytes())
    if value.get("version") != 1:
        raise ValueError(f"unsupported capture version: {path.name}")
    if value.get("path") != "managed_direct" or not value.get("success"):
        return None
    texts = value.get("input")
    if not isinstance(texts, list) or not 1 <= len(texts) <= 32 or not all(isinstance(s, str) for s in texts):
        raise ValueError("invalid captured text batch")
    instruction = value.get("instruction")
    if instruction is not None and not isinstance(instruction, str):
        raise ValueError("invalid captured instruction")
    if sum(len(s.encode()) for s in texts) + len((instruction or "").encode()) > 256 * 1024:
        raise ValueError("captured text batch exceeds the capture byte bound")
    if not isinstance(value.get("model"), str) or not value["model"]:
        raise ValueError("capture must identify a model")
    if value.get("task_type") not in ("RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY"):
        raise ValueError("unsupported captured task type")
    compare_vectors(value.get("vectors"), value.get("vectors"), 0)
    if len(value["vectors"]) != len(texts):
        raise ValueError("captured input/vector counts differ")
    return value


def replay_body(capture):
    return {key: capture[key] for key in ("model", "input", "task_type", "instruction") if capture.get(key) is not None}


def compare_vectors(expected, actual, tolerance):
    if not isinstance(expected, list) or not expected or not isinstance(actual, list) or len(expected) != len(actual):
        raise ValueError("embedding batch size mismatch")
    maximum = 0.0
    dimensions = None
    for left, right in zip(expected, actual):
        if not isinstance(left, list) or not left or not isinstance(right, list) or len(left) != len(right):
            raise ValueError("embedding dimension mismatch")
        if dimensions is not None and dimensions != len(left):
            raise ValueError("inconsistent embedding dimensions")
        dimensions = len(left)
        for a, b in zip(left, right):
            if isinstance(a, bool) or isinstance(b, bool) or not isinstance(a, (float, int)) or not isinstance(b, (float, int)) or not math.isfinite(a) or not math.isfinite(b):
                raise ValueError("non-finite or invalid embedding")
            maximum = max(maximum, abs(a - b))
    return {"passed": maximum <= tolerance, "max_absolute_error": maximum, "dimensions": dimensions}


def post(opener, url, body, timeout):
    request = urllib.request.Request(url, data=json.dumps(body, ensure_ascii=False).encode(), headers={"Content-Type": "application/json"})
    started = time.monotonic()
    with opener.open(request, timeout=timeout) as response:
        value = json.load(response)
    elapsed = time.monotonic() - started
    rows = sorted(value["data"], key=lambda row: row["index"])
    if [row["index"] for row in rows] != list(range(len(body["input"]))):
        raise ValueError("missing or duplicate embedding response indices")
    return [row["embedding"] for row in rows], elapsed, value.get("usage")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture_dir", type=Path)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--allow-remote", action="store_true")
    parser.add_argument("--max-batches", type=int, default=8)
    parser.add_argument("--batch-size", type=int, help="select captures with this many inputs (e.g. 8 to exclude creation probes)")
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--warmup", type=int, default=1, help="unmeasured replays of each exact batch")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--tolerance", type=float, default=1e-4)
    parser.add_argument("--output", type=Path, required=True, help="new private report file; never overwritten")
    args = parser.parse_args()
    if not 1 <= args.max_batches <= 64 or not 1 <= args.repeat <= 10 or not 0 <= args.warmup <= 3:
        parser.error("bounds: max-batches 1..64, repeat 1..10, warmup 0..3")
    if not math.isfinite(args.tolerance) or args.tolerance < 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("invalid tolerance or timeout")
    if args.batch_size is not None and not 1 <= args.batch_size <= 32:
        parser.error("batch-size must be 1..32")
    url = endpoint_url(args.url, args.allow_remote)
    # Reserve the report before making requests, so an existing report never
    # causes an expensive replay followed by an overwrite or late failure.
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    report = {"version": 1, "endpoint": url, "warmup_per_batch": args.warmup, "batches": [], "completed": False}
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        captures = []
        for path in sorted(args.capture_dir.glob("embed-*.json")):
            capture = load_capture(path)
            if capture is not None and (args.batch_size is None or len(capture["input"]) == args.batch_size):
                captures.append((path, capture))
                if len(captures) == args.max_batches:
                    break
        if not captures:
            raise ValueError("no successful managed_direct captures found")
        for path, capture in captures:
            body = replay_body(capture)
            samples = []
            for iteration in range(args.warmup + args.repeat):
                vectors, seconds, usage = post(opener, url, body, args.timeout)
                comparison = compare_vectors(capture["vectors"], vectors, args.tolerance)
                if iteration >= args.warmup:
                    samples.append({"seconds": seconds, "usage": usage, **comparison})
                if not comparison["passed"]:
                    raise ValueError(f"vector parity failed for {path.name}: {comparison['max_absolute_error']}")
            seconds = sum(sample["seconds"] for sample in samples)
            row = {"capture": path.name, "request_sha256": hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True).encode()).hexdigest(), "model": capture["model"], "items": len(body["input"]), "managed_backend": capture["backend"], "managed_timing_ns": capture["timing_ns"], "managed_shapes": capture["shapes"], "samples": samples, "http_embeddings_per_second": len(body["input"]) * len(samples) / seconds, "http_mean_seconds": statistics.mean(s["seconds"] for s in samples)}
            report["batches"].append(row)
            print(json.dumps({"capture": path.name, "items": row["items"], "http_embeddings_per_second": row["http_embeddings_per_second"], "parity": "passed"}), flush=True)
        report["completed"] = True
        items = sum(row["items"] for row in report["batches"])
        managed_seconds = sum(row["managed_timing_ns"]["total"] for row in report["batches"]) / 1e9
        http_seconds = sum(sample["seconds"] for row in report["batches"] for sample in row["samples"])
        report["aggregate"] = {"items_per_pass": items, "managed_boundary_seconds": managed_seconds, "managed_boundary_embeddings_per_second": items / managed_seconds if managed_seconds > 0 else None, "http_seconds": http_seconds, "http_embeddings_per_second": items * args.repeat / http_seconds}
    except Exception as error:
        # Exception messages from HTTP servers can contain response content;
        # keep the persisted failure diagnostic structural.
        report["error_type"] = type(error).__name__
        raise
    finally:
        with os.fdopen(fd, "w") as output:
            json.dump(report, output, indent=2)
            output.write("\n")


if __name__ == "__main__":
    main()
