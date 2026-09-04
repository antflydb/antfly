#!/usr/bin/env python3
"""Nomic v1.5 PyTorch/MPS reference and OpenAI-compatible benchmark endpoint.

All modes use F32 weights, eval(), torch.inference_mode(), and synchronize MPS
after every measured forward/request.  The direct mode consumes the checked-in
token fixture used by `bench-nomic-e2e`; `serve` exposes `/ai/v1/embeddings`
for a matched local endpoint comparison.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import resource
import statistics
import subprocess
import sys
import time
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="mode", required=True)

    def add_common(command: argparse.ArgumentParser) -> None:
        command.add_argument("--model-dir", required=True)
        command.add_argument("--model-name", default="nomic-ai/nomic-embed-text-v1.5")
        command.add_argument(
            "--fixture",
            default="src/bench/testdata/nomic_v15_tokens.json",
            help="shared pretokenized direct-benchmark fixture",
        )
        command.add_argument("--python-path", action="append", default=[])

    direct = subparsers.add_parser("direct", help="run pretokenized direct MPS cells")
    add_common(direct)
    direct.add_argument("--batches", default="1,2,4")
    direct.add_argument("--sequence-lengths", default="16,128")
    direct.add_argument("--warmups", type=int, default=3)
    direct.add_argument("--repeats", type=int, default=10)
    direct.add_argument("--print-embeddings", action="store_true")

    serve = subparsers.add_parser("serve", help="serve /ai/v1/embeddings over local HTTP")
    add_common(serve)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=18100)

    endpoint = subparsers.add_parser("endpoint", help="benchmark an OpenAI-compatible endpoint")
    endpoint.add_argument("--url", required=True)
    endpoint.add_argument("--model-name", default="nomic-ai/nomic-embed-text-v1.5")
    endpoint.add_argument("--model-sha", required=True)
    endpoint.add_argument("--backend", required=True, help="evidence label, e.g. antfly or pytorch")
    endpoint.add_argument("--device", required=True, help="evidence label, e.g. metal or mps")
    endpoint.add_argument("--batches", default="1,2,4")
    endpoint.add_argument("--sequence-lengths", default="16,128")
    endpoint.add_argument("--warmups", type=int, default=3)
    endpoint.add_argument("--repeats", type=int, default=10)
    endpoint.add_argument("--timeout", type=float, default=60.0)

    parity = subparsers.add_parser("parity", help="compare native Metal embeddings with PyTorch MPS")
    add_common(parity)
    parity.add_argument("--native-binary", required=True)
    parity.add_argument("--batches", default="1,2,4")
    parity.add_argument("--sequence-lengths", default="16,128")

    args = parser.parse_args()
    if args.mode in {"direct", "endpoint", "parity"}:
        args.batches = parse_int_set(args.batches, {1, 2, 4}, "batches")
        args.sequence_lengths = parse_int_set(args.sequence_lengths, {16, 128}, "sequence lengths")
        if args.mode in {"direct", "endpoint"} and (args.warmups != 3 or args.repeats != 10):
            parser.error("the benchmark contract requires exactly 3 warmups and 10 repeats")
    return args


def parse_int_set(value: str, allowed: set[int], label: str) -> list[int]:
    result = [int(item) for item in value.split(",")]
    if not result or any(item not in allowed for item in result):
        raise argparse.ArgumentTypeError(f"{label} must be drawn from {sorted(allowed)}")
    return result


def import_torch_and_transformers(extra_python_paths: list[str]):
    for path in reversed(extra_python_paths):
        sys.path.insert(0, path)
    import torch
    from transformers import AutoModel, AutoTokenizer

    return torch, AutoModel, AutoTokenizer


class NomicMpsEmbedder:
    def __init__(self, args: argparse.Namespace):
        torch, auto_model, auto_tokenizer = import_torch_and_transformers(args.python_path)
        if not torch.backends.mps.is_available():
            raise RuntimeError("PyTorch MPS is unavailable")
        self.torch = torch
        self.device = torch.device("mps")
        self.model = auto_model.from_pretrained(
            args.model_dir,
            trust_remote_code=True,
            torch_dtype=torch.float32,
        ).to(self.device).eval()
        self.tokenizer = auto_tokenizer.from_pretrained(args.model_dir, trust_remote_code=True)
        self.model_name = args.model_name
        self.model_sha = sha256_file(Path(args.model_dir) / "model.safetensors")

    def embed_ids(self, input_ids: Any, attention_mask: Any) -> list[list[float]]:
        with self.torch.inference_mode():
            result = self.model(input_ids=input_ids, attention_mask=attention_mask)
            hidden = result.last_hidden_state
            mask = attention_mask.unsqueeze(-1).to(dtype=hidden.dtype)
            pooled = (hidden * mask).sum(dim=1) / mask.sum(dim=1).clamp_min(1)
            embeddings = self.torch.nn.functional.normalize(pooled, p=2, dim=1)
            self.torch.mps.synchronize()
            return embeddings.cpu().tolist()

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        encoded = self.tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
        return self.embed_ids(
            encoded["input_ids"].to(self.device),
            encoded["attention_mask"].to(self.device),
        )


def load_fixture(path: str) -> dict[str, Any]:
    fixture = json.loads(Path(path).read_text())
    if (
        fixture.get("model") != "nomic-ai/nomic-embed-text-v1.5"
        or fixture.get("vocab_size") != 30528
        or fixture.get("attention_mask") != "all_ones"
    ):
        raise RuntimeError("unexpected Nomic benchmark fixture identity")
    for length in (16, 128):
        ids = fixture.get(f"input_ids_{length}")
        if not isinstance(ids, list) or len(ids) != length or any(not isinstance(value, int) or value < 0 or value >= 30528 for value in ids):
            raise RuntimeError(f"invalid input_ids_{length} fixture")
    return fixture


def direct(args: argparse.Namespace) -> None:
    embedder = NomicMpsEmbedder(args)
    fixture = load_fixture(args.fixture)
    torch = embedder.torch
    for sequence_length in args.sequence_lengths:
        row = fixture[f"input_ids_{sequence_length}"]
        for batch in args.batches:
            input_ids = torch.tensor([row] * batch, dtype=torch.long, device=embedder.device)
            attention_mask = torch.ones((batch, sequence_length), dtype=torch.long, device=embedder.device)
            for _ in range(args.warmups):
                embedder.embed_ids(input_ids, attention_mask)
            rss_before = rss_bytes()
            samples: list[float] = []
            embeddings: list[list[float]] = []
            for _ in range(args.repeats):
                start = time.perf_counter_ns()
                embeddings = embedder.embed_ids(input_ids, attention_mask)
                samples.append((time.perf_counter_ns() - start) / 1_000_000.0)
            emit_json(
                {
                    "kind": "nomic_direct_reference",
                    "model": embedder.model_name,
                    "model_sha": embedder.model_sha,
                    "backend": "pytorch",
                    "device": "mps",
                    "fixture_seed": fixture["seed"],
                    "batch": batch,
                    "sequence_length": sequence_length,
                    "warmups": args.warmups,
                    "repeats": args.repeats,
                    **summarize_ms(samples),
                    "gpu_frame_p50_ms": None,
                    "gpu_frame_p95_ms": None,
                    "command_counts": None,
                    "output_checksum": checksum(embeddings),
                    "rss_bytes": {"before": rss_before, "after": rss_bytes()},
                    "swap_bytes": macos_swap_bytes(),
                }
            )
            if args.print_embeddings:
                emit_json(
                    {
                        "kind": "nomic_direct_reference_embeddings",
                        "model_sha": embedder.model_sha,
                        "batch": batch,
                        "sequence_length": sequence_length,
                        "embeddings": embeddings,
                    }
                )


def serve(args: argparse.Namespace) -> None:
    embedder = NomicMpsEmbedder(args)

    class Handler(BaseHTTPRequestHandler):
        server_version = "NomicPyTorchMPS/1.0"

        def log_message(self, fmt: str, *values: object) -> None:
            sys.stderr.write("nomic_mps_endpoint: " + fmt % values + "\n")

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/ai/v1/embeddings":
                self.send_error(HTTPStatus.NOT_FOUND, "only /ai/v1/embeddings is available")
                return
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(content_length))
                inputs = body.get("input")
                if isinstance(inputs, str):
                    inputs = [inputs]
                if not isinstance(inputs, list) or not inputs or any(not isinstance(item, str) for item in inputs):
                    raise ValueError("input must be a non-empty string or list of strings")
                embeddings = embedder.embed_texts(inputs)
                response = {
                    "object": "list",
                    "model": body.get("model", embedder.model_name),
                    "data": [
                        {"object": "embedding", "index": index, "embedding": embedding}
                        for index, embedding in enumerate(embeddings)
                    ],
                    "usage": {"prompt_tokens": 0, "total_tokens": 0},
                }
                payload = json.dumps(response, separators=(",", ":")).encode()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except Exception as exc:  # report a valid API response for client evidence
                payload = json.dumps({"error": {"message": str(exc), "type": "invalid_request_error"}}).encode()
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(json.dumps({"kind": "nomic_pytorch_mps_endpoint", "url": f"http://{args.host}:{args.port}/ai/v1/embeddings", "model_sha": embedder.model_sha}), flush=True)
    server.serve_forever()


def endpoint(args: argparse.Namespace) -> None:
    for sequence_length in args.sequence_lengths:
        # `the` is a single vocabulary token for the BERT-family tokenizer;
        # adding CLS/SEP makes these payloads pad exactly to 16 and 128.
        text = " ".join(["the"] * (sequence_length - 2))
        for batch in args.batches:
            body = json.dumps({"model": args.model_name, "input": [text] * batch}).encode()
            for _ in range(args.warmups):
                request_endpoint(args.url, body, args.timeout)
            rss_before = rss_bytes()
            samples: list[float] = []
            embeddings: list[list[float]] = []
            for _ in range(args.repeats):
                elapsed, embeddings = request_endpoint(args.url, body, args.timeout)
                samples.append(elapsed)
            emit_json(
                {
                    "kind": "nomic_http_endpoint",
                    "model": args.model_name,
                    "model_sha": args.model_sha,
                    "backend": args.backend,
                    "device": args.device,
                    "url": args.url,
                    "batch": batch,
                    "sequence_length": sequence_length,
                    "warmups": args.warmups,
                    "repeats": args.repeats,
                    **summarize_ms(samples),
                    "output_checksum": checksum(embeddings),
                    "rss_bytes": {"before": rss_before, "after": rss_bytes()},
                    "swap_bytes": macos_swap_bytes(),
                }
            )


def parity(args: argparse.Namespace) -> int:
    embedder = NomicMpsEmbedder(args)
    fixture_path = Path(args.fixture).resolve()
    fixture = load_fixture(str(fixture_path))
    native_binary = Path(args.native_binary).resolve()
    if not native_binary.is_file():
        raise RuntimeError(f"native benchmark binary does not exist: {native_binary}")
    torch = embedder.torch
    passed = True
    for sequence_length in args.sequence_lengths:
        row = fixture[f"input_ids_{sequence_length}"]
        for batch in args.batches:
            input_ids = torch.tensor([row] * batch, dtype=torch.long, device=embedder.device)
            attention_mask = torch.ones((batch, sequence_length), dtype=torch.long, device=embedder.device)
            reference = embedder.embed_ids(input_ids, attention_mask)
            command = [
                str(native_binary),
                "--model-dir",
                str(Path(args.model_dir).resolve()),
                "--model-sha",
                embedder.model_sha,
                "--fixture",
                str(fixture_path),
                "--backend",
                "metal",
                "--batch",
                str(batch),
                "--seq-len",
                str(sequence_length),
                "--warmup-iters",
                "3",
                "--measure-iters",
                "10",
                "--print-embeddings",
            ]
            completed = subprocess.run(command, text=True, capture_output=True, check=True)
            records = []
            for line in (completed.stdout + "\n" + completed.stderr).splitlines():
                if not line.startswith("{"):
                    continue
                value = json.loads(line)
                if value.get("kind") == "nomic_direct_embeddings":
                    records.append(value)
            if len(records) != 1:
                raise RuntimeError(f"native parity cell b{batch}/s{sequence_length} emitted {len(records)} embedding records")
            actual = records[0].get("embeddings")
            if not isinstance(actual, list):
                raise RuntimeError(f"native parity cell b{batch}/s{sequence_length} has malformed embeddings")
            metrics = compare_embeddings(actual, reference)
            cell_passed = (
                metrics["max_abs_error"] <= 2e-6
                and metrics["mean_abs_error"] <= 3e-7
                and metrics["min_cosine"] >= 0.9999999
            )
            passed = passed and cell_passed
            emit_json(
                {
                    "kind": "nomic_direct_parity",
                    "model": embedder.model_name,
                    "model_sha": embedder.model_sha,
                    "batch": batch,
                    "sequence_length": sequence_length,
                    **metrics,
                    "passed": cell_passed,
                }
            )
    return 0 if passed else 1


def compare_embeddings(actual: list[list[float]], reference: list[list[float]]) -> dict[str, float]:
    if len(actual) != len(reference) or any(len(left) != len(right) for left, right in zip(actual, reference)):
        raise RuntimeError("native and PyTorch embedding shapes differ")
    errors = [abs(left_value - right_value) for left, right in zip(actual, reference) for left_value, right_value in zip(left, right)]
    cosines = []
    for left, right in zip(actual, reference):
        dot = sum(a * b for a, b in zip(left, right))
        left_norm = math.sqrt(sum(a * a for a in left))
        right_norm = math.sqrt(sum(b * b for b in right))
        cosines.append(dot / (left_norm * right_norm))
    return {
        "max_abs_error": max(errors),
        "mean_abs_error": statistics.fmean(errors),
        "min_cosine": min(cosines),
    }


def request_endpoint(url: str, body: bytes, timeout: float) -> tuple[float, list[list[float]]]:
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    start = time.perf_counter_ns()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read())
    elapsed = (time.perf_counter_ns() - start) / 1_000_000.0
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise RuntimeError("endpoint response lacks embeddings")
    embeddings = [item.get("embedding") for item in data]
    if any(not isinstance(embedding, list) or not embedding for embedding in embeddings):
        raise RuntimeError("endpoint response has an invalid embedding")
    return elapsed, embeddings


def summarize_ms(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    return {
        "mean_ms": statistics.fmean(samples),
        "p50_ms": statistics.median(samples),
        "p95_ms": ordered[math.ceil(0.95 * len(ordered)) - 1],
    }


def checksum(embeddings: list[list[float]]) -> float:
    return sum(sum(embedding[:16]) for embedding in embeddings)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def rss_bytes() -> int:
    # Darwin reports ru_maxrss in bytes; Linux reports KiB.
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def macos_swap_bytes() -> int | None:
    if sys.platform != "darwin":
        return None
    try:
        output = subprocess.check_output(["sysctl", "-n", "vm.swapusage"], text=True)
        match = re.search(r"used = ([0-9.]+)([KMG])", output)
        if match is None:
            return None
        scale = {"K": 1024, "M": 1024**2, "G": 1024**3}[match.group(2)]
        return int(float(match.group(1)) * scale)
    except Exception:
        return None


def emit_json(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True), flush=True)


if __name__ == "__main__":
    main_args = parse_args()
    if main_args.mode == "direct":
        direct(main_args)
    elif main_args.mode == "serve":
        serve(main_args)
    elif main_args.mode == "endpoint":
        endpoint(main_args)
    elif main_args.mode == "parity":
        raise SystemExit(parity(main_args))
    else:
        raise AssertionError(main_args.mode)
