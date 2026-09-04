#!/usr/bin/env python3
"""PyTorch reference, benchmark, and native parity harness for BAAI/bge-m3.

The direct and parity modes consume the same checked-in token IDs and masks as
`bench-bge-m3-e2e`. Model loading and tokenization are outside timed regions.
BGE-M3 dense embeddings use the normalized CLS token; sparse and ColBERT heads
are intentionally outside this dense endpoint contract.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import re
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


MODEL_NAME = "BAAI/bge-m3"
VOCAB_SIZE = 250002
PAD_TOKEN_ID = 1
ALLOWED_BATCHES = {1, 2, 4}
ALLOWED_LENGTHS = {16, 128}
INFERENCE_ROOT = Path(__file__).resolve().parents[3]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="mode", required=True)

    def add_common(command: argparse.ArgumentParser) -> None:
        command.add_argument("--model-dir", required=True)
        command.add_argument("--model-name", default=MODEL_NAME)
        command.add_argument(
            "--fixture",
            default=str(INFERENCE_ROOT / "src/bench/testdata/bge_m3_tokens.json"),
        )
        command.add_argument("--device", choices=("cpu", "mps"), required=True)
        command.add_argument("--batches", default="1,2,4")
        command.add_argument("--sequence-lengths", default="16,128")

    direct_parser = subparsers.add_parser("direct")
    add_common(direct_parser)
    direct_parser.add_argument("--warmups", type=int, default=3)
    direct_parser.add_argument("--repeats", type=int, default=10)
    direct_parser.add_argument("--print-embeddings", action="store_true")

    parity_parser = subparsers.add_parser("parity")
    add_common(parity_parser)
    parity_parser.add_argument("--native-binary", required=True)
    parity_parser.add_argument("--native-backend", choices=("native", "metal"), default="metal")
    parity_parser.add_argument("--native-warmups", type=int, default=1)
    parity_parser.add_argument("--native-repeats", type=int, default=1)

    args = parser.parse_args()
    args.batches = parse_int_set(args.batches, ALLOWED_BATCHES, "batches")
    args.sequence_lengths = parse_int_set(
        args.sequence_lengths, ALLOWED_LENGTHS, "sequence lengths"
    )
    if args.mode == "direct" and (args.warmups != 3 or args.repeats != 10):
        parser.error("direct evidence requires exactly 3 warmups and 10 repeats")
    return args


def parse_int_set(value: str, allowed: set[int], label: str) -> list[int]:
    result = [int(item) for item in value.split(",")]
    if not result or any(item not in allowed for item in result):
        raise argparse.ArgumentTypeError(f"{label} must be drawn from {sorted(allowed)}")
    return result


class BgeM3Embedder:
    def __init__(self, model_dir: str, model_name: str, device_name: str):
        import torch
        from transformers import AutoModel

        if device_name == "mps" and not torch.backends.mps.is_available():
            raise RuntimeError("PyTorch MPS is unavailable")
        self.torch = torch
        self.device_name = device_name
        self.device = torch.device(device_name)
        self.model = AutoModel.from_pretrained(
            model_dir,
            local_files_only=True,
            torch_dtype=torch.float32,
        ).to(self.device).eval()
        self.model_name = model_name
        self.model_sha = sha256_file(Path(model_dir) / "model.safetensors")

    def embed_ids(self, input_ids: Any, attention_mask: Any) -> Any:
        with self.torch.inference_mode():
            hidden = self.model(
                input_ids=input_ids,
                attention_mask=attention_mask,
            ).last_hidden_state
            embeddings = self.torch.nn.functional.normalize(hidden[:, 0], p=2, dim=1)
            if self.device_name == "mps":
                self.torch.mps.synchronize()
            return embeddings

    def release(self) -> None:
        del self.model
        gc.collect()
        if self.device_name == "mps":
            self.torch.mps.empty_cache()
            self.torch.mps.synchronize()


def load_fixture(path: str) -> dict[str, Any]:
    fixture = json.loads(Path(path).read_text())
    if (
        fixture.get("model") != MODEL_NAME
        or fixture.get("vocab_size") != VOCAB_SIZE
        or fixture.get("pad_token_id") != PAD_TOKEN_ID
    ):
        raise RuntimeError("unexpected BGE-M3 fixture identity")
    for length in ALLOWED_LENGTHS:
        ids = fixture.get(f"input_ids_{length}")
        mask = fixture.get(f"attention_mask_{length}")
        if (
            not isinstance(ids, list)
            or len(ids) != length
            or any(not isinstance(value, int) or value < 0 or value >= VOCAB_SIZE for value in ids)
            or not isinstance(mask, list)
            or len(mask) != length
            or any(value not in (0, 1) for value in mask)
        ):
            raise RuntimeError(f"invalid length-{length} BGE-M3 fixture")
    return fixture


def fixture_tensors(embedder: BgeM3Embedder, fixture: dict[str, Any], batch: int, length: int):
    torch = embedder.torch
    ids = [fixture[f"input_ids_{length}"]] * batch
    masks = [fixture[f"attention_mask_{length}"]] * batch
    return (
        torch.tensor(ids, dtype=torch.long, device=embedder.device),
        torch.tensor(masks, dtype=torch.long, device=embedder.device),
    )


def direct(args: argparse.Namespace) -> int:
    fixture = load_fixture(args.fixture)
    embedder = BgeM3Embedder(args.model_dir, args.model_name, args.device)
    try:
        for length in args.sequence_lengths:
            for batch in args.batches:
                input_ids, attention_mask = fixture_tensors(embedder, fixture, batch, length)
                for _ in range(args.warmups):
                    embedder.embed_ids(input_ids, attention_mask)
                rss_before = rss_bytes()
                swap_before = macos_swap_bytes()
                samples: list[float] = []
                output = None
                for _ in range(args.repeats):
                    start = time.perf_counter_ns()
                    output = embedder.embed_ids(input_ids, attention_mask)
                    samples.append((time.perf_counter_ns() - start) / 1_000_000.0)
                assert output is not None
                embeddings = output.cpu().tolist()
                emit_json(
                    {
                        "kind": "bge_m3_direct_reference",
                        "model": embedder.model_name,
                        "model_sha": embedder.model_sha,
                        "backend": "pytorch",
                        "device": args.device,
                        "fixture_seed": fixture["seed"],
                        "batch": batch,
                        "sequence_length": length,
                        "warmups": args.warmups,
                        "repeats": args.repeats,
                        **summarize_ms(samples),
                        "output_checksum": checksum(embeddings),
                        "rss_bytes": {"before": rss_before, "after": rss_bytes()},
                        "swap_bytes": {
                            "before": swap_before,
                            "after": macos_swap_bytes(),
                            "available": swap_before is not None,
                        },
                    }
                )
                if args.print_embeddings:
                    emit_json(
                        {
                            "kind": "bge_m3_direct_reference_embeddings",
                            "model_sha": embedder.model_sha,
                            "batch": batch,
                            "sequence_length": length,
                            "embeddings": embeddings,
                        }
                    )
    finally:
        embedder.release()
    return 0


def parity(args: argparse.Namespace) -> int:
    fixture_path = Path(args.fixture).resolve()
    fixture = load_fixture(str(fixture_path))
    native_binary = Path(args.native_binary).resolve()
    if not native_binary.is_file():
        raise RuntimeError(f"native benchmark binary does not exist: {native_binary}")

    embedder = BgeM3Embedder(args.model_dir, args.model_name, args.device)
    model_sha = embedder.model_sha
    references: dict[tuple[int, int], list[list[float]]] = {}
    for length in args.sequence_lengths:
        for batch in args.batches:
            inputs = fixture_tensors(embedder, fixture, batch, length)
            references[(batch, length)] = embedder.embed_ids(*inputs).cpu().tolist()
    # Never overlap the multi-gigabyte PyTorch and native model residencies on
    # resource-constrained Macs. This also keeps swap growth out of parity.
    embedder.release()
    del embedder

    passed = True
    for length in args.sequence_lengths:
        for batch in args.batches:
            command = [
                str(native_binary),
                "--model-dir", str(Path(args.model_dir).resolve()),
                "--model-sha", model_sha,
                "--fixture", str(fixture_path),
                "--backend", args.native_backend,
                "--batch", str(batch),
                "--seq-len", str(length),
                "--warmup-iters", str(args.native_warmups),
                "--measure-iters", str(args.native_repeats),
                "--print-embeddings",
            ]
            completed = subprocess.run(command, text=True, capture_output=True, check=True)
            records = [
                json.loads(line)
                for line in (completed.stdout + "\n" + completed.stderr).splitlines()
                if line.startswith("{")
                and json.loads(line).get("kind") == "bge_m3_direct_embeddings"
            ]
            if len(records) != 1:
                raise RuntimeError(
                    f"native b{batch}/s{length} emitted {len(records)} embedding records"
                )
            metrics = compare_embeddings(
                records[0]["embeddings"], references[(batch, length)]
            )
            cell_passed = (
                metrics["max_abs_error"] <= 3e-5
                and metrics["mean_abs_error"] <= 3e-6
                and metrics["min_cosine"] >= 0.999999
            )
            passed = passed and cell_passed
            emit_json(
                {
                    "kind": "bge_m3_direct_parity",
                    "model": args.model_name,
                    "model_sha": model_sha,
                    "reference_device": args.device,
                    "native_backend": args.native_backend,
                    "batch": batch,
                    "sequence_length": length,
                    **metrics,
                    "passed": cell_passed,
                }
            )
    return 0 if passed else 1


def compare_embeddings(actual: list[list[float]], reference: list[list[float]]) -> dict[str, float]:
    if len(actual) != len(reference) or any(
        len(left) != len(right) for left, right in zip(actual, reference)
    ):
        raise RuntimeError("native and PyTorch embedding shapes differ")
    errors = [
        abs(a - b)
        for left, right in zip(actual, reference)
        for a, b in zip(left, right)
    ]
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
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def macos_swap_bytes() -> int | None:
    if sys.platform != "darwin":
        return None
    try:
        output = subprocess.check_output(["sysctl", "-n", "vm.swapusage"], text=True)
    except (OSError, subprocess.CalledProcessError):
        return None
    match = re.search(r"used = ([0-9.]+)([KMG])", output)
    if match is None:
        return None
    scale = {"K": 1024, "M": 1024**2, "G": 1024**3}[match.group(2)]
    return round(float(match.group(1)) * scale)


def emit_json(value: dict[str, Any]) -> None:
    print(json.dumps(value, separators=(",", ":")), flush=True)


def main() -> int:
    args = parse_args()
    if args.mode == "direct":
        return direct(args)
    return parity(args)


if __name__ == "__main__":
    raise SystemExit(main())
