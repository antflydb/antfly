# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Offline, serial paired Q8 Decide-1B CUDA/PyTorch comparison.

Use an authenticated local bundle and a prepared capture with independent Q8
reference logits. No downloads or serving qualification. Raw samples stay in
the requested output directory. Both clocks include CPU input upload, encoder,
F32 classifier and completed CPU logit readback, excluding tokenization/load.
"""

from __future__ import annotations

import argparse
import contextlib
import gzip
import hashlib
import itertools
import json
import math
import mmap
import os
from pathlib import Path
import selectors
import signal
import struct
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import paired_benchmark


def emit(value):
    print(json.dumps(value, allow_nan=False, separators=(",", ":")), flush=True)


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def decoded_weights(path):
    """Decode GGUF F32/Q8_0 without expanding the entire bundle on the host."""
    import numpy as np
    import torch

    with (
        path.open("rb") as source,
        mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as data,
    ):
        offset = 0

        def take(fmt):
            nonlocal offset
            result = struct.unpack_from("<" + fmt, data, offset)
            offset += struct.calcsize("<" + fmt)
            return result[0]

        def string():
            nonlocal offset
            count = take("Q")
            result = data[offset : offset + count].decode()
            offset += count
            return result

        def value(kind):
            if kind == 8:
                return string()
            if kind == 9:
                element, count = take("I"), take("Q")
                return [value(element) for _ in range(count)]
            return take(
                {
                    0: "B",
                    1: "b",
                    2: "H",
                    3: "h",
                    4: "I",
                    5: "i",
                    6: "f",
                    7: "?",
                    10: "Q",
                    11: "q",
                    12: "d",
                }[kind]
            )

        if data[:4] != b"GGUF":
            raise ValueError("invalid GGUF magic")
        offset = 4
        if take("I") != 3:
            raise ValueError("unsupported GGUF version")
        tensors, metadata_count = take("Q"), take("Q")
        metadata = {}
        for _ in range(metadata_count):
            key = string()
            metadata[key] = value(take("I"))
        descriptors = []
        for _ in range(tensors):
            name, rank = string(), take("I")
            shape = [take("Q") for _ in range(rank)][::-1]
            descriptors.append((name, shape, take("I"), take("Q")))
        alignment = metadata.get("general.alignment", 32)
        if alignment <= 0 or alignment & (alignment - 1):
            raise ValueError("invalid GGUF alignment")
        base = (offset + alignment - 1) // alignment * alignment
        for name, shape, kind, relative in descriptors:
            count, start = math.prod(shape), base + relative
            if kind == 0:
                decoded = np.frombuffer(
                    data, dtype="<f4", count=count, offset=start
                ).copy()
            elif kind == 8 and count % 32 == 0:
                blocks = np.frombuffer(
                    data, dtype=np.uint8, count=count // 32 * 34, offset=start
                ).reshape(-1, 34)
                scales = blocks[:, :2].copy().view("<f2").astype(np.float32)
                decoded = (
                    blocks[:, 2:].view(np.int8).astype(np.float32) * scales
                ).reshape(-1)
                del blocks, scales
            else:
                raise ValueError(f"unsupported GGUF tensor {name}: {kind}")
            if not np.isfinite(decoded).all():
                raise ValueError(f"non-finite GGUF tensor {name}")
            yield name, torch.from_numpy(decoded.reshape(shape)).to("cuda")


def python_worker(args):
    import torch
    import transformers
    from transformers import ModernBertConfig, ModernBertModel
    from transformers.models.modernbert.modeling_modernbert import (
        ModernBertRotaryEmbedding,
    )

    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    config = ModernBertConfig.from_pretrained(
        args.model_dir / "encoder_config", local_files_only=True
    )
    config._attn_implementation = "sdpa"
    config.vocab_size = 50378
    with torch.device("meta"):
        model = ModernBertModel(config)
    weights = dict(decoded_weights(args.model_dir / "gliner2-encoder.Q8_0.gguf"))
    head = {
        name: tensor
        for name, tensor in weights.items()
        if name.startswith("classifier.")
    }
    model.load_state_dict(
        {
            name.removeprefix("encoder."): tensor
            for name, tensor in weights.items()
            if name.startswith("encoder.")
        },
        strict=True,
        assign=True,
    )
    del weights
    if len(head) != 4:
        raise ValueError("expected four F32 classifier tensors")
    if args.dtype == "fp16":
        model.half()
    model.rotary_emb = ModernBertRotaryEmbedding(config, device="cuda")
    model.eval()
    cases = json.loads(args.capture.read_text())
    prepared = []
    for case in cases:
        ids = torch.tensor([case["input_ids"]], dtype=torch.long)
        positions = (ids[0] == 50374).nonzero().flatten()
        if len(positions) != sum(map(len, case["logits"])):
            raise ValueError("label marker/logit count mismatch")
        prepared.append((ids, torch.ones_like(ids), positions))
    emit(
        {
            "event": "ready",
            "backend": "pytorch",
            "torch": torch.__version__,
            "transformers": transformers.__version__,
            "encoder_dtype": args.dtype,
            "classifier_dtype": "fp32",
            "attention": "sdpa",
            "tf32": False,
            "cases": len(cases),
            "resident_device_bytes": torch.cuda.memory_allocated(),
        }
    )
    with torch.inference_mode():
        for line in sys.stdin:
            if len(line) > 2048:
                raise ValueError("oversized command")
            command = json.loads(line)
            if command["op"] == "stop":
                return
            if command["op"] not in ("validate", "run"):
                raise ValueError("invalid command")
            index = command["case_index"]
            case = cases[index]
            ids_host, mask_host, positions_host = prepared[index]
            torch.cuda.synchronize()
            start = time.perf_counter_ns()
            ids, mask, positions = (
                ids_host.to("cuda"),
                mask_host.to("cuda"),
                positions_host.to("cuda"),
            )
            hidden = (
                model(input_ids=ids, attention_mask=mask)
                .last_hidden_state[0]
                .index_select(0, positions)
                .float()
            )
            hidden = torch.nn.functional.relu(
                torch.nn.functional.linear(
                    hidden, head["classifier.0.weight"], head["classifier.0.bias"]
                )
            )
            logits = (
                torch.nn.functional.linear(
                    hidden, head["classifier.2.weight"], head["classifier.2.bias"]
                )
                .flatten()
                .cpu()
                .tolist()
            )
            torch.cuda.synchronize()
            elapsed = (time.perf_counter_ns() - start) / 1e6
            rows, offset = [], 0
            for expected in case["logits"]:
                rows.append(logits[offset : offset + len(expected)])
                offset += len(expected)
            error = max(
                abs(got - want)
                for row, expected in zip(rows, case["logits"])
                for got, want in zip(row, expected)
            )
            if not args.reference and error > args.python_tolerance:
                raise ValueError(
                    f"{case['id']}: Python error {error} exceeds {args.python_tolerance}"
                )
            emit(
                {
                    "request_id": command["request_id"],
                    "id": case["id"],
                    "tokens": len(case["input_ids"]),
                    "core_ms": elapsed,
                    "max_logit_error": error,
                    "logits": rows,
                }
            )
    raise ValueError("protocol ended without stop")


class Worker:
    def __init__(self, name, command, env, directory):
        self.name, self.sequence, self.buffer = name, 0, bytearray()
        self.log = (directory / f"{name}.stderr.log").open("wb")
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            env=env,
            bufsize=0,
            start_new_session=True,
        )
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)

    def receive(self, timeout=120):
        deadline = time.monotonic() + timeout
        while b"\n" not in self.buffer:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{self.name}: response deadline exceeded")
            if self.selector.select(0.1):
                data = os.read(self.process.stdout.fileno(), 65536)
                if not data:
                    raise RuntimeError(
                        f"{self.name}: exited ({self.process.poll()}); see {self.log.name}"
                    )
                self.buffer.extend(data)
                if len(self.buffer) > 4 * 1024 * 1024:
                    raise ValueError("oversized response")
        line, _, self.buffer = self.buffer.partition(b"\n")
        return json.loads(line)

    def run(self, index, op="run"):
        self.sequence += 1
        self.process.stdin.write(
            json.dumps(
                {"request_id": self.sequence, "op": op, "case_index": index}
            ).encode()
            + b"\n"
        )
        self.process.stdin.flush()
        result = self.receive()
        if (
            result["request_id"] != self.sequence
            or not math.isfinite(result["core_ms"])
            or result["core_ms"] <= 0
        ):
            raise ValueError("invalid worker response")
        return result

    def close(self):
        if self.process.poll() is None:
            with contextlib.suppress(BrokenPipeError):
                self.process.stdin.write(b'{"request_id":0,"op":"stop"}\n')
                self.process.stdin.flush()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        self.selector.close()
        self.process.stdin.close()
        self.process.stdout.close()
        self.log.close()


def validate(result, case):
    if (
        result["id"] != case["id"]
        or result["tokens"] != len(case["input_ids"])
        or len(result["logits"]) != len(case["logits"])
    ):
        raise ValueError("case identity mismatch")
    for got, expected in zip(result["logits"], case["logits"]):
        if len(got) != len(expected) or not all(math.isfinite(value) for value in got):
            raise ValueError("invalid logits")
        if max(range(len(got)), key=got.__getitem__) != max(
            range(len(expected)), key=expected.__getitem__
        ):
            raise ValueError(f"{case['id']}: selected label mismatch")


def campaign(args):
    if args.pairs < 30 or args.tails < 200 or args.warmups < 3:
        raise ValueError(
            "require at least 30 paired rounds, 200 tail rounds, three warmups"
        )
    args.output.mkdir(parents=True, exist_ok=False)
    cases = json.loads(args.capture.read_text())
    workers, ready, final_stats, reports = {}, {}, {}, []
    native_command = [
        str(args.native),
        "--model-dir",
        str(args.model_dir),
        "--capture",
        str(args.capture),
        "--backend",
        "cuda",
        "--worker",
        "1",
        "--tolerance",
        str(args.native_tolerance),
    ]
    python_command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--model-dir",
        str(args.model_dir),
        "--capture",
        str(args.capture),
        "--python-tolerance",
        str(args.python_tolerance),
    ]
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("ANTFLY_"):
            del env[key]
    env["ANTFLY_CUDA_GLINER_1B_Q8_F16_MIRRORS"] = "1"
    try:
        for name, command, attention in [
            ("baseline", native_command, "0"),
            ("candidate", native_command, "1"),
            ("python", python_command, "1"),
        ]:
            workers[name] = Worker(
                name,
                command,
                {**env, "ANTFLY_CUDA_GLINER_1B_Q8_F16_ATTENTION": attention},
                args.output,
            )
            ready[name] = workers[name].receive(timeout=180)
            if ready[name].get("event") != "ready" or ready[name].get("cases") != len(
                cases
            ):
                raise ValueError("worker readiness mismatch")
        orders = list(itertools.permutations(workers))
        with gzip.open(args.output / "raw.jsonl.gz", "wt") as raw:
            for index, case in enumerate(cases):
                for worker in workers.values():
                    for _ in range(1 + args.warmups):
                        validate(worker.run(index, "validate"), case)
                samples = {name: [] for name in workers}
                errors = {name: 0.0 for name in workers}
                for iteration in range(args.pairs + args.tails):
                    results = {}
                    for name in orders[iteration % len(orders)]:
                        result = workers[name].run(index)
                        validate(result, case)
                        samples[name].append(result["core_ms"])
                        errors[name] = max(errors[name], result["max_logit_error"])
                        final_stats[name] = result.get("cuda_stats")
                        results[name] = result
                    raw.write(
                        json.dumps(
                            {
                                "case": case["id"],
                                "round": iteration,
                                "phase": "paired" if iteration < args.pairs else "tail",
                                "order": orders[iteration % len(orders)],
                                "results": results,
                            },
                            allow_nan=False,
                        )
                        + "\n"
                    )
                report = {
                    "id": case["id"],
                    "tokens": len(case["input_ids"]),
                    "latency_ms": {
                        name: paired_benchmark.distribution(values)
                        for name, values in samples.items()
                    },
                    "max_logit_error": errors,
                }
                for name, numerator, denominator in [
                    ("baseline_speedup", "baseline", "candidate"),
                    ("python_speedup", "python", "candidate"),
                ]:
                    report[name] = paired_benchmark.paired_log_ratio_ci(
                        list(
                            zip(
                                samples[numerator][: args.pairs],
                                samples[denominator][: args.pairs],
                            )
                        ),
                        seed=20261009,
                    )
                reports.append(report)
                print(case["id"], report["latency_ms"], flush=True)
        result = {
            "scope": "offline prepared CPU input upload, encoder, F32 classifier and completed CPU logit readback; excludes HTTP, model load and tokenization",
            "qualification": False,
            "pairs": args.pairs,
            "tails": args.tails,
            "warmups": args.warmups,
            "ordering": "serial balanced permutations of baseline/candidate/python; all models resident",
            "native_tolerance": args.native_tolerance,
            "python_tolerance": args.python_tolerance,
            "native_sha256": digest(args.native),
            "capture_sha256": digest(args.capture),
            "script_sha256": digest(Path(__file__).resolve()),
            "model_hashes": {
                str(path.relative_to(args.model_dir)): digest(path)
                for path in sorted(args.model_dir.rglob("*"))
                if path.is_file()
            },
            "gpu": subprocess.check_output(
                [
                    "nvidia-smi",
                    "--query-gpu=name,driver_version,pci.bus_id",
                    "--format=csv,noheader",
                ],
                text=True,
            ).strip(),
            "ready": ready,
            "final_stats": final_stats,
            "reports": reports,
        }
        (args.output / "report.json").write_text(
            json.dumps(result, indent=2, allow_nan=False) + "\n"
        )
    finally:
        for worker in workers.values():
            worker.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--native", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--pairs", type=int, default=30)
    parser.add_argument("--tails", type=int, default=200)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--native-tolerance", type=float, default=0.002)
    parser.add_argument("--python-tolerance", type=float, default=0.01)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--dtype", choices=("fp16", "fp32"), default="fp16")
    parser.add_argument(
        "--reference",
        action="store_true",
        help="emit independent F32 oracle logits; worker only",
    )
    args = parser.parse_args()
    if any(
        not math.isfinite(value) or value <= 0
        for value in (args.native_tolerance, args.python_tolerance)
    ):
        parser.error("logit tolerances must be finite and positive")
    if args.worker:
        if args.reference and args.dtype != "fp32":
            parser.error("reference worker requires fp32")
        python_worker(args)
    else:
        if args.native is None or args.output is None or args.reference:
            parser.error("campaign requires --native and --output")
        campaign(args)


if __name__ == "__main__":
    main()
