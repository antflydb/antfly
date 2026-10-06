# Copyright 2026 Antfly, Inc.
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

# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = ["numpy==2.2.6", "onnxruntime==1.22.1", "tokenizers==0.21.4", "gguf==0.19.0", "onnx==1.23.0"]
# ///
"""Qualify managed BGE-M3 dense embeddings against the official ONNX export.

Checks tokenization, normalized CLS output, mixed-length batches, single/batched
parity and selected CPU/Metal execution. Records cold and warm performance;
never treats successful download as runtime qualification. Sparse and ColBERT
heads are separate capabilities and are not claimed by this dense probe.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import platform
import statistics
import subprocess
from pathlib import Path

import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer


def cosine_rows(a, b):
    a, b = np.asarray(a, dtype=np.float64), np.asarray(b, dtype=np.float64)
    return np.clip(
        np.sum(a * b, axis=-1)
        / (np.linalg.norm(a, axis=-1) * np.linalg.norm(b, axis=-1)),
        -1,
        1,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--oracle-dir", type=Path, required=True)
    parser.add_argument(
        "--oracle-gguf",
        type=Path,
        help="independently dequantize these GGUF weights into the official graph",
    )
    parser.add_argument(
        "--model", action="append", required=True, help="label=managed-model-directory"
    )
    parser.add_argument("--backends", default="native,metal")
    parser.add_argument("--batches", default="1,2,4")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--minimum-cosine", type=float, default=0.995)
    parser.add_argument("--texts-json", type=Path)
    parser.add_argument(
        "--graph-runtime",
        choices=["compiled-preferred", "partitioned", "interpreter"],
        default="compiled-preferred",
    )
    parser.add_argument("--require-resident-onnx", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    batches = list(map(int, args.batches.split(",")))
    if not batches or any(batch < 1 or batch > 128 for batch in batches):
        parser.error("qualification batches must be between 1 and 128")
    if args.repeats < 1:
        parser.error("repeats must be positive")
    corpus_size = len(json.loads(args.texts_json.read_text())) if args.texts_json else 4
    if corpus_size < 1:
        parser.error("text corpus must not be empty")
    oracle_dir = (
        args.oracle_dir / "onnx"
        if (args.oracle_dir / "onnx/model.onnx").exists()
        else args.oracle_dir
    )
    tokenizer = Tokenizer.from_file(str(oracle_dir / "tokenizer.json"))
    session_options = ort.SessionOptions()
    session_options.intra_op_num_threads = 4
    retained_initializers = []
    if args.oracle_gguf:
        from gguf_reference import add_gguf_initializers

        retained_initializers = add_gguf_initializers(
            session_options, oracle_dir / "model.onnx", args.oracle_gguf
        )
    session = ort.InferenceSession(
        str(oracle_dir / "model.onnx"),
        sess_options=session_options,
        providers=["CPUExecutionProvider"],
    )
    report = {
        "scope": "BGE-M3 dense embeddings; no sparse/ColBERT or CUDA claim",
        "oracle": str(oracle_dir),
        "oracle_gguf": str(args.oracle_gguf) if args.oracle_gguf else None,
        "overridden_initializers": len(retained_initializers),
        "texts_json": str(args.texts_json) if args.texts_json else None,
        "host": platform.platform(),
        "minimum_cosine": args.minimum_cosine,
        "graph_runtime": args.graph_runtime,
        "resident_onnx_required": args.require_resident_onnx,
        "minimum_parity_cosine": 0.9999,
        "models": {},
        "runs": [],
        "passed": True,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    artifact_dir = args.output.with_suffix(".artifacts")
    artifact_dir.mkdir(exist_ok=True)
    native_results = {}
    try:
        for model in args.model:
            label, directory = model.split("=", 1)
            receipt = json.loads(
                (Path(directory) / ".antfly-download-complete.json").read_text()
            )
            report["models"][label] = receipt.get("source")
            for backend in args.backends.split(","):
                singles = {}
                # Qualify every row individually before comparing padded
                # batches, including the Chinese and accented French inputs.
                cases = [(1, offset) for offset in range(corpus_size)]
                cases += [(batch, 0) for batch in batches if batch != 1]
                for batch, offset in cases:
                    command = [
                        str(args.binary.resolve()),
                        "--managed-only",
                        "--fixture",
                        str(
                            Path(__file__).resolve().parents[3]
                            / "src/bench/testdata/bge_m3_tokens.json"
                        ),
                        "--model-dir",
                        directory,
                        "--backend",
                        backend,
                        "--batch",
                        str(batch),
                        "--text-offset",
                        str(offset),
                        "--measure-iters",
                        str(args.repeats),
                    ]
                    if args.texts_json:
                        command += ["--texts-json", str(args.texts_json.resolve())]
                    env = dict(os.environ)
                    is_metal_onnx = (
                        backend == "metal"
                        and receipt.get("source", {}).get("selected_format") == "onnx"
                    )
                    if is_metal_onnx:
                        env["TERMITE_GRAPH_RUNTIME"] = args.graph_runtime
                        env["TERMITE_GRAPH_PARTITION_REPORT"] = "1"
                        env["TERMITE_GRAPH_EXECUTOR_STATS"] = "1"
                        env["TERMITE_METAL_PARTITION_RESIDENCY_STATS"] = "1"
                        if args.require_resident_onnx:
                            env["TERMITE_GRAPH_RUNTIME_REQUIRE_NO_FALLBACK"] = "1"
                            env["TERMITE_GRAPH_RUNTIME_REQUIRE_NO_HOST_ASSISTED"] = "1"
                    try:
                        result = subprocess.run(
                            command,
                            env=env,
                            text=True,
                            capture_output=True,
                            timeout=args.timeout,
                            check=True,
                        )
                        artifact_prefix = (
                            artifact_dir / f"{label}-{backend}-{batch}-{offset}"
                        )
                        artifact_prefix.with_suffix(".log").write_text(
                            result.stdout + result.stderr
                        )
                        records = [
                            json.loads(line)
                            for line in (result.stdout + result.stderr).splitlines()
                            if line.startswith('{"kind":"bge_m3_managed_qualification"')
                        ]
                        if len(records) != 1:
                            raise ValueError("missing managed qualification result")
                        record = records[0]
                        if record["backend"] != backend:
                            raise ValueError(
                                "runtime silently selected a different backend"
                            )
                        graph = record.get("graph_after")
                        if is_metal_onnx:
                            before = record.get("graph_before")
                            if not graph or not before or graph["last_batch"] != batch:
                                raise ValueError("missing proof of actual graph batch")
                            if (
                                graph["executions"] - before["executions"]
                                != args.repeats
                            ):
                                raise ValueError(
                                    "managed batch was split into multiple graph executions"
                                )
                            if graph["plan_builds"] != before["plan_builds"]:
                                raise ValueError("warm request rebuilt its cached plan")
                            if (
                                args.require_resident_onnx
                                and args.graph_runtime == "interpreter"
                            ):
                                raise ValueError(
                                    "resident qualification requires planned execution"
                                )
                        partitions = [
                            dict(
                                zip(
                                    (
                                        "target_nodes",
                                        "fallback_nodes",
                                        "host_assisted_nodes",
                                    ),
                                    map(int, match),
                                )
                            )
                            for match in re.findall(
                                r"summary target_nodes=(\d+) fallback_nodes=(\d+) host_assisted_target_nodes=(\d+)",
                                result.stderr,
                            )
                        ]
                        if (
                            is_metal_onnx
                            and args.require_resident_onnx
                            and (
                                not partitions
                                or any(
                                    p["fallback_nodes"] or p["host_assisted_nodes"]
                                    for p in partitions
                                )
                            )
                        ):
                            raise ValueError("graph residency requirements not met")
                        executor_stats = [
                            dict(
                                (key, int(value))
                                for key, value in re.findall(r"(\w+)=(\d+)", line)
                            )
                            for line in result.stderr.splitlines()
                            if line.startswith("graph_executor_stats:")
                        ]
                        if is_metal_onnx and args.require_resident_onnx:
                            if not executor_stats or any(
                                stat.get("transfers", 0) or stat.get("host_outputs", 0)
                                for stat in executor_stats
                            ):
                                raise ValueError(
                                    "executor materialized intermediate tensors on CPU"
                                )
                            if not all(
                                stat.get("graph_plan_slots", 0) > 0
                                and stat.get("metal_frame_chunk_boundaries", 0) > 0
                                for stat in executor_stats
                            ):
                                raise ValueError(
                                    "planned buffers or bounded command frames were not used"
                                )
                        ids = [tokenizer.encode(text).ids for text in record["texts"]]
                        if ids != record["token_ids"]:
                            raise ValueError(
                                "token IDs differ from the official export tokenizer"
                            )
                        length = max(map(len, ids))
                        inputs = np.full((batch, length), 1, dtype=np.int64)
                        mask = np.zeros_like(inputs)
                        for i, row in enumerate(ids):
                            inputs[i, : len(row)] = row
                            mask[i, : len(row)] = 1
                        feed = {"input_ids": inputs, "attention_mask": mask}
                        if any(
                            item.name == "token_type_ids"
                            for item in session.get_inputs()
                        ):
                            feed["token_type_ids"] = np.zeros_like(inputs)
                        outputs = session.run(None, feed)
                        expected = outputs[0]
                        if expected.ndim == 3:
                            expected = expected[:, 0, :]
                        expected = expected / np.linalg.norm(
                            expected, axis=1, keepdims=True
                        )
                        actual = np.asarray(record["embeddings"])
                        if (
                            actual.shape != (batch, 1024)
                            or not np.isfinite(actual).all()
                        ):
                            raise ValueError(f"invalid embeddings: {actual.shape}")
                        np.testing.assert_allclose(
                            np.linalg.norm(actual, axis=1), 1, atol=1e-4
                        )
                        cosine = cosine_rows(actual, expected)
                        if float(cosine.min()) < args.minimum_cosine:
                            raise ValueError(
                                f"oracle cosine below threshold: {cosine.min()}"
                            )
                        parity_key = (label, batch, offset)
                        backend_cosine = None
                        if backend == "native":
                            native_results[parity_key] = actual
                        elif parity_key in native_results:
                            backend_cosine = float(
                                cosine_rows(actual, native_results[parity_key]).min()
                            )
                            if backend_cosine < 0.9999:
                                raise ValueError(
                                    f"CPU/GPU cosine below threshold: {backend_cosine}"
                                )
                        if batch == 1:
                            singles[record["texts"][0]] = actual[0]
                        batch_cosine = 1.0
                        for text, row in zip(record["texts"], actual):
                            if text in singles:
                                batch_cosine = min(
                                    batch_cosine,
                                    float(cosine_rows([row], [singles[text]])[0]),
                                )
                            if batch_cosine < 0.9999:
                                raise ValueError(
                                    "embedding changes under batch padding"
                                )
                        median = statistics.median(record["warm_ms"])
                        report["runs"].append(
                            {
                                "model": label,
                                "backend": backend,
                                "batch": batch,
                                "text_offset": offset,
                                "minimum_cosine": float(cosine.min()),
                                "cpu_gpu_minimum_cosine": backend_cosine,
                                "single_batch_minimum_cosine": batch_cosine,
                                "cold_ms": record["cold_ms"],
                                "warm_median_ms": median,
                                "embeddings_per_second": 1000 * batch / median,
                                "graph": graph,
                                "partitions": partitions,
                                "executor_stats": executor_stats,
                                "passed": True,
                            }
                        )
                    except (
                        subprocess.SubprocessError,
                        ValueError,
                        AssertionError,
                    ) as error:
                        report["passed"] = False
                        detail = str(error)
                        if isinstance(error, subprocess.CalledProcessError):
                            detail += "\n" + error.stderr[-6000:]
                        report["runs"].append(
                            {
                                "model": label,
                                "backend": backend,
                                "batch": batch,
                                "text_offset": offset,
                                "passed": False,
                                "error": detail,
                            }
                        )
                    args.output.write_text(json.dumps(report, indent=2) + "\n")
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
