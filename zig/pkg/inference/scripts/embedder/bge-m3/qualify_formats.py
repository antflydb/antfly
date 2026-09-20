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
# dependencies = ["numpy==2.2.6", "onnxruntime==1.22.1", "tokenizers==0.21.4"]
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
        "--model", action="append", required=True, help="label=managed-model-directory"
    )
    parser.add_argument("--backends", default="native,metal")
    parser.add_argument("--batches", default="1,2,4")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--minimum-cosine", type=float, default=0.995)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    batches = list(map(int, args.batches.split(",")))
    if not batches or any(batch < 1 or batch > 4 for batch in batches):
        parser.error("qualification batches must be between 1 and 4")
    if args.repeats < 1:
        parser.error("repeats must be positive")
    oracle_dir = (
        args.oracle_dir / "onnx"
        if (args.oracle_dir / "onnx/model.onnx").exists()
        else args.oracle_dir
    )
    tokenizer = Tokenizer.from_file(str(oracle_dir / "tokenizer.json"))
    session_options = ort.SessionOptions()
    session_options.intra_op_num_threads = 4
    session = ort.InferenceSession(
        str(oracle_dir / "model.onnx"),
        sess_options=session_options,
        providers=["CPUExecutionProvider"],
    )
    report = {
        "scope": "BGE-M3 dense embeddings; no sparse/ColBERT or CUDA claim",
        "oracle": str(oracle_dir),
        "host": platform.platform(),
        "minimum_cosine": args.minimum_cosine,
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
                cases = [(1, offset) for offset in range(max(batches))]
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
                    try:
                        result = subprocess.run(
                            command,
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
