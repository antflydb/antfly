#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Offline full-width Qwen3-Embedding-4B reference and native CLI comparison.

This independently checks last-token pooling, L2 normalization, exact encoded
IDs and ragged batches using Transformers. It does not attest upstream vLLM
GPU numerics. No weights, tokenizer or code downloads run implicitly.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any

import torch
from safetensors.torch import save_file

from assets import lock_model, verify_base_model


SCHEMA = "antfly.memorilla.encoder-reference.v1"
WIDTH = 2560
EOS = 151643
DTYPES = {"float32": torch.float32, "bfloat16": torch.bfloat16}
NATIVE_MANIFEST = (
    '{"type":"embedder","pooling":"last","normalize":true,'
    '"embedding_style":"qwen3_embedding","embedding_trailing_eos_id":151643,'
    '"embedding_profile":{"task_contract":"profiled","query":{"prefix":""},'
    '"document":{"prefix":""}}}\n'
).encode()
NATIVE_MANIFEST_SHA256 = (
    "cde71e9919a43e374ad7aae68f56943f01b9ddd84d26601cf8155a4c123631fa"
)


def install_native_manifest(model_dir: Path) -> None:
    """Explicitly add the catalog's recipe without changing upstream artifacts."""
    if hashlib.sha256(NATIVE_MANIFEST).hexdigest() != NATIVE_MANIFEST_SHA256:
        raise ValueError("native encoder recipe differs from pinned catalog bytes")
    path = model_dir / "model_manifest.json"
    if path.exists():
        if path.read_bytes() != NATIVE_MANIFEST:
            raise ValueError(
                "existing native model manifest differs from the memory recipe"
            )
        return
    with tempfile.NamedTemporaryFile(
        dir=model_dir, prefix=".memorilla-manifest-", delete=False
    ) as stream:
        temporary = Path(stream.name)
        stream.write(NATIVE_MANIFEST)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != NATIVE_MANIFEST:
                raise ValueError("native model manifest changed during publication")
    finally:
        temporary.unlink()


@dataclass(frozen=True)
class Case:
    id: str
    role: str
    text: str

    def rendered(self) -> str:
        if self.role == "document":
            return self.text
        if self.role == "question":
            # Match the released dataset-cache preparation. The online answer
            # wrapper accepts raw strings; callers must choose the same policy.
            return self.text.strip()
        raise ValueError(f"unsupported encoder case role: {self.role}")


def default_cases() -> list[Case]:
    return [
        Case("empty_document", "document", ""),
        Case("short_document", "document", "Antfly stores immutable source versions."),
        Case(
            "unicode_document",
            "document",
            "Café notes: María prefers tea. Her morning meetings start at 09:30; "
            "the database keeps corrections and their supporting evidence.",
        ),
        Case(
            "question",
            "question",
            "  What does Antfly preserve when a source changes?  ",
        ),
    ]


def verify_tokenizer_cases(cases: list[Case], token_ids: list[list[int]]) -> None:
    fixture = json.loads(
        Path(__file__)
        .with_name("fixtures")
        .joinpath("encoder-tokenizer.json")
        .read_text()
    )
    actual = [
        dict(asdict(case), rendered=case.rendered(), token_ids=row)
        for case, row in zip(cases, token_ids, strict=True)
    ]
    lock = lock_model("encoder")
    sidecars = [
        entry for entry in lock["files"] if not entry["path"].endswith(".safetensors")
    ]
    if (
        fixture.get("schema") != "antfly.memorilla.encoder-tokenizer.v1"
        or fixture.get("model") != lock["model"]
        or fixture.get("revision") != lock["revision"]
        or fixture.get("verified_sidecars") != sidecars
        or actual != fixture.get("cases")
    ):
        raise ValueError("encoded cases differ from the pinned tokenizer fixture")


def active_ids(encoded: dict[str, torch.Tensor]) -> list[list[int]]:
    ids, mask = encoded["input_ids"], encoded["attention_mask"]
    if ids.ndim != 2 or ids.shape != mask.shape or not ids.shape[0]:
        raise ValueError("encoder inputs must be nonempty matching batch matrices")
    if not torch.all((mask == 0) | (mask == 1)).item():
        raise ValueError("attention mask must contain only zero and one")
    result = []
    for row, used in zip(ids, mask, strict=True):
        active = row[used.bool()].tolist()
        if not active or active[-1] != EOS:
            raise ValueError(f"encoder input must end with tokenizer EOS {EOS}")
        result.append(active)
    return result


def last_token_pool(hidden: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    if hidden.ndim != 3 or hidden.shape[:2] != mask.shape:
        raise ValueError("hidden states and padding mask have incompatible shapes")
    if (
        not torch.all((mask == 0) | (mask == 1)).item()
        or torch.any(mask.sum(1) == 0).item()
    ):
        raise ValueError("pooling requires a binary mask with an active token per row")
    positions = torch.arange(mask.shape[1], device=mask.device).expand_as(mask)
    last = positions.masked_fill(mask == 0, -1).max(dim=1).values
    return hidden[torch.arange(hidden.shape[0], device=hidden.device), last]


def normalize_features(hidden: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    pooled = last_token_pool(hidden, mask)
    if pooled.shape[1] != WIDTH or not torch.isfinite(pooled).all().item():
        raise ValueError(f"encoder must produce finite full-width {WIDTH} states")
    if torch.any(pooled.float().norm(dim=-1) == 0).item():
        raise ValueError("encoder produced a zero pooled vector")
    # Upstream vLLM 0.9.1 normalizes in the pooled tensor's dtype, without an
    # explicit float32 conversion. Record the chosen reference dtype below.
    return torch.nn.functional.normalize(pooled, p=2, dim=-1)


def storage_features(normalized: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
    if (
        normalized.ndim != 2
        or normalized.shape[1] != WIDTH
        or not torch.isfinite(normalized).all().item()
    ):
        raise ValueError(f"stored features must be finite [batch, {WIDTH}]")
    stored = normalized.to(torch.float16)
    return stored, stored.to(torch.bfloat16)


def compare_vectors(
    actual: torch.Tensor,
    expected: torch.Tensor,
    *,
    atol: float,
    min_cosine: float,
) -> dict:
    if (
        not math.isfinite(atol)
        or atol < 0
        or not math.isfinite(min_cosine)
        or not 0 <= min_cosine <= 1
    ):
        raise ValueError("invalid numerical gates")
    if actual.ndim != 2 or actual.shape != expected.shape or actual.shape[1] != WIDTH:
        raise ValueError(f"comparison requires matching [batch, {WIDTH}] vectors")
    actual, expected = actual.float(), expected.float()
    if (
        not torch.isfinite(actual).all().item()
        or not torch.isfinite(expected).all().item()
    ):
        raise ValueError("nonfinite comparison vectors")
    if (
        torch.any(actual.norm(dim=1) == 0).item()
        or torch.any(expected.norm(dim=1) == 0).item()
    ):
        raise ValueError("zero comparison vector")
    cosine = torch.nn.functional.cosine_similarity(actual, expected, dim=1)
    error = (actual - expected).abs().amax(dim=1)
    unit_error = (actual.norm(dim=1) - 1).abs()
    return {
        "passed": bool(
            torch.all(error <= atol).item()
            and torch.all(cosine >= min_cosine).item()
            and torch.all(unit_error <= max(atol, 1e-5)).item()
        ),
        "max_abs_error": float(error.max()),
        "min_cosine": float(cosine.min()),
        "max_unit_norm_error": float(unit_error.max()),
        "limits": {
            "atol": atol,
            "min_cosine": min_cosine,
            "unit_norm_atol": max(atol, 1e-5),
        },
    }


def validate_native_response(
    response: dict, token_ids: list[list[int]], backend: str
) -> torch.Tensor:
    if response.get("schema") != "antfly.embedding.diagnostics.v1":
        raise ValueError("native encoder must emit tokenization/backend diagnostics")
    if response.get("backend") != backend:
        raise ValueError("native encoder did not execute the required backend")
    if backend == "cuda":
        validate_cuda_execution(response.get("cuda_execution"))
    if response.get("scope") == "offline-strict-f32-encoder-reference":
        lock = lock_model("encoder")
        evidence = response["cuda_execution"]
        low_precision = (
            "bf16_cublaslt_linear_calls",
            "bf16_cublaslt_qkv_calls",
            "f16_cublaslt_linear_calls",
            "f16_cublaslt_qkv_calls",
        )
        if (
            backend != "cuda"
            or response.get("arithmetic")
            != "BF16-source-weights-widened-F32; F32-activations"
            or response.get("model") != lock["model"]
            or response.get("revision") != lock["revision"]
            or response.get("base_model_lock_sha256")
            != hashlib.sha256(
                Path(__file__).with_name("base_models.json").read_bytes()
            ).hexdigest()
            or evidence.get("strict_f32_weights") is not True
            or type(evidence.get("resident_weights")) is not int
            or evidence["resident_weights"] <= 0
            or any(
                type(evidence.get(key)) is not int or evidence[key] != 0
                for key in (
                    "low_precision_activation_staging_calls",
                    "low_precision_matmul_calls",
                )
            )
            or any(evidence["counters"][key] != 0 for key in low_precision)
        ):
            raise ValueError(
                "invalid strict-F32 encoder provenance or precision evidence"
            )
    if response.get("token_ids") != token_ids:
        raise ValueError("native encoder token IDs differ from the reference")
    if response.get("text_prefix") != "" or response.get("trailing_eos_id") != EOS:
        raise ValueError("native encoder must use the raw Memorilla prefix/EOS recipe")
    if response.get("pooling") != "last" or response.get("normalize") is not True:
        raise ValueError(
            "native encoder must use last-token pooling and L2 normalization"
        )
    try:
        vectors = torch.tensor(response["embeddings"], dtype=torch.float32)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("invalid native encoder vectors") from error
    if vectors.shape != (len(token_ids), WIDTH):
        raise ValueError(
            f"native encoder must return full-width {WIDTH} vectors in input order"
        )
    return vectors


def validate_cuda_execution(evidence: dict | None) -> None:
    """Require forward work and reject every instrumented host fallback route."""
    if not isinstance(evidence, dict) or (
        evidence.get("scope") != "text-forward-after-asset-init"
        or evidence.get("host_output_readback_allowed") is not True
    ):
        raise ValueError("missing CUDA execution evidence for the text forward")
    for key in ("resident_weight_bytes_before", "resident_weight_bytes_after"):
        value = evidence.get(key)
        if type(value) is not int or value <= 0:
            raise ValueError("CUDA execution evidence lacks resident model weights")
    counters = evidence.get("counters")
    linear = (
        "linear_kernel_launches",
        "linear_qkv_kernel_launches",
        "bf16_cublaslt_linear_calls",
        "bf16_cublaslt_qkv_calls",
        "f16_cublaslt_linear_calls",
        "f16_cublaslt_qkv_calls",
    )
    fallback = (
        "host_attention_fallbacks",
        "paged_attention_host_fallbacks",
        "rope_host_fallbacks",
        "cross_backend_sync_fallbacks",
    )
    names = (
        "kernel_launches",
        "attention_kernel_launches",
        "d2h_bytes",
        *linear,
        *fallback,
    )
    if not isinstance(counters, dict) or any(
        type(counters.get(key)) is not int or counters[key] < 0 for key in names
    ):
        raise ValueError("CUDA execution evidence has missing or invalid counters")
    if (
        counters["kernel_launches"] == 0
        or counters["attention_kernel_launches"] == 0
        or not any(counters[key] > 0 for key in linear)
    ):
        raise ValueError(
            "CUDA execution evidence lacks GPU kernel/linear/attention work"
        )
    if any(counters[key] != 0 for key in fallback):
        raise ValueError(
            "CUDA execution evidence reports a host or synchronization fallback"
        )


def verify_memory_recipe(model_dir: Path) -> None:
    manifest_bytes = (model_dir / "model_manifest.json").read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != NATIVE_MANIFEST_SHA256:
        raise ValueError(
            "native model directory lacks the pinned Memorilla encoder recipe bytes"
        )
    manifest = json.loads(manifest_bytes)
    profile = manifest.get("embedding_profile", {})
    if (
        manifest.get("type") != "embedder"
        or manifest.get("embedding_style") != "qwen3_embedding"
        or manifest.get("pooling") != "last"
        or manifest.get("normalize") is not True
        or manifest.get("embedding_trailing_eos_id") != EOS
        or profile.get("query", {}).get("prefix") != ""
        or profile.get("document", {}).get("prefix") != ""
        or profile.get("query", {}).get("instruction_template", "") != ""
    ):
        raise ValueError(
            "native model directory lacks the explicit Memorilla encoder recipe"
        )


def _native(
    binary: Path,
    model_dir: Path,
    texts: list[str],
    expected_ids: list[list[int]],
    backend: str,
    timeout: float,
) -> tuple[torch.Tensor, dict]:
    command = [
        str(binary.resolve()),
        "embed",
        str(model_dir.resolve()),
        "--backend",
        backend,
        "--diagnostics",
    ]
    for text in texts:
        command += ["--text", text]
    result = subprocess.run(
        command, check=False, capture_output=True, text=True, timeout=timeout
    )
    if result.returncode:
        raise RuntimeError(
            f"native encoder failed ({result.returncode}): {result.stderr[-6000:]}"
        )
    response = json.loads(result.stdout)
    return validate_native_response(response, expected_ids, backend), {
        "backend": response["backend"],
        "kernel_fallback_attested": backend == "cuda",
        "cuda_execution": response.get("cuda_execution"),
        "execution_scope": response.get("scope", "embedding-cli"),
        "arithmetic": response.get("arithmetic"),
        "model": response.get("model"),
        "revision": response.get("revision"),
        "base_model_lock_sha256": response.get("base_model_lock_sha256"),
        "stderr": result.stderr[-16000:],
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
    }


def generate_reference(
    model_dir: Path,
    output: Path,
    *,
    dtype: str = "float32",
    native_binary: Path | None = None,
    native_model_dir: Path | None = None,
    backend: str = "native",
    atol: float = 1e-4,
    min_cosine: float = 0.99999,
    threads: int = 8,
    timeout: float = 600,
    prepare_native_manifest: bool = False,
) -> dict:
    if (
        dtype not in DTYPES
        or not 1 <= threads <= 128
        or backend not in ("native", "cuda")
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("invalid encoder reference execution options")
    if (
        not math.isfinite(atol)
        or atol < 0
        or not math.isfinite(min_cosine)
        or not 0 <= min_cosine <= 1
    ):
        raise ValueError("invalid numerical gates")
    if backend == "cuda" and native_binary is None:
        raise ValueError(
            "CUDA comparison requires --native-binary; the reference itself runs on CPU"
        )
    if output.exists():
        raise ValueError("reference output directory already exists")
    lock = verify_base_model(model_dir, "encoder")
    if prepare_native_manifest:
        install_native_manifest(native_model_dir or model_dir)
    from transformers import (
        AutoModel,
        AutoTokenizer,
        __version__ as transformers_version,
    )

    torch.set_num_threads(threads)
    torch.manual_seed(0)
    cases = default_cases()
    texts = [case.rendered() for case in cases]
    tokenizer = AutoTokenizer.from_pretrained(
        str(model_dir),
        local_files_only=True,
        trust_remote_code=False,
        padding_side="left",
    )
    encoded = tokenizer(texts, padding=True, truncation=False, return_tensors="pt")
    ids = active_ids(encoded)
    verify_tokenizer_cases(cases, ids)
    if max(map(len, ids)) > 256:
        raise ValueError("bounded encoder reference exceeds 256 tokens")
    model = AutoModel.from_pretrained(
        str(model_dir),
        local_files_only=True,
        trust_remote_code=False,
        torch_dtype=DTYPES[dtype],
        attn_implementation="eager",
    ).eval()
    if (
        model.config.hidden_size != WIDTH
        or model.config.num_hidden_layers != 36
        or model.config.head_dim != 128
    ):
        raise ValueError("unexpected Qwen3-Embedding-4B architecture")
    with torch.inference_mode():
        batch = normalize_features(
            model(**encoded, use_cache=False).last_hidden_state,
            encoded["attention_mask"],
        )
        singleton_rows = []
        for text, expected in zip(texts, ids, strict=True):
            single = tokenizer(
                [text], padding=True, truncation=False, return_tensors="pt"
            )
            if active_ids(single) != [expected]:
                raise ValueError("reference batch and singleton token IDs differ")
            singleton_rows.append(
                normalize_features(
                    model(**single, use_cache=False).last_hidden_state,
                    single["attention_mask"],
                )[0]
            )
        singletons = torch.stack(singleton_rows)
    stored, memory = storage_features(batch)
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "model": lock["model"],
        "revision": lock["revision"],
        "verified_artifacts": lock["files"],
        "reference": {
            "engine": "transformers-cpu",
            "torch": torch.__version__,
            "transformers": transformers_version,
            "dtype": dtype,
            "normalization_dtype": dtype,
            "checkpoint": "bfloat16",
            "attention": "eager",
            "threads": threads,
            "upstream_vllm_gpu_attested": False,
        },
        "recipe": {
            "document_prefix": "",
            "question_prefix": "",
            "question_whitespace": "strip-for-dataset-cache",
            "trailing_eos_id": EOS,
            "pooling": "last",
            "normalize": "l2",
            "dimensions": WIDTH,
            "storage_dtype": "float16",
            "memory_dtype": "bfloat16",
        },
        "cases": [
            dict(asdict(case), rendered=text, token_ids=row)
            for case, text, row in zip(cases, texts, ids, strict=True)
        ],
        "reference_batch_singleton": compare_vectors(
            batch, singletons, atol=atol, min_cosine=min_cosine
        ),
    }
    tensors = {
        "reference.normalized": batch.float().contiguous(),
        "reference.singleton": singletons.float().contiguous(),
        "stored.fp16": stored.contiguous(),
        "memory.bf16": memory.contiguous(),
    }
    if native_binary is not None:
        native_dir = native_model_dir or model_dir
        verify_base_model(native_dir, "encoder")
        verify_memory_recipe(native_dir)
        native, receipt = _native(
            native_binary, native_dir, texts, ids, backend, timeout
        )
        native_rows = []
        singleton_execution = []
        for text, row in zip(texts, ids, strict=True):
            result, singleton_receipt = _native(
                native_binary, native_dir, [text], [row], backend, timeout
            )
            native_rows.append(result[0])
            singleton_execution.append(singleton_receipt["cuda_execution"])
        native_singletons = torch.stack(native_rows)
        report["native"] = dict(
            receipt,
            singleton_cuda_execution=singleton_execution,
            manifest_sha256=NATIVE_MANIFEST_SHA256,
            reference_comparison=compare_vectors(
                native,
                batch,
                atol=atol,
                min_cosine=min_cosine,
            ),
            batch_singleton=compare_vectors(
                native,
                native_singletons,
                atol=atol,
                min_cosine=min_cosine,
            ),
        )
        tensors["native.normalized"] = native.contiguous()
        tensors["native.singleton"] = native_singletons.contiguous()
    report["passed"] = report["reference_batch_singleton"]["passed"] and (
        "native" not in report
        or (
            report["native"]["reference_comparison"]["passed"]
            and report["native"]["batch_singleton"]["passed"]
        )
    )
    output.mkdir(parents=True)
    tensor_path = output / "encoder.safetensors"
    save_file(tensors, str(tensor_path))
    report["tensor_file"] = {
        "path": tensor_path.name,
        "sha256": hashlib.sha256(tensor_path.read_bytes()).hexdigest(),
    }
    (output / "encoder.json").write_text(
        json.dumps(report, sort_keys=True, indent=2) + "\n"
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dtype", choices=sorted(DTYPES), default="float32")
    parser.add_argument("--native-binary", type=Path)
    parser.add_argument("--native-model-dir", type=Path)
    parser.add_argument("--backend", choices=("native", "cuda"), default="native")
    parser.add_argument("--atol", type=float, default=1e-4)
    parser.add_argument("--min-cosine", type=float, default=0.99999)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument(
        "--prepare-native-manifest",
        action="store_true",
        help="Explicitly install the pinned Antfly memory encoder recipe; existing different manifests are rejected",
    )
    args = vars(parser.parse_args())
    try:
        report = generate_reference(**args)
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "passed": report["passed"],
                "output": str(args["output"]),
                "native": "native" in report,
            }
        )
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
