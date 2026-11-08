#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Generate CPU PyTorch oracles for native Memorilla module qualification.

This uses PyTorch's packed MultiheadAttention implementation independently of
the native split-projection execution. Inputs and intermediate outputs stay in
SafeTensors; JSON records identities and execution policy, never tensor values.
No model, data, or source downloads are implicit.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType
from typing import Any, Iterator

import torch
from torch import nn
from safetensors.torch import save_file


UPSTREAM_REVISION = "d8f18891a50bf33c18f04fe5167c22f51e58c9cd"
UPSTREAM_MEMORY_SHA256 = (
    "09acdf9ec6b8be550bd90d01cd825365de5dde6d6e19eccf4ece436ed94626a7"
)
DTYPES = {"float32": torch.float32, "bfloat16": torch.bfloat16}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def pack_attention_weights(weights: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
    """Reassemble exported Q/K/V tensors for the independent PyTorch oracle."""
    packed = dict(weights)
    prefixes = sorted(
        name.removesuffix(".q_proj.weight")
        for name in weights
        if name.endswith(".q_proj.weight")
    )
    for prefix in prefixes:
        for kind in ("weight", "bias"):
            names = [f"{prefix}.{part}_proj.{kind}" for part in ("q", "k", "v")]
            try:
                tensors = [packed.pop(name) for name in names]
            except KeyError as error:
                raise ValueError(
                    f"incomplete split attention projection: {prefix}"
                ) from error
            packed[f"{prefix}.in_proj_{kind}"] = torch.cat(tensors, dim=0)
    return packed


@dataclass(frozen=True)
class Inputs:
    documents: torch.Tensor
    padding_mask: torch.Tensor
    question: torch.Tensor

    def tensors(self) -> dict[str, torch.Tensor]:
        return {
            "input.documents": self.documents,
            "input.padding_mask": self.padding_mask,
            "input.question": self.question,
        }


def validate_inputs(inputs: Inputs, width: int) -> None:
    docs, mask, question = inputs.documents, inputs.padding_mask, inputs.question
    if docs.ndim != 3 or docs.shape[2] != width or docs.shape[0] == 0:
        raise ValueError("documents must have shape [nonzero batch, documents, width]")
    if mask.dtype != torch.bool or mask.shape != docs.shape[:2]:
        raise ValueError(
            "padding mask must be boolean [batch, documents]; True is masked"
        )
    if question.shape != (docs.shape[0], width):
        raise ValueError("question must have shape [batch, width]")
    if docs.shape[1] == 0 or torch.any(torch.all(mask, dim=1)).item():
        raise ValueError(
            "empty collection requires closed-book fallback before readout"
        )
    if (
        docs.dtype != question.dtype
        or docs.device != question.device
        or mask.device != docs.device
    ):
        raise ValueError("inputs must share device and documents/question dtype")
    if docs.dtype not in DTYPES.values():
        raise ValueError("only F32 and BF16 oracle inputs are supported")
    if (
        not torch.isfinite(docs).all().item()
        or not torch.isfinite(question).all().item()
    ):
        raise ValueError("oracle inputs must be finite, including masked rows")


class ReferenceMemory(nn.Module):
    """Literal frozen inference architecture with traced module boundaries."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__()
        if config["num_self_attn_layers"] != 0 or config["retrieval_init"]:
            raise ValueError(
                "oracle supports the released no-self-attention learned-slot architecture"
            )
        self.config = dict(config)
        d, h = config["embedding_dim"], config["output_dim"]
        heads, blocks = config["num_heads"], config["num_cross_attn_layers"]
        dropout = config["dropout"]
        self.memory_queries = nn.Parameter(torch.empty(1, config["num_memories"], d))
        self.query_cond_attn = nn.MultiheadAttention(
            d, heads, dropout=dropout, batch_first=True
        )
        self.query_cond_norm = nn.LayerNorm(d, eps=1e-5)
        self.doc_norm = nn.LayerNorm(d, eps=1e-5)
        self.cross_attn_norms = nn.ModuleList(
            nn.LayerNorm(d, eps=1e-5) for _ in range(blocks)
        )
        self.cross_attn_layers = nn.ModuleList(
            nn.MultiheadAttention(d, heads, dropout=dropout, batch_first=True)
            for _ in range(blocks)
        )
        self.ffn_norms = nn.ModuleList(nn.LayerNorm(d, eps=1e-5) for _ in range(blocks))
        self.ffn_layers = nn.ModuleList(
            nn.Sequential(
                nn.Linear(d, 4 * d),
                nn.GELU(approximate="none"),
                nn.Dropout(dropout),
                nn.Linear(4 * d, d),
                nn.Dropout(dropout),
            )
            for _ in range(blocks)
        )
        self.layer_norm = nn.LayerNorm(d, eps=1e-5)
        self.memory_projection = nn.Linear(d, h)

    def trace(self, inputs: Inputs) -> dict[str, torch.Tensor]:
        if self.training:
            raise ValueError(
                "reference fixtures require eval mode; dropout must be disabled"
            )
        validate_inputs(inputs, self.config["embedding_dim"])
        trace: dict[str, torch.Tensor] = {}

        def record(name: str, value: torch.Tensor) -> torch.Tensor:
            trace[name] = value.detach().cpu().contiguous().clone()
            return value

        slots = record(
            "slots.initial",
            self.memory_queries.expand(inputs.documents.shape[0], -1, -1),
        )
        question = inputs.question.unsqueeze(1)
        conditioned, _ = self.query_cond_attn(slots, question, question)
        record("question.attended", conditioned)
        slots = record("slots.conditioned", self.query_cond_norm(slots + conditioned))
        docs = record("documents.normalized", self.doc_norm(inputs.documents))
        for index, (norm, attention, ffn_norm, ffn) in enumerate(
            zip(
                self.cross_attn_norms,
                self.cross_attn_layers,
                self.ffn_norms,
                self.ffn_layers,
                strict=True,
            )
        ):
            prefix = f"block{index}"
            query = record(f"{prefix}.cross.normalized", norm(slots))
            attended, _ = attention(
                query, docs, docs, key_padding_mask=inputs.padding_mask
            )
            record(f"{prefix}.cross.attended", attended)
            slots = record(f"{prefix}.slots.after_attention", slots + attended)
            hidden = record(f"{prefix}.ffn.normalized", ffn_norm(slots))
            hidden = record(f"{prefix}.ffn.hidden", ffn[0](hidden))
            hidden = record(f"{prefix}.ffn.activated", ffn[1](hidden))
            hidden = record(f"{prefix}.ffn.output", ffn[3](hidden))
            slots = record(f"{prefix}.slots.output", slots + hidden)
        normalized = record("slots.final_normalized", self.layer_norm(slots))
        record("memory_tokens", self.memory_projection(normalized))
        return trace


def fixture_inputs(width: int, dtype: torch.dtype, seed: int) -> dict[str, Inputs]:
    """Use private RNG and upstream FP16 storage -> BF16 input casts.

    The F32 diagnostic widens those same BF16-valued features. Comparing its
    outputs with BF16 therefore isolates arithmetic rather than changing inputs.
    """
    generator = torch.Generator(device="cpu").manual_seed(seed)

    def vectors(*shape: int) -> torch.Tensor:
        values = torch.randn(shape, generator=generator, dtype=torch.float32)
        values = torch.nn.functional.normalize(values, p=2, dim=-1)
        return values.to(torch.float16).to(torch.bfloat16).to(dtype)

    docs = vectors(2, 5, width)
    question = vectors(2, width)
    mask = torch.tensor([[False] * 5, [False, False, True, True, True]])
    ragged = Inputs(docs, mask, question)
    changed = docs.clone()
    changed[mask] = (
        (vectors(int(mask.sum().item()), width).float() * 19)
        .to(torch.float16)
        .to(torch.bfloat16)
        .to(dtype)
    )
    order = torch.tensor([4, 1, 3, 0, 2])
    return {
        "single_document": Inputs(
            docs[:1, :1].clone(), mask[:1, :1].clone(), question[:1].clone()
        ),
        "ragged": ragged,
        "masked_padding_changed": Inputs(changed, mask.clone(), question.clone()),
        "document_permutation": Inputs(
            docs[:, order].clone(), mask[:, order].clone(), question.clone()
        ),
        "duplicate_document": Inputs(
            torch.cat((docs[:1], docs[:1, :1]), dim=1),
            torch.zeros((1, 6), dtype=torch.bool),
            question[:1].clone(),
        ),
        "question_changed": Inputs(docs.clone(), mask.clone(), vectors(2, width)),
    }


@contextmanager
def deterministic_cpu() -> Iterator[None]:
    old_threads = torch.get_num_threads()
    old_deterministic = torch.are_deterministic_algorithms_enabled()
    old_fastpath = torch.backends.mha.get_fastpath_enabled()
    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    torch.backends.mha.set_fastpath_enabled(False)
    try:
        yield
    finally:
        torch.backends.mha.set_fastpath_enabled(old_fastpath)
        torch.use_deterministic_algorithms(old_deterministic)
        torch.set_num_threads(old_threads)


def load_upstream(
    source_root: Path, config: dict[str, Any], packed: dict[str, torch.Tensor]
) -> nn.Module:
    """Import the hash-pinned MemoryModule without optional training dependencies.

    The utility shim covers only the source file's checkpoint-I/O imports; its
    constructor and forward execute unchanged. No checkpoint utility is invoked.
    """
    source = source_root / "memorilla" / "memory.py"
    source_bytes = source.read_bytes()
    if hashlib.sha256(source_bytes).hexdigest() != UPSTREAM_MEMORY_SHA256:
        raise ValueError("upstream memory.py differs from the pinned source")
    name = "_antfly_memorilla_pinned_reference"
    utility_name = "memorilla.utils"
    utility = ModuleType(utility_name)
    utility.CHECKPOINT_FILE = "memory.pt"
    utility.CONFIG_FILE = "config.json"

    def not_used(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("checkpoint I/O is disabled in the source reference loader")

    utility.checkpoint_file = not_used
    utility.load_memory_state = not_used
    previous_utility = sys.modules.get(utility_name)
    previous_module = sys.modules.get(name)
    spec = importlib.util.spec_from_file_location(name, source)
    if spec is None or spec.loader is None:
        raise ValueError("cannot import pinned source")
    module = importlib.util.module_from_spec(spec)
    sys.modules[utility_name] = utility
    sys.modules[name] = module
    try:
        # Execute the verified snapshot, rather than trusting a timestamp-based
        # bytecode cache or rereading source after its digest check.
        exec(compile(source_bytes, str(source), "exec"), module.__dict__)
        with torch.device("meta"):
            result = module.MemoryModule(**config)
        result.load_state_dict(packed, strict=True, assign=True)
        return result.eval()
    finally:
        for key, previous in (
            (utility_name, previous_utility),
            (name, previous_module),
        ):
            if previous is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = previous


def generate_fixtures(
    bundle_dir: Path,
    output_dir: Path,
    *,
    dtype: str = "float32",
    seed: int = 23,
    upstream_source: Path | None = None,
) -> dict[str, Any]:
    if dtype not in DTYPES:
        raise ValueError(f"unsupported oracle dtype: {dtype}")
    if output_dir.exists():
        raise ValueError("fixture output directory must not already exist")
    # Import locally so --help and the oracle have no implicit source downloads.
    from exporter import load_bundle

    config, split_weights, bundle_manifest = load_bundle(bundle_dir)
    packed = pack_attention_weights(split_weights)
    with torch.device("meta"):
        model = ReferenceMemory(config)
    model.load_state_dict(packed, strict=True, assign=True)
    model = model.eval().to(dtype=DTYPES[dtype], device="cpu")
    source_model = None
    if upstream_source is not None:
        source_model = load_upstream(upstream_source, config, packed).to(
            dtype=DTYPES[dtype], device="cpu"
        )
    tensors: dict[str, torch.Tensor] = {}
    checks: dict[str, Any] = {}
    cases = fixture_inputs(config["embedding_dim"], DTYPES[dtype], seed)
    with deterministic_cpu(), torch.inference_mode():
        for case_name, inputs in cases.items():
            values = {**inputs.tensors(), **model.trace(inputs)}
            for name, value in values.items():
                if value.dtype != torch.bool and not torch.isfinite(value).all().item():
                    raise ValueError(f"nonfinite fixture: {case_name}.{name}")
                tensors[f"{case_name}.{name}"] = value.contiguous()
            if source_model is not None:
                actual = source_model(
                    inputs.documents, inputs.padding_mask, inputs.question
                )
                torch.testing.assert_close(
                    actual, values["memory_tokens"], rtol=0, atol=0
                )
        base = tensors["ragged.memory_tokens"]
        for case in ("masked_padding_changed", "document_permutation"):
            candidate = tensors[f"{case}.memory_tokens"]
            tolerance = 1e-5 if dtype == "float32" else 0.03125
            torch.testing.assert_close(base, candidate, rtol=tolerance, atol=tolerance)
            checks[case] = {
                "max_absolute_difference": float(
                    (base.float() - candidate.float()).abs().max()
                )
            }
        for inputs in (
            Inputs(
                torch.empty(1, 0, config["embedding_dim"], dtype=DTYPES[dtype]),
                torch.empty(1, 0, dtype=torch.bool),
                cases["single_document"].question,
            ),
            Inputs(
                cases["single_document"].documents,
                torch.ones(1, 1, dtype=torch.bool),
                cases["single_document"].question,
            ),
        ):
            try:
                model.trace(inputs)
            except ValueError as error:
                if "closed-book fallback" not in str(error):
                    raise
            else:
                raise AssertionError("empty collection reached attention")
    receipt = {
        "schema_version": 1,
        "kind": "memorilla_module_reference",
        "native_parity_verified": False,
        "release_bundle": bundle_manifest["release"],
        "fixture_inputs": "synthetic normalized vectors; no text encoder executed",
        "bundle_manifest_sha256": sha256(bundle_dir / "manifest.json"),
        "bundle_config_sha256": sha256(bundle_dir / "config.json"),
        "bundle_weights_sha256": sha256(bundle_dir / "memory.safetensors"),
        "bundle_manifest_identity": _canonical_hash(bundle_manifest),
        "upstream_revision": UPSTREAM_REVISION,
        "upstream_memory_sha256": UPSTREAM_MEMORY_SHA256,
        "upstream_forward_compared": source_model is not None,
        "oracle_sha256": sha256(Path(__file__)),
        "torch_version": torch.__version__,
        "execution": {
            "device": "cpu",
            "dtype": dtype,
            "threads": 1,
            "deterministic": True,
            "mha_fastpath": False,
            "dropout": False,
            "layer_norm_epsilon": 1e-5,
            "gelu": "erf",
            "input_cast": "F32 normalized -> FP16 stored -> BF16 module input -> requested arithmetic dtype",
        },
        "seed": seed,
        "cases": list(cases),
        "empty_collection": {
            "action": "closed_book_fallback",
            "attention_executed": False,
        },
        "invariance_checks": checks,
        "decoder": {
            "status": "not_run",
            "reason": "module oracle; exact decoder qualification is separate",
        },
    }
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{output_dir.name}-", dir=output_dir.parent
    ) as temporary:
        staging = Path(temporary) / "fixtures"
        staging.mkdir()
        tensor_path = staging / "reference.safetensors"
        save_file(tensors, tensor_path)
        receipt["tensors"] = {
            "path": tensor_path.name,
            "sha256": sha256(tensor_path),
            "inventory": {
                name: {"shape": list(t.shape), "dtype": str(t.dtype)}
                for name, t in sorted(tensors.items())
            },
        }
        (staging / "reference.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n"
        )
        staging.rename(output_dir)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bundle", type=Path, required=True, help="strict exported Memorilla bundle"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="new fixture destination"
    )
    parser.add_argument("--dtype", choices=sorted(DTYPES), default="float32")
    parser.add_argument("--seed", type=int, default=23)
    parser.add_argument(
        "--upstream-source", type=Path, help="optional local pinned Memorilla checkout"
    )
    args = parser.parse_args()
    receipt = generate_fixtures(
        args.bundle,
        args.output,
        dtype=args.dtype,
        seed=args.seed,
        upstream_source=args.upstream_source,
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "cases": receipt["cases"],
                "upstream_forward_compared": receipt["upstream_forward_compared"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
