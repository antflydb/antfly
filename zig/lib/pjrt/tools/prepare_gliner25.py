#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Export GLiNER2.5 encoder/classifier HLO; preparation is outside native timing."""

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path

os.environ.setdefault("JAX_PLATFORMS", "cpu")
os.environ.setdefault("JAX_ENABLE_X64", "1")
os.environ.setdefault("HF_HUB_OFFLINE", "1")

import numpy as np
import torch
from gliner2 import AutoExtractor, Schema
from safetensors import safe_open

TASKS = [
    {
        "task": "intent",
        "labels": [
            "maintenance",
            "room_change",
            "checkout",
            "billing",
            "complaint",
            "information",
        ],
    },
    {
        "task": "topics",
        "labels": ["hvac", "billing", "noise", "cleanliness", "wifi", "staff"],
        "multi_label": True,
        "cls_threshold": 0.4,
    },
    {"task": "urgent", "labels": ["yes", "no"]},
]


class Core(torch.nn.Module):
    def __init__(self, model):
        super().__init__()
        # Retain the complete checkpoint inventory in both native arms, including
        # unused extraction heads. keep_unused=True preserves its argument slots.
        self.model = model

    def forward(self, ids, mask, positions, valid):
        x = self.model.encoder(input_ids=ids, attention_mask=mask).last_hidden_state
        x = x.gather(1, positions.unsqueeze(-1).expand(-1, -1, x.shape[-1]))
        logits = self.model.classifier(x).squeeze(-1).float()
        return torch.where(valid.bool(), logits, -1e4)


class EntityCore(Core):
    def forward(self, ids, mask, text_idx, text_mask, query_idx, query_mask):
        x = self.model.encoder(input_ids=ids, attention_mask=mask).last_hidden_state

        def gather(indices, valid):
            return x.gather(
                1, indices.unsqueeze(-1).expand(-1, -1, x.shape[-1])
            ) * valid.unsqueeze(-1).to(x.dtype)

        y = self.model.boundary_head(
            gather(text_idx, text_mask),
            text_mask.bool(),
            gather(query_idx, query_mask),
            query_mask.bool(),
            collect_diagnostics=False,
        )
        return torch.cat(
            [
                y.start_logits.flatten(),
                y.end_logits.flatten(),
                y.inside_logits.flatten(),
                y.null_logits.flatten(),
                y.count_log_rates.flatten(),
                y.candidates.pair_logits.flatten(),
                y.candidates.indices.float().flatten(),
                y.candidates.valid_mask.float().flatten(),
            ]
        ).float()


def digest(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def preparation_provenance(model):
    import gliner2

    root = Path(gliner2.__file__).parent
    source = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        source.update(
            str(path.relative_to(root)).encode() + b"\0" + path.read_bytes() + b"\0"
        )
    return {
        "versions": {
            name: importlib.metadata.version(name)
            for name in (
                "gliner2",
                "torch",
                "transformers",
                "torchax",
                "jax",
                "jaxlib",
                "numpy",
            )
        },
        "gliner2_source_sha256": source.hexdigest(),
        "jax_matmul_precision": "highest",
        "model_sidecars": {
            str(path.relative_to(model)): digest(path)
            for path in (
                model / "config.json",
                model / "encoder_config/config.json",
                model / "tokenizer.json",
            )
        },
    }


def main():
    import jax
    import torchax

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--model", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--precision", choices=("fp32", "bf16"), default="fp32")
    p.add_argument(
        "--kind", choices=("classification", "entities"), default="classification"
    )
    p.add_argument("--lengths", type=int, nargs="+", default=[128, 512])
    p.add_argument("--batches", type=int, nargs="+", default=[1, 8])
    a = p.parse_args()
    if any(n < 96 or n > 512 for n in a.lengths) or any(
        n not in (1, 8, 32) for n in a.batches
    ):
        p.error("Use 96..512 tokens and batches 1/8/32")
    a.out.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(4)
    jax.config.update("jax_default_matmul_precision", "highest")
    model = AutoExtractor.from_pretrained(str(a.model), local_files_only=True).eval()
    model.requires_grad_(False)
    if a.precision == "bf16":
        model.to(torch.bfloat16)
    if a.kind == "entities" and model.architecture != "boundary":
        p.error("The extraction-head comparison requires a boundary checkpoint")
    core = (EntityCore(model) if a.kind == "entities" else Core(model)).eval()
    parameters = {**dict(core.named_parameters()), **dict(core.named_buffers())}
    specs = []
    with safe_open(a.model / "model.safetensors", framework="pt", device="cpu") as f:
        names = set(f.keys())
        for name, tensor in sorted(parameters.items()):
            dtype = {
                torch.float32: "float32",
                torch.bfloat16: "bfloat16",
                torch.int64: "int64",
            }[tensor.dtype]
            spec = {"name": name, "shape": list(tensor.shape), "dtype": dtype}
            source = name.removeprefix("model.")
            if source in names:
                assert list(f.get_slice(source).get_shape()) == list(tensor.shape)
                spec["source"] = source
            else:
                spec["data"] = tensor.reshape(-1).tolist()
            specs.append(spec)
    shapes = {
        name: jax.ShapeDtypeStruct(
            t.shape,
            {
                torch.float32: np.float32,
                torch.bfloat16: jax.numpy.bfloat16,
                torch.int64: np.int64,
            }[t.dtype],
        )
        for name, t in sorted(parameters.items())
    }

    def forward(params, *inputs):
        return torch.func.functional_call(core, params, inputs)

    fn = jax.jit(torchax.interop.jax_view(forward), keep_unused=True)
    schema = Schema.from_dict(
        {"entities": ["person", "location", "organization", "species", "date"]}
        if a.kind == "entities"
        else {"classifications": TASKS}
    ).build()
    marker_id = model.processor.tokenizer.convert_tokens_to_ids("[L]")
    cases = []
    for length in a.lengths:
        count = length - 80
        for _ in range(12):
            prefix = (
                "John Smith joined Apple in London on Monday to study wolves. "
                if a.kind == "entities"
                else "Guest in room 512 says the AC is broken and wants to move tonight. "
            )
            text = prefix + " ".join(["river"] * count)
            batch = model.processor.collate_fn_inference(
                [(text, schema)],
                max_len=512,
                architecture=model.architecture,
                error_policy="raise",
                build_targets=False,
            )
            actual = batch.input_ids.shape[1]
            if actual == length:
                break
            count += length - actual
            if count < 0:
                raise ValueError("Schema exceeds requested length")
        if actual != length:
            raise ValueError("Unable to construct exact-length input")
        positions = torch.where(batch.input_ids[0] == marker_id)[0][None, :]
        if a.kind == "classification" and positions.numel() != 14:
            raise ValueError("Classification schema did not encode fourteen labels")
        for size in a.batches:
            if a.kind == "entities":
                tensors = [
                    value.long().repeat(size, 1)
                    for value in (
                        batch.input_ids,
                        batch.attention_mask,
                        batch.text_word_indices,
                        batch.text_word_mask,
                        batch.query_marker_indices,
                        batch.query_marker_mask,
                    )
                ]
                names = (
                    "input_ids",
                    "attention_mask",
                    "text_word_indices",
                    "text_word_mask",
                    "query_marker_indices",
                    "query_marker_mask",
                )
                words, queries = tensors[2].shape[1], tensors[4].shape[1]
                capacity = model.boundary_head.settings.candidate_budget
                output_shape = [
                    size * queries * (2 * (words + 1) + words + 2 + capacity * 4)
                ]
            else:
                tensors = [
                    batch.input_ids.repeat(size, 1),
                    batch.attention_mask.repeat(size, 1),
                    positions.repeat(size, 1),
                    torch.ones(size, positions.shape[1], dtype=torch.int64),
                ]
                names = (
                    "input_ids",
                    "attention_mask",
                    "decision_marker_positions",
                    "decision_marker_mask",
                )
                output_shape = [size, 14]
            case_id = f"{a.kind}_s{length}_b{size}"
            inputs = [
                {
                    "name": name,
                    "shape": list(value.shape),
                    "dtype": "int64",
                    "data": value.reshape(-1).tolist(),
                }
                for name, value in zip(names, tensors, strict=True)
            ]
            hlo = fn.lower(
                shapes, *(jax.ShapeDtypeStruct(t.shape, np.int64) for t in tensors)
            ).compiler_ir("hlo")
            program = case_id + ".hlo"
            (a.out / program).write_bytes(hlo.as_serialized_hlo_module_proto())
            case = {
                "id": case_id,
                "kind": a.kind,
                "text": text,
                "program": program,
                "inputs": inputs,
                "outputs": [
                    {"name": "logits", "shape": output_shape, "dtype": "float32"}
                ],
            }
            if a.kind == "entities":
                case["candidate_indices_offset"] = (
                    size * queries * (2 * (words + 1) + words + 2 + capacity)
                )
            if size == 1 and length == min(a.lengths):
                with torch.inference_mode():
                    case["cpu_logits"] = core(*tensors).reshape(-1).tolist()
            cases.append(case)
            print("exported", case_id, flush=True)
    manifest = {
        "family": "gliner25",
        "scope": "prepared encoder and "
        + (
            "boundary extraction head"
            if a.kind == "entities"
            else "label-marker classifier"
        )
        + "; excludes tokenization and final decoding",
        "precision": a.precision,
        "parameters": specs,
        "cases": cases,
        "tasks": TASKS,
        "model_sha256": digest(a.model / "model.safetensors"),
        "architecture": model.architecture,
        "parameter_bytes": sum(
            int(np.prod(p["shape"]))
            * (2 if p["dtype"] == "bfloat16" else 8 if p["dtype"] == "int64" else 4)
            for p in specs
        ),
    }
    (a.out / "model.json").write_text(
        json.dumps(
            {**manifest, "preparation": preparation_provenance(a.model)},
            indent=2,
            allow_nan=False,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
