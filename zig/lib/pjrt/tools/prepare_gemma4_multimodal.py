# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Export official Gemma E4B text/media + cached decode to native-PJRT artifacts."""

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path

os.environ.setdefault("JAX_PLATFORMS", "cpu")
os.environ.setdefault("JAX_ENABLE_X64", "1")
import jax
import numpy as np
import torch
import torchax
from gemma4_graph import (
    load_multimodal_meta,
    make_multimodal_step,
    media_metadata,
    state_shapes,
)
from gemma4_inputs import prepare, processor
from gemma4_weights import parameter_specs


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for part in iter(lambda: f.read(1 << 24), b""):
            h.update(part)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "processor", "suite", "gguf", "mmproj", "out"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--only", action="append", default=[])
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=False)
    config = json.loads(a.config.read_text())
    m = load_multimodal_meta(config)
    specs = parameter_specs(m, a.gguf, a.mmproj)
    shapes = {
        s["name"]: jax.ShapeDtypeStruct(
            s["shape"], jax.numpy.bfloat16 if s["dtype"] == "bfloat16" else np.float32
        )
        for s in specs
    }
    suite = json.loads(a.suite.read_text())
    proc = processor(a.processor)
    manifest = {
        "version": 2,
        "family": "gemma4",
        "precision": "bf16",
        "parameters": specs,
        "cases": [],
        "context_limit": suite["context_tokens"],
        "scope": "CPU official processor and native Zig/PJRT generation; weights from identical QAT GGUF files; BF16 dense weights and KV; excludes HTTP and cold load/compile",
        "parameter_bytes": sum(
            math.prod(s["shape"]) * (2 if s["dtype"] == "bfloat16" else 4)
            for s in specs
        ),
        "preparation": {
            "versions": {
                name: importlib.metadata.version(name)
                for name in (
                    "torch",
                    "transformers",
                    "torchax",
                    "jax",
                    "jaxlib",
                    "numpy",
                    "gguf",
                    "av",
                )
            },
            "config_sha256": digest(a.config),
            "gguf_sha256": digest(a.gguf),
            "mmproj_sha256": digest(a.mmproj),
            "source_sha256": {
                f.name: digest(f)
                for f in (Path(__file__), *Path(__file__).parent.glob("gemma4_*.py"))
            },
            "representation": "GGUF audio scales are already softplus; audio norm names inverted; vision patch convolution reordered to NHWC; QAT clipping retained",
        },
    }
    for case in suite["cases"]:
        if a.only and case["id"] not in a.only:
            continue
        values, metadata = prepare(proc, case, a.suite.parent)
        meta = media_metadata(m, values)
        if len(values["input_ids"][0]) != case["expected_prompt_tokens"]:
            raise ValueError(("prepared tokens changed", case["id"]))
        capacity = ((len(values["input_ids"][0]) + case["max_tokens"] + 15) // 16) * 16
        if capacity > suite["context_tokens"]:
            raise ValueError("Context exceeded")
        states = state_shapes(m.config.text_config, capacity)
        state = tuple(
            jax.ShapeDtypeStruct(shape, jax.numpy.bfloat16) for shape in states
        )
        inputs = tuple(
            jax.ShapeDtypeStruct(
                values[k].shape,
                np.int64 if values[k].dtype == torch.int64 else np.float32,
            )
            for k in meta["keys"]
        )
        entry = {
            **case,
            "inputs": [
                {
                    "name": key,
                    "shape": list(values[key].shape),
                    "dtype": "int64" if values[key].dtype == torch.int64 else "float32",
                }
                for key in meta["keys"]
            ],
            "state_shapes": states,
            "cache_capacity": capacity,
            "processor_metadata": metadata,
        }
        for phase in ("prefill", "decode", "logits"):
            fn = make_multimodal_step(
                m,
                capacity,
                meta=meta,
                prefill=phase != "decode",
                logits=phase == "logits",
            )
            lowered = jax.jit(torchax.interop.jax_view(fn), keep_unused=True).lower(
                shapes,
                state,
                inputs
                if phase != "decode"
                else (jax.ShapeDtypeStruct((1, 1), np.int64),),
                jax.ShapeDtypeStruct((), np.int64),
            )
            hlo = lowered.compiler_ir(dialect="hlo")
            name = f"{case['id']}-{phase}.hlo"
            (a.out / name).write_bytes(hlo.as_serialized_hlo_module_proto())
            entry[phase] = name
            print(
                "exported",
                name,
                "parameters",
                len(hlo.program_shape().parameter_shapes()),
                flush=True,
            )
        manifest["cases"].append(entry)
        (a.out / "model.json").write_text(json.dumps(manifest, indent=2) + "\n")
    if not manifest["cases"]:
        raise ValueError("No selected cases")
    print(
        "complete",
        len(manifest["cases"]),
        "cases",
        manifest["parameter_bytes"],
        "resident parameter bytes",
        flush=True,
    )


if __name__ == "__main__":
    main()
