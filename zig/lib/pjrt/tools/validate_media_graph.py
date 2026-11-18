# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare static export wrapper with unmodified upstream forward on CPU BF16."""

import argparse
import json
from pathlib import Path

import torch
from media_graph import load_meta, make_fn
from media_inputs import prepare, processor
from media_validation import validate_oracle
from safetensors.torch import load_file

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--model", type=Path, required=True)
p.add_argument("--suite", type=Path, required=True)
p.add_argument("--out", type=Path, required=True)
p.add_argument("--case", action="append")
p.add_argument("--dtype", choices=("bf16", "fp32"), default="bf16")
a = p.parse_args()
torch.set_num_threads(2)
m, _ = load_meta(a.model)
m.load_state_dict(load_file(a.model / "model.safetensors"), strict=True, assign=True)
if a.dtype == "fp32":
    m.to(dtype=torch.float32)
original = m.forward
proc = processor(a.model)
results = []
with torch.inference_mode():
    for c in json.loads(a.suite.read_text())["cases"]:
        if a.case and c["name"] not in a.case:
            continue
        inputs = prepare(proc, c, a.suite.parent)
        official = dict(inputs)
        if "input_features_mask" in official:
            official["input_features_mask"] = official["input_features_mask"].bool()
        hidden = original(**official).last_hidden_state.float()
        mask = inputs["attention_mask"].unsqueeze(-1).float()
        expected = torch.nn.functional.normalize(
            (hidden * mask).sum(1) / mask.sum(1), dim=-1
        )
        fn = make_fn(m, inputs)
        params = {**dict(m.named_parameters()), **dict(m.named_buffers())}
        actual = fn(params, *inputs.values())
        diff = (actual - expected).abs().max().item()
        cos = torch.nn.functional.cosine_similarity(actual, expected).item()
        row = {
            "name": c["name"],
            "max_abs": diff,
            "cosine": cos,
            "upstream_vector": expected.flatten().tolist(),
            "wrapper_vector": actual.flatten().tolist(),
        }
        # Failed/nonfinite rows must not look like completed CPU validation to
        # a later campaign. The consumer independently checks these vectors.
        validate_oracle({"cases": [row]}, [c["name"]], 1e-6)
        results.append(row)
        a.out.write_text(
            json.dumps(
                {
                    "precision": a.dtype + " weights/official FP32 boundaries",
                    "cases": results,
                },
                indent=2,
                allow_nan=False,
            )
        )
        print(c["name"], diff, cos, flush=True)
