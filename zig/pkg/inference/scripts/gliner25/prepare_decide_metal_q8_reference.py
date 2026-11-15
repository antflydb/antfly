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

"""Decode the exact Decide Q8 bundle for independent Fastino parity captures.

The resulting FP32 checkpoint represents the quantized weights, not the original
FP32 model. Use it for correctness only; time the original Fastino checkpoint in
separate fresh processes so reference construction cannot affect memory peaks.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil

from benchmark_decide_quant import decoded_weights


PINS = {
    "gliner2-encoder.Q8_0.gguf": "8d9bea7df21ab4a8a17333f51f246faab5280159e5751ebbadd81a85ec96c871",
    "gliner2-head.Q8_0.gguf": "52cb6197d3dcfe5ad6276ceb037a3333d2921bbcf3dfb8289aeff82fbd5693dc",
}


def sha(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    from safetensors import safe_open
    from safetensors.torch import save_file

    for name, expected in PINS.items():
        if sha(args.bundle / name) != expected:
            raise ValueError(f"Unexpected Q8 artifact: {name}")
    source = args.source / "model.safetensors"
    if (
        sha(source)
        != "40a5a23ff860dc3dff426cecd1048cacdd29c648c96db209dad818e9686dc997"
    ):
        raise ValueError("Unexpected original FP32 checkpoint")
    weights = {}
    for name in PINS:
        for key, tensor in decoded_weights(args.bundle / name, device="cpu"):
            # The GGUF exporter removes the outer encoder wrapper from its
            # transformer stack, while keeping embedding/head names intact.
            if key.startswith(
                (
                    "embeddings.",
                    "encoder.layer.",
                    "encoder.rel_embeddings.",
                    "encoder.LayerNorm.",
                )
            ):
                key = "encoder." + key
            if key in weights:
                raise ValueError(f"Duplicate tensor {key}")
            weights[key] = tensor
    with safe_open(source, framework="pt", device="cpu") as original:
        if set(weights) != set(original.keys()):
            difference = sorted(set(weights) ^ set(original.keys()))
            raise ValueError(
                f"Tensor inventory differs ({len(difference)}): {difference[:12]}"
            )
        for key, tensor in weights.items():
            if list(tensor.shape) != original.get_slice(key).get_shape():
                raise ValueError(f"Tensor shape differs: {key}")
    args.output.mkdir(parents=True, exist_ok=False)
    for name in (
        "config.json",
        "encoder_config/config.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "special_tokens_map.json",
    ):
        target = args.output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.bundle / name, target)
    target = args.output / "model.safetensors"
    save_file(weights, target, metadata={"format": "pt"})
    receipt = dict(
        purpose="Independent decoded-Q8 correctness reference; never the FP32 timing baseline",
        source_bundle=PINS,
        tensor_count=len(weights),
        sha256=sha(target),
    )
    (args.output / "q8_reference.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
