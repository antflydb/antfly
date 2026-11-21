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

"""Independent F64 oracle for the pinned tiny Llama acceptance fixture.

Uses only Python's standard library. Compare full logits written by
bench-unified-execution --seq-len 8 --decoder-steps 4. The production runtime's
F16 KV cache and target-specific arithmetic require an explicit tolerance.
This intentionally small reference does not implement general model loading.
"""

import argparse
import array
import hashlib
import json
import math
from pathlib import Path
import struct
import sys


WEIGHT_SHA256 = "b8d35934a10afdfb74d779714390fd482aab8e5a01991b11d001f4b0bba15207"


def float32_values(payload):
    values = array.array("f")
    values.frombytes(payload)
    if sys.byteorder != "little":
        values.byteswap()
    return values


class TinyLlamaReference:
    def __init__(self, directory):
        raw = (directory / "model.safetensors").read_bytes()
        if hashlib.sha256(raw).hexdigest() != WEIGHT_SHA256:
            raise ValueError("expected pinned IlyasMoutawwakil/tiny-random-llama weights")
        header_size = struct.unpack_from("<Q", raw)[0]
        header = json.loads(raw[8 : 8 + header_size])
        self.weights = {}
        for name, metadata in header.items():
            if name == "__metadata__":
                continue
            if metadata["dtype"] != "F32":
                raise ValueError("reference requires F32 weights")
            start, end = metadata["data_offsets"]
            self.weights[name] = (
                metadata["shape"],
                float32_values(raw[8 + header_size + start : 8 + header_size + end]),
            )
        config = json.loads((directory / "config.json").read_text())
        self.width = config["hidden_size"]
        self.heads = config["num_attention_heads"]
        self.dim = self.width // self.heads
        self.layers = config["num_hidden_layers"]
        self.eps = config["rms_norm_eps"]
        self.theta = config["rope_theta"]
        self.vocab = config["vocab_size"]
        if (self.width, self.heads, self.layers, self.vocab) != (16, 4, 2, 32000):
            raise ValueError("configuration does not match the pinned tiny fixture")
        if config.get("num_key_value_heads", self.heads) != self.heads:
            raise ValueError("reference requires multi-head attention")
        self.cache = [([], []) for _ in range(self.layers)]

    def linear(self, x, name):
        (rows, cols), weights = self.weights[name]
        if cols != len(x):
            raise ValueError(f"invalid linear input for {name}")
        return [
            sum(x[k] * weights[j * cols + k] for k in range(cols))
            for j in range(rows)
        ]

    def norm(self, x, name):
        weights = self.weights[name][1]
        inv = 1 / math.sqrt(sum(v * v for v in x) / len(x) + self.eps)
        return [v * inv * scale for v, scale in zip(x, weights)]

    def rope(self, x, position):
        output = list(x)
        for head in range(self.heads):
            for j in range(self.dim // 2):
                angle = position / self.theta ** (2 * j / self.dim)
                cosine, sine = math.cos(angle), math.sin(angle)
                a = head * self.dim + j
                b = a + self.dim // 2
                output[a] = x[a] * cosine - x[b] * sine
                output[b] = x[b] * cosine + x[a] * sine
        return output

    def forward(self, token, position):
        embeddings = self.weights["model.embed_tokens.weight"][1]
        x = list(embeddings[token * self.width : (token + 1) * self.width])
        for layer in range(self.layers):
            prefix = f"model.layers.{layer}."
            normalized = self.norm(x, prefix + "input_layernorm.weight")
            query = self.rope(
                self.linear(normalized, prefix + "self_attn.q_proj.weight"), position
            )
            key = self.rope(
                self.linear(normalized, prefix + "self_attn.k_proj.weight"), position
            )
            value = self.linear(normalized, prefix + "self_attn.v_proj.weight")
            keys, values = self.cache[layer]
            keys.append(key)
            values.append(value)
            attention = [0.0] * self.width
            for head in range(self.heads):
                offset = head * self.dim
                scores = [
                    sum(query[offset + j] * k[offset + j] for j in range(self.dim))
                    / math.sqrt(self.dim)
                    for k in keys
                ]
                maximum = max(scores)
                probabilities = [math.exp(score - maximum) for score in scores]
                denominator = sum(probabilities)
                probabilities = [p / denominator for p in probabilities]
                for j in range(self.dim):
                    attention[offset + j] = sum(
                        p * v[offset + j] for p, v in zip(probabilities, values)
                    )
            projected = self.linear(attention, prefix + "self_attn.o_proj.weight")
            x = [a + b for a, b in zip(x, projected)]
            normalized = self.norm(x, prefix + "post_attention_layernorm.weight")
            gate = self.linear(normalized, prefix + "mlp.gate_proj.weight")
            up = self.linear(normalized, prefix + "mlp.up_proj.weight")
            activated = [g / (1 + math.exp(-g)) * u for g, u in zip(gate, up)]
            down = self.linear(activated, prefix + "mlp.down_proj.weight")
            x = [a + b for a, b in zip(x, down)]
        return x

    def logits(self, hidden):
        return self.linear(self.norm(hidden, "model.norm.weight"), "lm_head.weight")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", required=True, type=Path)
    parser.add_argument("--output", action="append", required=True, type=Path)
    parser.add_argument("--prefill-output", action="append", default=[], type=Path, help="Chunked-prefill final-prompt logits, checked against the same independent reference")
    parser.add_argument("--reference-output", type=Path, help="Write the independently computed full-logit F32 reference")
    parser.add_argument("--prompt-length", type=int, default=8)
    parser.add_argument("--decode-steps", type=int, default=4)
    parser.add_argument("--tolerance", type=float, required=True)
    options = parser.parse_args()
    if not 4 <= options.prompt_length <= 64 or not 1 <= options.decode_steps <= 32:
        parser.error("reference geometry exceeds this small acceptance workload")
    if not math.isfinite(options.tolerance) or options.tolerance <= 0:
        parser.error("tolerance must be finite and positive")
    model = TinyLlamaReference(options.model_dir)
    for position in range(options.prompt_length):
        hidden = model.forward(1 + position, position)
    reference = model.logits(hidden)
    for step in range(options.decode_steps):
        hidden = model.forward(11 + step, options.prompt_length + step)
        reference.extend(model.logits(hidden))
    if options.reference_output:
        options.reference_output.write_bytes(array.array("f", reference).tobytes())
    passed = True
    outputs = [(path, options.decode_steps + 1) for path in options.output]
    outputs += [(path, 1) for path in options.prefill_output]
    for path, steps in outputs:
        values = float32_values(path.read_bytes())
        if len(values) != model.vocab * steps or not all(math.isfinite(v) for v in values):
            raise ValueError(f"invalid output length or nonfinite logits: {path}")
        for step in range(steps):
            start, end = step * model.vocab, (step + 1) * model.vocab
            deltas = [abs(a - b) for a, b in zip(values[start:end], reference[start:end])]
            maximum = max(deltas)
            passed = passed and maximum <= options.tolerance
            print(json.dumps({
                "output": str(path), "step": step, "max_abs": maximum,
                "rms": math.sqrt(sum(d * d for d in deltas) / len(deltas)),
                "tolerance": options.tolerance, "passed": maximum <= options.tolerance,
            }, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
