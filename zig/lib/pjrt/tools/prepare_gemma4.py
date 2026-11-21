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

"""Export cached Gemma 4 BF16 HLO with dynamic parameter, token and cache inputs."""

import argparse, json, os
from pathlib import Path

os.environ.setdefault("JAX_PLATFORMS", "cpu")
os.environ.setdefault("JAX_ENABLE_X64", "1")
import torch, numpy as np, jax, torchax
from transformers import Gemma4TextConfig
from transformers.models.gemma4.modeling_gemma4 import Gemma4TextRotaryEmbedding
from transformers.modeling_gguf_pytorch_utils import (
    get_gguf_hf_weights_map,
    TensorProcessor,
)
from gemma4_graph import configure_model, make_step, patch_casts

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--config", type=Path, required=True)
parser.add_argument("--cases", type=Path, required=True)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
out = args.out
out.mkdir(parents=True, exist_ok=False)
cfg = Gemma4TextConfig(**json.loads(args.config.read_text())["text_config"])
patch_casts()
m = configure_model(cfg)
m.model.rotary_emb = Gemma4TextRotaryEmbedding(cfg, device="cpu")
for mod in m.modules():
    if hasattr(mod, "scalar_embed_scale"):
        mod.embed_scale = torch.tensor(mod.scalar_embed_scale, dtype=torch.float32)
params = {**dict(m.named_parameters()), **dict(m.named_buffers())}
mapping = get_gguf_hf_weights_map(m, TensorProcessor())
reverse = {v: k for k, v in mapping.items()}
expected = set(m.state_dict())
specs = []
shapes = {}
for name, t in sorted(params.items()):
    spec = {
        "name": name,
        "shape": list(t.shape),
        "dtype": "bfloat16" if name in expected else "float32",
    }
    if name in expected:
        source = reverse[name]
        if source.endswith(".layer_output_scale"):
            source += ".weight"
        spec["source"] = source
    else:
        assert not t.is_meta, (name, "meta")
        spec["data"] = t.reshape(-1).tolist()
    specs.append(spec)
    shapes[name] = jax.ShapeDtypeStruct(
        t.shape, jax.numpy.bfloat16 if name in expected else np.float32
    )
manifest = {
    "version": 1,
    "family": "gemma4",
    "parameters": specs,
    "cases": [],
    "repeats": 4,
    "warmups": 1,
}
cases = json.loads(args.cases.read_text())
for c in cases:
    p = {"ids": c["prompt_ids"]}
    cap = ((len(p["ids"]) + c["max_tokens"] + 15) // 16) * 16
    state_shapes = []
    for i in range(cfg.num_hidden_layers - cfg.num_kv_shared_layers):
        dim = cfg.per_layer_config[i].head_dim
        state_shapes.extend([[1, cfg.num_key_value_heads, cap, dim]] * 2)
    state = tuple(jax.ShapeDtypeStruct(s, jax.numpy.bfloat16) for s in state_shapes)
    raw = make_step(m, cap)

    def generate(params, state, ids, pos):
        logits, state, pos = raw(params, state, ids, pos)
        return logits.argmax(-1).unsqueeze(1), state, pos

    fn = jax.jit(torchax.interop.jax_view(generate), keep_unused=True)
    paths = []
    for phase, length in [("prefill", len(p["ids"])), ("decode", 1)]:
        lowered = fn.lower(
            shapes,
            state,
            jax.ShapeDtypeStruct((1, length), np.int64),
            jax.ShapeDtypeStruct((), np.int64),
        )
        hlo = lowered.compiler_ir(dialect="hlo")
        filename = f"{c['id']}-{phase}.hlo"
        (out / filename).write_bytes(hlo.as_serialized_hlo_module_proto())
        paths.append(filename)
        print(
            "exported",
            filename,
            "parameters",
            len(hlo.program_shape().parameter_shapes()),
            flush=True,
        )
    oracle = c.get("oracle_tokens", [])
    manifest["cases"].append(
        {
            "id": c["id"],
            "prompt_ids": p["ids"],
            "max_tokens": c["max_tokens"],
            "prefill": paths[0],
            "decode": paths[1],
            "state_shapes": state_shapes,
            "oracle_tokens": oracle,
        }
    )
(out / "model.json").write_text(json.dumps(manifest, indent=2))
