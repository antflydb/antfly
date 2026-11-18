# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Export parameterized EmbeddingGemma2 text HLO without loading GPU weights."""

import argparse, json, os
from pathlib import Path

os.environ.setdefault("JAX_PLATFORMS", "cpu")
os.environ.setdefault("JAX_ENABLE_X64", "1")
import jax, numpy as np, torchax
from tokenizers import Tokenizer
from embedding_graph import load_meta, make_fn

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--model-dir", required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--lengths", type=int, nargs="+", default=[128, 512, 2048, 8192])
args = parser.parse_args()
out = args.out
out.mkdir(parents=True, exist_ok=False)
m, spec = load_meta(args.model_dir)
tok = Tokenizer.from_file(str(Path(args.model_dir) / "tokenizer.json"))
cases = []
for length in args.lengths:
    n = length - 8
    for _ in range(8):
        text = ("river " * n).strip()
        ids = tok.encode("title: none | text: " + text).ids
        if len(ids) == length:
            break
        n += length - len(ids)
    assert len(ids) == length, (len(ids), length)
    cases.append(
        {
            "id": f"text_{length}",
            "text": text,
            "prompt_ids": ids,
            "max_tokens": 1,
            "prefill": f"text_{length}.hlo",
        }
    )
params = {
    p["name"]: jax.ShapeDtypeStruct(
        p["shape"], jax.numpy.bfloat16 if p["dtype"] == "bfloat16" else np.float32
    )
    for p in spec
}
fn = jax.jit(torchax.interop.jax_view(make_fn(m)), keep_unused=True)
for c in cases:
    n = len(c["prompt_ids"])
    h = fn.lower(
        params,
        jax.ShapeDtypeStruct((1, n), np.int64),
        jax.ShapeDtypeStruct((1, n), np.int64),
    ).compiler_ir("hlo")
    (out / c["prefill"]).write_bytes(h.as_serialized_hlo_module_proto())
    print("exported", c["id"], len(h.program_shape().parameter_shapes()), flush=True)
(out / "model.json").write_text(
    json.dumps(
        {
            "version": 1,
            "family": "embeddinggemma2",
            "parameters": spec,
            "cases": cases,
            "repeats": 8,
            "warmups": 2,
        },
        indent=2,
    )
)
