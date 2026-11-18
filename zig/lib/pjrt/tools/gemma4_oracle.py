# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Export teacher-forced logit HLO and independent Torchax/JAX BF16 oracles.

Writes validation.json and a steps*vocabulary*4 byte logit file beside model.json.
This GPU preparation process is separate from native timing.
"""

import argparse, json
from pathlib import Path
import numpy as np, torch
from torchax_backend import Backend
from gemma4_graph import make_step, cache_state
from gguf_reference import load

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--weights", type=Path, required=True)
p.add_argument("--config", type=Path, required=True)
p.add_argument("--manifest", type=Path, required=True)
p.add_argument("--steps", type=int, default=216)
args = p.parse_args()
if args.steps < 1:
    p.error("--steps must be positive")
b = Backend()
torch.set_num_threads(4)
m, params, info = load(args.weights, args.config, b)
manifest = json.loads(args.manifest.read_text())
c = manifest["cases"][0]
cap = c["state_shapes"][0][2]
raw = make_step(m, cap)


def logits_step(params, state, ids, pos):
    logits, state, pos = raw(params, state, ids, pos)
    return logits.float(), state, pos


jfn = b.jax.jit(b.ax.interop.jax_view(logits_step), keep_unused=True)
fn = b.ax.interop.torch_view(jfn)
jparams = b.env.t2j_iso(params)
state_shapes = tuple(
    b.jax.ShapeDtypeStruct(s, b.jax.numpy.bfloat16) for s in c["state_shapes"]
)
for phase, q in [("prefill", len(c["prompt_ids"])), ("decode", 1)]:
    hlo = jfn.lower(
        jparams,
        state_shapes,
        b.jax.ShapeDtypeStruct((1, q), np.int64),
        b.jax.ShapeDtypeStruct((), np.int64),
    ).compiler_ir("hlo")
    (args.manifest.parent / (phase + "-logits.hlo")).write_bytes(
        hlo.as_serialized_hlo_module_proto()
    )
with b.context():
    state = cache_state(m.config, cap, b.device)
    ids = b.upload(torch.tensor([c["prompt_ids"]]))
    pos = b.upload(torch.tensor(0))
teacher = []
with (args.manifest.parent / "oracle-logits.f32").open("wb") as f:
    for i in range(args.steps):
        with b.context(), torch.no_grad():
            out = fn(params, state, ids, pos)
            b.sync(out[0])
        logits = b.cpu(out[0]).float()
        f.write(logits.numpy().tobytes())
        next_id = int(logits.argmax(-1).item())
        teacher.append(next_id)
        with b.context():
            ids = b.upload(torch.tensor([[next_id]]))
        state, pos = out[1:]
        if i % 32 == 0:
            print("reference step", i, "argmax", next_id, flush=True)
c.update(
    id="short-teacher-forced",
    prefill="prefill-logits.hlo",
    decode="decode-logits.hlo",
    max_tokens=args.steps,
    teacher_ids=teacher[:-1],
    oracle_logits="oracle-logits.f32",
    oracle_tokens=[],
)
manifest.update(family="gemma4-validation", cases=[c], repeats=1, warmups=0)
(args.manifest.parent / "validation.json").write_text(json.dumps(manifest))
print("reference_complete", flush=True)
