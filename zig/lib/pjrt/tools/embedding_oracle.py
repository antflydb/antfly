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

"""Add BF16 Torchax/JAX vector oracles to an existing native artifact manifest.

Run separately from native timing. This command updates the manifest in place.
"""

import argparse
import json
from pathlib import Path
import numpy as np
import torch
from safetensors import safe_open
from embedding_graph import load_meta, make_fn
from torchax_backend import Backend

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--model-dir", type=Path, required=True)
p.add_argument("--manifest", type=Path, required=True)
args = p.parse_args()
backend = Backend()
model, specs = load_meta(args.model_dir)
params = {}
with safe_open(args.model_dir / "model.safetensors", framework="pt", device="cpu") as f:
    for spec in specs:
        tensor = (
            f.get_tensor(spec["source"])
            if "source" in spec
            else torch.tensor(spec["data"], dtype=torch.float32).reshape(spec["shape"])
        )
        params[spec["name"]] = backend.upload(tensor)
fn = backend.ax.interop.jax_jit(make_fn(model))
manifest = json.loads(args.manifest.read_text())
for case in manifest["cases"]:
    with backend.context(), torch.no_grad():
        ids = backend.upload(torch.tensor([case["prompt_ids"]]))
        mask = backend.upload(
            torch.ones((1, len(case["prompt_ids"])), dtype=torch.int64)
        )
        out = fn(params, ids, mask)
        backend.sync(out)
    vector = backend.cpu(out).reshape(-1).tolist()
    if not np.isfinite(vector).all() or abs(np.linalg.norm(vector) - 1) > 1e-5:
        raise ValueError("Invalid reference vector")
    case["oracle_vector"] = vector
    print(case["id"], "oracle norm", np.linalg.norm(vector), flush=True)
args.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
