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

"""Export parameterized full image/audio programs from official model modules."""

import argparse
import json
import os
from pathlib import Path

os.environ.setdefault("JAX_PLATFORMS", "cpu")
os.environ.setdefault("JAX_ENABLE_X64", "1")
import jax
import numpy as np
import torchax
from media_graph import load_meta, make_fn
from media_inputs import prepare, processor

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--model", type=Path, required=True)
p.add_argument("--suite", type=Path, required=True)
p.add_argument("--out", type=Path, required=True)
p.add_argument("--case", action="append")
a = p.parse_args()
a.out.mkdir(parents=True, exist_ok=True)
m, spec = load_meta(a.model)
proc = processor(a.model)
params = {
    s["name"]: jax.ShapeDtypeStruct(
        s["shape"], jax.numpy.bfloat16 if s["dtype"] == "bfloat16" else np.float32
    )
    for s in spec
}
cases = []
for c in json.loads(a.suite.read_text())["cases"]:
    if a.case and c["name"] not in a.case:
        continue
    inputs = prepare(proc, c, a.suite.parent)
    specs = [
        jax.ShapeDtypeStruct(tuple(t.shape), np.dtype(str(t.numpy().dtype)))
        for t in inputs.values()
    ]
    fn = jax.jit(torchax.interop.jax_view(make_fn(m, inputs)), keep_unused=True)
    hlo = fn.lower(params, *specs).compiler_ir("hlo").as_serialized_hlo_module_proto()
    filename = c["name"] + ".hlo"
    (a.out / filename).write_bytes(hlo)
    cases.append(
        {
            "id": c["name"],
            "program": filename,
            "inputs": [
                {"name": k, "shape": list(t.shape), "dtype": str(t.numpy().dtype)}
                for k, t in inputs.items()
            ],
        }
    )
    print("exported", c["name"], len(hlo), flush=True)
    (a.out / "model.json").write_text(
        json.dumps(
            {"family": "embeddinggemma2-media", "parameters": spec, "cases": cases},
            indent=2,
        )
    )
