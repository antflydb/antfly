# /// script
# requires-python = ">=3.10"
# dependencies = ["torch", "transformers", "gguf", "numpy"]
# ///
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

"""Fast output-space sensitivity for Hugging Face encoder embedders.

The Antfly-runtime harness (embedding_sensitivity.py) re-exports and re-embeds
once per tensor and format. This one exports each candidate format once,
uniformly, with `antfly inference export`, dequantizes the tensors Antfly wrote
with gguf-py, maps each to its Hugging Face parameter by value, and swaps one
tensor at a time inside a PyTorch reference model. The quantized values are
exactly Antfly's; only the forward pass is PyTorch's.

    uv run torch_embedding_sensitivity.py --model MODEL --corpus corpus.txt \
        --weight-sensitivity weight.json \
        --uniform q4_k=q4_k.gguf,q5_k=q5_k.gguf,q8_0=q8_0.gguf \
        measure --texts 64 --output cosine.json
    uv run torch_embedding_sensitivity.py ... evaluate --start 64 --texts 300 \
        --plan plan.json --mixed-gguf plan.json=model.mixed.gguf

`measure` and `evaluate` should use disjoint corpus lines (`--start`) so a plan
is judged on text it was not calibrated on. `--mixed-gguf` checks that an
exported plan's tensors are byte-for-byte the planned formats' tensors.
"""

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import torch
from gguf import GGUFReader
from gguf.quants import dequantize
from transformers import AutoModel, AutoTokenizer

torch.set_grad_enabled(False)


def load_texts(path, start, count):
    lines = [l.strip() for l in Path(path).read_text().splitlines() if l.strip()]
    return lines[start : start + count]


class Embedder:
    def __init__(self, model_dir, texts, pooling, max_length, batch=32):
        self.pooling = pooling
        self.tok = AutoTokenizer.from_pretrained(model_dir)
        self.model = AutoModel.from_pretrained(model_dir, dtype=torch.float32).eval()
        self.params = dict(self.model.named_parameters())
        self.batches = [
            self.tok(
                texts[i : i + batch],
                padding=True,
                truncation=True,
                max_length=max_length,
                return_tensors="pt",
            )
            for i in range(0, len(texts), batch)
        ]

    def embed(self):
        out = []
        for enc in self.batches:
            hidden = self.model(**enc).last_hidden_state
            if self.pooling == "cls":
                h = hidden[:, 0]
            else:
                mask = enc["attention_mask"].unsqueeze(-1).to(hidden.dtype)
                h = (hidden * mask).sum(1) / mask.sum(1)
            out.append(torch.nn.functional.normalize(h, dim=-1))
        return torch.cat(out)


def gguf_tensors(path):
    reader = GGUFReader(path)
    result = {}
    for t in reader.tensors:
        arr = dequantize(t.data, t.tensor_type)
        shape = tuple(int(x) for x in reversed(t.shape.tolist()))
        result[t.name] = (
            t.tensor_type.name,
            torch.from_numpy(np.array(arr, dtype=np.float32, copy=True)).reshape(shape),
        )
    return result


def map_names(planned_names, deq, params):
    """GGUF name -> HF param name, by nearest values among same-shape params."""
    mapping = {}
    for name in planned_names:
        _, w = deq[name]
        best, best_err = None, math.inf
        for pname, p in params.items():
            if tuple(p.shape) != tuple(w.shape):
                continue
            err = torch.mean((p - w) ** 2).item() / (torch.mean(p**2).item() + 1e-30)
            if err < best_err:
                best, best_err = pname, err
        if best is None or best_err > 0.05:
            raise SystemExit(
                f"no HF parameter matches {name} (best {best} rel err {best_err})"
            )
        mapping[name] = best
    if len(set(mapping.values())) != len(mapping):
        raise SystemExit("ambiguous tensor mapping")
    return mapping


def cos_loss(ref, emb):
    return float((1 - (ref * emb).sum(-1)).mean())


def recall(ref, emb, k=10):
    def topk(x):
        s = x @ x.T
        s.fill_diagonal_(-2)
        return s.topk(k, dim=-1).indices

    a, b = topk(ref), topk(emb)
    return float(
        sum(len(set(a[i].tolist()) & set(b[i].tolist())) / k for i in range(len(ref)))
        / len(ref)
    )


def measured_groups(groups: list[dict], tensors: list[dict]) -> list[dict]:
    """Fused-kernel groups whose members were all measured (others are dropped,
    so a filtered run still allocates; its unmeasured tensors stay at the base
    format)."""
    names = {t["name"] for t in tensors}
    return [g for g in groups if all(m in names for m in g["members"])]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--weight-sensitivity", required=True)
    ap.add_argument(
        "--uniform", required=True, help="fmt=path.gguf,... uniform antfly exports"
    )
    ap.add_argument(
        "--pooling",
        choices=("cls", "mean"),
        default="cls",
        help="cls for BGE; mean for most sentence-transformers",
    )
    ap.add_argument("--max-length", type=int, default=256)
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("measure")
    m.add_argument("--texts", type=int, default=96)
    m.add_argument("--output", required=True)
    e = sub.add_parser("evaluate")
    e.add_argument("--start", type=int, default=96)
    e.add_argument("--texts", type=int, default=300)
    e.add_argument("--plan", action="append", default=[])
    e.add_argument("--mixed-gguf", action="append", default=[])
    args = ap.parse_args()

    weight = json.loads(Path(args.weight_sensitivity).read_text())
    names = [t["name"] for t in weight["tensors"]]
    uniform = dict(kv.split("=", 1) for kv in args.uniform.split(","))
    deq = {fmt: gguf_tensors(path) for fmt, path in uniform.items()}
    texts = load_texts(
        args.corpus, 0 if args.cmd == "measure" else args.start, args.texts
    )
    em = Embedder(args.model, texts, args.pooling, args.max_length)
    any_fmt = next(iter(deq))
    mapping = map_names(names, deq[any_fmt], em.params)
    dense = {n: em.params[mapping[n]].detach().clone() for n in names}
    ref = em.embed()

    def assign(choice):
        for n in names:
            src = dense[n] if choice.get(n, "none") == "none" else deq[choice[n]][n][1]
            em.params[mapping[n]].data.copy_(src)

    if args.cmd == "measure":
        out = []
        for t in weight["tensors"]:
            cands = []
            for c in t["candidates"]:
                if c["format"] not in deq:
                    continue
                assign({t["name"]: c["format"]})
                cands.append({**c, "loss": max(cos_loss(ref, em.embed()), 0.0)})
            assign({})
            out.append(
                {"name": t["name"], "elements": t["elements"], "candidates": cands}
            )
            print(
                t["name"],
                " ".join(f"{c['format']}={c['loss']:.2e}" for c in cands),
                file=sys.stderr,
                flush=True,
            )
        Path(args.output).write_text(
            json.dumps(
                {
                    "schema": "antfly-quant-sensitivity/v1",
                    "metric": "embedding_cosine_loss",
                    "tensors": out,
                    "groups": measured_groups(weight.get("groups", []), out),
                },
                indent=2,
            )
        )
        return

    bytes_of = {
        (t["name"], c["format"]): c["bytes"]
        for t in weight["tensors"]
        for c in t["candidates"]
    }
    rows = []
    for fmt in deq:
        assign({n: fmt for n in names})
        e2 = em.embed()
        rows.append(
            {
                "variant": f"uniform {fmt}",
                "planned_bytes": sum(bytes_of[(n, fmt)] for n in names),
                "cosine_loss": cos_loss(ref, e2),
                "recall@10": recall(ref, e2),
            }
        )
    for plan_path in args.plan:
        plan = json.loads(Path(plan_path).read_text())["tensors"]
        assign(plan)
        e2 = em.embed()
        rows.append(
            {
                "variant": f"plan {Path(plan_path).name}",
                "planned_bytes": sum(bytes_of[(n, plan[n])] for n in names),
                "cosine_loss": cos_loss(ref, e2),
                "recall@10": recall(ref, e2),
            }
        )
    # Check the antfly mixed exports carry exactly the planned tensors.
    for spec in args.mixed_gguf:
        plan_path, gguf_path = spec.split("=", 1)
        plan = json.loads(Path(plan_path).read_text())["tensors"]
        mixed = gguf_tensors(gguf_path)
        mismatched = [
            n for n in names if not torch.equal(mixed[n][1], deq[plan[n]][n][1])
        ]
        types = {n: mixed[n][0] for n in names}
        print(
            f"{gguf_path}: {len(names) - len(mismatched)}/{len(names)} planned tensors identical to the uniform export of their planned format; "
            f"types {sorted(set(types.values()))}",
            file=sys.stderr,
        )
        if mismatched:
            raise SystemExit(f"mismatched tensors: {mismatched[:5]}")
    print(json.dumps({"texts": len(texts), "results": rows}, indent=2))


if __name__ == "__main__":
    main()
