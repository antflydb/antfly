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

"""Token-KL sensitivity and plan evaluation for decoder LMs from Antfly GGUFs.

The decoder counterpart of torch_embedding_sensitivity.py. Each candidate
format is exported once, uniformly, with `antfly inference export`; the
tensors Antfly wrote are dequantized with gguf-py, mapped to the Hugging Face
parameters by value, and swapped into a PyTorch reference model. The loss is
the mean next-token KL(dense || quantized) over packed corpus windows, so the
quantized values are exactly Antfly's and only the forward pass is PyTorch's.

    uv run torch_lm_sensitivity.py --model MODEL --corpus corpus.txt \\
        --weight-sensitivity weight.json \\
        --uniform q4_k=q4_k.gguf,q5_k=q5_k.gguf,q6_k=q6_k.gguf,q8_0=q8_0.gguf \\
        measure --output kl.json
    uv run torch_lm_sensitivity.py ... evaluate --offset 4 \\
        --plan portable.json --plan metal.json --plan untied.json

`measure` keeps the file's fused-kernel `groups`, so `antfly inference
quantize-plan` still ties them. Use a CUDA or MPS device (`--device`) for
models above a few hundred million parameters; on CPU, limit `--tensors`.
"""

import argparse
import json
import math
import re
import sys
from pathlib import Path

import numpy as np
import torch
from gguf import GGUFReader
from gguf.quants import dequantize
from transformers import AutoModelForCausalLM, AutoTokenizer

torch.set_grad_enabled(False)


class QuantizedTensors:
    """Antfly-exported tensors per format, dequantized on demand from the
    memory-mapped GGUFs (holding every format dense would need one model
    copy per format)."""

    def __init__(self, paths):
        self.tensors = {}
        for fmt, path in paths.items():
            self.tensors[fmt] = {t.name: t for t in GGUFReader(path).tensors}

    def formats(self):
        return list(self.tensors)

    def get(self, fmt, name):
        t = self.tensors[fmt][name]
        arr = dequantize(t.data, t.tensor_type)
        shape = tuple(int(x) for x in reversed(t.shape.tolist()))
        return torch.from_numpy(np.array(arr, dtype=np.float32, copy=True)).reshape(
            shape
        )


def map_names(names, quantized, fmt, params):
    """GGUF name -> HF parameter name, by nearest values among same-shape
    parameters (robust to any naming scheme, rejects permuted layouts)."""
    mapping = {}
    for name in names:
        w = quantized.get(fmt, name)
        best, best_err = None, math.inf
        for pname, p in params.items():
            if tuple(p.shape) != tuple(w.shape):
                continue
            pf = p.detach().float()
            err = torch.mean((pf - w) ** 2).item() / (torch.mean(pf**2).item() + 1e-30)
            if err < best_err:
                best, best_err = pname, err
        if best is None or best_err > 0.05:
            raise SystemExit(
                f"no HF parameter matches {name} (best {best}, relative error {best_err:.3g})"
            )
        mapping[name] = best
    if len(set(mapping.values())) != len(mapping):
        raise SystemExit("ambiguous tensor mapping")
    return mapping


def windows(tokenizer, corpus, count, length, offset):
    ids = tokenizer(
        Path(corpus).read_text(encoding="utf-8"), return_tensors="pt"
    ).input_ids[0]
    need = (offset + count) * length
    if ids.numel() < need:
        raise SystemExit(f"corpus has {ids.numel()} tokens; {need} needed")
    return ids[offset * length : need].reshape(count, length)


class Scorer:
    def __init__(self, model, batch, device):
        self.model, self.batch, self.device = model, batch, device
        self.reference = None

    def logprobs(self):
        out = []
        for chunk in self.batch:
            logits = self.model(input_ids=chunk.to(self.device)).logits[:, :-1].float()
            out.append(torch.log_softmax(logits, dim=-1).cpu())
        return out

    def capture_reference(self):
        self.reference = [lp.half() for lp in self.logprobs()]

    def score(self):
        kl_sum, agree, count = 0.0, 0, 0
        for ref16, q in zip(self.reference, self.logprobs()):
            ref = ref16.float()
            kl_sum += (ref.exp() * (ref - q)).sum(-1).sum().item()
            agree += (ref.argmax(-1) == q.argmax(-1)).sum().item()
            count += ref.shape[0] * ref.shape[1]
        return max(kl_sum / count, 0.0), agree / count


def measured_groups(groups, tensors):
    names = {t["name"] for t in tensors}
    return [g for g in groups if all(m in names for m in g["members"])]


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--model",
        required=True,
        help="dense Hugging Face model directory (the reference)",
    )
    ap.add_argument(
        "--corpus", required=True, help="UTF-8 text; packed into token windows"
    )
    ap.add_argument(
        "--weight-sensitivity",
        required=True,
        help="sensitivity file from antfly export --quantize-sensitivity",
    )
    ap.add_argument(
        "--uniform",
        required=True,
        help="fmt=path.gguf,... uniform antfly exports, one per candidate format",
    )
    ap.add_argument("--windows", type=int, default=4)
    ap.add_argument("--window-length", type=int, default=256)
    ap.add_argument("--batch-size", type=int, default=2)
    ap.add_argument("--device", default="cpu", help="cpu, cuda or mps")
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("measure")
    m.add_argument(
        "--offset",
        type=int,
        default=0,
        help="first window index (keep measure and evaluate disjoint)",
    )
    m.add_argument("--tensors", help="regex: only measure matching tensors")
    m.add_argument("--output", required=True)
    e = sub.add_parser("evaluate")
    e.add_argument("--offset", type=int, default=4)
    e.add_argument("--plan", action="append", default=[])
    args = ap.parse_args()

    weight = json.loads(Path(args.weight_sensitivity).read_text())
    names = [t["name"] for t in weight["tensors"]]
    quantized = QuantizedTensors(
        dict(kv.split("=", 1) for kv in args.uniform.split(","))
    )
    formats = quantized.formats()

    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model = (
        AutoModelForCausalLM.from_pretrained(args.model, dtype=torch.float32)
        .to(args.device)
        .eval()
    )
    params = dict(model.named_parameters())
    finest = "q8_0" if "q8_0" in formats else formats[-1]
    mapping = map_names(names, quantized, finest, params)
    dense = {n: params[mapping[n]].detach().clone() for n in names}
    ids = windows(tokenizer, args.corpus, args.windows, args.window_length, args.offset)
    scorer = Scorer(model, list(ids.split(args.batch_size)), args.device)
    scorer.capture_reference()

    def assign(choice):
        for n in names:
            fmt = choice.get(n, "none")
            src = dense[n] if fmt == "none" else quantized.get(fmt, n)
            params[mapping[n]].data.copy_(src.to(params[mapping[n]].device))

    if args.cmd == "measure":
        pattern = re.compile(args.tensors) if args.tensors else None
        out = []
        for t in weight["tensors"]:
            if pattern and not pattern.search(t["name"]):
                continue
            cands = []
            for c in t["candidates"]:
                if c["format"] not in formats:
                    continue
                assign({t["name"]: c["format"]})
                kl, _ = scorer.score()
                cands.append({**c, "loss": kl})
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
                    "metric": "token_kl",
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
    elements = sum(t["elements"] for t in weight["tensors"])
    rows = []
    variants = [(f"uniform {fmt}", {n: fmt for n in names}) for fmt in formats]
    variants += [
        (f"plan {Path(p).name}", json.loads(Path(p).read_text())["tensors"])
        for p in args.plan
    ]
    for label, choice in variants:
        assign(choice)
        kl, agree = scorer.score()
        planned = sum(
            bytes_of[(n, choice[n])] for n in names if choice.get(n, "none") != "none"
        )
        rows.append(
            {
                "variant": label,
                "planned_bytes": planned,
                "bits_per_weight": round(planned * 8 / elements, 4),
                "token_kl": kl,
                "top1_agreement": agree,
            }
        )
        print(json.dumps(rows[-1]), file=sys.stderr, flush=True)
    print(json.dumps({"tokens": int(ids.numel()), "results": rows}, indent=2))


if __name__ == "__main__":
    main()
