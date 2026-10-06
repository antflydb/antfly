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

"""What does training the top of the Antenna trunk for embeddings cost extraction?

For each (k, anchor): fine-tune the top k trunk layers (and the final norm) plus
a linear head on mean-pooled states with symmetric in-batch InfoNCE over SQuAD
(question, paragraph) pairs, optionally anchored by the z-space MSE between the
updated and the original trunk's final states on paragraph tokens. Then score
SQuAD validation retrieval and write a GLiNER checkpoint (the student's neck and
heads, the updated trunk) for the baseline extraction evaluation.

    ANTFLY_ANTENNA_DATA=<cache> python embedding_unfreeze_probe.py <antenna student dir> <out dir> \\
        4:0 4:1 8:0 8:1

Each <k>:<anchor> setting writes <out dir>/topk<k>-anchor<anchor>/ (a GLiNER
checkpoint for baselines.py) and appends retrieval to <out dir>/topk.json.
"""

import json
import os
import random
import shutil
import sys
import time

from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import torch  # noqa: E402
from safetensors.torch import load_file, save_file  # noqa: E402
from transformers import AutoTokenizer  # noqa: E402

import embedding_probe as probe  # noqa: E402

dev = "mps"
BATCH = 16
MAX_LEN = 256


def mean_pool(hidden, mask):
    m = mask.unsqueeze(-1).float()
    return (hidden * m).sum(1) / m.sum(1)


def run(
    student_dir, out_dir, k, anchor, train_pairs, train_ctx, eval_q, eval_ctx, gold
):
    torch.manual_seed(11)
    random.seed(11)
    model = probe.encoder_from_student(student_dir).to(dev)
    reference = (
        probe.encoder_from_student(student_dir).to(dev).eval() if anchor else None
    )
    if reference is not None:
        for p in reference.parameters():
            p.requires_grad_(False)
    layers = model.config.num_hidden_layers
    trainable = []
    for name, p in model.named_parameters():
        top = name.startswith("final_norm") or any(
            name.startswith(f"layers.{i}.") for i in range(layers - k, layers)
        )
        p.requires_grad_(top)
        if top:
            trainable.append(p)
    head = torch.nn.Linear(768, 768).to(dev)
    opt = torch.optim.AdamW(
        [{"params": trainable, "lr": 2e-5}, {"params": head.parameters(), "lr": 1e-3}],
        weight_decay=0.01,
    )
    tok = AutoTokenizer.from_pretrained(student_dir)
    model.train()
    order = list(range(len(train_pairs)))
    random.shuffle(order)
    t0 = time.time()
    steps = len(order) // BATCH
    for step in range(steps):
        batch = [train_pairs[i] for i in order[step * BATCH : (step + 1) * BATCH]]
        q = tok(
            [x for x, _ in batch],
            padding=True,
            truncation=True,
            max_length=MAX_LEN,
            return_tensors="pt",
        ).to(dev)
        c = tok(
            [train_ctx[ci] for _, ci in batch],
            padding=True,
            truncation=True,
            max_length=MAX_LEN,
            return_tensors="pt",
        ).to(dev)
        qh = model(**q).last_hidden_state
        ch = model(**c).last_hidden_state
        qv = torch.nn.functional.normalize(
            head(mean_pool(qh, q["attention_mask"])), dim=-1
        )
        cv = torch.nn.functional.normalize(
            head(mean_pool(ch, c["attention_mask"])), dim=-1
        )
        ids = torch.tensor([ci for _, ci in batch], device=dev)
        same = (ids.unsqueeze(0) == ids.unsqueeze(1)) & ~torch.eye(
            len(batch), dtype=torch.bool, device=dev
        )
        logits = (qv @ cv.T / 0.05).masked_fill(same, -1e4)
        labels = torch.arange(len(batch), device=dev)
        loss = (
            torch.nn.functional.cross_entropy(logits, labels)
            + torch.nn.functional.cross_entropy(logits.T, labels)
        ) / 2
        if reference is not None:
            with torch.no_grad():
                rh = reference(**c).last_hidden_state
            mask = c["attention_mask"].bool()
            r, s = rh[mask], ch[mask]
            loss = (
                loss
                + anchor * (((s - r) / (r.std(0, keepdim=True) + 1e-4)) ** 2).mean()
            )
        opt.zero_grad()
        loss.backward()
        opt.step()
        if step % 200 == 0:
            print(
                json.dumps(
                    {
                        "k": k,
                        "anchor": anchor,
                        "step": step,
                        "of": steps,
                        "loss": round(float(loss), 4),
                        "sec": round(time.time() - t0),
                    }
                ),
                flush=True,
            )
    model.eval()
    head.eval()
    eq = probe.embed(model, tok, eval_q, "mean")
    ec = probe.embed(model, tok, eval_ctx, "mean")
    with torch.no_grad():
        result = probe.retrieval(head(eq.to(dev)).cpu(), head(ec.to(dev)).cpu(), gold)
    # GLiNER checkpoint with the updated trunk.
    target = f"{out_dir}/topk{k}-anchor{anchor:g}"
    if os.path.exists(target):
        shutil.rmtree(target)
    shutil.copytree(student_dir, target)
    weights = load_file(student_dir + "/model.safetensors")
    for name, value in model.state_dict().items():
        key = "encoder." + name
        if key in weights:
            weights[key] = value.detach().cpu().contiguous()
    save_file(weights, target + "/model.safetensors")
    torch.save(head.state_dict(), target + "/embedding_head.pt")
    return result, target


if __name__ == "__main__":
    student_dir, out_dir = sys.argv[1], sys.argv[2]
    settings = [(int(x.split(":")[0]), float(x.split(":")[1])) for x in sys.argv[3:]]
    train_contexts, train_pairs = probe.squad("train")
    random.seed(7)
    random.shuffle(train_pairs)
    train_pairs = train_pairs[: int(os.environ.get("TRAIN_PAIRS", "20000"))]
    used = sorted({c for _, c in train_pairs})
    remap = {c: i for i, c in enumerate(used)}
    train_ctx = [train_contexts[c] for c in used]
    train_pairs = [(q, remap[c]) for q, c in train_pairs]
    eval_ctx, eval_pairs = probe.squad("validation")
    eval_q = [q for q, _ in eval_pairs]
    gold = [c for _, c in eval_pairs]
    report = {}
    for k, anchor in settings:
        result, target = run(
            student_dir,
            out_dir,
            k,
            anchor,
            train_pairs,
            train_ctx,
            eval_q,
            eval_ctx,
            gold,
        )
        report[f"top{k} anchor{anchor:g}"] = {"retrieval": result, "checkpoint": target}
        print(json.dumps({f"top{k} anchor{anchor:g}": result}), flush=True)
        json.dump(report, open(out_dir + "/topk.json", "w"), indent=1)
