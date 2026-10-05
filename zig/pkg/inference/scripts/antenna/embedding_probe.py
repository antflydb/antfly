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

"""Can a head on the frozen Antenna trunk embed? A retrieval probe.

Features: the frozen trunk's mean-pooled final states. Head: linear or MLP,
trained on cached features with symmetric in-batch InfoNCE over SQuAD
(question, paragraph) pairs and/or cosine distillation toward
granite-embedding-english-r2 (CLS, L2-normalized). Eval: SQuAD validation
questions retrieving their paragraph among all validation paragraphs.

    ANTFLY_ANTENNA_DATA=<cache> python embedding_probe.py <antenna student dir> \\
        <granite-embedding-english-r2 dir> <ModernBERT-base dir> <out.json>

SQuAD (CC BY-SA 4.0) is fetched at a pinned revision through antenna_datasets.
Runs on MPS.
"""

import json
import os
import random
import sys
import time

from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import antenna_datasets as datasets  # noqa: E402
import torch  # noqa: E402
from safetensors.torch import load_file  # noqa: E402
from transformers import AutoModel, AutoTokenizer, ModernBertConfig, ModernBertModel  # noqa: E402

dev = "mps"
torch.manual_seed(7)
random.seed(7)
TRAIN_PAIRS = int(os.environ.get("TRAIN_PAIRS", "30000"))
MAX_LEN = 256
SQUAD = "https://huggingface.co/datasets/rajpurkar/squad/resolve/7b6d24c440a36b6815f21b70d25016731768db1f/plain_text/{}-00000-of-00001.parquet"


def squad(split):
    import io

    import pyarrow.parquet as pq

    rows = pq.read_table(io.BytesIO(datasets._fetch(SQUAD.format(split)))).to_pylist()
    contexts, index, pairs = [], {}, []
    for row in rows:
        c = row["context"].strip()
        if c not in index:
            index[c] = len(contexts)
            contexts.append(c)
        pairs.append((row["question"].strip(), index[c]))
    return contexts, pairs


def encoder_from_student(path):
    weights = {
        k[len("encoder.") :]: v
        for k, v in load_file(path + "/model.safetensors").items()
        if k.startswith("encoder.")
    }
    config = ModernBertConfig.from_pretrained(path + "/encoder_config")
    config.vocab_size = weights["embeddings.tok_embeddings.weight"].shape[0]
    model = ModernBertModel(config)
    missing, unexpected = model.load_state_dict(weights, strict=False)
    assert not unexpected and all("rotary" in m or "inv_freq" in m for m in missing), (
        missing,
        unexpected,
    )
    return model


@torch.inference_mode()
def embed(model, tokenizer, texts, pooling, batch=64):
    model.eval().to(dev)
    out = []
    order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
    for start in range(0, len(texts), batch):
        idx = order[start : start + batch]
        enc = tokenizer(
            [texts[i] for i in idx],
            padding=True,
            truncation=True,
            max_length=MAX_LEN,
            return_tensors="pt",
        ).to(dev)
        hidden = model(**enc).last_hidden_state.float()
        if pooling == "cls":
            pooled = hidden[:, 0]
        else:
            mask = enc["attention_mask"].unsqueeze(-1).float()
            pooled = (hidden * mask).sum(1) / mask.sum(1)
        out.append((idx, pooled.cpu()))
    result = torch.empty(len(texts), out[0][1].shape[1])
    for idx, pooled in out:
        result[idx] = pooled
    return result


def retrieval(q, c, gold):
    q = torch.nn.functional.normalize(q, dim=-1)
    c = torch.nn.functional.normalize(c, dim=-1)
    scores = q @ c.T
    target = scores[torch.arange(len(gold)), torch.tensor(gold)]
    rank = (scores > target.unsqueeze(1)).sum(1) + 1
    r = rank.float()
    return {
        "r@1": round(float((r <= 1).float().mean()), 4),
        "r@10": round(float((r <= 10).float().mean()), 4),
        "mrr@10": round(
            float(torch.where(r <= 10, 1 / r, torch.zeros_like(r)).mean()), 4
        ),
        "ndcg@10": round(
            float(
                torch.where(r <= 10, 1 / torch.log2(r + 1), torch.zeros_like(r)).mean()
            ),
            4,
        ),
    }


def train_head(sq, sc, gq, gc, pairs, kind, infonce, distill, epochs=6):
    dim = sq.shape[1]
    head = (
        torch.nn.Linear(dim, 768)
        if kind == "linear"
        else torch.nn.Sequential(
            torch.nn.Linear(dim, 2048), torch.nn.GELU(), torch.nn.Linear(2048, 768)
        )
    )
    head.to(dev)
    opt = torch.optim.AdamW(head.parameters(), lr=1e-3, weight_decay=0.01)
    q_idx = torch.arange(len(pairs))
    c_idx = torch.tensor([c for _, c in pairs])
    sq, sc, gq, gc = sq.to(dev), sc.to(dev), gq.to(dev), gc.to(dev)
    gq_n, gc_n = (
        torch.nn.functional.normalize(gq, dim=-1),
        torch.nn.functional.normalize(gc, dim=-1),
    )
    batch = 256
    for _ in range(epochs):
        perm = torch.randperm(len(pairs))
        for start in range(0, len(pairs), batch):
            b = perm[start : start + batch]
            qi, ci = q_idx[b], c_idx[b]
            qv = torch.nn.functional.normalize(head(sq[qi]), dim=-1)
            cv = torch.nn.functional.normalize(head(sc[ci]), dim=-1)
            loss = torch.zeros((), device=dev)
            if infonce:
                logits = qv @ cv.T / 0.05
                # Duplicate paragraphs in a batch are positives too.
                same = (ci.unsqueeze(0) == ci.unsqueeze(1)).to(dev)
                logits_q = logits.masked_fill(
                    same & ~torch.eye(len(b), dtype=torch.bool, device=dev), -1e4
                )
                labels = torch.arange(len(b), device=dev)
                loss = (
                    loss
                    + (
                        torch.nn.functional.cross_entropy(logits_q, labels)
                        + torch.nn.functional.cross_entropy(logits_q.T, labels)
                    )
                    / 2
                )
            if distill:
                loss = (
                    loss
                    + (1 - (qv * gq_n[qi]).sum(-1)).mean()
                    + (1 - (cv * gc_n[ci]).sum(-1)).mean()
                )
            opt.zero_grad()
            loss.backward()
            opt.step()
    head.eval()
    return lambda x: head(x.to(dev)).detach().cpu()


if __name__ == "__main__":
    student_dir, granite_dir, base_dir, output = sys.argv[1:5]
    t0 = time.time()
    train_contexts, train_pairs = squad("train")
    random.shuffle(train_pairs)
    train_pairs = train_pairs[:TRAIN_PAIRS]
    used = sorted({c for _, c in train_pairs})
    remap = {c: i for i, c in enumerate(used)}
    train_ctx = [train_contexts[c] for c in used]
    train_pairs = [(q, remap[c]) for q, c in train_pairs]
    eval_contexts, eval_pairs = squad("validation")
    print(
        json.dumps(
            {
                "train_pairs": len(train_pairs),
                "train_contexts": len(train_ctx),
                "eval_questions": len(eval_pairs),
                "eval_contexts": len(eval_contexts),
            }
        ),
        flush=True,
    )
    train_q = [q for q, _ in train_pairs]
    eval_q = [q for q, _ in eval_pairs]
    gold = [c for _, c in eval_pairs]

    report = {
        "provenance": {
            "student": student_dir,
            "granite": granite_dir,
            "base": base_dir,
        },
        "results": {},
    }
    granite = AutoModel.from_pretrained(granite_dir, torch_dtype=torch.float32)
    gtok = AutoTokenizer.from_pretrained(granite_dir)
    G = {
        name: embed(granite, gtok, texts, "cls")
        for name, texts in (
            ("tq", train_q),
            ("tc", train_ctx),
            ("eq", eval_q),
            ("ec", eval_contexts),
        )
    }
    report["results"]["granite-embedding-english-r2 (teacher)"] = retrieval(
        G["eq"], G["ec"], gold
    )
    print(
        json.dumps(
            {
                "granite": report["results"]["granite-embedding-english-r2 (teacher)"],
                "sec": round(time.time() - t0),
            }
        ),
        flush=True,
    )
    del granite

    base = AutoModel.from_pretrained(base_dir, torch_dtype=torch.float32)
    btok = AutoTokenizer.from_pretrained(base_dir)
    report["results"]["ModernBERT-base, mean pool, untrained"] = retrieval(
        embed(base, btok, eval_q, "mean"),
        embed(base, btok, eval_contexts, "mean"),
        gold,
    )
    del base

    student = encoder_from_student(student_dir)
    stok = AutoTokenizer.from_pretrained(student_dir)
    S = {
        name: embed(student, stok, texts, "mean")
        for name, texts in (
            ("tq", train_q),
            ("tc", train_ctx),
            ("eq", eval_q),
            ("ec", eval_contexts),
        )
    }
    report["results"]["Antenna trunk, mean pool, no head"] = retrieval(
        S["eq"], S["ec"], gold
    )
    for kind in ("linear", "mlp"):
        for label, infonce, distill in (
            ("InfoNCE", True, False),
            ("granite distillation", False, True),
            ("InfoNCE + distillation", True, True),
        ):
            head = train_head(
                S["tq"], S["tc"], G["tq"], G["tc"], train_pairs, kind, infonce, distill
            )
            key = f"Antenna trunk frozen + {kind} head, {label}"
            report["results"][key] = retrieval(head(S["eq"]), head(S["ec"]), gold)
            print(
                json.dumps(
                    {key: report["results"][key], "sec": round(time.time() - t0)}
                ),
                flush=True,
            )
    open(output, "w").write(json.dumps(report, indent=1) + "\n")
