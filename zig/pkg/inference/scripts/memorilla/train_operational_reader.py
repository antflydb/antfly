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

"""Train a separate operational-QA reader; preserve every released checkpoint.

Only the reader learns. Gold targets enter training labels, never memory inputs.
Development and final worlds have disjoint entity strings and exact codes.
Final inference is forbidden until every development answer is exact. A Python
candidate cannot earn native serving qualification by passing this experiment.
"""

from __future__ import annotations

import argparse
import gc
import importlib.metadata
import json
import math
import os
from pathlib import Path
import random
import shutil
import time

import torch
from safetensors.torch import save_file

from assets import verify_base_model
from decoder_reference import DEFAULT_SYSTEM, prepare_prompt
from encoder_reference import active_ids, normalize_features
from exporter import sha256
from gpu_quality_reference import (
    candidate_proof,
    condition,
    elapsed,
    generate,
    save,
)
from reference import load_upstream
from released_reference import checkpoint_proof, source_recipe


def validate_curriculum(value):
    if value.get("schema") != "antfly.memorilla.operational-curriculum.v1":
        raise ValueError("unrecognized operational curriculum")
    ids, worlds, facts = set(), {}, {s: set() for s in ("train", "dev", "test")}
    rows = value["rows"]
    for row in rows:
        split = row["split"]
        if split not in facts or row["id"] in ids:
            raise ValueError("invalid split or duplicate case")
        ids.add(row["id"])
        if row["world"] in worlds and worlds[row["world"]] != split:
            raise ValueError("world leaked across splits")
        worlds[row["world"]] = split
        if row["answer"] != "UNKNOWN":
            facts[split].add(row["answer"])
        if (
            not row["question"].strip()
            or not row["answer"].strip()
            or not 1 <= len(row["documents"]) <= 64
        ):
            raise ValueError("empty question/answer or document bound exceeded")
        if any("<|" in t for t in row["documents"] + [row["question"], row["answer"]]):
            raise ValueError("curriculum contains control tokens")
    if any(
        facts[a] & facts[b]
        for a, b in (("train", "dev"), ("train", "test"), ("dev", "test"))
    ):
        raise ValueError("exact target fact leaked across splits")
    return rows


def canonical(value):
    return " ".join(value.strip().rstrip(".").split()).casefold()


def write_checkpoint(path, value):
    temporary = path.with_suffix(".partial")
    with temporary.open("wb") as stream:
        torch.save(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


@torch.inference_mode()
def evaluate(rows, reader, decoder, tokenizer, features, maximum):
    reader.eval()
    result = []
    for row in rows:
        docs = torch.stack([features[t] for t in row["documents"]])[None]
        query = features[row["question"].strip()][None]
        mask = torch.zeros(docs.shape[:2], dtype=torch.bool, device="cuda")
        memory = reader(docs, mask, query)[0]
        _, embeds, _ = prepare_prompt(
            tokenizer,
            decoder.get_input_embeddings(),
            memory,
            system_prompt=DEFAULT_SYSTEM,
            question=row["question"],
            device="cuda",
        )
        answer = generate(decoder, tokenizer, embeds, maximum)
        result.append(
            {
                "case_id": row["id"],
                "family": row["family"],
                **answer,
                "correct": canonical(answer["answer"]) == canonical(row["answer"]),
            }
        )
    return result


def main():
    from transformers import AutoModel, AutoModelForCausalLM, AutoTokenizer

    p = argparse.ArgumentParser(description=__doc__)
    for name in (
        "encoder",
        "decoder",
        "checkpoint",
        "upstream-source",
        "curriculum",
        "output",
    ):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--steps", type=int, default=1000)
    p.add_argument("--batch-size", type=int, default=2)
    p.add_argument("--learning-rate", type=float, default=1e-4)
    p.add_argument("--init-reader", type=Path)
    p.add_argument("--init-reader-sha256")
    p.add_argument("--resume-from", type=Path)
    p.add_argument("--run", action="store_true")
    args = p.parse_args()
    if (
        not 1 <= args.steps <= 5000
        or not 1 <= args.batch_size <= 4
        or not 0 < args.learning_rate <= 1e-3
    ):
        p.error("training bounds exceeded")
    rows = validate_curriculum(json.loads(args.curriculum.read_text()))
    config, proof = checkpoint_proof(args.checkpoint)
    if bool(args.init_reader) != bool(args.init_reader_sha256):
        p.error("initial candidate requires its explicit digest")
    initial_proof = proof
    if args.init_reader:
        initial_config, initial_proof = candidate_proof(
            args.init_reader, args.init_reader_sha256
        )
        if initial_config != config:
            raise ValueError("initial reader geometry changed")
    recipe = source_recipe(args.upstream_source)
    source_root = Path(__file__).resolve().parent
    source_files = sorted(
        p for p in source_root.rglob("*") if p.suffix in {".py", ".json"}
    )
    source_hashes = {str(p.relative_to(source_root)): sha256(p) for p in source_files}
    protocol = {
        "schema": "antfly.memorilla.operational-training.v1",
        "reader_start": initial_proof,
        "released_ancestor": proof,
        "upstream": recipe,
        "curriculum_sha256": sha256(args.curriculum),
        "runner_sha256": sha256(Path(__file__)),
        "sources": source_hashes,
        "steps": args.steps,
        "batch_size": args.batch_size,
        "learning_rate": args.learning_rate,
        "reader_dtype": "float32",
        "encoder_dtype": "float32",
        "decoder_dtype": "bfloat16",
        "feature_storage": "normalized F32 -> FP16 -> BF16 -> F32",
        "attention": "encoder eager, decoder sdpa",
        "tf32": False,
        "native_serving_qualified": False,
        "final_gate": "100% exact on dev before final inference",
        "splits": {
            s: sum(r["split"] == s for r in rows) for s in ("train", "dev", "test")
        },
        "seed": 19757,
        "torch": str(torch.__version__),
        "torch_cuda": torch.version.cuda,
        "transformers": importlib.metadata.version("transformers"),
        "safetensors": importlib.metadata.version("safetensors"),
    }
    if not args.run:
        print(json.dumps(protocol, indent=2))
        return
    resumed = (
        torch.load(args.resume_from, map_location="cpu", weights_only=True)
        if args.resume_from
        else None
    )
    if resumed is not None and resumed["protocol"] != protocol:
        raise ValueError(
            "resume protocol differs; keep the original source and arguments"
        )
    args.output.mkdir(parents=True, exist_ok=False)
    archive = args.output / "source"
    archive.mkdir()
    for path in source_files:
        relative = path.relative_to(source_root)
        destination = archive / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
        if sha256(destination) != source_hashes[str(relative)]:
            raise ValueError("source changed while archiving; no model was loaded")
    save(args.output / "source-manifest.json", source_hashes)
    shutil.copyfile(args.curriculum, args.output / "curriculum.json")
    result = {
        "protocol": protocol,
        "completed": False,
        "native_serving_qualified": False,
        "optimizer_steps": [],
        "heldout_executed": False,
    }
    save(args.output / "result.json", result)
    result["encoder_proof"] = verify_base_model(args.encoder, "encoder")
    result["decoder_proof"] = verify_base_model(args.decoder, "decoder")
    torch.set_num_threads(4)
    torch.manual_seed(protocol["seed"])
    rng = random.Random(protocol["seed"])
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.set_float32_matmul_precision("highest")
    active = [r for r in rows if r["split"] in ("train", "dev")]
    texts = list(
        dict.fromkeys(
            [t for r in active for t in r["documents"] + [r["question"].strip()]]
        )
    )
    tokenizer = AutoTokenizer.from_pretrained(
        args.encoder, local_files_only=True, padding_side="left"
    )
    encoder = (
        AutoModel.from_pretrained(
            args.encoder,
            local_files_only=True,
            torch_dtype=torch.float32,
            attn_implementation="eager",
        )
        .eval()
        .requires_grad_(False)
        .to("cuda")
    )
    features = {}
    started = time.perf_counter()
    with torch.inference_mode():
        for offset in range(0, len(texts), 8):
            batch = texts[offset : offset + 8]
            tokens = tokenizer(
                batch, return_tensors="pt", padding=True, truncation=False
            ).to("cuda")
            if any(len(ids) > 256 for ids in active_ids(tokens)):
                raise ValueError("training encoder text exceeds native bound")
            normalized = normalize_features(
                encoder(**tokens, use_cache=False).last_hidden_state,
                tokens["attention_mask"],
            )
            for text, value in zip(batch, normalized):
                features[text] = condition(value, torch.float32).clone()
            if offset % 256 == 0:
                print(
                    json.dumps(
                        {"phase": "encoding", "texts": offset, "total": len(texts)}
                    ),
                    flush=True,
                )
    result["feature_seconds"] = elapsed(started)
    # Inference tensors cannot be saved by the reader's autograd operations.
    features = {text: value.clone() for text, value in features.items()}
    save_file(
        {str(i): features[t].cpu() for i, t in enumerate(texts)},
        str(args.output / "features.safetensors"),
    )
    result["feature_texts"] = texts
    result["features_sha256"] = sha256(args.output / "features.safetensors")
    del encoder, tokenizer, normalized, tokens
    gc.collect()
    torch.cuda.empty_cache()
    weights = torch.load(
        (args.init_reader or args.checkpoint) / "memory.pt",
        map_location="cpu",
        weights_only=True,
        mmap=True,
    )
    reader = load_upstream(args.upstream_source, config, weights).to(
        device="cuda", dtype=torch.float32
    )
    del weights
    decoder = (
        AutoModelForCausalLM.from_pretrained(
            args.decoder,
            local_files_only=True,
            torch_dtype=torch.bfloat16,
            attn_implementation="sdpa",
        )
        .eval()
        .requires_grad_(False)
        .to("cuda")
    )
    tokenizer = AutoTokenizer.from_pretrained(args.decoder, local_files_only=True)
    decoder.gradient_checkpointing_enable(
        gradient_checkpointing_kwargs={"use_reentrant": False}
    )
    decoder.config.use_cache = False
    optimizer = torch.optim.AdamW(
        reader.parameters(), lr=args.learning_rate, weight_decay=0.01
    )
    training = [r for r in rows if r["split"] == "train"]
    development = [r for r in rows if r["split"] == "dev"]
    monitor = [r for r in development if int(r["id"].split("_")[-2]) == 0]
    result["initial_dev_monitor"] = evaluate(
        monitor, reader, decoder, tokenizer, features, 24
    )
    save(args.output / "result.json", result)
    order = []
    first_step = 0
    if resumed is not None:
        reader.load_state_dict(resumed["reader"])
        optimizer.load_state_dict(resumed["optimizer"])
        torch.set_rng_state(resumed["torch_rng"])
        torch.cuda.set_rng_state_all(resumed["cuda_rng"])
        rng.setstate(resumed["python_rng"])
        order = resumed["order"]
        first_step = resumed["next_step"]
        result.update(resumed["history"])
        result["resumed_checkpoint_sha256"] = sha256(args.resume_from)
        del resumed
    started = time.perf_counter()
    for step in range(first_step, args.steps):
        if len(order) < args.batch_size:
            order = list(range(len(training)))
            rng.shuffle(order)
        batch = [training[order.pop()] for _ in range(args.batch_size)]
        optimizer.zero_grad(set_to_none=True)
        reader.train()
        # Checkpointing is active only in train mode; dropout is explicitly
        # disabled in the frozen decoder to keep the target model deterministic.
        decoder.train()
        for module in decoder.modules():
            if isinstance(module, torch.nn.Dropout):
                module.p = 0.0
        embeds_list, labels_list = [], []
        for row in batch:
            docs = torch.stack([features[t] for t in row["documents"]])[None]
            query = features[row["question"].strip()][None]
            mask = torch.zeros(docs.shape[:2], dtype=torch.bool, device="cuda")
            memory = reader(docs, mask, query)[0]
            ids, embeds, _ = prepare_prompt(
                tokenizer,
                decoder.get_input_embeddings(),
                memory,
                system_prompt=DEFAULT_SYSTEM,
                question=row["question"],
                device="cuda",
            )
            target = tokenizer(row["answer"], add_special_tokens=False)["input_ids"] + [
                151645
            ]
            target = torch.tensor(target, device="cuda")
            embeds_list.append(
                torch.cat((embeds, decoder.get_input_embeddings()(target)))
            )
            labels_list.append(torch.cat((torch.full_like(ids, -100), target)))
        length = max(len(e) for e in embeds_list)
        embeddings = torch.stack(
            [
                torch.nn.functional.pad(e, (0, 0, 0, length - len(e)))
                for e in embeds_list
            ]
        )
        labels = torch.stack(
            [
                torch.nn.functional.pad(t, (0, length - len(t)), value=-100)
                for t in labels_list
            ]
        )
        attention = torch.stack(
            [torch.arange(length, device="cuda") < len(e) for e in embeds_list]
        )
        loss = decoder(
            inputs_embeds=embeddings,
            attention_mask=attention,
            labels=labels,
            use_cache=False,
        ).loss
        if not torch.isfinite(loss).item():
            raise ValueError("nonfinite training loss")
        loss.backward()
        norm = torch.nn.utils.clip_grad_norm_(
            reader.parameters(), 1.0, error_if_nonfinite=True
        )
        warmup = min(1.0, (step + 1) / max(1, args.steps // 10))
        scale = warmup * 0.5 * (1 + math.cos(math.pi * step / args.steps))
        for group in optimizer.param_groups:
            group["lr"] = args.learning_rate * scale
        optimizer.step()
        if step % 10 == 0 or step + 1 == args.steps:
            entry = {
                "step": step + 1,
                "loss": float(loss.detach()),
                "gradient_norm": float(norm),
                "seconds": elapsed(started),
            }
            result["optimizer_steps"].append(entry)
            save(args.output / "result.json", result)
            print(json.dumps(entry), flush=True)
        if (step + 1) % 200 == 0 or step + 1 == args.steps:
            decoder.eval()
            result.setdefault("dev_monitors", []).append(
                {
                    "step": step + 1,
                    "results": evaluate(
                        monitor, reader, decoder, tokenizer, features, 24
                    ),
                }
            )
            save(args.output / "result.json", result)
            write_checkpoint(
                args.output / "resume.pt",
                {
                    "protocol": protocol,
                    "reader": reader.state_dict(),
                    "optimizer": optimizer.state_dict(),
                    "next_step": step + 1,
                    "torch_rng": torch.get_rng_state(),
                    "cuda_rng": torch.cuda.get_rng_state_all(),
                    "python_rng": rng.getstate(),
                    "order": order,
                    "history": {
                        k: result[k] for k in ("optimizer_steps", "dev_monitors")
                    },
                },
            )
    decoder.eval()
    reader.eval()
    candidate = args.output / "candidate"
    candidate.mkdir()
    shutil.copyfile(args.checkpoint / "config.json", candidate / "config.json")
    write_checkpoint(
        candidate / "memory.pt",
        {k: v.detach().cpu().bfloat16() for k, v in reader.state_dict().items()},
    )
    result["candidate_sha256"] = sha256(candidate / "memory.pt")
    # Assess precisely the saved BF16 checkpoint, not unsaved master weights.
    reader.load_state_dict(torch.load(candidate / "memory.pt", weights_only=True))
    result["development"] = evaluate(
        development, reader, decoder, tokenizer, features, 24
    )
    result["development_exact"] = sum(r["correct"] for r in result["development"])
    result["development_total"] = len(development)
    result["development_gate_passed"] = result["development_exact"] == len(development)
    result["completed"] = True
    result["final_inference_pending"] = result["development_gate_passed"]
    result["heldout_executed"] = False
    result["cuda_peak_allocated_bytes"] = torch.cuda.max_memory_allocated()
    save(args.output / "result.json", result)
    print(
        json.dumps(
            {
                "completed": True,
                "development_exact": result["development_exact"],
                "development_total": len(development),
                "heldout_executed": False,
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
