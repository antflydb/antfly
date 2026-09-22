#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
# /// script
# requires-python = ">=3.11"
# dependencies = ["torch>=2.6,<3", "transformers>=4.51,<6", "safetensors>=0.5", "numpy>=2"]
# ///
"""Evaluate retained Laya data, or replay a native soft-CE recipe in PyTorch.

Consumes prepare_laya_qualification.py artifacts. Training replays recorded
record IDs, including epoch boundaries and partial accumulation. No downloads.
"""

import argparse
import hashlib
import importlib.util
import json
import math
import time
from pathlib import Path

import torch
from safetensors.torch import load_file, save_file
from transformers import AutoTokenizer, ModernBertConfig, ModernBertModel


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def metrics(rows, temperatures):
    ce, correct, ordinal = 0.0, 0, []
    for row in rows:
        logits = (
            torch.tensor(row["logits"], dtype=torch.float64)
            / temperatures[row["qtype"]]
        )
        target = torch.tensor(row["target"], dtype=torch.float64)
        target /= target.sum()
        logp = logits.log_softmax(-1)
        ce -= (target * logp).sum().item()
        correct += int(logits.argmax() == target.argmax())
        if row["qtype"] == 1:
            index = torch.arange(len(target))
            ordinal.append(abs(((logp.exp() - target) * index).sum().item()))
    return {
        "examples": len(rows),
        "soft_ce": ce / len(rows),
        "accuracy": correct / len(rows),
        "ordinal_mae": sum(ordinal) / len(ordinal) if ordinal else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--common", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--native-run", type=Path, help="Replay this completed soft-CE run"
    )
    parser.add_argument("--compare-predictions", type=Path)
    parser.add_argument(
        "--validate-inputs-only",
        action="store_true",
        help="Validate pinned assets and every prepared token sequence without training",
    )
    parser.add_argument("--device", choices=("cpu", "mps"), default="cpu")
    args = parser.parse_args()
    if args.native_run:
        job = json.loads((args.native_run / "job.json").read_text())
        native_report = json.loads((args.native_run / "report.json").read_text())
        if (
            native_report["status"] != "complete"
            or job["objective"] != "soft_ce"
            or job["head_dropout"] != 0
            or job["resume_from"] is not None
        ):
            raise ValueError(
                "Oracle replay requires completed fresh soft-CE with dropout disabled"
            )
        if digest(args.model / "model.safetensors") != digest(
            Path(job["model_dir"]) / "model.safetensors"
        ):
            raise ValueError("Replay source weights differ from native training")
        for name in ("config.json", "tokenizer.json"):
            if digest(args.model / name) != digest(Path(job["model_dir"]) / name):
                raise ValueError(f"Replay source {name} differs from native training")
        if not job.get("calibration_file") or digest(
            args.data / "calibration.jsonl"
        ) != digest(Path(job["calibration_file"])):
            raise ValueError("Replay calibration data differs from native training")
        for split in ("train", "eval"):
            if digest(args.data / f"{split}.jsonl") != native_report[f"{split}_sha256"]:
                raise ValueError(f"Replay {split} data differs from native training")
    source_paths = {
        key: args.model / name
        for key, name in (
            ("weights", "model.safetensors"),
            ("config", "config.json"),
            ("tokenizer", "tokenizer.json"),
        )
    }
    source_sha = {key: digest(path) for key, path in source_paths.items()}
    args.output.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(8)
    torch.manual_seed(42)
    device = torch.device(args.device)
    if args.device == "mps" and not torch.backends.mps.is_available():
        raise RuntimeError("MPS was requested but is unavailable")
    # Keep the same explicit head arithmetic as upstream; disable inference's
    # fused TransformerEncoder fast path so the oracle is independent of it.
    torch.backends.mha.set_fastpath_enabled(False)
    spec = importlib.util.spec_from_file_location("laya_common", args.common)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    raw = json.loads((args.model / "config.json").read_text())
    decision = raw.pop("laya")
    cfg = ModernBertConfig.from_dict(raw)
    cfg.reference_compile = False
    cfg._attn_implementation = "eager"
    model = common.DecisionModel(
        ModernBertModel(cfg),
        decision["head_layers"],
        len(decision["act_costs"]) + 1,
        dropout=0,
    ).eval()
    model.load_state_dict(load_file(args.model / "model.safetensors"), strict=True)
    model.to(device)
    for parameter in model.act_head.parameters():
        parameter.requires_grad_(False)
    data = {}
    data_sha, sequence_sha = {}, {}
    tokenizer = AutoTokenizer.from_pretrained(args.model, local_files_only=True)
    for split in ("train", "eval", "calibration"):
        record_bytes = (args.data / f"{split}.jsonl").read_bytes()
        sequence_bytes = (args.data / f"{split}_sequences.json").read_bytes()
        data_sha[split] = hashlib.sha256(record_bytes).hexdigest()
        sequence_sha[split] = hashlib.sha256(sequence_bytes).hexdigest()
        records = [json.loads(line) for line in record_bytes.splitlines()]
        sequences = json.loads(sequence_bytes)
        if [r["id"] for r in records] != [s["id"] for s in sequences]:
            raise ValueError("Record and sequence identities differ")
        if len({r["id"] for r in records}) != len(records):
            raise ValueError("Duplicate record identities")
        for record, sequence in zip(records, sequences):
            descriptions = record.get("descriptions", [""] * len(record["labels"]))
            criteria = (
                descriptions
                if record["kind"] == "score"
                else dict(zip(record["labels"], descriptions))
            )
            ids, markers = common.build_sequence(
                tokenizer,
                record["text"],
                {"t": record["kind"], "ins": record["instruction"], "crit": criteria},
                100000,
                decision["head_max_len"],
            )
            if (
                ids != sequence["ids"]
                or markers != sequence["markers"]
                or common.QTYPES[record["kind"]] != sequence["qtype"]
                or len(ids) > decision["max_len"]
            ):
                raise ValueError(
                    "Prepared token sequence differs from source tokenizer and record"
                )
        data[split] = {r["id"]: (r, s) for r, s in zip(records, sequences)}

    if args.validate_inputs_only:
        admission = {
            "passed": True,
            "source_sha256": source_sha,
            "data_sha256": data_sha,
            "sequence_sha256": sequence_sha,
            "decisions": {split: len(rows) for split, rows in data.items()},
        }
        (args.output / "admission.json").write_text(
            json.dumps(admission, indent=2) + "\n"
        )
        print(json.dumps(admission))
        return

    def forward(items):
        batch = common.collate_items([[s for _, s in items]], cfg.pad_token_id)
        logits, actions = model(
            *(
                batch[key].to(device)
                for key in (
                    "input_ids",
                    "attention_mask",
                    "marker_pos",
                    "marker_mask",
                    "qtype",
                )
            )
        )
        return logits, actions

    def evaluate(split):
        rows = []
        with torch.no_grad():
            for record, sequence in data[split].values():
                logits, actions = forward([(record, sequence)])
                rows.append(
                    {
                        "id": record["id"],
                        "domain": sequence["domain"],
                        "qtype": sequence["qtype"],
                        "target": record["target"],
                        "logits": logits[0].tolist(),
                        "action_logits": actions[0].tolist(),
                    }
                )
        return rows

    started = time.monotonic()
    baseline = evaluate("eval")
    source_scaled = []
    for row in baseline:
        count = len(row["logits"])
        bucket = (
            "2"
            if count <= 2
            else "3-5"
            if count <= 5
            else "6-10"
            if count <= 10
            else "11+"
        )
        kind = common.QTYPE_NAMES[row["qtype"]]
        temperature = decision.get("temperature_by_options", {}).get(
            f"{kind}:{bucket}", decision.get("temperature", [1, 1, 1])[row["qtype"]]
        )
        source_scaled.append(
            {**row, "logits": [z / temperature for z in row["logits"]]}
        )
    (args.output / "initial_predictions.json").write_text(json.dumps(baseline) + "\n")
    print(
        json.dumps({"event": "initial_eval", "metrics": metrics(baseline, [1, 1, 1])}),
        flush=True,
    )
    replay = None
    if args.native_run:
        events = [
            json.loads(line)
            for line in (args.native_run / "metrics.jsonl").read_text().splitlines()
        ]
        steps = [event for event in events if event["event"] == "step"]
        batches = math.ceil(len(data["train"]) / job["batch_size"])
        total_updates = (
            math.ceil(batches / job["gradient_accumulation"]) * job["epochs"]
        )
        if len(steps) != batches * job["epochs"]:
            raise ValueError("Cannot replay incomplete training")
        parameters = [(n, p) for n, p in model.named_parameters() if p.requires_grad]
        groups = [
            {
                "params": [p for n, p in parameters if n.startswith("encoder.")],
                "lr": job["encoder_lr"],
            },
            {
                "params": [p for n, p in parameters if not n.startswith("encoder.")],
                "lr": job["head_lr"],
            },
        ]
        optimizer = torch.optim.AdamW(
            groups, weight_decay=job["weight_decay"], foreach=False, fused=False
        )
        pending = updates = 0
        losses = []
        for index, event in enumerate(steps):
            items = [data["train"][key] for key in event["record_ids"]]
            logits, _ = forward(items)
            target = torch.zeros_like(logits)
            for i, (record, _) in enumerate(items):
                values = torch.tensor(record["target"], device=device)
                target[i, : len(values)] = values / values.sum()
            loss = -(target * logits.log_softmax(-1)).sum(-1).mean()
            loss.backward()
            pending += 1
            if pending == job["gradient_accumulation"] or event["batch"] == batches:
                for _, parameter in parameters:
                    parameter.grad.div_(pending)
                torch.nn.utils.clip_grad_norm_(
                    [p for _, p in parameters], job["max_grad_norm"], foreach=False
                )
                for group, initial in zip(
                    optimizer.param_groups, [job["encoder_lr"], job["head_lr"]]
                ):
                    floor = min(initial, 1e-6)
                    group["lr"] = floor + (initial - floor) * 0.5 * (
                        1 + math.cos(math.pi * updates / total_updates)
                    )
                optimizer.step()
                optimizer.zero_grad(set_to_none=True)
                pending = 0
                updates += 1
            row = {
                "microbatch": index + 1,
                "loss": loss.item(),
                "native_loss": event["report"]["ce"],
                "updates": updates,
            }
            losses.append(row)
            print(json.dumps(row), flush=True)
        save_file(
            {
                name: value.detach().cpu().contiguous()
                for name, value in model.state_dict().items()
            },
            args.output / "model.safetensors",
        )
        (args.output / "losses.json").write_text(json.dumps(losses) + "\n")
        replay = {
            "objective": job["objective"],
            "updates": updates,
            "microbatches": len(steps),
            "native_job_sha256": digest(args.native_run / "job.json"),
        }
    final = evaluate("eval") if replay else baseline
    temperatures = decision.get("temperature", [1, 1, 1])
    if replay:
        calibration = evaluate("calibration")
        temperatures = [1, 1, 1]
        for kind in range(3):
            subset = [row for row in calibration if row["qtype"] == kind]
            if len(subset) >= 10:
                candidates = [1.0] + [
                    math.exp(math.log(0.1) + i / 200 * math.log(100))
                    for i in range(201)
                ]
                temperatures[kind] = min(
                    candidates, key=lambda t: metrics(subset, [t] * 3)["soft_ce"]
                )
    parity = None
    if args.compare_predictions:
        native = json.loads(args.compare_predictions.read_text())
        if len(native) != len(final):
            raise ValueError("Prediction counts differ")
        errors = []
        for n, p in zip(native, final):
            if (
                len(n["target"]) != len(p["target"])
                or len(n["logits"]) != len(p["logits"])
                or n["kind"] != common.QTYPE_NAMES[p["qtype"]]
            ):
                raise ValueError("Prediction type or shape differs")
            # JSON decimal parsing differs from the native FP32 serialization.
            if max(abs(a - b) for a, b in zip(n["target"], p["target"])) > 1e-6:
                raise ValueError("Target order differs")
            t = temperatures[p["qtype"]]
            errors.append(
                (
                    torch.tensor(n["logits"]).div(t).softmax(-1)
                    - torch.tensor(p["logits"]).div(t).softmax(-1)
                )
                .abs()
                .max()
                .item()
            )
        parity = {
            "max_probability_error": max(errors),
            "tolerance": 5e-5,
            "passed": max(errors) <= 5e-5,
        }
    if source_sha != {key: digest(path) for key, path in source_paths.items()}:
        raise ValueError("Source assets changed during reference execution")
    result = {
        "format": "antfly-laya-quality-oracle/v1",
        "torch": torch.__version__,
        "device": str(device),
        "threads": torch.get_num_threads(),
        "source_sha256": source_sha,
        "weights_sha256": digest(args.model / "model.safetensors"),
        "common_sha256": digest(args.common),
        "trained_weights_sha256": digest(args.output / "model.safetensors")
        if replay
        else None,
        "data_sha256": data_sha,
        "sequence_sha256": sequence_sha,
        "elapsed_seconds": time.monotonic() - started,
        "replay": replay,
        "initial": metrics(baseline, [1, 1, 1]),
        "initial_serving_calibrated": metrics(source_scaled, [1, 1, 1]),
        "final_uncalibrated": metrics(final, [1, 1, 1]),
        "final": metrics(final, temperatures),
        "temperatures": temperatures,
        "by_kind": {
            common.QTYPE_NAMES[k]: metrics(
                [r for r in final if r["qtype"] == k], temperatures
            )
            for k in range(3)
        },
        "by_domain": {
            d: metrics([r for r in final if r["domain"] == d], temperatures)
            for d in sorted({r["domain"] for r in final})
        },
        "native_prediction_parity": parity,
    }
    (args.output / "predictions.json").write_text(json.dumps(final) + "\n")
    (args.output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)
    if parity and not parity["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
