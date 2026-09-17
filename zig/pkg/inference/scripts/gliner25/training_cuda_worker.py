#!/usr/bin/env python3
"""Pinned Fastino CUDA training worker for deterministic differential tests."""
from __future__ import annotations

import argparse
import copy
from contextlib import nullcontext
from dataclasses import replace
import gc
import json
import os
from pathlib import Path
import sys
import time

import benchmark_cpu as common
import metal_python_worker as runtime
import oracle

SCOPE = "gliner25_cuda_training_comparison_v1"


def cublas_version(torch):
    """Query the runtime behind PyTorch's current handle, outside timing."""
    import ctypes
    major = str(torch.version.cuda).split(".")[0]
    if not major.isdecimal():
        raise ValueError("PyTorch CUDA runtime identity is unavailable")
    handle = torch.cuda.current_blas_handle()
    library = ctypes.CDLL(f"libcublas.so.{major}")
    query = library.cublasGetVersion_v2
    query.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)]
    query.restype = ctypes.c_int
    version = ctypes.c_int()
    if not handle or query(ctypes.c_void_p(handle), ctypes.byref(version)) != 0 or version.value <= 0:
        raise ValueError("PyTorch cuBLAS runtime identity is unavailable")
    return version.value


def disable_query_sampling(model):
    # A ratio of one can still randomly drop absent queries. The source's zero
    # setting bypasses subsampling and preserves the complete query mask.
    settings = replace(model.boundary_head.settings, negative_query_ratio=0.0)
    model.boundary_head.settings = settings
    model.boundary_settings = settings


def adapt_row(row):
    """Translate the existing named annotation fixture without dropping tasks.

    This bounded adapter accepts its explicit unique occurrences only. It does
    not silently generalize ambiguous offsets to upstream surface matching.
    """
    text, schema = row["text"], row["schema"]
    def surface(span):
        if span.get("unit", "utf8_bytes") != "utf8_bytes":
            raise ValueError("training fixture requires UTF-8 byte offsets")
        if (type(span.get("start")) is not int or type(span.get("end")) is not int
                or not 0 <= span["start"] < span["end"] <= len(text.encode())):
            raise ValueError("invalid training surface offsets")
        value = text.encode()[span["start"]:span["end"]].decode()
        if not value or text.count(value) != 1:
            raise ValueError("ambiguous training surface cannot preserve exact offsets")
        return value
    entities = {name: [] for name in schema.get("entities", [])}
    by_id = {}
    for entity in row.get("entities", []):
        value = surface(entity["span"])
        entities[entity["type"]].append(value)
        by_id[entity["id"]] = value
    output = {"entities": entities}
    labels = {item["task"]: item["labels"] for item in row.get("classifications", [])}
    output["classifications"] = [
        {"task": task["name"], "labels": task["labels"], "true_label": labels[task["name"]],
         "multi_label": task["mode"] == "multi"}
        for task in schema.get("classifications", [])
    ]
    output["json_structures"] = []
    output["record_metadata"] = {}
    for name, spec in schema.get("structures", {}).items():
        output["record_metadata"][name] = {
            "mode": spec["mode"], "anchor": spec.get("anchor"),
            "occurrence_policy": spec.get("occurrence_policy", "latent_all"),
            "fields": {key: {"cardinality": field["cardinality"], "exclusive": field.get("exclusive", False)}
                       for key, field in spec["fields"].items()},
        }
    for record in row.get("records", []):
        fields = {}
        for field in record["fields"]:
            values = field["values"]
            if len(values) != 1 or len(values[0]["occurrences"]) != 1:
                raise ValueError("fixture adapter requires one explicit occurrence per field")
            fields[field["name"]] = surface(values[0]["occurrences"][0])
        output["json_structures"].append({record["type"]: fields})
    output["relations"] = [{edge["type"]: {"head": by_id[edge["head"]["entity"]], "tail": by_id[edge["tail"]["entity"]]}}
                           for edge in row.get("relations", [])]
    return text, output


def setup(args):
    runtime.configure_environment()
    os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
    os.environ["TRITON_F32_DEFAULT"] = "ieee"
    provenance, torch = oracle.prepare_runtime(args.upstream)
    torch.set_num_interop_threads(1)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.set_float32_matmul_precision("highest")
    runtime.verify_runtime(torch, "cuda")
    return provenance, torch


def worker(args):
    provenance, torch = setup(args)
    config = common.strict_json(args.config.read_bytes())
    run = config["run"]
    if run["mode"] not in ("heads", "full") or run["shuffle"] or run["scheduler"] != "constant" or run["warmup_steps"] != 0:
        raise ValueError("unsupported training benchmark configuration")
    bundle = oracle.verify_model_dir(args.model, Path(config["source_dir"]))
    raw = Path(config["train_file"]).read_bytes()
    rows = [common.strict_json(line) for line in raw.splitlines()]
    if not rows or len(rows) % (run["batch_size"] * run["accumulation"]):
        raise ValueError("dataset must contain complete accumulation windows")
    from gliner2 import AutoExtractor
    from gliner2.training.trainer import ExtractorTrainer, TrainingConfig

    def create():
        model = AutoExtractor.from_pretrained(config["source_dir"], local_files_only=True).float().cuda()
        model.strict_extraction = True
        disable_query_sampling(model)
        for module in model.modules():
            if isinstance(module, torch.nn.Dropout):
                module.p = 0.0
            for attr in ("drop_prob", "dropout_p"):
                if hasattr(module, attr) and isinstance(getattr(module, attr), (int, float)):
                    setattr(module, attr, 0.0)
        for parameter in model.encoder.parameters():
            parameter.requires_grad_(run["mode"] == "full")
        model.eval()
        runtime.verify_model_tensors(model, torch, "cuda")
        model.train()
        import training_cuda_quality as quality
        snapshot_parameters = quality.canonical_parameters(model)
        if args.profile == "compile_fp32":
            model.compile(dynamic=True)
        quality.canonical_parameters(model, snapshot_parameters)
        cfg = TrainingConfig(
            output_dir="/tmp/unused-gliner25-training-output", fp16=False, bf16=False,
            encoder_lr=run["encoder_lr"], task_lr=run["task_lr"],
            weight_decay=run["weight_decay"], adam_beta1=run["beta1"], adam_beta2=run["beta2"],
            adam_epsilon=run["epsilon"], max_grad_norm=run["max_grad_norm"],
            gradient_accumulation_steps=run["accumulation"], scheduler_type="constant",
            warmup_steps=0, warmup_ratio=0, fused_optimizer=True,
            gold_injection_start=1, gold_injection_end=1, log_proposal_metrics=False,
            strict_training=True,
        )
        trainer = ExtractorTrainer.__new__(ExtractorTrainer)
        trainer.model, trainer.config, trainer.device = model, cfg, torch.device("cuda")
        trainer.snapshot_parameters = snapshot_parameters
        trainer.is_distributed, trainer.global_step = False, 0
        trainer._skip_counter = None
        trainer._planned_max_steps = run["epochs"] * len(rows) // (run["batch_size"] * run["accumulation"])
        trainer.optimizer = trainer._create_optimizer()
        trainer.scheduler = torch.optim.lr_scheduler.LambdaLR(trainer.optimizer, lambda _: 1.0)
        trainer.microbatch = 0
        return trainer

    trainer = create()
    def prepare():
        start = (trainer.microbatch * run["batch_size"]) % len(rows)
        examples = [adapt_row(copy.deepcopy(row)) for row in rows[start:start + run["batch_size"]]]
        # Disable stochastic schema augmentation, while building supervised
        # targets. The model and upstream backward remain in training mode.
        return trainer.model.processor.collate_fn_inference(
            examples, max_len=128, error_policy="raise", architecture="boundary",
            build_targets=True, max_gold_per_query=trainer.model.boundary_head.settings.max_gold_per_query,
            on_capacity_exceeded="raise", ignore_missing_entities=False,
        ).to("cuda")

    common.emit({"event": "ready", "arm": "python", "scope": SCOPE, "device": "cuda",
                 "device_name": torch.cuda.get_device_name(), "dtype": "float32", "dropout": 0, "negative_query_sampling": False,
                 "profile": args.profile, "fused_optimizer": True, "model_bundle": bundle,
                 "cublas_version": cublas_version(torch),
                 "dataset_sha256": oracle.sha256_file(Path(config["train_file"])),
                 "config_sha256": oracle.sha256_file(args.config), "batch_size": run["batch_size"],
                 "accumulation": run["accumulation"], "mode": run["mode"],
                 "trainable_tensors": sum(p.requires_grad for p in trainer.model.parameters()),
                 "provenance": provenance, "qualification": False})
    previous = 0
    for line in sys.stdin.buffer:
        if len(line) > 2048 or previous >= 4096:
            raise ValueError("training command limit exceeded")
        c = common.strict_json(line)
        if set(c) != {"request_id", "op", "case_id"} or type(c["request_id"]) is not int or c["request_id"] <= previous:
            raise ValueError("invalid training command identity")
        previous = c["request_id"]
        identity = {"arm": "python", "request_id": previous, "case_id": c["case_id"]}
        try:
            if c["op"] == "stop":
                torch.cuda.synchronize()
                oracle.verify_upstream_checkout(args.upstream)
                if oracle.verify_model_dir(args.model, Path(config["source_dir"])) != bundle:
                    raise ValueError("source model changed")
                common.emit({"event": "stopped", **identity})
                return
            if c["op"] not in ("validate", "run"):
                raise ValueError("unknown operation")
            if c["case_id"] == "reset" and c["op"] == "validate":
                del trainer
                gc.collect()
                trainer = create()
                torch.cuda.synchronize()
                result = {}
                duration = 1
            elif c["case_id"] == "inputs" and c["op"] == "validate":
                batch = prepare()
                result = {"input_ids": batch.input_ids.detach().cpu().flatten().tolist(),
                          "encoder_shape": list(batch.input_ids.shape),
                          "query_layouts": repr(batch.query_layouts), "record_specs": repr(batch.record_specs)}
                del batch
                duration = 1
            elif c["case_id"] == "snapshot" and c["op"] == "validate":
                slots, offset = [], 0
                import training_cuda_quality as quality
                parameters = quality.canonical_parameters(trainer.model, trainer.snapshot_parameters)
                with args.snapshot.open("wb") as out:
                    for name, parameter in parameters:
                        if not parameter.requires_grad:
                            continue
                        state = trainer.optimizer.state.get(parameter, {})
                        slots.append({"canonical_name": name, "shape": list(parameter.shape), "elements": parameter.numel(),
                                      "offset": offset, "present": parameter.grad is not None,
                                      "adam_step": int(state.get("step", 0)), "group": 0 if "encoder" in name else 1})
                        for value in (parameter, parameter.grad, state.get("exp_avg"), state.get("exp_avg_sq")):
                            array = (torch.zeros_like(parameter) if value is None else value).detach().float().cpu().contiguous().numpy()
                            payload = array.tobytes(); out.write(payload); offset += len(payload)
                result = {"snapshot": str(args.snapshot), "size_bytes": offset, "slots": slots,
                          "identity": {"optimizer_step": trainer.global_step, "microbatch_step": trainer.microbatch},
                          "accumulated_microbatches": trainer.microbatch % run["accumulation"]}
                duration = 1
            elif c["case_id"] == "heldout" and c["op"] == "validate":
                if args.evaluation_layout is None:
                    raise ValueError("held-out evaluation was not configured")
                import training_cuda_quality as quality
                layouts = oracle.read_json(args.evaluation_layout)
                validation = args.validation_file
                if validation is None or oracle.sha256_file(validation) != args.validation_sha256:
                    raise ValueError("validation fixture hash differs")
                heldout = quality.read_rows(validation)
                # Optimizer tensors and gradients remain untouched. Restore
                # Python's weights even if native-weight evaluation fails.
                result = {"python": quality.evaluate(trainer.model, heldout, torch)}
                try:
                    quality.load_weights(trainer.model, layouts["native"], torch, trainer.snapshot_parameters)
                    result["native"] = quality.evaluate(trainer.model, heldout, torch)
                finally:
                    quality.load_weights(trainer.model, layouts["python"], torch, trainer.snapshot_parameters)
                torch.cuda.synchronize()
                duration = 1
            elif c["case_id"] in ("step", "trace_step"):
                tracing = c["case_id"] == "trace_step"
                if tracing and c["op"] != "validate":
                    raise ValueError("loss tracing is only supported outside throughput measurements")
                if tracing:
                    from training_cuda_trace import BoundaryTrace
                    trace = BoundaryTrace(trainer.model, torch, args.trace_native_input)
                runtime.verify_runtime(torch, "cuda")
                torch.cuda.synchronize()
                started = time.perf_counter_ns()
                batch = prepare()
                with trace if tracing else nullcontext():
                    loss = trainer._backward_one(batch, trainer.microbatch, False, torch.float32)
                stepped = (trainer.microbatch + 1) % run["accumulation"] == 0
                if stepped:
                    trainer._optimizer_step()
                    trainer.global_step += 1
                trainer.microbatch += 1
                del batch
                torch.cuda.synchronize()
                duration = time.perf_counter_ns() - started
                terms = {name.removesuffix("_loss"): float(value.detach()) for name, value in trainer._last_train_outputs.losses.items() if value is not None}
                terms["total"] = float(loss)
                if not torch.isfinite(loss) or int(trainer._skip_counter) != 0:
                    raise ValueError("upstream skipped or zeroed non-finite loss")
                result = {"terms": terms, "optimizer_stepped": stepped, "microbatch_step": trainer.microbatch,
                          "optimizer_step": trainer.global_step, "examples": run["batch_size"],
                          "grad_norm": float(trainer._last_grad_norm) if stepped else None,
                          "cuda_memory": runtime.cuda_memory(torch, "cuda")}
                if tracing:
                    result["loss_trace"] = trace.result
            else:
                raise ValueError("unknown training command")
            common.emit({"event": "result", **identity, "duration_ns": duration, **result})
        except Exception as exc:
            common.emit({"event": "error", **identity, **runtime.error_details(exc, "cuda")})
            raise
    raise ValueError("training protocol ended without stop")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--model", choices=("small", "base", "multi"), default="small")
    parser.add_argument("--evaluation-layout", type=Path)
    parser.add_argument("--trace-native-input", type=Path)
    parser.add_argument("--validation-file", type=Path)
    parser.add_argument("--validation-sha256")
    parser.add_argument("--profile", choices=("eager_fp32", "compile_fp32"), default="eager_fp32")
    worker(parser.parse_args())


if __name__ == "__main__":
    main()
