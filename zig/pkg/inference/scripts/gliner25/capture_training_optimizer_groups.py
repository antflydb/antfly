#!/usr/bin/env python3
"""Execute pinned optimizer grouping without importing Torch or loading weights.

The unmodified upstream method runs on synthetic named parameters. A recording
AdamW constructor captures its arguments; this fixture makes no optimizer-math,
CUDA, Metal, or model-execution claim.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import platform
import sys
from types import SimpleNamespace

import oracle


class Parameter:
    def __init__(self, name: str, trainable: bool):
        self.name = name
        self.requires_grad = trainable


class RecordingAdamW:
    def __init__(self, groups, **kwargs):
        self.groups = [
            {**group, "params": [parameter.name for parameter in group["params"]]}
            for group in groups
        ]
        self.kwargs = kwargs


def extract_method(source: str, path: Path):
    classes = [node for node in ast.parse(source).body
               if isinstance(node, ast.ClassDef) and node.name == "ExtractorTrainer"]
    if len(classes) != 1:
        raise oracle.ContractError("pinned trainer class differs")
    methods = [node for node in classes[0].body
               if isinstance(node, ast.FunctionDef) and node.name == "_create_optimizer"]
    if len(methods) != 1 or methods[0].decorator_list:
        raise oracle.ContractError("pinned optimizer method differs")
    method = methods[0]
    text = ast.get_source_segment(source, method)
    if text is None:
        raise oracle.ContractError("optimizer source segment is missing")
    namespace = {"AdamW": RecordingAdamW, "logger": SimpleNamespace(info=lambda *args: None)}
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["_create_optimizer"], method, text


def capture(source: Path):
    if sys.flags.optimize:
        raise oracle.ContractError("optimizer grouping capture requires enabled upstream assertions")
    checkout = oracle.verify_upstream_checkout(source)
    trainer = source / "gliner2/training/trainer.py"
    if not 0 < trainer.stat().st_size <= 1024 * 1024:
        raise oracle.ContractError("trainer source byte budget exceeded")
    invoke, method, method_source = extract_method(trainer.read_text(), trainer)
    reference = oracle.read_json(oracle.FIXTURES / "reference_manifest.json")
    inventories = {}
    parameter_names = None
    for variant in ("small", "base", "multi"):
        relative = f"models/{variant}/tensor_inventory.json"
        path = oracle.FIXTURES / relative
        pin = oracle.verify_file(path, reference["files"][relative])
        inventory = oracle.read_json(path)
        names = sorted(inventory["tensors"])
        if inventory["provenance"]["commit"] != oracle.UPSTREAM_COMMIT or len(names) != 334:
            raise oracle.ContractError("published inventory identity differs")
        if parameter_names is not None and names != parameter_names:
            raise oracle.ContractError("variant parameter names differ; separate profiles are required")
        parameter_names = names
        inventories[relative] = pin
    if parameter_names is None:
        raise oracle.ContractError("missing published names")

    config = dict(adam_beta1=0.9, adam_beta2=0.999, adam_epsilon=1e-8,
                  encoder_lr=1e-5, task_lr=5e-4, weight_decay=0.01)
    adapter_names = [
        "base_model.model.encoder.encoder.layer.0.attention.self.query_proj.lora_A.default.weight",
        "base_model.model.encoder.encoder.layer.0.attention.self.query_proj.lora_B.default.weight",
        "base_model.model.boundary_head.shared_pool_scorer.film.0.lora_A.default.weight",
        "base_model.model.boundary_head.shared_pool_scorer.film.0.lora_B.default.weight",
    ]
    magnitude_names = [
        "base_model.model.encoder.encoder.layer.0.attention.self.query_proj.lora_magnitude_vector.default.weight",
        "base_model.model.boundary_head.shared_pool_scorer.film.0.lora_magnitude_vector.default.weight",
    ]
    cases = []

    def add(name, *, selected=None, extra=(), use_lora=False, device="cpu", fused=False):
        all_names = parameter_names + list(extra)
        selected = set(all_names if selected is None else selected)
        if len(all_names) != len(set(all_names)) or not selected.issubset(all_names):
            raise oracle.ContractError("invalid synthetic parameter selection")
        parameters = [Parameter(key, key in selected) for key in all_names]
        model = SimpleNamespace(parameters=lambda: iter(parameters),
                                named_parameters=lambda: ((p.name, p) for p in parameters))
        options = {**config, "use_lora": use_lora, "fused_optimizer": fused}
        controller = SimpleNamespace(config=SimpleNamespace(**options), model=model,
                                     device=SimpleNamespace(type=device))
        case = dict(id=name, parameter_names=all_names,
                    trainable=[p.requires_grad for p in parameters], config=options, device_type=device)
        try:
            result = invoke(controller)
        except ValueError as exc:
            if str(exc) != "No LoRA parameters found. Check LoRA configuration.":
                raise
            case["expected_error"] = dict(type="ValueError", message=str(exc))
        else:
            case["expected"] = dict(groups=result.groups, optimizer_kwargs=result.kwargs)
        cases.append(case)

    add("full_cpu")
    add("head_only_cpu", selected=[name for name in parameter_names if not name.startswith("encoder.")])
    add("all_frozen_cpu", selected=[])
    add("lora_cpu", selected=adapter_names, extra=adapter_names, use_lora=True)
    add("dora_cpu", selected=adapter_names + magnitude_names, extra=adapter_names + magnitude_names, use_lora=True)
    add("lora_includes_every_trainable", selected=adapter_names + ["classifier.0.bias"],
        extra=adapter_names, use_lora=True)
    add("lora_no_trainable_error", selected=[], extra=adapter_names, use_lora=True)
    add("cpu_fused_request_uses_foreach", fused=True)
    add("cuda_fused_constructor_only", device="cuda", fused=True)
    add("cuda_nonfused_constructor_only", device="cuda")
    add("mps_constructor_only", device="mps", fused=True)
    return dict(
        format_version=1, scope="pinned_optimizer_parameter_grouping_without_torch",
        qualification=False, source_commit=oracle.UPSTREAM_COMMIT,
        provenance=dict(commit=checkout["commit"], python=platform.python_version(),
                        trainer_file="gliner2/training/trainer.py", trainer_sha256=oracle.sha256_file(trainer),
                        method="_create_optimizer", method_lines=[method.lineno, method.end_lineno],
                        method_sha256=hashlib.sha256(method_source.encode()).hexdigest(),
                        assertions_enabled=True),
        generator_sha256=oracle.sha256_file(Path(__file__)), oracle_sha256=oracle.sha256_file(Path(oracle.__file__)),
        inventory_files=inventories, cases=cases,
        notes=[
            "The exact pinned AST method executes unmodified; only named parameters, device metadata, logger, and the AdamW constructor are synthetic.",
            "No Torch import, parameter tensor allocation, checkpoint, optimizer update, or hardware operation occurs.",
            "Published parameter names are taken from all three verified 334-tensor inventories; the explicit sorted input order is recorded per case.",
            "Full training routes any name containing encoder to encoder_lr, including boundary_head.boundary_encoder and boundary_head.candidate_encoder.",
            "Head-only selection freezes only the top-level encoder subtree; learning-rate grouping still uses the upstream substring rule.",
            "The use_lora branch groups every requires_grad parameter at task_lr, including DoRA magnitude and any explicitly trainable non-adapter parameter.",
            "All groups retain the same configured weight decay; bias and normalization names are not exempted by this source method.",
            "Constructor flags for cuda and mps are symbolic source-branch coverage, not backend execution evidence.",
            "The separate training_adamw fixture covers actual Torch updates and None versus zero gradients.",
        ],
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=oracle.FIXTURES / "training_optimizer_groups.json")
    args = parser.parse_args()
    result = capture(args.upstream)
    payload = (json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n").encode()
    if len(payload) > 1024 * 1024:
        raise oracle.ContractError("optimizer grouping fixture byte budget exceeded")
    args.output.write_bytes(payload)
    print(f"wrote {args.output}: {len(result['cases'])} cases, SHA256 {oracle.sha256_file(args.output)}")


if __name__ == "__main__":
    main()
