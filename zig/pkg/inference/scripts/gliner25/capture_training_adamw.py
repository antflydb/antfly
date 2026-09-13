#!/usr/bin/env python3
"""Tiny Torch AdamW accumulation oracle, including pinned partial flush policy."""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import subprocess
import types


PIN = "3c913c7369301133d3b7699252074c4303ada50e"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[2] / "testdata/gliner25/training_adamw.json")
    args = parser.parse_args()
    actual = subprocess.check_output(["git", "-C", str(args.upstream), "rev-parse", "HEAD"], text=True).strip()
    if actual != PIN:
        raise RuntimeError("unexpected upstream revision")
    trainer = args.upstream / "gliner2/training/trainer.py"
    source = trainer.read_text()
    method = next(node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.FunctionDef) and node.name == "_renormalize_partial_accumulation")
    method_source = ast.get_source_segment(source, method)
    # Execute the pinned method itself on a tiny synthetic model/config. Its
    # source has no decorators/imports or external state beyond these fields.
    namespace = {}
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(trainer), "exec"), namespace)
    renormalize = namespace["_renormalize_partial_accumulation"]

    import torch
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    parameters = [
        dict(name="encoder.weight", shape=[2], initial=[0.4, -0.2], lr=0.01, weight_decay=0.1),
        dict(name="head.bias", shape=[1], initial=[0.1], lr=0.02, weight_decay=0.0),
        dict(name="adapter", shape=[2], initial=[0.3, -0.4], lr=0.03, weight_decay=0.05),
    ]
    values = {item["name"]: torch.nn.Parameter(torch.tensor(item["initial"], dtype=torch.float32)) for item in parameters}
    optimizer = torch.optim.AdamW([dict(params=[values[item["name"]]], lr=item["lr"], weight_decay=item["weight_decay"]) for item in parameters],
                                 betas=(0.9, 0.999), eps=1e-8, foreach=False, fused=False)
    model = types.SimpleNamespace(parameters=lambda: iter(values.values()))
    controller = types.SimpleNamespace(config=types.SimpleNamespace(gradient_accumulation_steps=2), model=model)
    microbatches = [
        dict(gradients={"encoder.weight": [3.0, -1.0], "head.bias": None, "adapter": [0.8, -0.6]}, flush=False),
        dict(gradients={"encoder.weight": [1.0, 0.5], "head.bias": [0.0], "adapter": None}, flush=True),
        dict(gradients={"encoder.weight": None, "head.bias": [1.7], "adapter": [0.0, 0.0]}, flush=True),
    ]
    flushes = []
    window = 0
    for index, microbatch in enumerate(microbatches):
        window += 1
        for name, gradient in microbatch["gradients"].items():
            if gradient is None:
                continue
            # Upstream trainer scales each loss by configured accumulation.
            contribution = torch.tensor(gradient, dtype=torch.float32) / 2
            parameter = values[name]
            if parameter.grad is None:
                parameter.grad = contribution
            else:
                parameter.grad.add_(contribution)
        if not microbatch["flush"]:
            continue
        renormalize(controller, window)
        before = {name: None if parameter.grad is None else parameter.grad.tolist() for name, parameter in values.items()}
        grad_norm = torch.nn.utils.clip_grad_norm_(values.values(), 0.7, foreach=False, error_if_nonfinite=True)
        clipped = {name: None if parameter.grad is None else parameter.grad.tolist() for name, parameter in values.items()}
        optimizer.step()
        after = {}
        for name, parameter in values.items():
            state = optimizer.state.get(parameter, {})
            after[name] = dict(weight=parameter.tolist(), step=int(state.get("step", torch.tensor(0)).item()),
                               exp_avg=state.get("exp_avg", torch.zeros_like(parameter)).tolist(),
                               exp_avg_sq=state.get("exp_avg_sq", torch.zeros_like(parameter)).tolist())
        flushes.append(dict(after_microbatch=index + 1, window_microbatches=window, partial_renormalization=2 / window,
                            gradients_before_clip=before, grad_norm=grad_norm.item(), gradients_after_clip=clipped, parameters=after))
        optimizer.zero_grad(set_to_none=True)
        window = 0
    if flushes[1]["parameters"]["encoder.weight"]["step"] != 1 or flushes[1]["parameters"]["adapter"]["step"] != 2:
        raise RuntimeError("absent versus zero gradients lost their distinct optimizer semantics")
    result = dict(format_version=1,
                  optimizer=dict(name="AdamW", betas=[0.9, 0.999], eps=1e-8, gradient_accumulation_steps=2,
                                 max_grad_norm=0.7, norm_type=2, clip_epsilon=1e-6, foreach=False, fused=False,
                                 partial_flush_policy="renormalize_to_actual_microbatch_count",
                                 legacy_difference="GLiNER2.5 upstream renormalizes a partial final window; legacy Antfly fixed-divisor accumulation is unchanged."),
                  parameters=parameters, microbatches=microbatches, flushes=flushes,
                  provenance=dict(python=platform.python_version(), torch=importlib.metadata.version("torch"), upstream_revision=PIN,
                                  trainer_file="gliner2/training/trainer.py", trainer_sha256=hashlib.sha256(trainer.read_bytes()).hexdigest(),
                                  partial_function="_renormalize_partial_accumulation", partial_function_lines=[method.lineno, method.end_lineno],
                                  partial_function_sha256=hashlib.sha256(method_source.encode()).hexdigest(),
                                  partial_call_line=1865, configured_loss_divisor_line=1217,
                                  generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), dtype="float32"))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(dict(path=str(args.output), flushes=len(flushes), sha256=hashlib.sha256(args.output.read_bytes()).hexdigest())))


if __name__ == "__main__":
    main()
