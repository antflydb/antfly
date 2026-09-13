#!/usr/bin/env python3
"""Tiny pinned PEFT linear-module forward/VJP oracle with explicit masks.

No model is loaded. One encoder projection is called with two row counts, then
an adapted classifier consumes its first result. Masks use the native replay
ABI, not PyTorch RNG equivalence. Base weights and biases remain frozen.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import inspect
import json
from pathlib import Path
import platform


MASK64 = (1 << 64) - 1


def mix(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK64
    return value ^ (value >> 31)


def use_stream(module: str, occurrence: int) -> int:
    value = 0xCBF29CE484222325
    for byte in module.encode():
        value = ((value ^ byte) * 0x100000001B3) & MASK64
    return mix(value ^ mix((occurrence + 0x757365) & MASK64))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[2] / "testdata/gliner25/training_peft")
    args = parser.parse_args()
    import torch
    from peft.tuners.lora import dora, layer, variants
    from peft.tuners.lora.layer import Linear
    from safetensors.torch import save_file

    if importlib.metadata.version("peft") != "0.17.1":
        raise RuntimeError("PEFT oracle must use pinned 0.17.1")
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    torch.manual_seed(250926)
    weights = {}
    tensors = {}
    cases = []
    replay = dict(seed=1701, optimizer_step=2, micro_batch=3, replica=0)

    def capture(table, name, value):
        table[name] = value.detach().contiguous().clone()

    def wave(shape, frequency, offset, scale):
        return torch.sin(torch.arange(torch.tensor(shape).prod().item(), dtype=torch.float32) * frequency + offset).reshape(shape) * scale

    for kind in ("lora", "dora"):
        for probability in (0.0, 0.25):
            for mode in ("eval", "train"):
                case_id = f"{kind}_p{int(probability * 100):02d}_{mode}"
                modules = []
                case = dict(id=case_id, kind=kind, probability=probability, mode=mode, replay=replay, masks=[])
                for index, (native, canonical, in_dim, out_dim) in enumerate((
                    ("encoder.layer.0.attention.self.query_proj", "encoder.encoder.layer.0.attention.self.query_proj", 4, 3),
                    ("classifier.0", "classifier.0", 3, 2),
                )):
                    base = torch.nn.Linear(in_dim, out_dim, bias=True)
                    with torch.no_grad():
                        base.weight.copy_(wave((out_dim, in_dim), 0.31, 0.2 + index, 0.7))
                        base.bias.copy_(wave((out_dim,), 0.43, 0.7 + index, 0.3))
                    for parameter in base.parameters():
                        parameter.requires_grad_(False)
                    module = Linear(base, "default", r=2, lora_alpha=3, lora_dropout=probability, use_dora=kind == "dora")
                    with torch.no_grad():
                        module.lora_A["default"].weight.copy_(wave((2, in_dim), 0.53, 0.4 + index, 0.2))
                        module.lora_B["default"].weight.copy_(wave((out_dim, 2), 0.29, 0.8 + index, 0.3))
                        if kind == "dora":
                            norm = torch.linalg.norm(base.weight + 1.5 * module.lora_B["default"].weight @ module.lora_A["default"].weight, dim=1)
                            module.lora_magnitude_vector["default"].weight.copy_(norm * torch.linspace(0.7, 1.3, out_dim) + 0.11)
                    module.train(mode == "train")
                    occurrence = [0]

                    def dropout(value, *, module=module, canonical=canonical, occurrence=occurrence):
                        if not module.training:
                            return value
                        call = occurrence[0]
                        occurrence[0] += 1
                        stream = (mix(replay["seed"] ^ 0x73656564) ^ mix(replay["optimizer_step"] ^ 0x73746570) ^
                                  mix(replay["micro_batch"] ^ 0x6D6963726F) ^ mix(replay["replica"] ^ 0x7265706C696361) ^ use_stream(canonical, call))
                        p = torch.tensor(probability, dtype=torch.float32).item()
                        threshold = int(p * 4294967296.0)
                        scale = (torch.tensor(1.0, dtype=torch.float32) / (torch.tensor(1.0, dtype=torch.float32) - p)).item()
                        mask = torch.tensor([0.0 if (mix(stream ^ mix(element)) >> 32) < threshold else scale
                                             for element in range(value.numel())], dtype=torch.float32).reshape(value.shape)
                        key = f"{case_id}.mask.{canonical}.{call}"
                        capture(tensors, key, mask)
                        case["masks"].append(dict(module=canonical, occurrence=call, tensor=key))
                        return value * mask

                    if probability:
                        module.lora_dropout["default"].forward = dropout
                    capture(weights, f"{case_id}.{native}.weight", base.weight)
                    capture(weights, f"{case_id}.{native}.bias", base.bias)
                    for name, parameter in module.named_parameters():
                        if parameter.requires_grad:
                            capture(weights, f"{case_id}.base_model.model.{canonical}.{name}", parameter)
                    modules.append((module, canonical))

                x0 = wave((2, 4), 0.23, -0.3, 0.9).requires_grad_()
                x1 = wave((5, 4), 0.17, 0.6, 0.8).requires_grad_()
                capture(tensors, f"{case_id}.__peft_x0", x0)
                capture(tensors, f"{case_id}.__peft_x1", x1)
                y0 = modules[0][0](x0)
                y1 = modules[0][0](x1)
                classifier = modules[1][0](y0)
                loss = torch.tensor(0.0)
                for index, (name, output) in enumerate((("query0", y0), ("query1", y1), ("classifier0", classifier))):
                    cotangent = wave(tuple(output.shape), 0.37, 0.2 + index, 0.7)
                    capture(tensors, f"{case_id}.output.{name}", output)
                    capture(tensors, f"{case_id}.cotangent.{name}", cotangent)
                    loss = loss + (output * cotangent).sum()
                loss.backward()
                for name, value in (("__peft_x0", x0), ("__peft_x1", x1)):
                    capture(tensors, f"{case_id}.gradient.{name}", value.grad)
                for module, canonical in modules:
                    for name, parameter in module.named_parameters():
                        if parameter.requires_grad:
                            if parameter.grad is None:
                                raise RuntimeError(f"missing gradient {name}")
                            capture(tensors, f"{case_id}.gradient.base_model.model.{canonical}.{name}", parameter.grad)
                        elif parameter.grad is not None:
                            raise RuntimeError("frozen base parameter received gradient")
                case["loss"] = loss.item()
                if len(case["masks"]) != (3 if probability and mode == "train" else 0):
                    raise RuntimeError("per-use dropout capture mismatch")
                cases.append(case)

    args.output.mkdir(parents=True, exist_ok=True)
    save_file(weights, str(args.output / "weights.safetensors"))
    save_file(tensors, str(args.output / "tensors.safetensors"))
    sources = {name: hashlib.sha256(Path(inspect.getfile(module)).read_bytes()).hexdigest()
               for name, module in (("layer.py", layer), ("dora.py", dora), ("variants.py", variants))}
    files = {name: hashlib.sha256((args.output / name).read_bytes()).hexdigest()
             for name in ("weights.safetensors", "tensors.safetensors")}
    manifest = dict(format_version=1, rank=2, alpha=3, cases=cases,
                    provenance=dict(python=platform.python_version(), torch=importlib.metadata.version("torch"),
                                    peft=importlib.metadata.version("peft"), peft_source_sha256=sources,
                                    generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                                    mask_protocol="boundary_peft_site_element_v1", dtype="float32"), files_sha256=files)
    (args.output / "capture.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(dict(cases=len(cases), weights=len(weights), tensors=len(tensors), files_sha256=files)))


if __name__ == "__main__":
    main()
