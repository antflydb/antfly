#!/usr/bin/env python3
"""Capture the exact DeBERTa encoder with externally replayed train dropout.

The masks are an explicit cross-runtime contract, not a claim that our counter
generator reproduces torch.manual_seed. No published weights are downloaded.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import inspect
import json
from pathlib import Path
import platform
import re


MASK64 = (1 << 64) - 1


def mix(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK64
    return value ^ (value >> 31)


def site_for(name: str) -> tuple[str, int, int]:
    if name == "embeddings.dropout":
        return "embeddings", 0, 0
    match = re.fullmatch(r"encoder\.layer\.(\d+)\.(.+)", name)
    if not match:
        raise ValueError(f"unexpected dropout site: {name}")
    names = {
        "attention.self.pos_dropout": ("relative_positions", 1),
        "attention.self.dropout": ("attention_probabilities", 2),
        "attention.output.dropout": ("attention_output", 3),
        "output.dropout": ("ffn_output", 4),
    }
    kind, index = names[match[2]]
    return kind, int(match[1]), index


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[2] / "testdata/gliner25/training_encoder")
    args = parser.parse_args()

    import torch
    import transformers.models.deberta_v2.modeling_deberta_v2 as implementation
    from safetensors.torch import save_file
    from transformers import DebertaV2Config, DebertaV2Model

    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    torch.manual_seed(250925)
    config = DebertaV2Config(
        vocab_size=64, hidden_size=16, intermediate_size=32,
        num_hidden_layers=2, num_attention_heads=4,
        max_position_embeddings=32, position_buckets=16,
        max_relative_positions=-1, relative_attention=True,
        position_biased_input=False, norm_rel_ebd="layer_norm",
        share_att_key=True, pos_att_type=["p2c", "c2p"],
        type_vocab_size=0, layer_norm_eps=1e-7,
        hidden_act="gelu", hidden_dropout_prob=0.1,
        attention_probs_dropout_prob=0.1, pad_token_id=0,
    )
    model = DebertaV2Model(config).float().cpu()
    tensors: dict[str, torch.Tensor] = {}
    cases = []
    active: dict[str, object] = {}

    def capture(name: str, value: torch.Tensor) -> None:
        tensors[name] = value.detach().contiguous().clone()

    def dropout_forward(name: str, module):
        kind, layer, kind_index = site_for(name)
        stream_id = (layer << 32) | (kind_index + 1)

        def apply(value):
            if not module.training:
                return value
            case = active["case"]
            prefix = active["prefix"]
            probability = torch.tensor(module.p, dtype=torch.float32).item()
            stream = (mix(case["seed"]) ^ mix(case["micro_batch"]) ^
                      mix((case["replica"] + 0x7265706C696361) & MASK64) ^ mix(stream_id))
            threshold = int(probability * 4294967296)
            scale = (torch.tensor(1.0, dtype=torch.float32) /
                     (torch.tensor(1.0, dtype=torch.float32) - probability)).item()
            values = [0.0 if (mix(stream ^ mix(index)) >> 32) < threshold else scale
                      for index in range(value.numel())]
            mask = torch.tensor(values, dtype=torch.float32).reshape(value.shape)
            key = f"{prefix}.dropout.{kind}.{layer}"
            if key in tensors:
                raise RuntimeError(f"dropout site unexpectedly repeated: {name}")
            capture(key, mask)
            case["dropouts"].append(dict(name=name, kind=kind, layer=layer, probability=probability, tensor=key))
            return value * mask

        return apply

    for name, module in model.named_modules():
        if isinstance(module, torch.nn.Dropout):
            module.forward = dropout_forward(name, module)

    def layer_hook(name):
        def hook(_module, _inputs, output):
            if isinstance(output, tuple):
                output = output[0]
            capture(f"{active['prefix']}.intermediate.{name}", output)
        return hook

    model.embeddings.register_forward_hook(layer_hook("embeddings"))
    for index, layer in enumerate(model.encoder.layer):
        layer.register_forward_hook(layer_hook(f"layer.{index}"))

    weights = {name: tensor.detach().contiguous().clone() for name, tensor in model.state_dict().items()}
    for name, batch, sequence, lengths in (("padded7", 2, 7, [7, 4]), ("buckets19", 1, 19, [19])):
        ids = (torch.arange(batch * sequence).reshape(batch, sequence) * 7 + 3) % 63 + 1
        mask = torch.zeros((batch, sequence), dtype=torch.long)
        for row, length in enumerate(lengths):
            mask[row, :length] = 1
            ids[row, length:] = 0
        capture(f"{name}.input_ids", ids.to(torch.int32))
        capture(f"{name}.attention_mask", mask.to(torch.int32))
        cotangent = torch.sin(torch.arange(batch * sequence * 16, dtype=torch.float32) * 0.13).reshape(batch, sequence, 16)
        cotangent *= mask.unsqueeze(-1)
        capture(f"{name}.cotangent", cotangent)
        case = dict(id=name, batch=batch, sequence=sequence, lengths=lengths,
                    seed=2509, micro_batch=3, replica=0, dropouts=[])
        active.update(case=case, prefix=f"{name}.eval")
        model.eval()
        with torch.no_grad():
            capture(f"{name}.eval.output", model(ids, attention_mask=mask).last_hidden_state)
        active["prefix"] = f"{name}.train"
        model.train()
        model.zero_grad(set_to_none=True)
        output = model(ids, attention_mask=mask).last_hidden_state
        capture(f"{name}.train.output", output)
        (output * cotangent).sum().backward()
        for parameter_name, parameter in model.named_parameters():
            if parameter.grad is None:
                raise RuntimeError(f"missing encoder gradient: {parameter_name}")
            capture(f"{name}.gradient.{parameter_name}", parameter.grad)
        if len(case["dropouts"]) != 9:
            raise RuntimeError("missing encoder dropout family")
        cases.append(case)

    args.output.mkdir(parents=True, exist_ok=True)
    save_file(weights, str(args.output / "weights.safetensors"))
    save_file(tensors, str(args.output / "tensors.safetensors"))
    source = Path(inspect.getfile(implementation))
    files = {name: hashlib.sha256((args.output / name).read_bytes()).hexdigest()
             for name in ("weights.safetensors", "tensors.safetensors")}
    manifest = dict(format_version=1, config=config.to_dict(), cases=cases,
                    provenance=dict(python=platform.python_version(), torch=importlib.metadata.version("torch"),
                                    transformers=importlib.metadata.version("transformers"),
                                    transformers_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                                    generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                                    mask_protocol="splitmix64_site_element_v1", dtype="float32"),
                    files_sha256=files)
    (args.output / "capture.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(dict(cases=len(cases), weights=len(weights), tensors=len(tensors), files_sha256=files)))


if __name__ == "__main__":
    main()
