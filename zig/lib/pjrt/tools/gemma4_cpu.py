# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Independent official CPU forwards with streamed QAT weights and lazy PLE rows."""

import argparse
import json
import math
import mmap
from pathlib import Path

import numpy as np
import torch
from gemma4_graph import load_multimodal_meta
from gemma4_inputs import prepare, processor
from gemma4_weights import parameter_specs
from gguf import GGML_QUANT_SIZES, GGUFReader, dequantize


class Weights:
    def __init__(self, gguf, mmproj):
        self.readers = {
            "gguf": GGUFReader(str(gguf)),
            "mmproj": GGUFReader(str(mmproj)),
        }
        self.tensors = {
            key: {t.name: t for t in reader.tensors}
            for key, reader in self.readers.items()
        }

    def discard(self):
        for reader in self.readers.values():
            try:
                reader.data._mmap.madvise(mmap.MADV_DONTNEED)
            except AttributeError:
                pass

    def load(self, spec):
        if "data" in spec:
            return torch.tensor(spec["data"], dtype=torch.float32).reshape(
                spec["shape"]
            )
        tensor = self.tensors[spec["file"]][spec["source"]]
        size = math.prod(spec["shape"])
        result = torch.empty(size, dtype=torch.float32)
        block, block_bytes = GGML_QUANT_SIZES[tensor.tensor_type]
        data = tensor.data.reshape(-1)
        step = 1 << 22
        if block != 1:
            data = data.view(np.uint8)
            step = (step // block) * block_bytes
        offset = 0
        for start in range(0, len(data), step):
            values = dequantize(data[start : start + step], tensor.tensor_type).reshape(
                -1
            )
            result[offset : offset + len(values)].copy_(torch.from_numpy(values.copy()))
            offset += len(values)
        if offset != size:
            raise ValueError("Dequantized size mismatch")
        if spec.get("transform") == "patch_nchw_to_nhwc":
            result = (
                result.reshape(spec["source_shape"]).permute(0, 2, 3, 1).contiguous()
            )
        elif spec.get("transform"):
            raise ValueError("Unsupported transform")
        if size * 4 >= 1 << 20:
            self.discard()
        return result.reshape(spec["shape"])

    def rows(self, spec, indices):
        tensor = self.tensors[spec["file"]][spec["source"]]
        rows, columns = spec["shape"]
        unique, inverse = np.unique(
            indices.detach().cpu().numpy().reshape(-1), return_inverse=True
        )
        if np.any(unique < 0) or np.any(unique >= rows):
            raise ValueError("Row index out of range")
        raw = tensor.data.reshape(rows, -1)[unique].copy()
        values = dequantize(raw, tensor.tensor_type).reshape(len(unique), columns)
        result = torch.from_numpy(values[inverse].copy()).reshape(
            *indices.shape, columns
        )
        self.discard()
        return result


class RowProxy:
    dtype = torch.float32
    device = torch.device("cpu")

    def __init__(self, weights, spec):
        self.weights, self.spec = weights, spec
        self.shape = torch.Size(spec["shape"])

    def __getitem__(self, index):
        if isinstance(index, tuple):
            row, columns = index
            return self.weights.rows(
                self.spec, torch.as_tensor(row, dtype=torch.int64)
            )[..., columns]
        return self.weights.rows(self.spec, torch.as_tensor(index, dtype=torch.int64))


class LazyEmbedding(torch.nn.Module):
    def __init__(self, weights, spec, scale):
        super().__init__()
        self.weight = RowProxy(weights, spec)
        self.scale = scale

    def forward(self, ids):
        return self.weight.weights.rows(self.weight.spec, ids) * self.scale


class StreamedHead(torch.nn.Module):
    def __init__(self, weights, spec):
        super().__init__()
        self.weights, self.spec = weights, spec

    def forward(self, hidden):
        rows = self.spec["shape"][0]
        result = torch.empty((*hidden.shape[:-1], rows), dtype=torch.float32)
        for start in range(0, rows, 1024):
            stop = min(rows, start + 1024)
            matrix = self.weights.rows(
                self.spec, torch.arange(start, stop, dtype=torch.int64)
            )
            result[..., start:stop] = torch.nn.functional.linear(hidden.float(), matrix)
        return result


def assign(model, name, value):
    parent, leaf = name.rsplit(".", 1)
    module = model.get_submodule(parent)
    if leaf in module._parameters:
        setattr(module, leaf, torch.nn.Parameter(value, requires_grad=False))
    elif leaf in module._buffers:
        setattr(module, leaf, value)
    else:
        raise ValueError(("Unknown model member", name))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "processor", "suite", "gguf", "mmproj", "out"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--only", action="append", default=[])
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(2)
    model = load_multimodal_meta(json.loads(a.config.read_text()))
    specs = parameter_specs(model, a.gguf, a.mmproj)
    by_name = {s["name"]: s for s in specs}
    weights = Weights(a.gguf, a.mmproj)
    prefix = "model.language_model.layers."
    suite = json.loads(a.suite.read_text())
    selected = [c for c in suite["cases"] if not a.only or c["id"] in a.only]
    need_vision = any(c.get("kind") in ("image", "video") for c in selected)
    need_audio = any(c.get("kind") == "audio" for c in selected)
    for spec in specs:
        if not need_vision and spec["name"].startswith(
            ("model.vision_tower.", "model.embed_vision.")
        ):
            continue
        if not need_audio and spec["name"].startswith(
            ("model.audio_tower.", "model.embed_audio.")
        ):
            continue
        if spec["name"].startswith(prefix) or spec["name"] in (
            "model.language_model.embed_tokens.weight",
            "model.language_model.embed_tokens_per_layer.weight",
            "lm_head.weight",
        ):
            continue
        assign(model, spec["name"], weights.load(spec))
    text = model.model.language_model
    token = by_name["model.language_model.embed_tokens.weight"]
    ple = by_name["model.language_model.embed_tokens_per_layer.weight"]
    token_scale = float(text.embed_tokens.embed_scale)
    ple_scale = float(text.embed_tokens_per_layer.embed_scale)
    text.embed_tokens = LazyEmbedding(weights, token, token_scale)
    text.embed_tokens_per_layer = LazyEmbedding(weights, ple, ple_scale)
    model.lm_head = StreamedHead(weights, by_name.get("lm_head.weight", token))
    hooks = []
    for i, layer in enumerate(text.layers):
        group = [s for s in specs if s["name"].startswith(f"{prefix}{i}.")]

        def before(module, args, group=group, i=i):
            if i % 6 == 0:
                print("CPU decoder layer", i, flush=True)
            for spec in group:
                assign(model, spec["name"], weights.load(spec))

        def after(module, args, output, group=group):
            for spec in group:
                assign(
                    model,
                    spec["name"],
                    torch.empty(spec["shape"], device="meta", dtype=torch.float32),
                )
            weights.discard()

        hooks.extend(
            (
                layer.register_forward_pre_hook(before),
                layer.register_forward_hook(after),
            )
        )
    print("CPU media/constants ready", flush=True)
    suite = json.loads(a.suite.read_text())
    proc = processor(a.processor)
    for case in suite["cases"]:
        if a.only and case["id"] not in a.only:
            continue
        print("CPU case", case["id"], flush=True)
        inputs, meta = prepare(proc, case, a.suite.parent)
        # The wire protocol uses i64 masks, but the official model indexes the
        # projected audio batch with this mask and requires boolean semantics.
        if "input_features_mask" in inputs:
            inputs["input_features_mask"] = inputs["input_features_mask"].bool()
        with torch.inference_mode():
            output = model(**inputs, use_cache=False, logits_to_keep=1)
        logits = output.logits[:, -1].float().numpy()
        if logits.shape != (1, 262144) or not np.isfinite(logits).all():
            raise ValueError("Invalid CPU logits")
        logits.tofile(a.out / (case["id"] + ".f32"))
        info = {
            "id": case["id"],
            "argmax": int(logits.argmax()),
            "shape": list(logits.shape),
            "precision": "FP32 CPU from same dequantized QAT GGUF",
            "metadata": meta,
        }
        (a.out / (case["id"] + ".json")).write_text(json.dumps(info, indent=2) + "\n")
        print(info, flush=True)
    for hook in hooks:
        hook.remove()


if __name__ == "__main__":
    main()
