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

"""Independent official CPU forwards with streamed QAT weights and lazy PLE rows."""

import argparse
import hashlib
import json
import math
import mmap
from pathlib import Path

import numpy as np
import torch
from gemma4_inputs import prepare, processor
from gemma4_reference import (
    load_reference_model,
    official_source_identity,
    reference_weight,
)
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
        return reference_weight(spec["name"], result.reshape(spec["shape"]))

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


def teacher_forced_inputs(inputs, tokens, eos_ids):
    """Predict the final token before stopping, without exposing that label."""
    if not tokens or any(
        type(token) is not int or not 0 <= token < 262144 for token in tokens
    ):
        raise ValueError("Invalid native continuation")
    step = next(
        (i + 1 for i, token in enumerate(tokens) if token in eos_ids), len(tokens)
    )
    result = dict(inputs)
    shape = inputs["input_ids"].shape
    if len(shape) != 2 or shape[0] != 1:
        raise ValueError("Expected one reference prompt")
    for key, fill in (
        ("input_ids", None),
        ("attention_mask", 1),
        ("mm_token_type_ids", 0),
    ):
        if key not in inputs:
            continue
        if inputs[key].shape != shape:
            raise ValueError("Invalid aligned sequence input: " + key)
        tail = (
            torch.tensor([tokens[: step - 1]], dtype=inputs[key].dtype)
            if fill is None
            else torch.full((1, step - 1), fill, dtype=inputs[key].dtype)
        )
        result[key] = torch.cat((inputs[key], tail), dim=1)
    return result, step, tokens[step - 1]


def decision_checks(logits, tokens):
    """Keep intermediate failures visible even when both endpoints match."""
    if logits.ndim != 2 or logits.shape[0] != len(tokens):
        raise ValueError("Incomplete native decision coverage")
    expected = np.asarray(tokens, dtype=np.int64)
    predictions = logits.argmax(axis=-1)
    matches = predictions == expected
    gaps = logits.max(axis=-1) - logits[np.arange(len(tokens)), expected]
    return {
        "argmax": predictions.tolist(),
        "native_tokens": tokens,
        "argmax_matches": bool(matches.all()),
        "mismatch_steps": (np.flatnonzero(~matches) + 1).tolist(),
        "reference_argmax_gaps": gaps.tolist(),
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "processor", "suite", "gguf", "mmproj", "out"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--only", action="append", default=[])
    p.add_argument("--capture-ple-input", action="store_true")
    p.add_argument(
        "--native-tokens",
        type=Path,
        help="Teacher-force a saved native result through its first EOS or final budget token",
    )
    p.add_argument(
        "--all-native-steps",
        action="store_true",
        help="Check every native decision through EOS; save decision diagnostics without large logit files",
    )
    a = p.parse_args()
    if a.all_native_steps and not a.native_tokens:
        p.error("--all-native-steps requires --native-tokens")
    a.out.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(2)
    config = json.loads(a.config.read_text())
    if (
        a.all_native_steps
        and config["text_config"].get("use_bidirectional_attention") is not None
    ):
        raise ValueError("All-step checking requires causal text attention")
    model = load_reference_model(config)
    identity = official_source_identity()
    identity["logit_position"] = (
        "teacher_forced_all_through_eos"
        if a.all_native_steps
        else "teacher_forced_last_before_eos"
        if a.native_tokens
        else "first_step"
    )
    native_rows = None
    if a.native_tokens:
        raw = a.native_tokens.read_bytes()
        identity["native_tokens_sha256"] = hashlib.sha256(raw).hexdigest()
        native_rows = json.loads(raw)["rows"]
    (a.out / "reference.json").write_text(json.dumps(identity, indent=2) + "\n")
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
    active_case = [None]
    if a.capture_ple_input:
        (a.out / "stages").mkdir()

        def capture_input(module, inputs):
            values = inputs[0].detach().float().numpy()
            values.tofile(a.out / "stages" / (active_case[0] + ".f32"))
            (a.out / "stages" / (active_case[0] + ".json")).write_text(
                json.dumps({"shape": list(values.shape)}) + "\n"
            )

        hooks.append(
            text.per_layer_model_projection.register_forward_pre_hook(capture_input)
        )
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
    generation_checks = []
    for case in suite["cases"]:
        if a.only and case["id"] not in a.only:
            continue
        print("CPU case", case["id"], flush=True)
        active_case[0] = case["id"]
        inputs, meta = prepare(proc, case, a.suite.parent)
        # The wire protocol uses i64 masks, but the official model indexes the
        # projected audio batch with this mask and requires boolean semantics.
        if "input_features_mask" in inputs:
            inputs["input_features_mask"] = inputs["input_features_mask"].bool()
        expected = None
        keep = 1
        if native_rows is not None:
            rows = [row for row in native_rows if row["id"] == case["id"]]
            if not rows or any(row["tokens"] != rows[0]["tokens"] for row in rows):
                raise ValueError("Missing or nondeterministic native continuation")
            tokens = rows[0]["tokens"]
            if len(tokens) != case["max_tokens"] or any(
                row["prompt_tokens"] != inputs["input_ids"].shape[1] for row in rows
            ):
                raise ValueError("Native/reference input or budget differs")
            eos_ids = config["eos_token_id"]
            if isinstance(eos_ids, int):
                eos_ids = [eos_ids]
            prompt_length = inputs["input_ids"].shape[1]
            inputs, step, expected = teacher_forced_inputs(inputs, tokens, eos_ids)
            meta["teacher_forced_step"] = step
            if a.all_native_steps:
                keep = torch.arange(prompt_length - 1, prompt_length + step - 1)
        with torch.inference_mode():
            output = model(**inputs, use_cache=False, logits_to_keep=keep)
        logits = (
            output.logits.float().numpy()
            if a.all_native_steps
            else output.logits[:, -1].float().numpy()
        )
        wanted_shape = (1, step, 262144) if a.all_native_steps else (1, 262144)
        if logits.shape != wanted_shape or not np.isfinite(logits).all():
            raise ValueError("Invalid CPU logits")
        if not a.all_native_steps:
            logits.tofile(a.out / (case["id"] + ".f32"))
        info = {
            "id": case["id"],
            "argmax": int(logits.argmax()) if not a.all_native_steps else None,
            "shape": list(logits.shape),
            "precision": "FP32 CPU from same dequantized QAT GGUF",
            "metadata": meta,
        }
        if expected is not None:
            if a.all_native_steps:
                info.update(decision_checks(logits[0], tokens[:step]))
            else:
                info.update(
                    native_token=expected,
                    argmax_matches=int(logits.argmax()) == expected,
                    reference_argmax_gap=float(logits.max() - logits[0, expected]),
                )
            generation_checks.append(info)
        (a.out / (case["id"] + ".json")).write_text(json.dumps(info, indent=2) + "\n")
        print(info, flush=True)
    for hook in hooks:
        hook.remove()
    if a.native_tokens:
        if not generation_checks:
            raise ValueError("No generation checks")
        passed = all(row["argmax_matches"] for row in generation_checks)
        (a.out / "generation-check.json").write_text(
            json.dumps(
                {
                    "scope": "all native greedy decisions under teacher forcing through first EOS or fixed budget"
                    if a.all_native_steps
                    else "teacher-forced final native decision through first EOS or fixed budget; not free-running generation parity",
                    "checked_decisions": sum(
                        len(row["native_tokens"]) if a.all_native_steps else 1
                        for row in generation_checks
                    ),
                    "reference": identity,
                    "passed": passed,
                    "rows": generation_checks,
                },
                indent=2,
            )
            + "\n"
        )
        return 0 if passed else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
