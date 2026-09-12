#!/usr/bin/env python3
"""Actual mixed-task training with explicit encoder/head/PEFT dropout masks.

Reuse the immutable zero-dropout composition driver without editing it. Only
model dropout configuration and dropout operations are replaced. Targets,
selection, objective, backward, accumulation, AdamW and resume remain its exact
pinned source path. This does not claim equivalence to either runtime's RNG.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import math
from pathlib import Path
import re
import struct
from unittest.mock import patch

import capture_training_step as composition
import oracle

HELPER_SHA256 = "2ca6610037b8a357766a81c7a0002af59e6e433bfea60fb0d51cf1d5eebf831b"
PROBABILITY = 0.125
SCALE = struct.unpack("<f", struct.pack("<f", 1 / (1 - PROBABILITY)))[0]
MICROBATCHES = (0, 1, 2, 1, 2)
HEAD_PREFIX = "__gliner25.dropout."
ENCODER_PREFIX = "__gliner25.encoder.dropout."
MAX_MASK_BYTES = 12 * 1024 * 1024


def checked(condition, message):
    if not condition:
        raise oracle.ContractError(message)


def encoder_site(name):
    if name == "encoder.embeddings.dropout":
        return "embeddings.0"
    match = re.fullmatch(r"encoder\.encoder\.layer\.(\d+)\.(.+)", name)
    checked(match is not None, f"unexpected encoder dropout name: {name}")
    kinds = {"attention.self.pos_dropout": "relative_positions", "attention.self.dropout": "attention_probabilities",
             "attention.output.dropout": "attention_output", "output.dropout": "ffn_output"}
    checked(match[2] in kinds, f"unexpected encoder dropout site: {name}")
    return kinds[match[2]] + "." + match[1]


class Masks:
    def __init__(self, model, mode, torch):
        self.model, self.mode, self.torch = model, mode, torch
        self.calls = 0
        self.active = None
        self.reports = {}
        self.tensors = {}
        self.sdpa_check = None
        self.peft_modules = {}
        self.hooks = []
        self.hooks.append(model.register_forward_pre_hook(self.begin))
        self.hooks.append(model.register_forward_hook(self.finish))
        self.hooks.append(model.classifier.register_forward_pre_hook(self.classifier))
        self.hooks.append(model.relation_scorer.register_forward_pre_hook(self.relation))
        self.install()

    def mask(self, name, shape):
        active = self.active
        checked(active is not None, "dropout outside an active model forward")
        if name not in active["masks"]:
            count = math.prod(shape)
            checked(0 < count * 4 <= MAX_MASK_BYTES, "dropout mask exceeds declared budget")
            seed = int.from_bytes(hashlib.sha256(f"gliner25.composed-mask/v1/{active['micro']}/{name}".encode()).digest()[:4], "little")
            elements = self.torch.arange(count, dtype=self.torch.int64)
            values = self.torch.where((elements * 7 + seed) % 8 == 0, 0.0, SCALE).to(self.torch.float32).reshape(shape)
            active["masks"][name] = values
            active["routes"][name] = []
        value = active["masks"][name]
        checked(list(value.shape) == list(shape), f"dropout shape changed: {name}")
        return value

    def begin(self, _module, inputs):
        checked(self.active is None and self.calls < len(MICROBATCHES), "unexpected or nested mixed-step forward")
        batch = inputs[0]
        micro = MICROBATCHES[self.calls]
        self.calls += 1
        b, s = batch.input_ids.shape
        w, q, cls = batch.text_word_mask.shape[1], batch.query_marker_mask.shape[1], batch.cls_marker_mask.shape[1]
        checked(b == 2 and s <= 128 and w <= 32 and q <= 32, "dropout forward exceeds tiny dimensions")
        self.active = {"micro": micro, "batch": batch, "masks": {}, "routes": {}, "visits": {},
                       "classifier_rows": None, "relation_rows": None, "relation_routes": None}
        e, h = self.model.encoder.config, self.model.boundary_settings
        self.mask(ENCODER_PREFIX + "embeddings.0", [b * s, e.hidden_size])
        for layer in range(e.num_hidden_layers):
            for kind, shape in (("relative_positions", [2 * e.position_buckets, e.hidden_size]),
                                ("attention_probabilities", [b * e.num_attention_heads, s, s]),
                                ("attention_output", [b * s, e.hidden_size]), ("ffn_output", [b * s, e.hidden_size])):
                self.mask(ENCODER_PREFIX + f"{kind}.{layer}", shape)
        n, d, c = w + 1, h.boundary_dim, h.pool_size
        sites = {"boundary_encoder.output": [b * n, d],
                 "boundary_encoder.attention.0.probabilities": [b * h.boundary_attention_heads, n, n],
                 "boundary_encoder.attention.0.output": [b * n, d],
                 "boundary_encoder.refinement.0.hidden": [b * n, int(d * h.boundary_ffn_multiplier)],
                 "boundary_encoder.refinement.0.output": [b * n, d],
                 "marginals.start": [b * n, d], "marginals.end": [b * n, d], "marginals.inside": [b * w, d],
                 "boundary_head.shared_pool_scorer.content_pooler": [b * c, h.content_dim],
                 "shared_pool.film_hidden": [b * c * q, 64]}
        if cls:
            sites["classifier"] = [b * cls, 2 * e.hidden_size]
        relation_count = sum(task == "relations" for tasks in batch.task_types for task in tasks)
        if relation_count:
            sites["relations.hidden"] = [relation_count * h.relation_pair_cap, e.hidden_size]
        for site, shape in sites.items():
            self.mask(HEAD_PREFIX + site, shape)
        for name, module in self.peft_modules.items():
            self.mask(f"__boundary_peft_mask.{name}.0", [b * s, module.in_features])
            if name.endswith((".query_proj", ".key_proj")):
                self.mask(f"__boundary_peft_mask.{name}.1", [2 * e.position_buckets, module.in_features])

    def visit(self, module):
        checked(self.active is not None, "dropout outside model forward")
        calls = self.active["visits"]
        occurrence = calls.get(module, 0)
        calls[module] = occurrence + 1
        return occurrence

    def apply(self, name, module, value):
        checked(module.training and module.p == PROBABILITY and not module.inplace, f"dropout configuration differs: {name}")
        call = self.visit(name)
        if name.startswith("encoder.") and ".lora_dropout." not in name:
            native = ENCODER_PREFIX + encoder_site(name)
        elif ".lora_dropout." in name:
            canonical = name.split(".lora_dropout.")[0]
            native = f"__boundary_peft_mask.{canonical}.{call}"
        else:
            routes = {
                "boundary_head.boundary_encoder.dropout": ["boundary_encoder.output"],
                "boundary_head.boundary_encoder.attention_blocks.0.dropout": ["boundary_encoder.attention.0.output"],
                "boundary_head.boundary_encoder.refinement_blocks.0.dropout": ["boundary_encoder.refinement.0.hidden", "boundary_encoder.refinement.0.output"],
                "boundary_head.boundary_query_head.dropout": ["marginals.start", "marginals.end", "marginals.inside"],
                "boundary_head.shared_pool_scorer.content_pooler.dropout": ["boundary_head.shared_pool_scorer.content_pooler"],
                "boundary_head.shared_pool_scorer.film_output.2": ["shared_pool.film_hidden"],
                "classifier.2": ["classifier"], "relation_scorer.mlp.2": ["relations.hidden"],
            }
            checked(name in routes, f"unexpected live head dropout: {name}")
            route = 0 if name == "classifier.2" else call
            checked(route < len(routes[name]), f"head dropout repeated: {name}")
            native = HEAD_PREFIX + routes[name][route]
        checked(native in self.active["masks"], f"undeclared dropout mask: {native}")
        mask = self.active["masks"][native]
        route = {"module": name, "occurrence": call, "source_shape": list(value.shape)}
        if name == "classifier.2":
            rows = self.active["classifier_rows"]
            checked(rows is not None and len(rows) == value.shape[0], "missing classification dropout routing")
            source_mask = mask[rows]
            route["native_rows"] = rows
        elif name == "relation_scorer.mlp.2":
            rows = self.active["relation_rows"]
            checked(rows is not None and len(rows) == value.shape[0], "missing relation dropout routing")
            # Invalid source proposal rows have zero output cotangents. Give
            # them explicit finite masks, then map each retained valid pair
            # to its compact native row without changing pair membership.
            source_mask = self.torch.full(value.shape, SCALE, dtype=self.torch.float32)
            for source_row, native_row in enumerate(rows):
                if native_row >= 0:
                    source_mask[source_row] = mask[native_row]
            route["native_rows"] = rows
        else:
            checked(mask.numel() == value.numel(), f"dropout element count differs: {name}")
            source_mask = mask.reshape(value.shape)
        checked(source_mask.shape == value.shape, f"dropout shape differs: {name}")
        self.active["routes"][native].append(route)
        return value * source_mask

    def install(self):
        for name, module in self.model.named_modules():
            if hasattr(module, "lora_dropout"):
                self.peft_modules[name] = module
            if isinstance(module, self.torch.nn.Dropout) and not getattr(module, "_gliner25_explicit_mask", False):
                module._gliner25_explicit_mask = True
                module.forward = lambda value, name=name, module=module: self.apply(name, module, value)

    def classifier(self, _module, inputs):
        value, batch = inputs[0], self.active["batch"]
        core = self.active.get("core")
        checked(core is not None, "classification ran before encoder routing")
        matches = []
        for b, specifications in enumerate(core["cls_specs"]):
            offset = 0
            for spec in specifications:
                choices = spec["choice_states"]
                count = choices.shape[0]
                if choices.data_ptr() == value.data_ptr() and choices.shape == value.shape:
                    matches.append(list(range(b * batch.cls_marker_mask.shape[1] + offset,
                                              b * batch.cls_marker_mask.shape[1] + offset + count)))
                offset += count
        checked(len(matches) == 1, "classification mask has ambiguous source rows")
        self.active["classifier_rows"] = matches[0]

    def relation(self, _module, inputs):
        pairs = inputs[3]
        mask = self.active["masks"][HEAD_PREFIX + "relations.hidden"]
        routes, metadata, count = [], [], 0
        for index, valid in enumerate(pairs.pair_mask.tolist()):
            routes.append(count if valid else -1)
            if valid:
                metadata.append({name: int(getattr(pairs, name)[index]) for name in
                                 ("batch_index", "relation_index", "head_start", "head_end", "tail_start", "tail_end")})
                count += 1
        checked(count <= mask.shape[0], "compact relation rows exceed native capacity")
        self.active["relation_rows"], self.active["relation_routes"] = routes, metadata

    def sdpa(self, original, query, key, value, attn_mask=None, dropout_p=0.0, is_causal=False, scale=None, enable_gqa=False):
        checked(self.active is not None and not is_causal and not enable_gqa and attn_mask is not None and
                attn_mask.dtype == self.torch.bool and dropout_p == PROBABILITY, "unexpected composed boundary SDPA")
        checked(query.ndim == 4 and query.shape == key.shape == value.shape, "unexpected boundary SDPA shape")
        native = HEAD_PREFIX + "boundary_encoder.attention.0.probabilities"
        checked(not self.active["routes"][native], "boundary SDPA repeated")
        scale = query.shape[-1] ** -0.5 if scale is None else scale
        def expanded(q, k, v):
            probabilities = self.torch.softmax((q @ k.transpose(-2, -1) * scale).masked_fill(~attn_mask, -self.torch.inf), dim=-1)
            return probabilities, probabilities @ v
        if self.sdpa_check is None:
            leaves = [item.detach().clone().requires_grad_() for item in (query, key, value)]
            genuine = original(*leaves, attn_mask=attn_mask, dropout_p=0.0, scale=scale)
            _, explicit = expanded(*leaves)
            cotangent = self.torch.sin(self.torch.arange(genuine.numel(), dtype=self.torch.float32) * .13).reshape(genuine.shape)
            actual = self.torch.autograd.grad(genuine, leaves, cotangent)
            expected = self.torch.autograd.grad(explicit, leaves, cotangent)
            deltas = [float((genuine - explicit).detach().abs().max())] + [float((a - b).detach().abs().max()) for a, b in zip(actual, expected)]
            checked(max(deltas) < 2e-5, "expanded zero-dropout SDPA differs from source forward/VJP")
            self.sdpa_check = {"source_shape": list(query.shape), "maximum_absolute_differences_forward_q_k_v": deltas}
        probabilities, _ = expanded(query, key, value)
        mask = self.active["masks"][native]
        checked(mask.numel() == probabilities.numel(), "boundary attention mask shape differs")
        self.active["routes"][native].append({"module": "boundary_head.boundary_encoder.attention_blocks.0.sdpa",
            "occurrence": 0, "source_shape": list(probabilities.shape)})
        return (probabilities * mask.reshape(probabilities.shape)) @ value

    def finish(self, _module, _inputs, _output):
        active = self.active
        checked(active is not None, "missing composed dropout capture")
        micro = active["micro"]
        entries = []
        for name, mask in active["masks"].items():
            routes = active["routes"][name]
            # The native graph has classification rows even when the entire
            # task is unsupervised. The exact source skips that classifier.
            checked(bool(routes) or name == HEAD_PREFIX + "classifier", f"required live dropout site was not consumed: {name}")
            key = f"{self.mode}.micro{micro}.dropout.{name}"
            if key in self.tensors:
                checked(self.torch.equal(self.tensors[key], mask), f"resumed dropout mask changed: {name}")
            else:
                self.tensors[key] = mask.detach().contiguous().clone()
            entries.append({"name": name, "shape": list(mask.shape), "probability": PROBABILITY,
                            "tensor": key, "source_calls": routes})
        evidence = {"masks": entries, "compact_relation_rows": active["relation_routes"]}
        if micro in self.reports:
            checked(self.reports[micro] == evidence, "resumed source mask routes changed")
        else:
            self.reports[micro] = evidence
        checked(sum(value.numel() * value.element_size() for value in self.tensors.values()) <= MAX_MASK_BYTES,
                "composed mask capture exceeds budget")
        self.active = None


def capture(source, output, modes):
    checked(oracle.sha256_file(Path(composition.__file__)) == HELPER_SHA256, "composition helper identity differs")
    provenance, torch = oracle.prepare_runtime(source)
    import gliner2
    import torch.nn.functional as functional
    constructor, original_sdpa = gliner2.BoundaryExtractor, functional.scaled_dot_product_attention
    save_tensors, write_json = oracle.save_tensors, oracle.write_json
    managers = []

    def factory(config, **kwargs):
        checked(len(managers) < len(modes), "unexpected composed model construction")
        config.boundary_head["dropout"] = PROBABILITY
        kwargs["encoder_config"].hidden_dropout_prob = PROBABILITY
        kwargs["encoder_config"].attention_probs_dropout_prob = PROBABILITY
        model = constructor(config, **kwargs)
        masks = Masks(model, modes[len(managers)], torch)
        managers.append(masks)
        encode, apply_lora = model._encode_core, model.apply_lora
        def routed(*args, **options):
            result = encode(*args, **options)
            checked(masks.active is not None, "encoded outside masked model forward")
            masks.active["core"] = result
            return result
        def adapted(*args, **options):
            options["dropout"] = PROBABILITY
            result = apply_lora(*args, **options)
            masks.install()
            return result
        model._encode_core, model.apply_lora = routed, adapted
        return model

    def sdpa(*args, **kwargs):
        active = [manager for manager in managers if manager.active is not None]
        checked(len(active) == 1, "boundary SDPA has no unique active model")
        return active[0].sdpa(original_sdpa, *args, **kwargs)

    def implicit_dropout(input, p=0.5, training=True, inplace=False):
        checked(not training or p == 0, "uncontrolled functional dropout in composed capture")
        return input

    tensor_aliases = {}

    def save(path, tensors, runtime):
        checked(len(managers) == len(modes) and all(manager.calls == 5 and manager.active is None for manager in managers),
                "composed mask capture did not complete every forward/resume")
        combined = dict(tensors)
        for manager in managers:
            checked(not (combined.keys() & manager.tensors.keys()), "duplicate explicit mask tensor")
            combined.update(manager.tensors)
        checked(sum(value.numel() * value.element_size() for value in combined.values()) <= 16 * 1024 * 1024,
                "composed dropout fixture exceeds 16 MiB")
        unique, aliases = composition.deduplicate_tensors(combined)
        tensor_aliases.update(aliases)
        return save_tensors(path, unique, runtime)

    def write(path, value):
        if path.name == "capture.json":
            value = composition.expand_metadata(value)
            value["scope"] = "synthetic_explicit_dropout_mixed_task_training_step"
            value["generator_sha256"] = oracle.sha256_file(Path(__file__))
            value["composition_helper_sha256"] = HELPER_SHA256
            value["settings"]["dropout"] = PROBABILITY
            value["dropout"] = {"probability": PROBABILITY, "peft_probability": PROBABILITY,
                "protocol": "explicit_named_f32_inverted_masks/v1", "rng_equivalence_claim": False,
                "native_binding": "complete finalized Plan.dropoutDescriptors set; shape and probability must match",
                "relation_rows": "source valid pair order compacts to native prefix; padded rows have zero logit cotangents",
                "classification_rows": "source per-task choices map to padded prepared classification-marker row order"}
            for profile, manager in zip(value["profiles"], managers):
                checked(profile["mode"] == manager.mode, "composed mask profile order differs")
                profile["zero_dropout_sdpa_check"] = manager.sdpa_check
                for micro in profile["microbatches"]:
                    # Live route/resume checks already consumed the diagnostic
                    # source calls. Native consumers need the exact mask bindings.
                    micro["explicit_dropout"] = {"masks": [
                        {key: item for key, item in mask.items() if key != "source_calls"}
                        for mask in manager.reports[micro["microbatch"]]["masks"]]}
            for name in ("encoding.py", "heads.py", "pool.py", "content.py"):
                path_in_source = "gliner2/models/boundary/" + name
                value["source_files"][path_in_source] = oracle.sha256_file(source / path_in_source)
            value["notes"][1] = "model.train() executes real auxiliary losses with encoder/head/PEFT dropout .125 replaced only by explicit captured inverted masks."
            value["notes"].append("The immutable composition driver supplies all objectives, gradients, AdamW and resume checks unchanged. Source classifier task slices and valid relation pair rows determine mask routing; gold never chooses mask bits.")
            value["notes"].append("The boundary SDPA probability dropout is expanded; zero-dropout forward and Q/K/V gradients are checked against genuine SDPA on the same actual activation shape.")
            value = composition.consolidate_metadata(composition.resolve_tensor_aliases(value, tensor_aliases))
        return write_json(path, value)

    with ExitStack() as stack:
        stack.enter_context(patch.object(oracle, "prepare_runtime", lambda actual: (provenance, torch) if actual == source else
                                        (_ for _ in ()).throw(oracle.ContractError("source checkout changed"))))
        stack.enter_context(patch.object(gliner2, "BoundaryExtractor", factory))
        stack.enter_context(patch.object(functional, "scaled_dot_product_attention", sdpa))
        stack.enter_context(patch.object(functional, "dropout", implicit_dropout))
        stack.enter_context(patch.object(oracle, "save_tensors", save))
        stack.enter_context(patch.object(oracle, "write_json", write))
        with oracle.atomic_output_directory(output) as staging:
            payload = staging / "payload"
            composition.capture(source, payload, modes)
            for file in payload.iterdir():
                checked(file.is_file(), "unexpected composed fixture directory")
                file.rename(staging / file.name)
            payload.rmdir()
    return {"profiles": modes, "capture_sha256": oracle.sha256_file(output / "capture.json"),
            "tensor_bytes": (output / "tensors.safetensors").stat().st_size, "qualification": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--modes", nargs="+", choices=("full", "heads", "lora", "dora"), default=["full", "heads", "lora", "dora"])
    args = parser.parse_args()
    checked(len(args.modes) == len(set(args.modes)) and not __import__("sys").flags.optimize,
            "distinct modes and enabled upstream assertions are required")
    print(capture(args.upstream, args.output_dir, args.modes))


if __name__ == "__main__":
    main()
