#!/usr/bin/env python3
"""Pinned DeBERTa attention methods with five independent projected-input VJPs.

Only projection returns and dropout masks are substituted for the leaf replay.
Attention scores, relative gathers, masking, softmax, context and backward are
the installed pinned Transformers methods. No checkpoint or tokenizer is loaded.
The explicit counter masks do not reproduce PyTorch's random-number stream.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import ExitStack
import hashlib
import importlib.metadata
import inspect
import json
import math
from pathlib import Path
import struct
import sys
import time
from unittest.mock import patch

import oracle

HERE = Path(__file__).resolve().parent
CONTRACT = HERE / "training_attention_contract_v1.json"
SCOPE = "gliner25_projected_training_attention/v1"
TRANSFORMERS_FILE = "transformers/models/deberta_v2/modeling_deberta_v2.py"
MAX_RSS = 2 * 1024**3
MAX_SECONDS = 120
OUTER_SECONDS = 180
MAX_ARTIFACT_BYTES = 64 * 1024**2
MAX_TENSOR_BYTES = 56 * 1024**2
MAX_METADATA_BYTES = 2 * 1024**2
MASK64 = (1 << 64) - 1
SEED = 0xFEDCBA9876543210
MICRO_BATCH = 0x100000002
REPLICA = 0x8000000000000003
CONFIG = {"hidden_size": 8, "intermediate_size": 16, "num_hidden_layers": 1,
          "num_attention_heads": 2, "max_position_embeddings": 512,
          "position_buckets": 256, "max_relative_positions": -1,
          "relative_attention": True, "share_att_key": True,
          "pos_att_type": ["p2c", "c2p"], "norm_rel_ebd": "layer_norm",
          "position_biased_input": False, "type_vocab_size": 0,
          "layer_norm_eps": 1e-7, "hidden_act": "gelu", "conv_kernel_size": 0}
METHODS = {"make_log_bucket_position", "build_relative_position", "scaled_size_sqrt", "build_rpos",
           "DisentangledSelfAttention.forward", "DisentangledSelfAttention.transpose_for_scores",
           "DisentangledSelfAttention.disentangled_attention_bias", "DebertaV2Encoder.get_attention_mask",
           "DebertaV2Encoder.get_rel_pos", "DebertaV2Encoder.get_rel_embedding"}


def checked(condition, message):
    if not condition:
        raise oracle.ContractError(message)


def f32(value):
    return struct.unpack("<f", struct.pack("<f", value))[0]


def mix(value):
    value = (value + 0x9E3779B97F4A7C15) & MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK64
    return value ^ (value >> 31)


def counter_parameters(probability, seed, micro_batch, replica, stream_id):
    probability = f32(probability)
    checked(math.isfinite(probability) and 0 <= probability < 1, "invalid dropout probability")
    checked(all(type(value) is int and 0 <= value <= MASK64 for value in (seed, micro_batch, replica, stream_id)),
            "dropout counter is not u64")
    stream = mix(seed) ^ mix(micro_batch) ^ mix((replica + 0x7265706C696361) & MASK64) ^ mix(stream_id)
    return stream, int(probability * 4294967296), f32(1 / f32(1 - probability))


def mask_value(index, parameters):
    checked(type(index) is int and 0 <= index <= MASK64, "dropout index is not u64")
    stream, threshold, scale = parameters
    return 0.0 if (mix(stream ^ mix(index)) >> 32) < threshold else scale


def i32_limb(value):
    return struct.unpack("<i", struct.pack("<I", value & 0xFFFFFFFF))[0]


def cases():
    result = []
    for name, sequence, lengths, cotangent in (("ragged7", 7, [7, 4], "all_rows"),
            ("fully_masked7", 7, [7, 0], "fully_masked_sample_only"),
            ("buckets512", 512, [512, 259], "all_rows")):
        for suffix, probability, layer in (("p0", 0.0, 0), ("p01", 0.1, 2), ("p0125", 0.125, 7)):
            result.append({"id": name + "_" + suffix, "batch": 2, "sequence": sequence,
                "lengths": lengths, "cotangent": cotangent, "probability": f32(probability),
                "logical_layer": layer, "seed": SEED, "micro_batch": MICRO_BATCH, "replica": REPLICA,
                "probability_stream_id": (layer << 32) | 3, "relative_stream_id": (layer << 32) | 2})
    return result


def control(case, bucket_map):
    b, s = case["batch"], case["sequence"]
    checked(b == 2 and s in (7, 512) and len(case["lengths"]) == b and
            all(type(length) is int and 0 <= length <= s for length in case["lengths"]), "invalid case geometry")
    checked(len(bucket_map) == 2 * s - 1 and all(type(value) is int and 0 <= value < 512 for value in bucket_map),
            "invalid source bucket map")
    prefix = [i32_limb(word >> shift) for word in (case["seed"], case["micro_batch"], case["replica"])
              for shift in (0, 32)]
    mask = [int(index < length) for length in case["lengths"] for index in range(s)]
    return prefix + mask + bucket_map


def digest(path):
    return {"size_bytes": path.stat().st_size, "sha256": oracle.sha256_file(path)}


def method_pins(path):
    source = path.read_text()
    tree = ast.parse(source)
    result = {}
    for node in tree.body:
        names = [(node.name, node)] if isinstance(node, ast.FunctionDef) else (
            [(node.name + "." + item.name, item) for item in node.body if isinstance(item, ast.FunctionDef)]
            if isinstance(node, ast.ClassDef) else [])
        for name, item in names:
            if name in METHODS:
                result[name] = {"lines": [item.lineno, item.end_lineno],
                    "sha256": hashlib.sha256(ast.get_source_segment(source, item).encode()).hexdigest()}
    checked(set(result) == METHODS, "attention method inventory differs")
    return result


def validate_contract(value):
    checked(value["scope"] == SCOPE and value["version"] == 1 and value["qualification"] is False and
            value["source_commit"] == oracle.UPSTREAM_COMMIT and value["cases"] == cases() and
            value["config"] == CONFIG, "attention source contract differs")
    checked(value["limits"] == {"child_rss_bytes": MAX_RSS, "cooperative_seconds": MAX_SECONDS,
        "outer_seconds": OUTER_SECONDS, "artifact_bytes": MAX_ARTIFACT_BYTES,
        "tensor_payload_bytes": MAX_TENSOR_BYTES, "metadata_bytes": MAX_METADATA_BYTES,
        "threads": 1, "interop_threads": 1}, "attention capture ceilings differ")
    checked(len(value["counter_vectors"]) == 3, "dropout counter vector inventory differs")
    for case, vector in zip(cases()[:3], value["counter_vectors"]):
        parameters = counter_parameters(case["probability"], case["seed"], case["micro_batch"],
                                        case["replica"], case["probability_stream_id"])
        checked(vector["case_id"] == case["id"] and vector["parameters"] == list(parameters) and
                vector["indices"] == [0, 1, 2, 3, 31, 32, 127, 128, 255, 256, 2**24, 2**24 + 1] and
                vector["values"] == [mask_value(index, parameters) for index in vector["indices"]],
                "dropout counter bit contract differs")
    return value


def preflight(upstream):
    checked(not sys.flags.optimize, "source assertions must remain enabled")
    value = validate_contract(oracle.read_json(CONTRACT))
    checked(digest(Path(oracle.__file__)) == value["oracle"], "frozen oracle helper changed")
    oracle.verify_upstream_checkout(upstream)
    runtime = oracle.verify_dependencies()
    checked(runtime == value["runtime"], "attention dependency runtime differs")
    for name, expected in value["fastino_files"].items():
        checked(digest(upstream / name) == expected, "Fastino attention loader changed: " + name)
    source = Path(importlib.metadata.distribution("transformers").locate_file(TRANSFORMERS_FILE))
    checked(digest(source) == value["transformers_source"] and method_pins(source) == value["methods"],
            "installed Transformers attention source changed")
    return value, source


def capture(upstream, output):
    contract, source_file = preflight(upstream)
    checked(not output.exists(), "refusing to overwrite attention capture")
    output.mkdir(parents=False)
    started = time.monotonic()
    provenance, torch = oracle.prepare_runtime(upstream)
    import psutil
    from safetensors.torch import save_file
    from transformers import DebertaV2Config
    from transformers.models.deberta_v2.modeling_deberta_v2 import DebertaV2Encoder

    checked(Path(inspect.getfile(DebertaV2Encoder)).resolve() == source_file.resolve(),
            "attention methods imported from an unpinned source path")
    torch.set_num_interop_threads(1)
    process = psutil.Process()
    peak_rss, tensor_bytes = 0, 0
    tensors, reports = {}, []

    def guard():
        nonlocal peak_rss
        peak_rss = max(peak_rss, process.memory_info().rss)
        checked(peak_rss <= MAX_RSS, "attention source RSS ceiling exceeded")
        checked(time.monotonic() - started <= MAX_SECONDS, "attention source cooperative deadline exceeded")

    def save(name, value):
        nonlocal tensor_bytes
        guard()
        checked(name not in tensors and value.dtype in (torch.float32, torch.int32), "duplicate or untyped capture tensor")
        amount = value.numel() * value.element_size()
        checked(tensor_bytes + amount <= MAX_TENSOR_BYTES, "attention tensor payload ceiling exceeded")
        checked(value.dtype == torch.int32 or bool(torch.isfinite(value).all()), "nonfinite source tensor")
        tensors[name] = value.detach().cpu().contiguous().clone()
        tensor_bytes += amount
        return name

    torch.manual_seed(251019)
    config = DebertaV2Config(**CONFIG, hidden_dropout_prob=0.0, attention_probs_dropout_prob=0.0)
    encoder = DebertaV2Encoder(config).float().cpu().train()
    attention = encoder.layer[0].attention.self
    checked(type(attention).__name__ == "DisentangledSelfAttention", "source attention implementation differs")
    weights = {name: value.detach().contiguous().clone() for name, value in encoder.state_dict().items()}
    save_file(weights, str(output / "weights.safetensors"))
    hidden_size, heads, relative_rows = CONFIG["hidden_size"], CONFIG["num_attention_heads"], 512

    for case in contract["cases"]:
        guard()
        name, b, s = case["id"], case["batch"], case["sequence"]
        attention.pos_dropout.p = case["probability"]
        attention.dropout.p = case["probability"]
        positions = torch.arange(b * s * hidden_size, dtype=torch.float32).reshape(b, s, hidden_size)
        hidden = torch.sin(positions * 0.017) + torch.cos(positions * 0.031) * 0.3
        mask = torch.tensor([[int(index < length) for index in range(s)] for length in case["lengths"]], dtype=torch.int32)
        pair_mask = encoder.get_attention_mask(mask)
        relative_pos = encoder.get_rel_pos(hidden)
        relative_embedding = encoder.get_rel_embedding()
        bucket_map = torch.cat((relative_pos[0, 0, 1:].flip(0), relative_pos[0, :, 0]))
        bucket_map = torch.clamp(bucket_map + 256, 0, relative_rows - 1).to(torch.int32)
        actual_ids = torch.clamp(relative_pos[0] + 256, 0, relative_rows - 1).to(torch.int32)
        difference = torch.arange(s)[:, None] - torch.arange(s)[None, :] + s - 1
        checked(torch.equal(actual_ids, bucket_map[difference]), "source bucket map is not diagonal-consistent")
        checked(torch.equal(pair_mask, mask[:, None, :, None] * mask[:, None, None, :]), "source mask is not query AND key")
        packet = control(case, bucket_map.tolist())

        def make_mask(shape, stream_id):
            parameters = counter_parameters(case["probability"], case["seed"], case["micro_batch"], case["replica"], stream_id)
            values = [mask_value(index, parameters) for index in range(math.prod(shape))]
            guard()
            return torch.tensor(values, dtype=torch.float32).reshape(shape)

        probability_mask = make_mask([b, heads, s, s], case["probability_stream_id"])
        relative_mask = make_mask([relative_rows, hidden_size], case["relative_stream_id"])
        projections, visits, phase = {}, {}, {"name": "original"}
        intermediates = {}

        def projection_hook(kind):
            def observe(_module, _inputs, result):
                visit = visits.get(kind, 0)
                visits[kind] = visit + 1
                key = kind if visit == 0 else kind + "r"
                checked(key in ("q", "k", "v", "qr", "kr") and key not in projections, "unexpected source projection call")
                projections[key] = result.detach().contiguous().clone()
            return observe

        def relative_dropout(value):
            checked(list(value.shape) == [relative_rows, hidden_size], "relative dropout was moved after row gathering")
            checked(torch.equal(value, relative_embedding), "relative LayerNorm/dropout ordering differs")
            result = value * relative_mask
            key = phase["name"] + ".relative_dropout"
            checked(key not in intermediates, "repeated source relative dropout")
            intermediates[key] = result
            return result

        def probability_dropout(value):
            checked(list(value.shape) == [b, heads, s, s], "probability dropout axes differ")
            key = phase["name"] + ".probabilities"
            checked(key not in intermediates, "repeated source probability dropout")
            intermediates[key] = value
            return value * probability_mask

        with ExitStack() as stack:
            stack.enter_context(patch.object(attention.pos_dropout, "forward", relative_dropout))
            stack.enter_context(patch.object(attention.dropout, "forward", probability_dropout))
            with ExitStack() as hooks:
                for kind, module in (("q", attention.query_proj), ("k", attention.key_proj), ("v", attention.value_proj)):
                    handle = module.register_forward_hook(projection_hook(kind))
                    hooks.callback(handle.remove)
                with torch.no_grad():
                    original, original_probabilities = attention(hidden, pair_mask, relative_pos=relative_pos,
                        rel_embeddings=relative_embedding, output_attentions=True)
            checked(visits == {"q": 2, "k": 2, "v": 1} and set(projections) == {"q", "k", "v", "qr", "kr"},
                    "shared source projection inventory differs")
            live = {key: value.detach().clone().requires_grad_(True) for key, value in projections.items()}
            visits.clear()

            def projection_return(kind):
                def apply(value):
                    visit = visits.get(kind, 0)
                    visits[kind] = visit + 1
                    key = kind if visit == 0 else kind + "r"
                    checked(key in live and value.shape[:-1] == live[key].shape[:-1], "projected leaf seam changed shape/order")
                    return live[key]
                return apply

            for kind, module in (("q", attention.query_proj), ("k", attention.key_proj), ("v", attention.value_proj)):
                stack.enter_context(patch.object(module, "forward", projection_return(kind)))
            phase["name"] = "leaf_replay"
            context, probabilities = attention(hidden, pair_mask, relative_pos=relative_pos,
                rel_embeddings=relative_embedding, output_attentions=True)
            checked(visits == {"q": 2, "k": 2, "v": 1}, "leaf replay projection calls differ")
            checked(torch.equal(original, context) and torch.equal(original_probabilities, probabilities),
                    "original attention methods and projected leaf replay differ")
            cotangent = torch.sin(positions * 0.013) * 0.7 + torch.cos(positions * 0.023) * 0.3
            if case["cotangent"] == "fully_masked_sample_only":
                cotangent = torch.zeros_like(context)
                cotangent[1] = (torch.arange(hidden_size, dtype=torch.float32) + 1) * 0.125
            ordered = ("q", "k", "v", "qr", "kr")
            gradients = dict(zip(ordered, torch.autograd.grad(context, [live[key] for key in ordered], grad_outputs=cotangent)))

        original_probs = intermediates["original.probabilities"]
        for sample, length in enumerate(case["lengths"]):
            if length < s:
                expected = torch.full_like(original_probs[sample, :, length:, :], 1 / s)
                checked(torch.equal(original_probs[sample, :, length:, :], expected), "fully masked query is not source-uniform")
        if case["cotangent"] == "fully_masked_sample_only":
            checked(all(int(torch.count_nonzero(gradients[key])) == 0 for key in ("q", "k", "qr", "kr")),
                    "fully masked query has a nonzero original-score VJP")
            checked(int(torch.count_nonzero(gradients["v"][0])) == 0 and int(torch.count_nonzero(gradients["v"][1])) > 0,
                    "fully masked query lost its value VJP")

        names = {"control": save(name + ".control", torch.tensor(packet, dtype=torch.int32)),
            "qkv": save(name + ".qkv", torch.cat([live[key].reshape(-1, hidden_size) for key in ("q", "k", "v")])),
            "relative": save(name + ".relative", torch.cat([live[key].reshape(relative_rows, hidden_size) for key in ("qr", "kr")])),
            "context": save(name + ".context", context.reshape(-1, hidden_size)),
            "cotangent": save(name + ".cotangent", cotangent.reshape(-1, hidden_size)),
            "gradient": save(name + ".gradient", torch.cat([gradients[key].reshape(-1, hidden_size) for key in ordered])),
            "probabilities_before_dropout": save(name + ".probabilities_before_dropout", original_probs),
            "probabilities_after_dropout": save(name + ".probabilities_after_dropout", original_probabilities),
            "probability_mask": save(name + ".probability_mask", probability_mask),
            "relative_normalized": save(name + ".relative_normalized", relative_embedding),
            "relative_dropout": save(name + ".relative_dropout", intermediates["original.relative_dropout"]),
            "relative_mask": save(name + ".relative_mask", relative_mask)}
        for key in ordered:
            names["gradient_" + key] = save(name + ".gradient_" + key, gradients[key].reshape(-1, hidden_size))
        reports.append({**case, "relative_rows": relative_rows, "hidden_size": hidden_size, "heads": heads,
            "head_dim": hidden_size // heads, "control_elements": len(packet), "original_vs_leaf_replay_exact": True,
            "uniform_fully_masked_queries": True, "gradient_leaf_order": list(ordered), "tensors": names})
        guard()
        # Release source tapes and hooks before constructing the next case.
        del live, gradients, projections, intermediates, context, probabilities, original, original_probabilities, original_probs

    save_file(tensors, str(output / "tensors.safetensors"))
    guard()
    files = {name: digest(output / name) for name in ("weights.safetensors", "tensors.safetensors")}
    manifest = {"version": 1, "scope": SCOPE, "qualification": False, "config": config.to_dict(), "cases": reports,
        "contract": digest(CONTRACT), "generator": digest(Path(__file__)), "provenance": provenance,
        "transformers_source": digest(source_file), "methods": contract["methods"], "files": files,
        "semantics": contract["semantics"], "limits": contract["limits"]}
    raw = (json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    checked(len(raw) <= MAX_METADATA_BYTES and len(raw) + sum(value["size_bytes"] for value in files.values()) <= MAX_ARTIFACT_BYTES,
            "attention capture artifact ceiling exceeded")
    with (output / "capture.json").open("xb") as destination:
        destination.write(raw)
    preflight(upstream)
    checked(digest(CONTRACT) == manifest["contract"] and digest(Path(__file__)) == manifest["generator"], "capture source changed")
    return {"status": "captured", "qualification": False, "cases": len(reports), "files": files,
            "capture": digest(output / "capture.json"), "peak_rss_bytes": peak_rss}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args()
    if args.preflight_only:
        preflight(args.upstream)
        checked(not any(name in sys.modules for name in ("torch", "gliner2", "peft")), "preflight imported a numerical runtime")
        result = {"scope": SCOPE, "qualification": False, "status": "preflight_only", "cases": len(cases())}
    else:
        checked(args.output_dir is not None, "capture requires --output-dir")
        result = capture(args.upstream, args.output_dir)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
