#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Prepare metadata for a vLLM reference using the same local Gemma4 GGUF.

Downloads configuration/tokenizer files only, pins their public repository
revision, and exports the GGUF's exact embedded chat template. It never downloads
or converts model weights. Generation/token-ID parity must still be checked.
Dense Gemma4 semantics are checked against GGUF metadata and rotary factors;
unrepresented attention/scaling modes are rejected before reference startup.
"""
import argparse
import hashlib
import json
import math
import mmap
from pathlib import Path
import struct


SCALARS = {0: "B", 1: "b", 2: "H", 3: "h", 4: "I", 5: "i", 6: "f", 7: "?", 10: "Q", 11: "q", 12: "d"}
METADATA_FILES = ["config.json", "tokenizer.json", "tokenizer_config.json", "chat_template.jinja", "processor_config.json", "generation_config.json"]


def read_header(path):
    """Read metadata and a bounded, non-learned rotary mask; never load a model."""
    with path.open("rb") as source, mmap.mmap(source.fileno(), min(path.stat().st_size, 128 * 1024 * 1024), access=mmap.ACCESS_READ) as data:
        position = 0

        def take(count):
            nonlocal position
            if count < 0 or count > len(data) - position:
                raise ValueError("GGUF header is truncated or exceeds the 128 MiB metadata ceiling")
            start = position
            position += count
            return start

        def number(fmt):
            return struct.unpack_from("<" + fmt, data, take(struct.calcsize("<" + fmt)))[0]

        def string(capture=True):
            length = number("Q")
            start = take(length)
            return data[start:start + length].decode() if capture else None

        def value(kind, capture=False, depth=0):
            if depth > 8:
                raise ValueError("excessive GGUF metadata nesting")
            if kind in SCALARS:
                return number(SCALARS[kind])
            if kind == 8:
                return string(capture)
            if kind == 9:
                element, count = number("I"), number("Q")
                if capture and element in SCALARS:
                    if count > 4096:
                        raise ValueError("excessive GGUF semantic metadata array")
                    return [number(SCALARS[element]) for _ in range(count)]
                if element in SCALARS:
                    take(count * struct.calcsize("<" + SCALARS[element]))
                else:
                    if count > 1_000_000:
                        raise ValueError("excessive GGUF metadata array")
                    for _ in range(count):
                        value(element, False, depth + 1)
                return {"element_type": element, "length": count}
            raise ValueError(f"unknown GGUF metadata kind: {kind}")

        if data[take(4):position] != b"GGUF" or number("I") != 3:
            raise ValueError("expected a little-endian GGUF v3 file")
        tensor_count, metadata_count = number("Q"), number("Q")
        if tensor_count > 100_000 or metadata_count > 100_000:
            raise ValueError("unbounded GGUF header counts")
        metadata = {}
        wanted = {"general.architecture", "general.alignment", "gemma4.block_count", "gemma4.embedding_length", "gemma4.embedding_length_per_layer_input", "tokenizer.ggml.tokens", "tokenizer.chat_template"}
        for _ in range(metadata_count):
            key, kind = string(), number("I")
            capture = key in wanted or key.startswith("gemma4.")
            result = value(kind, capture)
            if capture:
                if key in metadata:
                    raise ValueError(f"duplicate metadata: {key}")
                metadata[key] = result
        tensors = {}
        for _ in range(tensor_count):
            name, dimensions = string(), number("I")
            if dimensions > 4 or name in tensors:
                raise ValueError("invalid or duplicate GGUF tensor")
            shape = [number("Q") for _ in range(dimensions)]
            kind, offset = number("I"), number("Q")
            tensors[name] = {"shape_gguf_order": shape, "ggml_type": kind, "offset": offset}
        alignment = metadata.get("general.alignment", 32)
        if not isinstance(alignment, int) or alignment < 1 or alignment > 4096 or alignment & (alignment - 1):
            raise ValueError("invalid GGUF tensor-data alignment")
        data_offset = (position + alignment - 1) // alignment * alignment
        header = {"metadata": metadata, "tensors": tensors, "data_offset": data_offset}
        rotary = tensors.get("rope_freqs.weight")
        if rotary is not None:
            shape = rotary["shape_gguf_order"]
            if rotary["ggml_type"] != 0 or len(shape) != 1 or not 1 <= shape[0] <= 4096:
                raise ValueError("unsupported non-learned GGUF rotary factor tensor")
            count = shape[0]
            offset = data_offset + rotary["offset"]
            if offset < data_offset or offset > path.stat().st_size - count * 4:
                raise ValueError("truncated GGUF rotary factors")
            source.seek(offset)
            payload = source.read(count * 4)
            if len(payload) != count * 4:
                raise ValueError("truncated GGUF rotary factors")
            header["rope_freqs"] = list(struct.unpack("<" + "f" * count, payload))
        return header


def _positive_int(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(f"invalid or missing configuration: {name}")
    return value


def _same_number(actual, expected, name, *, positive=False):
    # GGUF stores many values as float32 while JSON preserves the decimal
    # literal. Permit representation error, not materially different semantics.
    if (type(actual) not in (int, float) or type(expected) not in (int, float)
            or not math.isfinite(actual) or not math.isfinite(expected)
            or (positive and (actual <= 0 or expected <= 0))
            or not math.isclose(actual, expected, rel_tol=1e-6, abs_tol=0)):
        raise ValueError(f"HF/GGUF configuration mismatch: {name}")


def _validate_rope(header, text):
    metadata = header["metadata"]
    parameters = text.get("rope_parameters")
    if not isinstance(parameters, dict) or set(parameters) != {"full_attention", "sliding_attention"}:
        raise ValueError("unsupported or missing Gemma4 RoPE configuration")
    for layer_type, dimension, theta_key, dimension_key in (
        ("sliding_attention", text["head_dim"], "gemma4.rope.freq_base_swa", "gemma4.rope.dimension_count_swa"),
        ("full_attention", text["global_head_dim"], "gemma4.rope.freq_base", "gemma4.rope.dimension_count"),
    ):
        rope = parameters[layer_type]
        if not isinstance(rope, dict) or set(rope) - {"rope_type", "rope_theta", "partial_rotary_factor"}:
            raise ValueError(f"unsupported Gemma4 RoPE scaling: {layer_type}")
        if _positive_int(metadata.get(dimension_key), dimension_key) != dimension or dimension % 2:
            raise ValueError(f"HF/GGUF RoPE frequency-domain mismatch: {layer_type}")
        _same_number(rope.get("rope_theta"), metadata.get(theta_key), f"{layer_type}.rope_theta", positive=True)
        partial = rope.get("partial_rotary_factor", 1.0)
        if type(partial) not in (int, float) or not math.isfinite(partial) or not 0 < partial <= 1:
            raise ValueError(f"invalid partial rotary factor: {layer_type}")
        if layer_type == "sliding_attention":
            if rope.get("rope_type") != "default" or partial != 1.0:
                raise ValueError("unsupported sliding-attention rotary behavior")
            continue
        if rope.get("rope_type") not in ("default", "proportional"):
            raise ValueError("unsupported full-attention rotary behavior")
        factors = header.get("rope_freqs")
        if "rope_freqs.weight" in header["tensors"] and factors is None:
            raise ValueError("GGUF rotary factor payload was not validated")
        if factors is None:
            active = dimension
        else:
            if len(factors) != dimension // 2:
                raise ValueError("GGUF rotary factor count differs from frequency domain")
            prefix = 0
            while prefix < len(factors) and factors[prefix] == 1.0:
                prefix += 1
            # Match the native binary prefix-mask interpretation exactly.
            # Arbitrary per-frequency scaling cannot be represented by HF's
            # proportional RoPE and must never be silently ignored.
            if prefix == 0 or any(not math.isfinite(v) or v < 1e20 for v in factors[prefix:]):
                raise ValueError("unsupported GGUF rotary factor scaling")
            active = prefix * 2
        _same_number(partial, active / dimension, "full_attention.partial_rotary_factor", positive=True)
        # Proportional RoPE computes its pair count after integer truncation.
        # A near-equal decimal can cross that boundary and disable a pair, so
        # floating-point tolerance alone does not prove equivalent rotation.
        if 2 * (int(dimension * partial) // 2) != active:
            raise ValueError("HF/GGUF effective active rotary dimensions differ")
        if rope.get("rope_type") == "default" and active != dimension:
            raise ValueError("partial full-attention RoPE requires proportional frequency semantics")
    if any(key.startswith("gemma4.rope.scaling.") and not (key.endswith(".type") and value == "none")
           for key, value in metadata.items()):
        raise ValueError("unsupported GGUF RoPE scaling metadata")


def _validate_semantics(header, text, layers):
    metadata = header["metadata"]
    integer_pairs = {
        "intermediate_size": "gemma4.feed_forward_length",
        "num_attention_heads": "gemma4.attention.head_count",
        "num_key_value_heads": "gemma4.attention.head_count_kv",
        "sliding_window": "gemma4.attention.sliding_window",
        "max_position_embeddings": "gemma4.context_length",
        "head_dim": "gemma4.attention.key_length_swa",
        "global_head_dim": "gemma4.attention.key_length",
    }
    for name, key in integer_pairs.items():
        if _positive_int(text.get(name), name) != _positive_int(metadata.get(key), key):
            raise ValueError(f"HF/GGUF configuration mismatch: {name}")
    if (_positive_int(metadata.get("gemma4.attention.value_length_swa"), "gemma4.attention.value_length_swa") != text["head_dim"]
            or _positive_int(metadata.get("gemma4.attention.value_length"), "gemma4.attention.value_length") != text["global_head_dim"]):
        raise ValueError("HF/GGUF attention value-head dimensions differ")
    global_kv = text.get("num_global_key_value_heads")
    if global_kv is not None and _positive_int(global_kv, "num_global_key_value_heads") != text["num_key_value_heads"]:
        raise ValueError("unsupported differing global KV head count")
    if text["num_attention_heads"] % text["num_key_value_heads"]:
        raise ValueError("invalid grouped-query head layout")
    shared = text.get("num_kv_shared_layers")
    if (type(shared) is not int or type(metadata.get("gemma4.attention.shared_kv_layers")) is not int
            or not 0 <= shared < layers or shared != metadata["gemma4.attention.shared_kv_layers"]):
        raise ValueError("HF/GGUF shared-KV configuration mismatch")
    pattern = metadata.get("gemma4.attention.sliding_window_pattern")
    if not isinstance(pattern, list) or len(pattern) != layers or any(type(v) is not bool for v in pattern):
        raise ValueError("missing or invalid GGUF per-layer attention pattern")
    if text.get("layer_types") != ["sliding_attention" if sliding else "full_attention" for sliding in pattern]:
        raise ValueError("HF/GGUF per-layer attention pattern mismatch")
    _same_number(text.get("rms_norm_eps"), metadata.get("gemma4.attention.layer_norm_rms_epsilon"), "rms_norm_eps", positive=True)
    _same_number(text.get("final_logit_softcapping"), metadata.get("gemma4.final_logit_softcapping"), "final_logit_softcapping")
    if text["final_logit_softcapping"] < 0:
        raise ValueError("invalid final logit softcap")
    if text.get("hidden_activation") != "gelu_pytorch_tanh":
        raise ValueError("unsupported Gemma4 activation")
    for name in ("attention_bias", "attention_k_eq_v", "enable_moe_block", "use_double_wide_mlp"):
        if text.get(name) is not False:
            raise ValueError(f"unsupported Gemma4 execution mode: {name}")
    if text.get("use_second_mlp_block", False) is not False:
        raise ValueError("unsupported Gemma4 second MLP block")
    if text.get("attn_logit_softcapping") not in (None, 0.0):
        raise ValueError("unsupported Gemma4 attention logit softcap")
    if text.get("use_bidirectional_attention") not in (None, False):
        raise ValueError("unsupported bidirectional attention")
    _validate_rope(header, text)


def validate_config(header, config):
    metadata, tensors = header["metadata"], header["tensors"]
    if metadata.get("general.architecture") != "gemma4" or config.get("model_type") != "gemma4":
        raise ValueError("both metadata sources must describe Gemma4")
    text = config.get("text_config")
    if not isinstance(text, dict):
        raise ValueError("missing Gemma4 text configuration")
    pairs = {"num_hidden_layers": "gemma4.block_count", "hidden_size": "gemma4.embedding_length",
             "hidden_size_per_layer_input": "gemma4.embedding_length_per_layer_input"}
    for key, gguf_key in pairs.items():
        if _positive_int(text.get(key), key) != _positive_int(metadata.get(gguf_key), gguf_key):
            raise ValueError(f"HF/GGUF configuration mismatch: {key}")
    if _positive_int(text.get("vocab_size"), "vocab_size") != metadata["tokenizer.ggml.tokens"]["length"]:
        raise ValueError("HF/GGUF vocabulary size mismatch")
    _positive_int(text.get("vocab_size_per_layer_input"), "vocab_size_per_layer_input")
    layers, hidden, ple = (text[key] for key in pairs)
    _validate_semantics(header, text, layers)
    required = {"per_layer_token_embd.weight": [layers * ple, text["vocab_size_per_layer_input"]],
                "per_layer_model_proj.weight": [hidden, layers * ple], "per_layer_proj_norm.weight": [ple]}
    for layer in range(layers):
        required.update({f"blk.{layer}.inp_gate.weight": [hidden, ple],
                         f"blk.{layer}.proj.weight": [ple, hidden],
                         f"blk.{layer}.post_norm.weight": [hidden]})
    for name, shape in required.items():
        if tensors.get(name, {}).get("shape_gguf_order") != shape:
            raise ValueError(f"missing or wrongly shaped PLE tensor: {name}")
    if not isinstance(metadata.get("tokenizer.chat_template"), str):
        raise ValueError("GGUF has no single embedded chat template")
    return required


def file_hash(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gguf", type=Path, required=True)
    parser.add_argument("--metadata-dir", type=Path, required=True)
    parser.add_argument("--repo", default="unsloth/gemma-4-E4B-it")
    parser.add_argument("--revision", help="Full public Hugging Face commit SHA; otherwise resolve main once and pin it")
    parser.add_argument("--expected-gguf-sha256")
    parser.add_argument("--offline", action="store_true", help="Validate already prepared metadata without network access")
    args = parser.parse_args()
    header = read_header(args.gguf)
    digest = file_hash(args.gguf)
    if args.expected_gguf_sha256 and digest != args.expected_gguf_sha256:
        raise ValueError("GGUF file does not match its expected SHA-256")
    revision = args.revision
    if not args.offline:
        from huggingface_hub import HfApi, snapshot_download
        revision = HfApi().model_info(args.repo, revision=revision).sha
        snapshot_download(args.repo, revision=revision, allow_patterns=METADATA_FILES,
                          local_dir=args.metadata_dir, max_workers=1)
    elif revision is None:
        raise ValueError("offline preparation requires the pinned metadata --revision")
    config = json.loads((args.metadata_dir / "config.json").read_text())
    required = validate_config(header, config)
    hashes = {name: file_hash(args.metadata_dir / name) for name in METADATA_FILES if (args.metadata_dir / name).exists()}
    template = args.metadata_dir / "chat_template.gguf.jinja"
    template.write_text(header["metadata"]["tokenizer.chat_template"])
    receipt = {"repo": args.repo, "revision": revision, "metadata_sha256": hashes,
               "gguf": str(args.gguf.resolve()), "gguf_sha256": digest,
               "chat_template": str(template.resolve()), "chat_template_sha256": file_hash(template),
               "required_ple_tensors": required, "tensor_count": len(header["tensors"]),
               "semantic_config_verified": True,
               "same_weights_no_conversion": True, "token_id_parity_verified": False,
               "required_launch_args": ["--tokenizer", str(args.metadata_dir.resolve()), "--hf-config-path", str(args.metadata_dir.resolve()), "--chat-template", str(template.resolve())]}
    (args.metadata_dir / "reference-metadata.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
