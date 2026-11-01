#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Add missing E4B PLE name mappings to an explicitly selected reference checkout.

This changes reference loader compatibility only. It never changes tensor values,
quantization, math, generation settings or kernels. Keep its receipt with every
benchmark, and identify the reference as patched rather than upstream stock.
"""
import argparse
import ast
import __future__
import difflib
import hashlib
import json
from pathlib import Path
import subprocess
import struct
from types import SimpleNamespace


PREFIXES = {
    "per_layer_token_embd.": "model.language_model.embed_tokens_per_layer.",
    "per_layer_model_proj.": "model.language_model.per_layer_model_projection.",
    "per_layer_proj_norm.": "model.language_model.per_layer_projection_norm.",
}
COMPONENTS = {
    "inp_gate": "per_layer_input_gate",
    "proj": "per_layer_projection",
    "post_norm": "post_per_layer_input_norm",
}


class MappingOracle:
    """The upstream WeightsMapper name operations, without importing Torch/CUDA."""
    def __init__(self, orig_to_new_prefix, orig_to_new_substr, orig_to_new_regex=None):
        self.prefix = orig_to_new_prefix
        self.substr = orig_to_new_substr
        self.regex = orig_to_new_regex or {}

    def map(self, name):
        for pattern, replacement in self.regex.items():
            name = pattern.sub(replacement, name)
        for original, replacement in self.substr.items():
            if original in name:
                name = name.replace(original, replacement, 1)
        for original, replacement in self.prefix.items():
            if name.startswith(original):
                name = name.replace(original, replacement, 1)
        return name


def extract_mapper(source):
    tree = ast.parse(source)
    constants = next(node for node in tree.body if isinstance(node, ast.AnnAssign)
                     and isinstance(node.target, ast.Name) and node.target.id == "GEMMA4_TEXT_SUBSTR")
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "build_gemma4_text_mapper")
    namespace = {"WeightsMapper": MappingOracle, "GEMMA4_TEXT_SUBSTR": ast.literal_eval(constants.value)}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "<reference-name-map>", "exec"), namespace)
    return namespace[function.name]()


def verify_maps(source):
    mapper = extract_mapper(source)
    checks = {}
    for original, replacement in PREFIXES.items():
        checks[original + "weight"] = replacement + "weight"
    for layer in (0, 23, 24, 41):
        for original, replacement in COMPONENTS.items():
            checks[f"blk.{layer}.{original}.weight"] = f"model.language_model.layers.{layer}.{replacement}.weight"
    # Guard against a broad 'proj.' substitution corrupting existing names.
    for original, replacement in {
        "attn_q": "self_attn.q_proj", "attn_output": "self_attn.o_proj",
        "ffn_down": "mlp.down_proj", "ffn_gate_inp": "router.proj",
    }.items():
        checks[f"blk.0.{original}.weight"] = f"model.language_model.layers.0.{replacement}.weight"
    checks["token_embd.weight"] = "model.language_model.embed_tokens.weight"
    checks["rope_freqs.weight"] = "rope_freqs.weight"  # intentionally not a learned weight
    for original, expected in checks.items():
        actual = mapper.map(original)
        if actual != expected:
            raise ValueError(f"incorrect reference mapping: {original}: {actual} != {expected}")
    return checks


def patch_source(source):
    tree = ast.parse(source)
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "build_gemma4_text_mapper")
    segment = ast.get_source_segment(source, function)
    if all(key in segment for key in PREFIXES):
        verify_maps(source)
        return source
    if any(key in segment for key in PREFIXES) or "orig_to_new_regex=" in segment:
        raise ValueError("partially changed upstream mapper; inspect it manually")
    marker = "    return WeightsMapper(\n        orig_to_new_prefix={\n"
    if segment.count(marker) != 1:
        raise ValueError("upstream mapper shape changed; refusing an ambiguous patch")
    regex = "".join(f'            re.compile(r"^blk\\.(\\d+)\\.{old}\\."): r"blk.\\1.{new}.",\n'
                    for old, new in COMPONENTS.items())
    prefixes = "".join(f'            "{old}": "{new}",\n' for old, new in PREFIXES.items())
    replacement = "    import re\n    return WeightsMapper(\n        orig_to_new_regex={\n" + regex + "        },\n        orig_to_new_prefix={\n" + prefixes
    patched = source.replace(segment, segment.replace(marker, replacement), 1)
    verify_maps(patched)
    return patched


def verify_tensor_coverage(source, header):
    """Require every learned tensor to have a distinct loader mapping.

    This static check precedes GPU loading; it does not claim that a running
    reference has consumed the mapped parameters or produced valid output.
    """
    mapper = extract_mapper(source)
    mapped, intentional, unexpected = {}, [], []
    for name, tensor in header["tensors"].items():
        destination = mapper.map(name)
        if destination == name:
            (intentional if name == "rope_freqs.weight" else unexpected).append(name)
        else:
            mapped[name] = {"hf_name": destination, **tensor}
    if unexpected:
        raise ValueError(f"unmapped learned GGUF tensors: {unexpected}")
    destinations = [tensor["hf_name"] for tensor in mapped.values()]
    if len(destinations) != len(set(destinations)):
        raise ValueError("multiple GGUF tensors map to the same HF tensor")
    return {"tensors": len(header["tensors"]), "mapped_count": len(mapped),
            "intentionally_unmapped": intentional, "unexpectedly_unmapped": unexpected,
            "mappings": mapped, "scope": "static name coverage; runtime parameter loading and output qualification still required"}


REPLICATED_LOADER = '''    if isinstance(layer, ReplicatedLinear):
        # ReplicatedLinear has only the dense copy loader, not loader_v2.
        # Preserve GGUF packed bytes/type while materializing via _store.
        def _gguf_replicated_weight_loader(param, loaded_weight, loaded_shard_id=None):
            if loaded_shard_id is not None:
                raise ValueError("Replicated GGUF weights cannot have tensor-parallel shards")
            store = getattr(param, "_store", None)
            if store is None:
                raise TypeError("Replicated GGUF loader requires a GGUF storage parameter")
            store(loaded_weight)
        return _gguf_replicated_weight_loader
'''


SHARED_QKV_HELPERS = '''def gemma4_shared_q_only_layers(names, text_config):
    shared = getattr(text_config, "num_kv_shared_layers", 0)
    if not 0 <= shared < text_config.num_hidden_layers:
        raise ValueError("Invalid Gemma4 KV sharing extent")
    first = text_config.num_hidden_layers - shared
    result = set()
    for layer in range(first, text_config.num_hidden_layers):
        prefix = f"blk.{layer}.attn_"
        has_k = prefix + "k.weight" in names
        has_v = prefix + "v.weight" in names
        if has_k != has_v:
            raise ValueError(f"Partially populated shared K/V projection at layer {layer}")
        if not has_k:
            if prefix + "q.weight" not in names:
                raise ValueError(f"Missing learned shared-layer Q projection at layer {layer}")
            result.add(layer)
    return result


def gemma4_expand_shared_qkv(name, weight, q_only_layers, text_config):
    # Stock Gemma4 still splits full QKV on shared-cache layers, although its
    # attention route ignores the newly projected K/V there. GGUF omits those
    # unused projections. Fill only these absent, unused slots with exact zeros.
    import re
    yield name, weight
    match = re.fullmatch(r"model\\.language_model\\.layers\\.(\\d+)\\.self_attn\\.q_proj\\.(weight|weight_type)", name)
    if match is None or int(match[1]) not in q_only_layers:
        return
    if match[2] == "weight_type":
        if weight.ndim != 0 or int(weight.item()) != 2:
            raise ValueError("Shared QKV placeholder packing is qualified only for scalar Q4_0 type 2")
        placeholder = weight.clone()
    else:
        layer_config = gemma4_layer_config(text_config, int(match[1]))
        q_rows = layer_config.num_attention_heads * layer_config.head_dim
        kv_rows = layer_config.num_key_value_heads * layer_config.head_dim
        if weight.ndim != 2 or weight.shape[0] != q_rows or not 0 < kv_rows <= q_rows or weight.dtype != torch.uint8 or layer_config.hidden_size % 32 or weight.shape[1] != layer_config.hidden_size // 32 * 18:
            raise ValueError(f"Invalid shared-layer Q projection geometry: {name}")
        # Zero packed GGUF blocks have zero scale and therefore decode to zero.
        # This retains the source Q quantization and never converts learned Q.
        placeholder = torch.zeros_like(weight[:kv_rows])
    yield name.replace(".q_proj.", ".k_proj."), placeholder
    yield name.replace(".q_proj.", ".v_proj."), placeholder


'''


def patch_shared_qkv(source):
    imported = "from vllm.transformers_utils.configs.gemma4 import gemma4_layer_config\n"
    marker = "class Gemma4GGUFAdapter(BaseGGUFWeightsAdapter):"
    if "def gemma4_shared_q_only_layers(" in source:
        if imported not in source or "self._shared_q_only_layers = gemma4_shared_q_only_layers" not in source or "for mapped_name, mapped_weight in gemma4_expand_shared_qkv(" not in source:
            raise ValueError("partial shared QKV compatibility patch")
        return source
    tree = ast.parse(source)
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Gemma4GGUFAdapter")
    methods = {node.name: node for node in cls.body if isinstance(node, ast.FunctionDef)}
    build = ast.get_source_segment(source, methods["build_name_map"])
    transform = ast.get_source_segment(source, methods["transform_weights"])
    build_marker = "        for name in sorted(get_gguf_tensor_names(files.all_files)):\n"
    transform_marker = "            yield from self._split_expert_weights(name, weight)"
    if build.count(build_marker) != 1 or build.count("        del model_config\n") != 1 or transform.count("        del model_config\n") != 1 or transform.count(transform_marker) != 1:
        raise ValueError("upstream shared QKV adapter shape changed; inspect manually")
    build_after = build.replace("        del model_config\n", "", 1).replace(build_marker,
        "        source_names = get_gguf_tensor_names(files.all_files)\n"
        "        text_config = model_config.hf_config.get_text_config()\n"
        "        self._shared_q_only_layers = gemma4_shared_q_only_layers(source_names, text_config)\n"
        "        for name in sorted(source_names):\n", 1)
    transform_after = transform.replace("        del model_config\n",
        "        text_config = model_config.hf_config.get_text_config()\n"
        "        q_only_layers = getattr(self, \"_shared_q_only_layers\", None)\n"
        "        if q_only_layers is None:\n"
        "            raise ValueError(\"Shared QKV source tensor inventory was not prepared\")\n", 1).replace(transform_marker,
        "            for mapped_name, mapped_weight in gemma4_expand_shared_qkv(\n"
        "                name, weight, q_only_layers, text_config\n"
        "            ):\n"
        "                yield from self._split_expert_weights(mapped_name, mapped_weight)", 1)
    patched = source.replace(build, build_after, 1).replace(transform, transform_after, 1)
    anchor = "from vllm.model_executor.models.utils import WeightsMapper\n"
    if patched.count(anchor) != 1 or patched.count(marker) != 1:
        raise ValueError("upstream shared QKV imports/class changed")
    return patched.replace(anchor, anchor + imported, 1).replace(marker, SHARED_QKV_HELPERS + marker, 1)


def shared_qkv_namespace(source, torch):
    functions = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)
                 and node.name in ("gemma4_shared_q_only_layers", "gemma4_expand_shared_qkv")]
    if len(functions) != 2:
        raise ValueError("missing shared QKV compatibility helpers")
    def layer_config(config, layer):
        return SimpleNamespace(**{**vars(config), "head_dim": config.global_head_dim if config.layer_types[layer] == "full_attention" else config.head_dim})
    namespace = {"torch": torch, "gemma4_layer_config": layer_config}
    exec(compile(ast.Module(body=functions, type_ignores=[]), "<reference-shared-qkv>", "exec"), namespace)
    return namespace


def verify_shared_qkv(source):
    class Tensor:
        def __init__(self, shape, value=None):
            self.shape, self.value, self.dtype = shape, value, "uint8"
            self.ndim = len(shape)
        def clone(self):
            return Tensor(self.shape, self.value)
        def item(self):
            return self.value
        def __getitem__(self, index):
            return Tensor((index.stop, self.shape[1]), self.value)
    torch = SimpleNamespace(uint8="uint8", zeros_like=lambda tensor: Tensor(tensor.shape, 0))
    helpers = shared_qkv_namespace(source, torch)
    config = SimpleNamespace(num_hidden_layers=42, num_kv_shared_layers=18, num_attention_heads=8,
                             num_key_value_heads=2, hidden_size=2560, head_dim=256, global_head_dim=512,
                             layer_types=["full_attention" if layer % 6 == 5 else "sliding_attention" for layer in range(42)])
    names = {f"blk.{layer}.attn_q.weight" for layer in range(42)}
    names.update(f"blk.{layer}.attn_{component}.weight" for layer in range(24) for component in ("k", "v"))
    q_only = helpers["gemma4_shared_q_only_layers"](names, config)
    if q_only != set(range(24, 42)):
        raise ValueError("incorrect shared Q-only layer classification")
    expand = helpers["gemma4_expand_shared_qkv"]
    for layer in range(42):
        dim = 512 if layer % 6 == 5 else 256
        weight = Tensor((8 * dim, 1440))
        result = list(expand(f"model.language_model.layers.{layer}.self_attn.q_proj.weight", weight, q_only, config))
        if result[0][1] is not weight or len(result) != (3 if layer >= 24 else 1):
            raise ValueError("shared QKV patch changed learned Q or a non-shared layer")
        for name, tensor in result[1:]:
            if tensor.shape != (2 * dim, 1440) or tensor.value != 0:
                raise ValueError("invalid derived unused K/V geometry/value")
    try:
        helpers["gemma4_shared_q_only_layers"](names | {"blk.24.attn_k.weight"}, config)
    except ValueError:
        pass
    else:
        raise ValueError("partial source K/V was accepted")
    try:
        list(expand("model.language_model.layers.24.self_attn.q_proj.weight_type", Tensor((), 14), q_only, config))
    except ValueError:
        pass
    else:
        raise ValueError("unqualified placeholder quantization was accepted")
    for bad in (Tensor((2047, 1440)), Tensor((2048, 1439))):
        try:
            list(expand("model.language_model.layers.24.self_attn.q_proj.weight", bad, q_only, config))
        except ValueError:
            pass
        else:
            raise ValueError("invalid source Q shape was accepted")
    try:
        helpers["gemma4_shared_q_only_layers"](names - {"blk.24.attn_q.weight"}, config)
    except ValueError:
        pass
    else:
        raise ValueError("missing learned Q was accepted")
    return {"shared_q_only_layers": sorted(q_only), "learned_q_identity_preserved": True,
            "non_shared_layers_unchanged": True, "local_256_global_512_shapes_verified": True,
            "partial_source_kv_and_unqualified_quantization_rejected": True}


def derived_unused_weights(gguf, header, config):
    text = config["text_config"]
    first = text["num_hidden_layers"] - text["num_kv_shared_layers"]
    if (first, text["num_hidden_layers"], text["num_key_value_heads"], text["num_attention_heads"]) != (24, 42, 2, 8):
        raise ValueError("derived unused-weight receipt is qualified only for the E4B 24+18 shared-KV layout")
    rows, learned = [], []
    with gguf.open("rb") as source:
        for layer in range(first, text["num_hidden_layers"]):
            kind = text["layer_types"][layer]
            donor = max(index for index in range(first) if text["layer_types"][index] == kind)
            dim = text["global_head_dim"] if kind == "full_attention" else text["head_dim"]
            q_name = f"blk.{layer}.attn_q.weight"
            q = header["tensors"][q_name]
            if q["ggml_type"] != 2 or q["shape_gguf_order"] != [text["hidden_size"], 8 * dim]:
                raise ValueError(f"unqualified source Q layout: {q_name}")
            if any(f"blk.{layer}.attn_{component}.weight" in header["tensors"] for component in ("k", "v")):
                raise ValueError("derived unused K/V requested for a present source tensor")
            width = text["hidden_size"] // 32 * 18
            source.seek(header["data_offset"] + q["offset"])
            data = source.read(8 * dim * width)
            if len(data) != 8 * dim * width:
                raise ValueError("truncated learned Q bytes")
            learned.append({"source": q_name, "raw_quant_type": 2, "packed_shape": [8 * dim, width],
                            "sha256": hashlib.sha256(data).hexdigest(), "transformation": "identity; no learned byte changed"})
            zeros = bytes(2 * dim * width)
            for component in ("k", "v"):
                name = f"model.language_model.layers.{layer}.self_attn.{component}_proj"
                reason = f"absent and unused projection: layer {layer} reads original {kind} donor layer {donor} KV; cache update is skipped"
                rows.append({"name": name + ".weight", "dtype": "uint8", "raw_quant_type": 2,
                             "packed_shape": [2 * dim, width], "logical_shape": [2 * dim, text["hidden_size"]],
                             "sha256": hashlib.sha256(zeros).hexdigest(), "reason": reason,
                             "block_scales": "all 80 Q4_0 fp16 scales per row are exactly zero; every quantized value decodes to zero"})
                rows.append({"name": name + ".weight_type", "dtype": "int64", "shape": [], "value": 2,
                             "sha256": hashlib.sha256(struct.pack("<q", 2)).hexdigest(), "reason": reason})
    return {"derived_unused_parameters": rows, "learned_q_preservation": learned}


def verify_unused_kv_route(vllm_dir):
    files = {name: (vllm_dir / name).read_text() for name in (
        "model_executor/layers/attention/attention.py", "v1/attention/backends/triton_attn.py",
        "model_executor/models/gemma4.py")}
    attention = ast.parse(files["model_executor/layers/attention/attention.py"])
    cls = next(node for node in attention.body if isinstance(node, ast.ClassDef) and node.name == "Attention")
    forward = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "forward")
    guards = [node for node in ast.walk(forward) if isinstance(node, ast.If)
              and "self.kv_sharing_target_layer_name is None" in ast.unparse(node.test)
              and any(isinstance(child, ast.Call) and "unified_kv_cache_update" in ast.unparse(child.func) for body in node.body for child in ast.walk(body))]
    if len(guards) != 2:
        raise ValueError("stock attention KV-write skip guards changed")
    backend = ast.parse(files["v1/attention/backends/triton_attn.py"])
    cls = next(node for node in backend.body if isinstance(node, ast.ClassDef) and node.name == "TritonAttentionImpl")
    forward = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "forward")
    encoder = [node for node in forward.body if isinstance(node, ast.If) and "self.attn_type in" in ast.unparse(node.test)]
    if len(encoder) != 1:
        raise ValueError("stock Triton encoder/decoder split changed")
    forward.body.remove(encoder[0])
    if any(isinstance(node, ast.Name) and node.id in ("key", "value") for node in ast.walk(forward)):
        raise ValueError("stock Triton decoder reads newly projected K/V")
    normalized = ast.unparse(forward)
    if "k=key_cache" not in normalized or "v=value_cache" not in normalized:
        raise ValueError("stock Triton decoder no longer reads cached donor K/V")
    if "forward_includes_kv_cache_update: bool = False" not in files["v1/attention/backends/triton_attn.py"]:
        raise ValueError("stock Triton cache-update ownership changed")
    return {"qualified_route": "Gemma4 decoder + TRITON_ATTN + F16 KV; no encoder/multimodal input",
            "stock_attention_write_skip_guards": len(guards), "decoder_direct_key_value_reads": 0,
            "decoder_attention_reads": "key_cache/value_cache; shared layers use donor cache",
            "source_sha256": {name: hashlib.sha256(source.encode()).hexdigest() for name, source in files.items()}}


def verify_replicated_loader(source):
    function = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)
                    and node.name == "_resolve_gguf_weight_loader")
    class Replicated:
        pass
    class Parameter:
        def _store(self, weight):
            self.stored = weight
    namespace = {"ReplicatedLinear": Replicated}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "<reference-replicated-loader>", "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    resolve = namespace[function.name]
    packed = object()
    parameter = Parameter()
    loader = resolve(Replicated(), lambda *args: (_ for _ in ()).throw(AssertionError("dense fallback used")))
    loader(parameter, packed)
    if parameter.stored is not packed:
        raise ValueError("replicated GGUF loader changed or dropped packed data")
    try:
        loader(parameter, packed, "q")
    except ValueError:
        pass
    else:
        raise ValueError("replicated GGUF loader accepted a tensor-parallel shard")
    try:
        loader(object(), packed)
    except TypeError:
        pass
    else:
        raise ValueError("replicated GGUF loader accepted a non-GGUF parameter")
    class V2:
        weight_loader_v2 = packed
    if resolve(V2(), None) is not packed or resolve(object(), packed) is not packed:
        raise ValueError("replicated fix changed existing v2/dense loader dispatch")
    return {"replicated_packed_data_preserved": True, "replicated_shards_rejected": True,
            "non_gguf_parameters_rejected": True, "existing_v2_and_fallback_dispatch_preserved": True}


def patch_replicated_loader(source):
    tree = ast.parse(source)
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "_resolve_gguf_weight_loader")
    segment = ast.get_source_segment(source, function)
    imported = "from vllm.model_executor.layers.linear import ReplicatedLinear\n"
    if "_gguf_replicated_weight_loader" in segment:
        if imported not in source:
            raise ValueError("partial replicated GGUF loader patch")
        verify_replicated_loader(source)
        return source
    if "ReplicatedLinear" in source or segment.count("    return (\n") != 1:
        raise ValueError("upstream replicated loader shape changed; inspect manually")
    anchor = "from vllm.model_executor.layers.fused_moe import RoutedExperts\n"
    if source.count(anchor) != 1:
        raise ValueError("upstream replicated loader imports changed")
    patched = source.replace(segment, segment.replace("    return (\n", REPLICATED_LOADER + "    return (\n", 1), 1)
    patched = patched.replace(anchor, anchor + imported, 1)
    verify_replicated_loader(patched)
    return patched


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plugin-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--gguf", type=Path, help="Also fail closed unless every learned tensor has a unique mapping")
    parser.add_argument("--metadata-dir", type=Path, help="Prepared HF config; record every derived unused shared-layer K/V parameter")
    parser.add_argument("--vllm-dir", type=Path, help="Installed vllm package; assert decoder ignores new shared-layer K/V and skips their cache writes")
    parser.add_argument("--prior-receipt", type=Path, action="append", default=[], help="Preserve earlier applied diffs in a hash-linked receipt chain")
    parser.add_argument("--cpu-qualification", type=Path, help="Successful explicit CPU loader/dequantization qualification receipt")
    args = parser.parse_args()
    target = args.plugin_dir / "vllm_gguf_plugin/weights_adapter/gemma4.py"
    before = target.read_text()
    after = patch_shared_qkv(patch_source(before))
    params_target = args.plugin_dir / "vllm_gguf_plugin/quantization/params.py"
    params_before = params_target.read_text()
    params_after = patch_replicated_loader(params_before)
    receipt = {"kind": "reference-loader-compatibility-only", "reference_is_patched": True,
               "file": str(target.resolve()), "before_sha256": hashlib.sha256(before.encode()).hexdigest(),
               "after_sha256": hashlib.sha256(after.encode()).hexdigest(), "mappings": verify_maps(after),
               "patch": "".join(difflib.unified_diff(before.splitlines(keepends=True), after.splitlines(keepends=True), fromfile="upstream/gemma4.py", tofile="patched/gemma4.py"))}
    receipt["replicated_loader"] = {"file": str(params_target.resolve()),
        "before_sha256": hashlib.sha256(params_before.encode()).hexdigest(),
        "after_sha256": hashlib.sha256(params_after.encode()).hexdigest(),
        "verification": verify_replicated_loader(params_after),
        "patch": "".join(difflib.unified_diff(params_before.splitlines(keepends=True), params_after.splitlines(keepends=True), fromfile="upstream/params.py", tofile="patched/params.py"))}
    receipt["shared_qkv_verification"] = verify_shared_qkv(after)
    if args.vllm_dir:
        receipt["shared_qkv_stock_route_proof"] = verify_unused_kv_route(args.vllm_dir)
        from email.parser import Parser
        receipt["installed_packages"] = {}
        for package in ("vllm", "torch", "transformers", "gguf", "vllm_gguf_plugin"):
            candidates = list(args.vllm_dir.parent.glob(package + "-*.dist-info/METADATA"))
            if len(candidates) != 1:
                raise ValueError(f"ambiguous/missing installed {package} metadata")
            path = candidates[0]
            metadata = Parser().parsestr(path.read_text())
            receipt["installed_packages"][package] = {"version": metadata["Version"], "metadata_file": str(path.resolve()),
                                                       "metadata_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    receipt["prior_receipts"] = [{"file": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in args.prior_receipt]
    if args.cpu_qualification:
        qualification = json.loads(args.cpu_qualification.read_text())
        if qualification.get("passed") is not True or qualification.get("cuda_initialized") is not False or {row["layer"] for row in qualification["shared_qkv"]} != set(range(24, 42)):
            raise ValueError("CPU loader qualification is missing or did not pass all shared layers without CUDA")
        receipt["cpu_qualification"] = {"file": str(args.cpu_qualification.resolve()),
                                       "sha256": hashlib.sha256(args.cpu_qualification.read_bytes()).hexdigest(), "passed": True}
    if args.gguf:
        from prepare_vllm_gguf_reference import read_header
        header = read_header(args.gguf)
        receipt["tensor_coverage"] = verify_tensor_coverage(after, header)
        receipt["gguf"] = str(args.gguf.resolve())
        if args.metadata_dir:
            receipt["shared_qkv_derived_weights"] = derived_unused_weights(args.gguf, header, json.loads((args.metadata_dir / "config.json").read_text()))
    revision = subprocess.run(["git", "-C", str(args.plugin_dir), "rev-parse", "HEAD"], capture_output=True, text=True)
    receipt["upstream_revision"] = revision.stdout.strip() if revision.returncode == 0 else None
    receipt["execution_precision"] = {"vllm_qualified_activation_dtype": "float16; backend-specific accumulation/quantized activation paths",
        "vllm_qualified_kv_dtype": "float16", "learned_weights": "original GGUF packed bytes, no conversion",
        "comparison_caveat": "matched model/workload; runtime activation and accumulation arithmetic need not be identical to Antfly's F32 hidden state, Q8 dot paths and BF16 prefill mirrors"}
    verified_files = {str(target.resolve()): hashlib.sha256(after.encode()).hexdigest(),
                      str(params_target.resolve()): hashlib.sha256(params_after.encode()).hexdigest()}
    for row in receipt["prior_receipts"]:
        verified_files[row["file"]] = row["sha256"]
    if args.cpu_qualification:
        verified_files[str(args.cpu_qualification.resolve())] = receipt["cpu_qualification"]["sha256"]
    if args.vllm_dir:
        for name, digest in receipt["shared_qkv_stock_route_proof"]["source_sha256"].items():
            verified_files[str((args.vllm_dir / name).resolve())] = digest
        for row in receipt["installed_packages"].values():
            verified_files[row["metadata_file"]] = row["metadata_sha256"]
    receipt["verified_files"] = verified_files
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Persist the exact proposed change before mutating the dependency.
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    if after != before:
        target.write_text(after)
    if params_after != params_before:
        params_target.write_text(params_after)
    print(json.dumps({key: value for key, value in receipt.items() if key not in ("patch", "tensor_coverage", "shared_qkv_derived_weights", "replicated_loader")}, indent=2))


if __name__ == "__main__":
    main()
