# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Reference compatibility must retain all PLE weights and exact prompt metadata."""
from pathlib import Path
import copy
import struct
import tempfile
import unittest

from patch_vllm_gemma4_gguf import extract_mapper, patch_source, verify_maps, verify_tensor_coverage, patch_replicated_loader, verify_replicated_loader, verify_shared_qkv, SHARED_QKV_HELPERS
from prepare_vllm_gguf_reference import read_header, validate_config


UPSTREAM = '''GEMMA4_TEXT_SUBSTR: dict[str, str] = {
    "attn_q.": "self_attn.q_proj.", "attn_output.": "self_attn.o_proj.",
    "ffn_down.": "mlp.down_proj.", "ffn_gate_inp.": "router.proj.",
}
def build_gemma4_text_mapper() -> WeightsMapper:
    return WeightsMapper(
        orig_to_new_prefix={
            "token_embd.": "model.language_model.embed_tokens.",
            "blk.": "model.language_model.layers.",
        },
        orig_to_new_substr=GEMMA4_TEXT_SUBSTR,
    )
'''


def string(value):
    encoded = value.encode()
    return struct.pack("<Q", len(encoded)) + encoded


def semantic_fixture():
    metadata = {"general.architecture": "gemma4", "gemma4.block_count": 2, "gemma4.embedding_length": 4,
                "gemma4.embedding_length_per_layer_input": 2, "tokenizer.chat_template": "{{ exact_prompt }}",
                "tokenizer.ggml.tokens": {"element_type": 8, "length": 3},
                "gemma4.feed_forward_length": 8, "gemma4.attention.head_count": 2,
                "gemma4.attention.head_count_kv": 1, "gemma4.attention.sliding_window": 16,
                "gemma4.context_length": 128, "gemma4.attention.shared_kv_layers": 1,
                "gemma4.attention.key_length_swa": 2, "gemma4.attention.value_length_swa": 2,
                "gemma4.attention.key_length": 4, "gemma4.attention.value_length": 4,
                "gemma4.attention.sliding_window_pattern": [True, False],
                "gemma4.attention.layer_norm_rms_epsilon": 1e-6, "gemma4.final_logit_softcapping": 30.0,
                "gemma4.rope.freq_base": 1e6, "gemma4.rope.freq_base_swa": 10000.0,
                "gemma4.rope.dimension_count": 4, "gemma4.rope.dimension_count_swa": 2}
    tensors = {"per_layer_token_embd.weight": [4, 3], "per_layer_model_proj.weight": [4, 4], "per_layer_proj_norm.weight": [2]}
    for layer in range(2):
        tensors.update({f"blk.{layer}.inp_gate.weight": [4, 2], f"blk.{layer}.proj.weight": [2, 4], f"blk.{layer}.post_norm.weight": [4]})
    header = {"metadata": metadata,
              "tensors": {name: {"shape_gguf_order": shape, "ggml_type": 0, "offset": 0} for name, shape in tensors.items()},
              "rope_freqs": [1.0, 1e30]}
    header["tensors"]["rope_freqs.weight"] = {"shape_gguf_order": [2], "ggml_type": 0, "offset": 0}
    text = {"num_hidden_layers": 2, "hidden_size": 4, "hidden_size_per_layer_input": 2,
            "vocab_size": 3, "vocab_size_per_layer_input": 3, "intermediate_size": 8,
            "num_attention_heads": 2, "num_key_value_heads": 1, "num_global_key_value_heads": None,
            "sliding_window": 16, "max_position_embeddings": 128, "head_dim": 2, "global_head_dim": 4,
            "num_kv_shared_layers": 1, "layer_types": ["sliding_attention", "full_attention"],
            "rms_norm_eps": 1e-6, "final_logit_softcapping": 30.0, "hidden_activation": "gelu_pytorch_tanh",
            "attention_bias": False, "attention_k_eq_v": False, "enable_moe_block": False,
            "use_double_wide_mlp": False, "use_bidirectional_attention": None,
            "rope_parameters": {"full_attention": {"rope_type": "proportional", "rope_theta": 1e6, "partial_rotary_factor": 0.5},
                                "sliding_attention": {"rope_type": "default", "rope_theta": 10000.0}}}
    return header, {"model_type": "gemma4", "text_config": text}


def encoded_fixture(header):
    records = b""
    for key, value in header["metadata"].items():
        if key == "tokenizer.ggml.tokens":
            records += string(key) + struct.pack("<IIQ", 9, 8, 3) + b"".join(string(v) for v in ("a", "b", "c"))
            continue
        if isinstance(value, list):
            records += string(key) + struct.pack("<IIQ", 9, 7, len(value)) + struct.pack("<" + "?" * len(value), *value)
            continue
        kind, encoded = (8, string(value)) if isinstance(value, str) else ((6, struct.pack("<f", value)) if isinstance(value, float) else (4, struct.pack("<I", value)))
        records += string(key) + struct.pack("<I", kind) + encoded
    table = b""
    for name, tensor in header["tensors"].items():
        shape = tensor["shape_gguf_order"]
        table += string(name) + struct.pack("<I", len(shape)) + struct.pack("<" + "Q" * len(shape), *shape) + struct.pack("<IQ", tensor["ggml_type"], tensor["offset"])
    data = b"GGUF" + struct.pack("<IQQ", 3, len(header["tensors"]), len(header["metadata"])) + records + table
    return data + b"\0" * (-len(data) % 32) + struct.pack("<ff", *header["rope_freqs"])


class ReferenceCompatibilityTests(unittest.TestCase):
    def test_shared_qkv_preserves_q_and_rejects_missing_learned_weights_wrong_shapes_and_types(self):
        result = verify_shared_qkv(SHARED_QKV_HELPERS)
        self.assertEqual(result["shared_q_only_layers"], list(range(24, 42)))
        self.assertTrue(result["learned_q_identity_preserved"])
    def test_replicated_linear_uses_existing_packed_materialization_and_fails_closed(self):
        source = '''from vllm.model_executor.layers.fused_moe import RoutedExperts
def _resolve_gguf_weight_loader(layer, fallback_weight_loader=None):
    return (
        layer.weight_loader_v2
        if hasattr(layer, "weight_loader_v2")
        else fallback_weight_loader
    )
'''
        patched = patch_replicated_loader(source)
        self.assertTrue(verify_replicated_loader(patched)["replicated_packed_data_preserved"])
        self.assertEqual(patch_replicated_loader(patched), patched)
        with self.assertRaises(ValueError):
            patch_replicated_loader(patched.replace("from vllm.model_executor.layers.linear import ReplicatedLinear\n", ""))
        with self.assertRaises(ValueError):
            patch_replicated_loader(source.replace("    return (", "    return("))

    def test_patch_preserves_existing_projections_and_maps_all_layer_boundaries(self):
        patched = patch_source(UPSTREAM)
        checks = verify_maps(patched)
        self.assertEqual(checks["blk.41.proj.weight"], "model.language_model.layers.41.per_layer_projection.weight")
        self.assertEqual(extract_mapper(patched).map("blk.41.attn_q.weight"), "model.language_model.layers.41.self_attn.q_proj.weight")
        self.assertEqual(patch_source(patched), patched)
        with self.assertRaises(ValueError):
            verify_maps(UPSTREAM)

    def test_partial_or_changed_upstream_mapping_fails_closed(self):
        with self.assertRaises(ValueError):
            patch_source(UPSTREAM.replace('"token_embd.":', '"per_layer_token_embd.":'))
        with self.assertRaises(ValueError):
            patch_source(UPSTREAM.replace("orig_to_new_prefix={", "orig_to_new_prefix = {"))

    def test_full_tensor_coverage_rejects_unmapped_weights_and_duplicate_destinations(self):
        source = patch_source(UPSTREAM)
        header = {"tensors": {name: {} for name in ("token_embd.weight", "blk.0.proj.weight", "rope_freqs.weight")}}
        result = verify_tensor_coverage(source, header)
        self.assertEqual((result["tensors"], result["mapped_count"]), (3, 2))
        self.assertEqual(result["intentionally_unmapped"], ["rope_freqs.weight"])
        header["tensors"]["unknown.weight"] = {}
        with self.assertRaisesRegex(ValueError, "unmapped learned"):
            verify_tensor_coverage(source, header)
        del header["tensors"]["unknown.weight"]
        header["tensors"].update({"blk.0.attn_q.weight": {}, "blk.0.self_attn.q_proj.weight": {}})
        with self.assertRaisesRegex(ValueError, "same HF tensor"):
            verify_tensor_coverage(source, header)

    def test_metadata_extracts_template_pattern_and_only_tiny_rotary_payload(self):
        header, config = semantic_fixture()
        payload = encoded_fixture(header)
        with tempfile.TemporaryDirectory() as temporary:
            file = Path(temporary) / "fixture.gguf"
            file.write_bytes(payload)
            parsed = read_header(file)
            self.assertEqual(parsed["metadata"]["tokenizer.chat_template"], "{{ exact_prompt }}")
            self.assertEqual(parsed["metadata"]["gemma4.attention.sliding_window_pattern"], [True, False])
            self.assertEqual(parsed["rope_freqs"][0], 1.0)
            self.assertGreater(parsed["rope_freqs"][1], 1e20)
            # Decimal JSON epsilon agrees with its rounded GGUF float32 value.
            self.assertEqual(len(validate_config(parsed, config)), 9)
            parsed["tensors"]["blk.1.proj.weight"]["shape_gguf_order"] = [4, 2]
            with self.assertRaisesRegex(ValueError, "PLE tensor"):
                validate_config(parsed, config)
            file.write_bytes(payload[:-1])
            with self.assertRaisesRegex(ValueError, "rotary factors"):
                read_header(file)

    def test_semantic_configuration_mismatches_fail_before_reference_launch(self):
        header, config = semantic_fixture()
        mutations = [
            (("rms_norm_eps",), 0.1), (("sliding_window",), 1),
            (("rope_parameters", "full_attention", "rope_theta"), 10),
            (("rope_parameters", "sliding_attention", "rope_theta"), 10),
            (("rope_parameters", "full_attention", "partial_rotary_factor"), 1.0),
            (("rope_parameters", "full_attention", "rope_type"), "default"),
            (("rope_parameters", "full_attention", "attention_factor"), 2.0),
            (("rope_parameters", "sliding_attention", "partial_rotary_factor"), 0.5),
            (("num_attention_heads",), 1), (("num_key_value_heads",), 2),
            (("num_global_key_value_heads",), 2), (("head_dim",), 4),
            (("global_head_dim",), 8), (("intermediate_size",), 16),
            (("max_position_embeddings",), 256), (("num_kv_shared_layers",), 0),
            (("final_logit_softcapping",), 0),
            (("layer_types",), ["full_attention", "sliding_attention"]),
            (("hidden_activation",), "relu"), (("attention_bias",), True),
            (("attention_k_eq_v",), True), (("enable_moe_block",), True),
            (("use_double_wide_mlp",), True), (("use_bidirectional_attention",), True),
            (("use_second_mlp_block",), True), (("attn_logit_softcapping",), 30.0),
        ]
        for path, value in mutations:
            with self.subTest(path=path):
                candidate = copy.deepcopy(config)
                target = candidate["text_config"]
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                with self.assertRaises(ValueError):
                    validate_config(header, candidate)

    def test_missing_or_unsupported_gguf_semantics_fail_closed(self):
        header, config = semantic_fixture()
        for key in ("gemma4.attention.layer_norm_rms_epsilon", "gemma4.attention.sliding_window_pattern",
                    "gemma4.rope.freq_base", "gemma4.rope.dimension_count"):
            with self.subTest(missing=key):
                candidate = copy.deepcopy(header)
                del candidate["metadata"][key]
                with self.assertRaises(ValueError):
                    validate_config(candidate, config)
        for factors in ([1.0, 2.0], [1.0, float("inf")], [1e30, 1.0], [1.0], [1.0, 1e30, 1e30]):
            with self.subTest(factors=factors):
                candidate = copy.deepcopy(header)
                candidate["rope_freqs"] = factors
                with self.assertRaises(ValueError):
                    validate_config(candidate, config)
        for key, value in (("gemma4.attention.value_length", 2),
                           ("gemma4.rope.scaling.type", "linear"),
                           ("gemma4.attention.sliding_window_pattern", [1, False])):
            with self.subTest(metadata=key):
                candidate = copy.deepcopy(header)
                candidate["metadata"][key] = value
                with self.assertRaises(ValueError):
                    validate_config(candidate, config)
        candidate = copy.deepcopy(header)
        del candidate["rope_freqs"]
        with self.assertRaisesRegex(ValueError, "payload was not validated"):
            validate_config(candidate, config)

    def test_full_rotary_mask_and_unscaled_default_are_equivalent(self):
        header, config = semantic_fixture()
        config["text_config"]["rope_parameters"]["full_attention"] = {"rope_type": "default", "rope_theta": 1e6}
        header["rope_freqs"] = [1.0, 1.0]
        self.assertEqual(len(validate_config(header, config)), 9)
        del header["rope_freqs"]
        del header["tensors"]["rope_freqs.weight"]
        self.assertEqual(len(validate_config(header, config)), 9)

    def test_partial_rotary_factor_must_preserve_backend_integer_pair_count(self):
        header, config = semantic_fixture()
        header["metadata"].update({"gemma4.attention.key_length": 512,
                                    "gemma4.attention.value_length": 512,
                                    "gemma4.rope.dimension_count": 512})
        header["tensors"]["rope_freqs.weight"]["shape_gguf_order"] = [256]
        header["rope_freqs"] = [1.0] * 64 + [1e30] * 192
        config["text_config"]["global_head_dim"] = 512
        rope = config["text_config"]["rope_parameters"]["full_attention"]
        rope["partial_rotary_factor"] = 0.25
        self.assertEqual(len(validate_config(header, config)), 9)
        # This passes a relative-tolerance comparison with .25, but vLLM
        # truncates to 63 frequency pairs instead of GGUF's 64 pairs.
        rope["partial_rotary_factor"] = 0.24999999
        with self.assertRaisesRegex(ValueError, "effective active rotary"):
            validate_config(header, config)
        for factor in (0, -0.1, 1e-10, float("nan"), float("inf")):
            with self.subTest(factor=factor):
                rope["partial_rotary_factor"] = factor
                with self.assertRaises(ValueError):
                    validate_config(header, config)
        header["rope_freqs"] = [1.0] * 256
        rope["partial_rotary_factor"] = 1.0000001
        with self.assertRaisesRegex(ValueError, "invalid partial rotary"):
            validate_config(header, config)


if __name__ == "__main__":
    unittest.main()
