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

"""Oracle independence and GGUF representation round trips."""

import unittest
from unittest.mock import patch

import numpy as np
import torch
from gemma4_cpu import decision_checks, teacher_forced_inputs
from gemma4_reference import (
    load_reference_model,
    official_source_identity,
    reference_weight,
)
from transformers import Gemma4Config, Gemma4ForCausalLM, Gemma4TextConfig
from transformers.models.gemma4.modeling_gemma4 import (
    Gemma4AudioAttention,
    Gemma4RMSNorm,
)


class OfficialReferenceTests(unittest.TestCase):
    def test_all_step_positions_match_separate_causal_prefixes(self):
        torch.manual_seed(17)
        config = Gemma4TextConfig(
            vocab_size=64,
            vocab_size_per_layer_input=64,
            hidden_size=32,
            intermediate_size=64,
            num_hidden_layers=4,
            num_attention_heads=4,
            num_key_value_heads=2,
            head_dim=8,
            global_head_dim=8,
            hidden_size_per_layer_input=4,
            sliding_window=4,
            num_kv_shared_layers=2,
            max_position_embeddings=32,
            pad_token_id=0,
            layer_types=["sliding_attention", "full_attention"] * 2,
        )
        config._attn_implementation = "eager"
        model = Gemma4ForCausalLM(config).eval().requires_grad_(False)
        ids = torch.tensor([[3, 4, 5, 6]])
        with torch.inference_mode():
            together = model(
                input_ids=ids, use_cache=False, logits_to_keep=torch.arange(1, 4)
            ).logits
            for index, length in enumerate((2, 3, 4)):
                separate = model(
                    input_ids=ids[:, :length], use_cache=False, logits_to_keep=1
                ).logits
                torch.testing.assert_close(
                    together[:, index], separate[:, -1], atol=1e-6, rtol=1e-5
                )

    def test_matching_endpoints_cannot_hide_intermediate_failure(self):
        report = decision_checks(np.eye(3, dtype=np.float32) * 2, [0, 0, 2])
        self.assertFalse(report["argmax_matches"])
        self.assertEqual(report["mismatch_steps"], [2])
        self.assertEqual(report["reference_argmax_gaps"], [0, 2, 0])

    def test_missing_intermediate_logits_cannot_pass(self):
        with self.assertRaisesRegex(ValueError, "coverage"):
            decision_checks(np.zeros((2, 3)), [0, 1, 2])

    def test_generated_label_is_excluded_from_teacher_forced_prefix(self):
        media = torch.ones(1, 3, 4)
        original = {
            "input_ids": torch.tensor([[2, 7]]),
            "attention_mask": torch.ones(1, 2, dtype=torch.int64),
            "mm_token_type_ids": torch.tensor([[1, 0]]),
            "input_features": media,
        }
        for tokens, expected_prefix, expected_step, expected_token in (
            ([8, 9, 106, 10], [2, 7, 8, 9], 3, 106),
            ([8, 9], [2, 7, 8], 2, 9),
        ):
            with self.subTest(tokens=tokens):
                inputs, step, token = teacher_forced_inputs(original, tokens, [1, 106])
                self.assertEqual(inputs["input_ids"].tolist(), [expected_prefix])
                self.assertEqual((step, token), (expected_step, expected_token))
                self.assertEqual(
                    inputs["attention_mask"].tolist(), [[1] * len(expected_prefix)]
                )
                self.assertEqual(
                    inputs["mm_token_type_ids"].tolist(),
                    [[1] + [0] * (len(expected_prefix) - 1)],
                )
                self.assertIs(inputs["input_features"], media)
        self.assertEqual(original["input_ids"].tolist(), [[2, 7]])

    def test_modified_runtime_method_cannot_be_an_oracle(self):
        with (
            patch.object(Gemma4RMSNorm, "forward", lambda self, value: value),
            self.assertRaisesRegex(ValueError, "monkeypatched"),
        ):
            official_source_identity()

    def test_constructing_oracle_preserves_official_forward_methods(self):
        norm, audio = Gemma4RMSNorm.forward, Gemma4AudioAttention.forward
        model = load_reference_model(
            Gemma4Config(text_config={}, vision_config={}, audio_config={}).to_dict()
        )
        self.assertIs(Gemma4RMSNorm.forward, norm)
        self.assertIs(Gemma4AudioAttention.forward, audio)
        self.assertEqual(
            model.forward.__module__, "transformers.models.gemma4.modeling_gemma4"
        )
        self.assertEqual(len(official_source_identity()["modeling_sha256"]), 64)

    def test_folded_audio_scale_round_trips_through_official_softplus(self):
        folded = torch.tensor([1e-8, 0.01, 0.1, 1.0, 8.0, 100.0])
        original = folded.clone()
        parameter = reference_weight(
            "model.audio_tower.layers.0.self_attn.per_dim_scale", folded
        )
        torch.testing.assert_close(
            torch.nn.functional.softplus(parameter), folded, rtol=2e-6, atol=1e-12
        )
        self.assertTrue(torch.equal(folded, original))
        self.assertIs(reference_weight("model.other.weight", folded), folded)

    def test_invalid_folded_scale_is_rejected(self):
        for value in (0.0, -1.0, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                reference_weight("x.self_attn.per_dim_scale", torch.tensor([value]))


if __name__ == "__main__":
    unittest.main()
