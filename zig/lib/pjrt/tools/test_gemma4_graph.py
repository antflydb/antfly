# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Independent CPU checks for the export adapter's functional KV/PLE behavior."""

import copy
import types
import unittest

import torch
from gemma4_graph import (
    cache_state,
    make_multimodal_step,
    make_step,
    media_metadata,
    state_shapes,
)
from transformers import Gemma4ForCausalLM, Gemma4TextConfig


class Wrapper(torch.nn.Module):
    def __init__(self, reference):
        super().__init__()
        self.config = types.SimpleNamespace(
            text_config=reference.config,
            image_token_id=63,
            audio_token_id=62,
            video_token_id=61,
        )
        self.model = torch.nn.Module()
        self.model.language_model = copy.deepcopy(reference.model)
        self.lm_head = copy.deepcopy(reference.lm_head)


class FunctionalCacheTests(unittest.TestCase):
    def test_original_text_runner_still_matches_official_cache(self):
        torch.set_num_threads(1)
        torch.manual_seed(11)
        cfg = Gemma4TextConfig(
            vocab_size=64,
            vocab_size_per_layer_input=64,
            hidden_size=32,
            intermediate_size=64,
            num_hidden_layers=2,
            num_attention_heads=4,
            num_key_value_heads=2,
            head_dim=8,
            global_head_dim=8,
            sliding_window=8,
            num_kv_shared_layers=0,
            hidden_size_per_layer_input=4,
            layer_types=["sliding_attention", "full_attention"],
            max_position_embeddings=32,
            pad_token_id=0,
        )
        cfg._attn_implementation = "eager"
        reference = Gemma4ForCausalLM(cfg).eval().requires_grad_(False)
        model = copy.deepcopy(reference)
        params = {**dict(model.named_parameters()), **dict(model.named_buffers())}
        state = tuple(value.float() for value in cache_state(cfg, 16, "cpu"))
        position = torch.tensor(0, dtype=torch.int64)
        step = make_step(model, 16)
        cache = None
        with torch.inference_mode():
            for ids in ([[5, 6, 7]], [[8]], [[9]]):
                tokens = torch.tensor(ids, dtype=torch.int64)
                expected = reference(
                    input_ids=tokens,
                    past_key_values=cache,
                    use_cache=True,
                    logits_to_keep=1,
                )
                actual, state, position = step(params, state, tokens, position)
                torch.testing.assert_close(
                    actual, expected.logits[:, -1].float(), atol=2e-6, rtol=2e-6
                )
                cache = expected.past_key_values

    def test_prefill_and_decode_match_official_cache_across_sliding_wraps(self):
        torch.set_num_threads(1)
        for window, n in ((4, 1), (4, 7), (8, 12)):
            torch.manual_seed(11)
            cfg = Gemma4TextConfig(
                vocab_size=64,
                vocab_size_per_layer_input=64,
                hidden_size=32,
                intermediate_size=64,
                num_hidden_layers=4,
                num_attention_heads=4,
                num_key_value_heads=2,
                head_dim=8,
                global_head_dim=8,
                sliding_window=window,
                num_kv_shared_layers=2,
                hidden_size_per_layer_input=4,
                layer_types=[
                    "sliding_attention",
                    "full_attention",
                    "sliding_attention",
                    "full_attention",
                ],
                max_position_embeddings=128,
                pad_token_id=0,
                bos_token_id=1,
                eos_token_id=2,
            )
            cfg._attn_implementation = "eager"
            reference = Gemma4ForCausalLM(cfg).eval().requires_grad_(False)
            model = Wrapper(reference).eval()
            params = {**dict(model.named_parameters()), **dict(model.named_buffers())}
            ids = torch.tensor([[5 + i % 20 for i in range(n)]], dtype=torch.int64)
            values = {"input_ids": ids}
            meta = media_metadata(model, values)
            shapes = state_shapes(cfg, 32)
            state = tuple(torch.zeros(shape) for shape in shapes)
            prefill = make_multimodal_step(model, 32, meta, True, logits=True)
            decode = make_multimodal_step(model, 32, meta, False, logits=True)
            with torch.inference_mode():
                expected = reference(input_ids=ids, use_cache=True, logits_to_keep=1)
                actual, state, pos = prefill(
                    params, state, (ids,), torch.tensor(0, dtype=torch.int64)
                )
                torch.testing.assert_close(
                    actual, expected.logits[:, -1].float(), atol=2e-6, rtol=2e-6
                )
                cache = expected.past_key_values
                for step in range(10):
                    token = torch.tensor([[7 + step]], dtype=torch.int64)
                    expected = reference(
                        input_ids=token,
                        past_key_values=cache,
                        use_cache=True,
                        logits_to_keep=1,
                    )
                    actual, state, pos = decode(params, state, (token,), pos)
                    torch.testing.assert_close(
                        actual, expected.logits[:, -1].float(), atol=2e-6, rtol=2e-6
                    )
                    cache = expected.past_key_values
            self.assertEqual(int(pos), n + 10)
            self.assertEqual(state[0].shape[2], window)
            self.assertEqual(state[2].shape[2], 32)


if __name__ == "__main__":
    unittest.main()
