# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace

import pytest
import torch

from decoder_reference import (
    prepare_prompt,
    teacher_force,
    select_features,
    DEFAULT_QUESTION,
    fp16_roundtrip_cache,
)


class Tokenizer:
    pad_token = None
    eos_token = "<eos>"

    def add_special_tokens(self, value):
        assert value == {"additional_special_tokens": ["<|memory|>"]}

    def convert_tokens_to_ids(self, value):
        assert value == "<|memory|>"
        return 5

    def apply_chat_template(self, messages, **kwargs):
        assert kwargs == {
            "tokenize": False,
            "add_generation_prompt": True,
            "enable_thinking": False,
        }
        assert messages[1]["content"] == "Question?"
        return messages[0]["content"]

    def __call__(self, text, **kwargs):
        assert kwargs == {"return_tensors": "pt", "add_special_tokens": False}
        return {"input_ids": torch.tensor([[2] + [5] * text.count("<|memory|>") + [3]])}


def test_soft_injection_preserves_text_rows_and_upstream_rounding():
    embedding = torch.nn.Embedding(10, 6)
    memory = torch.arange(12, dtype=torch.float32).reshape(2, 6)
    ids, values, prompt = prepare_prompt(
        Tokenizer(), embedding, memory, system_prompt="System", question=" Question? "
    )
    original = embedding(ids)
    assert prompt["memory_positions"] == [1, 2]
    assert prompt["enable_thinking"] is False
    torch.testing.assert_close(values[[0, 3]], original[[0, 3]], rtol=0, atol=0)
    expected = original[1:3] + (memory - original[1:3])
    torch.testing.assert_close(values[1:3], expected, rtol=0, atol=0)
    assert embedding.num_embeddings == 10


@pytest.mark.parametrize(
    "system,question", [("<|memory|>", "Question?"), ("System", "<|memory|>")]
)
def test_user_supplied_placeholder_is_rejected(system, question):
    with pytest.raises(ValueError, match="literal memory placeholders"):
        prepare_prompt(
            Tokenizer(),
            torch.nn.Embedding(10, 6),
            torch.zeros(2, 6),
            system_prompt=system,
            question=question,
        )


def test_out_of_vocabulary_placeholder_does_not_resize_model():
    with pytest.raises(ValueError, match="outside the frozen"):
        prepare_prompt(
            Tokenizer(),
            torch.nn.Embedding(5, 6),
            torch.zeros(2, 6),
            system_prompt="System",
            question="Question?",
        )


def test_teacher_forcing_uses_each_previous_logit_row():
    class Model:
        calls = []

        def __call__(self, **kwargs):
            assert kwargs["use_cache"] is True
            step = len(self.calls)
            if step == 0:
                assert "inputs_embeds" in kwargs
            else:
                assert kwargs["input_ids"].tolist() == [[step]]
                assert kwargs["past_key_values"] == step
            self.calls.append(kwargs)
            logits = torch.zeros(1, 1, 8)
            logits[0, 0, step + 1] = 2
            return SimpleNamespace(logits=logits, past_key_values=step + 1)

    model = Model()
    prefill, ids, decode = teacher_force(model, torch.zeros(4, 6), 3)
    assert prefill.argmax().item() == 1
    assert ids.tolist() == [1, 2, 3]
    assert decode.argmax(dim=1).tolist() == [2, 3, 4]
    assert len(model.calls) == 4


def test_synthetic_feature_selection_is_explicit():
    inputs, question, provenance = select_features({"embedding_dim": 8}, 23, None, None)
    assert inputs.documents.shape == (1, 1, 8)
    assert question == DEFAULT_QUESTION
    assert provenance["feature_source"].startswith("synthetic-")
    assert torch.equal(inputs.documents, inputs.documents.bfloat16().float())


def test_fp16_cache_rounds_before_prefill_and_retains_rounded_history():
    cache = fp16_roundtrip_cache()
    key = torch.tensor([[[[0.1234567, -1.234567]]]], dtype=torch.float32)
    value = torch.tensor([[[[2.345678, -0.3456789]]]], dtype=torch.float32)
    original_key = key.clone()
    rounded_key, rounded_value = cache.update(key, value, 0)
    assert torch.equal(rounded_key, key.half().float())
    assert torch.equal(rounded_value, value.half().float())
    assert not torch.equal(rounded_key, key)
    assert torch.equal(key, original_key)
    appended_key, appended_value = cache.update(key * 3, value * 3, 0)
    assert torch.equal(
        appended_key, torch.cat((key.half().float(), (key * 3).half().float()), dim=2)
    )
    assert torch.equal(
        appended_value,
        torch.cat((value.half().float(), (value * 3).half().float()), dim=2),
    )


def test_teacher_forcing_rejects_unknown_cache_precision():
    with pytest.raises(ValueError, match="KV dtype"):
        teacher_force(None, torch.zeros(2, 3), 1, kv_dtype="bf16")


def test_real_encoder_features_use_the_same_question_in_decoder(monkeypatch, tmp_path):
    import feature_inputs

    sentinel = object()
    monkeypatch.setattr(
        feature_inputs,
        "load_encoder_features",
        lambda directory, config: (
            sentinel,
            "Encoded question?",
            {"feature_source": "encoder-reference"},
        ),
    )
    inputs, question, provenance = select_features({}, 23, tmp_path, None)
    assert inputs is sentinel
    assert question == "Encoded question?"
    assert provenance["feature_source"] == "encoder-reference"
    with pytest.raises(ValueError, match="differs from the encoded question"):
        select_features({}, 23, tmp_path, "A different question")
