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

from pathlib import Path
from types import SimpleNamespace
import json
import os

import pytest
import torch

from decoder_reference import prepare_prompt
from encoder_reference import normalize_features
from exporter import RELEASE_CONFIG
from quality_cases import load_suite
from released_reference import (
    CASE_IDS,
    EOS_IDS,
    SYSTEM,
    cache_dtype,
    checkpoint_proof,
    conditioning_features,
    greedy,
    numpy_storage,
    selected_cases,
    source_recipe,
)


def test_fixed_cases_and_no_prompt_adaptation():
    cases = selected_cases(load_suite())
    assert tuple(c["id"] for c in cases) == CASE_IDS
    assert cases[0]["question"] == cases[1]["question"]
    assert cases[2]["question"] == cases[3]["question"]
    assert SYSTEM == "You are a helpful assistant."


def test_fp16_intermediate_is_not_direct_bf16_rounding():
    raw = torch.tensor([[1.00392, 1e-8, -1e-8]], dtype=torch.float32)
    stored, memory = numpy_storage(raw)
    assert stored.dtype == torch.float16 and memory.dtype == torch.bfloat16
    torch.testing.assert_close(memory, raw.half().bfloat16(), atol=0, rtol=0)
    assert not torch.equal(memory, raw.bfloat16())
    assert memory[0, 1] == 0  # BF16 itself could preserve this small value.
    assert raw.bfloat16()[0, 1] != 0


def test_redundant_fp16_cache_write_is_idempotent():
    x = torch.randn(3, 2560, generator=torch.Generator().manual_seed(123)).bfloat16()
    stored, memory = numpy_storage(x)
    assert torch.equal(stored.view(torch.int16), stored.half().view(torch.int16))
    assert torch.equal(memory, stored.bfloat16())


@pytest.mark.parametrize(
    "profile,dtype", [("float32", torch.float32), ("bfloat16", torch.bfloat16)]
)
def test_reader_conditioning_dtype_and_storage(profile, dtype):
    x = torch.tensor([[0.123, -0.456]], dtype=dtype)
    stored, reader = conditioning_features(x, profile)
    assert stored.dtype == torch.float16
    assert reader.dtype == dtype
    torch.testing.assert_close(
        reader, x.float().half().bfloat16().to(dtype), atol=0, rtol=0
    )


def test_bf16_normalization_must_precede_widening():
    hidden = torch.randn(
        1, 2, 2560, generator=torch.Generator().manual_seed(192)
    ).bfloat16()
    mask = torch.tensor([[1, 1]])
    bf16 = normalize_features(hidden, mask)
    f32 = normalize_features(hidden.float(), mask)
    assert bf16.dtype == torch.bfloat16 and f32.dtype == torch.float32
    assert not torch.equal(bf16.float(), f32)


@pytest.mark.parametrize("bad", [torch.tensor([[float("nan")]]), torch.tensor([1.0])])
def test_invalid_pooled_features_rejected(bad):
    with pytest.raises(ValueError):
        numpy_storage(bad)


def test_profile_dtype_mismatch_rejected():
    with pytest.raises(ValueError, match="arithmetic"):
        conditioning_features(torch.ones(1, 2), "bfloat16")


def test_bf16_injection_preserves_delta_scatter_rounding():
    class Tok:
        pad_token = None
        eos_token = "EOS"

        def add_special_tokens(self, _):
            pass

        def convert_tokens_to_ids(self, _):
            return 3

        def apply_chat_template(self, *args, **kwargs):
            return "rendered"

        def __call__(self, *args, **kwargs):
            return {"input_ids": torch.tensor([[1, 3, 3, 2]])}

    lookup = torch.nn.Embedding(8, 4, dtype=torch.bfloat16)
    with torch.no_grad():
        lookup.weight.fill_(3.14)
    memory = torch.full((2, 4), 0.1, dtype=torch.bfloat16)
    ids, actual, _ = prepare_prompt(
        Tok(), lookup, memory, system_prompt=SYSTEM, question="q"
    )
    base = lookup(ids)
    expected = base.clone()
    expected[[1, 2]] = base[[1, 2]] + (memory - base[[1, 2]])
    assert actual.dtype == torch.bfloat16
    torch.testing.assert_close(actual, expected, atol=0, rtol=0)
    assert not torch.equal(actual[[1, 2]], memory)


class Model:
    def __init__(self, tokens, dtype):
        self.tokens = iter(tokens)
        self.dtype = dtype
        self.calls = 0

    def __call__(self, **kwargs):
        self.calls += 1
        logits = torch.zeros(1, 1, 151936, dtype=self.dtype)
        logits[0, 0, next(self.tokens)] = 2
        cache = ((torch.zeros(1, dtype=self.dtype), torch.zeros(1, dtype=self.dtype)),)
        return SimpleNamespace(logits=logits, past_key_values=cache)


class Tokenizer:
    def decode(self, ids, **kwargs):
        assert kwargs["skip_special_tokens"] is True
        return " ".join(str(i) for i in ids if i not in EOS_IDS)


@pytest.mark.parametrize("dtype", [torch.float32, torch.bfloat16])
@pytest.mark.parametrize("eos", EOS_IDS)
def test_greedy_uses_native_eos_and_full_logits(dtype, eos):
    model = Model([42, eos], dtype)
    answer, logits = greedy(
        model, Tokenizer(), torch.zeros(3, 4, dtype=dtype), 48, dtype
    )
    assert answer["token_ids"] == [42, eos]
    assert answer["text"] == "42" and answer["finish_reason"] == "eos"
    assert model.calls == 2 and logits.shape == (2, 151936)
    assert logits.dtype == torch.float32  # Reporting widens, model arithmetic does not.
    assert all(row["margin"] == 2 for row in answer["top_margins"])


def test_greedy_maximum_not_extra_forward():
    model = Model([42, 43], torch.float32)
    answer, _ = greedy(model, Tokenizer(), torch.zeros(3, 4), 2, torch.float32)
    assert answer["token_ids"] == [42, 43] and answer["finish_reason"] == "length"
    assert model.calls == 2


def test_cache_precision_mismatch_rejected():
    with pytest.raises(ValueError, match="precision"):
        cache_dtype(((torch.zeros(1), torch.zeros(1)),), torch.bfloat16)


def test_checkpoint_wrong_digest_rejected_before_loading(tmp_path):
    (tmp_path / "config.json").write_text(json.dumps(RELEASE_CONFIG))
    (tmp_path / "memory.pt").write_bytes(b"untrusted checkpoint")
    with pytest.raises(ValueError, match="pinned Stage 3"):
        checkpoint_proof(tmp_path)


@pytest.mark.skipif(
    not os.environ.get("MEMORILLA_SOURCE_ROOT"),
    reason="optional unchanged upstream source",
)
def test_original_source_recipe_is_pinned():
    proof = source_recipe(Path(os.environ["MEMORILLA_SOURCE_ROOT"]))
    assert proof["prompt"] == SYSTEM and proof["memory_slots"] == 16
    assert "NumPy FP16" in proof["feature_recipe"]
    assert len(proof["files_sha256"]) == 6
