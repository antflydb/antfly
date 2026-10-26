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

"""Standard no-repeat ngram sampling through vLLM's V1 logits processor API.

Used only to compare Florence with native Antfly's decoder policy. The model,
attention, scheduling, and graph execution remain in the official vLLM engine.
"""

from dataclasses import dataclass, field

import torch
import triton
import triton.language as tl
from vllm.v1.worker.gpu.sample.logits_processor import (
    LogitsProcessor as LogitsProcessorV2,
)
from vllm.v1.sample.logits_processor.interface import (
    LogitsProcessor,
    MoveDirectionality,
)


@dataclass
class Request:
    size: int
    prefix: list[int]
    outputs: list[int]
    consumed: int = 0
    history: list[int] = field(default_factory=list)
    ngrams: dict[tuple[int, ...], set[int]] = field(default_factory=dict)

    def append(self, token):
        self.history.append(token)
        if len(self.history) >= self.size:
            key = tuple(self.history[-self.size : -1])
            self.ngrams.setdefault(key, set()).add(token)

    def advance(self):
        if len(self.outputs) < self.consumed:
            raise ValueError("speculative rollback is outside this OCR comparison")
        for token in self.outputs[self.consumed :]:
            self.append(token)
        self.consumed = len(self.outputs)

    def banned(self):
        self.advance()
        if len(self.history) < self.size - 1:
            return ()
        return self.ngrams.get(tuple(self.history[-(self.size - 1) :]), ())


class NoRepeatNgramLogitsProcessor(LogitsProcessor):
    @classmethod
    def validate_params(cls, sampling_params):
        size = (sampling_params.extra_args or {}).get("ocr_no_repeat_ngram_size", 0)
        if not isinstance(size, int) or size < 0 or size == 1:
            raise ValueError("OCR no-repeat size must be zero or an integer >= 2")

    def __init__(self, vllm_config, device, is_pin_memory):
        self.device = device
        self.requests = {}

    def is_argmax_invariant(self):
        return False

    def update_state(self, batch_update):
        if batch_update is None:
            return
        for index in batch_update.removed:
            self.requests.pop(index, None)
        for index, params, prompt, outputs in batch_update.added:
            self.requests.pop(index, None)
            extra = params.extra_args or {}
            size = extra.get("ocr_no_repeat_ngram_size", 0)
            if size:
                prefix = extra.get("ocr_decoder_prefix", prompt)
                if prefix is None:
                    raise ValueError("Florence decoder prefix is required")
                request = Request(size, list(prefix), outputs)
                for token in prefix:
                    request.append(token)
                self.requests[index] = request
        for source, destination, direction in batch_update.moved:
            first = self.requests.pop(source, None)
            second = self.requests.pop(destination, None)
            if first is not None:
                self.requests[destination] = first
            if direction == MoveDirectionality.SWAP and second is not None:
                self.requests[source] = second

    def apply(self, logits):
        rows, tokens = [], []
        for index, request in self.requests.items():
            if not 0 <= index < logits.shape[0]:
                raise ValueError(
                    "vLLM no-repeat request index is outside the live batch"
                )
            for token in request.banned():
                rows.append(index)
                tokens.append(token)
        if rows:
            row_ids = torch.tensor(rows, dtype=torch.long, device=logits.device)
            token_ids = torch.tensor(tokens, dtype=torch.long, device=logits.device)
            logits[row_ids, token_ids] = -torch.inf
        return logits


# Model Runner V2 keeps committed histories in persistent device slots. The
# standard mask below reads those histories directly, without copying logits
# or generated tokens to the host. V1 remains available for older engines.


@triton.jit
def _ngram_mask_v2(
    logits,
    logits_stride,
    mapping,
    histories,
    history_stride,
    prompt_lengths,
    total_lengths,
    sizes,
    prefixes,
    BLOCK: tl.constexpr,
):
    row = tl.program_id(0)
    slot = tl.load(mapping + row)
    size = tl.load(sizes + slot)
    if size == 0:
        return
    prompt = tl.load(prompt_lengths + slot)
    output_len = tl.load(total_lengths + slot) - prompt
    length = output_len + 2
    positions = tl.arange(0, BLOCK)
    valid = positions + size <= length
    matches = valid
    for offset in range(size - 1):
        old_pos = positions + offset
        tail_pos = length - size + 1 + offset
        old_prefix = tl.load(prefixes + slot * 2 + old_pos, old_pos < 2, 0)
        old_token = tl.load(
            histories + slot * history_stride + prompt + old_pos - 2,
            (old_pos >= 2) & (old_pos < length),
            0,
        )
        old = tl.where(old_pos < 2, old_prefix, old_token)
        tail_prefix = tl.load(
            prefixes + slot * 2 + tail_pos, (tail_pos >= 0) & (tail_pos < 2), 0
        )
        tail_token = tl.load(
            histories + slot * history_stride + prompt + tail_pos - 2,
            (tail_pos >= 2) & (tail_pos < length),
            0,
        )
        tail = tl.where(tail_pos < 2, tail_prefix, tail_token)
        matches &= old == tail
    banned_pos = positions + size - 1
    banned_prefix = tl.load(prefixes + slot * 2 + banned_pos, banned_pos < 2, 0)
    banned_output = tl.load(
        histories + slot * history_stride + prompt + banned_pos - 2,
        (banned_pos >= 2) & (banned_pos < length),
        0,
    )
    banned = tl.where(banned_pos < 2, banned_prefix, banned_output)
    tl.store(logits + row * logits_stride + banned, -float("inf"), matches)


class NoRepeatNgramLogitsProcessorV2(LogitsProcessorV2):
    validate_params = NoRepeatNgramLogitsProcessor.validate_params

    def __init__(self, vllm_config, req_states):
        self.req_states = req_states
        self.sizes = torch.zeros(req_states.max_num_reqs, dtype=torch.int32)
        self.prefixes = torch.zeros((req_states.max_num_reqs, 2), dtype=torch.int32)
        self.device_sizes = self.sizes.to(req_states.device)
        self.device_prefixes = self.prefixes.to(req_states.device)
        self.dirty = False

    def add_request(self, req_idx, sampling_params):
        self.validate_params(sampling_params)
        extra = sampling_params.extra_args or {}
        size = extra.get("ocr_no_repeat_ngram_size", 0)
        prefix = extra.get("ocr_decoder_prefix", [])
        if size and len(prefix) != 2:
            raise ValueError(
                "Florence comparison requires its two decoder prefix tokens"
            )
        self.sizes[req_idx] = size
        self.prefixes[req_idx] = torch.tensor(prefix if size else [0, 0])
        self.dirty = True
        return bool(size)

    def apply_staged_writes(self):
        if self.dirty:
            self.device_sizes.copy_(self.sizes)
            self.device_prefixes.copy_(self.prefixes)
            self.dirty = False

    def apply(self, logits, ctx):
        if logits.shape[0] != len(ctx.idx_mapping_np):
            raise ValueError("speculative decoding is outside this OCR comparison")
        state = self.req_states
        _ngram_mask_v2[(logits.shape[0],)](
            logits,
            logits.stride(0),
            ctx.expanded_idx_mapping,
            state.all_token_ids.gpu,
            state.all_token_ids.gpu.stride(0),
            state.prompt_len.gpu,
            state.total_len.gpu,
            self.device_sizes,
            self.device_prefixes,
            BLOCK=triton.next_power_of_2(state.all_token_ids.gpu.shape[1] + 2),
        )
        return logits
