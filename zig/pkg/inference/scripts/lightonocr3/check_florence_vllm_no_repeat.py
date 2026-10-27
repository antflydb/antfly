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

"""Check the V1 comparison sampler against Transformers' standard ngram mask."""

import argparse
import json
from types import SimpleNamespace

import numpy as np

import torch
from transformers.generation.logits_process import NoRepeatNGramLogitsProcessor
from vllm import SamplingParams
from vllm.v1.sample.logits_processor.interface import BatchUpdate, MoveDirectionality

from florence_vllm_no_repeat import (
    NoRepeatNgramLogitsProcessor,
    NoRepeatNgramLogitsProcessorV2,
)


def check_v2(device):
    histories = torch.full((7, 256), 18, dtype=torch.int32, device=device)
    prompt = torch.full((7,), 5, dtype=torch.int32, device=device)
    lengths = prompt.clone()
    state = SimpleNamespace(
        device=device,
        max_num_reqs=7,
        vocab_size=19,
        all_token_ids=SimpleNamespace(gpu=histories),
        prompt_len=SimpleNamespace(gpu=prompt),
        total_len=SimpleNamespace(gpu=lengths),
    )
    processor = NoRepeatNgramLogitsProcessorV2(None, state)
    cases = 0
    for size in (2, 3, 4, 5):
        outputs = {slot: [] for slot in (1, 4, 6)}
        params = SamplingParams(
            temperature=0,
            extra_args={
                "ocr_no_repeat_ngram_size": size,
                "ocr_decoder_prefix": [2, 0],
            },
        )
        for slot in outputs:
            assert processor.add_request(slot, params)
        disabled = SamplingParams(temperature=0)
        assert not processor.add_request(3, disabled)
        processor.apply_staged_writes()
        oracle = NoRepeatNGramLogitsProcessor(size)
        for step in range(128):
            mapping = [6, 3, 1, 4] if step % 2 else [4, 1, 3, 6]
            ctx = SimpleNamespace(
                idx_mapping_np=np.array(mapping),
                expanded_idx_mapping=torch.tensor(mapping, device=device),
            )
            logits = torch.randn(len(mapping), 19, device=device)
            expected = logits.clone()
            for row, slot in enumerate(mapping):
                if slot == 3:
                    continue
                out = outputs[slot]
                lengths[slot] = 5 + len(out)
                if out:
                    histories[slot, 5 : 5 + len(out)] = torch.tensor(out, device=device)
                ids = torch.tensor([[2, 0, *out]], device=device)
                expected[row : row + 1] = oracle(ids, expected[row : row + 1])
            torch.testing.assert_close(
                processor.apply(logits, ctx), expected, rtol=0, atol=0
            )
            cases += 1
            for slot, out in outputs.items():
                out.append((step % 7 + slot * 3) % 19)
        # Recycle a populated slot and ensure the previous policy is gone.
        assert not processor.add_request(4, disabled)
        processor.apply_staged_writes()
        assert processor.device_sizes[4].item() == 0
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    args = parser.parse_args()
    device = torch.device(args.device)
    torch.manual_seed(91)
    cases = 0
    for size in (2, 3, 4, 5):
        processor = NoRepeatNgramLogitsProcessor(None, device, False)
        params = SamplingParams(
            temperature=0,
            extra_args={
                "ocr_no_repeat_ngram_size": size,
                "ocr_decoder_prefix": [2, 0],
            },
        )
        processor.validate_params(params)
        outputs = [[], []]
        processor.update_state(
            BatchUpdate(
                2, [], [(i, params, None, out) for i, out in enumerate(outputs)], []
            )
        )
        oracle = NoRepeatNGramLogitsProcessor(size)
        mapping = [0, 1]

        def compare():
            nonlocal cases
            logits = torch.randn(len(mapping), 19, device=device)
            expected = logits.clone()
            for row, original in enumerate(mapping):
                ids = torch.tensor([[2, 0, *outputs[original]]], device=device)
                expected[row : row + 1] = oracle(ids, expected[row : row + 1])
            actual = processor.apply(logits.clone())
            torch.testing.assert_close(actual, expected, rtol=0, atol=0)
            cases += 1

        for step in range(128):
            compare()
            for original, out in enumerate(outputs):
                # Long repeated subsequences plus distinct page histories.
                out.append((step % 7 + original * 3) % 19)
            processor.update_state(None)
        processor.update_state(
            BatchUpdate(2, [], [], [(0, 1, MoveDirectionality.SWAP)])
        )
        mapping = [1, 0]
        compare()
        processor.update_state(
            BatchUpdate(1, [0], [], [(1, 0, MoveDirectionality.UNIDIRECTIONAL)])
        )
        mapping = [0]
        compare()
        disabled = SamplingParams(temperature=0)
        processor.update_state(BatchUpdate(1, [0], [(0, disabled, [2, 0], [])], []))
        logits = torch.randn(1, 19, device=device)
        torch.testing.assert_close(
            processor.apply(logits.clone()), logits, rtol=0, atol=0
        )
        assert not processor.requests
    if args.device == "cuda":
        cases += check_v2(device)
    print(
        json.dumps(
            dict(
                status="passed",
                device=args.device,
                oracle_cases=cases,
                covers=[
                    "live output references",
                    "prefix tokens",
                    "batch swap",
                    "batch compaction",
                    "removal",
                    "disabled policy",
                    "V2 device histories and persistent slot reuse"
                    if args.device == "cuda"
                    else "V1 CPU oracle",
                ],
            )
        )
    )


if __name__ == "__main__":
    main()
