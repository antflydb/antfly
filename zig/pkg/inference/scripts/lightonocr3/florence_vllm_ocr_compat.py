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

"""Diagnostic OCR-only input adapter for bart-plugin 0.6.2 and vLLM 0.32.

Keeps the official plugin's model, weights and execution unchanged. Uses the
original checkpoint's image processor and tokenizer; spans the complete encoder
output with a multimodal placeholder, including the OCR task tokens.
"""

import json
from functools import lru_cache
from pathlib import Path


def install(model_dir):
    import torch
    from transformers import AutoImageProcessor
    from transformers.feature_extraction_utils import BatchFeature
    from vllm.multimodal.processing import PromptReplacement
    import vllm_bart_plugin.florence2 as flo

    @lru_cache(maxsize=1)
    def image_processor():
        return AutoImageProcessor.from_pretrained(
            str(Path(model_dir).resolve()), use_fast=False, trust_remote_code=False
        )

    task = "What is the text in the image?"
    text_config = json.loads((Path(model_dir) / "config.json").read_text())[
        "text_config"
    ]
    decoder_prefix = [text_config["decoder_start_token_id"]]
    if text_config.get("forced_bos_token_id") is not None:
        decoder_prefix.append(text_config["forced_bos_token_id"])

    def task_ids(self):
        return self.info.get_tokenizer().encode(task, add_special_tokens=True)

    def call(self, hf_data, hf_kwargs):
        data = dict(image_processor()(images=hf_data["images"], return_tensors="pt"))
        data["encoder_input_ids"] = torch.tensor(
            [task_ids(self)] * data["pixel_values"].shape[0]
        )
        return BatchFeature(data)

    def prompt(self, ids):
        text = self.info.get_tokenizer().decode(ids, skip_special_tokens=False)
        if text.strip() not in ("<OCR>", "<s><OCR></s>"):
            raise ValueError(f"OCR-only adapter received unexpected prompt: {text!r}")
        return task_ids(self)

    def updates(self, mm_items, hf_processor_mm_kwargs, out_mm_kwargs):
        ids = task_ids(self)
        count = self.info.get_num_image_tokens() + len(ids)
        pad = self.info.get_hf_config().pad_token_id
        return [
            PromptReplacement(modality="image", target=ids, replacement=[pad] * count)
        ]

    def decoder(self, prompt, mm_items):
        return list(decoder_prefix)

    flo.Florence2MultiModalProcessor.skip_decoder_start_token = True
    flo.Florence2MultiModalProcessor._call_hf_processor = call
    flo.Florence2MultiModalProcessor._postprocess_prompt = prompt
    flo.Florence2MultiModalProcessor._get_prompt_updates = updates
    flo.Florence2MultiModalProcessor.create_decoder_prompt = decoder
    flo.Florence2DummyInputsBuilder.get_dummy_text = lambda self, counts: "<OCR>"
    return {
        "scope": "OCR-only processor API bridge; official plugin model and execution unchanged",
        "image_processor": type(image_processor()).__name__,
        "task": task,
        "decoder_prefix": decoder_prefix,
        "decoder_prefix_policy": "original decoder_start_token_id plus forced_bos_token_id; renderer insertion disabled",
    }
