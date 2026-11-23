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

"""Hugging Face transformers reference for the tile Whisper example (f32, CPU).

    pip install torch transformers numpy
    python whisper_reference.py <hf-model-dir> <audio.wav (16 kHz mono)> <out-dir>

Writes mel.f32 (the model input), encoder_out.f32, step0_logits.f32 and
reference.json (greedy tokens with the generation config's suppress lists,
and the transcript). `zig build tile-whisper -- <model> <out-dir>/mel.f32
--reference <out-dir>` and scripts/tile/run_whisper_opencl.py compare
against these.
"""
import json, sys, wave
import numpy as np
import torch
from transformers import WhisperFeatureExtractor, WhisperForConditionalGeneration, WhisperTokenizer

model_dir, wav_path, out_dir = sys.argv[1:4]
torch.set_grad_enabled(False)
w = wave.open(wav_path)
audio = np.frombuffer(w.readframes(w.getnframes()), dtype=np.int16).astype(np.float32) / 32768.0
fe = WhisperFeatureExtractor.from_pretrained(model_dir)
feats = fe(audio, sampling_rate=16000, return_tensors="pt").input_features  # [1, 80, 3000]
model = WhisperForConditionalGeneration.from_pretrained(model_dir, torch_dtype=torch.float32).eval()
gen = json.load(open(f"{model_dir}/generation_config.json"))
tok = WhisperTokenizer.from_pretrained(model_dir)

feats.numpy().astype(np.float32).tofile(f"{out_dir}/mel.f32")
enc = model.model.encoder(feats).last_hidden_state
enc[0].numpy().astype(np.float32).tofile(f"{out_dir}/encoder_out.f32")

prompt = [50258, 50259, 50359, 50363]  # <|startoftranscript|><|en|><|transcribe|><|notimestamps|>
ids = list(prompt)
for step in range(100):
    logits = model(encoder_outputs=(enc,), decoder_input_ids=torch.tensor([ids])).logits[0, -1].clone()
    if step == 0:
        logits.numpy().astype(np.float32).tofile(f"{out_dir}/step0_logits.f32")
    logits[gen["suppress_tokens"]] = -float("inf")
    if step == 0:
        logits[gen["begin_suppress_tokens"]] = -float("inf")
    nxt = int(torch.argmax(logits))
    ids.append(nxt)
    if nxt == gen["eos_token_id"]:
        break
text = tok.decode(ids, skip_special_tokens=True)
gen_ids = model.generate(feats, language="en", task="transcribe")[0].tolist()
json.dump({"prompt": prompt, "tokens": ids, "text": text,
           "generate_text": tok.decode(gen_ids, skip_special_tokens=True),
           "suppress_tokens": gen["suppress_tokens"], "begin_suppress_tokens": gen["begin_suppress_tokens"]},
          open(f"{out_dir}/reference.json", "w"), indent=1)
print(json.dumps({"tokens": ids, "text": text, "generate_text": tok.decode(gen_ids, skip_special_tokens=True)}))
