#!/usr/bin/env python3
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

"""Regenerate the offline Whisper golden features using Hugging Face's reference.

Run with the inference E2E environment (transformers and numpy); no models,
network access, or GPU are needed. OpenAI's audio.py specifies periodic Hann,
centered reflect STFT, Slaney filters, and fixed affine log normalization:
https://github.com/openai/whisper/blob/main/whisper/audio.py
"""

from pathlib import Path
import numpy as np
from transformers import WhisperFeatureExtractor


def main():
    output = Path(__file__).resolve().parents[1] / "src" / "testdata" / "whisper"
    output.mkdir(exist_ok=True)
    for count in (733, 16000):
        samples = ((np.arange(count, dtype=np.int64) * 73 % 257) - 128).astype(
            np.float32
        ) / 512
        padded = np.zeros(16000, dtype=np.float32)
        padded[:count] = samples
        for bins in (80, 128):
            extractor = WhisperFeatureExtractor(feature_size=bins, chunk_length=1)
            for divisor in (1, 16):
                features = extractor._np_extract_fbank_features(
                    (padded / divisor)[None], device="cpu"
                )[0]
                assert features.shape == (bins, 100)
                (output / f"{count}-{bins}-{divisor}.f32").write_bytes(
                    features.astype("<f4").tobytes()
                )


if __name__ == "__main__":
    main()
