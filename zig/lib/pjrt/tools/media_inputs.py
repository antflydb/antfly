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

"""Raw media fixtures and the official CPU processor used in end-to-end timing."""

import argparse
import hashlib
import json
import wave
from pathlib import Path

import numpy as np
from PIL import Image


def processor(model):
    from transformers import AutoProcessor

    return AutoProcessor.from_pretrained(model, local_files_only=True)


def prepare(proc, case, base):
    """Includes file read/decode and official preprocessing; no cached features."""
    images, audio = [], []
    for part in case["parts"]:
        if part["kind"] == "image":
            with Image.open(base / part["path"]) as image:
                images.append(image.convert("RGB"))
        elif part["kind"] == "audio":
            with wave.open(str(base / part["path"]), "rb") as wav:
                if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) != (
                    1,
                    2,
                    16000,
                ):
                    raise ValueError("This campaign requires mono 16kHz PCM16 WAV")
                audio.append(
                    np.frombuffer(wav.readframes(wav.getnframes()), dtype="<i2").astype(
                        np.float32
                    )
                    / 32768
                )
    kwargs = {"text": [case["processor_prompt"]], "return_tensors": "pt"}
    if images:
        kwargs["images"] = [images]
    if audio:
        kwargs.update(audio=audio, sampling_rate=16000)
    result = proc(**kwargs)
    if "input_features_mask" in result:
        result["input_features_mask"] = result["input_features_mask"].long()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--include-text",
        action="store_true",
        help="Add exact-length promotion text cases",
    )
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    for name, height, width, kind in [
        ("small.png", 64, 96, "PNG"),
        ("large.jpg", 768, 1024, "JPEG"),
    ]:
        y, x = np.indices((height, width))
        rgb = np.stack(
            ((x * 3 + y) % 256, (y * 4 + x) % 256, (x * 7 + y * 5) % 256), -1
        ).astype(np.uint8)
        Image.fromarray(rgb).save(args.out / name, format=kind)
    for duration in (1, 12, 30):
        t = np.arange(16000 * duration, dtype=np.float64) / 16000
        signal = 0.12 * np.sin(2 * np.pi * (180 * t + 8 * t * t)) + 0.07 * np.sin(
            2 * np.pi * 440 * t
        )
        pcm = (signal * 32767).astype("<i2")
        with wave.open(str(args.out / f"audio-{duration}s.wav"), "wb") as wav:
            wav.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
            wav.writeframes(pcm.tobytes())
    text = ("The river flows past the city and its railway station. " * 80).strip()
    cases = [
        {
            "name": "image_png",
            "parts": [{"kind": "image", "path": "small.png"}],
            "processor_prompt": "<|image|>",
        },
        {
            "name": "image_jpeg",
            "parts": [{"kind": "image", "path": "large.jpg"}],
            "processor_prompt": "<|image|>",
        },
        {
            "name": "text_image",
            "parts": [
                {"kind": "text", "text": text + "\n"},
                {"kind": "image", "path": "large.jpg"},
            ],
            "processor_prompt": "title: none | text: " + text + "\n<|image|>",
        },
    ]
    cases += [
        {
            "name": f"audio_{seconds}s",
            "parts": [{"kind": "audio", "path": f"audio-{seconds}s.wav"}],
            "processor_prompt": "<|audio|>",
        }
        for seconds in (1, 12, 30)
    ]
    proc = processor(args.model)
    if args.include_text:
        text_cases = []
        for length in (128, 512, 2048, 8192):
            text = " ".join(["river"] * (length - 8))
            prompt = "title: none | text: " + text
            if len(proc.tokenizer.encode(prompt)) != length:
                raise ValueError(
                    "Tokenizer does not match the promotion fixture recipe"
                )
            text_cases.append(
                {
                    "name": f"text_{length}",
                    "parts": [{"kind": "text", "text": text}],
                    "processor_prompt": prompt,
                }
            )
        cases = text_cases + cases
    for case in cases:
        inputs = prepare(proc, case, args.out)
        case["token_ids"] = inputs["input_ids"][0].tolist()
        case["input_shapes"] = {k: list(v.shape) for k, v in inputs.items()}
        print(case["name"], case["input_shapes"], flush=True)
    files = {
        p.name: {
            "bytes": p.stat().st_size,
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
        }
        for p in args.out.iterdir()
        if p.is_file()
    }
    (args.out / "suite.json").write_text(
        json.dumps({"cases": cases, "files": files}, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
