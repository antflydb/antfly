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

"""Acquire the pinned official checkpoint and generate an F32 text oracle.

Downloads are atomically published, verified against the HF LFS SHA256 when
available, and recorded with hashes. A failed acquisition never publishes a
receipt. No conversion or implicit reduced-precision inference is performed.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import urllib.request

MODEL = "google/embeddinggemma-2"
REVISION = "914f7f89142e33e77833254d9c9b90c3cef7303b"
TRANSFORMERS_REVISION = "92cd495f2720c064bc78eb2d93e28704c5bce51f"
SOURCE_HASHES = {
    "models/embedding_gemma2/modeling_embedding_gemma2.py": "132d8714852fa8f0c376af797229e7b48fe94e0cf6cae6f6640681e934062923",
    "models/embedding_gemma2/processing_embedding_gemma2.py": "6bc770072c6c1df02c3cde4f79c65720c84e1d7f76ca4092baa0dbd217151b52",
    "models/gemma4/modeling_gemma4.py": "edc123ab83fbb25548ec65536eaecee998c088653adf683be3ef69d62e2bfea9",
    "models/gemma4/image_processing_gemma4.py": "5d280d5448b1c219183a27e95b6aa7178b350275772d07881c91c65f13fa815e",
    "models/gemma4/feature_extraction_gemma4.py": "40545340a144aad4d74c6cedb4eae5daa4efd54ae432649583e19b5d756c02c7",
}


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_reference(directory):
    import transformers

    source_root = Path(transformers.__file__).parent
    for relative, expected in SOURCE_HASHES.items():
        if sha256(source_root / relative) != expected:
            raise ValueError(
                f"reference implementation differs from pinned commit: {relative}"
            )
    receipt = json.loads((directory / "embeddinggemma2_receipt.json").read_text())
    if receipt["revision"] != REVISION or receipt["model"] != MODEL:
        raise ValueError("reference checkpoint is not the pinned official revision")
    for entry in receipt["files"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("unsafe receipt path")
        if sha256(directory / relative) != entry["sha256"]:
            raise ValueError(f"reference checkpoint changed: {relative}")


def acquire(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(
        f"https://huggingface.co/api/models/{MODEL}/revision/{REVISION}?blobs=true",
        timeout=60,
    ) as r:
        info = json.load(r)
    if info["sha"] != REVISION:
        raise ValueError("checkpoint revision mismatch")
    files = []
    for entry in info["siblings"]:
        name = entry["rfilename"]
        if not (
            name.endswith(".json")
            or name
            in (
                "tokenizer.model",
                "model.safetensors",
                "LICENSE",
                "NOTICE",
                "README.md",
            )
        ):
            continue
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("unsafe checkpoint path")
        path = directory / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        expected = entry.get("lfs", {}).get("sha256")
        # Only the authoritative LFS digest permits reuse. A previous local
        # sidecar without an upstream digest is not evidence of this revision.
        if not path.exists() or not expected or sha256(path) != expected:
            # Exclusive temp ownership prevents deleting another downloader's
            # file during error cleanup.
            temporary = path.with_name(path.name + f".{os.getpid()}.partial")
            with temporary.open("xb") as out:
                try:
                    with urllib.request.urlopen(
                        f"https://huggingface.co/{MODEL}/resolve/{REVISION}/{name}",
                        timeout=120,
                    ) as response:
                        for chunk in iter(lambda: response.read(4 * 1024 * 1024), b""):
                            out.write(chunk)
                    out.flush()
                    os.fsync(out.fileno())
                except BaseException:
                    temporary.unlink(missing_ok=True)
                    raise
            digest = sha256(temporary)
            if expected and digest != expected:
                temporary.unlink()
                raise ValueError(f"checkpoint hash mismatch: {name}")
            temporary.replace(path)
        files.append(
            {"path": name, "sha256": sha256(path), "bytes": path.stat().st_size}
        )
        print(name, files[-1]["bytes"], flush=True)
    required = {
        "config.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "model.safetensors",
        "processor_config.json",
        "config_sentence_transformers.json",
        "1_Pooling/config.json",
    }
    if not required.issubset({f["path"] for f in files}):
        raise ValueError("incomplete official checkpoint")
    receipt = {
        "model": MODEL,
        "revision": REVISION,
        "recipe": "embeddinggemma2-f32-mean-v1",
        "files": files,
    }
    receipt_tmp = directory / f".receipt.{os.getpid()}.partial"
    with receipt_tmp.open("x") as out:
        json.dump(receipt, out, indent=2)
        out.write("\n")
    receipt_tmp.replace(directory / "embeddinggemma2_receipt.json")


def oracle(directory, output):
    verify_reference(directory)
    import torch
    import transformers
    from transformers import AutoModel, AutoTokenizer

    torch.set_num_threads(4)
    torch.manual_seed(17)
    # This model needs the pinned upstream implementation (pinned development commit).
    model = AutoModel.from_pretrained(
        directory, dtype=torch.float32, local_files_only=True
    ).eval()
    tokenizer = AutoTokenizer.from_pretrained(directory, local_files_only=True)
    cases = [
        "title: none | text: A fox jumps over a log.",
        "task: search result | query: What animal jumps?",
        "task: clustering | query: Instruction: Route the request\nInput: Reset my password",
        "task: clustering | query: Instruction: Route the request\nCategory: Account access problems",
        "task: classification | query: A multilingual sentence: Bonjour 世界 مرحبا.",
    ]
    vectors = []
    token_ids = []
    with torch.inference_mode():
        for text in cases:
            inputs = tokenizer(text, return_tensors="pt")
            hidden = model(**inputs).last_hidden_state
            mask = inputs["attention_mask"].unsqueeze(-1)
            pooled = (hidden * mask).sum(1) / mask.sum(1)
            pooled = torch.nn.functional.normalize(pooled, p=2, dim=-1)
            vectors.append(pooled[0].tolist())
            token_ids.append(inputs["input_ids"][0].tolist())
    result = {
        "model": MODEL,
        "revision": REVISION,
        "transformers_revision": TRANSFORMERS_REVISION,
        "torch": torch.__version__,
        "transformers": transformers.__version__,
        "precision": "float32",
        "cases": cases,
        "token_ids": token_ids,
        "embeddings": vectors,
    }
    output.write_text(json.dumps(result, indent=2) + "\n")


def media_oracle(directory, output):
    verify_reference(directory)
    import math
    import wave
    import numpy as np
    import torch
    from PIL import Image
    from transformers import AutoModel, AutoProcessor

    torch.set_num_threads(4)
    torch.manual_seed(17)
    assets = output.parent
    assets.mkdir(parents=True, exist_ok=True)
    pixels = np.empty((64, 96, 3), dtype=np.uint8)
    for y in range(64):
        for x in range(96):
            pixels[y, x] = ((x * 3 + y) % 256, (y * 4 + x) % 256, (x * 7 + y * 5) % 256)
    image = Image.fromarray(pixels)
    image.save(assets / "image.png")
    waveform = np.array(
        [0.2 * math.sin(2 * math.pi * 440 * i / 16000) for i in range(16000)],
        dtype=np.float32,
    )
    pcm = (waveform * 32767).astype("<i2")
    with wave.open(str(assets / "audio.wav"), "wb") as wav:
        wav.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
        wav.writeframes(pcm.tobytes())
    # Use the decoded PCM values as both native and upstream inputs.
    waveform = pcm.astype(np.float32) / 32768
    model = AutoModel.from_pretrained(
        directory, dtype=torch.float32, local_files_only=True
    ).eval()
    processor = AutoProcessor.from_pretrained(directory, local_files_only=True)
    cases = [
        {"name": "image", "images": [[image]], "text": ["<|image|>"]},
        {"name": "audio", "audio": [waveform], "text": ["<|audio|>"]},
        {
            "name": "text_image",
            "images": [[image]],
            "text": ["title: none | text: Describe this image.\n<|image|>"],
        },
        {
            "name": "image_text",
            "images": [[image]],
            "text": ["title: none | text: <|image|>\nDescribe this image."],
        },
        {
            "name": "mixed",
            "images": [[image]],
            "audio": [waveform],
            "text": [
                "title: none | text: Describe these inputs.\n<|audio|>\n<|image|>"
            ],
        },
    ]
    results = []
    with torch.inference_mode():
        for case in cases:
            name = case["name"]
            args = {k: v for k, v in case.items() if k != "name"}
            if "audio" in args:
                args["sampling_rate"] = 16000
            inputs = processor(**args, return_tensors="pt")
            print(
                name,
                {k: list(v.shape) for k, v in inputs.items() if hasattr(v, "shape")},
                flush=True,
            )
            hidden = model(**inputs).last_hidden_state
            mask = inputs["attention_mask"].unsqueeze(-1)
            pooled = torch.nn.functional.normalize(
                (hidden * mask).sum(1) / mask.sum(1), dim=-1
            )
            results.append(
                {
                    "name": name,
                    "token_ids": inputs["input_ids"][0].tolist(),
                    "embedding": pooled[0].tolist(),
                }
            )
    output.write_text(
        json.dumps(
            {
                "revision": REVISION,
                "transformers_revision": TRANSFORMERS_REVISION,
                "cases": results,
            },
            indent=2,
        )
        + "\n"
    )


def media_stages(directory, output):
    verify_reference(directory)
    """Matched producer boundaries for diagnosing native media parity."""
    import wave
    import numpy as np
    import torch
    from PIL import Image
    from transformers import AutoModel, AutoProcessor

    torch.set_num_threads(4)
    output.mkdir(parents=True, exist_ok=True)
    assets = output.parent
    model = AutoModel.from_pretrained(
        directory, dtype=torch.float32, local_files_only=True
    ).eval()
    processor = AutoProcessor.from_pretrained(directory, local_files_only=True)

    def save(name, value):
        if isinstance(value, tuple):
            value = value[0]
        value.detach().float().cpu().contiguous().numpy().astype("<f4").tofile(
            output / (name + ".f32")
        )

    with torch.inference_mode():
        image = Image.open(assets / "image.png").convert("RGB")
        inputs = processor(images=[[image]], text=["<|image|>"], return_tensors="pt")
        valid = (inputs["image_position_ids"][0] != -1).all(-1)
        save("image_patches", (inputs["pixel_values"][0, valid] - 0.5) * 2)
        hooks = []
        for i, layer in enumerate(model.vision_tower.encoder.layers):
            hooks.append(
                layer.register_forward_hook(
                    lambda m, args, out, i=i: save(
                        f"image_layer_{i}",
                        out[0, valid]
                        if isinstance(out, torch.Tensor)
                        else out[0][0, valid],
                    )
                )
            )
        features = model.get_image_features(
            inputs["pixel_values"], inputs["image_position_ids"]
        )
        save("image_projected", features.pooler_output[0])
        for hook in hooks:
            hook.remove()
        with wave.open(str(assets / "audio.wav"), "rb") as wav:
            waveform = (
                np.frombuffer(wav.readframes(wav.getnframes()), dtype="<i2").astype(
                    np.float32
                )
                / 32768
            )
        inputs = processor(
            audio=[waveform],
            text=["<|audio|>"],
            sampling_rate=16000,
            return_tensors="pt",
        )
        save("audio_features", inputs["input_features"])
        hooks = [
            model.audio_tower.subsample_conv_projection.register_forward_hook(
                lambda m, args, out: save("audio_subsample", out)
            )
        ]
        for name in ("feed_forward1", "self_attn", "lconv1d", "feed_forward2"):
            hooks.append(
                getattr(model.audio_tower.layers[0], name).register_forward_hook(
                    lambda m, args, out, name=name: save("audio_0_" + name, out)
                )
            )
        for i, layer in enumerate(model.audio_tower.layers):
            hooks.append(
                layer.register_forward_hook(
                    lambda m, args, out, i=i: save(f"audio_layer_{i}", out)
                )
            )
        audio = model.audio_tower(
            inputs["input_features"], inputs["input_features_mask"], return_dict=True
        )
        save("audio_output", audio.last_hidden_state)
        save("audio_projected", model.embed_audio(audio.last_hidden_state))
        for hook in hooks:
            hook.remove()


def long_oracle(directory, output):
    verify_reference(directory)
    import torch
    import transformers
    from transformers import AutoModel, AutoTokenizer

    torch.set_num_threads(4)
    model = AutoModel.from_pretrained(
        directory, dtype=torch.float32, local_files_only=True
    ).eval()
    tokenizer = AutoTokenizer.from_pretrained(directory, local_files_only=True)
    seed = tokenizer.encode(
        "This passage describes rivers, forests, cities, and their history. ",
        add_special_tokens=False,
    )
    vectors, token_ids = [], []
    with torch.inference_mode():
        for length in (128, 512, 8192):
            ids = [2] + (seed * ((length - 2) // len(seed) + 1))[: length - 2] + [1]
            inputs = {
                "input_ids": torch.tensor([ids]),
                "attention_mask": torch.ones((1, length), dtype=torch.long),
            }
            pooled = torch.nn.functional.normalize(
                model(**inputs).last_hidden_state.mean(1), dim=-1
            )
            vectors.append(pooled[0].tolist())
            token_ids.append(ids)
            print(f"completed {length} expanded tokens", flush=True)
    output.write_text(
        json.dumps(
            {
                "model": MODEL,
                "revision": REVISION,
                "transformers_revision": TRANSFORMERS_REVISION,
                "torch": torch.__version__,
                "transformers": transformers.__version__,
                "precision": "float32",
                "cases": [
                    "128-token passage",
                    "512-token passage",
                    "8192-token passage",
                ],
                "token_ids": token_ids,
                "embeddings": vectors,
            },
            indent=2,
        )
        + "\n"
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "operation",
        choices=("acquire", "oracle", "media-oracle", "media-stages", "long-oracle"),
    )
    p.add_argument("directory", type=Path)
    p.add_argument("--output", type=Path, default=Path("embeddinggemma2_oracle.json"))
    args = p.parse_args()
    if args.operation == "acquire":
        acquire(args.directory)
    elif args.operation == "oracle":
        oracle(args.directory, args.output)
    elif args.operation == "long-oracle":
        long_oracle(args.directory, args.output)
    elif args.operation == "media-stages":
        media_stages(args.directory, args.output)
    else:
        media_oracle(args.directory, args.output)


if __name__ == "__main__":
    main()
