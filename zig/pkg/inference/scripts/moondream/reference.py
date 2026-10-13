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

"""Upstream Moondream2 (2024-03-04) reference capture for the native runtime.

Runs the pinned upstream ``vision_encoder.py`` (timm SigLIP + projector) and
transformers' ``PhiForCausalLM`` loaded with the checkpoint's phi-msft decoder
weights, in float32 on CPU, using upstream ``Moondream.answer_question``'s
prompt layout and stop tokens, and writes the values the native parity test
compares:

- ``pixels.f32``: the preprocessed image, [3, 378, 378] CHW,
- ``image_embeddings.f32``: projected image embeddings, [729, 2048],
- ``first_logits.f32``: next-token logits after the full prompt, [51200],
- ``image.png``: a copy of the input image,
- ``reference.json``: prompt token ids, greedy generated ids, answer text,
  stop ids, and summary statistics.

Usage (from the repository root, in a venv with torch, torchvision,
transformers==4.38.2, timm, einops, safetensors, pillow)::

    python zig/pkg/inference/scripts/moondream/reference.py \\
        --model-dir ~/.antfly/inference/models/vikhyatk/<moondream2 dir> \\
        --image zig/pkg/inference/testdata/moondream/hello.png \\
        --output <dir outside Git>

``--write-fixture`` regenerates the checked-in HELLO bitmap fixture (the same
image the ``/read`` e2e builds with ``make_text_png_uri(["HELLO"], scale=10)``).
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import struct
import sys
import types
import urllib.request
import zlib
from pathlib import Path

UPSTREAM_REPO = "vikhyatk/moondream2"
# Tag 2024-03-04 of vikhyatk/moondream2.
UPSTREAM_COMMIT = "b3d72d14957e9965facc14f53dc01c7e3bbe0eec"
UPSTREAM_FILES = ("vision_encoder.py",)

_FONT = {
    "H": ["10001", "10001", "10001", "11111", "10001", "10001", "10001"],
    "E": ["11111", "10000", "10000", "11110", "10000", "10000", "11111"],
    "L": ["10000", "10000", "10000", "10000", "10000", "10000", "11111"],
    "O": ["01110", "10001", "10001", "10001", "10001", "10001", "01110"],
}


def hello_png(scale: int = 10, padding: int = 12) -> bytes:
    """Byte-identical to e2e ``make_text_png_uri(["HELLO"], scale=10)``."""
    text = "HELLO"
    width = padding * 2 + len(text) * 6 * scale
    height = padding * 2 + 7 * scale
    canvas = bytearray([255] * (width * height * 3))
    for i, ch in enumerate(text):
        x0 = padding + i * 6 * scale
        for gy, row in enumerate(_FONT[ch]):
            for gx, bit in enumerate(row):
                if bit != "1":
                    continue
                for sy in range(scale):
                    for sx in range(scale):
                        idx = (
                            (padding + gy * scale + sy) * width + x0 + gx * scale + sx
                        ) * 3
                        canvas[idx : idx + 3] = b"\x00\x00\x00"

    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    stride = width * 3
    raw = b"".join(
        b"\x00" + bytes(canvas[y * stride : (y + 1) * stride]) for y in range(height)
    )
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"".join(
        [
            b"\x89PNG\r\n\x1a\n",
            chunk(b"IHDR", ihdr),
            chunk(b"IDAT", zlib.compress(raw)),
            chunk(b"IEND", b""),
        ]
    )


def fetch_upstream(dest: Path) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    for name in UPSTREAM_FILES:
        path = dest / name
        if not path.exists():
            url = f"https://huggingface.co/{UPSTREAM_REPO}/resolve/{UPSTREAM_COMMIT}/{name}"
            path.write_bytes(urllib.request.urlopen(url).read())
    return dest


def load_upstream(src: Path):
    """Import the upstream files as package ``moondream_upstream``."""
    package = types.ModuleType("moondream_upstream")
    package.__path__ = [str(src)]
    sys.modules["moondream_upstream"] = package
    modules = {}
    for name in UPSTREAM_FILES:
        mod_name = f"moondream_upstream.{name[:-3]}"
        spec = importlib.util.spec_from_file_location(mod_name, src / name)
        module = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = module
        spec.loader.exec_module(module)
        modules[name[:-3]] = module
    return modules


def phi_reference(state, tokenizer):
    from transformers import PhiConfig, PhiForCausalLM

    config = PhiConfig(
        vocab_size=51200,
        hidden_size=2048,
        intermediate_size=8192,
        num_hidden_layers=24,
        num_attention_heads=32,
        num_key_value_heads=32,
        hidden_act="gelu_new",
        max_position_embeddings=2048,
        layer_norm_eps=1e-5,
        rope_theta=10000.0,
        partial_rotary_factor=0.5,
        qk_layernorm=False,
        tie_word_embeddings=False,
        bos_token_id=tokenizer.bos_token_id,
        eos_token_id=tokenizer.eos_token_id,
    )
    config._attn_implementation = "eager"
    model = PhiForCausalLM(config)
    d = config.hidden_size
    mapped = {
        "model.embed_tokens.weight": state["text_model.transformer.embd.wte.weight"],
        "model.final_layernorm.weight": state["text_model.lm_head.ln.weight"],
        "model.final_layernorm.bias": state["text_model.lm_head.ln.bias"],
        "lm_head.weight": state["text_model.lm_head.linear.weight"],
        "lm_head.bias": state["text_model.lm_head.linear.bias"],
    }
    for i in range(config.num_hidden_layers):
        src = f"text_model.transformer.h.{i}."
        dst = f"model.layers.{i}."
        wqkv, bqkv = state[src + "mixer.Wqkv.weight"], state[src + "mixer.Wqkv.bias"]
        for j, name in enumerate(("q_proj", "k_proj", "v_proj")):
            mapped[dst + f"self_attn.{name}.weight"] = wqkv[j * d : (j + 1) * d]
            mapped[dst + f"self_attn.{name}.bias"] = bqkv[j * d : (j + 1) * d]
        mapped[dst + "self_attn.dense.weight"] = state[src + "mixer.out_proj.weight"]
        mapped[dst + "self_attn.dense.bias"] = state[src + "mixer.out_proj.bias"]
        mapped[dst + "input_layernorm.weight"] = state[src + "ln.weight"]
        mapped[dst + "input_layernorm.bias"] = state[src + "ln.bias"]
        for fc in ("fc1", "fc2"):
            mapped[dst + f"mlp.{fc}.weight"] = state[src + f"mlp.{fc}.weight"]
            mapped[dst + f"mlp.{fc}.bias"] = state[src + f"mlp.{fc}.bias"]
    model.load_state_dict(mapped, strict=True)
    return model.float().eval()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--model-dir", type=Path)
    parser.add_argument("--image", type=Path)
    parser.add_argument("--question", default="Describe this image.")
    parser.add_argument("--max-new-tokens", type=int, default=256)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--upstream", type=Path, help="Directory for the pinned upstream .py files"
    )
    parser.add_argument(
        "--write-fixture", type=Path, help="Write the HELLO fixture PNG and exit"
    )
    args = parser.parse_args()

    if args.write_fixture:
        args.write_fixture.parent.mkdir(parents=True, exist_ok=True)
        args.write_fixture.write_bytes(hello_png())
        return 0
    if not (args.model_dir and args.image and args.output):
        parser.error("--model-dir, --image, and --output are required")

    import numpy as np
    import torch
    from PIL import Image
    from safetensors.torch import load_file
    from transformers import AutoTokenizer

    torch.manual_seed(0)
    torch.set_grad_enabled(False)
    upstream = fetch_upstream(args.upstream or (args.output / "upstream"))
    mods = load_upstream(upstream)

    state = load_file(str(args.model_dir / "model.safetensors"))
    tokenizer = AutoTokenizer.from_pretrained(str(args.model_dir))

    # Vision: the pinned upstream VisionEncoder (timm SigLIP + projector).
    vision = mods["vision_encoder"].VisionEncoder()
    vision_state = {
        k[len("vision_encoder.") :]: v
        for k, v in state.items()
        if k.startswith("vision_encoder.")
    }
    vision.load_state_dict(vision_state, strict=True)
    vision = vision.float().eval()

    # Text: upstream's phi-msft generation path does not run under any
    # transformers release that also runs its cache-less path, so the decoder
    # reference is transformers' own PhiForCausalLM with the phi-msft weights
    # renamed (the same mapping Xenova/moondream2's export uses).
    text_model = phi_reference(state, tokenizer)

    image = Image.open(args.image)
    pixels = vision.preprocess(image.convert("RGB")).unsqueeze(0).float()
    image_embeds = vision(image)  # [1, 729, 2048]

    # Moondream.answer_question / input_embeds at the pinned revision.
    prompt = f"<image>\n\nQuestion: {args.question}\n\nAnswer: "
    before, after = prompt.split("<image>")
    before_ids = tokenizer(f"{before}<image>", add_special_tokens=False).input_ids
    after_ids = tokenizer(f"</image>{after}", add_special_tokens=False).input_ids
    stop_ids = tokenizer("<END>", add_special_tokens=False).input_ids
    emb = text_model.get_input_embeddings()
    inputs_embeds = torch.cat(
        [
            emb(torch.tensor([[tokenizer.bos_token_id]])),
            emb(torch.tensor([before_ids])),
            image_embeds,
            emb(torch.tensor([after_ids])),
        ],
        dim=1,
    )
    first_logits = text_model(inputs_embeds=inputs_embeds).logits[0, -1].float()
    output_ids = text_model.generate(
        inputs_embeds=inputs_embeds,
        attention_mask=torch.ones(inputs_embeds.shape[:2], dtype=torch.long),
        eos_token_id=stop_ids,
        bos_token_id=tokenizer.bos_token_id,
        pad_token_id=tokenizer.eos_token_id,
        max_new_tokens=args.max_new_tokens,
        do_sample=False,
    )[0].tolist()
    generated_text = tokenizer.decode(output_ids, skip_special_tokens=True)
    answer = re.sub("<$|<END$", "", generated_text).strip()

    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "image.png").write_bytes(args.image.read_bytes())
    pixels.numpy().astype("<f4").tofile(args.output / "pixels.f32")
    image_embeds[0].numpy().astype("<f4").tofile(args.output / "image_embeddings.f32")
    first_logits.numpy().astype("<f4").tofile(args.output / "first_logits.f32")
    top = torch.topk(first_logits, 10)
    report = {
        "upstream": {"repo": UPSTREAM_REPO, "commit": UPSTREAM_COMMIT},
        "image_sha256": hashlib.sha256(args.image.read_bytes()).hexdigest(),
        "question": args.question,
        "dtype": "float32",
        "before_ids": before_ids,
        "after_ids": after_ids,
        "stop_ids": stop_ids,
        "prompt_rows": int(inputs_embeds.shape[1]),
        "first_top_ids": top.indices.tolist(),
        "first_top_logits": top.values.tolist(),
        "generated_ids": output_ids,
        "generated_text": generated_text,
        "answer": answer,
        "pixels_mean": float(pixels.mean()),
        "image_embeddings_mean": float(image_embeds.mean()),
        "image_embeddings_abs_max": float(image_embeds.abs().max()),
        "torch": torch.__version__,
        "numpy": np.__version__,
    }
    (args.output / "reference.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {k: report[k] for k in ("prompt_rows", "generated_ids", "answer")}, indent=2
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
