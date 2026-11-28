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

"""Generate the pinned Inkling 2 Mini reference and compare native CUDA logits."""

import argparse
import hashlib
import json
from pathlib import Path
import time

import numpy as np

TEXT_IDS = [42, 34851, 220, 110104, 314, 159, 9599, 42, 1883, 220, 481]
TEXT_POSITIONS = [0, 1, 4, len(TEXT_IDS) - 1]
PROMPTS = {
    "code": "def square(x):\n    return",
    "german": "Die Hauptstadt von Deutschland ist",
    "math": "17 * 19 =",
    "holdout-product": "29 * 31 =",
    "holdout-root": "The square root of 144 is",
    "holdout-french": "La capitale de l'Espagne est",
    "holdout-code": "def factorial(n):\n    if n < 2:",
}
PROMPT_IDS = {
    "code": [1719, 152397, 7, 87, 8, 25, 198, 149862, 150583],
    "german": [31814, 155465, 52015, 149993, 154307, 150012],
    "math": [68107, 196987, 220, 68028, 196977],
    "holdout-product": [68237, 196987, 220, 68429, 196977],
    "holdout-root": [371, 152397, 151798, 149879, 220, 72916, 149884],
    "holdout-french": [2658, 184184, 149877, 149902, 6, 127341, 149987],
    "holdout-code": [
        1719,
        150935,
        888,
        7,
        77,
        8,
        25,
        198,
        149862,
        150003,
        149934,
        196982,
        220,
        17,
        25,
    ],
}

# The GB10 path expands expert weights to BF16 inside the projection.
# Transformers dequantizes them at load time. The bounds allow for the two
# runtimes' different accumulation orders. Replay and packed-weight tests
# use exact output identity.
GATES = {
    "min_cosine": 0.98,
    "max_absolute": 4.0,
    "max_p99_centered_absolute": 1.0,
    "max_kl_nats": 0.10,
    "min_top10_overlap": 7,
    "max_reference_argmax_gap": 0.5,
}


def reference(model_dir, output, device):
    from PIL import Image
    import torch
    from transformers import AutoModelForMultimodalLM, AutoProcessor, __version__

    if __version__ != "5.24.0":
        raise ValueError("The reference expects Transformers 5.24.0")
    torch.set_num_threads(6)
    start = time.perf_counter()
    print("versions", torch.__version__, __version__, device, flush=True)
    model = AutoModelForMultimodalLM.from_pretrained(
        model_dir,
        dtype=torch.bfloat16,
        device_map=device,
        attn_implementation="eager",
        experts_implementation="eager",
        local_files_only=True,
    ).eval()
    print("loaded", time.perf_counter() - start, type(model).__name__, flush=True)
    width = model.config.text_config.unpadded_vocab_size
    receipt = {
        "model_class": type(model).__name__,
        "torch": torch.__version__,
        "transformers": __version__,
        "device": str(model.device),
        "dtype": str(model.dtype),
        "text_ids": TEXT_IDS,
        "text_positions": TEXT_POSITIONS,
        "prompt_ids": PROMPT_IDS,
        "vocabulary_size": width,
        "outputs": {},
    }

    def save(name, logits):
        array = logits[:width].float().cpu().numpy().astype("<f4", copy=False)
        assert len(array) == width and np.isfinite(array).all(), name
        path = output / f"reference-{name}.f32"
        data = array.tobytes()
        path.write_bytes(data)
        receipt["outputs"][name] = {
            "sha256": hashlib.sha256(data).hexdigest(),
            "argmax": int(array.argmax()),
            "peak": float(array.max()),
        }
        print(name, receipt["outputs"][name], flush=True)

    with torch.inference_mode():
        ids = torch.tensor([TEXT_IDS], device=model.device)
        keep = torch.tensor(TEXT_POSITIONS, device=model.device)
        result = model(input_ids=ids, use_cache=False, logits_to_keep=keep).logits[0]
        for position, logits in zip(TEXT_POSITIONS, result, strict=True):
            save(str(position), logits)

        processor = AutoProcessor.from_pretrained(model_dir, local_files_only=True)
        for name, prompt in PROMPTS.items():
            ids = processor.tokenizer.encode(prompt, add_special_tokens=False)
            if ids != PROMPT_IDS[name]:
                raise ValueError(f"Tokenizer drift for {name}")
            output_logits = model(
                input_ids=torch.tensor([ids], device=model.device),
                use_cache=False,
                logits_to_keep=1,
            ).logits[0, 0]
            save(name, output_logits)

        image = Image.frombytes(
            "RGB", (57, 62), bytes((i * 17) % 256 for i in range(57 * 62 * 3))
        )
        inputs = processor.image_processor([image], return_tensors="pt").to(
            model.device
        )
        image_rows = int(inputs["image_grid_thw"][0].prod())
        assert image_rows == 4
        image_ids = [model.config.image_token_id] * image_rows + [TEXT_IDS[0]]
        receipt["image_ids"] = image_ids
        receipt["image_size"] = list(image.size)
        result = model(
            input_ids=torch.tensor([image_ids], device=model.device),
            **inputs,
            use_cache=False,
            logits_to_keep=1,
        ).logits[0, 0]
        save("image", result)

    print("finished", time.perf_counter() - start, flush=True)
    (output / "reference-manifest.json").write_text(
        json.dumps(receipt, indent=2) + "\n"
    )


def compare(output):
    manifest = json.loads((output / "reference-manifest.json").read_text())
    if (
        manifest["text_ids"] != TEXT_IDS
        or manifest["text_positions"] != TEXT_POSITIONS
        or manifest["prompt_ids"] != PROMPT_IDS
    ):
        raise ValueError("The reference does not describe the CUDA test sequence")
    vocab = manifest["vocabulary_size"]
    rows = []
    text_cases = [str(position) for position in TEXT_POSITIONS] + list(PROMPTS)
    cases = [
        (name, f"cuda-{mode}-{name}")
        for mode in ("default", "fast")
        for name in text_cases
    ] + [("image", "cuda-image"), ("image", "cuda-fast-image")]
    for ref_name, cuda_name in cases:
        reference_data = (output / f"reference-{ref_name}.f32").read_bytes()
        ref_hash = hashlib.sha256(reference_data).hexdigest()
        if ref_hash != manifest["outputs"][ref_name]["sha256"]:
            raise ValueError(f"Reference checksum mismatch for {ref_name}")
        expected = np.frombuffer(reference_data, dtype="<f4").astype(np.float64)
        cuda_data = (output / f"{cuda_name}.f32").read_bytes()
        actual = np.frombuffer(cuda_data, dtype="<f4").astype(np.float64)
        if (
            len(actual) != vocab
            or len(expected) != vocab
            or not np.isfinite(actual).all()
            or not np.isfinite(expected).all()
        ):
            raise ValueError(f"Invalid vocabulary logits in {cuda_name}")
        expected_argmax, actual_argmax = int(expected.argmax()), int(actual.argmax())
        expected_top = np.argsort(-expected, kind="stable")[:10]
        actual_top = np.argsort(-actual, kind="stable")[:10]
        differences = actual - expected
        # A shared score shift leaves token ranks and softmax probabilities unchanged.
        offset = float(differences.mean())
        reference_log_probs = expected - np.logaddexp.reduce(expected)
        cuda_log_probs = actual - np.logaddexp.reduce(actual)
        kl = float(np.exp(reference_log_probs) @ (reference_log_probs - cuda_log_probs))
        cosine = float(
            expected @ actual / (np.linalg.norm(expected) * np.linalg.norm(actual))
        )
        row = {
            "case": cuda_name,
            "reference_sha256": ref_hash,
            "cuda_sha256": hashlib.sha256(cuda_data).hexdigest(),
            "cosine": cosine,
            "max_absolute": float(np.abs(differences).max()),
            "p99_absolute": float(np.percentile(np.abs(differences), 99)),
            "mean_offset": offset,
            "p99_centered_absolute": float(
                np.percentile(np.abs(differences - offset), 99)
            ),
            "kl_nats_reference_to_cuda": kl,
            "top10_overlap": len(set(expected_top) & set(actual_top)),
            "reference_argmax": expected_argmax,
            "cuda_argmax": actual_argmax,
            "reference_argmax_gap": float(
                expected[expected_argmax] - expected[actual_argmax]
            ),
        }
        row["passed"] = bool(
            cosine >= GATES["min_cosine"]
            and row["max_absolute"] <= GATES["max_absolute"]
            and row["p99_centered_absolute"] <= GATES["max_p99_centered_absolute"]
            and kl <= GATES["max_kl_nats"]
            and row["top10_overlap"] >= GATES["min_top10_overlap"]
            and row["reference_argmax_gap"] <= GATES["max_reference_argmax_gap"]
        )
        rows.append(row)
    passed = all(row["passed"] for row in rows)
    (output / "logit-comparison.json").write_text(
        json.dumps({"passed": passed, "gates": GATES, "cases": rows}, indent=2) + "\n",
    )
    for row in rows:
        print(
            f"{row['case']:<19} cosine={row['cosine']:.6f} max={row['max_absolute']:.4f}"
            f" p99={row['p99_absolute']:.4f} centered_p99={row['p99_centered_absolute']:.4f}"
            f" KL={row['kl_nats_reference_to_cuda']:.5f} top10={row['top10_overlap']}"
            f" argmax={row['reference_argmax']}/{row['cuda_argmax']}"
            f" reference_gap={row['reference_argmax_gap']:.4f} pass={row['passed']}"
        )
    return passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--reference-model",
        type=Path,
        help="Generate reference logits from this local checkpoint",
    )
    parser.add_argument(
        "--device",
        default="cpu",
        help="Torch device used for the independent reference",
    )
    args = parser.parse_args()
    if args.reference_model:
        args.output.mkdir(parents=True, exist_ok=True)
        reference(args.reference_model, args.output, args.device)
    else:
        return 0 if compare(args.output) else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
