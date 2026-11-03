#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Pinned natural-image and codec fixtures with independent upstream F32 vectors."""

import argparse
import hashlib
import json
import time
from pathlib import Path

import cpu_http_qualification as common
import reference

DATASET = "jxie/flickr8k"
DATASET_REVISION = "56f58c967835f7c508d684f36bd7897cca9d7634"
ARCHIVE_SHA256 = "f3bc4c3548ec1d9bb3c445c871cfcd3aca6003e3d4786e404fc9356b5215410d"
HOLDOUT_IMAGES = 128


def prepare(archive, output):
    import numpy as np
    import pyarrow.parquet as pq
    from PIL import Image

    if common.digest(archive) != ARCHIVE_SHA256:
        raise ValueError("Flickr8k test archive differs from pinned publisher SHA256")
    rows = pq.read_table(archive).to_pylist()
    if len(rows) != 1000:
        raise ValueError("complete Flickr8k test split required")
    rows.sort(key=lambda row: hashlib.sha256(row["image"]["bytes"]).hexdigest())
    selected = rows[:HOLDOUT_IMAGES]
    if (
        len({hashlib.sha256(row["image"]["bytes"]).hexdigest() for row in selected})
        != HOLDOUT_IMAGES
    ):
        raise ValueError("duplicate holdout images")
    output.mkdir(parents=True, exist_ok=True)
    assets = output / "assets"
    assets.mkdir(exist_ok=True)
    suite = {
        "version": 1,
        "dataset": DATASET,
        "dataset_revision": DATASET_REVISION,
        "archive_sha256": ARCHIVE_SHA256,
        "complete_test_split_images": 1000,
        "selection": "first 128 by SHA256 of original JPEG bytes",
        "cases": [],
        "queries": [],
    }

    def add(name, path, role="edge", parts=None):
        with Image.open(path) as image:
            image.seek(0)
            rgb = image.convert("RGB")
            record = {
                "name": name,
                "role": role,
                "path": str(path.resolve()),
                "sha256": common.digest(path),
                "mime": Image.MIME[image.format],
                "width": image.width,
                "height": image.height,
                "mode": image.mode,
                "decoded_rgb_sha256": hashlib.sha256(rgb.tobytes()).hexdigest(),
            }
        record["parts"] = parts or [{"image": name}]
        suite["cases"].append(record)
        return record

    for index, row in enumerate(selected):
        name = f"flickr-{index:03}"
        original = assets / (name + ".jpg")
        original.write_bytes(row["image"]["bytes"])
        add(name, original, "retrieval")
        captions = [row[f"caption_{n}"] for n in range(5)]
        if any(
            not isinstance(caption, str) or not caption.strip() for caption in captions
        ):
            raise ValueError("every holdout image requires five nonempty captions")
        for n, caption in enumerate(captions):
            suite["queries"].append(
                {
                    "name": f"caption-{index:03}-{n}",
                    "text": caption,
                    "relevant_image": name,
                }
            )
        if index < 8:
            path = assets / (name + "-decoded.png")
            with Image.open(original) as image:
                image.convert("RGB").save(path)
            add(name + "-decoded", path)

    shapes = [
        (1, 1),
        (31, 29),
        (96, 64),
        (513, 257),
        (257, 513),
        (1024, 16),
        (16, 1024),
        (2048, 1536),
    ]
    for width, height in shapes:
        y, x = np.indices((height, width))
        pixels = np.stack(
            [(x * 3 + y) % 256, (x + y * 5) % 256, (x * 7 + y * 11) % 256], axis=-1
        ).astype(np.uint8)
        path = assets / f"rgb-{width}x{height}.png"
        Image.fromarray(pixels).save(path)
        add(path.stem, path)
    y, x = np.indices((97, 129))
    rgb = np.stack(
        [(x * 3 + y) % 256, (x + y * 5) % 256, (x * 7 + y * 11) % 256], axis=-1
    ).astype(np.uint8)
    image = Image.fromarray(rgb)
    for name, mode, extension, options in [
        ("gray", "L", "png", {}),
        ("rgba", "RGBA", "png", {}),
        ("palette", "P", "png", {}),
        ("baseline-jpeg", "RGB", "jpg", {"quality": 92}),
        ("progressive-jpeg", "RGB", "jpg", {"quality": 92, "progressive": True}),
        ("lossless-webp", "RGB", "webp", {"lossless": True}),
        ("bmp", "RGB", "bmp", {}),
    ]:
        converted = image.convert(mode)
        if mode == "RGBA":
            alpha = Image.fromarray(((x + y) % 256).astype(np.uint8))
            converted.putalpha(alpha)
        path = assets / (name + "." + extension)
        converted.save(path, **options)
        add(name, path)
    first = Image.new("RGB", (120, 80), (240, 25, 25))
    second = Image.new("RGB", first.size, (25, 25, 240))
    path = assets / "animated.gif"
    first.save(path, save_all=True, append_images=[second], duration=100, loop=0)
    add("animated-gif", path)
    first.save(assets / "animated-first.png")
    add("animated-first", assets / "animated-first.png")
    # Ordered combinations exercise prefix placement and two-image composition.
    path = assets / "rgb-96x64.png"
    for name, parts in [
        ("text-image", [{"text": "Describe this image."}, {"image": "rgb-96x64"}]),
        ("image-text", [{"image": "rgb-96x64"}, {"text": "Describe this image."}]),
        ("two-images", [{"image": "rgb-96x64"}, {"image": "rgb-513x257"}]),
    ]:
        add(name, path, "ordered", parts)
    if len(suite["queries"]) != HOLDOUT_IMAGES * 5:
        raise ValueError("caption holdout coverage differs")
    common.save(output / "suite.json", suite)
    query_cases = [
        {
            "name": row["name"],
            "text": row["text"],
            "task": "RETRIEVAL_QUERY",
            "iterations": 1,
            "warmups": 0,
        }
        for row in suite["queries"]
    ]
    common.save(output / "caption-suite.json", {"cases": query_cases})
    for at in range(0, len(query_cases), 256):
        common.save(
            output / f"captions-{at // 256:03}.suite.json",
            {"cases": query_cases[at : at + 256]},
        )
    print(
        "prepared",
        len(suite["cases"]),
        "image cases and",
        len(suite["queries"]),
        "captions",
        flush=True,
    )


def rendered_parts(case):
    parts = case["parts"]
    content = "\n".join(part.get("text", "<|image|>") for part in parts)
    return (
        "title: none | text: " + content
        if any("text" in part for part in parts)
        else content
    )


def oracle(model_dir, suite_path, output):
    reference.verify_reference(model_dir)
    import torch
    import transformers
    from PIL import Image
    from transformers import AutoModel, AutoProcessor

    torch.set_num_threads(4)
    torch.set_num_interop_threads(1)
    torch.manual_seed(17)
    suite = json.loads(suite_path.read_text())
    by_name = {case["name"]: case for case in suite["cases"]}
    if len(by_name) != len(suite["cases"]):
        raise ValueError("duplicate oracle cases")
    model = AutoModel.from_pretrained(
        model_dir, dtype=torch.float32, local_files_only=True
    ).eval()
    processor = AutoProcessor.from_pretrained(model_dir, local_files_only=True)
    result = {
        "version": 1,
        "status": "running",
        "suite_sha256": common.digest(suite_path),
        "checkpoint_receipt_sha256": common.digest(
            model_dir / "embeddinggemma2_receipt.json"
        ),
        "revision": reference.REVISION,
        "transformers_revision": reference.TRANSFORMERS_REVISION,
        "source_hashes": reference.SOURCE_HASHES,
        "torch": torch.__version__,
        "transformers": transformers.__version__,
        "precision": "float32",
        "cases": {},
        "queries": {},
    }
    with torch.inference_mode():
        for case in suite["cases"]:
            images = []
            for part in case["parts"]:
                if "image" in part:
                    row = by_name[part["image"]]
                    path = Path(row["path"])
                    if common.digest(path) != row["sha256"]:
                        raise ValueError("image asset differs")
                    with Image.open(path) as image:
                        image.seek(0)
                        images.append(image.convert("RGB"))
            started = time.perf_counter()
            inputs = processor(
                text=[rendered_parts(case)], images=[images], return_tensors="pt"
            )
            hidden = model(**inputs).last_hidden_state
            mask = inputs["attention_mask"].unsqueeze(-1)
            vector = torch.nn.functional.normalize(
                (hidden * mask).sum(1) / mask.sum(1), dim=-1
            )[0]
            if vector.numel() != 768 or not torch.isfinite(vector).all():
                raise ValueError("invalid independent F32 image vector")
            result["cases"][case["name"]] = {
                "vector": vector.tolist(),
                "input_tokens": inputs["input_ids"].shape[-1],
                "seconds": time.perf_counter() - started,
            }
            common.save(output, result)
            print(
                case["name"],
                "seconds",
                result["cases"][case["name"]]["seconds"],
                flush=True,
            )
        for index, query in enumerate(suite["queries"]):
            inputs = processor.tokenizer(
                "task: search result | query: " + query["text"], return_tensors="pt"
            )
            hidden = model(**inputs).last_hidden_state
            mask = inputs["attention_mask"].unsqueeze(-1)
            vector = torch.nn.functional.normalize(
                (hidden * mask).sum(1) / mask.sum(1), dim=-1
            )[0]
            if vector.numel() != 768 or not torch.isfinite(vector).all():
                raise ValueError("invalid independent caption vector")
            result["queries"][query["name"]] = {"vector": vector.tolist()}
            if (index + 1) % 32 == 0:
                common.save(output, result)
                print(
                    "caption references",
                    index + 1,
                    "of",
                    len(suite["queries"]),
                    flush=True,
                )
        result["status"] = "pass"
        common.save(output, result)
    print("independent image and caption oracle complete", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["prepare", "oracle"])
    parser.add_argument(
        "--archive",
        type=Path,
        default=Path.home()
        / f".cache/antfly/conformance-fixtures/flickr8k/{ARCHIVE_SHA256}.parquet",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--suite", type=Path)
    parser.add_argument("--model-dir", type=Path)
    args = parser.parse_args()
    if args.operation == "prepare":
        prepare(args.archive, args.output)
    else:
        oracle(args.model_dir, args.suite, args.output)


if __name__ == "__main__":
    main()
