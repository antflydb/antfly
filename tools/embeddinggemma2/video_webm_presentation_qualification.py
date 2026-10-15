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

"""Pretrained WebM presentation oracle using pinned PyTorch and independent RGB.

Crop/roll/aspect and VP8 container Colour are explicit processor extensions.
This does not claim the upstream loader automatically applies these policies.
Compressed Cluster hashes must match the unmodified synthetic source fixtures.
"""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import types

import compare
import reference
import video_compare

RECIPE = "embeddinggemma2-f32-mean-v6-webm-presentation"
ROOT = Path(__file__).resolve().parents[2]
ASSETS = ROOT / "zig/pkg/inference/src/testdata/video/presentation"


def fixtures(assets):
    manifest = json.loads((assets / "manifest.json").read_text())
    for row in manifest["cases"].values():
        if reference.sha256(assets / row["path"]) != row["sha256"]:
            raise ValueError("presentation fixture changed")
        if reference.sha256(ROOT / row["source"]) != row["source_sha256"]:
            raise ValueError("unmodified source fixture changed")
    return manifest


def pixels(row, processor):
    import numpy as np

    source = ROOT / row["source"]
    frames, metadata, decoding = video_compare.decoded(source, processor)
    policy = row["policy"]
    display = None
    if "colour" in policy:
        if policy["colour"] != {"matrix": 1, "transfer": 1, "primaries": 1, "range": 2}:
            raise ValueError("unqualified independent colour oracle")
        planar = subprocess.check_output(
            [
                "ffmpeg",
                "-v",
                "error",
                "-i",
                str(source),
                "-fps_mode",
                "passthrough",
                "-pix_fmt",
                "yuv420p",
                "-f",
                "rawvideo",
                "-",
            ],
            timeout=120,
        )
        raw = subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-f",
                "rawvideo",
                "-pix_fmt",
                "yuv420p",
                "-s",
                f"{metadata.width}x{metadata.height}",
                "-colorspace",
                "bt709",
                "-color_range",
                "pc",
                "-i",
                "-",
                "-f",
                "rawvideo",
                "-pix_fmt",
                "rgb24",
                "-",
            ],
            input=planar,
            stdout=subprocess.PIPE,
            check=True,
            timeout=120,
        ).stdout
        count = metadata.total_num_frames
        decoded = np.frombuffer(raw, dtype=np.uint8).reshape(
            count, metadata.height, metadata.width, 3
        )
        frames = decoded[decoding["selected_frames"]].copy()
        decoding["colour_policy"] = (
            "independent FFmpeg planar decode then explicit BT.709 full-range RGB24"
        )
    if "crop" in policy:
        left, top, right, bottom = policy["crop"]
        frames = frames[
            :, top : frames.shape[1] - bottom, left : frames.shape[2] - right
        ].copy()
        height = frames.shape[1]
        dw, dh = policy["display"]
        display_width = (height * dw + dh // 2) // dh
        display = (height, display_width)
        roll = policy["roll"]
        if roll not in (90, -90):
            raise ValueError("unqualified rotation")
        frames = np.rot90(frames, k=1 if roll == 90 else -1, axes=(1, 2)).copy()
        display = display[::-1]
    decoding["rgb_sha256"] = hashlib.sha256(frames.tobytes()).hexdigest()
    decoding["presentation_policy"] = policy
    decoding["display_height_width"] = display
    return frames, metadata, decoding, display


def pytorch(args, manifest, report):
    import torch
    import torchvision.transforms.v2.functional as tvf
    from transformers import AutoModel, AutoProcessor
    from transformers.models.embedding_gemma2.video_processing_embedding_gemma2 import (
        get_aspect_ratio_preserving_size,
    )

    reference.verify_reference(args.model_dir)
    torch.set_num_threads(4)
    model = AutoModel.from_pretrained(
        args.model_dir, dtype=torch.float32, local_files_only=True
    ).eval()
    processor = AutoProcessor.from_pretrained(args.model_dir, local_files_only=True)
    original = processor.video_processor.aspect_ratio_preserving_resize
    report.update(
        torch_version=torch.__version__,
        torchvision_version=__import__("torchvision").__version__,
        revision=reference.REVISION,
        transformers_revision=reference.TRANSFORMERS_REVISION,
        source_hashes=reference.SOURCE_HASHES,
        ffmpeg=subprocess.check_output(["ffmpeg", "-version"], text=True).splitlines()[
            0
        ],
    )
    with torch.inference_mode():
        for name, row in manifest["cases"].items():
            processor.video_processor.aspect_ratio_preserving_resize = original
            frames, metadata, decoding, display = pixels(row, processor.video_processor)
            if display:

                def resize(
                    self,
                    video,
                    patch_size,
                    max_patches,
                    pooling_kernel_size,
                    resample,
                    shape=display,
                ):
                    target = get_aspect_ratio_preserving_size(
                        *shape, patch_size, max_patches, pooling_kernel_size
                    )
                    return tvf.resize(
                        video, list(target), interpolation=resample, antialias=True
                    )

                processor.video_processor.aspect_ratio_preserving_resize = (
                    types.MethodType(resize, processor.video_processor)
                )
            inputs = processor(
                text=["<|video|>"],
                videos=[frames],
                video_metadata=[metadata],
                do_sample_frames=False,
                return_tensors="pt",
            )
            hidden = model(**inputs).last_hidden_state
            mask = inputs["attention_mask"].unsqueeze(-1)
            vector = torch.nn.functional.normalize(
                (hidden * mask).sum(1) / mask.sum(1), dim=-1
            )[0].tolist()
            report["cases"][name] = dict(
                sha256=row["sha256"],
                embedding=vector,
                prompt_tokens=int(inputs["attention_mask"].sum()),
                decoding=decoding,
            )
            compare.publish(args.output, report)
            print(name, "reference complete", flush=True)


def native(args, manifest, report):
    reference.verify_checkpoint(args.model_dir)
    oracle = json.loads(args.reference.read_text())
    if (
        oracle.get("status") != "pass"
        or oracle.get("recipe") != RECIPE
        or oracle.get("checkpoint_sha256") != report["checkpoint_sha256"]
        or oracle.get("manifest_sha256") != report["manifest_sha256"]
        or oracle.get("revision") != reference.REVISION
        or oracle.get("transformers_revision") != reference.TRANSFORMERS_REVISION
        or oracle.get("source_hashes") != reference.SOURCE_HASHES
    ):
        raise ValueError("unqualified pretrained presentation reference")
    report.update(
        binary_sha256=reference.sha256(args.binary),
        reference_sha256=reference.sha256(args.reference),
    )
    args.url = None
    identity = None
    with video_compare.server(args) as url:
        for name, row in manifest["cases"].items():
            expected = oracle["cases"][name]
            body = {
                "model": args.model_dir.name,
                "input": [
                    {
                        "content": [
                            {
                                "type": "media",
                                "mime_type": "video/webm",
                                "data": base64.b64encode(
                                    (args.assets / row["path"]).read_bytes()
                                ).decode(),
                            }
                        ]
                    }
                ],
            }
            if identity:
                body["model_identity"] = identity
            response = compare.post(url + "/ai/v1", "/embed", body)
            if identity and response["model_identity"] != identity:
                raise ValueError("model identity changed")
            identity = response["model_identity"]
            agreement = compare.assert_agreement(
                response["data"][0]["embedding"], expected["embedding"]
            )
            if response["usage"]["prompt_tokens"] != expected["prompt_tokens"]:
                raise AssertionError("presentation token geometry differs")
            report["cases"][name] = dict(
                sha256=row["sha256"],
                embedding=response["data"][0]["embedding"],
                agreement=agreement,
                prompt_tokens=expected["prompt_tokens"],
            )
            compare.publish(args.output, report)
            print(name, "native parity complete", flush=True)
    report["model_identity"] = identity


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("cpu", "native", "metal"), required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--assets", type=Path, default=ASSETS)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.backend != "cpu" and (args.binary is None or args.reference is None):
        parser.error("native/metal require --binary and --reference")
    args.model_dir = args.model_dir.resolve()
    manifest = fixtures(args.assets)
    report = dict(
        status="running",
        backend=args.backend,
        recipe=RECIPE,
        scope=__doc__,
        checkpoint_sha256=reference.sha256(args.model_dir / "model.safetensors"),
        manifest_sha256=reference.sha256(args.assets / "manifest.json"),
        runner_sha256=reference.sha256(Path(__file__)),
        platform=video_compare.conditions(),
        cases={},
    )
    try:
        (pytorch if args.backend == "cpu" else native)(args, manifest, report)
        report["status"] = "pass"
    except Exception as error:
        report.update(status="failed", failure=video_compare.failure_details(error))
        raise
    finally:
        if hasattr(args, "server_memory"):
            report["server_memory"] = args.server_memory
        compare.publish(args.output, report)


if __name__ == "__main__":
    main()
