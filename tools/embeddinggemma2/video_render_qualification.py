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

"""Positive pretrained qualification for display/HDR recipe v3.

The reference uses pinned PyTorch/Transformers, independent Pillow JPEG and
FFmpeg integer YUV decoding, and float64 rendering. SAR changes the resize
geometry in a single resample. This explicitly extends the pinned processor;
it does not claim upstream automatically honors these display/HDR policies.
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
import render_reference
import video_compare

RECIPE = "embeddinggemma2-f32-mean-v3-display-hdr"
CASES = (
    ("rotation_90", "rotation-90.mov"),
    ("sar_2_1", "sar-2-1.mov"),
    ("pq_2020", "pq-2020.mp4"),
    ("hlg_2020", "hlg-2020.mp4"),
)


def pixels_for(name, path, processor):
    import numpy as np
    from transformers.video_utils import VideoMetadata

    if name in ("rotation_90", "sar_2_1"):
        original = Path(__file__).parent / "testdata/video/video-mjpeg.mov"
        pixels, metadata, decoding = video_compare.decoded(original, processor)
        if name == "rotation_90":
            pixels = np.rot90(pixels, k=-1, axes=(1, 2)).copy()
        return pixels, metadata, decoding
    probe = json.loads(
        subprocess.check_output(
            ["ffprobe", "-v", "error", "-show_streams", "-of", "json", str(path)],
            timeout=30,
        )
    )["streams"][0]
    width, height, count = (
        int(probe["width"]),
        int(probe["height"]),
        int(probe["nb_frames"]),
    )
    if (width, height, count, probe["pix_fmt"]) != (48, 48, 8, "yuv420p10le"):
        raise ValueError("HDR qualification fixture changed")
    transfer = "pq" if name == "pq_2020" else "hlg"
    if probe.get("color_primaries") != "bt2020" or probe.get("color_transfer") != (
        "smpte2084" if transfer == "pq" else "arib-std-b67"
    ):
        raise ValueError("HDR qualification signal changed")
    metadata = VideoMetadata(
        total_num_frames=count, fps=4, duration=2, width=width, height=height
    )
    indexes = processor.sample_frames(
        metadata,
        fps=processor.fps,
        max_frames=processor.max_frames,
        overflow_strategy=processor.overflow_strategy,
    ).tolist()
    raw = subprocess.check_output(
        [
            "ffmpeg",
            "-v",
            "error",
            "-noautorotate",
            "-i",
            str(path),
            "-pix_fmt",
            "yuv420p10le",
            "-f",
            "rawvideo",
            "-",
        ],
        timeout=30,
    )
    planes = np.frombuffer(raw, dtype="<u2").reshape(count, width * height * 3 // 2)
    result = []
    for index in indexes:
        frame = planes[index]
        luma = frame[: width * height].reshape(height, width)
        u = frame[width * height : width * height * 5 // 4].reshape(
            height // 2, width // 2
        )
        v = frame[width * height * 5 // 4 :].reshape(height // 2, width // 2)
        rgb = bytearray()
        for y in range(height):
            for x in range(width):
                rgb.extend(
                    render_reference.render(
                        render_reference.yuv_rgb(
                            int(luma[y, x]),
                            int(u[y // 2, x // 2]),
                            int(v[y // 2, x // 2]),
                        ),
                        transfer,
                    )
                )
        result.append(
            np.frombuffer(rgb, dtype=np.uint8).reshape(height, width, 3).copy()
        )
    pixels = np.stack(result)
    return (
        pixels,
        metadata,
        {
            "policy": render_reference.__doc__,
            "selected_frames": indexes,
            "rgb_sha256": hashlib.sha256(pixels.tobytes()).hexdigest(),
        },
    )


def pytorch(args):
    import torch
    import torchvision.transforms.v2.functional as tvf
    from transformers import AutoModel, AutoProcessor
    from transformers.models.embedding_gemma2.video_processing_embedding_gemma2 import (
        get_aspect_ratio_preserving_size,
    )

    reference.verify_reference(args.model_dir)
    torch.set_num_threads(4)
    model = (
        AutoModel.from_pretrained(
            args.model_dir, dtype=torch.float32, local_files_only=True
        )
        .eval()
        .to(args.backend)
    )
    processor = AutoProcessor.from_pretrained(args.model_dir, local_files_only=True)
    original_resize = processor.video_processor.aspect_ratio_preserving_resize
    report = {
        "status": "running",
        "backend": args.backend,
        "recipe": RECIPE,
        "precision": "float32",
        "torch_version": torch.__version__,
        "torchvision_version": __import__("torchvision").__version__,
        "checkpoint_sha256": reference.sha256(args.model_dir / "model.safetensors"),
        "ffmpeg": subprocess.check_output(
            ["ffmpeg", "-version"], text=True
        ).splitlines()[0],
        "revision": reference.REVISION,
        "transformers_revision": reference.TRANSFORMERS_REVISION,
        "source_hashes": reference.SOURCE_HASHES,
        "scope": __doc__,
        "platform": video_compare.conditions(),
        "cases": {},
    }
    with torch.inference_mode():
        for name, filename in CASES:
            path = args.assets / filename
            processor.video_processor.aspect_ratio_preserving_resize = original_resize
            pixels, metadata, decoding = pixels_for(
                name, path, processor.video_processor
            )
            if name == "sar_2_1":

                def resize(
                    self, video, patch_size, max_patches, pooling_kernel_size, resample
                ):
                    height, width = video.shape[-2:]
                    target = get_aspect_ratio_preserving_size(
                        height, width * 2, patch_size, max_patches, pooling_kernel_size
                    )
                    return tvf.resize(
                        video, list(target), interpolation=resample, antialias=True
                    )

                processor.video_processor.aspect_ratio_preserving_resize = (
                    types.MethodType(resize, processor.video_processor)
                )
            inputs = processor(
                text=["<|video|>"],
                videos=[pixels],
                video_metadata=[metadata],
                do_sample_frames=False,
                return_tensors="pt",
            )
            prompt_tokens = int(inputs["attention_mask"].sum())
            inputs = {k: v.to(args.backend) for k, v in inputs.items()}
            hidden = model(**inputs).last_hidden_state
            mask = inputs["attention_mask"].unsqueeze(-1)
            vector = (
                torch.nn.functional.normalize(
                    (hidden * mask).sum(1) / mask.sum(1), dim=-1
                )[0]
                .cpu()
                .tolist()
            )
            if args.backend == "mps":
                torch.mps.synchronize()
            report["cases"][name] = {
                "path": filename,
                "sha256": reference.sha256(path),
                "embedding": vector,
                "prompt_tokens": prompt_tokens,
                "decoding": decoding,
            }
            compare.publish(args.output, report)
    if args.backend == "mps":
        cpu = json.loads(args.reference.read_text())
        if (
            cpu.get("status") != "pass"
            or cpu.get("recipe") != RECIPE
            or cpu.get("checkpoint_sha256") != report["checkpoint_sha256"]
        ):
            raise ValueError("unqualified CPU rendering reference")
        report["reference_sha256"] = reference.sha256(args.reference)
        for name, row in report["cases"].items():
            expected = cpu["cases"][name]
            if (
                row["sha256"] != expected["sha256"]
                or row["prompt_tokens"] != expected["prompt_tokens"]
            ):
                raise ValueError("CPU/MPS rendering inputs differ")
            row["agreement_to_cpu"] = compare.assert_agreement(
                row["embedding"], expected["embedding"]
            )
    report["status"] = "pass"
    compare.publish(args.output, report)


def native(args):
    reference.verify_checkpoint(args.model_dir)
    oracle = json.loads(args.reference.read_text())
    if (
        oracle.get("status") != "pass"
        or oracle.get("revision") != reference.REVISION
        or oracle.get("recipe") != RECIPE
        or oracle.get("precision") != "float32"
        or oracle.get("backend") not in ("cpu", "mps")
        or oracle.get("source_hashes") != reference.SOURCE_HASHES
        or oracle.get("transformers_revision") != reference.TRANSFORMERS_REVISION
        or oracle.get("checkpoint_sha256")
        != reference.sha256(args.model_dir / "model.safetensors")
    ):
        raise ValueError("unqualified rendering reference")
    report = {
        "status": "running",
        "backend": args.backend,
        "recipe": RECIPE,
        "binary_sha256": reference.sha256(args.binary),
        "reference_sha256": reference.sha256(args.reference),
        "platform": video_compare.conditions(),
        "scope": __doc__,
        "cases": {},
    }
    args.url = None
    identity = None
    try:
        with video_compare.server(args) as url:
            for name, filename in CASES:
                path = args.assets / filename
                expected = oracle["cases"][name]
                if reference.sha256(path) != expected["sha256"]:
                    raise ValueError("rendering fixture changed")
                body = {
                    "model": args.model_dir.name,
                    "input": [
                        {
                            "content": [
                                {
                                    "type": "media",
                                    "mime_type": "video/quicktime"
                                    if filename.endswith("mov")
                                    else "video/mp4",
                                    "data": base64.b64encode(
                                        path.read_bytes()
                                    ).decode(),
                                }
                            ]
                        }
                    ],
                }
                if identity:
                    body["model_identity"] = identity
                response = compare.post(url + "/ai/v1", "/embed", body)
                if identity and identity != response["model_identity"]:
                    raise ValueError("model identity changed")
                identity = response["model_identity"]
                agreement = compare.assert_agreement(
                    response["data"][0]["embedding"], expected["embedding"]
                )
                if response["usage"]["prompt_tokens"] != expected["prompt_tokens"]:
                    raise AssertionError("display geometry changed token count")
                report["cases"][name] = {
                    "sha256": expected["sha256"],
                    "agreement": agreement,
                    "prompt_tokens": expected["prompt_tokens"],
                    "embedding": response["data"][0]["embedding"],
                }
                compare.publish(args.output, report)
        report["model_identity"] = identity
        report["status"] = "pass"
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = video_compare.failure_details(error)
        raise
    finally:
        if hasattr(args, "server_memory"):
            report["server_memory"] = args.server_memory
        compare.publish(args.output, report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend", choices=("cpu", "mps", "native", "metal"), required=True
    )
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--assets", type=Path, default=Path(__file__).parent / "testdata/render"
    )
    parser.add_argument(
        "--reference",
        type=Path,
        default=Path(__file__).parent / "testdata/render/pytorch-cpu.json",
    )
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.model_dir = args.model_dir.resolve()
    if args.backend in ("cpu", "mps"):
        pytorch(args)
    else:
        if args.binary is None:
            parser.error("--binary is required for native backends")
        args.binary = args.binary.resolve()
        native(args)


if __name__ == "__main__":
    main()
