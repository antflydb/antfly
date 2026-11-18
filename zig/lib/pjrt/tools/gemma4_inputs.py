# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Encoded media -> official CPU processor, with explicit one-Hz video frames."""

import hashlib
import json
import wave

import numpy as np
from PIL import Image


def processor(directory):
    from transformers import AutoProcessor

    return AutoProcessor.from_pretrained(directory, local_files_only=True)


def prepare(proc, case, base):
    kind = case.get("kind", "text")
    parts = ([] if kind == "text" else [{"type": kind}]) + [
        {"type": "text", "text": case["text"]}
    ]
    prompt = proc.apply_chat_template(
        [{"role": "user", "content": parts}],
        tokenize=False,
        add_generation_prompt=True,
        enable_thinking=False,
    )
    if kind == "video":
        # Native ChatTemplate renders its one video marker with a following
        # space. Preserve that convention in the reference, rather than timing
        # equal-length prompts with a different first text token after frames.
        if prompt.count("<|video|>") != 1:
            raise ValueError("Expected one rendered video marker")
        prompt = prompt.replace("<|video|>", "<|video|> ", 1)
    kwargs = {"text": [prompt], "return_tensors": "pt", "add_special_tokens": False}
    metadata = {}
    if kind == "image":
        with Image.open(base / case["path"]) as image:
            kwargs["images"] = [[image.convert("RGB")]]
    elif kind == "audio":
        with wave.open(str(base / case["path"]), "rb") as wav:
            if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) != (
                1,
                2,
                16000,
            ):
                raise ValueError("Use mono 16 kHz PCM16 fixtures")
            audio = (
                np.frombuffer(wav.readframes(wav.getnframes()), dtype="<i2").astype(
                    np.float32
                )
                / 32768
            )
        kwargs.update(audio=[audio], sampling_rate=16000)
    elif kind == "video":
        import av
        from transformers.video_utils import VideoMetadata

        selected, indices, pts = [], [], []
        with av.open(str(base / case["path"])) as container:
            stream = container.streams.video[0]
            fps = float(stream.average_rate)
            target = 0.0
            total = 0
            width, height = stream.width, stream.height
            for index, frame in enumerate(container.decode(stream)):
                timestamp = float(frame.pts * frame.time_base)
                total += 1
                if timestamp + 1e-6 >= target:
                    selected.append(frame.to_ndarray(format="rgb24"))
                    indices.append(index)
                    pts.append(timestamp)
                    target += 1.0
            duration = (
                float(stream.duration * stream.time_base)
                if stream.duration
                else total / fps
            )
        if (
            not selected
            or np.max(np.abs(np.array(indices) / fps - np.array(pts))) > 1e-6
        ):
            raise ValueError("This fixture recipe requires CFR timestamps")
        video = np.stack(selected)
        meta = VideoMetadata(
            total_num_frames=total,
            fps=fps,
            width=width,
            height=height,
            duration=duration,
            frames_indices=indices,
        )
        kwargs.update(videos=[video], video_metadata=[meta], do_sample_frames=False)
        metadata = {
            "frame_indices": indices,
            "timestamps_seconds": pts,
            "source_fps": fps,
            "selected_frames": len(selected),
        }
    values = proc(**kwargs)
    if "input_features_mask" in values:
        values["input_features_mask"] = values["input_features_mask"].long()
    for key, value in values.items():
        if hasattr(value, "device") and value.device.type != "cpu":
            raise ValueError(("processor used accelerator", key))
    return values, metadata


def make_fixtures(out, text_cases):
    """Small deterministic fixtures, retained by hash outside the source tree."""
    out.mkdir(parents=True, exist_ok=True)
    y, x = np.indices((768, 1024))
    rgb = np.stack(
        ((x * 3 + y) % 256, (y * 4 + x) % 256, (x * 7 + y * 5) % 256), -1
    ).astype(np.uint8)
    Image.fromarray(rgb).save(out / "image.jpg", format="JPEG")
    for duration in (1, 12, 30):
        t = np.arange(16000 * duration) / 16000
        pcm = np.round(
            (np.sin(t * 2 * np.pi * 220) * 0.12 + np.sin(t * 2 * np.pi * 440) * 0.04)
            * 32767
        ).astype("<i2")
        with wave.open(str(out / f"audio-{duration}s.wav"), "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            wav.setframerate(16000)
            wav.writeframes(pcm.tobytes())
    import av

    with av.open(str(out / "video-6s.mp4"), "w") as container:
        stream = container.add_stream("libx264", rate=10)
        stream.width = 1280
        stream.height = 720
        stream.pix_fmt = "yuv420p"
        stream.options = {"preset": "ultrafast", "crf": "28"}
        y, x = np.indices((720, 1280))
        for i in range(60):
            frame = np.stack(
                (
                    (x + i * 7) % 256,
                    (y + i * 11) % 256,
                    ((x // 3 + y // 3) + i * 13) % 256,
                ),
                -1,
            ).astype(np.uint8)
            for packet in stream.encode(
                av.VideoFrame.from_ndarray(frame, format="rgb24")
            ):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)
    cases = [
        {
            "id": c["id"],
            "kind": "text",
            "text": c["text"],
            "max_tokens": c["max_tokens"],
            "expected_prompt_tokens": len(c["prompt_ids"]),
        }
        for c in text_cases
    ]
    cases += [
        {
            "id": "image_64",
            "kind": "image",
            "text": "Describe this image in one paragraph.",
            "path": "image.jpg",
            "max_tokens": 64,
        }
    ]
    cases += [
        {
            "id": f"audio_{d}s_64",
            "kind": "audio",
            "text": "Transcribe this audio clip.",
            "path": f"audio-{d}s.wav",
            "max_tokens": 64,
        }
        for d in (1, 12, 30)
    ]
    cases += [
        {
            "id": "video_6s_64",
            "kind": "video",
            "text": "Describe what happens in this video.",
            "path": "video-6s.mp4",
            "max_tokens": 64,
        }
    ]
    files = {
        p.name: {
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            "bytes": p.stat().st_size,
        }
        for p in out.iterdir()
        if p.suffix in (".jpg", ".mp4", ".wav")
    }
    suite = {
        "warmups": 2,
        "repeats": 3,
        "context_tokens": 8192,
        "cases": cases,
        "files": files,
        "fixture_scope": "synthetic deterministic media; geometry/implementation qualification, not task quality",
    }
    (out / "suite.json").write_text(json.dumps(suite, indent=2) + "\n")
    return suite
