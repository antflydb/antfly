#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Generate synthetic codec inputs and independent FFmpeg decode receipts.

Requires av==16.0.1. PyAV/FFmpeg is qualification tooling, never a runtime
dependency. Pixel receipts use selected YUV420 frames, avoiding differences
between FFmpeg RGB conversion and the declared CUDA NV12 conversion policy.
"""

import argparse
from array import array
from fractions import Fraction
import hashlib
import json
import math
from pathlib import Path
import shutil
import struct

import av


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def video(path, encoder, tag, depth=8, duration=4):
    rate = 4 if duration == 4 else 1
    frame_count = duration * rate
    with av.open(str(path), "w", format="mp4") as output:
        stream = output.add_stream(encoder, rate=rate)
        stream.width = stream.height = 192
        stream.pix_fmt = "yuv420p" if depth == 8 else "yuv420p10le"
        ctx = stream.codec_context
        ctx.codec_tag = tag
        ctx.thread_count = 1
        ctx.gop_size = 8
        ctx.color_range = 1
        ctx.colorspace = ctx.color_primaries = ctx.color_trc = 1
        if encoder == "libx265":
            ctx.options = {
                "preset": "ultrafast",
                "crf": "18",
                "x265-params": "log-level=error:pools=none:frame-threads=1:bframes=2",
            }
        elif encoder == "libx264":
            ctx.options = {"preset": "ultrafast", "crf": "18", "bf": "2"}
        else:
            ctx.options = {"cpu-used": "8", "crf": "18", "lag-in-frames": "0"}
        for index in range(frame_count):
            frame = av.VideoFrame(192, 192, "yuv420p")
            frame.pts, frame.time_base = index, Fraction(1, rate)
            frame.color_range, frame.colorspace = 1, 1
            for plane_index, plane in enumerate(frame.planes):
                data = bytearray(plane.buffer_size)
                for y in range(plane.height):
                    for x in range(plane.width):
                        value = (
                            (32 + (x + y + index * 11) % 190)
                            if plane_index == 0
                            else (96 + (index * 7 + x // 8 + plane_index * 31) % 64)
                        )
                        data[y * plane.line_size + x] = value
                plane.update(data)
            if depth != 8:
                frame = frame.reformat(format="yuv420p10le")
            for packet in stream.encode(frame):
                output.mux(packet)
        for packet in stream.encode():
            output.mux(packet)


def audio(path, container, pattern="tone", speech_path=None):
    speech = []
    if speech_path is not None:
        with av.open(str(speech_path)) as source:
            resampler = av.AudioResampler(format="fltp", layout="stereo", rate=48000)
            for frame in source.decode(audio=0):
                for converted in resampler.resample(frame):
                    speech.extend(
                        memoryview(converted.planes[0]).cast("f")[: converted.samples]
                    )
            for converted in resampler.resample(None):
                speech.extend(
                    memoryview(converted.planes[0]).cast("f")[: converted.samples]
                )

    def sample(position, channel):
        t = position / 48000
        if speech_path is not None:
            return speech[position] if position < len(speech) else 0.0
        if pattern == "silence":
            return 0.0
        if pattern == "quiet":
            return 0.0001 * math.sin(2 * math.pi * (440 + 220 * channel) * t)
        if pattern == "transient":
            phase = position % (12000 + channel * 1700)
            return 0.7 * math.exp(-phase / 80) * math.sin(2 * math.pi * 1800 * t)
        if pattern == "music":
            notes = (261.6256, 329.6276, 391.9954, 523.2511)
            frequency = notes[int(t * 2 + channel) % len(notes)]
            return 0.12 * math.sin(2 * math.pi * frequency * t) + 0.06 * math.sin(
                2 * math.pi * frequency * 2 * t
            )
        return 0.25 * math.sin(2 * math.pi * (440 + 220 * channel) * t)

    with av.open(str(path), "w", format=container) as output:
        stream = output.add_stream("aac", rate=48000)
        stream.layout = "stereo"
        stream.bit_rate = 96000
        for offset in range(0, 192000, 1024):
            count = min(1024, 192000 - offset)
            frame = av.AudioFrame(format="fltp", layout="stereo", samples=count)
            frame.sample_rate = 48000
            frame.pts, frame.time_base = offset, Fraction(1, 48000)
            for channel, plane in enumerate(frame.planes):
                values = array(
                    "f",
                    (sample(offset + i, channel) for i in range(count)),
                )
                plane.update(values.tobytes())
            for packet in stream.encode(frame):
                output.mux(packet)
        for packet in stream.encode():
            output.mux(packet)


def mux_audio(video_path, audio_path, output_path):
    with (
        av.open(str(video_path)) as source,
        av.open(str(audio_path)) as sound,
        av.open(str(output_path), "w", format="mp4") as output,
    ):
        video_stream = output.add_stream_from_template(source.streams.video[0])
        audio_stream = output.add_stream_from_template(sound.streams.audio[0])
        packets = []
        for container, stream, target in (
            (source, source.streams.video[0], video_stream),
            (sound, sound.streams.audio[0], audio_stream),
        ):
            for packet in container.demux(stream):
                if packet.dts is not None:
                    packets.append((packet.dts * packet.time_base, packet, target))
        for _, packet, target in sorted(packets, key=lambda row: row[0]):
            packet.stream = target
            output.mux(packet)


def video_receipt(path):
    frames = []
    with av.open(str(path)) as source:
        stream = source.streams.video[0]
        codec = stream.codec_context.name
        config = stream.codec_context.extradata.hex()
        duration_us = int(stream.duration * stream.time_base * 1000000)
        for frame in source.decode(stream):
            planar = frame.reformat(format="yuv420p")
            digest = hashlib.sha256()
            for plane in planar.planes:
                data = bytes(plane)
                for y in range(plane.height):
                    digest.update(
                        data[y * plane.line_size : y * plane.line_size + plane.width]
                    )
            frames.append(
                {
                    "pts": frame.pts,
                    "time_base": str(frame.time_base),
                    "yuv420_sha256": digest.hexdigest(),
                }
            )
    samples = max(1, duration_us // 1000000)
    indexes = []
    for index in range(min(samples, 32)):
        second = index * (samples - 1) // 31 if samples > 32 else index
        indexes.append(
            min(len(frames) - 1, second * len(frames) * 1000000 // duration_us)
        )
    return {
        "codec": codec,
        "configuration": config,
        "frames": len(frames),
        "duration_us": duration_us,
        "selected_indexes": indexes,
        "selected_yuv420_sha256": [frames[i]["yuv420_sha256"] for i in indexes],
        "presentation": frames,
    }


def audio_reference(path, output_path):
    # Preserve the independent decoder's floating PCM. Quantizing a quiet
    # AAC noise floor to PCM16 changes log-mel features before model inference.
    samples = array("f")
    with av.open(str(path)) as source:
        stream = source.streams.audio[0]
        rate = stream.codec_context.sample_rate
        profile = stream.codec_context.profile
        end = (
            None
            if stream.duration is None
            else int(stream.duration * stream.time_base * rate)
        )
        for frame in source.decode(stream):
            if frame.format.name != "fltp":
                raise ValueError("expected independent planar F32 AAC decode")
            channels = [memoryview(plane).cast("f") for plane in frame.planes]
            for index in range(frame.samples):
                position = int(frame.pts * frame.time_base * rate) + index
                if position < 0 or (end is not None and position >= end):
                    continue
                value = sum(
                    max(-1, min(1, channel[index])) for channel in channels
                ) / len(channels)
                samples.append(value)
    payload = samples.tobytes()
    header = b"RIFF" + struct.pack("<I", 36 + len(payload)) + b"WAVEfmt "
    header += struct.pack("<IHHIIHH", 16, 3, 1, rate, rate * 4, 4, 32)
    header += b"data" + struct.pack("<I", len(payload))
    output_path.write_bytes(header + payload)
    return {
        "sample_rate": rate,
        "profile": profile,
        "samples": len(samples),
        "reference": output_path.name,
        "reference_sha256": sha(output_path),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=int, choices=(4, 60), default=4)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    specs = [
        ("h264.mp4", "libx264", "avc1", 8),
        ("hevc-hvc1.mp4", "libx265", "hvc1", 8),
        ("hevc-hev1.mp4", "libx265", "hev1", 8),
        ("av1.mp4", "libaom-av1", "av01", 8),
        ("hevc-10bit.mp4", "libx265", "hvc1", 10),
        ("av1-10bit.mp4", "libaom-av1", "av01", 10),
    ]
    receipt = {
        "generator_sha256": sha(Path(__file__)),
        "pyav": av.__version__,
        "libraries": av.library_versions,
        "provenance": "Original synthetic YUV pattern and stereo sine waves; generated for Antfly qualification.",
        "videos": [],
        "audio": [],
    }
    for name, encoder, tag, depth in specs:
        path = args.output / name
        video(path, encoder, tag, depth, args.duration_seconds)
        receipt["videos"].append(
            {
                "path": name,
                "sha256": sha(path),
                "bit_depth": depth,
                "accepted": depth == 8,
                **video_receipt(path),
            }
        )
    for name, container in (("aac-lc.m4a", "ipod"), ("aac-lc.aac", "adts")):
        path = args.output / name
        audio(path, container)
        reference = args.output / (name + ".reference.wav")
        receipt["audio"].append(
            {"path": name, "sha256": sha(path), **audio_reference(path, reference)}
        )
    speech_root = Path(__file__).resolve().parents[4] / "e2e/inference/testdata"
    for pattern, speech_name in (
        ("silence", None),
        ("quiet", None),
        ("transient", None),
        ("music", None),
        ("speech-en", "whisper_quality.wav"),
        ("speech-es", "whisper_spanish_quality.wav"),
    ):
        path = args.output / ("aac-lc-" + pattern + ".m4a")
        source = speech_root / speech_name if speech_name else None
        audio(path, "ipod", pattern=pattern, speech_path=source)
        row = {
            "path": path.name,
            "sha256": sha(path),
            "content": pattern,
            **audio_reference(path, args.output / (path.name + ".reference.wav")),
        }
        if source is not None:
            row.update(
                source_fixture=str(source.relative_to(speech_root.parents[2])),
                source_sha256=sha(source),
            )
        receipt["audio"].append(row)
    corpus = Path(__file__).resolve().parents[4] / "lib/audio/testdata/codec-corpus"
    for name, source_name in (
        ("he-aac-v1.m4a", "owned-he-aac-48000-mixed.m4a"),
        ("he-aac-v2.m4a", "owned-he-aac-v2-48000-mixed.m4a"),
    ):
        path = args.output / name
        shutil.copyfile(corpus / source_name, path)
        reference = args.output / (name + ".reference.wav")
        receipt["audio"].append(
            {
                "path": name,
                "source_fixture": source_name,
                "sha256": sha(path),
                **audio_reference(path, reference),
            }
        )
    mux_path = args.output / "hevc-aac.mp4"
    mux_audio(args.output / "hevc-hvc1.mp4", args.output / "aac-lc.m4a", mux_path)
    receipt["videos"].append(
        {
            "path": mux_path.name,
            "sha256": sha(mux_path),
            "bit_depth": 8,
            "accepted": True,
            **video_receipt(mux_path),
        }
    )
    receipt["audio"].append(
        {
            "path": mux_path.name,
            "sha256": sha(mux_path),
            **audio_reference(mux_path, args.output / "hevc-aac.mp4.reference.wav"),
        }
    )
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
