#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualify MP4 codecs and AAC against an owned CUDA embedding server.

Video pixel parity is checked by the native CUDA tests against the fixture
receipt. AAC embedding parity uses independent FFmpeg-decoded WAV inputs.
This does not substitute for matched-precision pretrained-model qualification.
"""

import argparse
from array import array
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import struct
import sys
import time
import urllib.request

from cuda_video_qualification import (
    cleanup_snapshot,
    media_input,
    request,
    sha256,
    validate_vector,
)


def agreement(actual, expected):
    validate_vector(actual)
    validate_vector(expected)
    return {
        "cosine": sum(x * y for x, y in zip(actual, expected))
        / math.sqrt(sum(x * x for x in actual) * sum(x * x for x in expected)),
        "max_abs": max(abs(x - y) for x, y in zip(actual, expected)),
    }


# Fixed, global acceptance bounds for normalized unit vectors. Cosine >= .995
# bounds Euclidean drift to .1; no component may move by more than 1% of unit
# norm. These are approximate decoder interoperability bounds, not exact parity.
FFMPEG_EMBEDDING_MIN_COSINE = 0.995
FFMPEG_EMBEDDING_MAX_ABS = 0.01


def ffmpeg_embedding_acceptance(actual, expected):
    measured = agreement(actual, expected)
    if (
        measured["cosine"] < FFMPEG_EMBEDDING_MIN_COSINE
        or measured["max_abs"] > FFMPEG_EMBEDDING_MAX_ABS
    ):
        raise ValueError(f"independent FFmpeg embedding acceptance failed: {measured}")
    return measured


def pcm_comparison(actual, expected):
    if len(actual) != len(expected) or not actual:
        raise ValueError("decoded PCM sample count differs")
    if any(not math.isfinite(x) for x in actual + expected):
        raise ValueError("decoded PCM is nonfinite")
    signal = sum(x * x for x in expected)
    noise = sum((x - y) ** 2 for x, y in zip(actual, expected))
    snr = (
        200.0
        if noise == 0
        else -200.0
        if signal == 0
        else 10 * math.log10(signal / noise)
    )
    return {
        "samples": len(actual),
        "snr_db": snr,
        "mean_abs": sum(abs(x - y) for x, y in zip(actual, expected)) / len(actual),
        "max_abs": max(abs(x - y) for x, y in zip(actual, expected)),
    }


def float_samples(payload):
    values = array("f")
    values.frombytes(payload)
    if sys.byteorder != "little":
        values.byteswap()
    return values


def native_pcm_reference(args, sound):
    prefix = args.output.parent / (args.output.stem + "." + sound["path"])
    raw = prefix.with_name(prefix.name + ".native.f32")
    subprocess.run(
        [
            str(args.pcm_probe.resolve()),
            str((args.fixtures / sound["path"]).resolve()),
            str(raw.resolve()),
        ],
        check=True,
        capture_output=True,
    )
    payload = raw.read_bytes()
    metadata = json.loads(Path(str(raw) + ".json").read_text())
    if (
        metadata["sample_rate"] != sound["sample_rate"]
        or metadata["samples"] != sound["samples"]
    ):
        raise ValueError("native AAC clock or trimmed sample count differs")
    actual = float_samples(payload)
    streamed = float_samples(Path(str(raw) + ".stream.f32").read_bytes())
    reference = (args.fixtures / sound["reference"]).read_bytes()
    if reference[20:24] != b"\x03\x00\x01\x00" or reference[36:40] != b"data":
        raise ValueError("expected the unquantized mono F32 reference WAV")
    expected = float_samples(reference[44:])
    comparison = pcm_comparison(actual, expected)
    comparison["stream_vs_full"] = pcm_comparison(streamed, actual)
    # AUDIO.md's 50 dB complete-output floor is the owned HE-AAC/PS
    # contract. For AAC-LC retain the tight absolute PCM error gate; PNS
    # transients and near-silence make a universal relative SNR misleading.
    comparison["minimum_snr_db"] = (
        50 if sound["profile"] in ("HE-AAC", "HE-AACv2") else None
    )
    if comparison["stream_vs_full"]["max_abs"] != 0:
        raise ValueError("streaming and full AAC decoders disagree")
    rate = sound["sample_rate"]
    header = b"RIFF" + struct.pack("<I", 36 + len(payload)) + b"WAVEfmt "
    header += struct.pack("<IHHIIHH", 16, 3, 1, rate, rate * 4, 4, 32)
    header += b"data" + struct.pack("<I", len(payload))
    return header + payload, comparison


@contextmanager
def server(args):
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", 0))
        port = endpoint.getsockname()[1]
    command = [str(args.binary.resolve())]
    if args.full_binary:
        command.append("inference")
    command.extend(
        [
            "run",
            "--port",
            str(port),
            "--models-dir",
            str(args.model_dir.resolve().parent),
            "--host-budget-mb",
            "8192",
            "--backend-budget-mb",
            "8192",
            "--combined-budget-mb",
            "16384",
            "--scratch-budget-mb",
            str(args.scratch_budget_mb),
        ]
    )
    if args.process_memory_budget_mb:
        command.extend(
            ["--process-memory-budget-mb", str(args.process_memory_budget_mb)]
        )
    with args.output.with_suffix(".server.log").open("w") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=log,
            env=dict(
                os.environ,
                ANTFLY_INFERENCE_REQUIRED_BACKEND="cuda",
                ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_STATS="1",
            ),
        )
        url = f"http://127.0.0.1:{port}"
        try:
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("owned CUDA server did not become healthy")
                try:
                    with urllib.request.urlopen(url + "/healthz", timeout=1):
                        break
                except OSError:
                    time.sleep(0.1)
            yield url, process, command
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--full-binary", action="store_true")
    parser.add_argument("--model-dir", required=True, type=Path)
    parser.add_argument("--pcm-probe", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--video-only", action="store_true")
    mode.add_argument("--audio-only", action="store_true")
    parser.add_argument(
        "--fixtures",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "src/testdata/video/codecs",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--scratch-budget-mb", type=int, default=4096)
    parser.add_argument(
        "--process-memory-budget-mb",
        type=int,
        default=0,
        help="Explicit whole-leaf-cgroup envelope; default uses automatic physical/cgroup limits",
    )
    parser.add_argument("--mixed-jobs", type=int, default=9)
    args = parser.parse_args()
    if args.iterations < 2 or args.mixed_jobs < 0 or args.process_memory_budget_mb < 0:
        parser.error("at least two iterations and nonnegative mixed jobs are required")
    if not args.video_only and args.pcm_probe is None:
        parser.error(
            "AAC qualification requires --pcm-probe for the native/independent PCM gates"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    receipt_path = args.fixtures / "receipt.json"
    receipt = json.loads(receipt_path.read_text())
    for row in receipt["videos"] + receipt["audio"]:
        if sha256(args.fixtures / row["path"]) != row["sha256"]:
            raise ValueError(f"fixture changed: {row['path']}")
        if (
            "reference" in row
            and sha256(args.fixtures / row["reference"]) != row["reference_sha256"]
        ):
            raise ValueError(f"reference changed: {row['reference']}")
    checkpoint = json.loads(
        (args.model_dir / "embeddinggemma2_receipt.json").read_text()
    )
    for row in checkpoint["files"]:
        if sha256(args.model_dir / row["path"]) != row["sha256"]:
            raise ValueError(f"checkpoint changed: {row['path']}")
    result = {
        "status": "running",
        "binary_sha256": sha256(args.binary),
        "tool_sha256": sha256(Path(__file__)),
        "fixture_receipt_sha256": sha256(receipt_path),
        "checkpoint_receipt": checkpoint,
        "cases": [],
        "source_sha": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "source_diff_sha256": hashlib.sha256(
            subprocess.check_output(["git", "diff", "HEAD"])
        ).hexdigest(),
        "gpu": subprocess.check_output(["nvidia-smi"], text=True),
        "scratch_budget_mb": args.scratch_budget_mb,
        "process_memory_budget_mb": args.process_memory_budget_mb or "auto",
        "audio_parity_policy": {
            "independent_he_aac_pcm_min_snr_db": 50,
            "independent_pcm_max_mean_abs": 0.0001,
            "native_pcm_cuda_embedding_max_abs": 1e-5,
            "ffmpeg_embedding_min_cosine": FFMPEG_EMBEDDING_MIN_COSINE,
            "ffmpeg_embedding_max_abs": FFMPEG_EMBEDDING_MAX_ABS,
        },
    }
    identity = None
    vectors = {}
    failures = []
    native_pcm = {}
    if not args.video_only:
        result["pcm_probe_sha256"] = sha256(args.pcm_probe)
        result["aac_pcm"] = {}
        for sound in receipt["audio"]:
            native_pcm[sound["path"]], measured = native_pcm_reference(args, sound)
            measured["status"] = (
                "pass"
                if (
                    measured["minimum_snr_db"] is None
                    or measured["snr_db"] >= measured["minimum_snr_db"]
                )
                and measured["mean_abs"] <= 0.0001
                else "failed"
            )
            result["aac_pcm"][sound["path"]] = measured
            if measured["status"] != "pass":
                failures.append("independent_pcm:" + sound["path"])

    def publish():
        args.output.write_text(json.dumps(result, indent=2) + "\n")

    try:
        with server(args) as (url, process, command):
            result["command"] = command

            def run(
                name,
                inputs,
                expected=200,
                deadline_ms=None,
                reference=None,
                ffmpeg_reference=None,
            ):
                nonlocal identity
                row = {"case": name, "expected_status": expected}
                result["cases"].append(row)
                started = time.monotonic()
                try:
                    status, response = request(
                        url,
                        {"model": args.model_dir.name, "input": inputs},
                        120,
                        deadline_ms,
                    )
                    row.update(http_status=status, seconds=time.monotonic() - started)
                    if status != expected:
                        row["response"] = response
                        raise ValueError(f"expected HTTP {expected}, got {status}")
                    vector = None
                    if status == 200:
                        vector = response["data"][0]["embedding"]
                        row["norm"] = validate_vector(vector)
                        row["vector"] = vector
                        current = response["model_identity"]
                        if identity is not None and identity != current:
                            raise ValueError("model identity changed")
                        identity = current
                        if name in vectors:
                            row["repeat"] = agreement(vector, vectors[name])
                            if row["repeat"]["max_abs"] > 1e-5:
                                raise ValueError("repeat or recovery changed vector")
                        if reference is not None:
                            row["reference"] = agreement(vector, reference)
                            if row["reference"]["max_abs"] > 1e-5:
                                raise ValueError(
                                    "AAC/native PCM CUDA embedding parity failed"
                                )
                        if ffmpeg_reference is not None:
                            row["ffmpeg_embedding_acceptance"] = (
                                ffmpeg_embedding_acceptance(vector, ffmpeg_reference)
                            )
                        vectors[name] = vector
                    else:
                        row["response"] = response
                    row["cleanup"] = cleanup_snapshot(url, process.pid)
                    row["status"] = "pass"
                    return vector
                except Exception as error:
                    row.update(status="failed", error=str(error))
                    failures.append(name)
                    try:
                        row["cleanup"] = cleanup_snapshot(url, process.pid)
                    except Exception as cleanup_error:
                        row["cleanup_error"] = str(cleanup_error)
                    return None
                finally:
                    print(name, row.get("status", "failed"), flush=True)
                    publish()

            text = ["Codec qualification recovery control."]
            run("text", text)
            for iteration in range(args.iterations):
                for clip in [] if args.audio_only else receipt["videos"]:
                    run(
                        clip["path"],
                        media_input(
                            "video/mp4", (args.fixtures / clip["path"]).read_bytes()
                        ),
                        expected=200 if clip["accepted"] else 400,
                    )
                for sound in [] if args.video_only else receipt["audio"]:
                    wav = run(
                        sound["reference"],
                        media_input(
                            "audio/wav",
                            (args.fixtures / sound["reference"]).read_bytes(),
                        ),
                    )
                    native = run(
                        sound["path"] + ".native.wav",
                        media_input("audio/wav", native_pcm[sound["path"]]),
                    )
                    mime = (
                        "audio/aac" if sound["path"].endswith(".aac") else "audio/mp4"
                    )
                    run(
                        "audio:" + sound["path"],
                        media_input(mime, (args.fixtures / sound["path"]).read_bytes()),
                        reference=native,
                        ffmpeg_reference=wav,
                    )
                if not args.audio_only:
                    run(
                        "corrupt",
                        media_input("video/mp4", b"invalid MP4"),
                        expected=400,
                    )
                for name in () if args.audio_only else ("hevc-hvc1.mp4", "av1.mp4"):
                    data = bytearray((args.fixtures / name).read_bytes())
                    # Leave the container intact but invalidate every coded
                    # byte, forcing decoder/error cleanup after inspection.
                    mdat = data.index(b"mdat")
                    size = int.from_bytes(data[mdat - 4 : mdat], "big")
                    if size < 8:
                        raise ValueError("fixture needs a bounded 32-bit mdat")
                    data[mdat + 4 : mdat - 4 + size] = b"\xff" * (size - 8)
                    run("corrupt_" + name, media_input("video/mp4", data), expected=400)
                if not args.video_only:
                    run(
                        "corrupt_aac",
                        media_input("audio/aac", b"invalid AAC"),
                        expected=400,
                    )
                    run(
                        "truncated_m4a",
                        media_input(
                            "audio/mp4",
                            (args.fixtures / "aac-lc.m4a").read_bytes()[:32],
                        ),
                        expected=400,
                    )
                run("text", text)
            if not args.video_only:
                anchors = [
                    sound
                    for sound in receipt["audio"]
                    if sound.get("content")
                    in ("silence", "transient", "music", "speech-en", "speech-es")
                ]
                result["ffmpeg_reference_retrieval"] = []
                for sound in anchors:
                    row = {"case": "ffmpeg_reference_retrieval:" + sound["path"]}
                    result["cases"].append(row)
                    try:
                        query = vectors["audio:" + sound["path"]]
                        scores = {
                            candidate["path"]: agreement(
                                query, vectors[candidate["reference"]]
                            )["cosine"]
                            for candidate in anchors
                        }
                        ranked = sorted(scores, key=scores.get, reverse=True)
                        if len(ranked) < 2 or ranked[0] != sound["path"]:
                            raise ValueError(
                                "native AAC lost its independent reference top-1 match"
                            )
                        # Swapping a genuinely different reference must fail
                        # the very same component/cosine acceptance predicate.
                        rejected = 0
                        for candidate in anchors:
                            if candidate["path"] == sound["path"]:
                                continue
                            try:
                                ffmpeg_embedding_acceptance(
                                    query, vectors[candidate["reference"]]
                                )
                            except ValueError:
                                rejected += 1
                            else:
                                raise ValueError(
                                    "independent gate accepted a mismatched audio reference"
                                )
                        row.update(
                            status="pass",
                            scores=scores,
                            top1_margin=scores[ranked[0]] - scores[ranked[1]],
                            mismatched_references_rejected=rejected,
                        )
                    except Exception as error:
                        row.update(status="failed", error=str(error))
                        failures.append(row["case"])
                    result["ffmpeg_reference_retrieval"].append(row)
                    print(row["case"], row["status"], flush=True)
                    publish()
            if "hevc-hvc1.mp4" in vectors and "hevc-aac.mp4" in vectors:
                result["muxed_audio_preserves_video"] = agreement(
                    vectors["hevc-hvc1.mp4"], vectors["hevc-aac.mp4"]
                )
                if result["muxed_audio_preserves_video"]["max_abs"] != 0:
                    failures.append("muxed audio changed video embedding")
            run(
                "deadline",
                media_input(
                    "audio/mp4" if args.audio_only else "video/mp4",
                    (
                        args.fixtures / ("aac-lc.m4a" if args.audio_only else "av1.mp4")
                    ).read_bytes(),
                ),
                expected=504,
                deadline_ms=1,
            )
            run("text", text)
            if not args.audio_only:
                run(
                    "h264.mp4",
                    media_input("video/mp4", (args.fixtures / "h264.mp4").read_bytes()),
                )
            if not args.video_only:
                run(
                    "audio:aac-lc.m4a",
                    media_input(
                        "audio/mp4", (args.fixtures / "aac-lc.m4a").read_bytes()
                    ),
                    reference=vectors.get("aac-lc.m4a.native.wav"),
                    ffmpeg_reference=vectors.get("aac-lc.m4a.reference.wav"),
                )
            if not failures and args.mixed_jobs:
                names = (
                    ["aac-lc.m4a", "he-aac-v1.m4a", "he-aac-v2.m4a"]
                    if args.audio_only
                    else ["hevc-hvc1.mp4", "av1.mp4", "h264.mp4"]
                    if args.video_only
                    else ["hevc-hvc1.mp4", "av1.mp4", "aac-lc.m4a"]
                )
                with ThreadPoolExecutor(max_workers=3) as executor:
                    jobs = {}
                    for index in range(args.mixed_jobs):
                        name = names[index % len(names)]
                        mime = "audio/mp4" if name.endswith(".m4a") else "video/mp4"
                        future = executor.submit(
                            request,
                            url,
                            {
                                "model": args.model_dir.name,
                                "input": media_input(
                                    mime, (args.fixtures / name).read_bytes()
                                ),
                            },
                            120,
                        )
                        jobs[future] = name
                    for future in as_completed(jobs):
                        name = jobs[future]
                        row = {"case": "mixed_" + name}
                        result["cases"].append(row)
                        try:
                            status, response = future.result()
                            row["http_status"] = status
                            if status != 200:
                                row["response"] = response
                                raise ValueError("mixed request failed")
                            vector = response["data"][0]["embedding"]
                            key = "audio:" + name if name.endswith(".m4a") else name
                            row["agreement"] = agreement(vector, vectors[key])
                            if (
                                response["model_identity"] != identity
                                or row["agreement"]["max_abs"] > 1e-5
                            ):
                                raise ValueError(
                                    "mixed request changed vector or model identity"
                                )
                            row["status"] = "pass"
                        except Exception as error:
                            row.update(status="failed", error=str(error))
                            failures.append(row["case"])
                        print(row["case"], row["status"], flush=True)
                result["mixed_cleanup"] = cleanup_snapshot(url, process.pid)
            result["model_identity"] = identity
    except Exception as error:
        result["error"] = str(error)
        failures.append("server")
    log = args.output.with_suffix(".server.log").read_text()
    if "selected backend cuda for" not in log:
        failures.append("CUDA backend confirmation")
    result["failures"] = failures
    result["status"] = "failed" if failures else "pass"
    publish()
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
