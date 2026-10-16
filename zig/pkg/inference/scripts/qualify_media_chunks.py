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

"""Compare completed chunk embeddings with independently embedding each payload.

Uses only the Python standard library and checked-in/generated fixtures. Start a
native or Metal inference server with an EmbeddingGemma 2 checkpoint, then run:
  python3 zig/pkg/inference/scripts/qualify_media_chunks.py --url http://127.0.0.1:18083 --model model
Use --video for a realistic local clip, --repeat for warm timings, and --stream
to qualify acknowledged NDJSON delivery. Reports are written only when --output
is supplied; checkpoint weights and result logs do not belong in source control.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import io
import json
import math
import os
import signal
import socket
import subprocess
import tempfile
from pathlib import Path
import struct
import time
import urllib.error
import urllib.parse
import urllib.request
import wave
import zlib

from chunk_qualification_common import HttpError, complete_request

ROOT = Path(__file__).resolve().parents[4]
FIXTURES = ROOT / "zig/pkg/inference/src/testdata"


def media(mime: str, data: bytes) -> dict:
    return {
        "type": "media",
        "mime_type": mime,
        "data": base64.b64encode(data).decode("ascii"),
    }


def png() -> bytes:
    def chunk(tag: bytes, body: bytes) -> bytes:
        return (
            struct.pack(">I", len(body))
            + tag
            + body
            + struct.pack(">I", zlib.crc32(tag + body))
        )

    pixels = b"".join(
        b"\0" + bytes(v for x in range(128) for v in (x * 2, y * 2, (x + y) % 256, 255))
        for y in range(96)
    )
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 128, 96, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(pixels))
        + chunk(b"IEND", b"")
    )


def fixtures(video: Path | None):
    sound = io.BytesIO()
    with wave.open(sound, "wb") as writer:
        writer.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
        writer.writeframes(
            b"".join(
                struct.pack("<h", round(8000 * math.sin(i * 2 * math.pi * 440 / 16000)))
                for i in range(16000)
            )
        )
    gif = bytes.fromhex(
        "4749463839610100010000000021ff0b4e45545343415045322e30030100000021f90401050000002c000000000100010081000000ff000000ff0000000002024c010021f90401070000002c000000000100010081000000ff000000ff0000000002025401003b"
    )
    movie = video or FIXTURES / "video/bframes.mp4"
    mime = "video/webm" if movie.suffix.lower() == ".webm" else "video/mp4"
    return {
        "text": (
            "Antfly preserves source spans. Héllo 世界 🐜. " * 3,
            {"target_tokens": 24, "overlap_tokens": 0},
        ),
        "image": (media("image/png", png()), {"tile_width": 64, "tile_height": 48}),
        "gif": (media("image/gif", gif), {}),
        "audio": (media("audio/wav", sound.getvalue()), {"window_ns": 500_000_000}),
        "pdf": (
            media("application/pdf", (FIXTURES / "chunk-pages.pdf").read_bytes()),
            {},
        ),
        "video": (
            media(mime, movie.read_bytes()),
            {"window_ns": 500_000_000, "step_ns": 250_000_000},
        ),
    }


def post(url: str, route: str, body: dict):
    request = urllib.request.Request(
        url.rstrip("/") + "/ai/v1/" + route,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    # Admission can remain busy briefly after a completed response while its
    # worker releases leases. Retry only explicit pre-response admission errors;
    # streaming coverage and transport failures retain their terminal semantics.
    for attempt in range(4):
        try:
            return urllib.request.urlopen(request, timeout=1800)
        except urllib.error.HTTPError as error:
            detail = error.read(16384).decode(errors="replace")
            error.close()
            try:
                reply = json.loads(detail)
            except ValueError:
                reply = None
            if (
                attempt < 3
                and error.code in (429, 503)
                and isinstance(reply, dict)
                and reply.get("retryable") is True
            ):
                delay = reply.get("retry_after_ms", 1000)
                if (
                    isinstance(delay, (int, float))
                    and math.isfinite(delay)
                    and 0 <= delay <= 10000
                ):
                    time.sleep(delay / 1000)
                    continue
            raise HttpError(
                f"{route}: HTTP {error.code}: {detail}",
                error.code in (429, 503)
                and isinstance(reply, dict)
                and reply.get("retryable") is True,
            ) from error


def chunk_response(url: str, body: dict):
    with post(url, "chunk", body) as response:
        if not body.get("stream"):
            return json.load(response)
        context = None
        items = []
        while line := response.readline(16 * 1024 * 1024 + 1):
            if len(line) > 16 * 1024 * 1024 or not line.endswith(b"\n"):
                raise AssertionError("unbounded or incomplete stream record")
            event = json.loads(line)
            if event["object"] == "chunk.embedding.start":
                assert context is None and not items
                context = event["context"]
            elif event["object"] == "chunk.embedding":
                assert context is not None
                assert (
                    event["data"]["descriptor"]["id"]
                    == f"{context['plan_id']}:{context['start_ordinal'] + len(items)}"
                )
                items.append(event["data"])
            elif event["object"] == "chunk.embedding.complete":
                terminal = event["response"]
                assert context is not None
                for key in (
                    "plan_id",
                    "source_fingerprint",
                    "source_version",
                    "model_identity",
                    "model_revision",
                    "start_ordinal",
                ):
                    assert terminal[key] == context[key], key
                assert terminal["covered_through_ordinal"] == context[
                    "start_ordinal"
                ] + len(items)
                terminal["data"] = items
                return terminal
            else:
                raise AssertionError(event)
        raise AssertionError("EOF before terminal coverage")


@contextlib.contextmanager
def owned_server(args):
    if args.binary is None:
        yield args.url or "http://127.0.0.1:18083"
        return
    if args.url or args.model_dir is None:
        raise ValueError(
            "--binary requires --model-dir and cannot be combined with --url"
        )
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", 0))
        port = endpoint.getsockname()[1]
    with tempfile.TemporaryFile(mode="w+") as log:
        process = subprocess.Popen(
            [
                str(args.binary.resolve()),
                "run",
                "--models-dir",
                str(args.model_dir.resolve().parent),
                "--port",
                str(port),
                "--host-budget-mb",
                "6144",
                "--backend-budget-mb",
                "12288",
                "--combined-budget-mb",
                "18432",
                "--scratch-budget-mb",
                "8192",
            ],
            env=dict(os.environ, ANTFLY_INFERENCE_REQUIRED_BACKEND=args.backend),
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
        url = f"http://127.0.0.1:{port}"
        try:
            for _ in range(300):
                if process.poll() is not None:
                    log.seek(0)
                    raise RuntimeError(
                        "owned inference server exited: " + log.read(16384)
                    )
                try:
                    with urllib.request.urlopen(url + "/healthz", timeout=1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("owned inference server health deadline exceeded")
            yield url
        finally:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


def telemetry(url: str) -> dict[str, float]:
    with urllib.request.urlopen(
        url.rstrip("/") + "/ml/v1/metrics", timeout=30
    ) as response:
        lines = response.read(4 * 1024 * 1024).decode().splitlines()
    result = {}
    for line in lines:
        if not line.startswith("antfly_inference_"):
            continue
        name, value = line.rsplit(" ", 1)
        if any(
            term in name.split("{", 1)[0]
            for term in ("metal", "admission", "resident_projection")
        ):
            result[name] = float(value)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--model-dir", type=Path)
    parser.add_argument("--model", default="model")
    parser.add_argument(
        "--backend",
        default="native",
        help="Label for the server's required backend",
    )
    parser.add_argument("--video", type=Path)
    parser.add_argument("--cases", default="text,image,gif,audio,pdf,video")
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--stream", action="store_true")
    parser.add_argument(
        "--telemetry",
        action="store_true",
        help="Capture server admission, transfer and Metal memory counters around each completed chunk request",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    assert args.repeat > 0
    with owned_server(args) as url:
        args.url = url
        qualify(args)


def qualify(args):
    cases = fixtures(args.video)
    query = urllib.parse.urlencode({"model": args.model, "task": "embed"})
    with urllib.request.urlopen(
        args.url.rstrip("/") + "/ai/v1/models?" + query, timeout=120
    ) as response:
        info = json.load(response)["embedders"][args.model]
    revision = info["model_revision"]
    assert (
        len(revision) == 64
        and info["inference_capabilities"]["model_aware_chunking_v1"]
    )
    report = {"backend": args.backend, "model": args.model, "runs": []}
    for iteration in range(args.repeat):
        for name in args.cases.split(","):
            source, policy = cases[name]
            before = telemetry(args.url) if args.telemetry else {}
            started = time.perf_counter()
            body = {
                "input": source,
                "embedding": {"model": args.model, "dimensions": 128},
                "policy": policy,
                "include_content": True,
                "stream": args.stream,
            }
            result = complete_request(args.url, body, chunk_response, lambda _: None)
            elapsed = time.perf_counter() - started
            after = telemetry(args.url) if args.telemetry else {}
            assert result["model_revision"] == revision, (
                "scoped discovery and execution used different model generations"
            )
            assert result["status"] == "complete" and not result["gaps"], result
            assert result["data"] and result["covered_through_ordinal"] == len(
                result["data"]
            )
            planning_body = dict(
                body,
                embedding={**body["embedding"], "execute": False},
                include_content=False,
            )
            planned = chunk_response(args.url, planning_body)
            assert (
                planned["status"] == "complete"
                and planned["plan_id"] == result["plan_id"]
            )
            assert len(planned["data"]) == len(result["data"])
            for dry, executed in zip(planned["data"], result["data"]):
                assert dry["descriptor"] == executed["descriptor"], (
                    name,
                    "planning differs from execution",
                    dry["descriptor"],
                    executed["descriptor"],
                )
                assert not dry.get("embedding"), "planning ran embedding inference"

            scores = []
            reference_started = time.perf_counter()
            for item in result["data"]:
                assert item.get("error") is None, item
                coordinates = item["descriptor"]["coordinates"]
                if name in ("image", "gif") or (
                    name == "pdf" and coordinates.get("region") is not None
                ):
                    width, height = (
                        coordinates["raster_width"],
                        coordinates["raster_height"],
                    )
                    assert width > 0 and height > 0, coordinates
                    if coordinates.get("region") is not None:
                        left, top, right, bottom = coordinates["region"]
                        assert (
                            0 <= left < right <= width and 0 <= top < bottom <= height
                        ), coordinates
                if name == "video":
                    part = dict(source)
                    part["sampling"] = {
                        "policy": "timestamps",
                        "start_ns": coordinates["start_ns"],
                        "end_ns": coordinates["end_ns"],
                        "step_ns": policy["step_ns"],
                        "max_frames": 32,
                        "overflow": "truncate",
                        "hold": True,
                    }
                elif item.get("text") is not None:
                    part = {"type": "text", "text": item["text"]}
                else:
                    part = {
                        "type": "media",
                        "mime_type": item["mime_type"],
                        "data": item["data"],
                    }
                with post(
                    args.url,
                    "embed",
                    {
                        "model": args.model,
                        "dimensions": 128,
                        "model_identity": result["model_identity"],
                        "task_type": "RETRIEVAL_DOCUMENT",
                        "input": [{"content": [part]}],
                    },
                ) as response:
                    reference = json.load(response)["data"][0]["embedding"]
                actual = item["embedding"]
                assert len(actual) == len(reference) == 128
                cosine = sum(a * b for a, b in zip(actual, reference)) / math.sqrt(
                    sum(a * a for a in actual) * sum(b * b for b in reference)
                )
                assert cosine >= 0.9995, (name, item["descriptor"], cosine)
                scores.append(cosine)
            run = {
                "case": name,
                "iteration": iteration,
                "chunks": len(scores),
                "completed_seconds": elapsed,
                "chunks_per_second": len(scores) / elapsed,
                "independent_seconds": time.perf_counter() - reference_started,
                "minimum_cosine": min(scores),
                "plan_id": result["plan_id"],
                "model_identity": result["model_identity"],
            }
            if args.telemetry:
                run["telemetry_after"] = after
                run["telemetry_delta"] = {
                    name: value - before.get(name, 0)
                    for name, value in after.items()
                    if name.split("{", 1)[0].endswith("_total")
                }
            report["runs"].append(run)
            print(json.dumps(run), flush=True)
    if args.output:
        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
