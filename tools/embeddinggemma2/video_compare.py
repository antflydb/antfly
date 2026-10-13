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

"""Matched real-video retrieval and completed CPU/MPS/native/Metal measurements.

The corpus supplies clips and queries, never relevance judgments. This evaluates
preservation of reference vectors and retrieval scores, not model usefulness.
"""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import contextlib
from bisect import bisect_left
from fractions import Fraction
import hashlib
import http.client
import json
import math
import os
import platform
from pathlib import Path
import resource
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import urllib.parse

if __package__:
    from . import compare, reference
else:
    import compare
    import reference


def failure_details(error):
    detail = {"type": type(error).__name__, "message": str(error)}
    if isinstance(error, urllib.error.HTTPError):
        try:
            body = error.read(65537)
        finally:
            error.close()
        detail.update(
            http_status=error.code,
            response_body=body[:65536].decode("utf-8", errors="replace"),
            response_body_truncated=len(body) > 65536,
        )
    return detail


def conditions():
    result = {
        "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "threads": {
            k: os.environ.get(k) for k in ("VECLIB_MAXIMUM_THREADS", "OMP_NUM_THREADS")
        },
    }
    if sys.platform == "darwin":
        for name, command in (
            ("hardware", ["sysctl", "hw.model", "hw.memsize", "hw.ncpu"]),
            ("swap", ["sysctl", "vm.swapusage"]),
            ("vm_stat", ["vm_stat"]),
        ):
            p = subprocess.run(command, capture_output=True, text=True, timeout=5)
            result[name] = {"exit_code": p.returncode, "output": p.stdout}
    elif sys.platform.startswith("linux"):
        for name in ("meminfo", "vmstat"):
            result[name] = Path("/proc", name).read_text()
    return result


def corpus(path):
    data = json.loads(path.read_text())
    names = []
    for kind in ("documents", "queries"):
        if not data.get(kind):
            raise ValueError(f"empty {kind}")
        for row in data[kind]:
            names.append(row["id"])
            if not isinstance(row["id"], str) or not row["id"]:
                raise ValueError("nonempty case IDs required")
    if len(names) != len(set(names)):
        raise ValueError("duplicate case IDs")
    for row in data["documents"]:
        source = Path(row["path"])
        if not source.is_absolute():
            source = path.parent / source
        source = source.resolve()
        if source.stat().st_size > 64 * 1024 * 1024:
            raise ValueError("clip exceeds model byte limit")
        if reference.sha256(source) != row["sha256"]:
            raise ValueError("clip changed")
        if not row.get("provenance"):
            raise ValueError("clip provenance required")
        row["path"] = str(source)
    if any(not isinstance(q["text"], str) or not q["text"] for q in data["queries"]):
        raise ValueError("nonempty text queries required")
    return data


def retrieval(data, vectors):
    return compare.retrieval_result(
        {
            "retrieval": {
                "queries": [q["id"] for q in data["queries"]],
                "documents": [d["id"] for d in data["documents"]],
                "purpose": "reference preservation; no relevance labels",
            }
        },
        {name: {"vector": value} for name, value in vectors.items()},
    )


def rank_agreement(actual, expected, top_k, tolerance):
    """Exact rankings are reported; inversions are gated by reference score gaps.

    A swap within the predeclared score tolerance is a numerical tie, not an
    application-quality failure. Report exact overlap as well as this tie gate.
    """
    if set(actual["scores"]) != set(expected["scores"]):
        raise ValueError("query sets differ")
    rows = {}
    for query, scores in expected["scores"].items():
        measured = actual["scores"][query]
        if set(scores) != set(measured):
            raise ValueError("document sets differ")
        if any(not math.isfinite(v) for v in [*scores.values(), *measured.values()]):
            raise ValueError("nonfinite retrieval score")
        ranking = actual["rankings"][query]
        if len(ranking) != len(scores) or set(ranking) != set(scores):
            raise ValueError("invalid ranking permutation")
        inversions = []
        for i, first in enumerate(ranking):
            for second in ranking[i + 1 :]:
                gap = scores[second] - scores[first]
                if gap > tolerance:
                    inversions.append({"first": first, "second": second, "gap": gap})
        rows[query] = {
            "max_score_error": max(abs(measured[d] - s) for d, s in scores.items()),
            "exact_ranking": ranking == expected["rankings"][query],
            "top_k_overlap": {
                str(k): len(
                    set(ranking[: min(k, len(scores))])
                    & set(expected["rankings"][query][: min(k, len(scores))])
                )
                / min(k, len(scores))
                for k in top_k
            },
            "non_tied_inversions": inversions,
        }
    return {
        "queries": rows,
        "pass": all(
            r["max_score_error"] <= tolerance and not r["non_tied_inversions"]
            for r in rows.values()
        ),
    }


def timestamp_selection(frames, time_base, sampling):
    """Independent exact-time oracle over FFprobe presentation pictures.

    Sort once and binary-search target timestamps; native code scans bounded
    metadata. No indexes or timing values are obtained from native inference.
    """
    allowed = {"policy", "start_ns", "end_ns", "step_ns", "max_frames", "overflow"}
    if not isinstance(sampling, dict) or set(sampling) - allowed:
        raise ValueError("invalid timestamp sampling object")
    if sampling.get("policy") != "timestamps":
        raise ValueError("expected timestamp policy")
    start = sampling.get("start_ns", 0)
    end = sampling.get("end_ns")
    step = sampling.get("step_ns", 1_000_000_000)
    cap = sampling.get("max_frames", 32)
    overflow = sampling.get("overflow", "uniform")
    if (
        any(type(value) is not int for value in (start, step, cap))
        or (end is not None and type(end) is not int)
        or any(value > 2**63 - 1 for value in (start, step, cap))
        or (end is not None and end > 2**63 - 1)
        or start < 0
        or step <= 0
        or not 1 <= cap <= 32
        or (end is not None and end <= start)
        or overflow not in ("uniform", "truncate")
    ):
        raise ValueError("invalid timestamp sampling bounds")
    scale = Fraction(time_base) * 1_000_000_000
    if scale <= 0 or not frames:
        raise ValueError("empty or invalid presentation timeline")
    ordered = sorted(
        (int(frame["best_effort_timestamp"]), index)
        for index, frame in enumerate(frames)
    )
    origin, latest = ordered[0][0], ordered[-1][0]
    times = [(pts - origin) * scale for pts, _ in ordered]
    if end is None:
        ends = []
        previous = next((pts for pts, _ in reversed(ordered) if pts < latest), None)
        for pts, index in ordered:
            duration = frames[index].get("duration", frames[index].get("pkt_duration"))
            if duration in (None, "N/A"):
                duration = (
                    latest - previous if pts == latest and previous is not None else 0
                )
            ends.append((pts - origin + int(duration)) * scale)
        tail = max(ends)
        end = -(-tail.numerator // tail.denominator)
    if end <= start:
        raise ValueError("empty timestamp window")
    candidates = (end - start + step - 1) // step
    count = min(candidates, cap)
    selected = []
    for i in range(count):
        ordinal = (
            i * (candidates - 1) // (count - 1)
            if overflow == "uniform" and candidates > count > 1
            else i
        )
        position = bisect_left(times, start + ordinal * step)
        if position < len(times) and times[position] < end:
            index = ordered[position][1]
            if not selected or selected[-1] != index:
                selected.append(index)
    if not selected:
        raise ValueError("empty timestamp window")
    return selected


def decoded(path, processor, sampling=None):
    """Stream independent RGB decoding and retain only upstream-selected frames.

    Default selection uses pinned HF; explicit windows use independent timestamps.
    FFmpeg/Pillow are qualification tools, never native runtime dependencies.
    """
    import numpy as np
    from PIL import Image
    import tempfile
    from transformers.video_utils import VideoMetadata

    command = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-count_frames",
        "-show_streams",
        "-of",
        "json",
        str(path),
    ]
    info = json.loads(subprocess.check_output(command, timeout=120))["streams"][0]
    # Container nb_frames counts transport samples, including individual PAFF
    # fields. The reference processor requires completed presentation pictures.
    count = int(info["nb_read_frames"])
    duration_value = info.get("duration")
    if duration_value in (None, "N/A"):
        # Matroska commonly declares duration on the Segment, not the track.
        container = json.loads(
            subprocess.check_output(
                ["ffprobe", "-v", "error", "-show_format", "-of", "json", str(path)],
                timeout=120,
            )
        )
        duration_value = container["format"]["duration"]
    duration = float(duration_value)
    width, height = int(info["width"]), int(info["height"])
    if not (
        0 < count <= 20000
        and 0 < width * height <= 16 * 1024 * 1024
        and math.isfinite(duration)
        and duration > 0
    ):
        raise ValueError("reference decode metadata exceeds bounds")
    if any(side.get("rotation", 0) for side in info.get("side_data_list", [])):
        raise ValueError("reference display orientation requires an explicit policy")
    metadata = VideoMetadata(
        total_num_frames=count,
        fps=count / duration,
        duration=duration,
        width=width,
        height=height,
    )
    timestamp_evidence = None
    if sampling is None or sampling == {"policy": "reference"}:
        selected = processor.sample_frames(
            metadata,
            fps=processor.fps,
            max_frames=processor.max_frames,
            overflow_strategy=processor.overflow_strategy,
        ).tolist()
    else:
        frames = json.loads(
            subprocess.check_output(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_frames",
                    "-show_entries",
                    "frame=best_effort_timestamp,duration,pkt_duration",
                    "-of",
                    "json",
                    str(path),
                ],
                timeout=120,
            )
        )["frames"]
        count = len(frames)
        metadata.total_num_frames = count
        metadata.fps = count / duration
        selected = timestamp_selection(frames, info["time_base"], sampling)
        timestamp_evidence = {
            "sampling": sampling,
            "time_base": info["time_base"],
            "presentation_frames": frames,
        }
    if not selected or len(selected) * width * height * 3 > 128 * 1024 * 1024:
        raise ValueError("reference selected RGB exceeds bounds")
    pictures = {}
    wanted = set(selected)
    if info["codec_name"] == "mjpeg":
        with tempfile.TemporaryDirectory() as temporary:
            subprocess.run(
                [
                    "ffmpeg",
                    "-v",
                    "error",
                    "-i",
                    str(path),
                    "-map",
                    "0:v:0",
                    "-c:v",
                    "copy",
                    str(Path(temporary) / "%d.jpg"),
                ],
                check=True,
                timeout=120,
            )
            for index in wanted:
                with Image.open(Path(temporary) / f"{index + 1}.jpg") as image:
                    pixels = np.array(image.convert("RGB"))
                if pixels.shape != (height, width, 3):
                    raise ValueError("reference JPEG geometry changed")
                pictures[index] = pixels
        policy = "FFmpeg packet demux + independent Pillow/libjpeg RGB; upstream-selected frames only"
    else:
        frame_bytes = width * height * 3
        with tempfile.TemporaryFile() as log:
            process = subprocess.Popen(
                [
                    "ffmpeg",
                    "-v",
                    "error",
                    "-i",
                    str(path),
                    "-map",
                    "0:v:0",
                    "-fps_mode",
                    "passthrough",
                    "-f",
                    "rawvideo",
                    "-pix_fmt",
                    "rgb24",
                    "-",
                ],
                stdout=subprocess.PIPE,
                stderr=log,
            )
            decode_deadline = threading.Timer(120, process.kill)
            decode_deadline.start()
            try:
                for index in range(count):
                    raw = process.stdout.read(frame_bytes)
                    if len(raw) != frame_bytes:
                        raise ValueError("reference decoded frame count differs")
                    if index in wanted:
                        pictures[index] = (
                            np.frombuffer(raw, dtype=np.uint8)
                            .reshape(height, width, 3)
                            .copy()
                        )
                if process.stdout.read(1):
                    raise ValueError("reference decoded extra frames")
                if process.wait(timeout=120) != 0:
                    raise ValueError("reference FFmpeg decode failed")
            finally:
                decode_deadline.cancel()
                process.stdout.close()
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
        policy = "FFmpeg default RGB, streamed; upstream-selected frames only"
    pixels = np.stack([pictures[index] for index in selected])
    return (
        pixels,
        metadata,
        dict(
            policy=policy,
            rgb_sha256=hashlib.sha256(pixels.tobytes()).hexdigest(),
            frames=count,
            selected_frames=selected,
            duration=duration,
            retained_rgb_bytes=pixels.nbytes,
            timestamp_evidence=timestamp_evidence,
        ),
    )


def process_tree_rss(root, records):
    """Include supervised model workers; exclude unrelated processes."""
    if root not in records:
        raise ValueError("owned root is absent from memory sample")
    children = {}
    for pid, (parent, _) in records.items():
        children.setdefault(parent, []).append(pid)
    pending, seen, total = [root], set(), 0
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        total += records[pid][1]
        pending.extend(children.get(pid, []))
    return total


class ProcessMemory:
    """Sample the owned server's RSS; GPU private memory needs backend telemetry."""

    def __init__(self, pid):
        self.pid = pid
        self.samples = []
        self.errors = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def sample(self):
        while not self.stop.is_set():
            try:
                records = {}
                if sys.platform.startswith("linux"):
                    for entry in Path("/proc").iterdir():
                        if not entry.name.isdigit():
                            continue
                        try:
                            status = dict(
                                line.split(":", 1)
                                for line in (entry / "status").read_text().splitlines()
                            )
                            records[int(entry.name)] = (
                                int(status["PPid"]),
                                int(status.get("VmRSS", "0 kB").split()[0]) * 1024,
                            )
                        except (OSError, ValueError, KeyError):
                            continue  # A process can exit between directory/read.
                else:
                    output = subprocess.check_output(
                        ["ps", "-axo", "pid=,ppid=,rss="], timeout=2
                    )
                    for line in output.splitlines():
                        pid, parent, kib = map(int, line.split())
                        records[pid] = (parent, kib * 1024)
                rss = process_tree_rss(self.pid, records)
                self.samples.append((time.perf_counter(), rss))
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                if not self.errors:
                    self.errors.append(type(error).__name__)
            self.stop.wait(0.1)

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=3)
        return dict(
            scope="sum of owned server and descendant RSS, 100ms samples; shared mappings may be counted more than once; excludes unmapped GPU private memory",
            samples=len(self.samples),
            peak_bytes=max((rss for _, rss in self.samples), default=None),
            first_bytes=self.samples[0][1] if self.samples else None,
            last_bytes=self.samples[-1][1] if self.samples else None,
            errors=self.errors,
        )


def disconnect_and_retry(url, body, expected):
    endpoint = urllib.parse.urlsplit(url)
    connection = http.client.HTTPConnection(
        endpoint.hostname, endpoint.port, timeout=10
    )
    try:
        connection.request(
            "POST",
            "/ai/v1/embed",
            json.dumps(body),
            {"Content-Type": "application/json"},
        )
        # Close during execution on the warmed model, rather than during parsing.
        time.sleep(0.05)
        connection.sock.shutdown(socket.SHUT_RDWR)
    finally:
        connection.close()
    start = time.perf_counter()
    retry = compare.post(url + "/ai/v1", "/embed", body)
    compare.assert_agreement(retry["data"][0]["embedding"], expected)
    return dict(
        peer_disconnect_requested=True,
        retry="pass",
        retry_seconds=time.perf_counter() - start,
        cancellation_completion="not observable through this HTTP response; do not infer from retry",
    )


@contextlib.contextmanager
def server(args):
    if args.url:
        yield args.url.rstrip("/")
        return
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", 0))
        port = endpoint.getsockname()[1]
    with args.output.with_suffix(".server.log").open("w") as log:
        process = subprocess.Popen(
            [
                str(args.binary.resolve()),
                "run",
                "--models-dir",
                str(args.model_dir.parent.resolve()),
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
            env=dict(
                os.environ,
                ANTFLY_INFERENCE_REQUIRED_BACKEND=args.backend,
                **(
                    {"ANTFLY_INFERENCE_TEST_FORCE_RUN_ADMISSION_DENIALS": "1"}
                    if getattr(args, "verify_capacity_retry", False)
                    else {}
                ),
            ),
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
        url = f"http://127.0.0.1:{port}"
        memory = ProcessMemory(process.pid)
        memory.thread.start()
        try:
            for _ in range(300):
                if process.poll() is not None:
                    raise RuntimeError("owned server exited before health")
                try:
                    with urllib.request.urlopen(url + "/healthz", timeout=1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("owned server health deadline exceeded")
            yield url
        finally:
            args.server_memory = memory.finish()
            os.killpg(process.pid, signal.SIGTERM) if process.poll() is None else None
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


def native(args, data, result):
    reference.verify_checkpoint(args.model_dir)
    with server(args) as url:
        bodies = {}
        identity = None
        for row in data["queries"] + data["documents"]:
            if "text" in row:
                body = {"input": [row["text"]], "task_type": "RETRIEVAL_QUERY"}
            else:
                body = {
                    "input": [
                        {
                            "content": [
                                {
                                    "type": "media",
                                    "mime_type": "video/quicktime"
                                    if row["path"].endswith(".mov")
                                    else "video/webm"
                                    if row["path"].endswith(".webm")
                                    else "video/mp2t"
                                    if row["path"].endswith(".ts")
                                    else "video/mp4",
                                    "data": base64.b64encode(
                                        Path(row["path"]).read_bytes()
                                    ).decode(),
                                }
                            ]
                        }
                    ]
                }
            if "sampling" in row:
                body["input"][0]["content"][0]["sampling"] = row["sampling"]
            body["model"] = args.model_dir.name
            bodies[row["id"]] = body
            samples = []
            cold = None
            for i in range(args.warmups + args.iterations):
                if identity:
                    body["model_identity"] = identity
                start = time.perf_counter()
                response = compare.post(url + "/ai/v1", "/embed", body)
                elapsed = time.perf_counter() - start
                if identity and identity != response["model_identity"]:
                    raise ValueError("model identity changed")
                identity = response["model_identity"]
                vector = response["data"][0]["embedding"]
                compare.assert_agreement(vector, vector)
                cold = elapsed if cold is None else cold
                if i >= args.warmups:
                    samples.append(elapsed)
            result["cases"][row["id"]] = {
                "vector": vector,
                "prompt_tokens": response["usage"]["prompt_tokens"],
                "cold_seconds": cold,
                "completed_timing": compare.timing(samples),
            }
            compare.publish(args.output, result)
            print(args.backend, row["id"], samples, flush=True)
        result["model_identity"] = identity
        first = data["documents"][0]["id"]
        result["disconnect_retry"] = disconnect_and_retry(
            url, bodies[first], result["cases"][first]["vector"]
        )
        jobs = [row["id"] for row in data["documents"]] * args.sustained_rounds

        def request(name):
            start = time.perf_counter()
            try:
                response = compare.post(url + "/ai/v1", "/embed", bodies[name])
                if response["model_identity"] != identity:
                    raise ValueError("model identity changed under concurrent load")
                compare.assert_agreement(
                    response["data"][0]["embedding"], result["cases"][name]["vector"]
                )
                return {
                    "case": name,
                    "status": 200,
                    "seconds": time.perf_counter() - start,
                }
            except urllib.error.HTTPError as error:
                if error.code not in (429, 503):
                    raise
                return {
                    "case": name,
                    "status": error.code,
                    "seconds": time.perf_counter() - start,
                }

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            completed = list(pool.map(request, jobs))
        elapsed = time.perf_counter() - start
        result["sustained"] = {
            "concurrency": args.concurrency,
            "requests": completed,
            "wall_seconds": elapsed,
            "completed_per_second": sum(r["status"] == 200 for r in completed)
            / elapsed,
        }


def pytorch(args, data, result):
    import torch
    from transformers import AutoModel, AutoProcessor, AutoTokenizer

    reference.verify_reference(args.model_dir)
    result["reference_provenance"] = {
        "transformers_revision": reference.TRANSFORMERS_REVISION,
        "source_hashes": reference.SOURCE_HASHES,
        "ffmpeg": subprocess.check_output(
            ["ffmpeg", "-version"], text=True
        ).splitlines()[0],
    }
    torch.set_num_threads(args.threads)
    if args.backend == "mps" and not torch.backends.mps.is_available():
        raise RuntimeError("MPS unavailable")
    sync = torch.mps.synchronize if args.backend == "mps" else lambda: None
    start = time.perf_counter()
    model = (
        AutoModel.from_pretrained(
            args.model_dir, dtype=torch.float32, local_files_only=True
        )
        .eval()
        .to(args.backend)
    )
    sync()
    result["model_load_seconds"] = time.perf_counter() - start
    processor = AutoProcessor.from_pretrained(args.model_dir, local_files_only=True)
    tokenizer = AutoTokenizer.from_pretrained(args.model_dir, local_files_only=True)
    with torch.inference_mode():
        for row in data["queries"] + data["documents"]:
            samples = []
            encoder_samples = []
            cold = None
            for i in range(args.warmups + args.iterations):
                sync()
                start = time.perf_counter()
                if "text" in row:
                    inputs = tokenizer(
                        "task: search result | query: " + row["text"],
                        return_tensors="pt",
                    )
                else:
                    pixels, metadata, policy = decoded(
                        row["path"], processor.video_processor, row.get("sampling")
                    )
                    inputs = processor(
                        text=["<|video|>"],
                        videos=[pixels],
                        video_metadata=[metadata],
                        do_sample_frames=False,
                        return_tensors="pt",
                    )
                    result.setdefault("decoding", {})[row["id"]] = policy
                inputs = {k: v.to(args.backend) for k, v in inputs.items()}
                sync()
                encoder_start = time.perf_counter()
                hidden = model(**inputs).last_hidden_state
                mask = inputs["attention_mask"].unsqueeze(-1)
                vector = (
                    torch.nn.functional.normalize(
                        (hidden * mask).sum(1) / mask.sum(1), dim=-1
                    )[0]
                    .cpu()
                    .tolist()
                )
                sync()
                encoder_elapsed = time.perf_counter() - encoder_start
                elapsed = time.perf_counter() - start
                cold = elapsed if cold is None else cold
                if i >= args.warmups:
                    samples.append(elapsed)
                    encoder_samples.append(encoder_elapsed)
            result["cases"][row["id"]] = {
                "vector": vector,
                # Text endpoint usage counts lexical tokens; video groups count
                # the expanded encoder sequence, including modality spans.
                "prompt_tokens": len(
                    tokenizer(
                        "task: search result | query: " + row["text"],
                        add_special_tokens=False,
                    )["input_ids"]
                )
                if "text" in row
                else int(inputs["attention_mask"].sum().item()),
                "encoder_tokens": int(inputs["attention_mask"].sum().item()),
                "cold_seconds": cold,
                "completed_timing": compare.timing(samples),
                "encoder_and_readback_timing": compare.timing(encoder_samples),
            }
            compare.publish(args.output, result)
            print(args.backend, row["id"], samples, flush=True)
    result["peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (
        1 if sys.platform == "darwin" else 1024
    )
    if args.backend == "mps":
        result["mps_live_bytes"] = torch.mps.current_allocated_memory()
        result["mps_driver_bytes"] = torch.mps.driver_allocated_memory()
    result["torch"] = torch.__version__


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend", choices=("native", "metal", "cpu", "mps"), required=True
    )
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--url")
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--sustained-rounds", type=int, default=2)
    parser.add_argument("--score-tolerance", type=float, default=1e-4)
    parser.add_argument("--top-k", type=int, nargs="+", default=[1, 5, 10])
    args = parser.parse_args()
    if not (
        0 <= args.warmups <= 100
        and 1 <= args.iterations <= 100
        and 1 <= args.concurrency <= 32
        and 1 <= args.sustained_rounds <= 100
        and 1 <= args.threads <= 64
        and 0 < args.score_tolerance <= 0.01
        and all(k > 0 for k in args.top_k)
    ):
        parser.error("invalid bounded measurement controls")
    if args.backend in ("native", "metal") and not (args.binary or args.url):
        parser.error("native/metal requires --binary or --url")
    data = corpus(args.corpus)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "version": 1,
        "status": "running",
        "backend": args.backend,
        "precision": "float32",
        "purpose": "reference preservation, not end-user relevance",
        "corpus_sha256": reference.sha256(args.corpus),
        "checkpoint_receipt_sha256": reference.sha256(
            args.model_dir / "embeddinggemma2_receipt.json"
        ),
        "tool_sha256": reference.sha256(Path(__file__)),
        "cases": {},
        "controls": {
            k: getattr(args, k)
            for k in (
                "warmups",
                "iterations",
                "threads",
                "concurrency",
                "sustained_rounds",
                "score_tolerance",
                "top_k",
            )
        },
        "cold_scope": "new process/model; artifact file cache is warmed by receipt verification; later case first calls use the resident model",
        "timing_scope": "HTTP including decode/preparation/inference/final response"
        if args.backend in ("native", "metal")
        else "decode/preparation/inference/synchronized final readback; excludes HTTP",
        "conditions_before": conditions(),
    }
    if args.binary:
        result["binary_sha256"] = reference.sha256(args.binary)
    try:
        (native if args.backend in ("native", "metal") else pytorch)(args, data, result)
        result["retrieval"] = retrieval(
            data, {n: r["vector"] for n, r in result["cases"].items()}
        )
        if args.reference:
            expected = json.loads(args.reference.read_text())
            if (
                any(
                    result[k] != expected[k]
                    for k in ("corpus_sha256", "checkpoint_receipt_sha256", "precision")
                )
                or expected["status"] != "pass"
            ):
                raise ValueError("incomplete or mismatched reference")
            if set(result["cases"]) != set(expected["cases"]):
                raise ValueError("reference cases differ")
            result["vector_agreement"] = {
                n: compare.agreement(c["vector"], expected["cases"][n]["vector"])
                for n, c in result["cases"].items()
            }
            result["reference_sha256"] = reference.sha256(args.reference)
            result["retrieval_agreement"] = rank_agreement(
                result["retrieval"],
                expected["retrieval"],
                args.top_k,
                args.score_tolerance,
            )
            for name, case in result["cases"].items():
                reference_case = expected["cases"][name]
                if (
                    "prompt_tokens" in reference_case
                    and case["prompt_tokens"] != reference_case["prompt_tokens"]
                ):
                    raise ValueError(f"prompt-token parity failed: {name}")
                compare.assert_agreement(
                    case["vector"], expected["cases"][name]["vector"]
                )
            if not result["retrieval_agreement"]["pass"]:
                raise ValueError("retrieval parity gate failed; measurements retained")
        result["status"] = "pass"
    except Exception as error:
        result["failure"] = failure_details(error)
        raise
    finally:
        if result["status"] != "pass":
            result["status"] = "failed"
        if hasattr(args, "server_memory"):
            result["server_memory"] = args.server_memory
        result["conditions_after"] = conditions()
        compare.publish(args.output, result)


if __name__ == "__main__":
    main()
