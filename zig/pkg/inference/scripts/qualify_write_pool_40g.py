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

"""Check a pinned CUDA write pool against independent text/audio/OCR fixtures."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import io
import json
import math
from pathlib import Path
import re
import threading
import time
import wave

import qualify_read_pool_40g as q

WHISPER = "openai/whisper-small"
GLINER = "antflydb/GLiNER2.5"
OCR = "lightonai/LightOnOCR-3-0.8B"
ROOT = Path(__file__).resolve().parents[4]


def data_uri(path, mime):
    return f"data:{mime};base64," + base64.b64encode(path.read_bytes()).decode()


def words(text):
    return re.findall(r"[\w']+", text.casefold())


def wer(reference, actual):
    reference, actual = words(reference), words(actual)
    previous = list(range(len(actual) + 1))
    for i, token in enumerate(reference, 1):
        current = [i]
        for j, value in enumerate(actual, 1):
            current.append(
                min(
                    current[-1] + 1, previous[j] + 1, previous[j - 1] + (token != value)
                )
            )
        previous = current
    return previous[-1] / max(1, len(reference))


class WriteCampaign(q.Campaign):
    task_kinds = ("embed", "extract", "whisper", "ocr")

    def __init__(self, args):
        super().__init__(args)
        self.base = json.loads(
            (
                ROOT / "zig/pkg/inference/testdata/gliner25/pipeline_cases_base.json"
            ).read_text()
        )["cases"][1]
        self.ocr_root = ROOT / "zig/pkg/inference/scripts/lightonocr3/corpus"
        self.ocr_cases = json.loads((self.ocr_root / "cases.json").read_text())["cases"]
        self.audio = data_uri(
            ROOT / "zig/e2e/inference/testdata/whisper_quality.wav", "audio/wav"
        )
        self.audio_reference = (
            ROOT / "zig/pkg/inference/scripts/whisper/english-reference.txt"
        ).read_text()

    def extraction(self, count=1, padded=False):
        prefix = "inventory checked. " * 80 if padded else ""
        return {
            "schema_version": 2,
            "model": GLINER,
            "schema": self.base["schema"],
            "inputs": [
                {"id": f"item-{i}", "content": prefix + self.base["text"]}
                for i in range(count)
            ],
            "options": {
                "include_confidence": True,
                "include_spans": True,
                "offset_unit": "unicode_codepoints",
            },
        }

    def ocr(self, indices):
        return {
            "model": OCR,
            "images": [
                {
                    "url": data_uri(
                        (self.ocr_root / self.ocr_cases[i]["image"]).resolve(),
                        "image/png",
                    )
                }
                for i in indices
            ],
            "max_tokens": max(self.ocr_cases[i]["max_new_tokens"] for i in indices),
        }

    def whisper(self):
        return {
            "model": WHISPER,
            "audio": self.audio,
            "audio_options": {"language": "en", "timestamps": True},
        }

    def windowed_whisper(self, repeated=False):
        with wave.open(
            str(ROOT / "zig/e2e/inference/testdata/whisper_quality.wav"), "rb"
        ) as source:
            params = source.getparams()
            frames = source.readframes(source.getnframes())
        if repeated:
            payload, copies = frames * 24, 24
        else:
            payload, copies = (
                bytearray(params.framerate * params.nchannels * params.sampwidth * 68),
                3,
            )
            for second in (2, 32, 62):
                start = second * params.framerate * params.nchannels * params.sampwidth
                payload[start : start + len(frames)] = frames
        encoded = io.BytesIO()
        with wave.open(encoded, "wb") as target:
            target.setparams(params)
            target.writeframes(payload)
        return {
            "model": WHISPER,
            "audio": "data:audio/wav;base64,"
            + base64.b64encode(encoded.getvalue()).decode(),
            "audio_options": {"language": "en", "timestamps": True},
        }, copies

    def request_write(self, kind, body, cases=None):
        path = {
            "embed": "/embed",
            "extract": "/extract",
            "whisper": "/read",
            "ocr": "/read",
        }[kind]
        phase = self.phase
        code, response, seconds = q.http(self.args.url, path, body, timeout=300)
        row = {
            "phase": phase,
            "kind": kind,
            "status": code,
            "seconds": seconds,
            "request": body,
            "response": response,
        }
        assert code == 200, row
        if kind == "embed":
            deltas, similarities = [], []
            for value in response["data"]:
                vector = value["embedding"]
                assert (
                    all(math.isfinite(x) for x in vector)
                    and abs(sum(x * x for x in vector) - 1) < 0.01
                )
                if self.embedding_reference is None:
                    self.embedding_reference = vector
                deltas.append(
                    max(
                        abs(a - b)
                        for a, b in zip(vector, self.embedding_reference, strict=True)
                    )
                )
                reference = self.embedding_reference
                similarities.append(
                    sum(a * b for a, b in zip(vector, reference, strict=True))
                    / math.sqrt(
                        sum(a * a for a in vector) * sum(b * b for b in reference)
                    )
                )
            row["max_embedding_delta"] = max(deltas)
            row["min_embedding_cosine"] = min(similarities)
            # BF16 batched GEMMs use a different accumulation geometry from
            # the single-item path. Gate every item on direction and absolute
            # drift, and keep the tighter single-item traffic check.
            assert row["min_embedding_cosine"] >= 0.9999 and row[
                "max_embedding_delta"
            ] < (0.005 if len(body["input"]) > 1 else 0.002), row
        elif kind == "extract":
            assert len(response["data"]) == len(body["inputs"])
            for item, source in zip(response["data"], body["inputs"], strict=True):
                shift = len(source["content"]) - len(self.base["text"])
                expected = [
                    (
                        group["name"],
                        value["text"],
                        value["source"]["start"] + shift,
                        value["source"]["end"] + shift,
                        value["confidence"],
                    )
                    for group in self.base["expected"]["entities"]
                    for value in group["values"]
                ]
                actual = [
                    (
                        value["label"],
                        value["text"],
                        value["start"],
                        value["end"],
                        value["score"],
                    )
                    for value in item["entities"]
                ]
                assert [a[:4] for a in actual] == [e[:4] for e in expected], (
                    actual,
                    expected,
                )
                assert all(math.isfinite(a[4]) and 0 <= a[4] <= 1 for a in actual)
                if shift == 0:
                    assert all(
                        abs(a[4] - e[4]) <= 5e-4
                        for a, e in zip(actual, expected, strict=True)
                    ), (actual, expected)
        elif kind == "whisper":
            text = " ".join(item["text"] for item in response["data"])
            row["wer"] = wer(self.audio_reference * (cases or 1), text)
            assert row["wer"] <= 0.10, row
            for item in response["data"]:
                assert item.get("status", "complete") == "complete", item
                last = 0
                for segment in (item.get("audio") or {}).get("segments", []):
                    assert (
                        segment["start_ms"] >= last
                        and segment["end_ms"] >= segment["start_ms"]
                    ), segment
                    last = segment["start_ms"]
        else:
            assert len(response["data"]) == len(cases), response
            for item, index in zip(response["data"], cases, strict=True):
                text = re.sub(r"\s+", " ", item["text"]).casefold()
                assert item.get("status", "complete") == "complete", item
                for needle in self.ocr_cases[index]["required"]:
                    assert needle.casefold() in text, (
                        self.ocr_cases[index]["name"],
                        needle,
                        item,
                    )
        with self.lock:
            self.rows.append(row)
            with (self.args.output / "requests.jsonl").open("a") as out:
                out.write(json.dumps(row) + "\n")
        return row

    def execute(self):
        thread = threading.Thread(target=self.monitor, daemon=True)
        thread.start()

        def idle_reservations():
            until = time.monotonic() + 15
            previous = None
            while True:
                _, raw, _ = q.http(self.args.url, "/metrics", text=True)
                current = q.metrics(raw)
                snapshot = {
                    name: current[name]
                    for name in (
                        "antfly_inference_admission_host_scratch_bytes",
                        "antfly_inference_admission_backend_scratch_bytes",
                        "antfly_inference_admission_backend_kv_bytes",
                        "antfly_admission_inference_in_flight_requests",
                    )
                }
                if (
                    snapshot == previous
                    and snapshot["antfly_inference_admission_backend_kv_bytes"] == 0
                    and snapshot["antfly_admission_inference_in_flight_requests"] == 0
                ):
                    return snapshot
                assert time.monotonic() < until, snapshot
                previous = snapshot
                time.sleep(0.1)

        try:
            code, inventory, _ = q.http(self.args.url, "/models/instances")
            assert code == 200 and {i["model"] for i in inventory["data"]} == {
                q.EMBED,
                WHISPER,
                GLINER,
                OCR,
            }
            assert all(
                i["backend"] == "cuda" and i["startup_managed"]
                for i in inventory["data"]
            )
            self.instances_before = inventory["data"]
            _, raw, _ = q.http(self.args.url, "/metrics", text=True)
            assert q.metrics(raw)["antfly_inference_cuda_managed_limit_bytes"] == 38 * (
                1 << 30
            )
            jobs = [
                ("embed", self.embed(), None),
                ("extract", self.extraction(), None),
                ("whisper", self.whisper(), None),
                ("ocr", self.ocr([0]), [0]),
            ]
            self.set_phase("warmup")
            for job in jobs:
                self.request_write(*job)
            warmed_reservations = idle_reservations()
            self.set_phase("baseline")
            for _ in range(self.args.probes):
                for job in jobs:
                    self.request_write(*job)
            self.save()
            self.set_phase("mixed-steady")
            print(self.phase, flush=True)
            with ThreadPoolExecutor(max_workers=4) as pool:
                for round in range(self.args.rounds):
                    index = round % len(self.ocr_cases)
                    current = jobs[:3] + [("ocr", self.ocr([index]), [index])]
                    for future in [
                        pool.submit(self.request_write, *job) for job in current
                    ]:
                        future.result()
            self.save()
            self.set_phase("mixed-burst-and-batches")
            print(self.phase, flush=True)
            embedding = {**self.embed(), "input": self.embed()["input"] * 8}
            batch_jobs = [
                ("embed", embedding, None),
                ("extract", self.extraction(4, padded=True), None),
                ("whisper", self.whisper(), None),
                ("ocr", self.ocr([0, 1, 2, 3]), [0, 1, 2, 3]),
            ]
            with ThreadPoolExecutor(max_workers=8) as pool:
                for future in [
                    pool.submit(self.request_write, *batch_jobs[i % 4])
                    for i in range(self.args.burst)
                ]:
                    future.result()
            self.set_phase("limits-and-silence")
            request = {
                "model": OCR,
                "images": self.ocr([0])["images"] * 9,
                "max_tokens": 128,
            }
            code, response, seconds = q.http(self.args.url, "/read", request)
            assert code in (400, 413), (code, response)
            with (self.args.output / "negative-requests.jsonl").open("a") as out:
                out.write(
                    json.dumps(
                        {
                            "request": request,
                            "status": code,
                            "response": response,
                            "seconds": seconds,
                        }
                    )
                    + "\n"
                )
            silence = io.BytesIO()
            with wave.open(silence, "wb") as audio:
                audio.setnchannels(1)
                audio.setsampwidth(2)
                audio.setframerate(16000)
                audio.writeframes(bytes(32000))
            body = {
                "model": WHISPER,
                "audio": "data:audio/wav;base64,"
                + base64.b64encode(silence.getvalue()).decode(),
                "audio_options": {"language": "en"},
            }
            code, response, seconds = q.http(self.args.url, "/read", body)
            assert code == 200 and all(
                not item["text"].strip() for item in response["data"]
            ), response
            (self.args.output / "silence.json").write_text(
                json.dumps(
                    {"request": body, "response": response, "seconds": seconds},
                    indent=2,
                )
                + "\n"
            )
            self.set_phase("windowed-audio-and-eight-pages")
            body, copies = self.windowed_whisper()
            self.request_write("whisper", body, copies)
            self.request_write("ocr", self.ocr([0] * 8), [0] * 8)
            self.set_phase("ocr-deadline")
            body = self.ocr([0] * 8)
            code, response, seconds = q.http(
                self.args.url, "/read", body, timeout=30, deadline_ms=200
            )
            (self.args.output / "deadline.json").write_text(
                json.dumps(
                    {
                        "request": body,
                        "status": code,
                        "response": response,
                        "seconds": seconds,
                    },
                    indent=2,
                )
                + "\n"
            )
            assert code == 504 and response.get("retryable"), (code, response)
            if self.args.repeated_audio:
                self.set_phase("repeated-audio-accuracy")
                body, copies = self.windowed_whisper(repeated=True)
                self.request_write("whisper", body, copies)
            self.set_phase("recovery")
            for job in jobs:
                self.request_write(*job)
            code, inventory, _ = q.http(self.args.url, "/models/instances")
            self.instances_after = inventory["data"]
            assert code == 200 and {i["id"] for i in self.instances_before} == {
                i["id"] for i in self.instances_after
            }
            final_reservations = idle_reservations()
            (self.args.output / "idle-reservations.json").write_text(
                json.dumps(
                    {"warmed": warmed_reservations, "recovered": final_reservations},
                    indent=2,
                )
                + "\n"
            )
            # OCR retains an admitted temporary/deferred-free cache. Recovery
            # returns to its warmed reservation, rather than falsely requiring
            # every scratch-classified owner to have request lifetime.
            for name in (
                "antfly_inference_admission_host_scratch_bytes",
                "antfly_inference_admission_backend_scratch_bytes",
            ):
                assert final_reservations[name] <= warmed_reservations[name], (
                    warmed_reservations,
                    final_reservations,
                )
            self.require_memory_evidence()
            self.status = "passed"
        except BaseException as error:
            self.status, self.error = (
                (
                    "incomplete"
                    if isinstance(error, q.EvidenceUnavailable)
                    else "failed"
                ),
                str(error),
            )
            raise
        finally:
            self.stop_monitor(thread)
            path = self.args.output / "report.json"
            report = json.loads(path.read_text())
            report["validation_mode"] = (
                "write-pool co-residency, mixed traffic, batch activations and recovery"
            )
            report["scope"] = (
                "Spark memory-budget and functional experiment; synthetic fixtures are not a broad accuracy benchmark"
            )
            path.write_text(json.dumps(report, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18841")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cgroup", type=Path)
    parser.add_argument("--probes", type=int, default=5)
    parser.add_argument("--rounds", type=int, default=12)
    parser.add_argument("--burst", type=int, default=24)
    parser.add_argument(
        "--repeated-audio",
        action="store_true",
        help="Also gate accuracy on a stress clip repeating the sentence 24 times; currently fails and is kept separate from the sparse-window smoke fixture.",
    )
    args = parser.parse_args()
    args.pressure = False
    WriteCampaign(args).execute()


if __name__ == "__main__":
    main()
