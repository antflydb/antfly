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

"""Qualify bounded write serving, observed HTTP batching, and accuracy evidence.

Uses the pinned, locally cached LibriSpeech dummy dev-clean shard (73 clips,
one speaker), frozen GLiNER oracles and synthetic OCR pages. Passing this
envelope does not establish general ASR/OCR accuracy or A100 performance.
Requires numpy, av and pyarrow for the independent speech fixtures.
"""

import argparse
import base64
import copy
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
from pathlib import Path
import socket
import statistics
import struct
import sys
import threading
import time
from urllib.parse import urlsplit

import qualify_read_pool_40g as q
import qualify_write_pool_40g as w
from qualification_identity import verify_artifacts
from qualify_cooperative_execution import serving_identity
from qualify_read_pool_microbatch import embedding_equivalent, equivalent

ROOT = Path(__file__).resolve().parents[4]
CORPUS_SHA = "4e69a06fa5edc90921e5e7e39a7084881f8b3ed9c805c574f4f39c6fde27c603"
ACCEPTANCE = {
    "embedding_cosine_min": 0.9999,
    "embedding_max_abs": 0.005,
    "confidence_absolute_tolerance": 0.0005,
    "corpus_wer_max": 0.12,
    "long_audio_wer_max": 0.15,
    "batch_throughput_ratio_min": 0.90,
    "batch_promotion_ratio_min": 1.10,
    "native_batches_per_model_min": 3,
    "burst_tail_serial_work_ratio_max": 1.25,
    "mixed_tail_serial_work_ratio_max": 2.0,
    "mixed_p95_ms_max": {"embed": 1500, "extract": 2000, "whisper": 5000, "ocr": 15000},
    "recovery_seconds_max": 30,
    "managed_limit_gib": 38,
}


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def assert_equivalent(kind, reference, candidate):
    if kind == "embed":
        return embedding_equivalent(reference, candidate)
    if kind == "extract":
        equivalent(reference, candidate, tolerance=0.0005)
    else:
        # Physical batching attribution may legitimately differ. Everything
        # else is the caller's public response contract, including usage/IDs.
        left, right = copy.deepcopy(reference), copy.deepcopy(candidate)
        left.pop("execution", None)
        right.pop("execution", None)
        try:
            equivalent(left["usage"], right["usage"], tolerance=0)
            equivalent(left, right, tolerance=0.0005)
        except (AttributeError, TypeError, KeyError) as error:
            raise AssertionError("recognition response contract changed") from error
        assert all(item.get("status") == "complete" for item in candidate["data"]), (
            "incomplete recognition"
        )
    return {"equivalent": True}


def require_observed_batch(kind, before, after, responses):
    if kind == "ocr":
        widths = [
            (r.get("execution") or {}).get("max_native_step_items", 0)
            for r in responses
        ]
        if not widths or max(widths) < 2:
            raise q.EvidenceUnavailable("OCR has no observed multirow decoder step")
        return {
            "max_observed_rows": max(widths),
            "batched_requests": sum(x > 1 for x in widths),
        }
    names = (
        "antfly_inference_executor_microbatch_native_batches_total",
        "antfly_inference_executor_microbatch_native_items_total",
    )
    for snapshot in (before, after):
        for name in names:
            value = snapshot.get(name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise q.EvidenceUnavailable(
                    f"missing or invalid physical batch counter: {name}"
                )
    if any(after[name] < before[name] for name in names):
        raise q.EvidenceUnavailable(
            "physical batch counters reset during controlled traffic"
        )
    batches, items = (after[name] - before[name] for name in names)
    if not math.isfinite(batches) or batches < 1 or items <= batches:
        raise q.EvidenceUnavailable(f"{kind} has no observed physical multirow forward")
    return {
        "native_batches": batches,
        "native_items": items,
        "stage": "encoder" if kind == "whisper" else "model forward",
    }


def latency_gate(serial_seconds, concurrent_seconds, latencies):
    if (
        not serial_seconds
        or not latencies
        or min(serial_seconds, concurrent_seconds) <= 0
    ):
        raise q.EvidenceUnavailable("missing controlled timing evidence")
    ratio = serial_seconds / concurrent_seconds
    assert ratio >= ACCEPTANCE["batch_throughput_ratio_min"], (
        "throughput regression",
        ratio,
    )
    assert (
        max(latencies)
        <= serial_seconds * ACCEPTANCE["burst_tail_serial_work_ratio_max"] + 0.050
    ), "burst completion exceeds serialized work"
    return {
        "throughput_ratio": ratio,
        "promotion_10_percent_pass": ratio >= ACCEPTANCE["batch_promotion_ratio_min"],
    }


def frozen_boundary_output(expected, actual):
    """Compare the public projection with the independent frozen Python oracle."""

    def confidence(reference, observed):
        assert (
            type(observed) in (int, float)
            and math.isfinite(observed)
            and 0 <= observed <= 1
        )
        assert abs(observed - reference) <= 0.0005

    def value(reference, observed, *, field=False):
        assert observed["value" if field else "text"] == reference["text"]
        confidence(reference["confidence"], observed["score"])
        source = reference.get("source")
        if source is not None:
            assert (observed["start"], observed["end"]) == (
                source["start"],
                source["end"],
            )
        if field:
            assert observed["source"] == (
                "document" if source is not None else "schema"
            )
        for attribute in reference.get("attributes", []):
            returned = observed.get("attributes", {})[attribute["name"]]
            returned = returned if isinstance(returned, list) else [returned]
            assert len(returned) == len(attribute["labels"])
            for left, right in zip(attribute["labels"], returned, strict=True):
                assert left["label"] == right["label"]
                confidence(left["confidence"], right["confidence"])

    entities = [
        (group["name"], item)
        for group in expected["entities"]
        for item in group["values"]
    ]
    assert len(entities) == len(actual.get("entities", []))
    for (label, reference), observed in zip(
        entities, actual.get("entities", []), strict=True
    ):
        assert label == observed["label"]
        value(reference, observed)
    classifications = [
        (task["name"], label)
        for task in expected["classifications"]
        for label in task["labels"]
    ]
    assert len(classifications) == len(actual.get("classifications", []))
    for (name, reference), observed in zip(
        classifications, actual.get("classifications", []), strict=True
    ):
        assert name == observed["name"] and reference["label"] == observed["label"]
        confidence(reference["confidence"], observed["score"])
    assert len(expected["relations"]) == len(actual.get("relations", []))
    for reference, observed in zip(
        expected["relations"], actual.get("relations", []), strict=True
    ):
        assert reference["name"] == observed["type"]
        confidence(reference["confidence"], observed["score"])
        value(reference["head"], observed["source"])
        value(reference["tail"], observed["target"])
    structures = actual.get("structures", {})
    assert {k for k, v in structures.items() if v} == {
        g["name"] for g in expected["structures"] if g["instances"]
    }
    for group in expected["structures"]:
        records = structures.get(group["name"], [])
        assert len(records) == len(group["instances"])
        for reference, observed in zip(group["instances"], records, strict=True):
            for field in reference["fields"]:
                returned = observed.get(field["name"], [])
                returned = returned if isinstance(returned, list) else [returned]
                assert len(returned) == len(field["values"])
                for left, right in zip(field["values"], returned, strict=True):
                    value(left, right, field=True)


def require_extraction_observations(before, after, responses):
    expected_items = sum(len(row["data"]) for row in responses)
    expected_tokens = sum(row["usage"]["prompt_tokens"] for row in responses)
    expected = {
        "decoded_items": expected_items,
        "returned_items": expected_items,
        "decoded_prompt_tokens": expected_tokens,
    }
    observed = {}
    for name, count in expected.items():
        metric = f"antfly_inference_extract_v2_{name}_total"
        if metric not in before or metric not in after:
            raise q.EvidenceUnavailable(
                f"missing extraction observation counter: {metric}"
            )
        delta = after[metric] - before[metric]
        assert delta == count, (metric, "observed", delta, "expected", count)
        observed[name] = delta
    return observed


class Campaign(w.WriteCampaign):
    def telemetry(self):
        code, raw, _ = q.http(self.args.url, "/metrics", text=True)
        if code != 200:
            raise q.EvidenceUnavailable("broker telemetry unavailable")
        return {
            parts[0]: float(parts[1])
            for line in raw.splitlines()
            if not line.startswith("#") and len(parts := line.split()) == 2
        }

    def call(self, kind, body, *, expected=200, deadline=None):
        phase = self.phase
        route = {
            "embed": "/embed",
            "extract": "/extract",
            "whisper": "/read",
            "ocr": "/read",
        }[kind]
        status, response, seconds = q.http(
            self.args.url, route, body, deadline_ms=deadline
        )
        row = {
            "phase": phase,
            "kind": kind,
            "status": status,
            "seconds": seconds,
            "expected_status": expected,
            "completed_at": time.monotonic(),
            "request_sha256": hashlib.sha256(
                json.dumps(body, sort_keys=True).encode()
            ).hexdigest(),
            "response": response,
        }
        with self.lock:
            self.rows.append(row)
            with (self.args.output / "requests.jsonl").open("a") as out:
                out.write(json.dumps(row) + "\n")
        if expected is not None:
            assert status == expected, (phase, kind, status, str(response)[:800])
        if status == 200:
            assert response.get("data"), (kind, "no results")
            if kind == "embed":
                assert len(response["data"]) == len(body["input"])
                for item in response["data"]:
                    v = item["embedding"]
                    assert len(v) == body["dimensions"] and all(
                        math.isfinite(x) for x in v
                    )
                    assert abs(math.sqrt(sum(x * x for x in v)) - 1) <= 0.0001
            elif kind in ("ocr", "whisper"):
                assert all(
                    item.get("status") == "complete" for item in response["data"]
                ), "recognition incomplete"
        return row

    def idle(self):
        until = time.monotonic() + ACCEPTANCE["recovery_seconds_max"]
        names = (
            "antfly_inference_admission_host_scratch_bytes",
            "antfly_inference_admission_backend_scratch_bytes",
            "antfly_inference_admission_backend_kv_bytes",
            "antfly_admission_inference_in_flight_requests",
        )
        last = None
        while True:
            snapshot = self.telemetry()
            if any(name not in snapshot for name in names):
                raise q.EvidenceUnavailable("missing idle resource counters")
            current = {name: snapshot[name] for name in names}
            if current == last and current[names[2]] == current[names[3]] == 0:
                return current
            assert time.monotonic() < until, ("recovery timeout", current)
            last = current
            time.sleep(0.1)

    def cohort(self, kind, body):
        if kind == "whisper":
            import pyarrow.parquet as pq

            with self.args.speech_corpus.open("rb") as source:
                if hashlib.file_digest(source, "sha256").hexdigest() != CORPUS_SHA:
                    raise q.EvidenceUnavailable("speech corpus hash mismatch")
            clips = pq.read_table(self.args.speech_corpus).slice(0, 8).to_pylist()
            return [
                {
                    "model": w.WHISPER,
                    "audio": "data:audio/flac;base64,"
                    + base64.b64encode(clip["audio"]["bytes"]).decode(),
                    "audio_options": {"language": "en", "timestamps": True},
                }
                for clip in clips
            ]
        result = []
        for index in range(8):
            item = copy.deepcopy(body)
            if kind == "embed":
                item["task_type"] = "RETRIEVAL_DOCUMENT"
                item["input"] = [
                    (
                        "Account support explains password resets and security. "
                        * (12 + index % 3)
                    )
                    + f"Case {index}."
                ]
            elif kind == "extract":
                item["inputs"][0]["content"] += " inventory checked." * (index % 4)
                item["inputs"][0]["id"] = f"independent-{index}"
            elif kind == "ocr":
                item = self.ocr([index % 4])
            result.append(item)
        return result

    def speech_accuracy(self):
        import numpy as np
        import pyarrow.parquet as pq

        sys.path.insert(0, str(Path(__file__).parent / "gemma4"))
        import qualify_transcription as speech

        source = self.args.speech_corpus
        if speech.file_sha(source) != CORPUS_SHA:
            raise q.EvidenceUnavailable("speech corpus hash mismatch")
        corpus = pq.read_table(source).to_pylist()
        assert len(corpus) == 73
        scores = []
        self.set_phase("speech-corpus")
        for clip in corpus:
            body = {
                "model": w.WHISPER,
                "audio": "data:audio/flac;base64,"
                + base64.b64encode(clip["audio"]["bytes"]).decode(),
                "audio_options": {"language": "en", "timestamps": True},
            }
            row = self.call("whisper", body)
            text = " ".join(item["text"] for item in row["response"]["data"])
            scores.append(
                {
                    "id": clip["id"],
                    "input_sha256": hashlib.sha256(clip["audio"]["bytes"]).hexdigest(),
                    "reference": clip["text"],
                    "text": text,
                    "score": speech.score(clip["text"], text),
                }
            )
        errors = sum(r["score"]["errors"] for r in scores)
        words = sum(r["score"]["reference_words"] for r in scores)
        result = {
            "corpus_sha256": CORPUS_SHA,
            "clips": len(scores),
            "speakers": sorted({r["speaker_id"] for r in corpus}),
            "wer": errors / words,
            "errors": errors,
            "reference_words": words,
            "clips_scored": scores,
        }
        save(self.args.output / "speech-corpus.json", result)
        assert result["wer"] <= ACCEPTANCE["corpus_wer_max"], (
            "corpus WER",
            result["wer"],
        )
        self.set_phase("dense-varied-long-speech")
        long_rows = []
        for count in (8, 16):
            pcm = np.concatenate(
                [speech.decoded_pcm(clip["audio"]["bytes"]) for clip in corpus[:count]]
            )
            audio = speech.wav_bytes(pcm)
            reference = " ".join(clip["text"] for clip in corpus[:count])
            body = {
                "model": w.WHISPER,
                "audio": "data:audio/wav;base64," + base64.b64encode(audio).decode(),
                "audio_options": {"language": "en", "timestamps": True},
            }
            row = self.call("whisper", body)
            text = " ".join(item["text"] for item in row["response"]["data"])
            score = speech.score(reference, text)
            long_rows.append(
                {
                    "clips": count,
                    "seconds": len(pcm) / 16000,
                    "input_sha256": hashlib.sha256(audio).hexdigest(),
                    "reference": reference,
                    "text": text,
                    "score": score,
                    "response": row["response"],
                }
            )
            save(self.args.output / "long-speech.json", long_rows)
            assert score["wer"] <= ACCEPTANCE["long_audio_wer_max"], (
                "long speech WER",
                count,
                score,
            )
        return result

    def boundary_context_sweep(self):
        self.set_phase("boundary-context-sweep")
        base = self.call("extract", self.extraction())["response"]["usage"][
            "prompt_tokens"
        ]
        probe = self.extraction()
        probe["inputs"][0]["content"] += " inventory checked."
        step = self.call("extract", probe)["response"]["usage"]["prompt_tokens"] - base
        assert step > 0, "cannot calibrate extraction token lengths"
        result = []
        for level in (64, 128, 256, 512, 1024):
            requests, references, tokens = [], [], []
            for index in range(8):
                body = self.extraction()
                repeats = max(0, (level - base) // step - 7 + index)
                body["inputs"][0]["content"] += " inventory checked." * repeats
                body["inputs"][0]["id"] = f"context-{level}-{index}"
                row = self.call("extract", body)
                references.append(row["response"])
                tokens.append(row["response"]["usage"]["prompt_tokens"])
                requests.append(body)
            before = self.telemetry()
            with ThreadPoolExecutor(max_workers=8) as pool:
                rows = list(
                    pool.map(lambda request: self.call("extract", request), requests)
                )
            for reference, row in zip(references, rows, strict=True):
                assert_equivalent("extract", reference, row["response"])
            after = self.telemetry()
            evidence = (
                require_observed_batch(
                    "extract", before, after, [r["response"] for r in rows]
                )
                if level <= 512
                else None
            )
            if level > 512:
                assert (
                    after["antfly_inference_executor_microbatch_native_items_total"]
                    == before["antfly_inference_executor_microbatch_native_items_total"]
                ), "oversized extraction unexpectedly entered the short cohort"
            result.append(
                {
                    "target_tokens": level,
                    "actual_tokens": tokens,
                    "observed": evidence,
                    "singleton_reference_parity": True,
                }
            )
            save(self.args.output / "boundary-context-sweep.json", result)
        return result

    def boundary_schema_sweep(self):
        self.set_phase("boundary-schema-sweep")
        cases = json.loads(
            (
                ROOT / "zig/pkg/inference/testdata/gliner25/pipeline_cases_base.json"
            ).read_text()
        )["cases"]
        result = []
        for case in cases:
            if set(case["schema"]) <= {"classifications", "classification_constraints"}:
                result.append(
                    {
                        "case": case["id"],
                        "scope": "classification-only uses the decisions API; excluded from write extraction",
                    }
                )
                continue
            body = {
                "schema_version": 2,
                "model": w.GLINER,
                "schema": case["schema"],
                "inputs": [{"id": case["id"], "content": case["text"]}],
                "options": {
                    "include_confidence": True,
                    "include_spans": True,
                    "offset_unit": "unicode_codepoints",
                },
            }
            if "joint_ie" in case["schema"]:
                # The frozen native oracle uses the explicit native solver;
                # the separately fenced source-beam default is not this case.
                body["options"]["decoder"] = {"algorithm": "auto"}
            reference = self.call("extract", body)["response"]
            frozen_boundary_output(case["expected"], reference["data"][0])
            cohort = []
            for index in range(8):
                request = copy.deepcopy(body)
                request["inputs"][0]["id"] = f"{case['id']}-{index}"
                cohort.append(request)
            before = self.telemetry()
            with ThreadPoolExecutor(max_workers=8) as pool:
                rows = list(
                    pool.map(lambda request: self.call("extract", request), cohort)
                )
            for request, row in zip(cohort, rows, strict=True):
                expected = copy.deepcopy(reference)
                expected["data"][0]["id"] = request["inputs"][0]["id"]
                assert_equivalent("extract", expected, row["response"])
                frozen_boundary_output(case["expected"], row["response"]["data"][0])
            after = self.telemetry()
            observations = require_extraction_observations(
                before, after, [row["response"] for row in rows]
            )
            result.append(
                {
                    "case": case["id"],
                    "caller_observations": observations,
                    "requests": 9,
                    "frozen_oracle_and_singleton_parity": True,
                    "native_batches": after[
                        "antfly_inference_executor_microbatch_native_batches_total"
                    ]
                    - before[
                        "antfly_inference_executor_microbatch_native_batches_total"
                    ],
                }
            )
            save(self.args.output / "boundary-schema-sweep.json", result)
        return result

    def execute_production(self):
        harness = Path(__file__).read_bytes()
        (self.args.output / "harness.py").write_bytes(harness)
        result = {
            "status": "running",
            "qualification_kind": "diagnostic"
            if self.args.diagnostic
            else "serving-envelope",
            "acceptance": ACCEPTANCE,
            "scope": __doc__,
            "rounds": self.args.rounds,
            "soak_rounds_min": self.args.soak_rounds,
            "soak_seconds_min": self.args.soak_seconds,
            "harness_sha256": hashlib.sha256(harness).hexdigest(),
        }
        monitor = threading.Thread(target=self.monitor, daemon=True)
        monitor.start()
        try:
            result["identity"] = serving_identity(
                self.args, self, check_cooperative_mode=False
            )
            _, inventory, _ = q.http(self.args.url, "/models/instances")
            self.instances_before = inventory["data"]
            assert {i["model"] for i in self.instances_before} == {
                q.EMBED,
                w.GLINER,
                w.WHISPER,
                w.OCR,
            }
            assert all(
                i["backend"] == "cuda" and i["startup_managed"]
                for i in self.instances_before
            )
            jobs = [
                ("embed", self.embed()),
                ("extract", self.extraction()),
                ("whisper", self.whisper()),
                ("ocr", self.ocr([0])),
            ]
            self.set_phase("warmup")
            references = {
                kind: self.call(kind, body)["response"] for kind, body in jobs
            }
            expected_entities = [
                (
                    group["name"],
                    value["text"],
                    value["source"]["start"],
                    value["source"]["end"],
                    value["confidence"],
                )
                for group in self.base["expected"]["entities"]
                for value in group["values"]
            ]
            actual_entities = [
                (
                    value["label"],
                    value["text"],
                    value["start"],
                    value["end"],
                    value["score"],
                )
                for value in references["extract"]["data"][0]["entities"]
            ]
            assert [row[:4] for row in actual_entities] == [
                row[:4] for row in expected_entities
            ], "frozen extraction oracle differs"
            assert all(
                abs(a[4] - b[4]) <= 0.0005
                for a, b in zip(actual_entities, expected_entities, strict=True)
            )
            assert (
                w.wer(self.audio_reference, references["whisper"]["data"][0]["text"])
                <= 0.10
            ), "independent speech fixture differs"
            for needle in self.ocr_cases[0]["required"]:
                assert (
                    needle.casefold() in references["ocr"]["data"][0]["text"].casefold()
                ), "independent OCR fixture differs"
            cohorts = {kind: self.cohort(kind, body) for kind, body in jobs}
            # Establish retained-cache baselines at the configured concurrency
            # and page envelope, rather than treating legitimate first batch
            # workspace growth as a leak after singleton warmup.
            for kind, _ in jobs:
                with ThreadPoolExecutor(max_workers=8) as pool:
                    list(
                        pool.map(
                            lambda request: self.call(kind, request), cohorts[kind]
                        )
                    )
            long_extract = self.extraction(padded=True)
            long_extract["options"]["long_document"] = {
                "mode": "window",
                "window_words": 128,
                "overlap_words": 16,
            }
            long_reference = self.call("extract", long_extract)["response"]
            warmed = self.idle()
            result["batching"] = {}
            for kind, body in jobs:
                self.set_phase(kind + "-serialized")
                cohort = cohorts[kind]
                individual = [
                    self.call(kind, request)["response"] for request in cohort
                ]
                serial = []
                for _ in range(self.args.rounds):
                    start = time.monotonic()
                    for request, reference in zip(cohort, individual, strict=True):
                        row = self.call(kind, request)
                        assert_equivalent(kind, reference, row["response"])
                    serial.append(time.monotonic() - start)
                self.set_phase(kind + "-automatic-batching")
                batches, observed, tails = [], [], []
                first_counters = self.telemetry()
                responses = []
                for _ in range(self.args.rounds):
                    before = self.telemetry()
                    start = time.monotonic()
                    with ThreadPoolExecutor(max_workers=8) as pool:
                        rows = list(
                            pool.map(lambda request: self.call(kind, request), cohort)
                        )
                    batches.append(time.monotonic() - start)
                    for row, reference in zip(rows, individual, strict=True):
                        assert_equivalent(kind, reference, row["response"])
                    tails.append(max(r["seconds"] for r in rows))
                    responses.extend(row["response"] for row in rows)
                    after = self.telemetry()
                    observed.append(
                        {
                            "native_batches": after.get(
                                "antfly_inference_executor_microbatch_native_batches_total",
                                0,
                            )
                            - before.get(
                                "antfly_inference_executor_microbatch_native_batches_total",
                                0,
                            ),
                            "native_items": after.get(
                                "antfly_inference_executor_microbatch_native_items_total",
                                0,
                            )
                            - before.get(
                                "antfly_inference_executor_microbatch_native_items_total",
                                0,
                            ),
                            "decoder_widths": [
                                (r["response"].get("execution") or {}).get(
                                    "max_native_step_items"
                                )
                                for r in rows
                            ]
                            if kind == "ocr"
                            else [],
                        }
                    )
                evidence = require_observed_batch(
                    kind, first_counters, after, responses
                )
                # Collection is opportunistic and deadline bounded. Require
                # repeated physical execution across the controlled rounds;
                # an individual staggered burst may legitimately stay singleton.
                assert (
                    evidence.get("batched_requests", evidence.get("native_batches", 0))
                    >= ACCEPTANCE["native_batches_per_model_min"]
                ), ("insufficient repeated batching", kind, evidence)
                result["batching"][kind] = {
                    "serialized_seconds": serial,
                    "concurrent_seconds": batches,
                    "observed": observed,
                    "physical_evidence": evidence,
                    **latency_gate(
                        statistics.median(serial), statistics.median(batches), tails
                    ),
                }
                save(self.args.output / "qualification.json", result)
            self.set_phase("request-limits-and-isolation")
            invalid = [
                ("embed", {**self.embed(), "input": ["word " * 9000]}),
                (
                    "extract",
                    {**self.extraction(), "inputs": self.extraction()["inputs"] * 9},
                ),
                ("ocr", {**self.ocr([0]), "images": self.ocr([0])["images"] * 9}),
            ]
            for kind, body in invalid:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    good_body = next(request for name, request in jobs if name == kind)
                    healthy = pool.submit(self.call, kind, good_body)
                    denied = self.call(kind, body, expected=None)
                    assert denied["status"] in (400, 413, 422), (
                        "request limit not enforced",
                        kind,
                        denied["status"],
                    )
                    assert_equivalent(
                        kind, references[kind], healthy.result()["response"]
                    )
            self.set_phase("mixed-sustained-turnover")
            mixed_work = sum(
                statistics.median(result["batching"][kind]["serialized_seconds"])
                for kind, _ in jobs
            )
            soak_started = time.monotonic()
            turn = 0
            with ThreadPoolExecutor(max_workers=8) as pool:
                while (
                    turn < self.args.soak_rounds
                    or time.monotonic() - soak_started < self.args.soak_seconds
                ):
                    # Unequal OCR pages and independent source IDs prevent an
                    # identical-pair-only campaign from hiding ownership bugs.
                    current = []
                    for i in range(8):
                        kind, body = jobs[i % 4]
                        if kind == "ocr":
                            body = self.ocr([(turn + i // 4) % 4])
                        if kind == "extract":
                            body = (
                                copy.deepcopy(long_extract)
                                if turn % 4 == 0 and i == 5
                                else self.extraction()
                            )
                            body["inputs"][0]["id"] = f"turn-{turn}-{i}"
                        current.append((kind, body))
                    start = time.monotonic()
                    rows = list(pool.map(lambda job: self.call(*job), current))
                    assert (
                        time.monotonic() - start
                        <= mixed_work * ACCEPTANCE["mixed_tail_serial_work_ratio_max"]
                        + 0.050
                    ), "mixed queue starvation"
                    for index, ((kind, body), row) in enumerate(
                        zip(current, rows, strict=True)
                    ):
                        if kind == "extract":
                            assert (
                                row["response"]["data"][0]["id"]
                                == body["inputs"][0]["id"]
                            )
                            expected = copy.deepcopy(
                                long_reference
                                if body["options"].get("long_document")
                                else references[kind]
                            )
                            expected["data"][0]["id"] = body["inputs"][0]["id"]
                            assert_equivalent(kind, expected, row["response"])
                        elif kind == "embed":
                            assert_equivalent(kind, references[kind], row["response"])
                        elif kind == "ocr":
                            for needle in self.ocr_cases[(turn + index // 4) % 4][
                                "required"
                            ]:
                                assert (
                                    needle.casefold()
                                    in row["response"]["data"][0]["text"].casefold()
                                ), "OCR lost required values"
                    if turn % 5 == 0:
                        print("mixed turnover", turn, flush=True)
                    turn += 1
            result["soak"] = {
                "rounds": turn,
                "requests": turn * 8,
                "seconds": time.monotonic() - soak_started,
            }
            result["mixed_latency"] = {}
            for kind in self.task_kinds:
                summary = q.summary(
                    [
                        row
                        for row in self.rows
                        if row["phase"] == "mixed-sustained-turnover"
                        and row["kind"] == kind
                    ]
                )
                result["mixed_latency"][kind] = summary
                assert summary["p95_ms"] <= ACCEPTANCE["mixed_p95_ms_max"][kind], (
                    "mixed latency gate",
                    kind,
                    summary["p95_ms"],
                )
            save(self.args.output / "qualification.json", result)
            result["boundary_context_sweep"] = self.boundary_context_sweep()
            result["boundary_schema_sweep"] = self.boundary_schema_sweep()
            result["speech"] = self.speech_accuracy()
            self.set_phase("known-checkpoint-accuracy-limit")
            repeated, copies = self.windowed_whisper(repeated=True)
            repeated_row = self.call("whisper", repeated)
            repeated_text = " ".join(
                item["text"] for item in repeated_row["response"]["data"]
            )
            result["known_accuracy_limits"] = [
                {
                    "case": "identical sentence repeated 24 times",
                    "wer": w.wer(self.audio_reference * copies, repeated_text),
                    "text": repeated_text,
                    "serving_status": repeated_row["status"],
                    "interpretation": "Known checkpoint accuracy limitation; not a passed accuracy case. The declared varied-speech and serving gates do not qualify arbitrary repetitive speech.",
                }
            ]
            self.set_phase("deadlines-and-disconnects")
            for kind, body in jobs:
                # Very short deadlines exercise queue removal and claimed
                # drain; a healthy peer must retain its independent control.
                with ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(self.call, kind, body) for _ in range(4)]
                    expired = self.call(kind, body, expected=None, deadline=1)
                    assert expired["status"] == 504, (
                        "deadline not enforced",
                        kind,
                        expired["status"],
                    )
                    for f in futures:
                        f.result()
                host = urlsplit(self.args.url)
                route = (
                    "/embed"
                    if kind == "embed"
                    else "/extract"
                    if kind == "extract"
                    else "/read"
                )
                raw = json.dumps(body).encode()
                with socket.create_connection((host.hostname, host.port)) as client:
                    client.sendall(
                        f"POST /ai/v1{route} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {len(raw)}\r\n\r\n".encode()
                        + raw
                    )
                    time.sleep(0.01)
                    client.setsockopt(
                        socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                    )
                self.idle()
            self.set_phase("recovery")
            for kind, body in jobs:
                assert_equivalent(
                    kind, references[kind], self.call(kind, body)["response"]
                )
            recovered = self.idle()
            _, inventory, _ = q.http(self.args.url, "/models/instances")
            self.instances_after = inventory["data"]
            assert {i["id"] for i in self.instances_before} == {
                i["id"] for i in self.instances_after
            }, "unexpected model reload"
            for name in (
                "antfly_inference_admission_host_scratch_bytes",
                "antfly_inference_admission_backend_scratch_bytes",
            ):
                assert recovered[name] <= warmed[name], (
                    "retained scratch grew",
                    name,
                    warmed,
                    recovered,
                )
            result["reservations"] = {"warmed": warmed, "recovered": recovered}
            verify_artifacts(result["identity"], self.args)
            self.require_memory_evidence()
            self.status = result["status"] = "passed"
        except BaseException as error:
            self.status = result["status"] = (
                "incomplete" if isinstance(error, q.EvidenceUnavailable) else "failed"
            )
            self.error = result["error"] = str(error)[:1200]
            raise
        finally:
            self.stop_monitor(monitor)
            result["status"] = self.status
            result["error"] = self.error
            save(self.args.output / "qualification.json", result)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default="http://127.0.0.1:18841")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--cgroup", type=Path, required=True)
    p.add_argument("--speech-corpus", type=Path, required=True)
    p.add_argument("--rounds", type=int, default=5)
    p.add_argument("--soak-rounds", type=int, default=100)
    p.add_argument("--soak-seconds", type=float, default=300)
    p.add_argument(
        "--diagnostic",
        action="store_true",
        help="Allow a shorter soak; never promote this receipt as full qualification",
    )
    args = p.parse_args()
    if args.rounds < 3 or args.soak_rounds < 20:
        p.error(
            "qualification requires at least three controlled rounds and twenty turnover rounds"
        )
    if not args.diagnostic and (args.soak_rounds < 100 or args.soak_seconds < 300):
        p.error(
            "serving qualification requires at least 100 turnover rounds and five minutes; use --diagnostic for a preliminary run"
        )
    if args.output.exists() and any(args.output.iterdir()):
        p.error(
            "output must be a new or empty directory; preserve previous qualification receipts"
        )
    args.pressure = False
    Campaign(args).execute_production()


if __name__ == "__main__":
    main()
