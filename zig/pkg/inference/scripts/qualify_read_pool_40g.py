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

"""Exercise a resident read pool while Gemma's active and retained KV grow.

Runs against an already-started, owned server. Records synthetic requests,
responses, latency and memory samples. Spark results do not qualify A100 speed.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import ctypes
import json
import math
from pathlib import Path
import statistics
import threading
import time
import urllib.error
import urllib.request

GEMMA = "google/gemma-4-E4B-it-qat-q4_0-gguf"
DECIDE = "antflydb/GLiNER2.5-Decide"
EMBED = "google/embeddinggemma-2"


def http(url, path, body=None, timeout=300, text=False, deadline_ms=None):
    req = urllib.request.Request(
        url + ("/ml/v1" if path == "/metrics" else "/ai/v1") + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "X-Antfly-Inference-Timeout-Ms": str(deadline_ms if deadline_ms is not None else int(min(timeout, 3600) * 1000))},
    )
    started = time.monotonic()
    try:
        response = urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        return response.status, raw.decode() if text else json.loads(raw), time.monotonic() - started


def streamed_generation(url, body, deadline_seconds):
    """Measure client-observed first content separately from response completion."""
    req = urllib.request.Request(
        url + "/ai/v1/generate", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "X-Antfly-Inference-Timeout-Ms": str(deadline_seconds * 1000)},
    )
    started = time.monotonic()
    try:
        response = urllib.request.urlopen(req, timeout=deadline_seconds + 30)
    except urllib.error.HTTPError as error:
        with error:
            return error.code, json.loads(error.read()), time.monotonic() - started, {}
    content, reasoning, usage, finish = [], [], None, None
    first_content = last_content = None
    content_times = []
    done = False
    with response:
        for line in response:
            if not line.startswith(b"data:"):
                continue
            data = line[5:].strip()
            if data == b"[DONE]":
                done = True
                break
            event = json.loads(data)
            assert "error" not in event, event
            if event.get("usage"):
                usage = event["usage"]
            for choice in event.get("choices", []):
                delta = choice.get("delta", {})
                if delta.get("content"):
                    last_content = time.monotonic() - started
                    if first_content is None:
                        first_content = last_content
                    content_times.append(last_content)
                    content.append(delta["content"])
                if delta.get("reasoning_content"):
                    reasoning.append(delta["reasoning_content"])
                if choice.get("finish_reason"):
                    finish = choice["finish_reason"]
        assert done and usage is not None and first_content is not None, "incomplete generation stream"
        message = {"role": "assistant", "content": "".join(content)}
        if reasoning:
            message["reasoning_content"] = "".join(reasoning)
        assembled = {"model": body["model"], "choices": [{"index": 0, "message": message, "finish_reason": finish}], "usage": usage}
        timing = {"first_content_seconds": first_content, "after_first_content_seconds": last_content - first_content, "content_event_seconds": content_times, "max_content_gap_seconds": max((b-a for a,b in zip(content_times, content_times[1:])), default=0)}
        return response.status, assembled, time.monotonic() - started, timing


def metrics(raw):
    result = {}
    for line in raw.splitlines():
        if line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) == 2 and any(word in parts[0] for word in (
            "admission_", "cuda_managed_", "prompt_cache_", "native_scheduler_", "generation_", "models_loaded"
        )):
            result[parts[0]] = float(parts[1])
    return result


class Gpu:
    class Util(ctypes.Structure):
        _fields_ = [("gpu", ctypes.c_uint), ("memory", ctypes.c_uint)]

    class Process(ctypes.Structure):
        _fields_ = [("pid", ctypes.c_uint), ("bytes", ctypes.c_ulonglong), ("gpu_instance", ctypes.c_uint), ("compute_instance", ctypes.c_uint)]

    def __init__(self):
        self.lib = ctypes.CDLL("libnvidia-ml.so.1")
        self.handle = ctypes.c_void_p()
        if self.lib.nvmlInit_v2() or self.lib.nvmlDeviceGetHandleByIndex_v2(0, ctypes.byref(self.handle)):
            raise RuntimeError("NVML initialization failed")

    def sample(self):
        util = self.Util()
        power = ctypes.c_uint()
        count = ctypes.c_uint(64)
        processes = (self.Process * count.value)()
        status = self.lib.nvmlDeviceGetComputeRunningProcesses_v3(self.handle, ctypes.byref(count), processes)
        return {
            "gpu_util_percent": util.gpu if self.lib.nvmlDeviceGetUtilizationRates(self.handle, ctypes.byref(util)) == 0 else None,
            "gpu_power_mw": power.value if self.lib.nvmlDeviceGetPowerUsage(self.handle, ctypes.byref(power)) == 0 else None,
            "nvml_compute_processes": [{"pid": p.pid, "bytes": p.bytes if p.bytes < (1 << 64) - 1 else None} for p in processes[:count.value]] if status == 0 else None,
        }


def summary(rows):
    values = sorted(row["seconds"] for row in rows if row["status"] == 200)
    ttft = sorted(row["first_content_seconds"] for row in rows if row["status"] == 200 and "first_content_seconds" in row)
    return {
        "requests": len(rows),
        "successful": len(values),
        "statuses": {str(code): sum(r["status"] == code for r in rows) for code in sorted({r["status"] for r in rows})},
        "p50_ms": statistics.median(values) * 1000 if values else None,
        "p95_ms": values[min(len(values) - 1, math.ceil(len(values) * .95) - 1)] * 1000 if values else None,
        "first_content_p50_ms": statistics.median(ttft) * 1000 if ttft else None,
        "first_content_p95_ms": ttft[min(len(ttft) - 1, math.ceil(len(ttft) * .95) - 1)] * 1000 if ttft else None,
    }


class EvidenceUnavailable(RuntimeError):
    """Required memory evidence is missing; the qualification is incomplete."""


class Campaign:
    task_kinds = ("generate", "embed", "decide")

    def __init__(self, args):
        self.args = args
        args.output.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.rows = []
        self.samples = []
        self.phase = "startup"
        self.stop = threading.Event()
        self.sample_lock = threading.Lock()
        self.evidence_errors = []
        self.workload_phases = set()
        self.gpu_error = None
        try:
            self.gpu = Gpu()
        except Exception as error:
            self.gpu = None
            self.gpu_error = str(error)
        self.embedding_reference = None
        self.embedding_batch_reference = None
        self.decision_reference = None
        self.status = "running"
        self.error = None
        self.instances_before = None
        self.instances_after = None

    def request(self, kind, body, expected=200):
        path = {"embed": "/embed", "decide": "/decisions", "generate": "/generate"}[kind]
        phase = self.phase
        timing = {}
        if kind == "generate" and getattr(self.args, "disable_thinking", False):
            body = {**body, "chat_template_kwargs": {"enable_thinking": False}}
        if kind == "generate" and getattr(self.args, "stream_generations", False):
            body = {**body, "stream": True, "stream_options": {"include_usage": True}}
            status, response, seconds, timing = streamed_generation(self.args.url, body, self.args.generation_timeout)
        else:
            status, response, seconds = http(self.args.url, path, body, timeout=self.args.generation_timeout + 30 if kind == "generate" else 300, deadline_ms=self.args.generation_timeout * 1000 if kind == "generate" else None)
        row = {"phase": phase, "kind": kind, "status": status, "seconds": seconds, "completed_at": time.time(), "response": response, **timing}
        if status == 200 and kind == "embed":
            vectors = [item["embedding"] for item in response["data"]]
            assert len(vectors) == len(body["input"])
            for vector in vectors:
                assert all(math.isfinite(x) for x in vector)
                assert abs(sum(x*x for x in vector) - 1) < .01
            if self.embedding_batch_reference is None:
                self.embedding_reference = vectors[0]
                self.embedding_batch_reference = vectors
            row["max_embedding_delta"] = max(abs(a-b) for vector, reference in zip(vectors, self.embedding_batch_reference, strict=True) for a,b in zip(vector, reference, strict=True))
            assert row["max_embedding_delta"] < .002, row
        if status == 200 and kind == "decide":
            answers = response["answers"]
            assert answers[0]["choice"] == "account_access", answers
            if self.decision_reference is None:
                self.decision_reference = answers
            row["decisions_unchanged"] = answers == self.decision_reference
            assert row["decisions_unchanged"], row
        if status == 200 and kind == "generate":
            assert response["choices"][0]["message"]["content"].strip(), response
            assert response["usage"]["completion_tokens"] == body["max_tokens"], response
        with self.lock:
            self.rows.append(row)
            with (self.args.output / "requests.jsonl").open("a") as out:
                out.write(json.dumps({**row, "request": body}) + "\n")
        assert status == expected, (kind, status, response)
        return row

    @staticmethod
    def embed():
        return {"model": EMBED, "input": ["How can I reset my account password?"], "task_type": "RETRIEVAL_QUERY", "dimensions": 256}

    @staticmethod
    def decide():
        return {"model": DECIDE, "input": "I cannot sign in. Please help me reset my account password.", "questions": [{"name": "topic", "type": "choice", "instructions": "Choose the support request category.", "choices": [{"value": "account_access", "description": "Signing in, password resets, or account access."}, {"value": "billing", "description": "Invoices, payments, refunds, or subscription charges."}]}]}

    @staticmethod
    def generate(records, output_tokens, cache_key=None):
        prompt = "Review these warehouse records. Then summarize what they describe in plain English.\n"
        prompt += "".join(f"Record {i:06d}: warehouse received item {i % 997}, checked its label, and stored it on shelf {i % 31}.\n" for i in range(records))
        body = {"model": GEMMA, "backend": "cuda", "messages": [{"role": "user", "content": prompt}], "max_tokens": output_tokens, "temperature": 0, "ignore_eos": True, "cache_dtype": "f16", "prompt_cache": cache_key is not None}
        if cache_key is not None:
            body["prompt_cache_key"] = cache_key
        return body

    def validate_sample(self, row):
        if row["metrics_status"] != 200:
            raise EvidenceUnavailable("metrics request failed")
        required = ("antfly_inference_cuda_managed_live_bytes", "antfly_inference_cuda_managed_peak_bytes", "antfly_inference_cuda_managed_limit_bytes")
        for name in required:
            value = row["metrics"].get(name)
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise EvidenceUnavailable(f"missing or invalid managed memory metric: {name}")
        managed = row["metrics"]
        limit = managed[required[2]]
        if limit != 38 * (1 << 30):
            raise EvidenceUnavailable("expected 38 GiB shared CUDA limit")
        assert managed[required[0]] <= managed[required[1]] <= limit, "managed CUDA ceiling exceeded"
        if self.args.cgroup:
            if not row.get("service_pids"):
                raise EvidenceUnavailable("service cgroup has no process IDs")
            processes = row.get("nvml_compute_processes")
            if processes is None:
                raise EvidenceUnavailable(self.gpu_error or "NVML process memory unavailable")
            owned = [p for p in processes if p["pid"] in row["service_pids"]]
            if not owned or any(not isinstance(p.get("bytes"), int) or p["bytes"] <= 0 for p in owned):
                raise EvidenceUnavailable("NVML has no valid memory measurement for every visible service GPU process")
            row["service_nvml_bytes"] = sum(p["bytes"] for p in owned)
            assert row["service_nvml_bytes"] <= 40 * (1 << 30), "service GPU memory exceeded nominal 40 GiB target"
            for name in ("memory.current", "memory.peak", "memory.swap.current"):
                if not isinstance(row.get(name), int) or row[name] < 0:
                    raise EvidenceUnavailable(f"missing cgroup attribution: {name}")

    def _capture_sample(self):
        """Caller owns sample_lock so transitions cannot race attribution."""
        row = {"time": time.time(), "phase": self.phase}
        try:
            status, raw, _ = http(self.args.url, "/metrics", timeout=5, text=True)
            row.update(metrics_status=status, metrics=metrics(raw))
            row.update(self.gpu.sample() if self.gpu else {"gpu_util_percent": None, "nvml_compute_processes": None})
            if self.args.cgroup:
                for name in ("memory.current", "memory.peak", "memory.swap.current"):
                    row[name] = int((self.args.cgroup / name).read_text())
                row["service_pids"] = sorted(int(pid) for pid in (self.args.cgroup / "cgroup.procs").read_text().split())
            self.validate_sample(row)
            with self.lock:
                self.samples.append(row)
        except Exception as error:
            row["error"] = str(error)
            with self.lock:
                self.evidence_errors.append({"phase": row["phase"], "error": str(error), "type": type(error).__name__})
        with (self.args.output / "memory.jsonl").open("a") as out:
            out.write(json.dumps(row) + "\n")
        return "error" not in row

    def capture_sample(self):
        with self.sample_lock:
            return self._capture_sample()

    def set_phase(self, name):
        # Boundary samples cover even workloads shorter than the monitor period.
        with self.sample_lock:
            if self.phase in self.workload_phases:
                self._capture_sample()
            self.phase = name
            self.workload_phases.add(name)
            self._capture_sample()

    def require_memory_evidence(self):
        self.capture_sample()
        with self.lock:
            if any(e["type"] == "AssertionError" for e in self.evidence_errors):
                raise AssertionError(f"memory limit violation: {self.evidence_errors}")
            if self.evidence_errors:
                raise EvidenceUnavailable(f"{len(self.evidence_errors)} memory sampling failures: {self.evidence_errors[0]}")
            if not self.workload_phases or not self.samples:
                raise EvidenceUnavailable("no workload memory samples")
            for phase in self.workload_phases:
                samples = [s for s in self.samples if s["phase"] == phase]
                if len(samples) < 2:
                    raise EvidenceUnavailable(f"insufficient memory samples during {phase}")
                for sample in samples:
                    self.validate_sample(sample)

    def stop_monitor(self, thread):
        self.stop.set()
        thread.join(timeout=10)
        try:
            if thread.is_alive():
                raise EvidenceUnavailable("memory monitor did not stop")
            if self.status == "passed":
                self.require_memory_evidence()
        except Exception as error:
            self.status = "incomplete" if isinstance(error, EvidenceUnavailable) else "failed"
            self.error = str(error)
            raise
        finally:
            self.save()

    def monitor(self):
        while not self.stop.is_set():
            self.capture_sample()
            self.stop.wait(.25)

    def save(self):
        stages = {}
        for phase in sorted({r["phase"] for r in self.rows}):
            stages[phase] = {kind: summary([r for r in self.rows if r["phase"] == phase and r["kind"] == kind]) for kind in self.task_kinds}
            samples = [s for s in self.samples if s["phase"] == phase]
            names = {name for sample in samples for name in sample["metrics"]}
            stages[phase]["metric_peaks"] = {name: max(s["metrics"][name] for s in samples if name in s["metrics"]) for name in names}
            utils = [s["gpu_util_percent"] for s in samples if s["gpu_util_percent"] is not None]
            stages[phase]["mean_gpu_util_percent"] = statistics.mean(utils) if utils else None
            for name in ("service_nvml_bytes", "memory.current", "memory.peak"):
                values = [s[name] for s in samples if s.get(name) is not None]
                stages[phase][name + "_max"] = max(values) if values else None
        report = {"status": self.status, "error": self.error, "validation_mode": "KV reservation and deadline cleanup" if self.args.pressure else "completed generations and mixed read probes", "scope": "Spark read-pool co-residency and memory-pressure experiment; not A100 hardware qualification", "instances_before": self.instances_before, "instances_after": self.instances_after, "stages": stages, "memory_evidence": {"complete": self.status == "passed", "errors": self.evidence_errors, "samples_per_phase": {phase: sum(s["phase"] == phase for s in self.samples) for phase in sorted(self.workload_phases)}, "service_nvml_requested": bool(self.args.cgroup)}}
        (self.args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    def run(self):
        thread = threading.Thread(target=self.monitor, daemon=True)
        thread.start()
        try:
            code, inventory, _ = http(self.args.url, "/models/instances")
            assert code == 200 and {item["model"] for item in inventory["data"]} == {GEMMA, DECIDE, EMBED}, inventory
            assert all(item["backend"] == "cuda" and item["startup_managed"] for item in inventory["data"]), inventory
            self.instances_before = inventory["data"]
            code, raw, _ = http(self.args.url, "/metrics", text=True)
            assert code == 200 and metrics(raw).get("antfly_inference_cuda_managed_limit_bytes") == 38 * (1 << 30), "38 GiB physical CUDA guard is not active"
            for kind, body in (("embed", self.embed()), ("decide", self.decide()), ("generate", self.generate(4, 16))):
                self.request(kind, body)
            self.set_phase("baseline")
            time.sleep(.5)
            for _ in range(self.args.probes):
                self.request("embed", self.embed())
                self.request("decide", self.decide())
            self.save()
            for records in self.args.records:
                self.set_phase(f"kv-{records}-records")
                print(self.phase, flush=True)
                with ThreadPoolExecutor(max_workers=4) as pool:
                    generators = [pool.submit(self.request, "generate", self.generate(records, self.args.output_tokens)) for _ in range(self.args.generation_concurrency)]
                    rounds = 0
                    while rounds < self.args.probes or any(not future.done() for future in generators):
                        probes = [pool.submit(self.request, "embed", self.embed()), pool.submit(self.request, "decide", self.decide())]
                        for probe in probes:
                            probe.result()
                        rounds += 1
                        time.sleep(.1)
                    completed = [generator.result() for generator in generators]
                    assert all(row["response"]["choices"] == completed[0]["response"]["choices"] for row in completed), "identical greedy requests produced different outputs"
                if not self.args.skip_retained:
                    self.set_phase(self.phase + "-retained")
                    self.request("generate", self.generate(records, 8, f"read-pool-{records}"))
                    for _ in range(3):
                        self.request("embed", self.embed())
                        self.request("decide", self.decide())
                    time.sleep(1)
                self.save()
            self.set_phase("recovery")
            self.request("embed", self.embed())
            self.request("decide", self.decide())
            self.request("generate", self.generate(4, 16))
            code, inventory, _ = http(self.args.url, "/models/instances")
            self.instances_after = inventory["data"]
            assert code == 200 and {item["id"] for item in self.instances_before} == {item["id"] for item in self.instances_after}, "model residency changed"
            self.require_memory_evidence()
            self.status = "passed"
        except BaseException as error:
            self.status = "incomplete" if isinstance(error, EvidenceUnavailable) else "failed"
            self.error = str(error)
            raise
        finally:
            self.stop_monitor(thread)

    def pressure(self):
        """Reserve real long-context KV, enforce caller deadlines, then recover.

        This checks allocation/admission and cancellation, not completed long-
        context output quality. Two ~124k-token reservations exceed the shared
        KV ceiling; shorter read requests should retain admission headroom.
        """
        thread = threading.Thread(target=self.monitor, daemon=True)
        thread.start()
        try:
            code, inventory, _ = http(self.args.url, "/models/instances")
            assert code == 200
            self.instances_before = inventory["data"]
            _, raw, _ = http(self.args.url, "/metrics", text=True)
            assert metrics(raw).get("antfly_inference_cuda_managed_limit_bytes") == 38 * (1 << 30)
            self.request("embed", self.embed())
            self.request("decide", self.decide())
            for records in self.args.pressure_records:
                self.set_phase(f"pressure-{records}-records")
                print(self.phase, flush=True)
                def long_request():
                    body = self.generate(records, 16)
                    if self.args.disable_thinking:
                        body["chat_template_kwargs"] = {"enable_thinking": False}
                    code, response, seconds = http(self.args.url, "/generate", body, timeout=45, deadline_ms=5000)
                    row = {"phase": self.phase, "kind": "generate", "status": code, "seconds": seconds, "response": response, "intentional_deadline_ms": 5000}
                    with self.lock:
                        self.rows.append(row)
                        with (self.args.output / "requests.jsonl").open("a") as out:
                            out.write(json.dumps(row) + "\n")
                    return code, response
                with ThreadPoolExecutor(max_workers=4) as pool:
                    first = pool.submit(long_request)
                    for _ in range(40):
                        _, raw, _ = http(self.args.url, "/metrics", timeout=5, text=True)
                        if metrics(raw).get("antfly_inference_admission_backend_kv_bytes", 0) > 0 or first.done():
                            break
                        time.sleep(.05)
                    second = pool.submit(long_request)
                    probes = [pool.submit(self.request, "embed", self.embed()), pool.submit(self.request, "decide", self.decide())]
                    for probe in probes:
                        probe.result()
                    outcomes = [first.result(), second.result()]
                print("outcomes", [(code, body.get("error")) for code, body in outcomes], flush=True)
                assert all(code in (200, 503, 504) for code, _ in outcomes), outcomes
                if records == 4000:
                    assert any(code == 503 and body.get("retryable") for code, body in outcomes), outcomes
                for _ in range(120):
                    _, raw, _ = http(self.args.url, "/metrics", timeout=5, text=True)
                    if metrics(raw).get("antfly_inference_admission_backend_kv_bytes", 0) == 0:
                        break
                    time.sleep(.1)
                else:
                    raise AssertionError("KV admission did not recover after caller deadlines")
                self.set_phase(self.phase + "-recovered")
                self.request("embed", self.embed())
                self.request("decide", self.decide())
                self.save()
            self.set_phase("retained-cache-fill")
            for index in range(5):
                self.request("generate", self.generate(64, 8, f"pressure-retained-{index}"))
            _, raw, _ = http(self.args.url, "/metrics", text=True)
            assert metrics(raw).get("antfly_inference_prompt_cache_live_bytes", 0) <= 8 * (1 << 30)
            self.set_phase("retained-cache-recovered")
            for _ in range(self.args.probes):
                self.request("embed", self.embed())
                self.request("decide", self.decide())
            self.request("generate", self.generate(4, 16))
            _, inventory, _ = http(self.args.url, "/models/instances")
            self.instances_after = inventory["data"]
            assert {item["id"] for item in self.instances_before} == {item["id"] for item in self.instances_after}
            self.require_memory_evidence()
            self.status = "passed"
        except BaseException as error:
            self.status = "incomplete" if isinstance(error, EvidenceUnavailable) else "failed"
            self.error = str(error)
            raise
        finally:
            self.stop_monitor(thread)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18840")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cgroup", type=Path)
    parser.add_argument("--records", type=int, nargs="+", default=[64, 512, 2048])
    parser.add_argument("--output-tokens", type=int, default=256)
    parser.add_argument("--probes", type=int, default=12)
    parser.add_argument("--generation-timeout", type=int, default=300)
    parser.add_argument("--generation-concurrency", type=int, choices=range(1, 5), default=2)
    parser.add_argument("--pressure", action="store_true", help="Check KV reservation, deadline cleanup and retryable saturation; does not qualify long-context outputs")
    parser.add_argument("--pressure-records", type=int, nargs="+", default=[256, 1024, 4000], help="Pressure prompt sizes; use 1024 for the bounded 32K serving config")
    parser.add_argument("--skip-retained", action="store_true", help="Focus on concurrent generation latency without the separate retained-cache fill")
    parser.add_argument("--stream-generations", action="store_true", help="Also measure client time to first content using streamed responses")
    parser.add_argument("--disable-thinking", action="store_true", help="Measure an explicit thinking-off request policy separately from the default")
    args = parser.parse_args()
    campaign = Campaign(args)
    campaign.pressure() if args.pressure else campaign.run()


if __name__ == "__main__":
    main()
