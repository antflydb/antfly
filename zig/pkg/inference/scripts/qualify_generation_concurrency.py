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

"""Qualify mixed-context scheduling, bounded queue memory and disconnect cleanup."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
import time
import urllib.request

import qualify_read_pool_40g as q


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18840")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cgroup", type=Path)
    parser.add_argument("--references", type=Path)
    parser.add_argument("--long-records", type=int, default=1024)
    parser.add_argument("--long-output", type=int, default=128)
    parser.add_argument("--short-records", type=int, default=64)
    parser.add_argument("--short-output", type=int, default=128)
    parser.add_argument("--queue-probes", type=int, default=20)
    parser.add_argument("--queue-records", type=int, default=256)
    parser.add_argument("--disconnect", action="store_true")
    parser.add_argument("--generation-timeout", type=int, default=300)
    parser.add_argument("--disable-thinking", action="store_true")
    parser.add_argument(
        "--embedding-batch-size", type=int, choices=(1, 4, 8), default=1
    )
    parser.add_argument(
        "--probe-context-sentences", type=int, choices=range(0, 9), default=0
    )
    args = parser.parse_args()
    args.pressure = False
    assert 1 <= args.generation_timeout <= 3600
    args.stream_generations = False
    c = q.Campaign(args)
    embed, decide = c.embed, c.decide
    background = (
        " Warehouse shelving and shipping records are unrelated background context."
        * args.probe_context_sentences
    )
    c.embed = lambda: {
        **embed(),
        "input": [embed()["input"][0] + background] * args.embedding_batch_size,
    }
    c.decide = lambda: {**decide(), "input": decide()["input"] + background}
    reference = {}
    key = lambda body: json.dumps(body, sort_keys=True)
    if args.references:
        for line in args.references.read_text().splitlines():
            row = json.loads(line)
            if row["kind"] == "generate" and row["status"] == 200 and "request" in row:
                reference[key(row["request"])] = row["response"]["choices"]
    details = {}

    def read_metrics():
        code, raw, _ = q.http(args.url, "/metrics", text=True, timeout=5)
        assert code == 200
        return q.metrics(raw)

    def wait_for(predicate, seconds=15):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            m = read_metrics()
            if predicate(m):
                return m
            time.sleep(0.05)
        raise AssertionError("scheduler condition did not become ready")

    def probe_until(pool, futures):
        while any(not f.done() for f in futures):
            probes = [
                pool.submit(c.request, "embed", c.embed()),
                pool.submit(c.request, "decide", c.decide()),
            ]
            for probe in probes:
                probe.result()
            time.sleep(0.1)
        return [f.result() for f in futures]

    def expected(body):
        if key(body) not in reference:
            reference[key(body)] = c.request("generate", body)["response"]["choices"]
        return reference[key(body)]

    thread = threading.Thread(target=c.monitor, daemon=True)
    thread.start()
    try:
        code, inventory, _ = q.http(args.url, "/models/instances")
        assert code == 200 and {i["model"] for i in inventory["data"]} == {
            q.GEMMA,
            q.DECIDE,
            q.EMBED,
        }
        assert all(
            i["backend"] == "cuda" and i["startup_managed"] for i in inventory["data"]
        )
        c.instances_before = inventory["data"]
        m = read_metrics()
        assert m["antfly_inference_cuda_managed_limit_bytes"] == 38 * (1 << 30)
        assert "antfly_inference_generation_queued_requests" in m, (
            "new generation admission is not running"
        )
        c.set_phase("baseline")
        for _ in range(12):
            c.request("embed", c.embed())
            c.request("decide", c.decide())
        long_body = c.generate(args.long_records, args.long_output)
        short_body = c.generate(args.short_records, args.short_output)
        long_reference, short_reference = expected(long_body), expected(short_body)

        c.set_phase("mixed-contexts")
        print(c.phase, flush=True)
        with ThreadPoolExecutor(max_workers=4) as pool:
            long = pool.submit(c.request, "generate", long_body)
            wait_for(
                lambda m: (
                    m.get("antfly_inference_native_scheduler_prefill_requests", 0) > 0
                )
            )
            short = pool.submit(c.request, "generate", short_body)
            long_row, short_row = probe_until(pool, [long, short])
        assert long_row["response"]["choices"] == long_reference, (
            "long generation differs from reference"
        )
        assert short_row["response"]["choices"] == short_reference, (
            "short generation differs from reference"
        )
        assert short_row["completed_at"] < long_row["completed_at"], (
            "short request waited for the long request to finish"
        )
        details["mixed"] = {
            "long_seconds": long_row["seconds"],
            "short_seconds": short_row["seconds"],
            "short_finished_first": True,
            "both_reference_equal": True,
        }
        print(details["mixed"], flush=True)

        if args.queue_probes:
            c.set_phase("queue-memory-and-cancellation")
            print(c.phase, flush=True)
            body = c.generate(args.queue_records, 32)
            with ThreadPoolExecutor(max_workers=24) as pool:
                active = [pool.submit(c.request, "generate", body) for _ in range(2)]
                wait_for(
                    lambda m: (
                        m.get("antfly_inference_generation_active_requests") == 2
                        and m.get("antfly_inference_admission_backend_kv_bytes", 0) > 0
                    )
                )
                # Allow both active owners to finish resource admission before
                # measuring the queue: slot acquisition deliberately comes first.
                time.sleep(0.3)
                before = read_metrics()
                kv_before = before["antfly_inference_admission_backend_kv_bytes"]

                def queued(index):
                    request = (
                        c.generate(64, 8, "exclusive-queued")
                        if index == 0
                        else c.generate(args.long_records, 16)
                    )
                    code, response, elapsed = q.http(
                        args.url, "/generate", request, timeout=15, deadline_ms=1500
                    )
                    row = {
                        "phase": c.phase,
                        "kind": "generate",
                        "status": code,
                        "seconds": elapsed,
                        "request": request,
                        "response": response,
                        "intentional_deadline_ms": 1500,
                    }
                    with c.lock:
                        c.rows.append(row)
                        with (args.output / "requests.jsonl").open("a") as out:
                            out.write(json.dumps(row) + "\n")
                    assert code in (503, 504) and response.get("retryable"), row
                    return row

                queued_work = [pool.submit(queued, i) for i in range(args.queue_probes)]
                peak_kv = kv_before
                peak_waiting = 0
                while any(not f.done() for f in queued_work):
                    m = read_metrics()
                    peak_kv = max(
                        peak_kv, m["antfly_inference_admission_backend_kv_bytes"]
                    )
                    peak_waiting = max(
                        peak_waiting, m["antfly_inference_generation_queued_requests"]
                    )
                    time.sleep(0.05)
                outcomes = [f.result() for f in queued_work]
                assert peak_kv <= kv_before, (kv_before, peak_kv)
                assert peak_waiting > 0, "requests never exercised the queue"
                wait_for(
                    lambda m: m["antfly_inference_generation_queued_requests"] == 0
                )
                a, b = probe_until(pool, active)
                assert a["response"]["choices"] == b["response"]["choices"]
            assert read_metrics()["antfly_inference_admission_backend_kv_bytes"] == 0
            details["queue"] = {
                "waiting_peak": peak_waiting,
                "kv_before_bytes": kv_before,
                "kv_peak_with_waiters_bytes": peak_kv,
                "statuses": {
                    str(code): sum(r["status"] == code for r in outcomes)
                    for code in (503, 504)
                },
                "active_outputs_equal": True,
            }
            print(details["queue"], flush=True)
            c.set_phase("exclusive-cache-recovery")
            c.request("generate", c.generate(64, 8, "exclusive-recovered"))
            row = c.request("generate", short_body)
            assert row["response"]["choices"] == short_reference

        if args.disconnect:
            c.set_phase("client-disconnect")
            print(c.phase, flush=True)
            body = {
                **c.generate(64, 1024),
                "stream": True,
                "stream_options": {"include_usage": True},
                "chat_template_kwargs": {"enable_thinking": False},
            }
            request = urllib.request.Request(
                args.url + "/ai/v1/generate",
                data=json.dumps(body).encode(),
                headers={
                    "Content-Type": "application/json",
                    "X-Antfly-Inference-Timeout-Ms": "60000",
                },
            )
            with urllib.request.urlopen(request, timeout=90) as response:
                for line in response:
                    if line.startswith(b"data:") and line[5:].strip() != b"[DONE]":
                        event = json.loads(line[5:])
                        if any(
                            choice.get("delta", {}).get("content")
                            for choice in event.get("choices", [])
                        ):
                            break
                else:
                    raise AssertionError("stream ended before first content")
            wait_for(
                lambda m: (
                    m["antfly_inference_generation_active_requests"] == 0
                    and m["antfly_inference_admission_backend_kv_bytes"] == 0
                ),
                seconds=15,
            )
            details["disconnect"] = {
                "cancelled_after_first_content": True,
                "active_and_kv_recovered": True,
            }

        c.set_phase("recovery")
        for _ in range(12):
            c.request("embed", c.embed())
            c.request("decide", c.decide())
        row = c.request("generate", short_body)
        assert row["response"]["choices"] == short_reference
        code, inventory, _ = q.http(args.url, "/models/instances")
        c.instances_after = inventory["data"]
        assert code == 200 and {i["id"] for i in c.instances_before} == {
            i["id"] for i in c.instances_after
        }
        m = read_metrics()
        assert (
            m["antfly_inference_generation_active_requests"] == 0
            and m["antfly_inference_generation_queued_requests"] == 0
        )
        assert (
            m["antfly_inference_admission_backend_kv_bytes"] == 0
            and m["antfly_inference_admission_backend_scratch_bytes"] == 0
        )
        c.require_memory_evidence()
        c.status = "passed"
    except BaseException as error:
        c.status, c.error = (
            ("incomplete" if isinstance(error, q.EvidenceUnavailable) else "failed"),
            str(error),
        )
        raise
    finally:
        c.stop_monitor(thread)
        path = args.output / "report.json"
        report = json.loads(path.read_text())
        report["validation_mode"] = "mixed contexts, queue memory and cancellation"
        report["checks"] = details
        path.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
