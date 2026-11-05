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

"""Compare serialized and cooperative one-row serving on the same owned pool.

Run each mode against the same binary and model inventory, then --compare both
output directories. Gates are fixed here before measurement. GPU sharing remains
part of the recorded environment; these results never qualify a different GPU.
"""

import argparse
import hashlib
from qualification_identity import (
    model_artifacts,
    require_matching_artifacts,
    sha256_file,
    verify_artifacts,
)
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import statistics
import threading
import time
import qualify_read_pool_40g as q


def serving_identity(args, campaign, *, check_cooperative_mode=True):
    if not args.cgroup or not args.binary or not args.config:
        raise q.EvidenceUnavailable(
            "controlled qualification requires --cgroup, --binary and --config"
        )
    digest = sha256_file(args.binary)["sha256"]
    expected_port = str(urllib.parse.urlsplit(args.url).port)
    owners = []
    working_directory = None
    environment = {}
    keys = {
        "ANTFLY_INFERENCE_REQUIRED_BACKEND",
        "ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE",
        "ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY",
        "ANTFLY_SERVER_DECODE_GRAPH_REPLAY",
        "ANTFLY_INFERENCE_CUDA_GREEDY_DEVICE_TOKEN_HANDOFF",
    }
    for pid in (args.cgroup / "cgroup.procs").read_text().split():
        proc = Path("/proc") / pid
        if hashlib.sha256((proc / "exe").read_bytes()).hexdigest() != digest:
            continue
        argv = [x.decode() for x in (proc / "cmdline").read_bytes().split(b"\0") if x]
        assert (
            "--config" in argv
            and Path(argv[argv.index("--config") + 1]).resolve()
            == args.config.resolve()
        )
        assert "--port" in argv and argv[argv.index("--port") + 1] == expected_port
        values = dict(
            x.decode().split("=", 1)
            for x in (proc / "environ").read_bytes().split(b"\0")
            if b"=" in x
        )
        keys.update(
            k for k in values
            if k.startswith(("ANTFLY_INFERENCE_CUDA_", "ANTFLY_LIGHTONOCR3_", "TERMITE_WHISPER_"))
        )
        selected = {k: values.get(k) for k in sorted(keys)}
        assert not environment or selected == environment
        environment = selected
        owners.append(int(pid))
        cwd = (proc / "cwd").resolve(strict=True)
        if working_directory is not None and cwd != working_directory:
            raise q.EvidenceUnavailable(
                "serving workers have different working directories"
            )
        working_directory = cwd
    if not owners:
        raise q.EvidenceUnavailable(
            "expected serving binary is absent from the requested service cgroup"
        )
    configuration = json.loads(args.config.read_text())
    if check_cooperative_mode:
        expected = "on" if args.mode == "cooperative" else "off"
        assert configuration["inference"]["cooperative_execution"]["mode"] == expected
    gpu = campaign.gpu
    if gpu is None:
        raise q.EvidenceUnavailable(campaign.gpu_error)
    identity = {}
    for name, symbol in [("name", "nvmlDeviceGetName"), ("uuid", "nvmlDeviceGetUUID")]:
        value = q.ctypes.create_string_buffer(128)
        if (
            getattr(gpu.lib, symbol)(gpu.handle, value, q.ctypes.c_uint(len(value)))
            != 0
        ):
            raise q.EvidenceUnavailable("NVML GPU identity unavailable")
        identity[name] = value.value.decode()
    return {
        "binary_sha256": digest,
        "configuration": configuration,
        "gpu": identity,
        "runtime_environment": environment,
        "owned_pids": owners,
        "working_directory": str(working_directory),
        "model_artifacts": model_artifacts(configuration, args.url, working_directory),
    }


def prompt(records, tokens, marker, subject):
    body = q.Campaign.generate(records, tokens)
    records_text = body["messages"][0]["content"].replace("warehouse", subject)
    body["messages"][0]["content"] = (
        f"Begin your answer with the exact code {marker}. Then describe these {subject} records.\n"
        + records_text
        + f"\nBegin with {marker}. Use only this request’s code."
    )
    return body


def compare(serialized, cooperative, output):
    a = json.loads((serialized / "comparison.json").read_text())
    b = json.loads((cooperative / "comparison.json").read_text())
    assert a["mode"] == "serialized" and b["mode"] == "cooperative"
    assert a["workload"] == b["workload"] and a["references"] == b["references"], (
        "workload or output reference differs"
    )
    assert a["status"] == b["status"] == "passed"
    require_matching_artifacts(a["identity"], b["identity"])
    for field in ("binary_sha256", "gpu", "runtime_environment"):
        assert a["identity"][field] == b["identity"][field], (
            f"controlled identity differs: {field}"
        )
    configs = [json.loads(json.dumps(x["identity"]["configuration"])) for x in (a, b)]
    for cfg in configs:
        cfg["inference"]["cooperative_execution"].pop("mode")
    assert configs[0] == configs[1], (
        "configuration differs beyond the cooperative opt-in"
    )

    gates = {}
    assert a["pairs"] and a["pairs"].keys() == b["pairs"].keys(), (
        "response-mode coverage differs"
    )
    for stream in a["pairs"]:
        x, y = a["pairs"][stream], b["pairs"][stream]
        gates[stream + "_short_completion"] = {
            "ratio": y["short_seconds"] / x["short_seconds"],
            "maximum": 0.75,
        }
        gates[stream + "_long_slowdown"] = {
            "ratio": y["long_seconds"] / x["long_seconds"],
            "maximum": 1.35,
        }
        gates[stream + "_aggregate_throughput"] = {
            "ratio": x["wall_seconds"] / y["wall_seconds"],
            "minimum": 0.90,
        }
    gates["short_first_content"] = {
        "ratio": b["pairs"]["stream"]["short_first_content_seconds"]
        / a["pairs"]["stream"]["short_first_content_seconds"],
        "maximum": 0.75,
    }
    for kind in ("embed", "decide"):
        x, y = a["probes"][kind]["p95_ms"], b["probes"][kind]["p95_ms"]
        assert x is not None and y is not None
        gates[kind + "_p95_relative"] = {"ratio": y / x, "maximum": 2.0}
        gates[kind + "_p95_absolute_ms"] = {"value": y, "maximum": 1000}
    for gate in gates.values():
        value = gate.get("ratio", gate.get("value"))
        gate["passed"] = (
            value <= gate["maximum"] if "maximum" in gate else value >= gate["minimum"]
        )
    result = {
        "status": "passed" if all(g["passed"] for g in gates.values()) else "failed",
        "gates": gates,
        "scope": "controlled GB10 serving experiment; no A100 hardware or production SLO qualification",
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "performance-gates.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)
    assert result["status"] == "passed", (
        "cooperative performance acceptance criteria failed"
    )


def compare_batching(single, batched, output):
    a, b = (
        json.loads((path / "comparison.json").read_text()) for path in (single, batched)
    )
    assert a["status"] == b["status"] == "passed"
    assert a["mode"] == b["mode"] == "cooperative" and a["rows"] == 1 and b["rows"] == 2
    assert a["workload"] == b["workload"] and a["references"] == b["references"]
    require_matching_artifacts(a["identity"], b["identity"])
    for field in ("binary_sha256", "gpu", "runtime_environment"):
        assert a["identity"][field] == b["identity"][field], field
    configs = [json.loads(json.dumps(x["identity"]["configuration"])) for x in (a, b)]
    for cfg in configs:
        for field in ("mode", "max_step_items"):
            cfg["inference"]["generation_batching"].pop(field)
    assert configs[0] == configs[1], "configuration differs beyond row batching"
    gates = {}
    for mode in a["pairs"]:
        x, y = a["pairs"][mode], b["pairs"][mode]
        gates[mode + "_throughput"] = {
            "ratio": x["wall_seconds"] / y["wall_seconds"],
            "minimum": 1.10,
        }
        for request in ("short", "long"):
            gates[mode + "_" + request + "_completion"] = {
                "ratio": y[request + "_seconds"] / x[request + "_seconds"],
                "maximum": 1.25,
            }
    gates["short_first_content"] = {
        "ratio": b["pairs"]["stream"]["short_first_content_seconds"]
        / a["pairs"]["stream"]["short_first_content_seconds"],
        "maximum": 1.25,
    }
    for kind in ("embed", "decide"):
        x, y = a["probes"][kind]["p95_ms"], b["probes"][kind]["p95_ms"]
        assert x is not None and y is not None
        gates[kind + "_p95_relative"] = {"ratio": y / x, "maximum": 2.0}
        gates[kind + "_p95_absolute_ms"] = {"value": y, "maximum": 1000}
    for gate in gates.values():
        value = gate.get("ratio", gate.get("value"))
        gate["passed"] = (
            value >= gate["minimum"] if "minimum" in gate else value <= gate["maximum"]
        )
    result = {
        "status": "passed" if all(g["passed"] for g in gates.values()) else "failed",
        "gates": gates,
        "scope": "GB10 two-row experiment; no A100 or production SLO claim",
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "batching-gates.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)
    assert result["status"] == "passed", "row-batching acceptance criteria failed"


def batch_peer_disconnect(c, args, healthy_body, cancelled_body, reference):
    """Retire one streaming row after real shared steps; its peer must finish."""
    c.set_phase("batch-peer-disconnect")
    code, raw, _ = q.http(args.url, "/metrics", text=True)
    assert code == 200
    before = q.metrics(raw)["antfly_inference_native_scheduler_step_batch_size_2_total"]
    body = {
        **cancelled_body,
        "max_tokens": 1024,
        "stream": True,
        "stream_options": {"include_usage": True},
        "chat_template_kwargs": {"enable_thinking": False},
    }
    request = q.urllib.request.Request(
        args.url + "/ai/v1/generate",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "X-Antfly-Inference-Timeout-Ms": str(args.generation_timeout * 1000),
        },
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        with q.urllib.request.urlopen(
            request, timeout=args.generation_timeout
        ) as response:
            for line in response:
                if not line.startswith(b"data:"):
                    continue
                payload = line[5:].strip()
                assert payload != b"[DONE]", (
                    "cancel target finished before first content"
                )
                event = json.loads(payload)
                assert "error" not in event, event
                if any(
                    choice.get("delta", {}).get("content")
                    for choice in event.get("choices", [])
                ):
                    break
            else:
                raise AssertionError("cancel target produced no content")
            survivor = pool.submit(c.request, "generate", healthy_body)
            deadline = time.monotonic() + 30
            while True:
                code, raw, _ = q.http(args.url, "/metrics", text=True)
                assert code == 200
                observed = q.metrics(raw)
                if (
                    observed["antfly_inference_native_scheduler_decode_requests"] >= 2
                    and observed[
                        "antfly_inference_native_scheduler_step_batch_size_2_total"
                    ]
                    > before
                ):
                    break
                assert not survivor.done() and time.monotonic() < deadline, (
                    "disconnect target never shared a GPU step with its peer"
                )
                time.sleep(0.01)
        result = survivor.result()
    assert result["response"]["choices"] == reference, (
        "peer output changed after cohort cancellation"
    )
    deadline = time.monotonic() + 30
    while True:
        code, raw, _ = q.http(args.url, "/metrics", text=True)
        assert code == 200
        current = q.metrics(raw)
        if (
            current["antfly_inference_generation_active_requests"] == 0
            and current["antfly_inference_admission_backend_kv_bytes"] == 0
        ):
            break
        assert time.monotonic() < deadline, "cancelled cohort did not retire"
        time.sleep(0.05)
    return {
        "two_row_steps_before_disconnect": observed[
            "antfly_inference_native_scheduler_step_batch_size_2_total"
        ]
        - before,
        "healthy_peer_matches_reference": True,
        "active_and_kv_recovered": True,
    }


def run(args):
    args.pressure = False
    args.stream_generations = False
    args.disable_thinking = True
    if args.scenario == "decode-overlap":
        if not args.references:
            raise q.EvidenceUnavailable(
                "decode-overlap requires references from a passing mixed-context campaign"
            )
        args.turnover_rounds = 0
    c = q.Campaign(args)
    long = prompt(
        args.long_records, args.long_output, "cobalt-lantern-7319", "warehouse"
    )
    short = prompt(args.short_records, args.short_output, "tulip-harbor-9462", "garden")
    long["temperature"] = short["temperature"] = args.temperature
    workload = {
        "long": long,
        "short": short,
        "probe_interval": args.probe_interval,
        "turnover_rounds": args.turnover_rounds,
    }
    if args.scenario == "decode-overlap":
        workload["scenario"] = "decode-overlap"
    references = json.loads(args.references.read_text()) if args.references else {}
    pairs = {}
    identity = None
    batch_recovery = {}
    monitor = threading.Thread(target=c.monitor, daemon=True)
    monitor.start()

    def validated(body, label):
        row = c.request("generate", body)
        text = row["response"]["choices"][0]["message"]["content"]
        marker = "cobalt-lantern-7319" if label == "long" else "tulip-harbor-9462"
        other = "tulip-harbor-9462" if label == "long" else "cobalt-lantern-7319"
        assert marker in text and other not in text, "request identity contaminated"
        assert row["response"]["choices"] == references[label], (
            label,
            "output differs from independent serialized reference",
        )
        if label == "short":
            status, raw, _ = q.http(args.url, "/metrics", text=True, timeout=5)
            assert status == 200
            row["scheduler_at_completion"] = q.metrics(raw)
        return row

    try:
        identity = serving_identity(args, c)
        code, inventory, _ = q.http(args.url, "/models/instances")
        assert code == 200 and {i["model"] for i in inventory["data"]} == {
            q.GEMMA,
            q.EMBED,
            q.DECIDE,
        }
        assert all(
            i["backend"] == "cuda" and i["startup_managed"] for i in inventory["data"]
        )
        c.instances_before = inventory["data"]
        code, raw, _ = q.http(args.url, "/metrics", text=True, timeout=5)
        assert code == 200
        declared = q.metrics(raw)
        assert (
            declared["antfly_inference_generation_cooperative_capable_models"] == 1
        ), "E4B backend did not declare safe sharing"
        assert declared["antfly_inference_generation_cooperative_configured"] == (
            1 if args.mode == "cooperative" else 0
        )
        assert declared["antfly_inference_generation_row_batching_configured"] == (
            1 if args.rows > 1 else 0
        ), "row batching configuration differs from requested envelope"
        c.set_phase("warmup")
        c.request("embed", c.embed())
        c.request("decide", c.decide())
        c.set_phase("sharing-preflight")
        code, raw, _ = q.http(args.url, "/metrics", text=True, timeout=5)
        assert code == 200
        before_preflight = q.metrics(raw)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(c.request, "generate", q.Campaign.generate(16, 8))
                for _ in range(2)
            ]
            results = [future.result() for future in futures]
        assert results[0]["response"]["choices"] == results[1]["response"]["choices"], (
            "preflight output differs across identical requests"
        )
        code, raw, _ = q.http(args.url, "/metrics", text=True, timeout=5)
        assert code == 200
        entered = (
            q.metrics(raw)["antfly_inference_generation_owner_transitions_total"]
            - before_preflight["antfly_inference_generation_owner_transitions_total"]
        )
        assert entered > 0 if args.mode == "cooperative" else entered == 0, (
            "requested sharing mode never entered its backend ownership path"
        )
        if args.rows > 1:
            assert (
                q.metrics(raw)[
                    "antfly_inference_native_scheduler_step_batch_size_2_total"
                ]
                > before_preflight[
                    "antfly_inference_native_scheduler_step_batch_size_2_total"
                ]
            ), "preflight formed no real two-row steps"
        print("sharing-preflight", args.mode, "owner_transitions", entered, flush=True)
        if args.scenario == "decode-overlap":
            previous = json.loads(
                (args.references.parent / "comparison.json").read_text()
            )
            require_matching_artifacts(previous["identity"], identity)
            assert (
                previous["status"] == "passed"
                and previous["identity"]["binary_sha256"] == identity["binary_sha256"]
                and previous["identity"]["gpu"] == identity["gpu"]
            ), "references do not belong to a passing campaign on this binary/GPU"
        else:
            c.set_phase("independent-references")
            for label, body in (("long", long), ("short", short)):
                choices = c.request("generate", body)["response"]["choices"]
                if label in references:
                    assert choices == references[label], "independent reference differs"
                else:
                    references[label] = choices
        (args.output / "references.json").write_text(
            json.dumps(references, indent=2) + "\n"
        )
        for streaming in (
            (True,) if args.scenario == "decode-overlap" else (True, False)
        ):
            label = "stream" if streaming else "buffered"
            c.set_phase("mixed-" + label)
            args.stream_generations = streaming
            status, raw, _ = q.http(args.url, "/metrics", text=True, timeout=5)
            assert status == 200
            before = q.metrics(raw)
            with ThreadPoolExecutor(max_workers=4) as pool:
                started = time.monotonic()
                a = pool.submit(validated, long, "long")
                # Establish the long owner before the short arrival in both modes.
                deadline = time.monotonic() + (
                    args.generation_timeout if args.scenario == "decode-overlap" else 15
                )
                while True:
                    status, raw, _ = q.http(args.url, "/metrics", text=True, timeout=5)
                    assert status == 200
                    ready = q.metrics(raw)
                    if (
                        ready["antfly_inference_native_scheduler_decode_requests"] >= 1
                        if args.scenario == "decode-overlap"
                        else ready["antfly_inference_generation_active_requests"] >= 1
                    ):
                        break
                    assert time.monotonic() < deadline
                    if args.scenario == "decode-overlap":
                        for probe in (
                            pool.submit(c.request, "embed", c.embed()),
                            pool.submit(c.request, "decide", c.decide()),
                        ):
                            probe.result()
                    time.sleep(
                        args.probe_interval
                        if args.scenario == "decode-overlap"
                        else 0.025
                    )
                short_arrival = time.monotonic() - started
                b = pool.submit(validated, short, "short")
                while not (a.done() and b.done()):
                    probes = [
                        pool.submit(c.request, "embed", c.embed()),
                        pool.submit(c.request, "decide", c.decide()),
                    ]
                    for f in probes:
                        f.result()
                    time.sleep(args.probe_interval)
                x, y = a.result(), b.result()
                wall = time.monotonic() - started
            pairs[label] = {
                "long_seconds": x["seconds"],
                "short_seconds": y["seconds"],
                "short_arrival_seconds": short_arrival,
                "wall_seconds": wall,
                "output_tokens_per_second": (args.long_output + args.short_output)
                / wall,
                "long_content_event_seconds": x.get("content_event_seconds"),
                "long_max_content_gap_seconds": x.get("max_content_gap_seconds"),
                "short_first_content_seconds": y.get("first_content_seconds"),
            }
            progress = (
                y["scheduler_at_completion"][
                    "antfly_inference_native_scheduler_step_query_tokens_total"
                ]
                - before["antfly_inference_native_scheduler_step_query_tokens_total"]
            )
            pairs[label]["scheduled_query_tokens_at_short_completion"] = progress
            pairs[label]["short_prompt_tokens"] = y["response"]["usage"][
                "prompt_tokens"
            ]
            if args.mode == "cooperative":
                assert y["completed_at"] < x["completed_at"], (
                    "short request did not complete before long request"
                )
                if args.scenario == "decode-overlap":
                    decode_progress = (
                        y["scheduler_at_completion"][
                            "antfly_inference_native_scheduler_step_decode_items_total"
                        ]
                        - ready[
                            "antfly_inference_native_scheduler_step_decode_items_total"
                        ]
                    )
                    assert decode_progress > args.short_output, (
                        "no scheduled long-request decoding during short request"
                    )
                    both_decode = any(
                        s["phase"] == "mixed-" + label
                        and s["metrics"][
                            "antfly_inference_native_scheduler_decode_requests"
                        ]
                        >= 2
                        for s in c.samples
                    )
                    assert both_decode, "concurrent decoding was not observed"
                    pairs[label]["scheduled_decode_items_during_short"] = (
                        decode_progress
                    )
                    pairs[label]["both_decode_observed"] = both_decode
                else:
                    assert (
                        progress
                        > y["response"]["usage"]["prompt_tokens"] + args.short_output
                    ), "no long-request GPU progress while short request ran"
                if args.rows == 1:
                    assert (
                        y["scheduler_at_completion"][
                            "antfly_inference_native_scheduler_step_batch_size_2_total"
                        ]
                        == before[
                            "antfly_inference_native_scheduler_step_batch_size_2_total"
                        ]
                    ), "cooperative row-1 envelope widened"
                elif args.scenario == "decode-overlap":
                    assert (
                        y["scheduler_at_completion"][
                            "antfly_inference_native_scheduler_step_batch_size_2_total"
                        ]
                        > before[
                            "antfly_inference_native_scheduler_step_batch_size_2_total"
                        ]
                    ), "two-row decode execution was never observed"
            pairs[label]["execution_owner_counters"] = {
                k: v - before[k]
                for k, v in y["scheduler_at_completion"].items()
                if k.startswith("antfly_inference_generation_owner_")
            }
            print(label, json.dumps(pairs[label]), flush=True)
        args.stream_generations = False
        if args.rows > 1:
            batch_recovery = batch_peer_disconnect(
                c, args, short, short, references["short"]
            )
        if args.turnover_rounds:
            c.set_phase("ownership-references")
            bodies = {
                "turnover_large": q.Campaign.generate(256, 8),
                "turnover_small": q.Campaign.generate(4, 8),
                "turnover_cache": q.Campaign.generate(
                    8, 8, "cooperative-ownership-turnover"
                ),
            }
            for key, body in bodies.items():
                choices = c.request("generate", body)["response"]["choices"]
                if key in references:
                    assert choices == references[key]
                else:
                    references[key] = choices

            def turnover_request(key):
                row = c.request("generate", bodies[key])
                assert row["response"]["choices"] == references[key], (
                    "output changed during queue turnover"
                )
                return row

            for round_index in range(args.turnover_rounds):
                c.set_phase(f"ownership-turnover-{round_index}")
                with ThreadPoolExecutor(max_workers=8) as pool:
                    active = [
                        pool.submit(turnover_request, "turnover_large")
                        for _ in range(2)
                    ]
                    deadline = time.monotonic() + 15
                    while True:
                        code, raw, _ = q.http(
                            args.url, "/metrics", text=True, timeout=5
                        )
                        assert code == 200
                        expected = 2 if args.mode == "cooperative" else 1
                        if (
                            q.metrics(raw)[
                                "antfly_inference_generation_active_requests"
                            ]
                            >= expected
                        ):
                            break
                        assert time.monotonic() < deadline, (
                            "active turnover owners did not start"
                        )
                        time.sleep(0.025)
                    queued = [
                        pool.submit(turnover_request, "turnover_small")
                        for _ in range(4)
                    ]
                    exclusive = pool.submit(turnover_request, "turnover_cache")
                    for future in active + queued + [exclusive]:
                        future.result()
                print(
                    "ownership-turnover",
                    round_index,
                    "7 completed requests including exclusive cache route",
                    flush=True,
                )
            (args.output / "references.json").write_text(
                json.dumps(references, indent=2) + "\n"
            )
        c.set_phase("recovery")
        c.request("embed", c.embed())
        c.request("decide", c.decide())
        status, raw, _ = q.http(args.url, "/metrics", text=True, timeout=5)
        assert status == 200
        final = q.metrics(raw)
        assert (
            final["antfly_inference_generation_active_requests"]
            == final["antfly_inference_generation_queued_requests"]
            == 0
        )
        assert (
            final["antfly_inference_admission_backend_kv_bytes"]
            == final["antfly_inference_admission_backend_scratch_bytes"]
            == 0
        )
        code, inventory, _ = q.http(args.url, "/models/instances")
        c.instances_after = inventory["data"]
        assert code == 200 and {i["id"] for i in c.instances_before} == {
            i["id"] for i in c.instances_after
        }
        c.require_memory_evidence()
        verify_artifacts(identity, args)
        c.status = "passed"
    except BaseException as error:
        c.status = (
            "incomplete" if isinstance(error, q.EvidenceUnavailable) else "failed"
        )
        c.error = str(error)
        raise
    finally:
        try:
            c.stop_monitor(monitor)
        finally:
            probes = {
                kind: q.summary(
                    [
                        r
                        for r in c.rows
                        if r["kind"] == kind and r["phase"].startswith("mixed-")
                    ]
                )
                for kind in ("embed", "decide")
            }
            result = {
                "status": c.status,
                "error": c.error,
                "mode": args.mode,
                "rows": args.rows,
                "workload": workload,
                "references": references,
                "pairs": pairs,
                "probes": probes,
                "identity": identity,
                "batch_recovery": batch_recovery,
            }
            (args.output / "comparison.json").write_text(
                json.dumps(result, indent=2) + "\n"
            )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default="http://127.0.0.1:18840")
    p.add_argument("--mode", choices=("serialized", "cooperative"))
    p.add_argument("--rows", type=int, choices=(1, 2), default=1)
    p.add_argument(
        "--scenario",
        choices=("mixed-context", "decode-overlap"),
        default="mixed-context",
    )
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--cgroup", type=Path)
    p.add_argument("--references", type=Path)
    p.add_argument("--binary", type=Path)
    p.add_argument("--config", type=Path)
    p.add_argument("--temperature", type=float, default=0)
    p.add_argument("--long-records", type=int, default=1024)
    p.add_argument("--short-records", type=int, default=64)
    p.add_argument("--long-output", type=int, default=64)
    p.add_argument("--short-output", type=int, default=32)
    p.add_argument("--generation-timeout", type=int, default=600)
    p.add_argument("--probe-interval", type=float, default=0.25)
    p.add_argument("--turnover-rounds", type=int, default=2)
    p.add_argument(
        "--compare-batching", type=Path, nargs=2, metavar=("SINGLE_ROW", "TWO_ROWS")
    )
    p.add_argument(
        "--compare", type=Path, nargs=2, metavar=("SERIALIZED", "COOPERATIVE")
    )
    args = p.parse_args()
    if args.compare_batching:
        compare_batching(*args.compare_batching, args.output)
    elif args.compare:
        compare(*args.compare, args.output)
    else:
        p.error("--mode is required for a serving campaign") if not args.mode else run(
            args
        )


if __name__ == "__main__":
    main()
