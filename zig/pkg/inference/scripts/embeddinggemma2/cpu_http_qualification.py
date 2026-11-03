#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualify native F32 text queries through the production HTTP server.

The load policy is an explicit reference-machine acceptance envelope, not a
customer latency SLO or evidence for another architecture/container limit.
"""

import argparse
import hashlib
import http.client
import json
import math
import os
import socket
import statistics
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import qualify
from video_deadline_qualification import declared_request

MAX_ABS = 1e-5
MAX_WORKER_RSS = 4 * 1024**3
MAX_RSS_GROWTH = 128 * 1024**2


def save(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def server_provenance(args):
    process = Path("/proc") / str(args.server_pid)
    return {
        "binary_sha256": digest(process / "exe"),
        "pid": args.server_pid,
        "command": (process / "cmdline").read_bytes().decode().strip("\0").split("\0"),
        "machine": os.uname().machine,
        "allowed_cpus": sorted(os.sched_getaffinity(args.server_pid)),
        "processes": {
            pid: {
                "binary_sha256": digest(Path("/proc") / pid / "exe"),
                "start_ticks": int(
                    (Path("/proc") / pid / "stat")
                    .read_text()
                    .rsplit(")", 1)[1]
                    .split()[19]
                ),
            }
            for pid in memory_tree(args.server_pid)
        },
    }


def payload(case, identity=None, dimensions=768):
    body = {
        "model": "embeddinggemma2",
        "input": [case["text"]],
        "task_type": case.get("task", "RETRIEVAL_QUERY"),
        "dimensions": dimensions,
    }
    if identity:
        body["model_identity"] = identity
    return body


def normalized_prefix(vector, dimensions):
    prefix = vector[:dimensions]
    norm = math.sqrt(sum(x * x for x in prefix))
    return [x / norm for x in prefix]


def validate(result, expected, identity=None):
    if result.get("backend") != "native":
        raise ValueError("qualification requires the native CPU backend")
    actual_identity = result["model_identity"]
    if len(actual_identity) != 64 or (identity and identity != actual_identity):
        raise ValueError("model identity changed")
    if len(result["data"]) != len(expected) or [
        r["index"] for r in result["data"]
    ] != list(range(len(expected))):
        raise ValueError("batch coverage/order changed")
    error = max(
        qualify.compare(row["embedding"], vector, limit=MAX_ABS)
        for row, vector in zip(result["data"], expected)
    )
    return actual_identity, error


def collect(args):
    oracle = json.loads(args.oracle.read_text())
    report = {
        "version": 1,
        "server": server_provenance(args),
        "status": "running",
        "oracle_sha256": digest(args.oracle),
        "policy": {"max_abs": MAX_ABS, "norm_error": 1e-4},
        "cases": {},
    }
    identity = None
    try:
        for name, case in oracle["cases"].items():
            response, elapsed = qualify.post(
                args.url + "/ai/v1", "/embed", payload(case, identity)
            )
            identity, difference = validate(response, [case["vector"]], identity)
            report["model_identity"] = identity
            report["cases"][name] = {
                "vector": response["data"][0]["embedding"],
                "max_abs": difference,
                "seconds": elapsed,
                "tokens": len(case["token_ids"]),
            }
        # A mixed-text batch must preserve row order; different task types are
        # separate requests because the public task_type belongs to the batch.
        queries = [
            c for c in oracle["cases"].values() if c["task"] == "RETRIEVAL_QUERY"
        ][:4]
        body = payload(queries[0], identity)
        body["input"] = [c["text"] for c in queries]
        response, _ = qualify.post(args.url + "/ai/v1", "/embed", body)
        validate(response, [c["vector"] for c in queries], identity)
        report["batch_rows"] = len(queries)
        case = queries[0]
        report["dimensions"] = {}
        for dim in [128, 256, 512, 768]:
            response, _ = qualify.post(
                args.url + "/ai/v1", "/embed", payload(case, identity, dim)
            )
            _, error = validate(
                response, [normalized_prefix(case["vector"], dim)], identity
            )
            report["dimensions"][str(dim)] = error
        report["max_abs"] = max(c["max_abs"] for c in report["cases"].values())
        report["status"] = "pass"
    except Exception as error:
        report["status"] = "fail"
        report["failure"] = repr(error)
        raise
    finally:
        save(args.output, report)
    print(
        json.dumps({k: report[k] for k in ("status", "model_identity", "max_abs")}),
        flush=True,
    )


def memory_tree(root):
    pending = [root]
    values = {}
    while pending:
        pid = pending.pop()
        directory = Path("/proc") / str(pid)
        fields = {}
        for line in (directory / "status").read_text().splitlines():
            key, _, value = line.partition(":")
            if key in {"VmRSS", "VmHWM", "RssAnon"}:
                fields[key] = int(value.split()[0]) * 1024
        values[str(pid)] = fields
        pending.extend(
            int(n)
            for n in (directory / "task" / str(pid) / "children").read_text().split()
        )
    return values


def percentile(values, percent):
    values = sorted(values)
    return values[math.ceil(percent * len(values)) - 1]


def load(args):
    oracle = json.loads(args.oracle.read_text())
    reference = json.loads(args.cpu_receipt.read_text())
    if reference.get("status") != "pass":
        raise ValueError("CPU serving qualification did not pass")
    identity = reference["model_identity"]
    cases = [
        (name, case)
        for name, case in oracle["cases"].items()
        if case["task"] == "RETRIEVAL_QUERY"
        and case["text"].strip()
        and 0 < len(case["token_ids"]) <= 64
    ]
    if not cases:
        raise ValueError("load needs short reference queries")
    report = {
        "version": 1,
        "server": server_provenance(args),
        "status": "running",
        "model_identity": identity,
        "oracle_sha256": digest(args.oracle),
        "cpu_receipt_sha256": digest(args.cpu_receipt),
        "policy": {
            "max_abs": MAX_ABS,
            "max_worker_rss_bytes": MAX_WORKER_RSS,
            "max_worker_rss_growth_bytes": MAX_RSS_GROWTH,
            "p95_seconds_per_concurrent_client": 0.25,
        },
        "profiles": [],
    }
    try:
        for clients, duration in [
            (1, args.profile_seconds),
            (2, args.profile_seconds),
            (4, args.profile_seconds),
            (8, args.profile_seconds),
            (4, args.soak_seconds),
        ]:
            before = cpu_metrics(args.url)
            if before["active"] or before["admitted"]:
                raise ValueError("load campaign requires idle owned server")
            memory_before = memory_tree(args.server_pid)
            worker_pid = max(memory_before, key=lambda p: memory_before[p]["VmRSS"])
            started = time.monotonic()
            stop = started + duration

            def client(offset, stop=stop, clients=clients):
                samples = []
                errors = []
                index = offset
                while time.monotonic() < stop:
                    name, case = cases[index % len(cases)]
                    index += clients
                    try:
                        response, elapsed = qualify.post(
                            args.url + "/ai/v1", "/embed", payload(case, identity)
                        )
                        validate(
                            response, [reference["cases"][name]["vector"]], identity
                        )
                        samples.append(elapsed)
                    except (
                        AssertionError,
                        ValueError,
                        OSError,
                        http.client.HTTPException,
                    ) as request_error:
                        errors.append(repr(request_error))
                        break
                return samples, errors

            with ThreadPoolExecutor(max_workers=clients) as pool:
                futures = [pool.submit(client, offset) for offset in range(clients)]
                memory_samples = [memory_before]
                while not all(f.done() for f in futures):
                    time.sleep(1)
                    memory_samples.append(memory_tree(args.server_pid))
                results = [future.result() for future in futures]
            elapsed = time.monotonic() - started
            samples = [sample for rows, _ in results for sample in rows]
            errors = [error for _, rows in results for error in rows]
            if not samples:
                raise ValueError("empty load profile")
            after = cpu_metrics(args.url)
            memory_after = memory_tree(args.server_pid)
            if any(
                set(sample) != set(memory_before)
                for sample in memory_samples + [memory_after]
            ):
                raise ValueError("worker process changed during load")
            peak = max(
                row[worker_pid]["VmRSS"] for row in memory_samples + [memory_after]
            )
            growth = (
                memory_after[worker_pid]["VmRSS"] - memory_before[worker_pid]["VmRSS"]
            )
            result = {
                "clients": clients,
                "requested_seconds": duration,
                "elapsed_seconds": elapsed,
                "requests": len(samples),
                "errors": errors,
                "requests_per_second": len(samples) / elapsed,
                "p50_seconds": statistics.median(samples),
                "p95_seconds": percentile(samples, 0.95),
                "p99_seconds": percentile(samples, 0.99),
                "max_seconds": max(samples),
                "worker_pid": int(worker_pid),
                "worker_peak_rss_bytes": peak,
                "worker_rss_growth_bytes": growth,
                "memory_before": memory_before,
                "memory_after": memory_after,
                "metrics_before": before,
                "metrics_after": after,
                "latencies_seconds": samples,
            }
            report["profiles"].append(result)
            save(args.output, report)
            print(
                json.dumps(
                    {
                        k: v
                        for k, v in result.items()
                        if k
                        not in {"latencies_seconds", "memory_before", "memory_after"}
                    }
                ),
                flush=True,
            )
            if (
                errors
                or after["active"]
                or after["admitted"]
                or after["requests"] - before["requests"] != len(samples)
                or after["projected"] - before["projected"] != len(samples)
            ):
                raise ValueError("load errors or request cleanup/accounting failure")
            if (
                after["errors"] != before["errors"]
                or after["units"]
                or after["scratch"]
            ):
                raise ValueError(
                    "unexpected failure or retained inference resources under load"
                )
            if (
                peak > MAX_WORKER_RSS
                or growth > MAX_RSS_GROWTH
                or result["p95_seconds"] > 0.25 * clients
            ):
                raise ValueError("reference-machine memory/latency policy exceeded")
        report["status"] = "pass"
    except Exception as error:
        report["status"] = "fail"
        report["failure"] = repr(error)
        raise
    finally:
        save(args.output, report)


def cpu_metrics(url):
    # Flat native text requests use resident projection; the ordered-media
    # group counters intentionally exclude this production execution path.
    names = {
        "active": "antfly_inference_requests_active",
        "admitted": "antfly_admission_inference_in_flight_requests",
        "units": "antfly_admission_inference_in_flight_units",
        "requests": "antfly_inference_endpoint_requests_embed",
        "projected": "antfly_inference_embed_resident_projection_text_success_total",
        "errors": "antfly_inference_errors_total",
        "scratch": "antfly_inference_admission_host_scratch_bytes",
    }
    with urllib.request.urlopen(url + "/ml/v1/metrics", timeout=5) as response:
        raw = response.read(4 * 1024 * 1024 + 1)
    if len(raw) > 4 * 1024 * 1024:
        raise ValueError("metrics response exceeds bound")
    values = {}
    for line in raw.decode().splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] in names.values():
            value = float(fields[1])
            if not math.isfinite(value) or value < 0 or value != int(value):
                raise ValueError("invalid CPU serving metric")
            values[fields[0]] = int(value)
    if set(values) != set(names.values()):
        raise ValueError("required CPU lifecycle metrics missing")
    return {key: values[name] for key, name in names.items()}


def wait_idle(url):
    stop = time.monotonic() + 10
    while True:
        current = cpu_metrics(url)
        if all(current[key] == 0 for key in ["active", "admitted", "units", "scratch"]):
            return current
        if time.monotonic() >= stop:
            raise TimeoutError("CPU request did not release admission and scratch")
        time.sleep(0.02)


def recovery(args):
    oracle = json.loads(args.oracle.read_text())
    reference = json.loads(args.cpu_receipt.read_text())
    if reference.get("status") != "pass":
        raise ValueError("CPU serving qualification did not pass")
    identity = reference["model_identity"]
    name, case = max(
        oracle["cases"].items(), key=lambda pair: len(pair[1]["token_ids"])
    )
    body = payload(case, identity)
    expected = [reference["cases"][name]["vector"]]
    report = {
        "version": 1,
        "server": server_provenance(args),
        "status": "running",
        "model_identity": identity,
        "case": name,
    }
    try:
        for budget in ["0", "-1", "invalid", "3600001"]:
            status, _ = declared_request(args.url, body, budget)
            if status != 400:
                raise ValueError("invalid deadline header accepted")
        before = wait_idle(args.url)
        started = time.monotonic()
        status, response = declared_request(args.url, body, "100")
        report["deadline"] = {
            "status": status,
            "response": response,
            "seconds": time.monotonic() - started,
        }
        if (
            status != 504
            or response["error"] != "INFERENCE_TIMEOUT"
            or report["deadline"]["seconds"] > 5
        ):
            raise ValueError("deadline did not return bounded HTTP 504")
        after = wait_idle(args.url)
        if (
            after["projected"] != before["projected"]
            or after["requests"] != before["requests"] + 1
        ):
            raise ValueError(
                "expired CPU request completed projection or lost accounting"
            )
        report["deadline_metrics"] = {"before": before, "after": after}
        response, _ = qualify.post(args.url + "/ai/v1", "/embed", body)
        validate(response, expected, identity)
        endpoint = urllib.parse.urlsplit(args.url)
        before = wait_idle(args.url)
        connection = http.client.HTTPConnection(
            endpoint.hostname, endpoint.port, timeout=30
        )
        try:
            connection.request(
                "POST",
                "/ai/v1/embed",
                json.dumps(body),
                {"Content-Type": "application/json"},
            )
            stop = time.monotonic() + 5
            while True:
                active = cpu_metrics(args.url)
                if active["active"] > 0 and active["admitted"] > 0:
                    break
                if (
                    active["projected"] != before["projected"]
                    or time.monotonic() > stop
                ):
                    raise ValueError("missed disconnect observation window")
                time.sleep(0.005)
            connection.sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
        finally:
            connection.close()
        after = wait_idle(args.url)
        if (
            after["projected"] != before["projected"]
            or after["requests"] != before["requests"] + 1
        ):
            raise ValueError(
                "reset CPU request completed projection or lost accounting"
            )
        response, elapsed = qualify.post(args.url + "/ai/v1", "/embed", body)
        validate(response, expected, identity)
        report["disconnect"] = {
            "status": "pass",
            "transport": "tcp_reset",
            "metrics_before": before,
            "metrics_after": after,
            "retry_seconds": elapsed,
            "observation": "active request released admission and scratch without a successful projection",
        }
        before = wait_idle(args.url)
        connection = http.client.HTTPConnection(
            endpoint.hostname, endpoint.port, timeout=30
        )
        try:
            connection.request(
                "POST",
                "/ai/v1/embed",
                json.dumps(body),
                {"Content-Type": "application/json"},
            )
            connection.sock.shutdown(socket.SHUT_WR)
            response = connection.getresponse()
            result = json.loads(response.read(1024 * 1024))
            if response.status != 200:
                raise ValueError("legal half-close failed")
            validate(result, expected, identity)
        finally:
            connection.close()
        after = wait_idle(args.url)
        if (
            after["projected"] != before["projected"] + 1
            or after["requests"] != before["requests"] + 1
        ):
            raise ValueError("half-closed request did not complete exactly once")
        report["half_close"] = {
            "status": "pass",
            "metrics_before": before,
            "metrics_after": after,
        }
        report["status"] = "pass"
    except Exception as error:
        report["status"] = "fail"
        report["failure"] = repr(error)
        raise
    finally:
        save(args.output, report)
    print(json.dumps(report), flush=True)


def overload(args):
    oracle = json.loads(args.oracle.read_text())
    reference = json.loads(args.cpu_receipt.read_text())
    if reference.get("status") != "pass":
        raise ValueError("CPU serving qualification did not pass")
    identity = reference["model_identity"]
    name, case = max(
        oracle["cases"].items(), key=lambda pair: len(pair[1]["token_ids"])
    )
    body = payload(case, identity)
    encoded = json.dumps(body).encode()
    expected = [reference["cases"][name]["vector"]]
    barrier = threading.Barrier(32, timeout=10)
    report = {
        "version": 1,
        "server": server_provenance(args),
        "status": "running",
        "model_identity": identity,
        "clients": 32,
        "responses": [],
    }

    def request(_):
        barrier.wait()
        started = time.monotonic()
        req = urllib.request.Request(
            args.url + "/ai/v1/embed", encoded, {"Content-Type": "application/json"}
        )
        try:
            response = urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = json.loads(response.read(1024 * 1024))
            row = {"status": response.status, "seconds": time.monotonic() - started}
            if response.status == 200:
                validate(data, expected, identity)
            elif response.status == 503:
                if (
                    data.get("reason")
                    != {
                        "SERVICE_UNAVAILABLE": "inference_admission",
                        "MODEL_RESOURCE_BUSY": "inference_capacity",
                    }.get(data["error"], "invalid")
                    or data.get("retryable") is not True
                    or data.get("retry_after_ms", 0) <= 0
                    or int(response.headers.get("Retry-After", "0")) <= 0
                ):
                    raise ValueError(
                        f"overload did not expose retryable capacity denial: {data!r}, headers={dict(response.headers)!r}"
                    )
                row["error"] = data
                row["retry_after"] = response.headers["Retry-After"]
            else:
                raise ValueError(f"unexpected overload HTTP {response.status}: {data}")
            return row

    try:
        before = wait_idle(args.url)
        memory_before = memory_tree(args.server_pid)
        with ThreadPoolExecutor(max_workers=32) as pool:
            futures = [pool.submit(request, i) for i in range(32)]
            peak_admitted = 0
            while not all(future.done() for future in futures):
                peak_admitted = max(peak_admitted, cpu_metrics(args.url)["admitted"])
                time.sleep(0.02)
            report["responses"] = [future.result() for future in futures]
        after = wait_idle(args.url)
        report.update(
            metrics_before=before,
            metrics_after=after,
            peak_admitted=peak_admitted,
            memory_before=memory_before,
            memory_after=memory_tree(args.server_pid),
        )
        successes = sum(row["status"] == 200 for row in report["responses"])
        denials = sum(row["status"] == 503 for row in report["responses"])
        report.update(successes=successes, capacity_denials=denials)
        if (
            successes == 0
            or denials == 0
            or peak_admitted > 16
            or after["projected"] != before["projected"] + successes
        ):
            raise ValueError(
                "overload did not bound admission, reject excess work and complete admitted requests"
            )
        if set(memory_before) != set(report["memory_after"]):
            raise ValueError("overload replaced the worker")
        response, _ = qualify.post(args.url + "/ai/v1", "/embed", body)
        validate(response, expected, identity)
        report["retry_metrics"] = wait_idle(args.url)
        report["status"] = "pass"
    except Exception as error:
        report["status"] = "fail"
        report["failure"] = repr(error)
        raise
    finally:
        save(args.output, report)
    print(
        json.dumps(
            {
                k: report[k]
                for k in ["status", "successes", "capacity_denials", "peak_admitted"]
            }
        ),
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation", choices=["collect", "load", "recovery", "overload"]
    )
    parser.add_argument("--url", default="http://127.0.0.1:18791")
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu-receipt", type=Path)
    parser.add_argument("--server-pid", type=int, required=True)
    parser.add_argument("--profile-seconds", type=int, default=30)
    parser.add_argument("--soak-seconds", type=int, default=600)
    args = parser.parse_args()
    if args.operation != "collect" and args.cpu_receipt is None:
        parser.error("load/recovery require --cpu-receipt")
    if args.operation == "load" and (
        args.server_pid is None or min(args.profile_seconds, args.soak_seconds) <= 0
    ):
        parser.error("load requires --server-pid and positive durations")
    {"collect": collect, "load": load, "recovery": recovery, "overload": overload}[
        args.operation
    ](args)


if __name__ == "__main__":
    main()
