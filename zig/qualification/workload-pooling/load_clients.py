#!/usr/bin/env python3
"""Bounded open-loop native qualification. Every offered operation is accounted."""

import argparse
import concurrent.futures
import hashlib
import http.client
import json
import os
import pathlib
import threading
import time
import urllib.parse
from idle_clients import auth_headers, sample, seed_fixture, status


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]


def run_phase(target, phase, manifest, pid):
    # A full client envelope drops the offered operation immediately. This is
    # counted separately from server rejection and never silently retried.
    slots = threading.BoundedSemaphore(manifest["max_pending_clients"])
    results, resources, controls = [], [], []
    peers, peers_lock = [], threading.Lock()
    local = threading.local()
    stop = threading.Event()
    duration, rate = phase["duration_seconds"], phase["offered_per_second"]
    started = time.monotonic()

    def observer():
        while not stop.wait(0.1):
            try:
                resources.append(sample(pid))
                controls.append({"seconds": status(target.geturl()), "ok": True})
            except Exception:
                controls.append(
                    {"seconds": manifest["request_timeout_seconds"], "ok": False}
                )

    def operation(index, scheduled):
        result = {"offered_at_seconds": scheduled - started, "kind": "cheap"}
        try:
            if not hasattr(local, "peer"):
                local.peer = http.client.HTTPConnection(
                    target.hostname,
                    target.port,
                    timeout=manifest["request_timeout_seconds"],
                )
                with peers_lock:
                    peers.append(local.peer)
            peer = local.peer
            mixed = phase.get("workload", "mixed") == "mixed"
            if mixed and index % 16 == 15:
                result["kind"] = "slow_reader"
                path, body = (
                    "/db/v1/sql",
                    {"statement": "SELECT * FROM pooling_qualification ORDER BY id"},
                )
            elif mixed and index % 8 == 7:
                result["kind"] = "write"
                row = index % manifest["fixture_rows"]
                path = "/db/v1/tables/pooling_qualification/batch"
                body = {
                    "inserts": {
                        f"row-{row}": {
                            "id": 9007199254740993 + row,
                            "payload": "x" * manifest.get("fixture_payload_bytes", 0),
                        }
                    },
                    "sync_level": "write",
                }
            elif mixed and index % 4 == 3:
                result["kind"] = "expensive"
                path, body = (
                    "/db/v1/sql",
                    {
                        "statement": "SELECT id FROM pooling_qualification ORDER BY id DESC"
                    },
                )
            else:
                path, body = (
                    "/db/v1/sql",
                    {
                        "statement": "SELECT id FROM pooling_qualification WHERE id = 9007199254740993"
                    },
                )
            if result["kind"] != "write":
                body["limit"] = manifest["fixture_rows"]
            peer.request(
                "POST",
                path,
                json.dumps(body),
                headers=dict(auth_headers(), **{"Content-Type": "application/json"}),
            )
            response = peer.getresponse()
            if result["kind"] == "slow_reader":
                time.sleep(manifest["slow_reader_delay_seconds"])
            payload = response.read()
            result["status"] = response.status
            result["ok"] = response.status == 200 or (
                result["kind"] == "write" and response.status == 201
            )
            if result["ok"]:
                decoded = json.loads(payload)
                if (
                    not isinstance(decoded, dict)
                    or "error" in decoded
                    or decoded.get("errors")
                ):
                    result["ok"] = False
                    result["invalid_response"] = True
                elif result["kind"] != "write":
                    rows = decoded.get("rows")
                    columns = [c.get("name") for c in decoded.get("columns", [])]
                    if not isinstance(rows, list) or "id" not in columns:
                        result.update(ok=False, invalid_response=True)
                    else:
                        ordinal = columns.index("id")
                        ids = [row[ordinal] for row in rows]
                        expected = [
                            str(9007199254740993 + i)
                            for i in range(manifest["fixture_rows"])
                        ]
                        if result["kind"] == "cheap":
                            expected = expected[:1]
                        elif result["kind"] == "expensive":
                            expected.reverse()
                        if ids != expected:
                            result.update(ok=False, invalid_response=True)
            if not result["ok"]:
                try:
                    diagnostic = json.loads(payload)
                    code = diagnostic.get("code")
                    if isinstance(code, str) and len(code) == 5 and code.isalnum():
                        result["sql_error_code"] = code
                except (ValueError, AttributeError):
                    pass
                peer.close()
        except (OSError, http.client.HTTPException, ValueError):
            result.update(ok=False, client_error=True)
            if hasattr(local, "peer"):
                local.peer.close()
        finally:
            result["end_to_end_seconds"] = time.monotonic() - scheduled
            slots.release()
        return result

    monitor = threading.Thread(target=observer)
    monitor.start()
    futures = []
    offered = int(duration * rate)
    dropped = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=manifest["client_workers"]
        ) as executor:
            for index in range(offered):
                scheduled = started + index / rate
                delay = scheduled - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                if slots.acquire(blocking=False):
                    futures.append(executor.submit(operation, index, scheduled))
                else:
                    dropped += 1
            for future in futures:
                results.append(future.result())
    finally:
        stop.set()
        monitor.join()
        for peer in peers:
            peer.close()
    elapsed = time.monotonic() - started
    successful = [r["end_to_end_seconds"] for r in results if r["ok"]]
    all_latencies = [r["end_to_end_seconds"] for r in results]
    counts = {}
    for result in results:
        code = str(result.get("status", "client_error"))
        counts[code] = counts.get(code, 0) + 1
    return dict(
        name=phase["name"],
        offered=offered,
        submitted=len(futures),
        generator_dropped=dropped,
        completed=len(results),
        successful=len(successful),
        failed=len(results) - len(successful),
        retries=0,
        status_counts=counts,
        elapsed_seconds=elapsed,
        successful_per_second=len(successful) / elapsed,
        success_p95_seconds=percentile(successful, 0.95),
        success_p99_seconds=percentile(successful, 0.99),
        all_p95_seconds=percentile(all_latencies, 0.95),
        all_p99_seconds=percentile(all_latencies, 0.99),
        control_failures=sum(not x["ok"] for x in controls),
        control_max_seconds=max((x["seconds"] for x in controls), default=0),
        resource_peak={k: max(s[k] for s in resources) for k in resources[0]}
        if resources
        else sample(pid),
        # Preserve per-operation accounting for audit, without bodies/credentials.
        operations=results,
    )


def operator_request(target, method, body=None, authenticated=True):
    peer = http.client.HTTPConnection(target.hostname, target.port, timeout=2)
    try:
        peer.request(
            method,
            "/admin/v1/workload/query",
            json.dumps(body) if body is not None else None,
            headers=dict(
                auth_headers() if authenticated else {},
                **{"Content-Type": "application/json"},
            ),
        )
        response = peer.getresponse()
        payload = response.read()
        return response.status, json.loads(payload) if response.status == 200 else None
    finally:
        peer.close()


def verify_operator_controls(target):
    code, before = operator_request(target, "GET")
    if code != 200 or not before["memory"]["node_accounted"]:
        raise RuntimeError("native node accounting is not installed")
    if operator_request(target, "GET", authenticated=False)[0] != 401:
        raise RuntimeError("unauthenticated policy access succeeded")
    original = before["query"]
    replacement = dict(
        expected_version=original["policy_version"],
        capacity=1,
        waiting=original["waiting"],
        suspended=True,
    )
    code, suspended = operator_request(target, "PUT", replacement)
    if code != 200 or not suspended["query"]["suspended"]:
        raise RuntimeError("operator suspension failed")
    if operator_request(target, "PUT", replacement)[0] != 409:
        raise RuntimeError("stale policy update was accepted")
    peer = http.client.HTTPConnection(target.hostname, target.port, timeout=2)
    try:
        peer.request(
            "POST",
            "/db/v1/sql",
            json.dumps({"statement": "SELECT id FROM pooling_qualification"}),
            headers=dict(auth_headers(), **{"Content-Type": "application/json"}),
        )
        response = peer.getresponse()
        response.read()
        if response.status != 503:
            raise RuntimeError("fresh query was admitted during suspension")
    finally:
        peer.close()
        # Restore even if the suspension assertion fails; do not leave owner
        # policy altered for recovery measurements.
        code, resumed = operator_request(
            target,
            "PUT",
            dict(
                expected_version=suspended["query"]["policy_version"],
                capacity=original["capacity"],
                waiting=original["waiting"],
                suspended=False,
            ),
        )
        if code != 200 or resumed["query"]["suspended"]:
            raise RuntimeError("operator resume failed")
    return dict(before=before, suspended=suspended, resumed=resumed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--status-url", required=True)
    parser.add_argument("--pgwire-port", type=int, required=True)
    parser.add_argument("--manifest", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--memory-map", action="store_true")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    receipt = {
        "manifest": manifest,
        "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        "phases": [],
    }
    failures = []
    try:
        target = urllib.parse.urlsplit(args.status_url)
        seed_fixture(
            target, manifest["fixture_rows"], manifest.get("fixture_payload_bytes", 0)
        )
        # Warm-up is declared in the manifest and recorded, never silently
        # discarded. Cold profiles leave it at zero.
        warmup = manifest.get("warmup_operations", 0)
        if warmup:
            receipt["warmup"] = run_phase(
                target,
                dict(name="warmup", duration_seconds=1, offered_per_second=warmup),
                manifest,
                args.pid,
            )
            if (
                receipt["warmup"]["successful"] != warmup
                or receipt["warmup"]["generator_dropped"]
            ):
                failures.append("warm-up did not complete every operation successfully")
        status(args.status_url)
        receipt["baseline"] = sample(args.pid)
        for phase in manifest["phases"]:
            measured = run_phase(target, phase, manifest, args.pid)
            receipt["phases"].append(measured)
            limits = phase["thresholds"]
            if (
                measured["offered"]
                != measured["submitted"] + measured["generator_dropped"]
                or measured["completed"] != measured["submitted"]
            ):
                failures.append(
                    phase["name"] + ": operation accounting did not reconcile"
                )
            if measured["successful_per_second"] < limits["successful_per_second_min"]:
                failures.append(phase["name"] + ": successful throughput below floor")
            if (
                measured["success_p95_seconds"] is None
                or measured["success_p95_seconds"] > limits["success_p95_seconds_max"]
            ):
                failures.append(phase["name"] + ": success p95 exceeded ceiling")
            if measured["generator_dropped"] > limits["generator_dropped_max"]:
                failures.append(phase["name"] + ": generator envelope exceeded")
            if (
                measured["control_failures"]
                or measured["control_max_seconds"] > limits["control_seconds_max"]
            ):
                failures.append(
                    phase["name"] + ": protected status failed latency gate"
                )
            if any(
                r.get("status") in (400, 401, 403, 404, 422)
                for r in measured["operations"]
            ):
                failures.append(
                    phase["name"] + ": unexpected request or authorization error"
                )
            if any(r.get("invalid_response") for r in measured["operations"]):
                failures.append(phase["name"] + ": invalid successful response")
            for key, maximum in manifest["resource_thresholds"].items():
                if measured["resource_peak"][key] > maximum:
                    failures.append(
                        phase["name"] + ": resource ceiling exceeded: " + key
                    )
        if (
            manifest.get("verify_operator_controls")
            and os.environ.get("ANTFLY_QUALIFICATION_POLICY") == "fixed"
        ):
            receipt["operator_controls"] = verify_operator_controls(target)
        time.sleep(manifest["recovery_seconds"])
        receipt["recovery"] = sample(args.pid)
        receipt["recovery_status_seconds"] = status(args.status_url)
        if (
            receipt["recovery"]["fds"] - receipt["baseline"]["fds"]
            > manifest["recovery_fd_growth_max"]
        ):
            failures.append("descriptor recovery exceeded ceiling")
        if receipt["recovery_status_seconds"] > manifest["recovery_status_seconds_max"]:
            failures.append("post-overload status exceeded ceiling")
    except Exception as error:
        # Exception types are useful; response messages may contain user data.
        failures.append("qualification failed: " + type(error).__name__)
    receipt.update(passed=not failures, failures=failures)
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"passed": receipt["passed"], "failures": failures}))
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
