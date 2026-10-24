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
from pressure_clients import PressureClients


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]


def operation_kind(index, mixed):
    if not mixed:
        return "cheap"
    if index % 16 == 15:
        return "slow_reader"
    if index % 8 == 7:
        return "write"
    if index % 4 == 3:
        return "expensive"
    return "cheap"


def workload_breakdown(results, offered, dropped, elapsed):
    kinds = {}
    for kind, count in offered.items():
        selected = [r for r in results if r["kind"] == kind]
        successful = [r["end_to_end_seconds"] for r in selected if r["ok"]]
        statuses = {}
        for result in selected:
            code = str(result.get("status", "client_error"))
            statuses[code] = statuses.get(code, 0) + 1
        kinds[kind] = dict(
            offered=count,
            submitted=count - dropped.get(kind, 0),
            completed=len(selected),
            generator_dropped=dropped.get(kind, 0),
            successful=len(successful),
            failed=len(selected) - len(successful),
            successful_per_second=len(successful) / elapsed,
            success_p95_seconds=percentile(successful, 0.95),
            success_p99_seconds=percentile(successful, 0.99),
            status_counts=statuses,
        )
    return kinds


def admission_diagnostics(before, after):
    if before is None or after is None:
        return None
    counters = (
        "wait_ns_total",
        "wait_completed_total",
        "lease_hold_ns_total",
        "lease_completed_total",
        "rejected_total",
        "expired_total",
        "cancelled_total",
    )
    delta = {name: after["query"][name] - before["query"][name] for name in counters}
    if any(value < 0 for value in delta.values()):
        raise RuntimeError("admission counters decreased during qualification")
    delta["mean_wait_seconds"] = (
        delta["wait_ns_total"] / delta["wait_completed_total"] / 1e9
        if delta["wait_completed_total"]
        else None
    )
    delta["mean_completed_lease_hold_seconds"] = (
        delta["lease_hold_ns_total"] / delta["lease_completed_total"] / 1e9
        if delta["lease_completed_total"]
        else None
    )
    return delta


class ClientActivity:
    def __init__(self):
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0

    def start(self):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)

    def finish(self):
        with self.lock:
            if self.active == 0:
                raise RuntimeError("client activity retired without a matching start")
            self.active -= 1

    def snapshot(self):
        with self.lock:
            return self.active, self.peak


def run_phase(target, phase, manifest, pid):
    # A full client envelope drops the offered operation immediately. This is
    # counted separately from server rejection and never silently retried.
    slots = threading.BoundedSemaphore(manifest["max_pending_clients"])
    activity = ClientActivity()
    results, resources, controls = [], [], []
    peers, peers_lock = [], threading.Lock()
    local = threading.local()
    stop = threading.Event()
    duration, rate = phase["duration_seconds"], phase["offered_per_second"]
    workload_before = workload_snapshot(target)
    cpu_before = sample(pid)["cpu_seconds"]
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
        activity.start()
        result = {
            "offered_at_seconds": scheduled - started,
            "kind": operation_kind(index, phase.get("workload", "mixed") == "mixed"),
            "client_queue_seconds": time.monotonic() - scheduled,
        }
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
            activity.finish()
            slots.release()
        return result

    monitor = threading.Thread(target=observer)
    monitor.start()
    futures = []
    offered = int(duration * rate)
    dropped = 0
    offered_by_kind, dropped_by_kind = {}, {}
    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=manifest["client_workers"]
        ) as executor:
            for index in range(offered):
                kind = operation_kind(index, phase.get("workload", "mixed") == "mixed")
                offered_by_kind[kind] = offered_by_kind.get(kind, 0) + 1
                scheduled = started + index / rate
                delay = scheduled - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                if slots.acquire(blocking=False):
                    futures.append(executor.submit(operation, index, scheduled))
                else:
                    dropped += 1
                    dropped_by_kind[kind] = dropped_by_kind.get(kind, 0) + 1
            for future in futures:
                results.append(future.result())
    finally:
        stop.set()
        monitor.join()
        for peer in peers:
            peer.close()
    active, peak = activity.snapshot()
    if active != 0 or peak > manifest["client_workers"]:
        raise RuntimeError("client concurrency did not retire within its envelope")
    elapsed = time.monotonic() - started
    cpu_seconds = sample(pid)["cpu_seconds"] - cpu_before
    workload_after = workload_snapshot(target, require_waiting=True)
    successful = [r["end_to_end_seconds"] for r in results if r["ok"]]
    all_latencies = [r["end_to_end_seconds"] for r in results]
    counts = {}
    for result in results:
        code = str(result.get("status", "client_error"))
        counts[code] = counts.get(code, 0) + 1
    return dict(
        name=phase["name"],
        offered=offered,
        client_workers=manifest["client_workers"],
        active_clients_peak=peak,
        client_workers_used=len(peers),
        submitted=len(futures),
        generator_dropped=dropped,
        completed=len(results),
        successful=len(successful),
        failed=len(results) - len(successful),
        retries=0,
        status_counts=counts,
        by_kind=workload_breakdown(results, offered_by_kind, dropped_by_kind, elapsed),
        elapsed_seconds=elapsed,
        server_cpu_seconds=cpu_seconds,
        server_cpu_cores_average=cpu_seconds / elapsed,
        workload_before=workload_before,
        workload_after=workload_after,
        admission_diagnostics=admission_diagnostics(workload_before, workload_after),
        runnable_diagnostics=runnable_diagnostics(workload_before, workload_after),
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


def workload_snapshot(target, *, require_waiting=False):
    # Outside the timed phase: preserve server wait/allocation counters without
    # adding a candidate-only sampling load to the baseline comparison.
    policy = os.environ.get("ANTFLY_QUALIFICATION_POLICY")
    if policy not in ("fixed", "failfast", "runnable"):
        return None
    code, snapshot = operator_request(target, "GET")
    if code != 200:
        raise RuntimeError("candidate workload telemetry unavailable")
    if (
        require_waiting
        and policy in ("fixed", "runnable")
        and snapshot["query"].get("sql_waiting_enabled") is not True
    ):
        raise RuntimeError("fixed policy did not enable SQL waiting")
    if policy == "runnable":
        runnable = snapshot.get("runnable", {})
        if runnable.get("enabled") is not True or runnable.get("capacity_slots", 0) < 3:
            raise RuntimeError("runnable scheduler telemetry is disabled or missing")
    return snapshot


# Version 1 of runtime_workload_abi.RunnableServiceStats. These are active
# execution-lease wall slot-times, not thread CPU utilization or full coverage.
SERVICE_CLASSES = (
    "query_general",
    "write",
    "inference",
    "background",
    "query_bounded",
    "query_analytical",
)
SERVICE_COUNTERS = (
    "estimated_slot_ns",
    "observed_slot_ns",
    "unobserved_estimate_ns",
    "refunded_slot_ns",
    "debt_paid_slot_ns",
    "clipped_adjustments",
)


def service_diagnostics(previous, current):
    before, after = previous.get("service"), current.get("service")
    if before is None and after is None:
        return dict(available=False)
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise RuntimeError(
            "runnable service telemetry disappeared during qualification"
        )
    for stats in (before, after):
        if any(
            type(stats.get(key)) is not int or stats[key] != value
            for key, value in (
                ("version", 1),
                ("struct_size", 408),
                ("measurement_model", 1),
            )
        ):
            raise RuntimeError("unsupported runnable service telemetry contract")
        if type(stats.get("available")) is not bool:
            raise RuntimeError("invalid runnable service availability")
    if before["available"] != after["available"]:
        raise RuntimeError("runnable service availability changed during qualification")
    if not after["available"]:
        return dict(available=False)
    for stats in (before, after):
        quantum = stats.get("quantum_ns")
        if type(quantum) is not int or not 0 < quantum <= 100_000_000:
            raise RuntimeError("invalid runnable service quantum")
        for field in (*SERVICE_COUNTERS, "debt_slot_ns", "rebate_slot_ns"):
            values = stats.get(field)
            if (
                not isinstance(values, list)
                or len(values) != len(SERVICE_CLASSES)
                or any(
                    type(value) is not int or not 0 <= value <= 2**64 - 1
                    for value in values
                )
            ):
                raise RuntimeError("invalid runnable service class telemetry")
        for debt, rebate in zip(stats["debt_slot_ns"], stats["rebate_slot_ns"]):
            if (debt and rebate) or max(debt, rebate) > 64 * quantum:
                raise RuntimeError("invalid runnable service correction")
    if before["quantum_ns"] != after["quantum_ns"]:
        raise RuntimeError("runnable service quantum changed during qualification")
    delta = {
        field: [new - old for old, new in zip(before[field], after[field])]
        for field in SERVICE_COUNTERS
    }
    if any(value < 0 for values in delta.values() for value in values):
        raise RuntimeError("runnable service counters decreased during qualification")
    return dict(
        available=True,
        measurement_model=1,
        quantum_ns=after["quantum_ns"],
        classes=list(SERVICE_CLASSES),
        counters=delta,
        debt_slot_ns=after["debt_slot_ns"],
        rebate_slot_ns=after["rebate_slot_ns"],
    )


def runnable_diagnostics(before, after):
    if os.environ.get("ANTFLY_QUALIFICATION_POLICY") != "runnable":
        return None
    previous, current = before["runnable"], after["runnable"]
    for field in (
        "capacity_slots",
        "control_capacity_slots",
        "operation_capacity",
        "metadata_capacity_bytes",
        "control_metadata_capacity_bytes",
        "max_wait_ms",
        "quantum_ms",
        "checkpoint_budget",
    ):
        if previous[field] != current[field]:
            raise RuntimeError("runnable policy changed during qualification")
    if len(previous["grants"]) != 5 or len(current["grants"]) != 5:
        raise RuntimeError("runnable class telemetry is incomplete")
    grants = [new - old for old, new in zip(previous["grants"], current["grants"])]
    delta = {
        field: current[field] - previous[field]
        for field in ("yields", "backfills", "rejected", "wait_ns")
    }
    if min(grants + list(delta.values())) < 0:
        raise RuntimeError("runnable counters decreased during qualification")
    return dict(delta, grants=grants, service=service_diagnostics(previous, current))


def runnable_evidence_failures(
    measured, policy, minimum_yields=0, minimum_observed_slot_ns=0
):
    diagnostics = measured.get("runnable_diagnostics")
    stats = (measured.get("workload_after") or {}).get("runnable", {})
    if diagnostics is None or stats.get("enabled") is not True:
        return ["runnable scheduling was not measured"]
    expected = {
        "capacity_slots": policy.get("slots", 8),
        "control_capacity_slots": policy.get("control_slots", 1)
        + (policy.get("cleanup_slots", 0) or 1),
        "operation_capacity": policy.get("max_operations", 128),
        "metadata_capacity_bytes": policy.get("max_bytes", 8388608),
        "control_metadata_capacity_bytes": policy.get("control_bytes", 65536),
        "max_wait_ms": policy.get("max_wait_ms", 250),
        "quantum_ms": policy.get("quantum_ms", 2),
        "checkpoint_budget": policy.get("checkpoint_budget", 64),
    }
    failures = [
        "effective runnable capacity differs from manifest"
        for field, value in expected.items()
        if stats.get(field) != value
    ]
    kinds = measured.get("by_kind", {})
    if (
        any(
            kinds.get(kind, {}).get("successful", 0)
            for kind in ("cheap", "expensive", "slow_reader")
        )
        and diagnostics["grants"][0] == 0
    ):
        failures.append("successful queries did not acquire runnable grants")
    if kinds.get("write", {}).get("successful", 0) and diagnostics["grants"][1] == 0:
        failures.append("successful writes did not acquire runnable grants")
    for kind, count in kinds.items():
        if count.get("offered", 0) > 0 and count.get("successful", 0) == 0:
            failures.append(
                "offered " + kind + " work completed no successful operation"
            )
    if diagnostics["yields"] < minimum_yields:
        failures.append("runnable yielding was not exercised")
    if minimum_observed_slot_ns:
        service = diagnostics.get("service") or {}
        if not service.get("available"):
            failures.append("runnable service observations were not measured")
        elif sum(service["counters"]["observed_slot_ns"]) < minimum_observed_slot_ns:
            failures.append("runnable observed service was below the declared minimum")
    return failures


def waiting_evidence_failures(diagnostics, minimum):
    if minimum and (diagnostics or {}).get("wait_completed_total", 0) < minimum:
        return ["fixed waiting was not exercised"]
    return []


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
    pressure = None
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
        pressure = PressureClients(target, args.pgwire_port, manifest)
        receipt["retained_pressure_setup"] = pressure.start()
        if pressure.uploads and os.environ.get("ANTFLY_QUALIFICATION_POLICY") in (
            "fixed",
            "failfast",
        ):
            # Header ingress is asynchronous. Require real managed body heap
            # ownership before timing; declared Content-Length alone is insufficient.
            deadline = time.monotonic() + manifest["request_timeout_seconds"]
            while True:
                memory = workload_snapshot(target)["memory"]
                if (
                    memory["transport"]["used_bytes"]
                    >= receipt["retained_pressure_setup"]["declared_upload_bytes"]
                ):
                    receipt["retained_pressure_memory_setup"] = memory
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        "unfinished uploads did not retain the offered body heap"
                    )
                time.sleep(0.05)
        for phase in manifest["phases"]:
            measured = run_phase(target, phase, manifest, args.pid)
            receipt["phases"].append(measured)
            limits = phase["thresholds"]
            if os.environ.get("ANTFLY_QUALIFICATION_POLICY") in ("fixed", "runnable"):
                failures.extend(
                    phase["name"] + ": " + reason
                    for reason in waiting_evidence_failures(
                        measured["admission_diagnostics"],
                        limits.get("fixed_wait_completed_min", 0),
                    )
                )
            if os.environ.get("ANTFLY_QUALIFICATION_POLICY") == "runnable":
                failures.extend(
                    phase["name"] + ": " + reason
                    for reason in runnable_evidence_failures(
                        measured,
                        manifest["runnable_policy"],
                        limits.get("runnable_yields_min", 0),
                        limits.get("runnable_observed_slot_ns_min", 0),
                    )
                )
            if (
                measured["offered"]
                != measured["submitted"] + measured["generator_dropped"]
                or measured["completed"] != measured["submitted"]
            ):
                failures.append(
                    phase["name"] + ": operation accounting did not reconcile"
                )
            if any(
                kind["submitted"] != kind["completed"]
                or kind["offered"] != kind["submitted"] + kind["generator_dropped"]
                for kind in measured["by_kind"].values()
            ):
                failures.append(
                    phase["name"] + ": per-kind operation accounting did not reconcile"
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
        receipt["retained_pressure_verified"] = pressure.verify()
        pressure.close()
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
    finally:
        if pressure is not None:
            pressure.close()
    receipt.update(passed=not failures, failures=failures)
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"passed": receipt["passed"], "failures": failures}))
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
