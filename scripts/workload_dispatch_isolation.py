#!/usr/bin/env python3
"""Native H1 request-task partition correctness; no connection/performance claim."""

from __future__ import annotations

import argparse
import json
import secrets
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import workload_cluster_qualification as runner
import workload_frontend_overload as frontend

LOOKUP = "/db/v1/tables/policy_fixture/documents/a"
CONTROL = "/internal/v1/workload/control"
ACTIVE = "antfly_http_active_requests"
PERMIT = "antfly_http_request_permit_rejections_total"
EXECUTOR = "antfly_http_request_executor_rejections_total"


def make_plan(binary, revision, optimization):
    plan = frontend.make_plan(binary, revision, optimization)
    for node in plan["nodes"][1:]:
        admission = node["config"]["admission"]
        admission["query"]["max_concurrent_requests"] = 32
        admission["query"]["waiting"]["max_retained_bytes"] = 32 << 20
        admission["ingress"]["max_retained_bytes"] = 64 << 20
        if node["name"] == "api":
            admission["ingress"].update(
                max_requests=34, control_requests=1, recovery_requests=1
            )
            admission["remote_attempt_worker"] = {
                "max_attempts": 8,
                "max_bytes": 2 << 20,
                "max_run_ms": 5000,
            }
    plan.update(
        driver="workload_dispatch_isolation.py",
        concurrencies=[32],
        generator_max_workers=32,
        burst_timeout_seconds=10,
        upstream_delay_ms=3000,
        saturation_timeout_seconds=2,
        probe_timeout_seconds=1,
        note="32 general H1 tasks, one control and one authenticated recovery task; strict compiled API correctness only. No backend read scheduler/coordinator, connection exhaustion or performance qualification.",
    )
    return plan


def require_success(result):
    body = result.get("body")
    try:
        exact = json.loads(body) == {"marker": "durable-a"}
    except (TypeError, ValueError):
        exact = False
    if (
        result.get("status") == 200
        and not result.get("error")
        and not result.get("unknown_write_outcome")
        and exact
    ):
        return
    diagnostic = {
        "status": result.get("status"),
        "error": str(result["error"])[:512] if result.get("error") else None,
        "unknown_write_outcome": bool(result.get("unknown_write_outcome")),
        "body_prefix": body[:512] if isinstance(body, str) else None,
        "body_truncated": isinstance(body, str) and len(body) > 512,
    }
    raise AssertionError(
        "held lookup did not complete with the exact document: "
        + json.dumps(diagnostic, ensure_ascii=True)
    )


def require_denial(result):
    expected = {
        "error": "AdmissionFull",
        "reason": "instance_busy",
        "stage": "admission",
        "execution_started": False,
    }
    if (
        result.get("error")
        or result.get("unknown_write_outcome")
        or result.get("status") != 429
        or json.loads(result.get("body", "null")) != expected
        or {k.lower(): v for k, v in result.get("headers", {}).items()}.get(
            "retry-after"
        )
        != "1"
    ):
        raise AssertionError("expected exact pre-execution AdmissionFull HTTP 429")


def require_head_success(result):
    if result.get("error") or result.get("status") != 200 or result.get("body") != "":
        raise AssertionError("protected HEAD probe must return HTTP 200 and no body")


def require_overlap(before, after):
    if before != 32 or after != 32:
        raise AssertionError("protected probes must overlap all 32 held clients")


def metric_action(config, phase, timeout):
    action = frontend.metric_action(config, timeout, idle=phase == "idle")
    expected, ceilings = action["expected"], action["ceilings"]
    expected.update(
        antfly_remote_attempt_worker_enabled=1,
        antfly_http_request_task_capacity=50,  # public34 + health16
        antfly_http_request_task_reserved=50,
        antfly_http_request_executor_rejections_total=0,
        antfly_http_connection_dispatch_rejections_total=0,
        antfly_http_h2_stream_dispatch_rejections_total=0,
    )
    expected[PERMIT] = 0 if phase in ("initial", "saturated") else 2
    expected["antfly_http_request_dispatch_rejections_total"] = expected[PERMIT]
    ceilings["antfly_admission_query_peak_in_flight_requests"] = 32
    ceilings[ACTIVE] = 34
    if phase in ("saturated", "probed"):
        expected.update(
            antfly_admission_query_in_flight_requests=32,
            antfly_admission_query_queued_requests=0,
        )
        expected[ACTIVE] = 32
    elif phase in ("initial", "idle"):
        expected[ACTIVE] = 0
        expected["antfly_admission_query_in_flight_requests"] = 0
    return action


class DispatchCluster(frontend.FrontendCluster):
    def __init__(self, plan, output):
        super().__init__(plan, output)
        # The shared runner makes a disposable local key. This cell validates
        # its signed proof live and retains no signing secret or bearer token.
        runner.q.save(
            output / "local-test-auth.json",
            {"secret": "[REDACTED]", "issuer": self.issuer, "scope": "local fixture"},
        )

    def record(self, event):
        text = json.dumps(event).replace(self.secret, "[REDACTED]")
        super().record(json.loads(text))

    def sampled_phase(self, phase, submitted=None):
        action = metric_action(
            self.nodes["api"]["config"],
            phase,
            (
                self.plan["recovery_timeout_seconds"]
                if phase == "idle"
                else self.plan["saturation_timeout_seconds"]
            ),
        )
        samples = []

        def emit(row):
            samples.append(row)
            self.record({"event": "dispatch_metrics_sample", "phase": phase, **row})

        result = runner.poll_metrics(
            self.ports["api"]["health"],
            action,
            time.monotonic() if submitted is None else submitted,
            emit,
        )
        self.record({"event": "dispatch_metrics_result", "phase": phase, **result})
        if not result["passed"] or any(row.get("evidence_error") for row in samples):
            raise AssertionError(f"fresh task-partition metrics failed: {phase}")
        return samples[-1]["observed"]

    def probe(self, method, path, *, body=None, headers=None):
        result = runner.request(
            self.ports["api"]["api"],
            {
                "method": method,
                "path": path,
                "body": body,
                "is_write": False,  # only lookups, health, discovery; no mutations
                "expect": {"status": 200},
            },
            self.plan["probe_timeout_seconds"],
            headers=headers,
            raw=True,
        )
        self.record(
            {"event": "dispatch_probe", "method": method, "path": path, **result}
        )
        return result

    def protected_probes(self):
        require_denial(self.probe("GET", LOOKUP))
        require_denial(
            self.probe("POST", CONTROL, body={"workload_attempt_control": "discover"})
        )
        for path, expected in (("/healthz", "ok"), ("/readyz", "ready")):
            result = self.probe("GET", path)
            if (
                result.get("error")
                or result.get("status") != 200
                or json.loads(result["body"]).get("status") != expected
            ):
                raise AssertionError("protected control probe failed")
            require_head_success(self.probe("HEAD", path))
        nonce = secrets.randbits(128) or 1
        coordinator, destination = 7, self.nodes["api"]["node_id"]
        result = self.probe(
            "POST",
            CONTROL,
            body={"workload_attempt_control": "discover", "nonce": nonce},
            headers=runner.service_headers(self.secret, self.issuer, coordinator),
        )
        if result.get("error") or result.get("status") != 200:
            raise AssertionError("authenticated recovery discovery failed")
        headers = {k.lower(): v for k, v in result["headers"].items()}
        proof = runner.verify_discovery(
            headers.get("x-antfly-workload-evidence", ""),
            self.secret,
            self.issuer,
            coordinator,
            destination,
            nonce,
            protocol_version=3,
        )
        self.record(
            {
                "event": "dispatch_discovery_verified",
                "proof": proof,
                "scope": "live signature and nonce verified; not attempt retirement",
            }
        )

    def burst(self):
        submitted = [None]
        barrier = threading.Barrier(
            33, action=lambda: submitted.__setitem__(0, time.monotonic())
        )

        dispatched = [None] * 32
        dispatch_lock = threading.Lock()
        all_dispatched = threading.Event()

        def send(index):
            barrier.wait(timeout=10)
            with dispatch_lock:
                dispatched[index] = time.monotonic()
                if all(value is not None for value in dispatched):
                    all_dispatched.set()
            result = runner.request(
                self.ports["api"]["api"],
                {
                    "method": "GET",
                    "path": LOOKUP,
                    "is_write": False,
                    "expect": {"status": 200},
                },
                self.plan["burst_timeout_seconds"],
                submitted=submitted[0],
                raw=True,
            )
            self.record({"event": "dispatch_held_lookup", "index": index, **result})
            return result

        self.fault(
            {
                "action": "delay",
                "node": "data",
                "delay_ms": self.plan["upstream_delay_ms"],
            }
        )
        with ThreadPoolExecutor(max_workers=32) as pool:
            futures = [pool.submit(send, index) for index in range(32)]
            barrier.wait(timeout=10)
            try:
                if not all_dispatched.wait(self.plan["max_dispatch_span_ms"] / 1000):
                    raise frontend.GeneratorInvalid(
                        "generator did not dispatch all held reads within its declared bound"
                    )
                frontend.validate_dispatch(
                    [{"dispatch_monotonic": value} for value in dispatched],
                    submitted[0],
                    self.plan["max_dispatch_span_ms"],
                )
                self.sampled_phase("saturated", submitted=submitted[0])
                before = sum(not future.done() for future in futures)
                started = time.monotonic()
                self.protected_probes()
                self.sampled_phase("probed")
                after = sum(not future.done() for future in futures)
                require_overlap(before, after)
                self.record(
                    {
                        "event": "dispatch_protected_overlap",
                        "pending_before": before,
                        "pending_after": after,
                        "started_monotonic": started,
                        "finished_monotonic": time.monotonic(),
                    }
                )
            finally:
                healed = time.monotonic()
                self.fault({"action": "heal", "node": "data"})
            results = [future.result() for future in futures]
        timing = frontend.validate_dispatch(
            results, submitted[0], self.plan["max_dispatch_span_ms"]
        )
        for result in results:
            require_success(result)
        require_success(self.probe("GET", LOOKUP))
        self.sampled_phase("idle", submitted=healed)
        return {
            "successful_held_reads": 32,
            "structured_transport_rejections": 2,
            "recovery_seconds": time.monotonic() - healed,
            **timing,
        }


def run(binary, revision, optimization, output):
    plan = make_plan(binary, revision, optimization)
    runner.validate(plan)
    output.mkdir(parents=True, exist_ok=False)
    runner.q.save(output / "plan.json", plan)
    for module in (
        runner,
        runner.q,
        runner.scenarios,
        runner.q.vectors,
        runner.q.evidence,
        runner.proxy_module,
        runner.proxy_module.evidence,
        runner.attempt_evidence,
        frontend,
        frontend.policies,
        frontend.isolation,
    ):
        shutil.copy2(module.__file__, output / Path(module.__file__).name)
    shutil.copy2(__file__, output / Path(__file__).name)
    runner.q.save(
        output / "host.json",
        {"platform": runner.q.platform.platform(), "resource_envelope_enforced": False},
    )
    cluster, failure, cleanup, result, invalid = None, None, [], None, False
    try:
        (output / "artifacts").mkdir()
        artifact = output / "artifacts/candidate"
        shutil.copyfile(binary, artifact)
        if runner.q.checksum(artifact) != plan["artifacts"]["candidate"]["sha256"]:
            raise ValueError("copied binary checksum mismatch")
        artifact.chmod(0o500)
        cluster = DispatchCluster(plan, output)
        for node in plan["nodes"]:
            cluster.start(node["name"])
        for action in plan["setup"]:
            cluster.call(
                action["method"],
                action["path"],
                action.get("body"),
                status=action["expect"]["status"],
                checks=action["expect"].get("checks", []),
            )
        require_success(cluster.probe("GET", LOOKUP))
        cluster.sampled_phase("initial")
        result = cluster.burst()
    except frontend.GeneratorInvalid as error:
        failure, invalid = str(error), True
    except BaseException as error:  # noqa: BLE001
        failure = f"{type(error).__name__}: {error}"
    finally:
        if cluster:
            cleanup = cluster.close()
        receipt = {
            "passed": failure is None and not cleanup,
            "failure": failure,
            "generator_invalid": invalid,
            "cleanup_errors": cleanup,
            "result": result,
            "artifact": plan["artifacts"]["candidate"],
            "performance_qualification": False,
            "release_qualified": False,
            "scope": "H1 task partitions only; no connection exhaustion claim",
        }
        runner.q.save(output / "receipt.json", receipt)
        paths = [
            path
            for path in output.rglob("*")
            if path.is_file()
            and path.suffix in (".py", ".json", ".jsonl", ".log")
            and not any(
                part in {"data", "replicas", "snapshots", "artifacts"}
                for part in path.relative_to(output).parts
            )
        ]
        runner.q.save(
            output / "checksums.json",
            {str(path.relative_to(output)): runner.q.checksum(path) for path in paths},
        )
    print(json.dumps(receipt, indent=2))
    return 0 if receipt["passed"] else 2 if invalid else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument(
        "--optimization", choices=("Debug", "ReleaseFast"), default="Debug"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raise SystemExit(run(args.binary, args.revision, args.optimization, args.output))
