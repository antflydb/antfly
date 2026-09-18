#!/usr/bin/env python3
"""Bounded C40/C80 frontend admission correctness; no throughput qualification."""

from __future__ import annotations

import argparse
import json
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import workload_cluster_qualification as runner
import workload_destination_isolation as isolation
import workload_fixed_policy as policies


class GeneratorInvalid(RuntimeError):
    pass


def make_plan(binary, revision, optimization):
    plan = policies.correctness_plan()
    plan["artifacts"]["candidate"].update(
        binary=str(binary),
        revision=revision,
        sha256=runner.q.checksum(binary),
        optimization=optimization,
    )
    for node in plan["nodes"][1:]:
        admission = node["config"]["admission"]
        admission["ingress"]["max_requests"] = 128
        admission["read_execution"] = {"max_runnable_tasks": 0}
        admission["remote_attempt_worker"] = {"max_attempts": 0, "max_bytes": 0}
        admission["remote_attempt_coordinator"] = {
            "max_attempts": 0,
            "max_bytes": 0,
            "max_destination_attempts": 0,
            "max_destinations": 0,
        }
        admission["query"]["waiting"]["max_wait_ms"] = 5000
    plan.update(
        driver="workload_frontend_overload.py",
        concurrencies=[40, 80],
        generator_max_workers=80,
        max_dispatch_span_ms=100,
        burst_timeout_seconds=8,
        upstream_delay_ms=500,
        recovery_timeout_seconds=10,
        note="Frontend active4/queue8; finite byte budgets; remote attempts and backend read scheduler disabled. API listener healthz/readyz tested during observed query saturation. Sampled ceilings are not a whole-process memory/performance qualification.",
    )
    plan["actions"] = [{"action": "ready", "node": "api", "at": 0}]
    return plan


def classify(result):
    if result.get("error") or result.get("unknown_write_outcome"):
        raise AssertionError("unexpected transport failure")
    body = json.loads(result.get("body", ""))
    if result.get("status") == 200 and body == {"marker": "durable-a"}:
        return "success"
    if result.get("status") == 429 and body == {
        "error": "AdmissionQueueFull",
        "reason": "instance_busy",
        "stage": "admission",
        "execution_started": False,
    }:
        headers = {key.lower(): value for key, value in result["headers"].items()}
        if headers.get("retry-after") == "1":
            return "rejected"
    raise AssertionError(
        f"unexpected overload semantics: status={result.get('status')} body={body}"
    )


def metric_action(config, timeout=1, idle=False):
    expected = {
        "antfly_admission_query_diagnostics_available": 1,
        "antfly_remote_attempt_coordinator_enabled": 0,
        "antfly_remote_attempt_worker_enabled": 0,
    }
    ceilings = {}
    for kind in ("query", "write"):
        admission = config["admission"][kind]
        waiting = admission["waiting"]
        scope = "antfly_admission_" + kind + "_"
        expected.update(
            {
                scope + "capacity_requests": admission["max_concurrent_requests"],
                scope + "queue_capacity_requests": waiting["max_queued_requests"],
                scope + "queue_capacity_bytes": waiting["max_queued_bytes"],
                scope + "retained_capacity_bytes": waiting["max_retained_bytes"],
            }
        )
        ceilings.update(
            {
                scope + "in_flight_requests": admission["max_concurrent_requests"],
                scope + "queued_requests": waiting["max_queued_requests"],
                scope + "queued_bytes": waiting["max_queued_bytes"],
                scope + "retained_bytes": waiting["max_retained_bytes"],
            }
        )
    ingress = config["admission"]["ingress"]
    expected.update(
        antfly_recovery_ingress_enabled=1,
        antfly_recovery_ingress_capacity_requests=ingress["recovery_requests"],
        antfly_recovery_ingress_capacity_bytes=ingress["recovery_retained_bytes"],
    )
    ceilings.update(
        antfly_recovery_ingress_outstanding_requests=ingress["recovery_requests"],
        antfly_recovery_ingress_retained_bytes=ingress["recovery_retained_bytes"],
    )
    ceilings["antfly_admission_query_peak_in_flight_requests"] = 4
    if idle:
        expected.update(
            {
                "antfly_admission_query_" + name: 0
                for name in (
                    "in_flight_requests",
                    "queued_requests",
                    "queued_bytes",
                    "retained_bytes",
                    "outstanding_requests",
                )
            }
        )
    return {
        "timeout_seconds": timeout,
        "interval_seconds": 0.25,
        "max_age_seconds": 1,
        "stable_seconds": 1 if idle else 0,
        "expected": expected,
        "ceilings": ceilings,
    }


class FrontendCluster(isolation.IsolationCluster):
    def sampled(self, *, idle=False, submitted=None):
        action = metric_action(
            self.nodes["api"]["config"],
            self.plan["recovery_timeout_seconds"] if idle else 1,
            idle,
        )
        samples = []

        def emit(row):
            samples.append(row)
            self.record({"event": "frontend_metrics_sample", **row})

        result = runner.poll_metrics(
            self.ports["api"]["health"],
            action,
            time.monotonic() if submitted is None else submitted,
            emit,
        )
        self.record({"event": "frontend_metrics_result", **result})
        if not result["passed"]:
            raise AssertionError(
                "fresh frontend metric ceiling/policy/idle check failed"
            )
        return samples[-1]["observed"]

    def burst(self, concurrency):
        submitted = [None]
        barrier = threading.Barrier(
            concurrency + 1, action=lambda: submitted.__setitem__(0, time.monotonic())
        )
        action = {
            "method": "GET",
            "path": "/db/v1/tables/policy_fixture/documents/a",
            "is_write": False,
            "expect": {"status": 200},
        }

        def send(index):
            barrier.wait(timeout=10)
            result = runner.request(
                self.ports["api"]["api"],
                action,
                self.plan["burst_timeout_seconds"],
                submitted=submitted[0],
                raw=True,
            )
            self.record(
                {
                    "event": "frontend_request",
                    "concurrency": concurrency,
                    "index": index,
                    **result,
                }
            )
            return result

        self.fault(
            {
                "node": "data",
                "action": "delay",
                "delay_ms": self.plan["upstream_delay_ms"],
            }
        )
        saturated, control_checked = False, False
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(send, index) for index in range(concurrency)]
            barrier.wait(timeout=10)
            while not all(future.done() for future in futures):
                observed = self.sampled()
                active = observed.get("antfly_admission_query_in_flight_requests")
                queued = observed.get("antfly_admission_query_queued_requests")
                if active == 4 and queued == 8:
                    saturated = True
                    if not control_checked:
                        for path, status in (("/healthz", "ok"), ("/readyz", "ready")):
                            self.call(
                                "GET",
                                path,
                                checks=[{"path": ["status"], "equals": status}],
                                timeout=1,
                            )
                        control_checked = True
                time.sleep(0.1)
            results = [future.result() for future in futures]
        finished = time.monotonic()
        self.fault({"node": "data", "action": "heal"})
        dispatch_span = max(row["dispatch_monotonic"] for row in results) - min(
            row["dispatch_monotonic"] for row in results
        )
        outcomes = [classify(row) for row in results]
        if dispatch_span * 1000 > self.plan["max_dispatch_span_ms"]:
            raise GeneratorInvalid(
                f"C{concurrency} dispatch span {dispatch_span * 1000:.3f}ms"
            )
        if (
            not saturated
            or not control_checked
            or "success" not in outcomes
            or "rejected" not in outcomes
        ):
            raise AssertionError(
                "burst did not demonstrate saturation, rejection, semantic success and API control progress"
            )
        self.call(
            "GET", action["path"], checks=[{"path": ["marker"], "equals": "durable-a"}]
        )
        self.sampled(idle=True, submitted=finished)
        result = {
            "event": "frontend_burst_result",
            "concurrency": concurrency,
            "passed": True,
            "successful_reads": outcomes.count("success"),
            "admission_rejections": outcomes.count("rejected"),
            "dispatch_span_ms": dispatch_span * 1000,
            "api_control_progress_during_query_saturation": control_checked,
            "recovery_seconds": time.monotonic() - finished,
            "scope": "correctness and sampled finite caps; no performance qualification",
        }
        self.record(result)
        return result


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
        policies,
        isolation,
    ):
        shutil.copy2(module.__file__, output / Path(module.__file__).name)
    shutil.copy2(__file__, output / Path(__file__).name)
    runner.q.save(
        output / "host.json",
        {"platform": runner.q.platform.platform(), "resource_envelope_enforced": False},
    )
    cluster, failure, cleanup, bursts, generator_invalid = None, None, [], [], False
    try:
        (output / "artifacts").mkdir()
        artifact = output / "artifacts/candidate"
        shutil.copyfile(binary, artifact)
        if runner.q.checksum(artifact) != plan["artifacts"]["candidate"]["sha256"]:
            raise ValueError("copied binary checksum mismatch")
        artifact.chmod(0o500)
        cluster = FrontendCluster(plan, output)
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
        cluster.call(
            "GET",
            "/db/v1/tables/policy_fixture/documents/a",
            checks=[{"path": ["marker"], "equals": "durable-a"}],
        )
        cluster.sampled(idle=True)
        for concurrency in plan["concurrencies"]:
            bursts.append(cluster.burst(concurrency))
    except GeneratorInvalid as error:
        failure, generator_invalid = str(error), True
    except BaseException as error:  # noqa: BLE001
        # Preserve receipts and clean owned processes on any interrupted run.
        failure = f"{type(error).__name__}: {error}"
    finally:
        if cluster:
            cleanup = cluster.close()
        receipt = {
            "passed": failure is None and not cleanup,
            "failure": failure,
            "generator_invalid": generator_invalid,
            "cleanup_errors": cleanup,
            "bursts": bursts,
            "performance_qualification": False,
            "release_qualified": False,
            "artifact": plan["artifacts"]["candidate"],
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
    return 0 if receipt["passed"] else 2 if generator_invalid else 1


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
