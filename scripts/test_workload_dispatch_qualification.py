import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import workload_attempt_evidence as evidence
import workload_cluster_qualification as runner
import workload_dispatch_isolation as dispatch


def denial():
    return {
        "status": 429,
        "headers": {"Retry-After": "1"},
        "body": json.dumps(
            {
                "error": "AdmissionFull",
                "reason": "instance_busy",
                "stage": "admission",
                "execution_started": False,
            }
        ),
    }


class DispatchQualificationTests(unittest.TestCase):
    def test_finite_policy_isolates_transport_from_query_and_worker_admission(self):
        plan = dispatch.make_plan(Path("/usr/bin/true"), "a" * 40, "Debug")
        runner.validate(plan)
        api = plan["nodes"][-1]["config"]["admission"]
        data = plan["nodes"][1]["config"]["admission"]
        self.assertEqual(api["ingress"]["max_requests"], 34)
        self.assertEqual(api["ingress"]["control_requests"], 1)
        self.assertEqual(api["ingress"]["recovery_requests"], 1)
        self.assertEqual(api["query"]["max_concurrent_requests"], 32)
        self.assertEqual(data["query"]["max_concurrent_requests"], 32)
        self.assertEqual(data["ingress"]["max_requests"], 128)
        self.assertGreater(api["remote_attempt_worker"]["max_attempts"], 0)
        for admission in (api, data):
            self.assertEqual(admission["remote_attempt_coordinator"]["max_attempts"], 0)
            self.assertEqual(admission["read_execution"]["max_runnable_tasks"], 0)
        for phase, permits, active in (
            ("initial", 0, 0),
            ("saturated", 0, 32),
            ("probed", 2, 32),
            ("idle", 2, 0),
        ):
            action = dispatch.metric_action(plan["nodes"][-1]["config"], phase, 2)
            runner.validate_metrics_action(action)
            self.assertEqual(action["expected"][dispatch.PERMIT], permits)
            self.assertEqual(action["expected"][dispatch.ACTIVE], active)
            self.assertEqual(action["expected"][dispatch.EXECUTOR], 0)
            self.assertEqual(
                action["ceilings"]["antfly_admission_query_peak_in_flight_requests"], 32
            )

    def test_rejection_must_be_transport_admission_and_retry_safe(self):
        dispatch.require_denial(denial())
        for key, value in (
            ("error", "AdmissionQueueFull"),
            ("reason", "resource_exhausted"),
            ("stage", "execution"),
            ("execution_started", True),
        ):
            invalid = denial()
            invalid["body"] = json.dumps({**json.loads(invalid["body"]), key: value})
            with self.assertRaises(AssertionError):
                dispatch.require_denial(invalid)
        for invalid in (
            {**denial(), "status": 503},
            {**denial(), "error": "timeout"},
            {**denial(), "headers": {}},
            {**denial(), "unknown_write_outcome": True},
        ):
            with self.assertRaises(AssertionError):
                dispatch.require_denial(invalid)
        with self.assertRaises(AssertionError):
            dispatch.require_success(denial())

    def test_failed_lookup_preserves_status_and_bounded_non_json_body(self):
        dispatch.require_success({"status": 200, "body": '{"marker":"durable-a"}'})
        for result in (
            {"status": 404, "body": "not found"},
            {"status": 200, "body": '{"marker":'},
            {"status": 200, "body": "{}"},
            {"status": 200, "body": '{"marker":"durable-a"}', "error": "socket failed"},
            {
                "status": 200,
                "body": '{"marker":"durable-a"}',
                "unknown_write_outcome": True,
            },
            {"status": 500, "body": "x" * 1024},
        ):
            with self.assertRaises(AssertionError) as raised:
                dispatch.require_success(result)
            diagnostic = json.loads(str(raised.exception).split(": ", 1)[1])
            self.assertEqual(diagnostic["status"], result["status"])
            self.assertEqual(diagnostic["body_prefix"], result["body"][:512])
            self.assertEqual(diagnostic["body_truncated"], len(result["body"]) > 512)
            self.assertEqual(diagnostic["error"], result.get("error"))

    def test_head_requires_success_and_empty_client_body(self):
        dispatch.require_head_success(
            {"status": 200, "body": "", "headers": {"Content-Length": "20"}}
        )
        for result in (
            {"status": 405, "body": ""},
            {"status": 200, "body": "unexpected"},
            {"status": 200, "body": "", "error": "timeout"},
            {"status": 200},
        ):
            with self.assertRaises(AssertionError):
                dispatch.require_head_success(result)

    def test_all_held_clients_must_overlap_protected_probes(self):
        dispatch.require_overlap(32, 32)
        for before, after in ((31, 32), (32, 31), (0, 0), (33, 33)):
            with self.assertRaises(AssertionError):
                dispatch.require_overlap(before, after)

    def test_probes_verify_nonce_membership_protocol_and_signature(self):
        cluster = object.__new__(dispatch.DispatchCluster)
        cluster.nodes = {"api": {"node_id": 3}}
        cluster.secret, cluster.issuer = "x" * 64, "test"
        captured = []
        cluster.record = captured.append
        tamper = [None]
        seen_heads = []

        def probe(method, path, *, body=None, headers=None):
            if path == dispatch.LOOKUP or (
                path == dispatch.CONTROL and headers is None
            ):
                return denial()
            if method == "HEAD":
                seen_heads.append(path)
                return {"status": 200, "body": ""}
            if path != dispatch.CONTROL:
                return {
                    "status": 200,
                    "body": json.dumps(
                        {"status": "ok" if path == "/healthz" else "ready"}
                    ),
                }
            self.assertIn("X-Antfly-Trusted-Principal", headers)
            proof = {
                "version": 1,
                "protocol_version": 3,
                "coordinator": 7,
                "destination": 3,
                "nonce": body["nonce"],
                "worker_namespace": 45,
                "worker_incarnation": 8,
            }
            if tamper[0] and tamper[0] != "signature":
                proof[tamper[0]] += 1
            frame = evidence.sign(
                cluster.secret if tamper[0] != "signature" else "y" * 64,
                cluster.issuer,
                "antfly-workload-discovery-v1",
                proof,
            )
            return {
                "status": 200,
                "headers": {"X-Antfly-Workload-Evidence": frame},
                "body": "{}",
            }

        cluster.probe = probe
        cluster.protected_probes()
        self.assertEqual(seen_heads, ["/healthz", "/readyz"])
        self.assertEqual(len(captured), 1)
        for field in (
            "nonce",
            "coordinator",
            "destination",
            "protocol_version",
            "signature",
        ):
            tamper[0] = field
            with self.assertRaises(ValueError):
                cluster.protected_probes()
        self.assertEqual(len(captured), 1)

    def test_manifest_includes_data_node_logs_but_prunes_node_storage(self):
        included = {
            "plan.json",
            "receipt.json",
            "events.jsonl",
            "runner.py",
            "data/server-1.log",
            "data/server-2.log",
            "data/config.json",
            "data/catalog.txt",
            "api/server-1.log",
            "metadata/config.json",
            "evidence/data/diagnostic.log",
        }
        excluded = {
            "checksums.json",
            "artifacts/candidate",
            "artifacts/build.json",
            "data/data/wal/huge.log",
            "data/replicas/group/huge.log",
            "data/snapshots/state.json",
            "api/data/huge.log",
            "metadata/data/wal/huge.log",
            "metadata/replicas/config.json",
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            for name in included | excluded:
                path = output / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(name)
            hashed = []

            def checksum(path):
                name = str(path.relative_to(output))
                self.assertNotIn(name, excluded)
                hashed.append(name)
                return "digest:" + name

            with patch.object(runner.q, "checksum", checksum):
                manifest = dispatch.receipt_manifest(
                    output, {"data", "api", "metadata"}
                )
            self.assertEqual(set(manifest), included)
            self.assertEqual(set(hashed), included)
            self.assertEqual(manifest["data/server-1.log"], "digest:data/server-1.log")

    def test_receipts_do_not_retain_signing_secret(self):
        secret = "synthetic-secret-" * 4
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)

            def init(cluster, plan, output):
                cluster.secret, cluster.issuer = secret, "test"
                runner.q.save(output / "local-test-auth.json", {"secret": secret})

            with patch.object(dispatch.frontend.FrontendCluster, "__init__", init):
                cluster = dispatch.DispatchCluster({}, output)
            self.assertNotIn(secret, (output / "local-test-auth.json").read_text())
            captured = []
            with patch.object(
                dispatch.frontend.FrontendCluster,
                "record",
                lambda _, event: captured.append(event),
            ):
                cluster.record({"nested": {"message": secret}})
            self.assertNotIn(secret, json.dumps(captured))

    def test_corrupt_metric_evidence_cannot_pass_after_later_valid_sample(self):
        cluster = object.__new__(dispatch.DispatchCluster)
        plan = dispatch.make_plan(Path("/usr/bin/true"), "a" * 40, "Debug")
        cluster.plan, cluster.nodes, cluster.ports = (
            plan,
            {"api": plan["nodes"][-1]},
            {"api": {"health": 1234}},
        )
        cluster.record = lambda event: None

        def poll(port, action, submitted, emit):
            emit({"evidence_error": "HTML instead of metrics"})
            emit({"observed": {dispatch.ACTIVE: 32}})
            return {"passed": True}

        with patch.object(runner, "poll_metrics", poll), self.assertRaises(
            AssertionError
        ):
            cluster.sampled_phase("saturated")


if __name__ == "__main__":
    unittest.main()
