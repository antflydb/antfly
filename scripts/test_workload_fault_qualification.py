import hashlib
import http.client
import http.server
import json
import socket
import threading
import time
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

import workload_cluster_qualification as cluster

import workload_attempt_evidence as evidence
from workload_fault_proxy import FaultProxy


class AttemptEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.secret, self.issuer = "s" * 32, "fixture"
        self.attempt = {
            "coordinator": 3,
            "generation": 2,
            "sequence": 4,
            "operation": 55,
            "destination": 2,
            "worker_incarnation": 7,
            "worker_namespace": 999,
        }

    def test_request_digest_and_namespaced_terminal_bind_exact_work(self):
        frame = evidence.sign_request(
            self.secret,
            self.issuer,
            self.attempt,
            "POST",
            "/internal/v1/join",
            b"{}",
            1000,
        )
        value = evidence.verify(
            self.secret, self.issuer, "antfly-workload-request-v1", frame
        )
        self.assertEqual(value["version"], 3)
        self.assertEqual(
            value["request_digest"],
            list(
                evidence.framed(
                    (b"POST", b"/internal/v1/join", b"{}"), hashlib.sha256()
                )
            ),
        )
        payload = {
            "version": 1,
            "attempt": self.attempt,
            "status": 200,
            "response_digest": list(hashlib.sha256(b"{}").digest()),
        }
        terminal = evidence.sign(
            self.secret, self.issuer, "antfly-workload-terminal-v1", payload
        )
        self.assertEqual(
            evidence.terminal(
                self.secret, self.issuer, terminal, self.attempt, 200, b"{}"
            ),
            payload,
        )
        for changed in (
            {**self.attempt, "worker_namespace": 1000},
            {**self.attempt, "worker_incarnation": 8},
            {**self.attempt, "sequence": 5},
        ):
            with self.assertRaises(ValueError):
                evidence.terminal(
                    self.secret, self.issuer, terminal, changed, 200, b"{}"
                )
        with self.assertRaises(ValueError):
            evidence.terminal(
                self.secret, self.issuer, terminal, self.attempt, 200, b"changed"
            )
        with self.assertRaises(ValueError):
            evidence.sign_request(
                self.secret,
                self.issuer,
                {**self.attempt, "worker_namespace": 0},
                "POST",
                "/",
                b"",
                1000,
            )

    def test_fence_does_not_retire_foreign_namespace_or_unquiesced_generation(self):
        value = {
            "version": 1,
            **{
                field: self.attempt[field]
                for field in (
                    "coordinator",
                    "destination",
                    "worker_incarnation",
                    "worker_namespace",
                )
            },
            "fenced_through": 3,
            "quiesced_through": 2,
        }
        frame = evidence.sign(
            self.secret, self.issuer, "antfly-workload-fence-v1", value
        )
        self.assertEqual(
            evidence.fence(self.secret, self.issuer, frame, self.attempt), value
        )
        for changes in (
            {"worker_namespace": 2},
            {"quiesced_through": 1},
            {"quiesced_through": 4},
        ):
            invalid = evidence.sign(
                self.secret,
                self.issuer,
                "antfly-workload-fence-v1",
                {**value, **changes},
            )
            with self.assertRaises(ValueError):
                evidence.fence(self.secret, self.issuer, invalid, self.attempt)


class SignedRunnerTests(unittest.TestCase):
    def test_lost_response_stays_charged_until_exact_signed_status(self):
        class FakeCluster:
            def __init__(self, plan, output):
                self.nodes = {node["name"]: node for node in plan["nodes"]}
                self.ports = {name: {"api": 1} for name in self.nodes}
                self.processes, self.expected_stopped = {}, set()
                self.secret, self.issuer = "s" * 32, "fixture"

            def start(self, *_):
                pass

            def close(self):
                return []

            def record(self, _):
                pass

        binary = Path("/usr/bin/true")
        plan = cluster.template()
        plan["artifacts"]["candidate"].update(
            binary=str(binary), sha256=cluster.q.checksum(binary), revision="a" * 40
        )
        plan["actions"] = [
            {
                "at": 0,
                "action": "discover",
                "node": "data",
                "coordinator": 3,
                "id": "worker",
            },
            {
                "at": 0,
                "action": "request",
                "node": "data",
                "id": "a1",
                "method": "POST",
                "path": "/internal/v1/join",
                "body": {},
                "is_write": False,
                "expect": {"transport_error": True},
                "attempt": {
                    "from_discovery": "worker",
                    "generation": 1,
                    "sequence": 1,
                    "operation": 7,
                },
            },
            {"at": 0, "action": "attempt_status", "node": "data", "attempt_ref": "a1"},
        ]

        def request(_port, action, _timeout, headers=None, submitted=None):
            body = action.get("body", {})
            if body.get("workload_attempt_control") == "discover":
                value = {
                    "version": 1,
                    "protocol_version": 3,
                    "coordinator": 3,
                    "destination": 2,
                    "worker_namespace": 999,
                    "worker_incarnation": 7,
                    "nonce": body["nonce"],
                }
                frame = evidence.sign(
                    "s" * 32, "fixture", "antfly-workload-discovery-v1", value
                )
            else:
                signed = evidence.verify(
                    "s" * 32,
                    "fixture",
                    "antfly-workload-request-v1",
                    headers["X-Antfly-Workload-Attempt"],
                )
                self.assertEqual(signed["attempt"]["worker_namespace"], 999)
                if body.get("workload_attempt_control") != "status":
                    return {
                        "passed": True,
                        "error": "deliberately lost response",
                        "is_write": False,
                    }
                value = {
                    "version": 1,
                    "attempt": signed["attempt"],
                    "status": 200,
                    "response_digest": list(hashlib.sha256(b"{}").digest()),
                }
                frame = evidence.sign(
                    "s" * 32, "fixture", "antfly-workload-terminal-v1", value
                )
            return {
                "passed": True,
                "body": "{}",
                "headers": {"X-Antfly-Workload-Evidence": frame},
                "status": 200,
                "is_write": False,
            }

        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(cluster, "Cluster", FakeCluster),
            patch.object(cluster, "request", request),
        ):
            complete = cluster.run(plan, Path(tmp) / "complete")
            self.assertTrue(complete["correctness_passed"])
            self.assertEqual(
                complete["fixture_protocol_obligations"], {"a1": "verified_terminal"}
            )
            incomplete = cluster.run(
                {**plan, "actions": plan["actions"][:-1]}, Path(tmp) / "missing-proof"
            )
            self.assertFalse(incomplete["correctness_passed"])
            self.assertEqual(
                incomplete["fixture_protocol_obligations"], {"a1": "unproven"}
            )


class FaultProxyTests(unittest.TestCase):
    def setUp(self):
        self.receipts = []
        self.served = []
        served = self.served

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                served.append(self.path)
                body = b"x" * (200000 if self.path == "/large" else 128)
                if self.path == "/diagnostic":
                    body = b"request deadline exceeded fixture-secret"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.send_header("Authorization", "Bearer credential-must-not-leak")
                self.send_header("X-Antfly-Workload-Evidence", "signed-fixture-proof")
                self.end_headers()
                self.wfile.write(body)
                self.close_connection = True

            def log_message(self, *_):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        self.port = listener.getsockname()[1]
        self.proxy = FaultProxy(
            listener,
            self.server.server_port,
            self.receipts.append,
            "worker",
            buffer_bytes=4096,
            capture_response_bytes=2048,
            redact_values=("fixture-secret",),
        )

    def tearDown(self):
        self.proxy.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def get(self, path="/", timeout=2):
        client = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        try:
            client.request("GET", path)
            response = client.getresponse()
            return response.status, response.read()
        finally:
            client.close()

    def test_bounded_response_capture_keeps_proof_and_redacts_credentials(self):
        self.assertEqual(self.get("/diagnostic")[0], 200)
        deadline = time.monotonic() + 1
        while not any(
            row["event"] == "proxy_response_capture" for row in self.receipts
        ):
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.005)
        capture = next(
            row for row in self.receipts if row["event"] == "proxy_response_capture"
        )
        self.assertIn("200", capture["status_line"])
        self.assertIn(
            ["X-Antfly-Workload-Evidence", "signed-fixture-proof"], capture["headers"]
        )
        self.assertEqual(capture["body_prefix"], "request deadline exceeded [REDACTED]")
        self.assertFalse(capture["truncated"])
        self.assertNotIn("credential-must-not-leak", json.dumps(capture))

    def test_request_paths_are_redacted_in_all_receipts(self):
        self.assertEqual(self.get("/fixture-secret?token=fixture-secret")[0], 200)
        deadline = time.monotonic() + 1
        while not any(
            row["event"] == "proxy_response_capture" for row in self.receipts
        ):
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.005)
        self.assertNotIn("fixture-secret", json.dumps(self.receipts))
        self.assertNotIn("fixture-secret", json.dumps(self.proxy.snapshot()))
        capture = next(
            row for row in self.receipts if row["event"] == "proxy_response_capture"
        )
        transport = capture["transport"]
        self.assertGreater(transport["request_first_forwarded_ns"], 0)
        self.assertGreaterEqual(
            transport["response_last_forwarded_ns"],
            transport["request_first_forwarded_ns"],
        )
        self.assertEqual(transport["response_dropped_bytes"], 0)
        self.assertEqual(
            transport["response_forwarded_bytes"], capture["received_bytes"]
        )

    def test_healthy_delayed_half_close_drains_bounded_queues(self):
        self.proxy.set_policy(delay_ms=20)
        started = time.monotonic()
        status, body = self.get("/large")
        self.assertEqual(status, 200)
        self.assertEqual(len(body), 200000)
        self.assertGreaterEqual(time.monotonic() - started, 0.035)
        snapshot = self.proxy.snapshot()
        self.assertGreater(snapshot["forwarded_upstream_bytes"], 0)
        self.assertGreaterEqual(snapshot["forwarded_downstream_bytes"], 200000)
        self.assertEqual(snapshot["paths"]["/large"], 1)
        self.assertIsNone(snapshot["error"])
        deadline = time.monotonic() + 1
        while not any(
            row["event"] == "proxy_response_capture" for row in self.receipts
        ):
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.005)
        capture = next(
            row for row in self.receipts if row["event"] == "proxy_response_capture"
        )
        self.assertEqual(capture["captured_bytes"], 2048)
        self.assertTrue(capture["truncated"])
        self.assertGreaterEqual(capture["received_bytes"], 200000)

    def test_partition_and_response_discard_have_distinct_receipts_and_recover(self):
        self.proxy.set_policy(partition=True)
        with self.assertRaises((OSError, http.client.HTTPException)):
            self.get("/partition", timeout=0.2)
        self.assertNotIn("/partition", self.served)
        self.proxy.set_policy(drop_response=True)
        with self.assertRaises((OSError, http.client.HTTPException)):
            self.get("/dropped", timeout=0.2)
        self.assertIn("/dropped", self.served)
        snapshot = self.proxy.snapshot()
        self.assertGreater(snapshot["partition_rejections"], 0)
        self.assertGreater(snapshot["dropped_response_bytes"], 0)
        self.proxy.set_policy()
        self.assertEqual(self.get("/healed")[0], 200)
        self.assertFalse(self.proxy.snapshot()["policy"]["drop_response"])


class ProxyEvidenceBudgetTests(unittest.TestCase):
    def test_aggregate_event_and_byte_limits_fail_explicitly(self):
        for options in ({"max_evidence_events": 2}, {"max_evidence_bytes": 32}):
            with self.subTest(options=options):
                listener = socket.socket()
                listener.bind(("127.0.0.1", 0))
                receipts = []
                proxy = FaultProxy(listener, 1, receipts.append, "budget", **options)
                try:
                    for _ in range(100):
                        proxy.record({"event": "fixture", "body": "x" * 40})
                    self.assertEqual(
                        sum(row["event"] == "proxy_evidence_limit" for row in receipts),
                        1,
                    )
                    self.assertLessEqual(
                        proxy.evidence_events, proxy.max_evidence_events
                    )
                    self.assertLessEqual(proxy.evidence_bytes, proxy.max_evidence_bytes)
                    self.assertEqual(
                        proxy.snapshot()["error"], "proxy evidence budget exhausted"
                    )
                    with self.assertRaisesRegex(
                        RuntimeError, "evidence budget exhausted"
                    ):
                        proxy.close()
                finally:
                    proxy.stopping.set()
                    proxy.thread.join(timeout=2)
                    listener.close()


if __name__ == "__main__":
    unittest.main()
