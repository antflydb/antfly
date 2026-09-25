import base64
import hashlib
import hmac
import json
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import workload_cluster_qualification as cluster


class ClusterTests(unittest.TestCase):
    def plan(self):
        plan = cluster.template()
        binary = Path("/usr/bin/true")
        plan["artifacts"]["candidate"].update(
            binary=str(binary), sha256=cluster.q.checksum(binary), revision="a" * 40
        )
        return plan

    def test_plan_bounds_and_rolling_artifact_pins(self):
        plan = self.plan()
        cluster.validate(plan)
        for fault in (
            {"at": 0, "action": "shell", "node": "data"},
            {"at": -1, "action": "kill", "node": "data"},
            {"at": 0, "action": "kill", "node": "unowned"},
            {"at": 0, "action": "restart", "node": "data", "artifact": "unattested"},
        ):
            with self.subTest(fault=fault):
                with self.assertRaises(ValueError):
                    cluster.validate({**plan, "actions": [fault]})

    def test_topology_commands_keep_process_storage_and_endpoints_isolated(self):
        plan = self.plan()
        ports = {
            "metadata": {"api": 1, "raft": 2, "health": 3},
            "data": {"api": 4, "raft": 5, "health": 6},
        }
        argv = cluster.node_command(
            plan["nodes"][1],
            "/frozen/binary",
            Path("/owned/data"),
            ports["data"],
            ports,
        )
        self.assertEqual(argv[:2], ["/frozen/binary", "data"])
        self.assertEqual(argv[argv.index("--metadata-api") + 1], "http://127.0.0.1:1")
        self.assertEqual(argv[argv.index("--data-dir") + 1], "/owned/data/data")
        self.assertEqual(argv[argv.index("--store-id") + 1], "2")
        self.assertEqual(argv[argv.index("--health-port") + 1], "6")

    def test_discovery_hmac_binds_nonce_membership_protocol_and_issuer(self):
        secret = "s" * 32
        value = {
            "version": 1,
            "protocol_version": 2,
            "coordinator": 3,
            "destination": 2,
            "worker_incarnation": 9,
            "nonce": 123,
        }
        payload = cluster.encode(value)
        mac = hmac.new(secret.encode(), digestmod=hashlib.sha256)
        for part in (b"antfly-workload-discovery-v1", b"test", payload):
            mac.update(len(part).to_bytes(8, "big"))
            mac.update(part)
        frame = (
            payload + b"." + base64.urlsafe_b64encode(mac.digest()).rstrip(b"=")
        ).decode()
        self.assertEqual(
            cluster.verify_discovery(frame, secret, "test", 3, 2, 123), value
        )
        for coordinator, destination, nonce in ((4, 2, 123), (3, 4, 123), (3, 2, 124)):
            with self.assertRaises(ValueError):
                cluster.verify_discovery(
                    frame, secret, "test", coordinator, destination, nonce
                )
        with self.assertRaises(ValueError):
            cluster.verify_discovery(frame, secret, "wrong", 3, 2, 123)
        token = cluster.service_headers(secret, "test", 3)["X-Antfly-Trusted-Principal"]
        payload = token.split(".")[1]
        self.assertEqual(
            json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))[
                "sub"
            ],
            "node:3",
        )

    def test_expected_transport_failure_does_not_erase_unknown_write(self):
        class Connection:
            def __init__(self, *_, **__):
                pass

            def request(self, *_):
                raise OSError("lost acknowledgement")

            def close(self):
                pass

        with patch.object(cluster.http.client, "HTTPConnection", Connection):
            result = cluster.request(
                1,
                {
                    "method": "POST",
                    "path": "/db/v1/tables/t/batch",
                    "is_write": True,
                    "expect": {"transport_error": True},
                },
                1,
            )
        self.assertTrue(result["passed"])
        self.assertTrue(result["unknown_write_outcome"])

    def test_explicit_unknown_mutation_header_remains_unresolved_even_if_status_expected(
        self,
    ):
        class Response:
            status = 409
            chunks = [b"observe table state", b""]

            def read1(self, _):
                return self.chunks.pop(0)

            def getheaders(self):
                return [("X-Antfly-Raft-Mutation-Outcome", "unknown-v1")]

        class Connection:
            sock = None

            def __init__(self, *args, **kwargs):
                pass

            def request(self, *args, **kwargs):
                pass

            def getresponse(self):
                return Response()

            def close(self):
                pass

        with patch.object(cluster.http.client, "HTTPConnection", Connection):
            result = cluster.request(
                1,
                {
                    "method": "POST",
                    "path": "/db/v1/tables/t",
                    "is_write": True,
                    "body": {},
                    "expect": {"status": 409},
                },
                1,
            )
        self.assertTrue(result["passed"])
        self.assertTrue(result["unknown_write_outcome"])

    def test_unknown_setup_observation_only_reads_catalog_and_never_resolves_write(
        self,
    ):
        class Owned:
            nodes = {"api": {"metadata": "metadata"}}
            ports = {"api": {"api": 1}, "metadata": {"api": 2}}
            events = []

            def record(self, row):
                self.events.append(row)

        owned = Owned()
        calls = []

        def observe(port, action, timeout, **kwargs):
            calls.append((port, action, timeout))
            return {"passed": True, "status": 200, "body": "[]"}

        with patch.object(cluster, "request", side_effect=observe):
            rows = cluster.observe_unknown_setup(
                owned,
                {
                    "node": "api",
                    "path": "/db/v1/tables/t/batch",
                    "is_write": True,
                },
                30,
            )
        self.assertEqual(len(rows), 3)
        self.assertEqual(calls[1][1]["path"], "/db/v1/tables/t")
        self.assertTrue(
            all(
                action["method"] == "GET" and not action["is_write"] and timeout <= 2
                for _, action, timeout in calls
            )
        )
        self.assertTrue(all(row["original_mutation_remains_unknown"] for row in rows))

    def test_queued_request_does_not_restart_original_budget(self):
        class Connection:
            sent = 0

            def __init__(self, *_, **__):
                pass

            def request(self, *_):
                self.__class__.sent += 1

            def close(self):
                pass

        action = {
            "method": "POST",
            "path": "/db/v1/tables/t/batch",
            "is_write": True,
            "expect": {"transport_error": True},
        }
        with (
            patch.object(cluster.http.client, "HTTPConnection", Connection),
            patch.object(cluster.time, "monotonic", return_value=20),
        ):
            result = cluster.request(1, action, 1, submitted=10)
        self.assertEqual(Connection.sent, 0)
        self.assertEqual(result["client_wait_ms"], 10000)
        self.assertFalse(result["unknown_write_outcome"])
        self.assertIn("before dispatch", result["error"])

    def test_paused_process_is_resumed_before_cleanup_and_forced_kill_fails(self):
        class Process:
            pid = 123
            returncode = None
            signals = []

            def poll(self):
                return self.returncode

            def send_signal(self, value):
                self.signals.append(value)

            def terminate(self):
                self.signals.append(signal.SIGTERM)

            def wait(self, timeout):
                if self.returncode is None:
                    raise subprocess.TimeoutExpired("owned", timeout)

            def kill(self):
                self.signals.append(signal.SIGKILL)
                self.returncode = -signal.SIGKILL

        owner = object.__new__(cluster.Cluster)
        process = Process()
        owner.processes = {"data": process}
        owner.paused = {"data"}
        owner.expected_stopped = set()
        owner.logs = {}
        owner.record = lambda event: None
        with self.assertRaises(RuntimeError):
            owner.stop("data", cleanup=True)
        self.assertEqual(
            process.signals, [signal.SIGCONT, signal.SIGTERM, signal.SIGKILL]
        )

    def test_artifact_mismatch_retains_failure_and_checksums_without_starting(self):
        plan = self.plan()
        plan["artifacts"]["candidate"]["sha256"] = "0" * 64
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(cluster, "Cluster") as constructor,
        ):
            output = Path(tmp) / "evidence"
            result = cluster.run(plan, output)
            self.assertFalse(result["correctness_passed"])
            self.assertIn("SHA256 mismatch", result["error"])
            self.assertFalse(result["performance_qualified"])
            constructor.assert_not_called()
            self.assertIn(
                "summary.json", json.loads((output / "checksums.json").read_text())
            )


if __name__ == "__main__":
    unittest.main()
