import base64
import copy
import hashlib
import json
import unittest

import workload_attempt_evidence as protocol
import workload_proxy_evidence as evidence


class ProxyProofTests(unittest.TestCase):
    def setUp(self):
        self.secret, self.issuer = "s" * 32, "local-fixture"
        self.attempt = {
            "coordinator": 3,
            "destination": 2,
            "generation": 2,
            "sequence": 1,
            "operation": 123,
            "worker_namespace": 999,
            "worker_incarnation": 7,
        }

    def message(self, line, body, **headers):
        raw = (
            line
            + "\r\n"
            + "".join(f"{key}: {value}\r\n" for key, value in headers.items())
            + f"Content-Length: {len(body)}\r\n\r\n"
        ).encode() + body
        return evidence.capture_message(raw, len(raw)), raw

    def terminal_pair(self, attempt=None):
        attempt = attempt or self.attempt
        path = "/internal/v1/groups/1/tables/t/documents/a"
        request_frame = protocol.sign_request(
            self.secret, self.issuer, attempt, "GET", path, b"", 1000000
        )
        request, _ = self.message(
            f"GET {path} HTTP/1.1", b"", **{"X-Antfly-Workload-Attempt": request_frame}
        )
        body = b'{"id":"a"}'
        proof = {
            "version": 1,
            "attempt": attempt,
            "status": 200,
            "response_digest": list(hashlib.sha256(body).digest()),
        }
        frame = protocol.sign(
            self.secret, self.issuer, "antfly-workload-terminal-v1", proof
        )
        response, _ = self.message(
            "HTTP/1.1 200 OK", body, **{"X-Antfly-Workload-Evidence": frame}
        )
        return request, response

    def verify(self, request, response):
        return evidence.verify_exchange(
            request, response, self.secret, self.issuer, 3, 2
        )

    def observed(self, request, response, connection, first, last):
        return evidence.verify_capture(
            {
                "event": "proxy_response_capture",
                "proxy": "worker",
                "connection": connection,
                "request_message": request,
                "response_message": response,
                "transport": {
                    "observer": "one-relay-lifetime",
                    "request_first_received_ns": first,
                    "request_first_forwarded_ns": first,
                    "request_forwarded_bytes": request["received_bytes"],
                    "response_last_forwarded_ns": last,
                    "response_forwarded_bytes": response["received_bytes"],
                    "response_dropped_bytes": 0,
                },
            },
            self.secret,
            self.issuer,
            3,
            2,
        )

    def test_terminal_requires_exact_request_and_response(self):
        request, response = self.terminal_pair()
        self.assertEqual(self.verify(request, response)["attempt"], self.attempt)
        for field, value in (
            ("start_line", "HTTP/1.1 503 Unavailable"),
            ("body_base64", base64.b64encode(b"wrong").decode()),
        ):
            altered = {**response, field: value}
            with self.assertRaises(ValueError):
                self.verify(request, altered)
        changed_path = {
            **request,
            "start_line": request["start_line"].replace("documents/a", "documents/b"),
        }
        with self.assertRaises(ValueError):
            self.verify(changed_path, response)
        other, _ = self.terminal_pair({**self.attempt, "sequence": 2})
        with self.assertRaises(ValueError):
            self.verify(other, response)
        forged = copy.deepcopy(response)
        forged["headers"]["x-antfly-workload-evidence"] += "x"
        with self.assertRaises(ValueError):
            self.verify(request, forged)

    def test_capture_rejects_truncated_pipeline_and_credentials(self):
        captured, raw = self.message(
            "HTTP/1.1 200 OK",
            b"payload",
            Authorization="secret",
            **{"Set-Cookie": "secret"},
        )
        self.assertTrue(captured["complete"])
        self.assertNotIn("secret", json.dumps(captured))
        self.assertFalse(evidence.capture_message(raw[:-1], len(raw))["complete"])
        self.assertFalse(evidence.capture_message(raw + raw, 2 * len(raw))["complete"])
        self.assertFalse(
            evidence.capture_message(raw, len(raw), ("payload",))["complete"]
        )

    def test_discovery_nonce_and_generation_fence_binding(self):
        body = b'{"workload_attempt_control":"discover","nonce":123}'
        request, _ = self.message("POST /internal/v1/workload/control HTTP/1.1", body)
        proof = {
            "version": 1,
            "protocol_version": 3,
            "coordinator": 3,
            "destination": 2,
            "worker_namespace": 999,
            "worker_incarnation": 7,
            "nonce": 123,
        }
        frame = protocol.sign(
            self.secret, self.issuer, "antfly-workload-discovery-v1", proof
        )
        response, _ = self.message(
            "HTTP/1.1 200 OK", b"{}", **{"X-Antfly-Workload-Evidence": frame}
        )
        self.assertEqual(self.verify(request, response)["kind"], "discovery")
        request["body_base64"] = base64.b64encode(body.replace(b"123", b"124")).decode()
        with self.assertRaises(ValueError):
            self.verify(request, response)
        old = self.observed(*self.terminal_pair(), 1, 10, 20)
        body = b'{"workload_attempt_control":"close_generation"}'
        frame = protocol.sign_request(
            self.secret,
            self.issuer,
            self.attempt,
            "POST",
            "/internal/v1/workload/control",
            body,
            1000000,
        )
        request, _ = self.message(
            "POST /internal/v1/workload/control HTTP/1.1",
            body,
            **{"X-Antfly-Workload-Attempt": frame},
        )
        fence = {
            key: self.attempt[key]
            for key in (
                "coordinator",
                "destination",
                "worker_namespace",
                "worker_incarnation",
            )
        }
        fence.update(version=1, fenced_through=2, quiesced_through=2)
        frame = protocol.sign(
            self.secret, self.issuer, "antfly-workload-fence-v1", fence
        )
        response, _ = self.message(
            "HTTP/1.1 200 OK", b"{}", **{"X-Antfly-Workload-Evidence": frame}
        )
        closure = self.observed(request, response, 2, 30, 40)
        following = self.observed(
            *self.terminal_pair({**self.attempt, "generation": 3}), 3, 50, 60
        )
        self.assertEqual(
            evidence.verify_generation_closure(old, closure, following)[
                "closed_generation"
            ],
            2,
        )
        # Cryptographic coverage alone cannot establish observation order.
        unobserved = {
            key: value for key, value in closure.items() if key != "observation"
        }
        self.assertEqual(
            evidence.verify_generation_coverage(old, unobserved, following)[
                "closed_generation"
            ],
            2,
        )
        with self.assertRaises(ValueError):
            evidence.verify_generation_closure(old, unobserved, following)
        for changes in (
            {"response_last_forwarded_ns": 55},  # newer request already sent
            {"response_dropped_bytes": 1, "response_forwarded_bytes": 0},
            {"response_forwarded_bytes": 0},  # observed but never delivered
            {"observer": "different-relay-lifetime"},
        ):
            altered = {**closure, "observation": {**closure["observation"], **changes}}
            with self.assertRaises(ValueError):
                evidence.verify_generation_closure(old, altered, following)
        buffered_early = {
            **following,
            "observation": {
                **following["observation"],
                "request_first_received_ns": 35,
            },
        }
        with self.assertRaises(ValueError):
            evidence.verify_generation_closure(old, closure, buffered_early)
        for changed in (
            {**following, "attempt": {**following["attempt"], "generation": 2}},
            {
                **following,
                "attempt": {**following["attempt"], "worker_namespace": 1000},
            },
        ):
            with self.assertRaises(ValueError):
                evidence.verify_generation_closure(old, closure, changed)


if __name__ == "__main__":
    unittest.main()
