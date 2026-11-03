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

"""Transient recovery keeps the original construction deadline and admission errors."""

import importlib.util
import datetime
import json
import subprocess
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "hackernews_poc", Path(__file__).resolve().parents[1] / "poc.py"
)
poc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(poc)


class ConstructionRecoveryTests(unittest.TestCase):
    def test_startup_transports_retry_but_terminal_http_and_deadline_do_not(self):
        clock = [0.0]
        calls = []
        failures = [
            poc.RequestFailure.transport(
                "GET", "/tables", ConnectionRefusedError(61, "startup"), 1
            ),
            poc.RequestFailure.http("GET", "/tables", 503, b"{}", 1),
            None,
        ]

        def ready(timeout):
            calls.append(timeout)
            failure = failures.pop(0)
            if failure:
                raise failure

        def sleep(seconds):
            clock[0] += seconds

        poc.wait_for_server(ready, 3, lambda: None, now=lambda: clock[0], sleep=sleep)
        self.assertEqual(clock[0], 1)
        self.assertEqual(calls, [2, 2, 2])
        denied = poc.RequestFailure.http("GET", "/tables", 403, b"{}", 1)
        with self.assertRaises(poc.RequestFailure) as error:
            poc.wait_for_server(
                lambda _: (_ for _ in ()).throw(denied),
                3,
                lambda: None,
                now=lambda: clock[0],
                sleep=sleep,
            )
        self.assertIs(error.exception, denied)
        self.assertEqual(clock[0], 1)
        unavailable = poc.RequestFailure.transport(
            "GET", "/tables", ConnectionRefusedError(), 1
        )
        with self.assertRaisesRegex(RuntimeError, "startup timed out"):
            poc.wait_for_server(
                lambda _: (_ for _ in ()).throw(unavailable),
                2,
                lambda: None,
                now=lambda: clock[0],
                sleep=sleep,
            )
        self.assertEqual(clock[0], 2)

    def test_timeout_evidence_keeps_duration_and_code_without_provider_body(self):
        failure = poc.RequestFailure.http(
            "POST",
            "/query?token=private-token",
            504,
            json.dumps(
                {
                    "code": "query_timeout",
                    "retryable": True,
                    "message": "private-token gs://private-bucket/object",
                }
            ),
            30005.25,
        )
        self.assertEqual(failure.record["route"], "/query")
        self.assertEqual(failure.record["http_status"], 504)
        self.assertEqual(failure.record["code"], "query_timeout")
        self.assertEqual(failure.record["elapsed_ms"], 30005.25)
        self.assertTrue(failure.record["retryable"])
        self.assertNotIn("private", json.dumps(failure.record) + str(failure))
        for body in (
            b"private-token invalid JSON",
            json.dumps({"code": {"token": "private-token"}}),
            json.dumps({"code": "private_token", "retryable": "private-token"}),
        ):
            unknown = poc.RequestFailure.http("POST", "/query", 502, body, 10)
            self.assertEqual(unknown.record["code"], "http_error")
            self.assertNotIn("private", json.dumps(unknown.record) + str(unknown))

    def test_transport_evidence_preserves_errno_without_exception_text(self):
        failure = poc.RequestFailure.transport(
            "POST",
            "/query",
            poc.urllib.error.URLError(ConnectionResetError(54, "private-token")),
            4123.5,
        )
        self.assertEqual(failure.record["transport_type"], "ConnectionResetError")
        self.assertEqual(failure.record["errno"], 54)
        self.assertEqual(failure.record["elapsed_ms"], 4123.5)
        self.assertNotIn("private", json.dumps(failure.record) + str(failure))

    def test_gcs_credentials_require_lifetime_and_redact_helper_failures(self):
        expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            hours=1
        )
        body = json.dumps(
            {
                "credential": {
                    "access_token": "private-token",
                    "token_expiry": expiry.isoformat(),
                }
            }
        )
        with patch.object(
            poc.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, body)
        ) as run:
            self.assertEqual(poc.gcs_bearer("dev", 1800), "private-token")
            self.assertIn("1800s", run.call_args.args[0])
            self.assertTrue(run.call_args.kwargs["capture_output"])
        expired = json.dumps(
            {
                "credential": {
                    "access_token": "private-token",
                    "token_expiry": "2000-01-01T00:00:00Z",
                }
            }
        )
        with patch.object(
            poc.subprocess,
            "run",
            return_value=subprocess.CompletedProcess([], 0, expired),
        ):
            with self.assertRaisesRegex(
                RuntimeError, "sufficient remaining lifetime"
            ) as error:
                poc.gcs_bearer("dev")
            self.assertNotIn("private-token", str(error.exception))
        with patch.object(
            poc.subprocess,
            "run",
            side_effect=subprocess.CalledProcessError(
                1, "gcloud", output="private-token", stderr="secret"
            ),
        ):
            with self.assertRaises(RuntimeError) as error:
                poc.gcs_bearer("dev")
            self.assertNotIn("private-token", str(error.exception))

    def run_sequence(self, statuses, deadline=10):
        clock = [0]
        observed = {}
        polls = []

        def read():
            polls.append(clock[0])
            return statuses[min(len(polls) - 1, len(statuses) - 1)]

        def sleep(seconds):
            clock[0] += seconds

        result = poc.wait_for_index(
            read, deadline, lambda: None, observed, now=lambda: clock[0], sleep=sleep
        )
        return result, observed, polls

    def test_flat_source_is_staged_without_mutating_input(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, state = root / "input", root / "state"
            (source / "archive").mkdir(parents=True)
            parquet = source / "archive" / "part.parquet"
            parquet.write_bytes(b"unchanged input")
            staged = poc.prepare_filesystem_source(source, state)
            target = (
                staged / "archive" / "buckets" / "antfly" / "objects" / parquet.name
            )
            self.assertEqual(target.read_bytes()[:8], b"AFOBJ001")
            self.assertEqual(target.read_bytes()[84:], parquet.read_bytes())
            self.assertFalse((source / "archive" / "buckets").exists())
            self.assertEqual(poc.prepare_filesystem_source(source, state), staged)
            parquet.write_bytes(b"changed input")
            with self.assertRaisesRegex(ValueError, "Staged source changed"):
                poc.prepare_filesystem_source(source, state)

    def test_timeout_recovers_under_original_deadline(self):
        failed = {"status": {"readiness": {"state": "failed"}, "error": "Timeout"}}
        pending = {"status": {"readiness": {"state": "pending"}}}
        ready = {"status": {"readiness": {"queryable": True}}}
        result, observed, polls = self.run_sequence([failed, failed, pending, ready])
        self.assertEqual(result, ready)
        self.assertEqual(polls, [0, 1, 2, 3])
        self.assertEqual(observed["transient_failures"]["Timeout"]["observations"], 2)

    def test_interrupted_legacy_build_recovers_under_original_deadline(self):
        for reason in (
            "Canceled",
            "Cancelled",
            "TlsTransportReadFailed",
            "ConnectionResetByPeer",
            "NameServerFailure",
            "RateLimited",
            "RemoteUnavailable",
            "ConnectionRefused",
            "Closed",
            "NotConnected",
            "EndOfStream",
            "UnexpectedEof",
            "ReadFailed",
            "WriteFailed",
            "RecvFailed",
            "SendFailed",
            "StorageReadTemporarilyUnavailable",
        ):
            failed = {"status": {"readiness": {"state": "failed"}, "error": reason}}
            pending = {"status": {"readiness": {"state": "pending"}, "error": reason}}
            ready = {"status": {"readiness": {"queryable": True}}}
            result, observed, _ = self.run_sequence([failed, pending, ready])
            self.assertEqual(result, ready)
            self.assertEqual(observed["transient_failures"][reason]["observations"], 2)
            with self.assertRaisesRegex(RuntimeError, "build deadline"):
                self.run_sequence([failed], deadline=3)

    def test_repeated_timeout_does_not_extend_deadline(self):
        failed = {
            "status": {"readiness": {"state": "failed"}, "error": "DeadlineExceeded"}
        }
        with self.assertRaisesRegex(RuntimeError, "build deadline"):
            self.run_sequence([failed], deadline=3)

    def test_late_readiness_does_not_extend_deadline(self):
        clock = [0]

        def read():
            clock[0] = 4
            return {"status": {"readiness": {"queryable": True}}}

        with self.assertRaisesRegex(RuntimeError, "build deadline"):
            poc.wait_for_index(
                read, 3, lambda: None, {}, now=lambda: clock[0], sleep=lambda _: None
            )

    def test_permanent_admission_fails_immediately(self):
        for reason in (
            "SqlProgramLimitExceeded",
            "ArtifactIntegrityMismatch",
            "TlsBadRecordMac",
            "TlsAlert",
            "AccessDenied",
            "Unauthorized",
            "PreconditionFailed",
        ):
            failed = {
                "status": {
                    "readiness": {"state": "failed"},
                    "error": reason,
                }
            }
            with self.assertRaisesRegex(RuntimeError, "Index construction failed"):
                self.run_sequence([failed])


if __name__ == "__main__":
    unittest.main()
