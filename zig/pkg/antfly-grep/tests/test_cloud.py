#!/usr/bin/env python3
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

"""Cloud login over ordinary catalog/table APIs, with current-policy checks."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BINARY = str(Path(sys.argv.pop(1)).resolve())


class CloudTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.catalog_requests = []
        self.content_requests = []
        self.context_calls = self.directory / "context-calls"
        self.mode = "complete"
        self.aliases = ["manuals", "slides"]
        self.long_catalog = False
        self.instance_root = "/cloud/v1/instance"
        self.subject = "user:alice"
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def reply(self, status, value=None, **headers):
                raw = json.dumps(value or {}).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                for key, value in headers.items():
                    self.send_header(key.replace("_", "-"), value)
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                self.reply(404)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                is_catalog = self.path.endswith("/tables/antfly_sources/query")
                expected_path = (
                    test.instance_root
                    + "/db/v1/tables/"
                    + ("antfly_sources" if is_catalog else "documents")
                    + "/query"
                )
                if self.path != expected_path:
                    self.reply(404)
                    return
                token = self.headers.get("Authorization")
                catalog_call = len(test.catalog_requests)
                content_call = len(test.content_requests)
                if is_catalog:
                    test.catalog_requests.append(body)
                else:
                    test.content_requests.append(
                        (
                            self.headers.get("X-Antfly-Source-ID"),
                            self.headers.get("X-Antfly-Source-Access"),
                            body,
                        )
                    )
                if test.mode == "revoked":
                    self.reply(403)
                    return
                refresh_catalog = test.mode == "refresh-catalog" and is_catalog
                refresh_content = (
                    test.mode in ("refresh", "identity-changed", "instance-changed")
                    and not is_catalog
                )
                if (
                    refresh_catalog
                    or refresh_content
                    or (
                        test.mode == "refresh-continuation"
                        and not is_catalog
                        and body.get("search_after")
                    )
                ) and token == "Bearer token-1":
                    self.reply(401)
                    return
                subject = (
                    "user:bob"
                    if test.mode == "identity-changed" and token == "Bearer token-2"
                    else test.subject
                )
                if is_catalog:
                    if (
                        body.get("remote_snapshot")
                        and test.mode == "catalog-identity-changed"
                    ):
                        subject = "user:bob"
                    if (
                        body.get("remote_snapshot")
                        and test.mode == "catalog-policy-changed"
                    ):
                        self.reply(
                            409,
                            X_Antfly_Catalog_Access="catalog-2",
                            X_Antfly_Subject=subject,
                        )
                        return
                    if body.get("remote_snapshot") and test.mode == "catalog-failed":
                        self.reply(503)
                        return
                    count = 70 if test.long_catalog else 2
                    records = []
                    for i in range(count):
                        identity = f"source-{i:03}"
                        records.append(
                            {
                                "_id": identity,
                                "_source": {
                                    "version": 2 if test.mode == "bad-version" else 1,
                                    "doc_type": "source_catalog",
                                    "document_version": 1,
                                    "published_revision": "inventory-1",
                                    "source": {
                                        "id": identity,
                                        "alias": test.aliases[i]
                                        if i < 2
                                        else f"other-{i}",
                                        "table": "documents",
                                        "filter": {
                                            "equals": identity,
                                            "field": "source_id",
                                        },
                                        "document_field": "grep",
                                        "status": "stale"
                                        if test.mode == "stale"
                                        else "searchable",
                                        "valid_until": 1
                                        if test.mode == "expired"
                                        else int(time.time()) + 900,
                                    },
                                },
                            }
                        )
                    after = body.get("search_after", [""])[0]
                    hits = [row for row in records if row["_id"] > after][
                        : body["limit"]
                    ]
                    proof = (
                        "catalog-2"
                        if body.get("remote_snapshot")
                        and test.mode == "catalog-header-changed"
                        else "catalog-1"
                    )
                    headers = (
                        {}
                        if test.mode == "missing-policy"
                        else {"X_Antfly_Catalog_Access": proof}
                    )
                    self.reply(
                        200,
                        {
                            "responses": [
                                {
                                    "status": 200,
                                    "remote_snapshot": "catalog-snapshot",
                                    "hits": {"hits": hits},
                                }
                            ]
                        },
                        X_Antfly_Subject=subject,
                        **headers,
                    )
                    return
                identity = self.headers.get("X-Antfly-Source-ID")
                proof = "policy-" + str(identity)
                if body.get("search_after") and test.mode == "source-policy-changed":
                    self.reply(
                        409, X_Antfly_Source_Access="changed", X_Antfly_Subject=subject
                    )
                    return
                if self.headers.get("X-Antfly-Source-Access") not in (None, proof):
                    self.reply(
                        409, X_Antfly_Source_Access="changed", X_Antfly_Subject=subject
                    )
                    return
                hits = []
                if not body.get("search_after"):
                    # A full page exercises proof reuse on snapshot continuation.
                    count = body["limit"]
                    if test.mode not in (
                        "source-policy-changed",
                        "source-header-changed",
                        "refresh-continuation",
                    ):
                        count = 1
                    if test.mode == "refresh-continuation":
                        count = body["limit"]
                    for i in range(count):
                        content = (
                            "Doc retry budget"
                            if identity == "source-000"
                            or test.mode in ("overlap", "separate-connections")
                            else "Slide retry budget"
                        ) + "\n"
                        canonical = (
                            "shared"
                            if test.mode in ("overlap", "separate-connections")
                            else identity
                        )
                        hits.append(
                            {
                                "_id": f"{identity}/{i:04}",
                                "_sort": [f"{identity}/{i:04}"],
                                "_source": {
                                    "grep": {
                                        "version": 1,
                                        "uri": f"https://example.com/{canonical}/{i}",
                                        "path": f"{canonical}/{i}.txt",
                                        "revision": "revision-1",
                                        "indexed_at": 1,
                                        "content": content,
                                        # Current Cloud gateway authorizes private rows. No client reader union is supplied.
                                        "permissions": {
                                            "public": False,
                                            "readers": ["group:current-engineering"],
                                        },
                                        "metadata": {
                                            "connection_id": identity
                                            if test.mode == "separate-connections"
                                            else "shared-connection"
                                        },
                                    }
                                },
                            }
                        )
                headers = {}
                if test.mode != "source-missing-subject":
                    headers["X_Antfly_Subject"] = subject
                if test.mode != "source-missing-proof":
                    headers["X_Antfly_Source_Access"] = (
                        "changed-policy"
                        if body.get("search_after")
                        and test.mode == "source-header-changed"
                        else proof
                    )
                self.reply(
                    200,
                    {
                        "responses": [
                            {
                                "status": 200,
                                "remote_snapshot": "snapshot-" + str(identity),
                                "hits": {"hits": hits},
                            }
                        ]
                    },
                    **headers,
                )

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.root = f"http://127.0.0.1:{self.server.server_port}"
        self.cli = self.directory / "cloud-cli"
        self.write_cli()
        self.env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("ANTFLY_GREP_") and key != "RIPGREP_CONFIG_PATH"
        }
        self.env.update(ANTFLY_GREP_CONFIG_PATH="", RIPGREP_CONFIG_PATH="")

    def write_cli(self):
        self.cli.write_text(
            "#!"
            + sys.executable
            + "\nimport json,pathlib\np=pathlib.Path("
            + repr(str(self.context_calls))
            + ")\nn=int(p.read_text())+1 if p.exists() else 1\np.write_text(str(n))\nprint(json.dumps({'url':"
            + repr(self.root + self.instance_root)
            + ("+('/other' if n>1 else '')" if self.mode == "instance-changed" else "")
            + ",'token':'token-'+str(n)}))\n"
        )
        self.cli.chmod(0o700)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def run_cli(self, *args):
        return subprocess.run(
            [BINARY, "--cloud-cli", str(self.cli), *args],
            cwd=self.directory,
            env=self.env,
            capture_output=True,
            timeout=20,
            input=b"",
        )

    def test_private_mixed_sources_need_no_source_api_or_reader_list(self):
        result = self.run_cli(
            "--source", "manuals", "--source", "slides", "retry budget"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b"Doc retry budget", result.stdout)
        self.assertIn(b"Slide retry budget", result.stdout)
        self.assertEqual(len(self.catalog_requests), 1)
        self.assertEqual(
            {source for source, _, _ in self.content_requests},
            {"source-000", "source-001"},
        )

    def test_overlapping_authorized_sources_deduplicate_only_within_connection(self):
        for mode, count in (("overlap", 1), ("separate-connections", 2)):
            with self.subTest(mode=mode):
                self.mode = mode
                result = self.run_cli(
                    "--source", "manuals", "--source", "slides", "retry budget"
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.count(b"Doc retry budget"), count)

    def test_catalog_snapshot_pagination_uses_current_policy_proof(self):
        self.long_catalog = True
        result = self.run_cli("--source", "manuals", "retry budget")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.catalog_requests), 2)
        self.assertEqual(
            self.catalog_requests[1]["remote_snapshot"], "catalog-snapshot"
        )

    def test_token_refresh_on_catalog_and_content_queries(self):
        for mode in ("refresh", "refresh-catalog", "refresh-continuation"):
            with self.subTest(mode=mode):
                self.mode = mode
                self.context_calls.unlink(missing_ok=True)
                result = self.run_cli("--source", "manuals", "retry budget")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.context_calls.read_text(), "2")

    def test_identity_and_instance_changes_are_explicit(self):
        for mode, error in (
            ("identity-changed", b"CloudIdentityChanged"),
            ("instance-changed", b"CloudContextChanged"),
        ):
            with self.subTest(mode=mode):
                self.mode = mode
                self.context_calls.unlink(missing_ok=True)
                self.write_cli()
                result = self.run_cli("--source", "manuals", "retry budget")
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(error, result.stderr)

    def test_partial_or_changed_catalog_never_starts_search(self):
        self.long_catalog = True
        for mode in (
            "catalog-policy-changed",
            "catalog-header-changed",
            "catalog-failed",
            "missing-policy",
            "bad-version",
            "revoked",
            "catalog-identity-changed",
        ):
            with self.subTest(mode=mode):
                self.mode = mode
                result = self.run_cli("--source", "manuals", "retry budget")
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse(self.content_requests)

    def test_stale_and_expired_sources_never_start_content_queries(self):
        for mode in ("stale", "expired"):
            with self.subTest(mode=mode):
                self.mode = mode
                result = self.run_cli("--source", "manuals", "retry budget")
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(b"SourceNotSearchable", result.stderr)
                self.assertFalse(self.content_requests)

    def test_policy_changes_on_snapshot_continuation_preserve_partial_results(self):
        for mode in ("source-policy-changed", "source-header-changed"):
            with self.subTest(mode=mode):
                self.mode = mode
                result = self.run_cli("--source", "manuals", "retry budget")
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(b"CloudSourceAccessChanged", result.stderr)
                self.assertIn(b"Doc retry budget", result.stdout)
                self.assertTrue(self.content_requests[-1][1])

    def test_private_rows_require_authenticated_response_context(self):
        for mode in ("source-missing-proof", "source-missing-subject"):
            with self.subTest(mode=mode):
                self.mode = mode
                result = self.run_cli("--source", "manuals", "retry budget")
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse(result.stdout)

    def test_ids_deduplicate_and_ambiguous_aliases_fail(self):
        result = self.run_cli(
            "--source", "manuals", "--source", "source-000", "retry budget"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.content_requests), 1)
        self.content_requests.clear()
        self.aliases = ["duplicate", "duplicate"]
        result = self.run_cli("--source", "duplicate", "retry budget")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn(b"AmbiguousSource", result.stderr)
        self.assertFalse(self.content_requests)


if __name__ == "__main__":
    unittest.main()
