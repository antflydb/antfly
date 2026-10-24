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

"""Executable Cloud discovery/session contracts; fixtures never contact providers."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from urllib.parse import urlsplit, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BINARY = str(Path(sys.argv.pop(1)).resolve())


class CloudTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.requests = []
        self.revoked = False
        self.refresh = False
        self.changed_context = False
        self.overlapping = False
        self.different_connections = False
        self.identity_changed = False
        self.discovery_requests = []
        self.source_aliases = ["engineering", "work-drive"]
        self.selected_ids = []
        self.paginate = False
        self.discovery_failure = None
        self.discovery_refresh = False
        self.legacy_discovery = False
        self.too_many_sources = False
        self.query_scope_changed = False
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def reply(self, status, body=None, headers=None):
                encoded = json.dumps(body).encode() if body is not None else b""
                self.send_response(status)
                self.send_header("Content-Length", str(len(encoded)))
                self.send_header("Content-Type", "application/json")
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(encoded)

            def do_GET(self):
                test.discovery_requests.append(self.path)
                parsed = urlsplit(self.path)
                if parsed.path != "/cloud/v1/instance/sources":
                    self.reply(404)
                    return
                cursor = parse_qs(parsed.query).get("cursor", [None])[0]
                if (
                    cursor
                    and test.discovery_refresh
                    and self.headers.get("Authorization") == "Bearer old-session"
                ):
                    self.reply(401)
                    return
                if cursor and test.discovery_failure == "revoked":
                    self.reply(403)
                    return
                subject = (
                    "user:bob"
                    if test.identity_changed
                    and self.headers.get("Authorization") == "Bearer new-session"
                    else "user:alice"
                )
                if cursor and test.discovery_failure == "identity":
                    subject = "user:bob"
                tables = [
                    {
                        "id": "repo-one",
                        "table": "shared-sources",
                        "filter": {"equals": "repo-one", "field": "source_id"},
                        "alias": test.source_aliases[0],
                        "document_field": "grep",
                        "status": "searchable",
                        "access_id": "repo-access",
                    },
                    {
                        "id": "drive-two",
                        "table": "shared-sources",
                        "filter": {"equals": "drive-two", "field": "source_id"},
                        "alias": test.source_aliases[1],
                        "document_field": "grep",
                        "status": "searchable",
                        "access_id": "drive-access",
                    },
                ]
                generation = "generation-one"
                if cursor and test.discovery_failure == "changed":
                    generation = "generation-two"
                response = {
                    "version": 1 if test.legacy_discovery else 2,
                    "subject": subject,
                    "page_size": 1,
                    "principals": ["user:alice"],
                    "sources": tables,
                    "endpoint": "db/v1",
                    "discovery_id": generation,
                }
                if cursor and test.discovery_failure == "unsafe-endpoint":
                    response["endpoint"] = "https://other-host.example/db/v1"
                if test.too_many_sources:
                    response["sources"] = [
                        dict(tables[0], id=f"source-{n}") for n in range(1001)
                    ]
                if test.paginate:
                    response["sources"] = [tables[1] if cursor else tables[0]]
                    if cursor:
                        response["principals"].append("group:drive-reader")
                    if not cursor or test.discovery_failure == "loop":
                        response["next_cursor"] = "next-page"
                    if not cursor and test.discovery_failure == "unsafe-cursor":
                        response["next_cursor"] = "https://other-host.example/steal"
                    if cursor and test.discovery_failure == "duplicate":
                        response["sources"] = [tables[0]]
                if cursor and test.discovery_failure == "missing-access":
                    response["sources"][0].pop("access_id", None)
                if cursor and test.discovery_failure == "unsafe-access":
                    response["sources"][0]["access_id"] = "bad\r\nheader"
                if test.legacy_discovery:
                    for table in response["sources"]:
                        table.pop("access_id", None)
                self.reply(200, response)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                test.requests.append(
                    (self.path, self.headers.get("Authorization"), body)
                )
                source = self.headers.get("X-Antfly-Source-ID")
                test.selected_ids.append(source)
                test.assertEqual(
                    self.path, "/cloud/v1/instance/db/v1/tables/shared-sources/query"
                )
                test.assertEqual(
                    self.headers.get("X-Antfly-Source-Access"),
                    None
                    if test.legacy_discovery
                    else ("repo-access" if source == "repo-one" else "drive-access"),
                )
                if test.query_scope_changed and source == "drive-two":
                    self.reply(409, headers={"X-Antfly-Source-Access": "changed"})
                    return
                if test.revoked and source == "drive-two":
                    self.reply(403)
                    return
                if (
                    test.refresh
                    and body.get("search_after")
                    and self.headers.get("Authorization") == "Bearer old-session"
                ):
                    self.reply(401)
                    return
                hits = []
                if not body.get("search_after"):
                    hits = [
                        {
                            "_id": source + ":item",
                            "_sort": [source + ":item"],
                            "_score": 0,
                            "_source": {
                                "grep": {
                                    "version": 1,
                                    "path": source + "/item.txt",
                                    "uri": "https://example.com/"
                                    + ("shared-item" if test.overlapping else source),
                                    "revision": "rev-1",
                                    "indexed_at": 123,
                                    "content": "a shared retry budget\n",
                                    "permissions": {
                                        "public": False,
                                        "readers": ["group:drive-reader"]
                                        if test.paginate and source == "drive-two"
                                        else ["user:alice"],
                                    },
                                    "metadata": {
                                        "connection_id": source
                                        if test.different_connections
                                        else "shared-connection"
                                    },
                                }
                            },
                        }
                    ]
                # Match the real database projection contract. Never return
                # fields the client omitted, even if the fixture contains them.
                for hit in hits:
                    document = hit["_source"]["grep"]
                    projected = {}
                    for field in body.get("fields", []):
                        if not field.startswith("grep."):
                            continue
                        parts = field.split(".")[1:]
                        value = document
                        for part in parts:
                            if not isinstance(value, dict) or part not in value:
                                break
                            value = value[part]
                        else:
                            target = projected
                            for part in parts[:-1]:
                                target = target.setdefault(part, {})
                            target[parts[-1]] = value
                    hit["_source"]["grep"] = projected
                self.reply(
                    200,
                    {
                        "responses": [
                            {
                                "status": 200,
                                "remote_snapshot": "snapshot:" + source,
                                "hits": {"hits": hits},
                            }
                        ]
                    },
                )

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.context_url = (
            f"http://127.0.0.1:{self.server.server_port}/cloud/v1/instance"
        )
        self.cli = self.directory / "cloud-cli"
        counter = self.directory / "context-calls"
        self.cli.write_text(
            "#!" + sys.executable + "\n"
            "import json, pathlib, sys\n"
            "assert sys.argv[1:] == ['context', '--json']\n"
            f"p = pathlib.Path({str(counter)!r})\n"
            "n = int(p.read_text()) if p.exists() else 0\n"
            "p.write_text(str(n + 1))\n"
            f"url = {self.context_url!r}\n"
            "if pathlib.Path(__file__).with_name('trailing-slash').exists(): url += '/'\n"
            "if n and pathlib.Path(__file__).with_name('changed-context').exists(): url += '/other'\n"
            "print(json.dumps({'url':url,'token':'new-session' if n else 'old-session','org':'test','instance':'test'}))\n"
        )
        self.cli.chmod(0o700)
        self.env = {
            k: v for k, v in os.environ.items() if not k.startswith("ANTFLY_GREP_")
        }
        self.env.update(
            HOME=str(self.directory),
            XDG_CONFIG_HOME=str(self.directory),
            XDG_CACHE_HOME=str(self.directory),
        )

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def run_cli(self, *args):
        return subprocess.run(
            [BINARY, "--cloud-cli", str(self.cli), *args],
            cwd=self.directory,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
        )

    def test_mixed_sources_require_no_table_json_and_do_not_walk_local_files(self):
        (self.directory / "secret.txt").write_text("retry local-only secret")
        result = self.run_cli("retry")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b"repo-one/item.txt", result.stdout)
        self.assertIn(b"drive-two/item.txt", result.stdout)
        self.assertNotIn(b"secret.txt", result.stdout)
        self.assertTrue(all("/db/v1/tables/" in path for path, _, _ in self.requests))

    def test_instance_root_with_trailing_slash_and_refresh_uses_same_route(self):
        (self.directory / "trailing-slash").touch()
        self.refresh = True
        result = self.run_cli("--source", "engineering", "retry")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreaterEqual(len(self.discovery_requests), 2)
        self.assertTrue(
            all(
                path == "/cloud/v1/instance/sources" for path in self.discovery_requests
            )
        )

    def test_overlapping_logical_sources_do_not_duplicate_canonical_items(self):
        self.overlapping = True
        for flags in ((), ("--json",), ("--stats",)):
            with self.subTest(flags=flags):
                self.requests.clear()
                result = self.run_cli(*flags, "retry")
                self.assertEqual(result.returncode, 0, result.stderr)
                if "--json" in flags:
                    records = [json.loads(line) for line in result.stdout.splitlines()]
                    self.assertEqual(
                        sum(record["type"] == "match" for record in records), 1
                    )
                else:
                    self.assertEqual(result.stdout.count(b"a shared retry budget"), 1)
                for _, _, body in self.requests:
                    self.assertTrue(
                        "grep.metadata.connection_id" in body["fields"]
                        or "grep.metadata" in body["fields"]
                    )

    def test_same_uri_in_distinct_connection_namespaces_is_not_collapsed(self):
        self.overlapping = True
        self.different_connections = True
        result = self.run_cli("retry")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count(b"a shared retry budget"), 2)

    def test_repeated_selectors_are_deduplicated_and_ids_work(self):
        result = self.run_cli(
            "--source", "engineering", "--source", "repo-one", "retry"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count(b"a shared retry budget"), 1)
        self.assertTrue(all(source == "repo-one" for source in self.selected_ids))

    def test_source_id_takes_precedence_over_another_sources_alias(self):
        self.source_aliases[0] = "drive-two"
        result = self.run_cli("--source", "drive-two", "retry")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.requests)
        self.assertTrue(all(source == "drive-two" for source in self.selected_ids))
        self.assertNotIn(b"repo-one/item.txt", result.stdout)

    def test_ambiguous_alias_fails_before_any_query_but_ids_still_work(self):
        self.source_aliases = ["shared", "shared"]
        result = self.run_cli("--source", "shared", "retry")
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"AmbiguousSource", result.stderr)
        self.assertEqual(self.requests, [])
        result = self.run_cli("--source", "repo-one", "retry")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.requests)
        self.assertTrue(all(source == "repo-one" for source in self.selected_ids))

    def test_refresh_keeps_snapshot_and_cursor(self):
        self.refresh = True
        result = self.run_cli("--source", "engineering", "retry")
        self.assertEqual(result.returncode, 0, result.stderr)
        rejected = next(
            r
            for r in self.requests
            if r[1] == "Bearer old-session" and r[2].get("search_after")
        )
        retried = next(r for r in self.requests if r[1] == "Bearer new-session")
        self.assertEqual(rejected[2], retried[2])
        self.assertEqual(retried[2]["remote_snapshot"], "snapshot:repo-one")
        self.assertEqual(result.stdout.count(b"a shared retry budget"), 1)

    def test_refresh_rejects_authenticated_identity_change(self):
        self.refresh = True
        self.identity_changed = True
        result = self.run_cli("--source", "engineering", "retry")
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"CloudIdentityChanged", result.stderr)
        self.assertFalse(
            any(auth == "Bearer new-session" for _, auth, _ in self.requests)
        )

    def test_refresh_rejects_active_instance_change(self):
        self.refresh = True
        (self.directory / "changed-context").touch()
        result = self.run_cli("--source", "engineering", "retry")
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"CloudContextChanged", result.stderr)
        self.assertFalse(
            any(auth == "Bearer new-session" for _, auth, _ in self.requests)
        )

    def test_source_revocation_preserves_prior_results_and_is_incomplete(self):
        self.revoked = True
        result = self.run_cli("retry")
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"repo-one/item.txt", result.stdout)
        self.assertIn(b"IndexAuthorizationFailed", result.stderr)

    def test_paginated_discovery_collects_all_sources_and_reader_identities(self):
        self.paginate = True
        self.discovery_refresh = True
        result = self.run_cli("retry")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b"repo-one/item.txt", result.stdout)
        self.assertIn(b"drive-two/item.txt", result.stdout)
        self.assertTrue(
            any("cursor=next-page" in path for path in self.discovery_requests)
        )
        self.assertTrue(
            all(auth == "Bearer new-session" for _, auth, _ in self.requests)
        )

    def test_partial_discovery_is_never_used_to_start_searching(self):
        self.paginate = True
        for failure, error in (
            ("changed", b"CloudDiscoveryChanged"),
            ("identity", b"CloudIdentityChanged"),
            ("duplicate", b"DuplicateSourceID"),
            ("loop", b"InvalidCloudSourceDiscovery"),
            ("revoked", b"CloudSourceAccessDenied"),
            ("unsafe-cursor", b"InvalidCloudSourceDiscovery"),
            ("unsafe-endpoint", b"CloudContextChanged"),
            ("missing-access", b"InvalidCloudSourceDiscovery"),
            ("unsafe-access", b"InvalidCloudSourceDiscovery"),
        ):
            with self.subTest(failure=failure):
                self.discovery_failure = failure
                self.requests.clear()
                result = self.run_cli("retry")
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(error, result.stderr)
                self.assertEqual(self.requests, [])

    def test_discovery_has_an_aggregate_source_budget(self):
        self.too_many_sources = True
        result = self.run_cli("retry")
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"CloudDiscoveryBudgetExceeded", result.stderr)
        self.assertEqual(self.requests, [])

    def test_changed_reader_view_during_query_is_explicitly_incomplete(self):
        self.query_scope_changed = True
        result = self.run_cli("retry")
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"CloudSourceAccessChanged", result.stderr)
        self.assertIn(b"repo-one/item.txt", result.stdout)
        self.assertNotIn(b"drive-two/item.txt", result.stdout)

    def test_unsupported_catalog_versions_fail_before_querying(self):
        self.legacy_discovery = True
        result = self.run_cli("retry")
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"UnsupportedCloudSourceVersion", result.stderr)
        self.assertEqual(self.requests, [])

    def test_unknown_source_and_conflicting_local_scope_fail_explicitly(self):
        result = self.run_cli("--source", "missing", "retry")
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"UnknownTable", result.stderr)
        result = self.run_cli("retry", ".")
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"ConflictingCloudSourceOptions", result.stderr)


if __name__ == "__main__":
    unittest.main()
