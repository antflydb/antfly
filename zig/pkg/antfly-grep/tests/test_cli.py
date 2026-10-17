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

"""Black-box CLI contracts, including an actual HTTP decision round trip."""

import base64
import hashlib
import json
import os
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BINARY = str(Path(sys.argv.pop(1)).resolve())
NATIVE_LIBRARY = str(Path(sys.argv.pop(1)).resolve())


class DecisionHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def handle(self):
        try:
            super().handle()
        except ConnectionResetError:
            pass

    def log_message(self, *_args):
        pass

    def do_POST(self):
        with self.server.lock:
            self.server.active += 1
            self.server.peak = max(self.server.peak, self.server.active)
        try:
            if self.server.barrier:
                try:
                    self.server.barrier.wait(timeout=3)
                except threading.BrokenBarrierError:
                    pass
            if self.server.delay:
                time.sleep(self.server.delay)
            self.decision()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with self.server.lock:
                self.server.active -= 1

    def index_query(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.index_requests.append(
            (self.path, self.headers.get("Authorization"), body)
        )
        if body.get("order_by") != [{"field": "_id", "desc": False}]:
            self.send_response(400)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        mode = self.server.index_mode
        if body.get("search_after"):
            self.server.index_next.set()
            if mode == "blocked-prefetch":
                self.server.index_release.wait(5)
        if mode == "retry" and len(self.server.index_requests) == 1:
            self.send_response(429)
            self.send_header("Retry-After", "Sun, 06 Nov 1994 08:49:37 GMT")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if mode.startswith("embedded-retry-") and len(self.server.index_requests) == 2:
            encoded = json.dumps(
                {
                    "responses": [
                        {"status": int(mode.rsplit("-", 1)[1]), "error": "transient"}
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
            return
        if mode == "oversized" and body["limit"] > 1:
            self.send_response(413)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if mode == "expired" and body.get("search_after"):
            self.send_response(409)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        after = (body.get("search_after") or [""])[0]
        docs = [doc for doc in self.server.index_documents if doc["_id"] > after]

        def candidate_matches(query, doc):
            if not query:
                return True
            if "disjuncts" in query:
                return any(candidate_matches(q, doc) for q in query["disjuncts"])
            if "must_not" in query:
                return not candidate_matches(query["must_not"], doc)
            if query.get("field") == "grep_trigrams_version":
                return doc["_source"].get("grep_trigrams_version") == query["term"]
            if "conjuncts" in query:
                return all(candidate_matches(q, doc) for q in query["conjuncts"])
            if query.get("field") == "grep_trigrams":
                return query["term"] in doc["_source"]["grep_trigrams"]
            return True

        docs = [doc for doc in docs if candidate_matches(body.get("filter_query"), doc)]
        hits = docs[: body["limit"]]
        if mode == "cursor" and after:
            hits = self.server.index_documents[:1]
        snapshot = "changed" if mode == "changed" and after else "fixture-snapshot-v1"
        encoded = json.dumps(
            {
                "responses": [
                    {
                        "status": (
                            409
                            if mode == "embedded-expired"
                            else 403
                            if mode == "embedded-denied"
                            else 200
                        )
                        if after
                        else 200,
                        "took": 1,
                        "remote_snapshot": snapshot,
                        "hits": {"hits": hits},
                    }
                ]
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def decision(self):
        if "/tables/" in self.path:
            return self.index_query()
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append(
            (self.path, self.headers.get("Authorization"), body)
        )
        self.server.client_ports.append(self.client_address[1])
        if body.get("context_version") == 1:
            body = json.loads(json.dumps(body))
            for item in body["inputs"]:
                if "context_index" in item:
                    item["input"] = (
                        body["contexts"][item["context_index"]] + "\n" + item["input"]
                    )
        if self.server.mode == "prefetch" and len(self.server.requests) == 1:
            self.server.prefetch_seen = self.server.index_next.wait(2)
        if "state" in body:
            encoded = json.dumps(
                {
                    "answers": {
                        key: {
                            "type": "noul",
                            "noul": 0.9 if "STOP" in body["state"] else 0.1,
                        }
                        for key in body["questions"]
                    }
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
            return
        if self.server.mode == "http_error" or (
            self.server.mode == "fail_second" and len(self.server.requests) == 2
        ):
            self.send_response(429)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if (
            self.server.mode in ("transient", "retry_date")
            and len(self.server.requests) == 1
        ) or (self.server.mode == "split" and len(body["inputs"]) > 1):
            self.send_response(429 if self.server.mode != "split" else 413)
            if self.server.mode == "retry_date":
                self.send_header("Retry-After", "Sun, 06 Nov 1994 08:49:37 GMT")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        data = []
        for i, item in enumerate(body["inputs"]):
            # A deterministic protocol fixture; this is not a model quality test.
            target = "\n".join(
                line
                for line in item["input"].splitlines()
                if line.startswith("TARGET ") and ":" in line
            )
            data.append(
                {
                    "input_index": i,
                    "id": item["id"],
                    "answers": [
                        {
                            "name": "relevant",
                            "type": "predicate",
                            "decision_method": "typed",
                            "probability": (
                                0.95
                                if "STOP HIGH" in target
                                else (
                                    0.7
                                    if "STOP LOW" in target
                                    else 0.9
                                    if "STOP" in target
                                    else 0.1
                                )
                            ),
                        }
                    ],
                }
            )
        if self.server.mode in ("reject_lines", "accept_lines", "reject_line_policy"):
            for row in data:
                refined = body["inputs"][row["input_index"]]["id"].startswith("refine-")
                row["answers"][0]["probability"] = (
                    (0.9 if refined else 0.1)
                    if self.server.mode == "accept_lines"
                    else (0.1 if refined else 0.9)
                )
        if self.server.mode == "reject_blocks":
            for row in data:
                if body["inputs"][row["input_index"]]["id"].startswith("region-"):
                    row["answers"][0]["probability"] = 0.1
        if self.server.mode == "exploration":
            for row in data:
                if body["inputs"][row["input_index"]]["id"] in (
                    "directory",
                    "file-preview",
                ):
                    row["answers"][0]["probability"] = 0.9
        for row in data:
            prototype = row["answers"][0]
            row["answers"] = [
                {**prototype, "name": q["name"]} for q in body["questions"]
            ]
        if self.server.mode == "reject_line_policy":
            for row in data:
                for answer in row["answers"]:
                    answer["probability"] = (
                        0.1
                        if answer["name"].startswith("include_")
                        and body["inputs"][row["input_index"]]["id"].startswith(
                            "refine-"
                        )
                        else 0.9
                    )
        if self.server.mode == "duplicate":
            data[-1]["input_index"] = 0
        if self.server.mode == "similarity":
            data[0]["answers"][0]["decision_method"] = "embedding_similarity"
        if self.server.mode == "out_of_range":
            data[0]["answers"][0]["probability"] = 1.1
        encoded = json.dumps(
            {
                "data": list(reversed(data)),
                "usage": {"input_tokens": 10, "output_tokens": 2},
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class CliTests(unittest.TestCase):
    def table_config(self, documents, page_size=1, **options):
        self.server.index_documents = [
            {"_id": str(i), "_sort": [str(i)], "_score": 0, "_source": {"grep": doc}}
            for i, doc in enumerate(documents)
        ]
        for row in self.server.index_documents:
            content = row["_source"]["grep"]["content"].encode()
            row["_source"]["grep_trigrams_version"] = "raw-byte-trigrams-v1"
            row["_source"]["grep_trigrams"] = sorted(
                {content[i : i + 3].hex() for i in range(len(content) - 2)}
            )
        config = {
            "version": 1,
            "endpoint": f"http://127.0.0.1:{self.server.server_port}/api/v1",
            "page_size": page_size,
            "tables": [{"name": "source files", "alias": "sources"}],
            **options,
        }
        self.env["ANTFLY_GREP_TABLE_TOKEN"] = "independent-table-token"
        return str(self.write("tables.json", json.dumps(config)))

    def indexed_doc(self, path="retry.py", content="STOP retry enforcement\n"):
        return {
            "version": 1,
            "path": path,
            "uri": "s3://example/" + path,
            "content": content,
            "revision": "version-42",
            "indexed_at": 1700000000,
            "permissions": {"public": True, "readers": []},
            "metadata": {"provider": "s3"},
            "regions": [{"start_line": 1, "end_line": 1}],
        }

    def test_indexed_exact_scan_cursor_provenance_and_lexical_parity(self):
        documents = [
            self.indexed_doc("a.py", "before\nhit twice hit\n"),
            self.indexed_doc("b.go", "hit once\n"),
        ]
        config = self.table_config(documents)
        result = self.raw_cli(
            "--table-config", config, "--table", "sources", "--json", "hit"
        )
        events = [json.loads(row) for row in result.stdout.splitlines()]
        matches = [row for row in events if row["type"] == "match"]
        self.assertEqual(
            [(row["path"], row["line_number"]) for row in matches],
            [("a.py", 2), ("b.go", 1)],
        )
        self.assertEqual(matches[0]["source"]["revision"], "version-42")
        self.assertEqual(matches[0]["source"]["freshness"], "indexed")
        self.assertEqual(matches[0]["source"]["snapshot"], "fixture-snapshot-v1")
        self.assertEqual(events[-1]["stats"]["index_requests"], 3)
        self.assertEqual(len(self.server.index_requests), 3)
        for path, token, body in self.server.index_requests:
            self.assertEqual(path, "/api/v1/tables/source%20files/query")
            self.assertEqual(token, "Bearer independent-table-token")
            self.assertEqual(body["fields"], ["grep"])
        self.assertEqual(
            self.server.index_requests[1][2]["remote_snapshot"], "fixture-snapshot-v1"
        )
        self.assertEqual(self.server.index_requests[1][2]["search_after"], ["0"])
        self.write("a.py", documents[0]["content"])
        local = [
            json.loads(row)
            for row in self.raw_cli("--json", "hit", "a.py").stdout.splitlines()
        ]
        local_match = next(row for row in local if row["type"] == "match")
        for field in ("line_number", "byte_offset", "text", "data"):
            self.assertEqual(local_match[field], matches[0][field])
        self.assertEqual(local_match["source"]["backend"], "local")

    def test_indexed_conservative_candidates_preserve_exact_results(self):
        documents = [
            self.indexed_doc("a.py", "TARGET xyz\n"),
            self.indexed_doc("b.py", "irrelevant\n"),
            self.indexed_doc("c.py", "TAR GET xyz\n"),
        ]
        args = ["-F", "TARGET"]
        baseline = self.raw_cli(
            "--table-config", self.table_config(documents), *args
        ).stdout
        config = self.table_config(
            documents,
            tables=[
                {
                    "name": "source files",
                    "candidate_index": {"version": 1, "field": "grep_trigrams"},
                }
            ],
        )
        self.server.index_requests = []
        result = self.raw_cli("--table-config", config, *args)
        self.assertEqual(result.stdout, baseline)
        self.assertEqual(len(self.server.index_requests), 2)
        filters = self.server.index_requests[0][2]["filter_query"]["disjuncts"][0][
            "conjuncts"
        ][1]["disjuncts"][0]["conjuncts"]
        self.assertTrue(all(term["field"] == "grep_trigrams" for term in filters))
        self.server.index_documents[1]["_source"].pop("grep_trigrams_version")
        self.server.index_requests = []
        self.assertEqual(self.raw_cli("--table-config", config, *args).stdout, baseline)
        self.assertEqual(len(self.server.index_requests), 3)
        for flags in (
            ["--files"],
            ["-F", "-v", "TARGET"],
            ["-F", "--files-without-match", "TARGET"],
            ["-F", "--include-zero", "-c", "TARGET"],
            ["-Fi", "TARGET"],
            ["-F", "-e", "TARGET", "-e", "TA"],
            ["-F", "TA"],
            ["--json", "-F", "TARGET"],
            ["--stats", "-F", "TARGET"],
        ):
            self.server.index_requests = []
            self.raw_cli("--table-config", config, *flags)
            self.assertNotIn("filter_query", self.server.index_requests[0][2], flags)
        self.server.index_requests = []
        self.raw_cli(
            "--table-config",
            config,
            "--table-filter",
            '{"term":"active","field":"state"}',
            *args,
        )
        self.assertEqual(
            self.server.index_requests[0][2]["filter_query"]["conjuncts"][0],
            {"term": "active", "field": "state"},
        )

    def test_indexed_globs_types_permissions_and_server_filter(self):
        denied = self.indexed_doc("secret.py")
        denied["permissions"] = {"public": False, "readers": ["alice"]}
        config = self.table_config(
            [denied, self.indexed_doc("b.go"), self.indexed_doc("c.py")]
        )
        output = self.raw_cli(
            "--table-config",
            config,
            "--files",
            "-tpy",
            "--table-filter",
            '{"term":"active","field":"state"}',
        ).stdout
        self.assertEqual(output, b"c.py\n")
        self.assertEqual(
            self.server.index_requests[0][2]["filter_query"],
            {"term": "active", "field": "state"},
        )
        config = self.table_config([denied], principals=["alice"])
        self.assertEqual(
            self.raw_cli("--table-config", config, "--files").stdout, b"secret.py\n"
        )
        self.raw_cli("--table-config", config, "-g!*.py", "STOP", code=1)

    def test_indexed_failures_budgets_and_retry_preserve_partial_results(self):
        for mode, message in [
            ("expired", "IndexSnapshotExpired"),
            ("embedded-expired", "IndexSnapshotExpired"),
            ("embedded-denied", "IndexAuthorizationFailed"),
            ("changed", "IndexSnapshotChanged"),
            ("cursor", "InvalidIndexCursor"),
        ]:
            with self.subTest(mode=mode):
                config = self.table_config(
                    [self.indexed_doc("a.py"), self.indexed_doc("b.py")]
                )
                self.server.index_mode = mode
                self.server.index_requests = []
                result = self.raw_cli(
                    "--table-config", config, "--json", "STOP", code=2
                )
                rows = [json.loads(row) for row in result.stdout.splitlines()]
                self.assertTrue(any(row["type"] == "match" for row in rows))
                self.assertTrue(any(row.get("message") == message for row in rows))
                self.assertFalse(rows[-1]["stats"]["complete"])
        self.server.index_mode = "ok"
        config = self.table_config([self.indexed_doc("a.py"), self.indexed_doc("b.py")])
        self.raw_cli(
            "--table-config", config, "--max-index-documents", "1", "STOP", code=2
        )
        self.server.index_mode = "retry"
        self.server.index_requests = []
        self.assertEqual(
            self.raw_cli("--table-config", config, "-l", "STOP").stdout, b"a.py\nb.py\n"
        )
        self.assertEqual(len(self.server.index_requests), 4)
        self.raw_cli("--table-config", config, "--table", "unknown", "STOP", code=2)

    def test_indexed_embedded_transients_retry_same_snapshot_and_cursor(self):
        for status in (408, 429, 500, 503):
            with self.subTest(status=status):
                config = self.table_config(
                    [self.indexed_doc("a.py"), self.indexed_doc("b.py")]
                )
                self.server.index_mode = f"embedded-retry-{status}"
                self.server.index_requests = []
                result = self.raw_cli(
                    "--table-config", config, "--retries", "2", "-l", "STOP"
                )
                self.assertEqual(result.stdout, b"a.py\nb.py\n")
                self.assertEqual(len(self.server.index_requests), 4)
                self.assertEqual(
                    self.server.index_requests[1][2], self.server.index_requests[2][2]
                )
        self.server.index_mode = "embedded-retry-503"
        self.server.index_requests = []
        result = self.raw_cli(
            "--table-config", config, "--retries", "0", "-l", "STOP", code=2
        )
        self.assertEqual(result.stdout, b"a.py\n")
        self.assertEqual(len(self.server.index_requests), 2)

        config = self.table_config(
            [self.indexed_doc(f"{i}.py") for i in range(4)], page_size=2
        )
        self.server.index_mode = "embedded-retry-413"
        self.server.index_requests = []
        result = self.raw_cli("--table-config", config, "-l", "STOP")
        self.assertEqual(result.stdout, b"0.py\n1.py\n2.py\n3.py\n")
        failed, retried = [r[2] for r in self.server.index_requests[1:3]]
        self.assertEqual(failed["search_after"], retried["search_after"])
        self.assertEqual(failed["remote_snapshot"], retried["remote_snapshot"])
        self.assertEqual((failed["limit"], retried["limit"]), (2, 1))

    def test_indexed_regex_and_union_candidates_are_conservative(self):
        documents = [
            self.indexed_doc("a", "TARGET 42\n"),
            self.indexed_doc("b", "other value\n"),
            self.indexed_doc("c", "irrelevant\n"),
        ]
        config = self.table_config(
            documents,
            tables=[
                {
                    "name": "source files",
                    "candidate_index": {"version": 1, "field": "grep_trigrams"},
                }
            ],
        )
        for args in (
            [r"TARGET \d+"],
            ["-F", "-e", "TARGET", "-e", "other"],
            ["-e", r"TARGET \d+", "-e", "other.*value"],
        ):
            with self.subTest(args=args):
                self.server.index_requests = []
                optimized = self.raw_cli("--table-config", config, *args).stdout
                self.assertIn("filter_query", self.server.index_requests[0][2])
                self.assertEqual(
                    len(self.server.index_requests), 2 if len(args) == 1 else 3
                )
                for d in self.server.index_documents:
                    d["_source"].pop("grep_trigrams_version", None)
                self.server.index_requests = []
                self.assertEqual(
                    self.raw_cli("--table-config", config, *args).stdout, optimized
                )
                self.assertEqual(len(self.server.index_requests), 4)
                config = self.table_config(
                    documents,
                    tables=[
                        {
                            "name": "source files",
                            "candidate_index": {"version": 1, "field": "grep_trigrams"},
                        }
                    ],
                )

    def test_indexed_and_local_decision_results_share_ranking_and_evidence(self):
        self.server.mode = "exploration"
        config = self.table_config(
            [self.indexed_doc("indexed.py", "def retry():\n    STOP()\n")]
        )
        self.write("local.py", "def retry():\n    STOP()\n")
        result = self.raw_cli(
            "ask",
            "retry enforcement",
            "--table-config",
            config,
            "--model",
            "fixture",
            "--endpoint",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
            "--no-cache",
            "--broad",
            "--json",
            "local.py",
        )
        rows = [json.loads(row) for row in result.stdout.splitlines()]
        files = [row for row in rows if row["type"] == "file"]
        self.assertEqual({row["path"] for row in files}, {"local.py", "indexed.py"})
        self.assertEqual(
            {row["source"]["backend"] for row in files}, {"local", "antfly"}
        )
        for row in files:
            self.assertTrue(row["parse_complete"])
            self.assertEqual(row["selection"]["query"], "retry enforcement")
            self.assertIn("retry", {lead["name"] for lead in row["reading_leads"]})
        self.assertEqual(rows[-1]["stats"]["files"], 2)
        self.assertTrue(
            all(
                token != "Bearer independent-table-token"
                for _, token, _ in self.server.requests
            )
        )

    def test_indexed_json_expansion_has_a_separate_memory_budget(self):
        config = self.table_config(
            [self.indexed_doc(content="STOP " + "x" * 262144)], max_page_memory=16384
        )
        result = self.raw_cli("--table-config", config, "--json", "STOP", code=2)
        events = [json.loads(row) for row in result.stdout.splitlines()]
        self.assertTrue(
            any(row.get("message") == "IndexPageMemoryExceeded" for row in events)
        )
        self.assertFalse(events[-1]["stats"]["complete"])

    def test_indexed_multiline_source_budget_counts_each_byte_once(self):
        content = "hit\nagain\n"
        config = self.table_config([self.indexed_doc(content=content)])
        self.raw_cli(
            "--table-config",
            config,
            "-UF",
            "hit\nagain",
            "--max-source-bytes",
            str(len(content)),
        )
        self.raw_cli(
            "--table-config",
            config,
            "-UF",
            "hit\nagain",
            "--max-source-bytes",
            str(len(content) - 1),
            code=2,
        )

    def test_indexed_adaptive_pages_preserve_snapshot_and_do_not_duplicate(self):
        config = self.table_config(
            [self.indexed_doc("a.py"), self.indexed_doc("b.py")], page_size=4
        )
        self.server.index_mode = "oversized"
        output = self.raw_cli("--table-config", config, "-l", "STOP").stdout
        self.assertEqual(output, b"a.py\nb.py\n")
        self.assertEqual(
            [row[2]["limit"] for row in self.server.index_requests[:3]], [4, 2, 1]
        )
        self.assertTrue(
            all(
                row[2].get("remote_snapshot") == "fixture-snapshot-v1"
                for row in self.server.index_requests
                if row[2].get("search_after")
            )
        )

    def test_indexed_nested_contract_and_timestamp_sorting(self):
        docs = [self.indexed_doc("a.py"), self.indexed_doc("b.py")]
        docs[0]["modified_at"] = 200
        docs[1]["modified_at"] = 100
        config = self.table_config(
            docs, tables=[{"name": "nested", "document_field": "source.grep"}]
        )
        for hit, doc in zip(self.server.index_documents, docs):
            hit["_source"] = {"source": {"grep": doc}}
        output = self.raw_cli(
            "--table-config", config, "--sort", "modified", "-l", "STOP"
        ).stdout
        self.assertEqual(output, b"b.py\na.py\n")
        self.assertEqual(self.server.index_requests[0][2]["fields"], ["source.grep"])
        self.raw_cli("--table-config", config, "--sort", "created", "STOP", code=2)

    def test_indexed_decision_discovery_respects_dependency_directories(self):
        self.server.mode = "exploration"
        config = self.table_config(
            [self.indexed_doc("node_modules/a.py"), self.indexed_doc("src/b.py")]
        )
        args = [
            "ask",
            "retry enforcement",
            "--table-config",
            config,
            "--model",
            "fixture",
            "--endpoint",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
            "--json",
            "--broad",
            "--no-cache",
        ]
        rows = [json.loads(row) for row in self.raw_cli(*args).stdout.splitlines()]
        self.assertEqual(
            [row["path"] for row in rows if row["type"] == "file"], ["src/b.py"]
        )
        rows = [
            json.loads(row)
            for row in self.raw_cli(*args, "--include-dependencies").stdout.splitlines()
        ]
        self.assertEqual(
            {row["path"] for row in rows if row["type"] == "file"},
            {"src/b.py", "node_modules/a.py"},
        )

    def test_indexed_contract_rejects_unknown_version_and_missing_permissions(self):
        for key, value in [
            ("version", 99),
            ("permissions", None),
            ("indexed_at", "yesterday"),
        ]:
            doc = self.indexed_doc()
            doc[key] = value
            config = self.table_config([doc])
            self.raw_cli("--table-config", config, "STOP", code=2)
        self.raw_cli("--table", "sources", "STOP", code=2)

    @unittest.skipIf(os.name == "nt", "POSIX cooperative signal contract")
    def test_interrupt_cancels_provider_and_emits_partial_summary(self):
        self.write("retry.py", "STOP retry\n")
        self.server.delay = 2
        process = subprocess.Popen(
            [BINARY, *self.decision_args(), "--json", "--no-cache", "retry.py"],
            cwd=self.root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 5
            while self.server.active == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertGreater(self.server.active, 0)
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 2, stderr.decode())
            rows = [json.loads(row) for row in stdout.splitlines()]
            self.assertFalse(rows[-1]["stats"]["complete"])
            self.assertTrue(any(row.get("message") == "Canceled" for row in rows))
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
            self.server.delay = 0
            deadline = time.monotonic() + 3
            while self.server.active and time.monotonic() < deadline:
                time.sleep(0.01)

    @unittest.skipIf(os.name == "nt", "POSIX stdin cancellation contract")
    def test_interrupt_does_not_wait_for_stdin_eof(self):
        process = subprocess.Popen(
            [BINARY, "--json", "hit"],
            cwd=self.root,
            env=self.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            process.stdin.write(b"hit first\n")
            process.stdin.flush()
            ready, _, _ = select.select([process.stdout], [], [], 5)
            self.assertTrue(ready)
            # Leave the pipe open and its producer idle.
            process.send_signal(signal.SIGINT)
            process.wait(timeout=3)
            stdout = process.stdout.read()
            stderr = process.stderr.read()
            self.assertEqual(process.returncode, 2, stderr.decode())
            rows = [json.loads(row) for row in stdout.splitlines()]
            self.assertFalse(rows[-1]["stats"]["complete"])
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdin.close()
            process.stdout.close()
            process.stderr.close()

    def test_indexed_sorting_and_pending_output_budget(self):
        config = self.table_config([self.indexed_doc("z.py"), self.indexed_doc("a.py")])
        self.assertEqual(
            self.raw_cli(
                "--table-config", config, "--sort", "path", "-l", "STOP"
            ).stdout,
            b"a.py\nz.py\n",
        )
        self.server.mode = "exploration"
        result = self.raw_cli(
            "ask",
            "retry enforcement",
            "--table-config",
            config,
            "--model",
            "fixture",
            "--endpoint",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
            "--json",
            "--broad",
            "--no-cache",
            "--max-pending-output",
            "64",
            code=2,
        )
        rows = [json.loads(row) for row in result.stdout.splitlines()]
        self.assertFalse(rows[-1]["stats"]["complete"])
        self.assertTrue(
            any(row.get("message") == "PendingOutputBudgetExceeded" for row in rows)
        )

    def test_decision_cache_expiry_and_explicit_bypass(self):
        self.write("a", "STOP retry\n")
        args = [*self.decision_args(), "--cache-dir", "scores", "a"]
        self.raw_cli(*args)
        requests = len(self.server.requests)
        self.raw_cli(*args)
        self.assertEqual(len(self.server.requests), requests)
        for cache in (self.root / "scores").iterdir():
            os.utime(cache, (1, 1))
        self.raw_cli(*args)
        self.assertGreater(len(self.server.requests), requests)
        requests = len(self.server.requests)
        self.raw_cli(*args, "--cache-ttl", "0")
        self.assertGreater(len(self.server.requests), requests)

    @unittest.skipIf(os.name == "nt", "POSIX preprocessor executable contract")
    def test_completed_file_output_does_not_wait_for_other_files(self):
        self.write("a.txt", "unused\n")
        self.write("b.txt", "unused\n")
        pre = self.write(
            "pre.py",
            "#!/usr/bin/env python3\nimport pathlib, sys, time\nif pathlib.Path(sys.argv[1]).name == 'a.txt':\n    print('hit')\nelse:\n    while not pathlib.Path('release').exists(): time.sleep(0.01)\n    print('miss')\n",
        )
        pre.chmod(0o755)
        process = subprocess.Popen(
            [BINARY, "--pre", str(pre), "-j2", "-l", "hit", "a.txt", "b.txt"],
            cwd=self.root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            ready, _, _ = select.select([process.stdout], [], [], 5)
            self.assertTrue(
                ready, "completed file was retained until the other preprocessor exited"
            )
            first = process.stdout.readline()
            self.assertEqual(first, b"a.txt\n")
            self.assertIsNone(process.poll())
        finally:
            self.write("release", "")
            stdout, stderr = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0, stderr.decode())
        self.assertEqual(stdout, b"")

    def test_pinned_local_differential_fixtures(self):
        fixtures = json.loads(
            Path(__file__).with_name("local_fixtures.json").read_text()
        )
        for case in fixtures["cases"]:
            with self.subTest(args=case["args"], subject=case["input"][:50]):
                actual = self.raw_cli(
                    *case["args"],
                    input=base64.b64decode(case["input"]),
                    code=case["code"],
                )
                self.assertEqual(actual.stdout, base64.b64decode(case["stdout"]))

    def test_labeled_coding_search_pipeline(self):
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).parents[1] / "bench/evaluate.py"),
                BINARY,
                "--fixture",
                "--min-recall",
                "1",
                "--min-precision",
                "1",
            ],
            capture_output=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        report = json.loads(result.stdout)
        self.assertEqual(report["evaluation"], "fixture_pipeline")
        self.assertEqual(
            len(report["questions"]),
            len(
                json.loads(Path(__file__).with_name("relevance.json").read_text())[
                    "questions"
                ]
            ),
        )

    def test_generated_differential_goldens(self):
        import differential
        import platform

        manifest = json.loads(
            Path(__file__).with_name("generated_fixtures.json").read_text()
        )
        host = platform.system().lower() + "-" + platform.machine()
        for index, case in enumerate(manifest["cases"]):
            source = base64.b64decode(case["input"])
            # Pinned Linux x86 PCRE2 JIT visits intermediate UTF bytes on empty
            # matches; native ARM references advance characters. Replay portable
            # ASCII cases everywhere, and the full corpus on its oracle platform.
            if (
                manifest.get("platform", "linux-x86_64") != host
                and not source.isascii()
            ):
                continue
            with self.subTest(index=index, args=case["args"]):
                result = self.raw_cli(*case["args"], input=source, code=case["code"])
                actual = (
                    differential.normalized_output(case["args"], result.stdout)
                    if case["code"] in (0, 1)
                    else b""
                )
                self.assertEqual(actual, base64.b64decode(case["stdout"]))

    def test_pinned_pcre2_dialect_fixtures(self):
        fixtures = json.loads(
            Path(__file__).with_name("pcre2_dialect_fixtures.json").read_text()
        )
        for case in fixtures["cases"]:
            with self.subTest(args=case["args"]):
                actual = self.raw_cli(
                    *case["args"],
                    input=base64.b64decode(case["input"]),
                    code=case["code"],
                )
                self.assertEqual(actual.stdout, base64.b64decode(case["stdout"]))

    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), DecisionHandler)
        cls.server.daemon_threads = True
        cls.server.lock = threading.Lock()
        cls.server.active = cls.server.peak = 0
        cls.server.delay = 0
        cls.server.barrier = None
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="antfly-grep-test-")
        self.root = Path(self.temp.name)
        (self.root / ".git").mkdir()
        self.env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
        }
        self.env["HOME"] = str(self.root)
        self.env["XDG_CONFIG_HOME"] = str(self.root / "config")
        self.server.requests = []
        self.server.client_ports = []
        self.server.index_requests = []
        self.server.index_next = threading.Event()
        self.server.index_release = threading.Event()
        self.server.prefetch_seen = False
        self.server.index_documents = []
        self.server.index_mode = "ok"
        self.server.mode = "ok"
        self.server.peak = 0
        self.server.delay = 0
        self.server.barrier = None

    def tearDown(self):
        self.temp.cleanup()

    def write(self, name, text):
        p = self.root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(text.encode() if isinstance(text, str) else text)
        return p

    def run_cli(self, *args, input=None, code=0):
        result = subprocess.run(
            [BINARY, "-nH", "--no-cache", "--no-refine", "--retries=0", "-j1", *args],
            check=False,
            cwd=self.root,
            env=self.env,
            input=input,
            capture_output=True,
            timeout=15,
        )
        self.assertEqual(
            result.returncode, code, result.stderr.decode(errors="replace")
        )
        self.assertNotIn(b"leaked", result.stderr)
        return result

    def ask(self, *args, code=0):
        return self.run_cli(
            "--ask",
            "enforces retry budget",
            "--model",
            "fixture",
            "--endpoint",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
            *args,
            code=code,
        )

    def test_pcre2_lookaround_backreferences_captures_and_auto_engine(self):
        self.write("pcre", "name=abc abc\nname=abc xyz\nÉÉ\n")
        self.assertEqual(
            self.raw_cli("-Po", r"(?<=name=)(\w+) \1", "pcre").stdout, b"abc abc\n"
        )
        self.assertEqual(
            self.raw_cli("--engine=auto", "-o", r"(?<=name=)\w+", "pcre").stdout,
            b"abc\nabc\n",
        )
        self.assertEqual(
            self.raw_cli(
                "-Po", "-r", "${word}", r"(?<word>\w+) (?=abc)", "pcre"
            ).stdout,
            b"abc\n",
        )
        self.raw_cli("-P", "(", "pcre", code=2)

    def test_compressed_native_formats_and_concatenated_gzip(self):
        import bz2
        import gzip
        import lzma

        data = b"first\nhit compressed\nlast\n"
        for ext, encoded in [
            ("gz", gzip.compress(data)),
            ("xz", lzma.compress(data)),
            ("bz2", bz2.compress(data)),
            ("lzma", lzma.compress(data, format=lzma.FORMAT_ALONE)),
        ]:
            self.write("a." + ext, encoded)
            self.assertEqual(
                self.raw_cli("-z", "hit", "a." + ext).stdout, b"hit compressed\n"
            )
        self.write(
            "multi.gz", gzip.compress(b"hit one\n") + gzip.compress(b"hit two\n")
        )
        self.assertEqual(
            self.raw_cli("-z", "hit", "multi.gz").stdout, b"hit one\nhit two\n"
        )
        self.write("bad.gz", b"not gzip")
        self.raw_cli("-z", "hit", "bad.gz", code=2)

    def test_multiquery_shares_source_and_request(self):
        self.write("a", "STOP\n")
        self.raw_cli(
            *self.decision_args(),
            "--query",
            "another question",
            "--unit",
            "line",
            "--no-cache",
            "a",
        )
        self.assertEqual(len(self.server.requests), 1)
        body = self.server.requests[0][2]
        self.assertEqual(len(body["questions"]), 2)
        self.assertEqual(len(body["inputs"]), 1)

    def test_retry_split_and_independent_line_discovery(self):
        self.write("a", "def retry():\n    STOP\n    noise\n")
        self.server.mode = "reject_blocks"
        result = self.raw_cli(*self.decision_args(), "--no-cache", "-n", "a")
        self.assertIn(b"2:0.900:    STOP", result.stdout)
        self.assertNotIn(b"3:0.900:", result.stdout)
        self.server.mode = "transient"
        self.server.requests.clear()
        self.raw_cli(*self.decision_args(), "--no-cache", "--unit", "line", "a")
        self.assertEqual(len(self.server.requests), 2)
        self.server.mode = "split"
        self.server.requests.clear()
        self.raw_cli(*self.decision_args(), "--no-cache", "--unit", "line", "a")
        self.assertEqual(len(self.server.requests), 5)

    def test_native_bzip2_rle_large_blocks_and_lz4(self):
        import bz2
        import random

        rng = random.Random(42)
        data = (
            b"a" * 120000
            + bytes(rng.randrange(256) for _ in range(120000))
            + b"\nhit end\n"
        )
        self.write("large.bz2", bz2.compress(data, compresslevel=1))
        self.assertEqual(
            self.raw_cli("-az", "-o", "hit end", "large.bz2").stdout, b"hit end\n"
        )
        plain = b"hit lz4\n"
        frame = (
            bytes.fromhex("04224d18604082")
            + (len(plain) | 0x80000000).to_bytes(4, "little")
            + plain
            + b"\0" * 4
        )
        self.write("a.lz4", frame)
        self.assertEqual(self.raw_cli("-z", "hit", "a.lz4").stdout, plain)
        damaged = bytearray(bz2.compress(b"hit\n"))
        damaged[-5] ^= 1
        self.write("damaged.bz2", damaged)
        self.raw_cli("-z", "hit", "damaged.bz2", code=2)

    def test_bzip2_bulk_checksum_and_huffman_roundtrips(self):
        import bz2
        import hashlib
        import random

        source = (
            bytes(range(256)) * 600
            + random.Random(131).randbytes(80000)
            + b"q" * 10000
            + b"\n"
        )
        expected = hashlib.sha256(source).digest()
        for level in [1, 5, 9]:
            with self.subTest(level=level):
                self.write("mixed.bz2", bz2.compress(source, compresslevel=level))
                output = self.raw_cli(
                    "-az", "--passthru", "absent-pattern", "mixed.bz2", code=1
                ).stdout
                self.assertEqual(hashlib.sha256(output).digest(), expected)

    def test_randomized_bzip2_and_legacy_lz4(self):
        import bz2

        plain = bytes(range(32, 127)) * 30 + b"\nhit randomized\n"
        # Construct the obsolete randomized bitstream explicitly: contemporary
        # encoders no longer produce it. Python's libbz2 is an independent oracle.
        shuffled = bytearray(plain)
        left = 0
        index = 0
        for i in range(len(shuffled)):
            if left == 0:
                left = [619, 720, 127, 481, 931, 816][index]
                index += 1
            left -= 1
            if left == 1:
                shuffled[i] ^= 1
        rotations = sorted(
            range(len(shuffled)), key=lambda i: shuffled[i:] + shuffled[:i]
        )
        origin = rotations.index(0)
        last = [shuffled[(i - 1) % len(shuffled)] for i in rotations]
        alphabet = list(range(256))
        symbols = []
        zeros = 0

        def flush():
            nonlocal zeros
            if zeros:
                n = zeros - 1
                while True:
                    symbols.append(n & 1)
                    if n < 2:
                        break
                    n = (n - 2) // 2
                zeros = 0

        for ch in last:
            at = alphabet.index(ch)
            alphabet.insert(0, alphabet.pop(at))
            if at == 0:
                zeros += 1
            else:
                flush()
                symbols.append(at + 1)
        flush()
        symbols.append(257)
        crc = 0xFFFFFFFF
        for ch in plain:
            crc ^= ch << 24
            for _ in range(8):
                crc = (
                    (crc << 1) ^ (0x04C11DB7 if crc & 0x80000000 else 0)
                ) & 0xFFFFFFFF
        crc ^= 0xFFFFFFFF
        bits = []

        def put(value, width):
            bits.append(format(value, f"0{width}b"))

        put(0x314159265359, 48)
        put(crc, 32)
        put(1, 1)
        put(origin, 24)
        put(0xFFFF, 16)
        for _ in range(16):
            put(0xFFFF, 16)
        put(2, 3)
        put((len(symbols) + 49) // 50, 15)
        for _ in range((len(symbols) + 49) // 50):
            put(0, 1)
        for _ in range(2):
            put(9, 5)
            for _ in range(258):
                put(0, 1)
        for symbol in symbols:
            put(symbol, 9)
        put(0x177245385090, 48)
        put(crc, 32)
        encoded = "".join(bits)
        encoded += "0" * ((-len(encoded)) % 8)
        member = b"BZh1" + int(encoded, 2).to_bytes(len(encoded) // 8, "big")
        self.assertEqual(bz2.decompress(member), plain)
        self.write("random.bz2", member)
        self.assertEqual(
            self.raw_cli("-z", "hit", "random.bz2").stdout, b"hit randomized\n"
        )
        plain = b"hit legacy\n"
        block = bytes([len(plain) << 4]) + plain
        self.write(
            "legacy.lz4",
            bytes.fromhex("02214c18") + len(block).to_bytes(4, "little") + block,
        )
        self.assertEqual(self.raw_cli("-z", "hit", "legacy.lz4").stdout, plain)

    def test_empty_config_environment_is_ignored(self):
        self.env["RIPGREP_CONFIG_PATH"] = ""
        self.assertEqual(self.raw_cli("-P", "-o", "-e", "a", input=b"a").stdout, b"a\n")
        self.env["ANTFLY_GREP_CONFIG_PATH"] = ""
        self.assertEqual(self.raw_cli("-o", "-e", "a", input=b"a").stdout, b"a\n")

    @unittest.skipUnless(shutil.which("rg"), "optional multiline JSON oracle")
    def test_multiline_json_counts_and_limits(self):
        self.write("a", "foo\nbar\nfoo\nbar\nx\n")
        for pattern in [r"foo\nbar", r"foo|bar", r"foo\n", "$", ""]:
            for flags in [
                ["-U", "--json"],
                ["-Uc"],
                ["-U", "--count-matches"],
                ["-Um1"],
            ]:
                with self.subTest(pattern=pattern, flags=flags):
                    expected = subprocess.run(
                        ["rg", *flags, pattern, "a"],
                        check=False,
                        cwd=self.root,
                        capture_output=True,
                        timeout=5,
                    )
                    actual = self.raw_cli(
                        *flags, pattern, "a", code=expected.returncode
                    )
                    if "--json" in flags:
                        e = [
                            x["data"]
                            for line in expected.stdout.splitlines()
                            if (x := json.loads(line))["type"] == "match"
                        ]
                        r = [
                            x["data"]
                            for line in actual.stdout.splitlines()
                            if (x := json.loads(line))["type"] == "match"
                        ]
                        self.assertEqual(r, e)
                    else:
                        self.assertEqual(actual.stdout, expected.stdout)

    @unittest.skipUnless(shutil.which("rg"), "optional extended regex oracle")
    def test_extended_regex_differential_matrix(self):
        self.write("a", "aaa ab abc aaab\nfoo bar\nÉé Été Σίσυφος Kk\n  \n\nlast")
        patterns = [
            "",
            "a*",
            "a*?",
            "a+",
            r"(?:a|ab)+",
            r"(?:ab|a)+",
            r"(?i)[^a]+",
            r"[^a]*",
            r"[a-z]{1,3}?",
            r"[a-z&&[^aeiou]]+",
            r"\b\w+\b",
            r"\B",
            r"^|$",
            r"(?m)^.*$",
            r"\p{Greek}+",
            r"(?i)é",
            r"(?i)k",
            r"[[:alpha:]]+",
            r"(?:foo|bar)",
            r"(?P<x>a)(b)?",
            r"[\s\S]+",
            r"\p{L}{2,4}",
            r"a{0,2}",
            r"\w*?",
            r"(?i)[A-Z]+",
            r"[a-z--[aeiou]]+",
            r"(?i)\p{Lu}+",
            r"(?-u)\w+",
        ]
        for pattern in patterns:
            for flags in [[], ["-o"], ["-c"], ["-nC1"], ["-o", "-r", "<$0>"]]:
                with self.subTest(pattern=pattern, flags=flags):
                    expected = subprocess.run(
                        ["rg", "--color=never", *flags, pattern, "a"],
                        check=False,
                        cwd=self.root,
                        env=self.env,
                        capture_output=True,
                        timeout=5,
                    )
                    self.assertEqual(
                        self.raw_cli(
                            *flags, pattern, "a", code=expected.returncode
                        ).stdout,
                        expected.stdout,
                    )

    def test_native_brotli_reference_fixtures_and_bounds(self):
        import base64

        data = json.loads(Path(__file__).with_name("brotli_fixtures.json").read_text())
        fixtures = data["fixtures"]
        for fixture in fixtures:
            with self.subTest(quality=fixture["quality"], corpus=fixture["corpus"]):
                compressed = base64.b64decode(fixture["compressed"])
                expected = base64.b64decode(data["sources"][fixture["corpus"]])
                self.write("a.br", compressed)
                actual = self.raw_cli("-az", "--passthru", "-F", "hit Brotli", "a.br")
                self.assertEqual(actual.stdout, expected)
        self.write(
            "a.br",
            base64.b64decode(
                next(
                    f["compressed"]
                    for f in fixtures
                    if f["corpus"] == 1 and f["quality"] == 5
                )
            ),
        )
        self.raw_cli("-az", "--max-file-size", "128", "hit", "a.br", code=2)
        self.write("a.br", base64.b64decode(fixtures[0]["compressed"])[:-1])
        self.raw_cli("-az", "hit", "a.br", code=2)

    def test_zstd_raw_and_trained_dictionary_reference_fixtures(self):
        import base64

        data = json.loads(
            (Path(__file__).parent / "zstd_dictionary_fixtures.json").read_text()
        )
        source = base64.b64decode(data["source"])
        expected = b"".join(
            line + b"\n" for line in source.splitlines() if b"hit" in line
        )
        for fixture in data["fixtures"]:
            with self.subTest(dictionary=fixture["dictionary"], level=fixture["level"]):
                self.write(
                    "dictionary",
                    base64.b64decode(data["dictionaries"][fixture["dictionary"]]),
                )
                self.write("a.zst", base64.b64decode(fixture["compressed"]))
                for flags in [[], ["-U"]]:
                    self.assertEqual(
                        self.raw_cli(
                            "-z", "--dictionary", "dictionary", *flags, "hit", "a.zst"
                        ).stdout,
                        expected,
                    )
                if fixture["dictionary"] == "trained":
                    self.raw_cli("-z", "hit", "a.zst", code=2)

    def test_lz4_dictionary_reference_fixtures(self):
        import base64

        data = json.loads(
            Path(__file__).with_name("lz4_dictionary_fixtures.json").read_text()
        )
        self.write("dictionary", base64.b64decode(data["dictionary"]))
        source = base64.b64decode(data["source"])
        expected = b"".join(
            line + b"\n" for line in source.splitlines() if b"hit" in line
        )
        for fixture in data["fixtures"]:
            with self.subTest(flags=fixture["flags"]):
                self.write("a.lz4", base64.b64decode(fixture["compressed"]))
                for flags in [[], ["-U"]]:
                    result = self.raw_cli(
                        "-z", "--dictionary", "dictionary", *flags, "hit", "a.lz4"
                    )
                    self.assertEqual(result.stdout, expected)

    def test_xz_delta_filter_chains_stream_and_check_original_bytes(self):
        import lzma

        source = b"noise across delta histories\n" * 12000 + b"hit end\n"
        for distances in [[1], [2], [256], [1, 17, 256]]:
            filters = [{"id": lzma.FILTER_DELTA, "dist": d} for d in distances]
            filters.append({"id": lzma.FILTER_LZMA2, "dict_size": 1 << 20})
            for check in [lzma.CHECK_CRC32, lzma.CHECK_CRC64, lzma.CHECK_SHA256]:
                with self.subTest(distances=distances, check=check):
                    self.write(
                        "delta.xz", lzma.compress(source, filters=filters, check=check)
                    )
                    self.assertEqual(
                        self.raw_cli("-z", "hit", "delta.xz").stdout, b"hit end\n"
                    )

    def test_xz_branch_filters_preserve_bytes_across_stream_boundaries(self):
        import hashlib
        import lzma
        import random

        rng = random.Random(87531)
        # Relative calls, long instructions, and random encodings exercise both
        # converted instructions and bytes that must pass through unchanged.
        seeds = bytes.fromhex(
            "e800000000e9ffffff00000000eb00000094000000900100004840000000f000f800"
        )
        source = rng.randbytes(70000) + seeds * 3000 + b"\nhit end\n"
        for name in [
            "X86",
            "POWERPC",
            "IA64",
            "ARM",
            "ARMTHUMB",
            "SPARC",
            "ARM64",
            "RISCV",
        ]:
            kind = getattr(lzma, "FILTER_" + name, None)
            if kind is None:
                continue
            for start in [0, 4096, 0xFFFFF000]:
                for with_delta in [False, True]:
                    with self.subTest(name=name, start=start, delta=with_delta):
                        filters = [{"id": kind, "start_offset": start}]
                        if with_delta:
                            filters.append({"id": lzma.FILTER_DELTA, "dist": 17})
                        filters.append({"id": lzma.FILTER_LZMA2, "dict_size": 1 << 20})
                        self.write(
                            "branches.xz", lzma.compress(source, filters=filters)
                        )
                        output = self.raw_cli(
                            "-az", "--passthru", "absent-pattern", "branches.xz", code=1
                        ).stdout
                        self.assertEqual(
                            hashlib.sha256(output).digest(),
                            hashlib.sha256(source).digest(),
                        )

    def test_xz_arm64_and_riscv_reference_fixtures(self):
        import base64
        import hashlib

        data = json.loads(Path(__file__).with_name("bcj_fixtures.json").read_text())
        source = (
            bytes.fromhex(data["seed"]) * data["repetitions"] + data["suffix"].encode()
        )
        for fixture in data["fixtures"]:
            with self.subTest(
                filter=fixture["filter"], start=fixture["start"], delta=fixture["delta"]
            ):
                self.write("branches.xz", base64.b64decode(fixture["compressed"]))
                output = self.raw_cli(
                    "-az", "--passthru", "absent-pattern", "branches.xz", code=1
                ).stdout
                self.assertEqual(
                    hashlib.sha256(output).digest(), hashlib.sha256(source).digest()
                )

    def test_xz_checks_indexes_and_lzma_window_limits(self):
        import lzma

        source = b"noise across decoder buffers\n" * 12000 + b"hit end\n"
        for check in [
            lzma.CHECK_NONE,
            lzma.CHECK_CRC32,
            lzma.CHECK_CRC64,
            lzma.CHECK_SHA256,
        ]:
            with self.subTest(check=check):
                encoded = lzma.compress(source, check=check)
                self.write("a.xz", encoded)
                self.assertEqual(self.raw_cli("-z", "hit", "a.xz").stdout, b"hit end\n")
                for offset in [-1, -12, -16, 8]:
                    corrupted = bytearray(encoded)
                    corrupted[offset] ^= 1
                    self.write("bad.xz", corrupted)
                    self.raw_cli("-z", "hit", "bad.xz", code=2)
                self.write("a.xz", encoded + b"\0" * 8 + encoded)
                self.assertEqual(
                    self.raw_cli("-z", "hit", "a.xz").stdout, b"hit end\nhit end\n"
                )
        for ext, format in [("xz", lzma.FORMAT_XZ), ("lzma", lzma.FORMAT_ALONE)]:
            self.write("a." + ext, lzma.compress(source, format=format))
            self.raw_cli(
                "-z", "--max-decompression-window", "1M", "hit", "a." + ext, code=2
            )

    @unittest.skipIf(os.name == "nt", "POSIX unlinked snapshot contract")
    def test_large_decoded_random_access_snapshots_and_cleanup(self):
        import bz2
        import gzip
        import lzma

        temporary = self.root / "temporary"
        temporary.mkdir()
        self.env["TMPDIR"] = str(temporary)
        source = (
            b"noise through bounded decode buffers\n" * 40000 + b"hit\nacross records\n"
        )
        for ext, encode in [
            ("gz", gzip.compress),
            ("bz2", bz2.compress),
            ("xz", lzma.compress),
        ]:
            with self.subTest(codec=ext):
                self.write("a." + ext, encode(source))
                self.assertEqual(
                    self.raw_cli("-zU", r"hit\nacross records", "a." + ext).stdout,
                    b"hit\nacross records\n",
                )
                self.assertEqual(list(temporary.iterdir()), [])
        self.write("input", source)
        if shutil.which("cat"):
            self.assertEqual(
                self.raw_cli(
                    "-U", "--pre", shutil.which("cat"), r"hit\nacross records", "input"
                ).stdout,
                b"hit\nacross records\n",
            )
            self.assertEqual(list(temporary.iterdir()), [])
        self.env["TMPDIR"] = str(temporary / "missing")
        self.raw_cli("-zU", "hit", "a.gz", code=2)
        self.write("small.gz", gzip.compress(b"hit\n"))
        self.assertEqual(self.raw_cli("-zU", "hit", "small.gz").stdout, b"hit\n")

    def test_http_date_retry_after_round_trip(self):
        self.write("a", "STOP\n")
        self.server.mode = "retry_date"
        self.raw_cli(*self.decision_args(), "--no-cache", "--unit", "line", "a")
        self.assertEqual(len(self.server.requests), 2)

    def test_brotli_large_windows_and_raw_dictionary_reference_fixtures(self):
        import base64

        data = json.loads(
            (Path(__file__).parent / "brotli_extended_fixtures.json").read_text()
        )
        for fixture in data["fixtures"]:
            with self.subTest(
                quality=fixture["quality"],
                window=fixture["window"],
                dictionary=fixture.get("dictionary"),
            ):
                source = base64.b64decode(data["sources"][fixture["source"]])
                self.write("extended.br", base64.b64decode(fixture["compressed"]))
                flags = ["--max-decompression-window", "1G"]
                if "dictionary" in fixture:
                    self.write(
                        "dictionary",
                        base64.b64decode(data["dictionaries"][fixture["dictionary"]]),
                    )
                    flags += ["--dictionary", "dictionary"]
                expected = b"".join(
                    line + b"\n" for line in source.splitlines() if b"hit" in line
                )
                self.assertEqual(
                    self.raw_cli("-z", *flags, "hit", "extended.br").stdout, expected
                )

    def test_brotli_serialized_dictionaries_stream_snapshot_and_workers(self):
        import base64

        data = json.loads(
            Path(__file__).with_name("brotli_serialized_fixtures.json").read_text()
        )
        for fixture in data["fixtures"]:
            source = base64.b64decode(fixture["plain"])
            self.write("a.br", base64.b64decode(fixture["compressed"]))
            for field, fmt in [
                ("dictionary", "auto"),
                ("dictionary", "brotli"),
                ("google_dictionary", "brotli-google"),
            ]:
                self.write("dictionary", base64.b64decode(fixture[field]))
                for flags in [[], ["-U"]]:
                    with self.subTest(name=fixture["name"], format=fmt, flags=flags):
                        result = self.raw_cli(
                            "-z",
                            "-a",
                            "--dictionary",
                            "dictionary",
                            "--dictionary-format",
                            fmt,
                            *flags,
                            "--passthru",
                            "antfly",
                            "a.br",
                            code=0,
                        )
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertEqual(result.stdout, source)
            self.write("dictionary", base64.b64decode(fixture["dictionary"]))
            self.write("copies/a.br", base64.b64decode(fixture["compressed"]))
            self.write("copies/b.br", base64.b64decode(fixture["compressed"]))
            result = self.raw_cli(
                "-z", "--dictionary", "dictionary", "-j4", "-c", "antfly", "copies"
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(result.stdout.splitlines()), 2)
            limited = self.raw_cli(
                "-z",
                "--dictionary",
                "dictionary",
                "--max-dictionary-size",
                "1",
                "antfly",
                "a.br",
                code=2,
            )
            self.assertEqual(limited.returncode, 2)
        self.write("dictionary", b"\x91\x00\x00\x41")
        malformed = self.raw_cli(
            "-z", "--dictionary", "dictionary", "antfly", "a.br", code=2
        )
        self.assertEqual(malformed.returncode, 2)
        self.assertIn(b"InvalidDictionary", malformed.stderr)

    def test_native_encoding_bom_legacy_and_preprocessors(self):
        for codec, label, text in [
            ("utf-16", "auto", "hit É\n"),
            ("utf-16-be", "utf-16be", "hit É\n"),
            ("cp1252", "windows-1252", "hit É\n"),
            ("shift_jis", "shift_jis", "hit 日本\n"),
            ("gb18030", "gb18030", "hit 中文😀\n"),
            ("euc_kr", "euc-kr", "hit 한국\n"),
            ("iso2022_jp", "iso-2022-jp", "hit 日本\n"),
        ]:
            with self.subTest(codec=codec):
                self.write("encoded", text.encode(codec))
                self.assertEqual(
                    self.raw_cli("--encoding", label, "hit", "encoded").stdout,
                    text.encode(),
                )
        self.write("a", "hit pre\n")
        self.assertEqual(self.raw_cli("--pre", "cat", "hit", "a").stdout, b"hit pre\n")
        self.raw_cli("--pre", "false", "hit", "a", code=2)
        self.assertEqual(
            self.raw_cli("--pre", "false", "--pre-glob", "*.py", "hit", "a").stdout,
            b"hit pre\n",
        )

    def test_native_mmap_smart_case_and_custom_output(self):
        self.write("a", "é É\nhit hit\n")
        self.assertEqual(
            self.raw_cli("--mmap", "-S", "É", "a").stdout, "é É\n".encode()
        )
        self.assertEqual(self.raw_cli("-So", "É", "a").stdout, "É\n".encode())
        self.assertEqual(
            self.raw_cli("-So", r"\p{L}+", "a").stdout, "é\nÉ\nhit\nhit\n".encode()
        )
        self.assertEqual(
            self.raw_cli("-Hn", "--field-match-separator", "|", "hit", "a").stdout,
            b"a|2|hit hit\n",
        )
        self.assertEqual(
            self.raw_cli("--include-zero", "-c", "absent", "a", code=1).stdout, b"0\n"
        )
        output = self.raw_cli(
            "--color=always", "--colors", "match:fg:blue", "hit", "a"
        ).stdout
        self.assertIn(b"\x1b[34m", output)
        self.assertIn(
            b"\x1b]8;;file://",
            self.raw_cli("-H", "--hyperlink-format=file", "hit", "a").stdout,
        )
        self.assertEqual(
            self.raw_cli("--files", "-tpy", "--type-clear", "py", ".", code=1).stdout,
            b"",
        )

    def test_semantic_excerpts_include_marked_context(self):
        self.server.mode = "exploration"
        self.write("a.py", "before()\nSTOP()\nafter()\n")
        args = [
            "ask",
            "stop behavior",
            "--model",
            "fixture",
            "--endpoint",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
            "--no-cache",
            "--unit",
            "line",
            "-C1",
            "--json",
            "a.py",
        ]
        events = [json.loads(line) for line in self.raw_cli(*args).stdout.splitlines()]
        file = next(event for event in events if event["type"] == "file")
        self.assertEqual(
            [row["matched"] for row in file["lines"]], [False, True, False]
        )
        self.assertEqual(
            [row["text"] for row in file["lines"]], ["before()", "STOP()", "after()"]
        )
        summary = next(event for event in events if event["type"] == "summary")
        self.assertEqual(summary["stats"]["matches"], 1)

    @unittest.skipUnless(shutil.which("rg"), "optional multi-file heading oracle")
    def test_multi_file_headings_and_mapped_binary_detection(self):
        self.write("a", "hit\n")
        self.write("b", "hit\n")
        flags = ["--heading", "--sort", "path", "-Hn", "hit", "."]
        expected = subprocess.run(
            ["rg", *flags], check=False, cwd=self.root, capture_output=True, timeout=5
        )
        self.assertEqual(self.raw_cli(*flags).stdout, expected.stdout)
        self.write("binary", b"hit\n" + b"none\n" * 5000 + b"x\0hit\n")
        for flags in [["hit", "binary"], ["--binary", "hit", "."], ["hit", "."]]:
            expected = subprocess.run(
                ["rg", "--sort", "path", *flags],
                check=False,
                cwd=self.root,
                capture_output=True,
                timeout=5,
            )
            self.assertEqual(
                self.raw_cli("--sort", "path", *flags, code=expected.returncode).stdout,
                expected.stdout,
            )

    def test_repository_exploration_reports_declarations_calls_and_omissions(self):
        self.server.mode = "exploration"
        self.write(
            "src/retry.py",
            "class Worker:\n    def retry(\n        self, count\n    ):\n        STOP(count)\n        noise()\n",
        )
        args = [
            "ask",
            "retry enforcement",
            "--model",
            "fixture",
            "--endpoint",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
            "--no-cache",
        ]
        events = [
            json.loads(row)
            for row in self.raw_cli(
                *args, "--json", "--max-source-bytes", "1", "."
            ).stdout.splitlines()
        ]
        file = next(row for row in events if row["type"] == "file")
        self.assertEqual(file["relevance"], 0.9)
        self.assertIn("retry", [decl["name"] for decl in file["reading_leads"]])
        self.assertIn("STOP", [call["name"] for call in file["calls"]])
        self.assertGreater(file["omitted"]["lines"], 0)
        ids = [
            item["id"]
            for _, _, body in self.server.requests
            for item in body.get("inputs", [])
        ]
        self.assertIn("directory", ids)
        self.assertIn("file-preview", ids)
        self.assertTrue(any(id.startswith("refine-") for id in ids))
        self.server.requests.clear()
        result = self.raw_cli(*args, "-l", "-n1", ".")
        self.assertIn(b"Reading leads", result.stdout)
        ids = [
            item["id"]
            for _, _, body in self.server.requests
            for item in body.get("inputs", [])
        ]
        self.assertFalse(any(id.startswith("region-") for id in ids))

    def test_pinned_pcre_empty_utf_platform_contract(self):
        self.write("pinned-utf.txt", "éΣ\naéa\n")
        count = b"10\n" if sys.platform.startswith("linux") else b"7\n"
        self.assertEqual(
            self.raw_cli("-P", "--count-matches", "", "pinned-utf.txt").stdout, count
        )
        self.assertEqual(
            self.raw_cli(
                "--engine=auto", "--count-matches", "(?=)", "pinned-utf.txt"
            ).stdout,
            count,
        )
        self.assertEqual(
            self.raw_cli(
                "-P", "--no-pcre2-unicode", "--count-matches", "", "pinned-utf.txt"
            ).stdout,
            b"10\n",
        )
        self.assertEqual(
            self.raw_cli("--count-matches", "", "pinned-utf.txt").stdout, b"10\n"
        )

    @unittest.skipUnless(shutil.which("rg"), "optional empty UTF match oracle")
    def test_empty_matches_respect_each_engines_utf_mode(self):
        self.write("utf", "éΣ\naéa\n")
        for flags, patterns in [
            ([], ["", "(?:)"]),
            (["--no-unicode"], ["", "(?:)"]),
            (["-P"], ["", "(?=)"]),
            (["-P", "--no-pcre2-unicode"], ["", "(?=)"]),
            (["--engine=auto"], ["(?=)"]),
        ]:
            for pattern in patterns:
                for output in [["--count-matches"], ["-r", "R"]]:
                    with self.subTest(flags=flags, pattern=pattern, output=output):
                        args = [*flags, *output, pattern, "utf"]
                        expected = subprocess.run(
                            ["rg", *args],
                            cwd=self.root,
                            env=self.env,
                            capture_output=True,
                            timeout=5,
                            check=False,
                        )
                        self.assertEqual(
                            self.raw_cli(*args, code=expected.returncode).stdout,
                            expected.stdout,
                        )

    @unittest.skipUnless(shutil.which("rg"), "optional empty-match boundary oracle")
    def test_empty_match_iteration_at_unterminated_line_boundaries(self):
        for source in ["a", "a\n"]:
            self.write("source", source)
            for pattern in ["", "$", "a|$", "a*"]:
                for backend in ["--mmap", "--no-mmap", "-U"]:
                    for output in [["--count-matches"], ["-r", "R"], ["--json"]]:
                        with self.subTest(
                            source=source,
                            pattern=pattern,
                            backend=backend,
                            output=output,
                        ):
                            args = [backend, *output, pattern, "source"]
                            ref = subprocess.run(
                                ["rg", *args],
                                cwd=self.root,
                                env=self.env,
                                capture_output=True,
                                check=False,
                                timeout=5,
                            )
                            actual = self.raw_cli(*args, code=ref.returncode).stdout
                            if output == ["--json"]:

                                def normalize(data):
                                    rows = [
                                        json.loads(row) for row in data.splitlines()
                                    ]
                                    return (
                                        [
                                            row["data"]["submatches"]
                                            for row in rows
                                            if row["type"] == "match"
                                        ],
                                        next(
                                            row["data"]["stats"]["matches"]
                                            for row in rows
                                            if row["type"] == "summary"
                                        ),
                                    )

                                self.assertEqual(
                                    normalize(actual), normalize(ref.stdout)
                                )
                            else:
                                self.assertEqual(actual, ref.stdout)

    def test_mmap_counts_suppress_duplicate_empty_matches(self):
        self.write("source", "a\n")
        for flags in [["--mmap"], ["--no-mmap"], ["-U"]]:
            self.assertEqual(
                self.raw_cli(*flags, "--count-matches", "a*", "source").stdout, b"1\n"
            )

    def test_mmap_count_limits_and_json_statistics_count_occurrences(self):
        self.write("source", "aaa\nbbb\n")
        for flags in [["--mmap"], ["--no-mmap"]]:
            self.assertEqual(
                self.raw_cli(
                    *flags, "--count-matches", "-m", "1", "a|b", "source"
                ).stdout,
                b"3\n",
            )
        rows = [
            json.loads(row)
            for row in self.raw_cli(
                "--mmap", "--json", "a", "source"
            ).stdout.splitlines()
        ]
        summary = next(row for row in rows if row["type"] == "summary")
        self.assertEqual(summary["data"]["stats"]["matches"], 3)

    def test_multiline_match_limit_propagates_as_search_error(self):
        self.write("source", "a\n")
        result = self.raw_cli(
            "-UP", "--pcre2-match-limit", "1", "(?=a)a", "source", code=2
        )
        self.assertIn(b"MatchLimitExceeded", result.stderr)

    def test_dense_counts_need_no_match_output_or_capture_storage(self):
        self.write("dense", b"k " * 131072 + "K ſ S\n".encode())
        for flags in [
            ["--count-matches"],
            ["-c", "-o"],
            ["--count-matches", "-r", "$0"],
            ["-U", "--count-matches"],
            ["--mmap", "--count-matches"],
            ["-U", "-c", "-o"],
        ]:
            self.assertEqual(
                self.raw_cli("-i", *flags, "k", "dense").stdout, b"131073\n"
            )
        self.assertEqual(
            self.raw_cli("-i", "--count-matches", "s", "dense").stdout, b"2\n"
        )
        self.assertEqual(
            self.raw_cli("--count-matches", "k+", "dense").stdout, b"131072\n"
        )

    @unittest.skipUnless(shutil.which("rg"), "optional engine selection oracle")
    def test_auto_selects_one_engine_for_the_pattern_set(self):
        self.write("utf", "éΣ\n")
        for patterns in [["", "(?=)"], ["(?=)", ""], ["(?:)", "(?=)"]]:
            args = [
                "--engine=auto",
                "--count-matches",
                "-e",
                patterns[0],
                "-e",
                patterns[1],
                "utf",
            ]
            expected = subprocess.run(
                ["rg", *args],
                cwd=self.root,
                env=self.env,
                capture_output=True,
                timeout=5,
                check=False,
            )
            self.assertEqual(
                self.raw_cli(*args, code=expected.returncode).stdout, expected.stdout
            )

    def test_typescript_grammar_reaches_decision_evidence(self):
        self.server.mode = "exploration"
        self.write(
            "worker.tsx",
            "interface Box<T> { value: T }\nclass Worker<T> {\n  retry<U extends T>(value: U): U {\n    STOP(value);\n    return value;\n  }\n}\nconst view = <T,>(value: T) => <div>{show(value)}</div>;\n",
        )
        args = [
            "ask",
            "retry enforcement",
            "--model",
            "fixture",
            "--endpoint",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
            "--no-cache",
            "--json",
            "--broad",
            "worker.tsx",
        ]
        events = [json.loads(row) for row in self.raw_cli(*args).stdout.splitlines()]
        file = next(row for row in events if row["type"] == "file")
        self.assertTrue(file["parse_complete"])
        self.assertTrue(
            {"Box", "Worker", "retry", "view"}.issubset(
                {decl["name"] for decl in file["reading_leads"]}
            )
        )
        self.assertIn("STOP", {call["name"] for call in file["calls"]})

    def test_python_go_rust_grammars_reach_decision_evidence(self):
        self.server.mode = "exploration"
        sources = {
            "worker.py": "class Worker[T]:\n    def retry[U](self, value: U) -> U:\n        match value:\n            case [first, *rest]:\n                STOP(first)\n        return value\n",
            "worker.go": "package worker\ntype Worker[T any] struct { Value T }\nfunc (w *Worker[T]) retry(value T) T { _ = new(int32(3)); STOP(value); return value }\n",
            "worker.rs": 'struct Worker<T> { value: T }\nimpl<T> Worker<T> { fn retry(&self, value: T) -> T { STOP(value); value } }\nunsafe extern "C" { fn foreign(); }\n',
        }
        for path, source in sources.items():
            self.write(path, source)
            with self.subTest(path=path):
                result = self.raw_cli(
                    "ask",
                    "retry enforcement",
                    "--model",
                    "fixture",
                    "--endpoint",
                    f"http://127.0.0.1:{self.server.server_port}/decisions",
                    "--no-cache",
                    "--json",
                    "--broad",
                    path,
                )
                file = next(
                    json.loads(row)
                    for row in result.stdout.splitlines()
                    if json.loads(row)["type"] == "file"
                )
                self.assertTrue(file["parse_complete"])
                self.assertTrue(
                    {"Worker", "retry"}.issubset(
                        {decl["name"] for decl in file["reading_leads"]}
                    )
                )
                self.assertIn("STOP", {call["name"] for call in file["calls"]})

    def test_auto_falls_back_after_default_engine_size_limit(self):
        self.write("source", "abc123\n")
        self.raw_cli("--regex-size-limit", "1", "[a-z]+[0-9]+", "source", code=2)
        result = self.raw_cli(
            "--engine=auto", "--regex-size-limit", "1", "[a-z]+[0-9]+", "source"
        )
        self.assertEqual(result.stdout, b"abc123\n")

    def test_auth_help_does_not_read_credentials(self):
        result = self.raw_cli("auth", "--help")
        self.assertIn(b"Usage: antfly-grep auth", result.stdout)
        self.assertIn(b"--provider", result.stdout)

    def test_native_setup_doctor_and_skill_installation(self):
        result = self.raw_cli(
            "auth",
            "--provider",
            "antfly",
            "--model",
            "fixture",
            "--base-url",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
            "--stdin",
            input=b"owned-secret\n",
        )
        self.assertNotIn(b"owned-secret", result.stdout + result.stderr)
        result = self.raw_cli("doctor")
        self.assertIn(b"valid response", result.stdout)
        self.assertEqual(self.server.requests[-1][1], "Bearer owned-secret")
        self.raw_cli("skill", "--agent", "codex", "--agent", "claude", "--yes")
        self.assertTrue((self.root / ".agents/skills/antfly-grep/SKILL.md").is_file())
        self.assertTrue((self.root / ".claude/skills/antfly-grep/SKILL.md").is_file())

    def test_provider_presets_custom_routes_and_credentials(self):
        presets = {
            "typesafe": ("https://api.typesafe.ai/v1/systemone", "jev-1.13.0"),
            "vercel": (
                "https://ai-gateway.vercel.sh/typesafe/v1/systemone",
                "typesafe-ai/jev",
            ),
            "openrouter": (
                "https://openrouter.ai/api/v1/systemone",
                "typesafe/jev-1.13",
            ),
            "opencode": ("https://opencode.ai/zen/v1/systemone", "jev-1.13"),
        }
        credentials = self.root / "config/antfly-grep/credentials.json"
        for provider, (endpoint, model) in presets.items():
            with self.subTest(provider=provider):
                result = self.raw_cli(
                    "auth", "--provider", provider, "--stdin", input=b"fixture-secret\n"
                )
                self.assertNotIn(b"fixture-secret", result.stdout + result.stderr)
                data = json.loads(credentials.read_text())
                self.assertEqual((data["endpoint"], data["model"]), (endpoint, model))
                if os.name != "nt":
                    self.assertEqual(credentials.stat().st_mode & 0o777, 0o600)
        endpoint = f"http://127.0.0.1:{self.server.server_port}/v1"
        self.raw_cli(
            "auth",
            "--provider",
            "custom",
            "--base-url",
            endpoint,
            "--model",
            "fixture",
            "--stdin",
            input=b"fixture-secret\n",
        )
        self.raw_cli("doctor")
        self.assertEqual(self.server.requests[-1][0], "/v1/systemone")
        for endpoint in [
            "http://example.com/v1",
            "https://user:secret@example.com",
            "https://example.com?token=secret",
        ]:
            self.raw_cli(
                "auth",
                "--provider",
                "custom",
                "--endpoint",
                endpoint,
                "--model",
                "fixture",
                "--stdin",
                input=b"fixture-secret\n",
                code=2,
            )

    @unittest.skipIf(os.name == "nt", "POSIX terminal echo contract")
    def test_interactive_auth_restores_echo_on_success_and_signal(self):
        import pty
        import signal
        import termios

        for interrupt in [False, True]:
            with self.subTest(interrupt=interrupt):
                master, slave = pty.openpty()
                original = termios.tcgetattr(slave)
                proc = subprocess.Popen(
                    [BINARY, "auth", "--provider", "typesafe"],
                    cwd=self.root,
                    env=self.env,
                    stdin=slave,
                    stdout=slave,
                    stderr=slave,
                )
                transcript = b""
                try:
                    deadline = time.monotonic() + 10
                    while (
                        b"(hidden): " not in transcript and time.monotonic() < deadline
                    ):
                        if select.select([master], [], [], 0.1)[0]:
                            transcript += os.read(master, 4096)
                    self.assertIn(b"(hidden): ", transcript)
                    self.assertFalse(termios.tcgetattr(slave)[3] & termios.ECHO)
                    if interrupt:
                        proc.send_signal(signal.SIGINT)
                    else:
                        os.write(master, b"never-echo-this-secret\n")
                    self.assertEqual(proc.wait(timeout=10), 130 if interrupt else 0)
                    while select.select([master], [], [], 0)[0]:
                        transcript += os.read(master, 4096)
                    self.assertNotIn(b"never-echo-this-secret", transcript)
                    self.assertEqual(termios.tcgetattr(slave), original)
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait()
                    os.close(master)
                    os.close(slave)

    @unittest.skipUnless(shutil.which("rg"), "optional ripgrep differential oracle")
    def test_buffered_regex_proof_preserves_boundaries_and_late_binary(self):
        for position in (65530, 65535, 65536, 131070):
            for suffix in (b"\n", b"\n" + b"x" * 65536 + b"\0late\n"):
                self.write("proof", b"x\n" * (position // 2) + b"TARGET42\n" + suffix)
                for flags in (
                    ["--no-mmap", "-a", "-n"],
                    ["--no-mmap", "-I"],
                    ["--no-mmap", "-c"],
                ):
                    args = [*flags, r"TARGET\d+", "proof"]
                    expected = subprocess.run(
                        ["rg", *args], cwd=self.root, env=self.env, capture_output=True
                    )
                    result = self.raw_cli(*args, code=expected.returncode)
                    self.assertEqual(
                        result.stdout, expected.stdout, (position, suffix[-8:], flags)
                    )

    def test_refinement_batches_lines_across_rejected_regions(self):
        self.server.mode = "reject_blocks"
        self.write(
            "a.txt",
            "STOP one\nnoise\nSTOP two\nnoise\nSTOP three\nnoise\nSTOP four\nnoise\n",
        )
        result = self.raw_cli(
            *self.decision_args(),
            "--no-cache",
            "--refine",
            "--chunk-lines",
            "2",
            "--batch-size",
            "8",
            "-j1",
            "a.txt",
        )
        self.assertIn(b"STOP one", result.stdout)
        self.assertIn(b"STOP four", result.stdout)
        refined = [
            body
            for _, _, body in self.server.requests
            if any(i["id"].startswith("refine-") for i in body.get("inputs", []))
        ]
        self.assertEqual(len(refined), 1)
        self.assertEqual(len(refined[0]["inputs"]), 8)
        self.assertEqual(len({i["id"] for i in refined[0]["inputs"]}), 8)
        self.assertTrue(
            all("CONTEXT region lines" in i["input"] for i in refined[0]["inputs"])
        )
        self.assertTrue(all(set(i) == {"id", "input"} for i in refined[0]["inputs"]))

    def test_escaped_decision_payload_splits_before_exceeding_body_budget(self):
        line = "STOP " + '\\"' * 65536 + "\n"
        self.write("large.txt", line * 64)
        self.raw_cli(
            *self.decision_args(),
            "--no-cache",
            "--unit",
            "line",
            "--context-lines",
            "0",
            "--batch-size",
            "64",
            "-j1",
            "-l",
            "large.txt",
        )
        bodies = [body for _, _, body in self.server.requests]
        self.assertGreater(len(bodies), 1)
        self.assertEqual(sum(len(body["inputs"]) for body in bodies), 64)
        self.assertTrue(
            all(
                item["input"].endswith(line)
                for body in bodies
                for item in body["inputs"]
            )
        )
        self.assertTrue(
            all(len(json.dumps(body).encode()) <= 16 * 1024 * 1024 for body in bodies)
        )

    def test_refinement_partial_budget_keeps_independent_evidence(self):
        self.server.mode = "reject_blocks"
        self.write(
            "a.txt",
            "STOP one\nnoise\nSTOP two\nnoise\nSTOP three\nnoise\nSTOP four\nnoise\n",
        )
        result = self.raw_cli(
            *self.decision_args(),
            "--no-cache",
            "--refine",
            "--chunk-lines",
            "2",
            "--batch-size",
            "8",
            "--max-regions",
            "7",
            "-j1",
            "a.txt",
            code=2,
        )
        self.assertIn(b"STOP one", result.stdout)
        self.assertIn(b"STOP two", result.stdout)
        self.assertNotIn(b"STOP four", result.stdout)
        self.assertIn(b"RegionBudgetExceeded", result.stderr)
        refined = [
            body
            for _, _, body in self.server.requests
            if any(i["id"].startswith("refine-") for i in body.get("inputs", []))
        ]
        self.assertEqual(sum(len(b["inputs"]) for b in refined), 3)

    def test_typesafe_refinement_shares_context_and_retains_all_targets(self):
        self.write("a.txt", "STOP one\nnoise\nSTOP two\nnoise\n")
        self.raw_cli(
            *self.decision_args(),
            "--backend",
            "typesafe",
            "--no-cache",
            "--refine",
            "--chunk-lines",
            "2",
            "--batch-size",
            "8",
            "-j1",
            "a.txt",
        )
        refined = [
            body
            for _, _, body in self.server.requests
            if "BEGIN SHARED CONTEXT" in body.get("state", "")
        ]
        self.assertEqual(len(refined), 1)
        state = refined[0]["state"]
        self.assertEqual(state.count("BEGIN SHARED CONTEXT"), 2)
        self.assertEqual(state.count("CONTEXT region lines"), 2)
        self.assertEqual(state.count("BEGIN TARGET INPUT"), 4)
        self.assertEqual(len(refined[0]["questions"]), 4)

    def test_typesafe_protocol_shares_multiquery_source(self):
        self.write("a", "STOP\n")
        self.raw_cli(
            *self.decision_args(),
            "--backend",
            "typesafe",
            "--query",
            "second question",
            "--no-cache",
            "--unit",
            "line",
            "a",
        )
        self.assertEqual(len(self.server.requests), 1)
        body = self.server.requests[0][2]
        self.assertIn("state", body)
        self.assertEqual(len(body["questions"]), 2)
        self.assertEqual(body["state"].count("TARGET 1: STOP"), 1)

    @unittest.skipUnless(shutil.which("rg"), "optional byte-folding oracle")
    def test_byte_mode_folds_ascii_only(self):
        self.write("bytes", b"A a\n\xc0 \xe0\nK k\n")
        for pattern in [r"\xC0", r"[\xC0]", r"[^\xC0]", r"[A-Z]", r"k"]:
            for flags in [["--no-unicode"], ["-P", "--no-pcre2-unicode"]]:
                with self.subTest(pattern=pattern, flags=flags):
                    args = ["-ai", "-o", *flags, pattern, "bytes"]
                    expected = subprocess.run(
                        ["rg", *args],
                        cwd=self.root,
                        env=self.env,
                        capture_output=True,
                        timeout=5,
                        check=False,
                    )
                    self.assertEqual(
                        self.raw_cli(*args, code=expected.returncode).stdout,
                        expected.stdout,
                    )

    @unittest.skipUnless(shutil.which("rg"), "optional PCRE2 condition oracle")
    def test_pcre_assertion_and_relative_conditions(self):
        self.write("conditions", "abc\nabb\nbbc\ncc\nab\nb\naac\nname=abc\n")
        for pattern in [
            r"(?(?=a)a|b)b",
            r"(?(?!a)b|a)b",
            r"(a)?(?(-1)b|c)",
            r"(?(+1)a|b)(a)?",
            r"(?<n>a)?(?(<n>)b|c)",
            r"(?<n>a)?(?('n')b|c)",
            r"(?(?=(a))\1b|cc)",
            r"(?(?<=a)b|c)",
            r"(?(?<!a)b|a)",
        ]:
            for flags in [[], ["-r", "$1-R"]]:
                with self.subTest(pattern=pattern, flags=flags):
                    args = ["-P", "-n", *flags, pattern, "conditions"]
                    expected = subprocess.run(
                        ["rg", *args],
                        cwd=self.root,
                        env=self.env,
                        capture_output=True,
                        timeout=5,
                        check=False,
                    )
                    self.assertEqual(
                        self.raw_cli(*args, code=expected.returncode).stdout,
                        expected.stdout,
                    )

    @unittest.skipUnless(shutil.which("rg"), "optional PCRE2 oracle")
    def test_native_nonregular_engine_differential(self):
        self.write("a", "abc abc\nname=abc\naaab\nabab\n<abc>\n((x))\n")
        for pattern in [
            r"(?<=name=)\w+",
            r"(\w+) \1",
            r"a++a",
            r"(?>a+)a",
            r"a+?",
            r"a*",
            r"(a)?b(?(1)c|b)",
            r"(?<x>a)b(?&x)",
            r"<(?<x>\w+)>\k<x>",
            r"\((?:[^()]|(?R))*\)",
        ]:
            with self.subTest(pattern=pattern):
                expected = subprocess.run(
                    ["rg", "-Po", pattern, "a"],
                    check=False,
                    cwd=self.root,
                    capture_output=True,
                    timeout=5,
                )
                actual = self.raw_cli("-Po", pattern, "a", code=expected.returncode)
                self.assertEqual(actual.stdout, expected.stdout)

    @unittest.skipUnless(shutil.which("rg"), "optional output/discovery oracle")
    def test_expanded_output_and_binary_differential(self):
        self.write("a", "hit hit\nnoise\nhit\n")
        cases = [
            ["--vimgrep"],
            ["-M0"],
            ["--count-matches", "-v"],
            ["-Hc0"],
            ["--include-zero", "-c"],
            ["-o", "--color=always"],
            ["-Hn", "--color=always"],
            ["--field-match-separator=|", "-Hn"],
            ["--mmap", "-nC1"],
        ]
        for flags in cases:
            with self.subTest(flags=flags):
                expected = subprocess.run(
                    ["rg", *flags, "hit", "a"],
                    check=False,
                    cwd=self.root,
                    capture_output=True,
                    timeout=5,
                )
                actual = self.raw_cli(*flags, "hit", "a", code=expected.returncode)
                self.assertEqual(actual.stdout, expected.stdout)
        for data in [b"hit\0tail\n", b"hit\nnoise\0\n", b"noise\0\nhit\n"]:
            self.write("binary", data)
            for flags in [[], ["--binary"], ["-a"], ["-l"], ["-c"], ["-q"]]:
                with self.subTest(data=data, flags=flags):
                    expected = subprocess.run(
                        ["rg", *flags, "hit", "binary"],
                        check=False,
                        cwd=self.root,
                        capture_output=True,
                        timeout=5,
                    )
                    actual = self.raw_cli(
                        *flags, "hit", "binary", code=expected.returncode
                    )
                    self.assertEqual(actual.stdout, expected.stdout)

    def test_git_config_includes_and_generation(self):
        self.write("global-ignore", "ignored.txt\n")
        config = self.root / "included.cfg"
        config.write_text(
            "[core]\n excludesFile = " + str(self.root / "global-ignore") + "\n"
        )
        self.write(".gitconfig", "[include]\n path = " + str(config) + "\n")
        self.write("ignored.txt", "hit\n")
        self.write("kept.py", "hit\n")
        self.assertNotIn(b"ignored.txt", self.raw_cli("--files", ".").stdout)
        self.assertIn(
            b"kept.py", self.raw_cli("--files", "--iglob", "*.PY", ".").stdout
        )
        for kind in [
            "man",
            "complete-bash",
            "complete-zsh",
            "complete-fish",
            "complete-powershell",
        ]:
            self.assertIn(b"antfly-grep", self.raw_cli("--generate", kind).stdout)
        self.assertIn(b"engine=", self.raw_cli("--debug", "hit", "kept.py").stderr)

    @unittest.skipUnless(shutil.which("rg"), "optional late binary stream oracle")
    def test_late_binary_stream_boundaries(self):
        for offset in [65535, 65536, 65537, 131071, 300007]:
            self.write(
                "binary/a", b"hit\n" + b"x" * (offset - 5) + b"\n\0hit\nendhit\n"
            )
            for target in ["binary/a", "binary"]:
                for flags in [[], ["-n"], ["-c"], ["-l"], ["-L"], ["--binary"]]:
                    with self.subTest(offset=offset, target=target, flags=flags):
                        args = ["--no-mmap", *flags, "hit", target]
                        expected = subprocess.run(
                            ["rg", *args],
                            cwd=self.root,
                            env=self.env,
                            capture_output=True,
                            timeout=10,
                            check=False,
                        )
                        self.assertEqual(
                            self.raw_cli(*args, code=expected.returncode).stdout,
                            expected.stdout,
                        )

    def test_git_config_forward_remote_condition_and_continuations(self):
        self.write(".git/HEAD", "ref: refs/heads/main\n")
        self.write(
            ".git/config",
            '[remote "origin"]\n url = https://example.com/team/project\n',
        )
        self.write("remote-ignore", "ignored.txt\n")
        self.write(
            "conditional.cfg",
            '[core]\n excludesFile = "' + str(self.root) + '/remote-\\\nignore"\n',
        )
        self.write(
            ".gitconfig",
            '[includeIf "hasconfig:remote.*.url:https://example.com/**"]\n path = '
            + str(self.root / "conditional.cfg")
            + "\n",
        )
        self.write("ignored.txt", "hit\n")
        self.write("kept.txt", "hit\n")
        paths = self.raw_cli("--files", ".").stdout
        self.assertNotIn(b"ignored.txt", paths)
        self.assertIn(b"kept.txt", paths)
        self.write(
            ".git/config",
            '[remote "origin"]\n url = https://elsewhere.example/project\n',
        )
        self.assertIn(b"ignored.txt", self.raw_cli("--files", ".").stdout)

    @unittest.skipUnless(shutil.which("rg"), "optional Unicode namespace oracle")
    def test_unicode_property_namespaces_extensions_and_binary_aliases(self):
        self.write("unicode", "A a ー ぁ カ α Σ ∑ ≤ 😀 𝛂 \u0301 \u0378\n")
        for pattern in [
            r"\p{scx=Hira}",
            r"\p{sc=Hiragana}",
            r"\p{scx:Kana}",
            r"\p{gc=Letter}",
            r"\p{gc!=Letter}",
            r"\P{gc!=Letter}",
            r"\p{sc:Greek}",
            r"\p{Age=1.1}",
            r"\p{Age=6.0}",
            r"\p{Age=V6_0}",
            r"\p{Grapheme_Cluster_Break=Extend}",
            r"\p{Word_Break=ALetter}",
            r"\p{Sentence_Break=Upper}",
            r"\p{Emoji}",
            r"\p{Emoji_Presentation}",
            r"\p{Alpha}",
            r"\p{Lower}",
            r"\p{Upper}",
            r"\p{Bidi_Mirrored}",
            r"\p{WSpace}",
            r"\p{gc=Greek}",
            r"\p{sc=Letter}",
            r"\p{Alphabetic=Yes}",
            r"\p{InCB}",
        ]:
            with self.subTest(pattern=pattern):
                args = ["-o", pattern, "unicode"]
                expected = subprocess.run(
                    ["rg", *args],
                    cwd=self.root,
                    env=self.env,
                    capture_output=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(
                    self.raw_cli(*args, code=expected.returncode).stdout,
                    expected.stdout,
                )

    def test_pipeline_large_inventory_and_literal_unicode_modes(self):
        for i in range(600):
            self.write(f"many/{i:04}.txt", "noise\nK kelvin K long ſ\n")
        for flags, expected in [
            (["-Fi", "k"], 600),
            (["-Fi", "s"], 600),
            (["-F", "K"], 600),
        ]:
            result = self.raw_cli("--threads", "3", "-l", *flags, "many")
            self.assertEqual(len(result.stdout.splitlines()), expected)
        self.assertEqual(
            len(self.raw_cli("--threads", "1", "--files", "many").stdout.splitlines()),
            600,
        )

    def test_streamed_concatenated_members_and_corrupt_gzip_footer(self):
        import gzip
        import lzma

        for ext, encode in [("gz", gzip.compress), ("xz", lzma.compress)]:
            self.write("joined." + ext, encode(b"hi") + encode(b"t across members\n"))
            self.assertEqual(
                self.raw_cli("-z", "hit", "joined." + ext).stdout,
                b"hit across members\n",
            )
        broken = bytearray(gzip.compress(b"hit\n"))
        broken[-8] ^= 1
        self.write("bad.gz", broken)
        self.raw_cli("-z", "hit", "bad.gz", code=2)

    @unittest.skipUnless(
        shutil.which("rg"), "optional absolute-anchor replacement oracle"
    )
    def test_absolute_anchor_replacement_buffer_context(self):
        for data in [
            b"foo1\nfoo2\nnoise\nfoo3\nfoo4\n",
            b"foo1\nfoo2\nnoise\nfoo3\nfoo4",
            b"foo1\n" + b"x\n" * 32760 + b"foo2\nfoo3\nfoo4",
        ]:
            self.write("a", data)
            for pattern in [r"\Afoo", r"\A(foo)(\d+)", r"\Afoo|o\d", r"foo\z"]:
                for flags in [
                    ["-r", "R"],
                    ["-or", "$2-$1"],
                    ["-nbr", "R"],
                    ["--color=always", "-r", "R"],
                    ["-P", "-r", "R"],
                ]:
                    with self.subTest(pattern=pattern, flags=flags, size=len(data)):
                        args = [*flags, pattern, "a"]
                        expected = subprocess.run(
                            ["rg", *args],
                            cwd=self.root,
                            env=self.env,
                            capture_output=True,
                            timeout=10,
                            check=False,
                        )
                        self.assertEqual(
                            self.raw_cli(*args, code=expected.returncode).stdout,
                            expected.stdout,
                        )

    @unittest.skipUnless(shutil.which("rg"), "optional replacement backend oracle")
    def test_absolute_anchor_replacement_explicit_backends(self):
        self.write("a", "foo1\nfoo2\nfoo3")
        for backend in ["--mmap", "--no-mmap"]:
            for pattern in [r"\Afoo", r"\Afoo|o\d"]:
                args = [backend, "-r", "R", pattern, "a"]
                expected = subprocess.run(
                    ["rg", *args],
                    cwd=self.root,
                    env=self.env,
                    capture_output=True,
                    timeout=5,
                    check=False,
                )
                self.assertEqual(
                    self.raw_cli(*args, code=expected.returncode).stdout,
                    expected.stdout,
                )

    @unittest.skipUnless(shutil.which("rg"), "optional multiline oracle")
    def test_multiline_whole_line_replacement_differential(self):
        for data in ["xfoo\nbarx\nfoo\nbar\n", "xfoo\nbarx\nnoise\nfoo\nbar\n"]:
            self.write("a", data)
            for flags in [[], ["-n"], ["-nb"], ["-nC1"], ["--color=always"]]:
                for replacement in ["R", "A\nB", "$2-$1"]:
                    args = [*flags, "-U", r"(foo)\n(bar)", "-r", replacement, "a"]
                    expected = subprocess.run(
                        ["rg", *args],
                        check=False,
                        cwd=self.root,
                        capture_output=True,
                        timeout=5,
                    )
                    self.assertEqual(
                        self.raw_cli(*args, code=expected.returncode).stdout,
                        expected.stdout,
                    )

    def test_native_lzip_and_unix_compress(self):
        import lzma
        import zlib

        plain = b"hit lzip\n" * 100
        compressed = lzma.compress(
            plain,
            format=lzma.FORMAT_RAW,
            filters=[
                {
                    "id": lzma.FILTER_LZMA1,
                    "dict_size": 1 << 22,
                    "lc": 3,
                    "lp": 0,
                    "pb": 2,
                }
            ],
        )
        size = 6 + len(compressed) + 20
        member = (
            b"LZIP\x01\x16"
            + compressed
            + zlib.crc32(plain).to_bytes(4, "little")
            + len(plain).to_bytes(8, "little")
            + size.to_bytes(8, "little")
        )
        self.write("a.lz", member)
        self.assertEqual(self.raw_cli("-z", "-m1", "hit", "a.lz").stdout, b"hit lzip\n")
        plain = b"hit Z\n"
        bits = sum(byte << (9 * i) for i, byte in enumerate(plain))
        self.write(
            "a.Z", b"\x1f\x9d\x90" + bits.to_bytes((9 * len(plain) + 7) // 8, "little")
        )
        self.assertEqual(self.raw_cli("-z", "hit", "a.Z").stdout, plain)
        if shutil.which("compress"):
            data = (b"abcdefghijklmno0123456789\n" * 20000) + b"hit wide dictionary\n"
            compressed = subprocess.run(
                ["compress", "-c"], input=data, capture_output=True, check=True
            ).stdout
            self.write("large.Z", compressed)
            self.assertEqual(
                self.raw_cli("-z", "hit", "large.Z").stdout, b"hit wide dictionary\n"
            )

    @unittest.skipUnless(shutil.which("rg"), "optional Unicode/PCRE oracle")
    def test_native_graphemes_and_extended_pcre_escapes(self):
        self.write("a", "é 👩‍💻 🇺🇸\na\tb\n\n")
        for flags, pattern in [
            (["-Po"], r"\X"),
            (["-Po"], r"\h+"),
            (["-Po"], r"\v+"),
            (["-Po"], r"(?n)(a)(?<word>b)"),
            (["-UPo"], r"\R"),
            (["-Po"], r"\Qé\E"),
            (["-Po"], r"(?<word>a)\g{word}"),
            (["-Po"], r"\G\w"),
        ]:
            expected = subprocess.run(
                ["rg", *flags, pattern, "a"],
                check=False,
                cwd=self.root,
                capture_output=True,
                timeout=5,
            )
            self.assertEqual(
                self.raw_cli(*flags, pattern, "a", code=expected.returncode).stdout,
                expected.stdout,
            )

    @unittest.skipUnless(shutil.which("rg"), "optional native backtracking oracle")
    def test_native_branch_reset_and_control_verbs(self):
        self.write("a", 'ab cd ef\nword "quoted" tail\naaab\naaa\n')
        for pattern in [
            r"(?|(a)(b)|(c)(d))\2",
            r"(?|(a)|(c)(d))",
            r'"[^"\n]*"(*SKIP)(*F)|\w+',
            r"a(*PRUNE)b|a",
            r"a(*COMMIT)b|c",
            r"a(*ACCEPT)b",
            r"(?|(a)|(b))\1",
            r"a(*THEN)b|a",
            r"(a(*THEN)b|ac)d|ac",
            r"(a+(*THEN)b|a+)c",
            r"a(*MARK:x)b(*SKIP:x)(*F)|b",
            r"a(*SKIP:missing)b|a",
            r"(*:name)a",
            r"(*MARK:x)a(?C1)b",
            r"(?J)(?:(?<n>a)|(?<n>b))\k<n>",
            r"(?J)(?:(?<n>a)|(?<n>b))(?(n)c|d)",
            r"a(?C{some }} text})b",
        ]:
            with self.subTest(pattern=pattern):
                expected = subprocess.run(
                    ["rg", "-Po", pattern, "a"],
                    check=False,
                    cwd=self.root,
                    capture_output=True,
                    timeout=5,
                )
                self.assertEqual(
                    self.raw_cli("-Po", pattern, "a", code=expected.returncode).stdout,
                    expected.stdout,
                )
        # Deep ordinary captures must not compile every nested body again.
        self.raw_cli("-P", "(" * 100 + "word" + ")" * 100, "a")

    @unittest.skipUnless(shutil.which("rg"), "optional CLI interaction oracle")
    def test_glob_order_zero_dfa_and_encoded_size_limits(self):
        self.write("A.TXT", "hit\n")
        self.write("a.txt", "hit\n")
        self.write("nested/a.txt", "hit\n")
        for flags in [
            ["--iglob", "*.txt", "-g", "!A.TXT"],
            ["-g", "!A.TXT", "--iglob", "*.txt"],
            ["-g", "/a.txt"],
            ["-g", "!nested/"],
            ["--dfa-size-limit", "0"],
        ]:
            with self.subTest(flags=flags):
                expected = subprocess.run(
                    ["rg", "--sort", "path", *flags, "hit", "."],
                    check=False,
                    cwd=self.root,
                    env=self.env,
                    capture_output=True,
                    timeout=5,
                )
                self.assertEqual(
                    self.raw_cli(
                        "--sort", "path", *flags, "hit", ".", code=expected.returncode
                    ).stdout,
                    expected.stdout,
                )
        self.write("encoded", "É\n".encode("cp1252"))
        self.raw_cli("--max-filesize", "2", "--encoding", "cp1252", "É", "encoded")
        self.raw_cli("--max-filesize", "0", "hit", "a.txt", code=1)

    @unittest.skipIf(os.name == "nt", "Unix filenames may contain arbitrary bytes")
    def test_json_non_utf8_paths_and_file_statistics(self):
        import base64

        path = os.fsencode(self.root) + b"/invalid-\xff"
        try:
            with open(path, "wb") as output:
                output.write(b"hit\n")
        except OSError as error:
            if error.errno == 92:
                self.skipTest("filesystem rejects non-UTF8 names")
            raise
        self.write("valid", "hit\n")
        result = self.raw_cli("--json", "--sort", "path", "hit", ".")
        events = [json.loads(line) for line in result.stdout.splitlines()]
        ends = [x["data"] for x in events if x["type"] == "end"]
        self.assertEqual(len(ends), 2)
        self.assertEqual(base64.b64decode(ends[0]["path"]["bytes"]), b"./invalid-\xff")
        for end in ends:
            self.assertEqual(end["stats"]["matched_lines"], 1)
            self.assertGreater(end["stats"]["bytes_printed"], 0)
            self.assertGreater(end["stats"]["elapsed"]["nanos"], 0)

    def test_policy_questions_share_request_and_do_not_invert_exclusions(self):
        self.write("a", "STOP\n")
        self.raw_cli(
            *self.decision_args(), "--unit", "line", "--only", "code", "--no-cache", "a"
        )
        self.assertEqual(len(self.server.requests), 1)
        self.assertEqual(len(self.server.requests[0][2]["questions"]), 2)
        self.server.requests.clear()
        self.raw_cli(
            *self.decision_args(),
            "--unit",
            "line",
            "--not",
            "code",
            "--no-cache",
            "-v",
            "a",
            code=1,
        )
        self.assertEqual(len(self.server.requests), 1)

    def test_transcoded_stdin_streams_before_eof(self):
        for flag, payload in [
            ("auto", "hit É\n".encode("utf-16")),
            ("utf-16be", "hit É\n".encode("utf-16-be")),
            ("windows-1252", "hit É\n".encode("cp1252")),
        ]:
            with self.subTest(encoding=flag):
                proc = subprocess.Popen(
                    [BINARY, "--encoding", flag, "hit"],
                    cwd=self.root,
                    env=self.env,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                try:
                    proc.stdin.write(payload)
                    proc.stdin.flush()
                    ready, _, _ = select.select([proc.stdout], [], [], 3)
                    self.assertTrue(ready, "transcoded output waited for EOF")
                    self.assertEqual(proc.stdout.readline(), "hit É\n".encode())
                    proc.stdin.close()
                    proc.stdin = None
                    self.assertEqual(proc.wait(timeout=5), 0)
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait()
                    if proc.stdin:
                        proc.stdin.close()
                    proc.stdout.close()
                    proc.stderr.close()

    def test_inventory_and_natural_mode_flag_budgets(self):
        self.write("src/a.py", "STOP\n")
        self.write("node_modules/a.js", "STOP\n")
        self.write("credentials.json", "STOP\n")
        result = json.loads(self.raw_cli("files", "--json", ".").stdout)
        self.assertEqual(result["type"], "inventory")
        self.assertEqual(result["files"], 1)
        self.assertEqual(result["groups"][0]["name"], "src")
        result = json.loads(
            self.raw_cli(
                "files", "--include-dependencies", "--include-sensitive", "--json", "."
            ).stdout
        )
        self.assertEqual(result["files"], 3)
        self.server.mode = "exploration"
        self.write("src/b.py", "STOP\n")
        result = self.raw_cli(
            "ask",
            "retry enforcement",
            "--model",
            "fixture",
            "--endpoint",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
            "--no-cache",
            "-n1",
            "-T0.6",
            "-m1",
            "--max-regions=1",
            "--json",
            ".",
        )
        events = [json.loads(row) for row in result.stdout.splitlines()]
        self.assertEqual(sum(row["type"] == "file" for row in events), 1)
        self.assertTrue(any(row["type"] == "omitted_files" for row in events))

    @unittest.skipUnless(shutil.which("rg"), "optional precedence oracle")
    def test_flag_precedence_differential(self):
        self.write("a", "hit hit\nnoise\nhit\n")
        for flags in [
            ["-c", "-l"],
            ["-l", "-c"],
            ["--json", "-c"],
            ["-c", "--json"],
            ["--count-matches", "--count"],
            ["--count", "--count-matches"],
            ["-c", "-o"],
            ["--json", "-l"],
            ["-i", "--no-ignore-case"],
            ["-F", "--no-fixed-strings"],
            ["--stop-on-nonmatch"],
            ["--sort-files"],
        ]:
            with self.subTest(flags=flags):
                expected = subprocess.run(
                    ["rg", *flags, "hit", "a"],
                    check=False,
                    cwd=self.root,
                    capture_output=True,
                    timeout=5,
                )
                actual = self.raw_cli(*flags, "hit", "a", code=expected.returncode)
                if "--json" in flags and flags[-1] == "--json":
                    self.assertEqual(
                        [json.loads(row)["type"] for row in actual.stdout.splitlines()],
                        [
                            json.loads(row)["type"]
                            for row in expected.stdout.splitlines()
                        ],
                    )
                else:
                    self.assertEqual(actual.stdout, expected.stdout)

    @unittest.skipUnless(shutil.which("rg"), "optional gitwildmatch oracle")
    def test_gitwildmatch_differential_matrix(self):
        paths = [
            "a.txt",
            "b.txt",
            "1.txt",
            "a/b.txt",
            "a/x/b.txt",
            "a/x/z.go",
            "src/main.go",
            "src/deep/main.go",
            "#lit",
            "!lit",
            "space ",
            "space",
            "{a,b}.txt",
            "a1.txt",
            "ab.txt",
            "nested/file",
            "[abc",
            "[]",
            "[x]",
            "dir/b",
            "dir/c/d/b",
        ]
        for path in paths:
            self.write(path, "hit\n")
        cases = [
            "*.txt\n!a.txt\n",
            "/a.txt\n",
            "a/**/b.txt\n",
            "**/main.go\n",
            "src/**\n",
            "src/\n!src/deep/main.go\n",
            "[[:digit:]].txt\n",
            "[ab].txt\n",
            "[!a].txt\n",
            "a**.txt\n",
            "[abc\n",
            "[]\n",
            "[***\n",
            "***/[!ab]\n",
            "[!ab]??[!ab]*\n",
            "**/[!ab]\n",
            "[]a]\n",
            "{a,b}.txt\n",
            r"\#lit" + "\n",
            r"\!lit" + "\n",
            r"space\ " + "\n",
            "space \n",
            "a/\n!a/\n!a/b.txt\n",
        ]
        # Deterministic generated combinations exercise classes adjacent to
        # path separators/stars and malformed-bracket literal fallbacks.
        import random

        rng = random.Random(4321)
        atoms = [
            "a",
            "b",
            "src",
            "/",
            "*",
            "**",
            "***",
            "?",
            "[ab]",
            "[!ab]",
            "[a-z]",
            "[",
            r"\[",
            "{a,b}",
        ]
        cases += [
            "".join(rng.choices(atoms, k=rng.randint(1, 6))) + "\n" for _ in range(100)
        ]
        for rules in cases:
            with self.subTest(rules=rules):
                self.write(".gitignore", rules)
                expected = subprocess.run(
                    ["rg", "--files", "--sort=path", "."],
                    check=False,
                    cwd=self.root,
                    env=self.env,
                    capture_output=True,
                    timeout=5,
                )
                actual = self.raw_cli(
                    "--files", "--sort=path", ".", code=expected.returncode
                )
                self.assertEqual(actual.stdout, expected.stdout)

    def test_packaged_tooling_matches_cli(self):
        generated = Path(__file__).resolve().parents[1] / "generated"
        for name, kind in [
            ("antfly-grep.bash", "complete-bash"),
            ("antfly-grep.zsh", "complete-zsh"),
            ("antfly-grep.fish", "complete-fish"),
            ("antfly-grep.ps1", "complete-powershell"),
            ("antfly-grep.1", "man"),
        ]:
            self.assertEqual(
                self.run_cli("--generate", kind).stdout,
                (generated / name).read_bytes(),
                name,
            )

    def test_help_and_version(self):
        self.assertIn(b"--ask", self.run_cli("--help").stdout)
        self.assertIn(b"antfly-grep", self.run_cli("--version").stdout)

    def test_regex_shorthands_case_anchors_and_crlf(self):
        self.write("source.txt", b"Retry_budget:OK42\r\nno\nRETRY_budget:ok43")
        r = self.run_cli("--crlf", "-i", r"^retry\w+:[a-z]+\d{2}$", "source.txt")
        self.assertEqual(
            r.stdout,
            b"source.txt:1:Retry_budget:OK42\nsource.txt:3:RETRY_budget:ok43\n",
        )

    def test_literal_word_whole_line_and_multiple_patterns(self):
        self.write("a", "a.b\nxa.b\na_b\nother\n")
        self.assertEqual(self.run_cli("-Fw", "a.b", "a").stdout, b"a:1:a.b\n")
        self.assertEqual(self.run_cli("-Fx", "a.b", "a").stdout, b"a:1:a.b\n")
        self.assertEqual(
            self.run_cli("-c", "-e", "other", "-e", r"a\.b", "a").stdout, b"a:3\n"
        )

    def test_escapes_and_class_case_folding(self):
        self.write("a", "a\tZ\nAZ\nzZ\n")
        self.assertEqual(self.run_cli("-i", r"a\t[A-Z]", "a").stdout, b"a:1:a\tZ\n")
        self.assertEqual(self.run_cli("-i", "^[^a-y]Z", "a").stdout, b"a:3:zZ\n")
        self.run_cli(r"\bfoo", "a", code=1)
        self.run_cli("(?=a)", "a", code=2)

    def test_context_invert_counts_and_limits(self):
        self.write("a", "before\nhit\nafter\ngap\nlast\nhit\n")
        r = self.run_cli("-C1", "hit", "a")
        self.assertEqual(
            r.stdout, b"a-1-before\na:2:hit\na-3-after\n--\na-5-last\na:6:hit\n"
        )
        self.assertEqual(self.run_cli("-vc", "hit", "a").stdout, b"a:4\n")
        self.assertEqual(self.run_cli("-m1", "hit", "a").stdout, b"a:2:hit\n")
        self.assertEqual(self.run_cli("-l", "hit", "a").stdout, b"a\n")
        self.assertEqual(self.run_cli("-q", "hit", "a").stdout, b"")

    def test_stdin_dash_and_exit_codes(self):
        self.assertEqual(self.run_cli("hi", input=b"hi\nbye").stdout, b"<stdin>:1:hi\n")
        self.assertEqual(self.run_cli("hi", "-", input=b"hi").stdout, b"<stdin>:1:hi\n")
        self.run_cli("absent", input=b"hi", code=1)
        self.run_cli("x", "missing", code=2)
        self.write("a", "hit\n")
        self.run_cli("hit", "missing", "a", code=2)

    def test_nested_ignore_negation_hidden_symlinks_and_globs(self):
        self.write(".gitignore", "*.log\nignored/\n!keep.log\n")
        self.write(".git/info/exclude", "private.txt\n")
        for name in [
            "a.txt",
            "skip.log",
            "keep.log",
            "ignored/a.txt",
            "private.txt",
            ".hidden",
            "nested/a.txt",
            "nested/keep.log",
            "nested/skip.txt",
        ]:
            self.write(name, "hit\n")
        self.write("nested/.ignore", "!*.log\nskip.txt\n")
        (self.root / "link").symlink_to(self.root / "nested", target_is_directory=True)
        lines = set(self.run_cli("-l", "hit", ".").stdout.decode().splitlines())
        self.assertEqual(
            lines, {"./a.txt", "./keep.log", "./nested/a.txt", "./nested/keep.log"}
        )
        self.assertIn(b".hidden", self.run_cli("--hidden", "-l", "hit", ".").stdout)
        self.assertIn(b"skip.log", self.run_cli("--no-ignore", "-l", "hit", ".").stdout)
        self.assertEqual(
            set(
                self.run_cli(
                    "-g", "**/*.txt", "-g", "!nested/**", "-l", "hit", "."
                ).stdout.splitlines()
            ),
            {b"./a.txt", b"./private.txt"},
        )

    def test_literal_alternation_shortcut_matches_grouped_vm(self):
        self.write(
            "alternatives.txt",
            "a aa baa aA A\nfoo foobar bar\nα Σ σ\na\naa\nab\nba\nα\nΣ\nfoo\nfoobar\nbar\n",
        )
        for pattern in ("a|aa", "aa|a", "α|Σ", "a|b|A", "foo|foobar|bar", "ab|ba"):
            for flags in ([], ["-i"], ["-S"], ["-w"], ["-x"], ["-i", "-w"]):
                for mode in (["-o"], ["-c"], ["-r", "<$0:$1>"]):
                    with self.subTest(pattern=pattern, flags=flags, mode=mode):
                        fast = self.raw_cli(
                            *flags, *mode, "-e", pattern, "alternatives.txt"
                        )
                        reference = self.raw_cli(
                            *flags, *mode, "-e", f"(?:{pattern})", "alternatives.txt"
                        )
                        self.assertEqual(fast.stdout, reference.stdout)

    def test_automatic_literal_file_concurrency_honors_explicit_threads(self):
        self.write("threads/file.txt", "hit\n")
        auto = self.raw_cli("--debug", "-l", "hit", "threads")
        workers = int(auto.stderr.split(b"threads=", 1)[1].split()[0])
        self.assertGreaterEqual(workers, 1)
        self.assertLessEqual(workers, 8)
        for jobs in (1, 12):
            explicit = self.raw_cli("--debug", f"-j{jobs}", "-l", "hit", "threads")
            self.assertIn(f"threads={jobs} ".encode(), explicit.stderr)
            self.assertEqual(explicit.stdout, auto.stdout)

    def test_fused_binary_literal_proof_keeps_cross_window_matches(self):
        needle = b"unfindable-literal"
        for offset in (65520, 65535, 65536, 131066):
            with self.subTest(offset=offset):
                self.write("boundary.txt", b"x" * offset + needle + b" tail\nsuffix\n")
                expected = str(offset).encode() + b":" + needle + b"\n"
                self.assertEqual(
                    self.raw_cli("-Fbo", needle.decode(), "boundary.txt").stdout,
                    expected,
                )
                self.assertEqual(
                    self.raw_cli(
                        "--no-mmap", "-Fbo", needle.decode(), "boundary.txt"
                    ).stdout,
                    expected,
                )

    def test_literal_prefix_skipping_preserves_records_and_output(self):
        self.write(
            "prefix.txt",
            "miss first\n" * 500
            + "noise needle\nnoise xneedleSuffix\nnoise K\nlast NEEDLE\n",
        )
        for flags in (
            [],
            ["-i"],
            ["-w"],
            ["-x"],
            ["-i", "-w"],
            ["-A1"],
            ["-B1"],
            ["-m1"],
        ):
            for mode in (["-nH"], ["-l"], ["-c"], ["-o"], ["-r", "<$0>"]):
                with self.subTest(flags=flags, mode=mode):
                    expected_code = 1 if "-x" in flags else 0
                    fast = self.raw_cli(
                        *flags,
                        *mode,
                        "-e",
                        "needle|NEEDLE|K",
                        "prefix.txt",
                        code=expected_code,
                    )
                    reference = self.raw_cli(
                        *flags,
                        *mode,
                        "-e",
                        "(?:needle|NEEDLE|K)",
                        "prefix.txt",
                        code=expected_code,
                    )
                    self.assertEqual(fast.stdout, reference.stdout)

    def test_directory_lookahead_preserves_ignore_precedence_and_overflow(self):
        # Exercise both cache limits and streaming fallback. Ignore files are
        # created after entries so their rules must apply to earlier entries.
        self.write(".git/config", "")
        for label, count, width in [
            ("small", 12, 8),
            ("entries", 140, 8),
            ("names", 90, 120),
        ]:
            for i in range(count):
                self.write(f"{label}/{i:04}-{'x' * width}.txt", "hit\n")
            self.write(f"{label}/.gitignore", "*.txt\n!keep.txt\n")
            self.write(f"{label}/.ignore", "!*.txt\nblocked.txt\n")
            self.write(f"{label}/keep.txt", "hit\n")
            self.write(f"{label}/blocked.txt", "hit\n")
            paths = set(self.raw_cli("--files", label).stdout.splitlines())
            self.assertEqual(len(paths), count + 1)
            self.assertIn(f"{label}/keep.txt".encode(), paths)
            self.assertNotIn(f"{label}/blocked.txt".encode(), paths)
            self.assertEqual(
                paths, set(self.raw_cli("-l", "hit", label).stdout.splitlines())
            )
            self.assertEqual(
                len(self.raw_cli("--files", "--no-ignore", label).stdout.splitlines()),
                count + 2,
            )

    def test_parallel_discovery_matches_serial_with_nested_scopes_and_wide_frontier(
        self,
    ):
        self.write(".ignore", "*.log\n")
        for i in range(150):
            prefix = f"tree/{i:03}"
            self.write(f"{prefix}/.ignore", "*.txt\n!keep.txt\n")
            self.write(f"{prefix}/keep.txt", "hit\n")
            self.write(f"{prefix}/blocked.txt", "hit\n")
            self.write(f"{prefix}/blocked.log", "hit\n")
            self.write(f"{prefix}/child/.ignore", "!blocked.txt\n")
            self.write(f"{prefix}/child/blocked.txt", "hit\n")
        serial = set(self.raw_cli("--files", "-j1", "tree").stdout.splitlines())
        self.assertEqual(len(serial), 300)
        nul = self.raw_cli("--files", "-0", "-j4", "tree").stdout
        self.assertTrue(nul.endswith(b"\0"))
        self.assertEqual(set(nul[:-1].split(b"\0")), serial)
        bounded = self.raw_cli(
            "--files", "-j4", "--max-output-bytes=64", "tree", code=2
        )
        self.assertLessEqual(len(bounded.stdout), 64)
        self.assertIn(b"OutputBudgetExceeded", bounded.stderr)
        self.assertEqual(
            set(
                self.raw_cli(
                    "--files", "-j4", "--line-buffered", "tree"
                ).stdout.splitlines()
            ),
            serial,
        )
        for jobs in (None, 2, 4, 12):
            with self.subTest(jobs=jobs):
                parallel = self.raw_cli(
                    "--files", *([] if jobs is None else [f"-j{jobs}"]), "tree"
                ).stdout.splitlines()
                self.assertEqual(len(parallel), len(serial))
                self.assertEqual(set(parallel), serial)
                self.assertEqual(
                    set(
                        self.raw_cli(
                            "-l",
                            *([] if jobs is None else [f"-j{jobs}"]),
                            "hit",
                            "tree",
                        ).stdout.splitlines()
                    ),
                    serial,
                )
        self.assertEqual(
            self.raw_cli("--files", "--sort=path", "tree").stdout.splitlines(),
            sorted(serial),
        )
        self.assertEqual(
            set(
                self.raw_cli(
                    "--files", "-j4", "--max-depth=2", "tree"
                ).stdout.splitlines()
            ),
            {p for p in serial if b"/child/" not in p},
        )

    def test_subtree_keeps_ancestor_ignores(self):
        self.write(".gitignore", "*.log\n")
        self.write("sub/a.log", "hit\n")
        self.write("sub/a.txt", "hit\n")
        self.assertEqual(self.run_cli("--files", "sub").stdout, b"sub/a.txt\n")

    def test_global_ignore(self):
        self.write("config/git/ignore", "*.log\n")
        self.write("a.log", "hit\n")
        self.write("a.txt", "hit\n")
        self.assertEqual(self.run_cli("-l", "hit", ".").stdout, b"./a.txt\n")

    def test_binary_skip_json_offsets_and_invalid_utf8(self):
        self.write("binary", b"hit\0\n")
        self.assertIn(b"binary file matches", self.run_cli("hit", "binary").stdout)
        self.assertIn(b"hit\0", self.run_cli("-a", "hit", "binary").stdout)
        self.write("a", b"no\r\nhit\xff\n")
        events = [
            json.loads(x)
            for x in self.run_cli("--json", "hit", "a").stdout.splitlines()
        ]
        self.assertEqual(
            [e["type"] for e in events], ["begin", "match", "end", "summary"]
        )
        self.assertEqual(events[1]["byte_offset"], 4)
        self.assertEqual(events[1]["line_number"], 2)
        self.assertIsNone(events[1]["text"])
        self.assertEqual(events[1]["bytes_base64"], "aGl0/w==")

    def test_invalid_flags_regex_and_size_ceiling(self):
        self.write("a", "hit\n")
        for args in [
            ("--unknown", "x"),
            ("[", "a"),
            ("*x", "a"),
            ("a**", "a"),
            ("--threshold", "nan", "hit", "a"),
            ("--batch-size", "129", "hit", "a"),
            ("--ask", "q", "a"),
            ("--max-file-size", "2", "hit", "a"),
        ]:
            with self.subTest(args=args):
                self.run_cli(*args, code=2)

    def test_decision_batch_real_http_target_context_and_auth(self):
        self.write("a", "noise\nSTOP retrying\nnoise\nSTOP budget\n")
        self.env["ANTFLY_GREP_TOKEN"] = "fixture-secret"
        r = self.ask("--chunk-lines", "1", "--batch-size", "2", "--json", "a")
        events = [json.loads(x) for x in r.stdout.splitlines()]
        hits = [e for e in events if e["type"] == "match"]
        self.assertEqual([h["line_number"] for h in hits], [2, 4])
        self.assertTrue(
            all(h["scope"] == "line" and h["probability"] == 0.9 for h in hits)
        )
        self.assertEqual(len(self.server.requests), 2)
        path, auth, request = self.server.requests[0]
        self.assertEqual((path, auth), ("/decisions", "Bearer fixture-secret"))
        self.assertEqual(request["model"], "fixture")
        self.assertEqual(request["questions"][0]["type"], "predicate")
        self.assertIn("CONTEXT", request["inputs"][0]["input"])
        self.assertEqual(events[-1]["stats"]["regions_evaluated"], 4)
        self.assertEqual(events[-1]["input_tokens"], 20)

    def test_decision_prefilter_threshold_and_inversion(self):
        self.write("a", "noise\nSTOP retry\nSTOP budget\n")
        self.assertIn(b"a:2:", self.ask("--unit", "line", "-e", "retry", "a").stdout)
        self.assertEqual(len(self.server.requests), 1)
        self.assertEqual(len(self.server.requests[0][2]["inputs"]), 1)
        self.ask("--unit", "line", "--threshold", "0.99", "a", code=1)
        r = self.ask("--unit", "line", "-v", "a")
        self.assertIn(b"a:1:", r.stdout)
        self.assertNotIn(b"a:2:", r.stdout)

    def test_budget_is_partial_and_keeps_evaluated_matches(self):
        self.write("a", "STOP\nnoise\nSTOP\n")
        events = [
            json.loads(x)
            for x in self.ask(
                "--unit", "line", "--max-regions", "1", "--json", "a", code=2
            ).stdout.splitlines()
        ]
        self.assertTrue(any(e["type"] == "match" for e in events))
        self.assertFalse(events[-1]["stats"]["complete"])
        self.assertEqual(events[-1]["stats"]["regions_evaluated"], 1)

    def test_failed_later_batch_keeps_successful_results(self):
        self.write("a", "STOP\nnoise\n")
        self.server.mode = "fail_second"
        events = [
            json.loads(x)
            for x in self.ask(
                "--unit", "line", "--batch-size", "1", "--json", "a", code=2
            ).stdout.splitlines()
        ]
        self.assertEqual(
            [e["line_number"] for e in events if e["type"] == "match"], [1]
        )
        self.assertFalse(events[-1]["stats"]["complete"])

    @unittest.skipUnless(shutil.which("rg"), "optional ripgrep differential oracle")
    def test_supported_regex_differential_against_ripgrep(self):
        self.write(
            "a",
            "alpha\nAlpha42\nretry_budget:OK42\na.b\nword wordy\n\n42\ntab\tvalue\nlast",
        )
        cases = [
            ([], "alpha"),
            (["-i"], "alpha"),
            ([], r"\w+:\w+\d{2}"),
            ([], "^$"),
            (["-w"], "word"),
            (["-x"], "42"),
            (["-F"], "a.b"),
            (["-v"], "alpha"),
            ([], "alpha|last"),
            ([], "[a-z]{3,5}"),
            ([], r"tab\svalue"),
        ]
        for flags, pattern in cases:
            with self.subTest(flags=flags, pattern=pattern):
                expected = subprocess.run(
                    [
                        "rg",
                        "--color=never",
                        "--no-heading",
                        "--with-filename",
                        "-n",
                        *flags,
                        pattern,
                        "a",
                    ],
                    check=False,
                    cwd=self.root,
                    capture_output=True,
                    timeout=5,
                )
                actual = self.run_cli(*flags, pattern, "a", code=expected.returncode)
                self.assertEqual(actual.stdout, expected.stdout)

    def raw_cli(self, *args, input=None, code=0, timeout=20):
        result = subprocess.run(
            [BINARY, *args],
            check=False,
            cwd=self.root,
            env=self.env,
            input=input,
            capture_output=True,
            timeout=timeout,
        )
        self.assertEqual(
            result.returncode, code, result.stderr.decode(errors="replace")
        )
        self.assertNotIn(b"leaked", result.stderr)
        return result

    def decision_args(self):
        return [
            "--ask",
            "find retry enforcement",
            "--model",
            "fixture",
            "--endpoint",
            f"http://127.0.0.1:{self.server.server_port}/decisions",
        ]

    @unittest.skipUnless(shutil.which("rg"), "optional ripgrep differential oracle")
    def test_native_unicode_spans_replacements_and_output_options(self):
        self.write("a", "éé Σίσυφος\nword=42 aaaa ab\nfoo\nbar\nlast\n")
        cases = [
            ([], r"\b\w+\b"),
            (["-o"], r"\p{Greek}+"),
            (["-i", "-o"], "é+"),
            (["-o"], "a+?"),
            (["-o"], "a|aa"),
            (["-r", "$2-$1"], r"(\w+)=(\d+)"),
            (["-r", "${name}:$2:$$"], r"(?P<name>\w+)=(\d+)"),
            (["-n", "--column", "-o"], r"\d+"),
            (["-U", "-o"], r"foo\nbar"),
            (["-U", "--multiline-dotall", "-o"], "foo.*bar"),
            (["-w", "-o"], "a|ab"),
            (["-b", "-o"], r"\p{L}+"),
            (["-o"], "[[:alpha:]]+"),
            (["-o"], "[a-z&&[^aeiou]]+"),
            (["-nC", "1"], "foo"),
            (["-Hn"], "foo"),
            (["--heading", "-Hn"], "foo"),
            (["--count-matches"], r"\w+"),
            (["-c"], "absent"),
            (["-S"], "FOO"),
            (["-iS"], "FOO"),
            (["-Si"], "FOO"),
            (["--null", "-l"], "foo"),
            (["-f", "patterns"], None),
        ]
        self.write("patterns", "foo\nbar\n")
        for flags, pattern in cases:
            with self.subTest(flags=flags, pattern=pattern):
                args = [*flags, *([] if pattern is None else [pattern]), "a"]
                expected = subprocess.run(
                    ["rg", *args],
                    check=False,
                    cwd=self.root,
                    env=self.env,
                    capture_output=True,
                    timeout=10,
                )
                actual = self.raw_cli(*args, code=expected.returncode)
                self.assertEqual(actual.stdout, expected.stdout)

    @unittest.skipUnless(shutil.which("rg"), "optional ripgrep differential oracle")
    def test_ordered_vm_greedy_empty_and_capture_regressions(self):
        self.write("a", "aaa ab aaab abc\né Σ\n\n")
        for pattern in [
            "a*",
            "a*?",
            "(a|aa)+",
            "(aa|a)+",
            "(a?)*",
            "(?:ab|a)b?",
            "(a+)(b?)",
            "a{0,2}",
            "a{0,2}?",
            r"\b\w+\b",
            "(?i)[^a]+",
            "(?i)é",
            "(?U)a+",
            "(?x) a + # comment",
            "[a-z--[aeiou]]+",
        ]:
            for output in [["-o"], ["-r", "$1-$2"]]:
                with self.subTest(pattern=pattern, output=output):
                    args = [*output, pattern, "a"]
                    expected = subprocess.run(
                        ["rg", *args],
                        check=False,
                        cwd=self.root,
                        env=self.env,
                        capture_output=True,
                        timeout=10,
                    )
                    actual = self.raw_cli(*args, code=expected.returncode)
                    self.assertEqual(actual.stdout, expected.stdout)

    def test_stdin_emits_before_eof_for_regex_and_decisions(self):
        for args in [
            ["-n", "hit"],
            [*self.decision_args(), "--unit", "line", "--no-cache", "-n"],
        ]:
            with self.subTest(args=args):
                proc = subprocess.Popen(
                    [BINARY, *args],
                    cwd=self.root,
                    env=self.env,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                try:
                    proc.stdin.write(b"hit STOP\n")
                    proc.stdin.flush()
                    self.assertTrue(
                        select.select([proc.stdout], [], [], 5)[0],
                        "no output before EOF",
                    )
                    line = proc.stdout.readline()
                    self.assertIn(b"hit STOP", line)
                    proc.stdin.close()
                    self.assertEqual(proc.wait(timeout=5), 0, proc.stderr.read())
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait()
                    proc.stdout.close()
                    proc.stderr.close()

    def test_files_larger_than_old_ceiling_stream_successfully(self):
        self.write("large", b"noise\n" * (3 * 1024 * 1024) + b"hit\n")
        self.assertEqual(self.raw_cli("hit", "large").stdout, b"hit\n")

    def test_static_musl_local_inference_reports_loader_requirement(self):
        if b"linux-musl static" not in self.raw_cli("--version").stdout:
            self.skipTest("static musl loader regression")
        self.write("a", "STOP\n")
        result = self.raw_cli(
            "--ask",
            "retry",
            "--model",
            "fixture",
            "--backend",
            "local",
            "--library",
            NATIVE_LIBRARY,
            "a",
            code=2,
        )
        self.assertIn(b"DynamicInferenceRequiresDynamicLibc", result.stderr)

    def test_native_in_process_decisions_require_no_http_endpoint(self):
        if b"linux-musl static" in self.raw_cli("--version").stdout:
            self.skipTest("static musl cannot relocate inference shared libraries")
        self.write("a", "noise\nSTOP\n")
        result = self.raw_cli(
            "--ask",
            "find retry enforcement",
            "--model",
            "fixture",
            "--backend",
            "local",
            "--library",
            NATIVE_LIBRARY,
            "--unit",
            "line",
            "--context-lines",
            "0",
            "--no-cache",
            "a",
        )
        self.assertIn(b"STOP", result.stdout)
        self.assertNotIn(b"noise", result.stdout)
        self.assertEqual(len(self.server.requests), 0)

    def test_decision_cache_scope_and_no_cache(self):
        self.write("a", "STOP\nnoise\n")
        args = [*self.decision_args(), "--unit", "line", "a"]
        first = self.raw_cli(*args)
        n = len(self.server.requests)
        self.assertGreater(n, 0)
        self.assertEqual(self.raw_cli(*args).stdout, first.stdout)
        self.assertEqual(len(self.server.requests), n)
        self.raw_cli(*args, "--no-cache")
        self.assertGreater(len(self.server.requests), n)
        self.raw_cli("cache", "clear")
        self.assertFalse((self.root / ".cache/antfly-grep/decisions").exists())

    def test_decision_batches_and_files_are_concurrent_and_ranked(self):
        self.server.delay = 0.05
        self.server.barrier = threading.Barrier(2)
        self.write("a", "STOP LOW\n")
        self.write("b", "STOP HIGH\n")
        r = self.raw_cli(
            *self.decision_args(), "--unit", "line", "--no-cache", "-j2", "a", "b"
        )
        self.assertGreaterEqual(self.server.peak, 2)
        self.assertTrue(r.stdout.startswith(b"b:"), r.stdout)
        self.server.peak = 0
        self.server.requests = []
        self.write("big", "STOP\n" * 8)
        self.raw_cli(
            *self.decision_args(),
            "--unit",
            "line",
            "--batch-size",
            "1",
            "--no-cache",
            "-j4",
            "big",
        )
        self.assertGreaterEqual(self.server.peak, 2)
        self.assertLessEqual(self.server.peak, 4)

    def test_completed_negative_refinement_does_not_restore_region(self):
        self.write(
            "negative.py", "def retry():\n    unrelated = 1\n    return unrelated\n"
        )
        for mode, extra in (
            ("reject_lines", []),
            ("accept_lines", ["-v"]),
            ("reject_line_policy", ["--only", "allowed"]),
        ):
            with self.subTest(mode=mode):
                self.server.mode = mode
                result = self.raw_cli(
                    *self.decision_args(),
                    "--no-cache",
                    "--refine",
                    "--json",
                    *extra,
                    "negative.py",
                    code=1,
                )
                self.assertFalse(
                    any(
                        json.loads(line)["type"] == "match"
                        for line in result.stdout.splitlines()
                    )
                )

    def test_default_request_budget_keeps_foreground_evidence(self):
        docs = [
            self.indexed_doc(f"f{i:04}.txt", "STOP retry\n" * 100) for i in range(501)
        ]
        outputs = []
        for prefetch in (False, True):
            self.server.requests.clear()
            self.server.index_requests.clear()
            config = self.table_config(docs, prefetch=prefetch)
            self.server.index_documents.sort(key=lambda doc: doc["_id"])
            result = self.raw_cli(
                *self.decision_args(),
                "--no-cache",
                "--no-refine",
                "--chunk-lines=2000",
                "--table-config",
                config,
                code=2,
                timeout=120,
            )
            self.assertEqual(len(self.server.requests), 500)
            self.assertEqual(len(self.server.index_requests), 500)
            self.assertEqual(len(result.stdout.splitlines()), 50000)
            outputs.append(result.stdout)
        self.assertEqual(*outputs)

    def test_types_include_hidden_files_and_keep_directory_and_ignore_rules(self):
        self.write("visible.py", "hit\n")
        self.write(".hidden.py", "hit\n")
        self.write(".hidden-dir/inside.py", "hit\n")
        self.write("ignored.py", "hit\n")
        self.write(".gitignore", "ignored.py\n")
        for threads in ("-j1", "-j4"):
            result = self.raw_cli("--files", "--type=py", threads, ".")
            self.assertEqual(
                set(result.stdout.splitlines()), {b"./visible.py", b"./.hidden.py"}
            )
            result = self.raw_cli("--type=py", threads, "hit", ".")
            self.assertEqual(
                set(result.stdout.splitlines()),
                {b"./visible.py:hit", b"./.hidden.py:hit"},
            )
            self.assertEqual(
                self.raw_cli("--files", "--type=all", threads, ".").stdout.count(
                    b".hidden.py"
                ),
                1,
            )
        config = self.table_config(
            [self.indexed_doc(".hidden.py"), self.indexed_doc(".hidden-dir/inside.py")]
        )
        result = self.raw_cli("--table-config", config, "--type=py", "STOP")
        self.assertIn(b".hidden.py", result.stdout)
        self.assertNotIn(b"inside.py", result.stdout)

    def test_decision_workers_and_connections_survive_batch_waves(self):
        self.server.delay = 0.003
        self.write("a", "STOP retry\n" * 64)
        result = self.raw_cli(
            *self.decision_args(),
            "--no-cache",
            "--unit=line",
            "--no-refine",
            "--batch-size=1",
            "-j4",
            "a",
        )
        self.assertEqual(len(result.stdout.splitlines()), 64)
        self.assertEqual(len(self.server.requests), 64)
        self.assertLessEqual(len(set(self.server.client_ports)), 4)
        self.assertGreaterEqual(self.server.peak, 2)

    def test_persistent_decision_workers_release_after_partial_budget_failure(self):
        self.write("a", "STOP retry\n" * 64)
        result = self.raw_cli(
            *self.decision_args(),
            "--no-cache",
            "--unit=line",
            "--no-refine",
            "--batch-size=1",
            "--max-requests=2",
            "-j4",
            "--json",
            "a",
            code=2,
        )
        rows = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(sum(row["type"] == "match" for row in rows), 2)
        self.assertEqual(len(self.server.requests), 2)
        self.assertFalse(rows[-1]["stats"]["complete"])

    def test_span_replay_preserves_json_color_and_zero_width_output(self):
        self.write("a", "ab aaaa\n\nlast ab")
        for pattern in ("a", "a*", "^|$", "a|ab", "[ab]+"):
            for flags in (
                ["--json"],
                ["--color=always"],
                ["-o", "--color=never"],
                ["-o", "--column", "--color=always"],
            ):
                with self.subTest(pattern=pattern, flags=flags):
                    result = self.raw_cli(*flags, pattern, "a")
                    if "--json" in flags:
                        records = [
                            json.loads(line) for line in result.stdout.splitlines()
                        ]
                        matches = [r for r in records if r["type"] == "match"]
                        self.assertTrue(matches)
                        self.assertTrue(all("source" in r for r in matches))
                    if shutil.which("rg") and "--json" not in flags:
                        expected = subprocess.run(
                            ["rg", *flags, pattern, "a"],
                            cwd=self.root,
                            capture_output=True,
                            check=False,
                        )
                        self.assertEqual(result.stdout, expected.stdout)
        # A bare final CR becomes an empty unterminated record under CRLF.
        # Suppressed terminal-empty spans must not inflate occurrence counters.
        self.write("bare-cr", b"\r")
        result = self.raw_cli("--json", "--crlf", "-F", "", "bare-cr")
        rows = [json.loads(line) for line in result.stdout.splitlines()]
        end = next(row for row in rows if row["type"] == "end")
        self.assertEqual(end["data"]["stats"]["matches"], 0)
        # Binary submatches and finite budgets retain valid JSON framing.
        self.write("bytes", b"\xff\xff\n")
        result = self.raw_cli("--json", "--no-unicode", "-o", ".", "bytes")
        matches = [
            json.loads(line)
            for line in result.stdout.splitlines()
            if json.loads(line)["type"] == "match"
        ]
        self.assertEqual(
            matches[0]["data"]["submatches"][0]["match"], {"bytes": "/w=="}
        )
        self.write("dense", "k " * 262144 + "\n")
        result = self.raw_cli("--json", "--max-output-bytes=1000", "k", "dense", code=2)
        for line in result.stdout.splitlines():
            json.loads(line)

    def test_explicit_request_budget_prioritizes_decisions_over_prefetch(self):
        docs = [self.indexed_doc(f"f{i}.txt", "STOP retry\n" * 1000) for i in range(2)]
        outputs = []
        for prefetch in (False, True):
            self.server.requests.clear()
            self.server.index_requests.clear()
            config = self.table_config(docs, prefetch=prefetch)
            result = self.raw_cli(
                *self.decision_args(),
                "--no-cache",
                "--no-refine",
                "--chunk-lines=2000",
                "--max-requests=2",
                "--table-config",
                config,
                code=2,
            )
            self.assertEqual(len(self.server.requests), 1)
            self.assertEqual(len(self.server.index_requests), 1)
            self.assertEqual(len(result.stdout.splitlines()), 1000)
            outputs.append(result.stdout)
        self.assertEqual(*outputs)
        # An explicit default still expresses a budget and must keep ordering.
        self.server.mode = "prefetch"
        self.server.requests.clear()
        self.server.prefetch_seen = False
        self.server.index_next.clear()
        config = self.table_config(docs, prefetch=True)
        self.raw_cli(
            *self.decision_args(),
            "--no-cache",
            "--no-refine",
            "--max-requests=1000",
            "--table-config",
            config,
        )
        self.assertFalse(self.server.prefetch_seen)

    def test_batched_cache_validates_records_and_retains_partial_scores(self):
        self.write("a", "STOP retry\nnoise\n" * 16)
        args = [
            *self.decision_args(),
            "--unit=line",
            "--no-refine",
            "--cache-dir=scores",
            "a",
        ]
        first = self.raw_cli(*args)
        records = list((self.root / "scores").glob("batch-v6-*"))
        self.assertTrue(records)
        self.assertLess(len(records), 32)
        requests = len(self.server.requests)
        mtimes = [p.stat().st_mtime_ns for p in records]
        self.assertEqual(first.stdout, self.raw_cli(*args).stdout)
        self.assertEqual(len(self.server.requests), requests)
        self.assertEqual(mtimes, [p.stat().st_mtime_ns for p in records])
        # Missing entries are reevaluated; valid zero scores remain cache hits.
        for path in records:
            data = json.loads(path.read_text())
            data["scores"][0] = None
            path.write_text(json.dumps(data))
        self.assertEqual(first.stdout, self.raw_cli(*args).stdout)
        self.assertGreater(len(self.server.requests), requests)
        requests = len(self.server.requests)
        # Malformed/version-mismatched records must never supply evidence.
        for path in records:
            path.write_text('{"version":6,"scores":[2]}')
        self.assertEqual(first.stdout, self.raw_cli(*args).stdout)
        self.assertGreater(len(self.server.requests), requests)
        self.assertFalse(list((self.root / "scores").glob("*.tmp")))

    def test_legacy_cache_migrates_to_atomic_batch_records(self):
        self.write("a", "STOP retry\nSTOP retry\n")
        args = [
            *self.decision_args(),
            "--unit=line",
            "--no-refine",
            "--cache-dir=scores",
            "a",
        ]
        expected = self.raw_cli(*args).stdout
        requests = list(self.server.requests)
        directory = self.root / "scores"
        for path in directory.iterdir():
            path.unlink()
        # Published v5 key framing and scalar format remain readable by v6.
        legacy = []
        for _, _, body in requests:
            for item in body["inputs"]:
                digest = hashlib.sha256()
                parts = [
                    b"antfly-grep-decisions-v5",
                    self.decision_args()[-1].encode(),
                    b"fixture",
                    b"",
                    b"http",
                    b"",
                    b"",
                    b"find retry enforcement",
                    b"line",
                    b"direct",
                    hashlib.sha256(b"").digest(),
                    item["input"].encode(),
                ]
                for part in parts:
                    digest.update(len(part).to_bytes(8, "little"))
                    digest.update(part)
                for tag in (b"filters", b"only", b"not"):
                    digest.update(tag)
                path = directory / digest.hexdigest()
                path.write_text("0.9")
                legacy.append(path)
        self.server.requests.clear()
        self.assertEqual(expected, self.raw_cli(*args).stdout)
        self.assertEqual(len(self.server.requests), 0)
        self.assertTrue(list(directory.glob("batch-v6-*")))
        for path in set(legacy):
            path.unlink()
        self.assertEqual(expected, self.raw_cli(*args).stdout)
        self.assertEqual(len(self.server.requests), 0)
        self.env["ANTFLY_GREP_TOKEN"] = "different-credential"
        self.raw_cli(*args)
        self.assertGreater(len(self.server.requests), 0)

    def test_dense_column_needs_only_first_span(self):
        self.write("dense", "  k " * 65536 + "\n")
        for io_mode in ("--mmap", "--no-mmap"):
            result = self.raw_cli(
                "--color=never", "-n", "--column", io_mode, "k", "dense"
            )
            self.assertEqual(result.stdout, b"1:3:" + ("  k " * 65536).encode() + b"\n")
            stats = self.raw_cli(
                "--color=never", "--column", "--stats", io_mode, "k", "dense"
            )
            self.assertIn(b"65536 matches", stats.stderr)
        self.write("a", "xx kk\nlast\n")
        self.assertEqual(
            self.raw_cli("--column", "-o", "k", "a").stdout, b"1:4:k\n1:5:k\n"
        )
        self.assertEqual(
            self.raw_cli("--column", "-r", "X", "k", "a").stdout, b"1:4:xx XX\n"
        )

    def test_indexed_prefetch_overlaps_decisions_and_preserves_output(self):
        docs = [self.indexed_doc(f"f{i}.txt", "STOP retry\n") for i in range(3)]
        config = self.table_config(docs, prefetch=True)
        self.server.mode = "prefetch"
        parallel = self.raw_cli(
            *self.decision_args(),
            "--no-cache",
            "--max-requests=unlimited",
            "--table-config",
            config,
        )
        self.assertTrue(self.server.prefetch_seen)
        self.server.mode = "ok"
        config = self.table_config(docs, prefetch=False)
        sequential = self.raw_cli(
            *self.decision_args(), "--no-cache", "--table-config", config
        )
        self.assertEqual(parallel.stdout, sequential.stdout)

    def test_indexed_prefetch_cancels_blocked_request_on_output_limit(self):
        config = self.table_config([self.indexed_doc(f"f{i}.txt") for i in range(3)])
        self.server.mode = "prefetch"
        self.server.index_mode = "blocked-prefetch"
        start = time.monotonic()
        try:
            self.raw_cli(
                *self.decision_args(),
                "--no-cache",
                "--max-requests=unlimited",
                "--table-config",
                config,
                "--max-output-bytes=1",
                "--timeout-ms=10000",
                code=2,
            )
            self.assertTrue(self.server.index_next.is_set())
            self.assertLess(time.monotonic() - start, 3)
        finally:
            self.server.index_release.set()

    def test_indexed_quiet_does_not_prefetch(self):
        config = self.table_config([self.indexed_doc(f"f{i}.txt") for i in range(3)])
        self.raw_cli("--table-config", config, "-q", "STOP")
        self.assertEqual(len(self.server.index_requests), 1)

    def test_shared_decision_context_matches_legacy(self):
        self.write(
            "context.py",
            "def retry():\n    unrelated = 1\n    # STOP retries\n    return unrelated\n",
        )
        args = [*self.decision_args(), "--no-cache", "--refine", "context.py"]
        legacy = self.raw_cli(*args)
        self.server.requests.clear()
        shared = self.raw_cli(*args, "--decision-context=shared-v1")
        self.assertEqual(shared.stdout, legacy.stdout)
        batches = [
            body
            for _, _, body in self.server.requests
            if any(x["id"].startswith("refine-") for x in body.get("inputs", []))
        ]
        self.assertTrue(batches)
        body = batches[0]
        self.assertEqual(body["context_version"], 1)
        self.assertEqual(len(body["contexts"]), 1)
        self.assertEqual({x["context_index"] for x in body["inputs"]}, {0})
        self.assertTrue(all("CONTEXT region" not in x["input"] for x in body["inputs"]))

    def test_declaration_regions_refine_to_independently_scored_lines(self):
        self.write(
            "retry.py",
            "def retry():\n    unrelated = 1\n    # STOP retries\n    return unrelated\n",
        )
        result = self.raw_cli(*self.decision_args(), "--no-cache", "--json", "retry.py")
        events = [json.loads(x) for x in result.stdout.splitlines()]
        self.assertEqual(
            [e["line_number"] for e in events if e["type"] == "match"], [3]
        )
        self.assertGreater(len(self.server.requests), 1)
        self.assertTrue(
            any(
                "refine-" in item["id"]
                for _, _, body in self.server.requests
                for item in body["inputs"]
            )
        )

    def test_discovery_git_config_types_follow_and_sort(self):
        self.write("a.py", "hit\n")
        self.write("b.go", "hit\n")
        self.write("sub/c.py", "hit\n")
        self.write("custom.ignore", "b.go\n")
        self.write(
            ".gitconfig",
            '[core]\nexcludesFile = "' + str(self.root / "custom.ignore") + '"\n',
        )
        self.assertNotIn(b"b.go", self.raw_cli("--files", "--sort", "path", ".").stdout)
        self.assertEqual(
            self.raw_cli("--files", "--sort", "path", "-tpy", ".").stdout,
            b"./a.py\n./sub/c.py\n",
        )
        (self.root / "link.py").symlink_to(self.root / "a.py")
        self.assertIn(b"link.py", self.raw_cli("--files", "-L", ".").stdout)
        self.assertNotIn(
            b"sub/c.py", self.raw_cli("--files", "--max-depth", "1", ".").stdout
        )
        self.assertIn(
            b"sub/c.py", self.raw_cli("--files", "--max-depth", "2", ".").stdout
        )

    def test_nul_paths_counts_buffering_and_json_contract(self):
        self.write("a", "hit hit\nnoise\nhit\n")
        self.assertEqual(self.raw_cli("--files", "-0", "a").stdout, b"a\0")
        self.assertEqual(self.raw_cli("-l0", "hit", "a").stdout, b"a\0")
        self.assertEqual(
            self.raw_cli("--block-buffered", "-c", "hit", "a").stdout, b"2\n"
        )
        self.assertEqual(self.raw_cli("--count-matches", "hit", "a").stdout, b"3\n")
        events = [
            json.loads(row)
            for row in self.raw_cli("--json", "hit", "a").stdout.splitlines()
        ]
        self.assertEqual(
            [row["type"] for row in events],
            ["begin", "match", "match", "end", "summary"],
        )
        self.assertEqual(events[1]["data"]["lines"], {"text": "hit hit\n"})
        self.assertEqual(events[1]["data"]["submatches"][1]["start"], 4)
        self.assertEqual(events[-1]["data"]["stats"]["matches"], 3)
        events = [
            json.loads(row)
            for row in self.raw_cli("--json", "absent", "a", code=1).stdout.splitlines()
        ]
        self.assertEqual([row["type"] for row in events], ["summary"])

    def test_color_contains_native_match_highlighting(self):
        self.write("a", "before hit after\n")
        result = self.raw_cli("--color", "always", "hit", "a")
        self.assertIn(b"\x1b[", result.stdout)
        self.assertIn(b"hit\x1b[0m", result.stdout)

    def test_malformed_and_failed_decisions_are_not_negative_searches(self):
        self.write("a", "STOP\nnoise\n")
        for mode in ["duplicate", "similarity", "out_of_range", "http_error"]:
            with self.subTest(mode=mode):
                self.server.mode = mode
                self.ask("--unit", "line", "a", code=2)


if __name__ == "__main__":
    unittest.main()
