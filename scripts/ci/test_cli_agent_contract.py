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

"""Process contract checks and a paired CLI benchmark; no model claims.

Build: cd zig && zig build antfly-cli-contract-bin
Run: python3 scripts/ci/test_cli_agent_contract.py --binary zig/zig-out/bin/antfly-cli-contract
The fixture runs the product remote CLI; local backup staging has its own suite.
"""

import argparse
import contextlib
import http.server
import json
import os
import subprocess
import struct
import tempfile
import zlib
import threading
import time
from pathlib import Path


@contextlib.contextmanager
def server(body, status=200, delay=0):
    calls = []

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            length = int(self.headers.get("Content-Length", "0"))
            request = self.rfile.read(length)
            calls.append((self.path, dict(self.headers), request))
            if delay:
                time.sleep(delay)
            encoded = json.dumps(body, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Connection", "close")
            if status == 307:
                self.send_header("Location", "/redirected")
            self.end_headers()
            self.wfile.write(encoded)

        do_POST = do_GET
        do_DELETE = do_GET
        do_PUT = do_GET

        def log_message(self, *args):
            pass

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}", calls
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


def environment(url=None, **updates):
    env = os.environ.copy()
    for key in (
        "ANTFLY_AGENT_MODE",
        "CODEX_THREAD_ID",
        "CLAUDECODE",
        "CURSOR_AGENT",
        "CLINE_ACTIVE",
        "ANTFLY_TOKEN",
        "ANTFLY_USERNAME",
        "ANTFLY_PASSWORD",
        "ANTFLY_DATABASE",
        "ANTFLY_NAMESPACE",
    ):
        env.pop(key, None)
    env["ANTFLY_URL"] = url or "http://127.0.0.1:1"
    env.update(updates)
    return env


def invoke(binary, arguments, env):
    started = time.perf_counter()
    result = subprocess.run(
        [binary, *arguments], env=env, capture_output=True, timeout=20, check=False
    )
    return result, round((time.perf_counter() - started) * 1000, 3)


def lines(raw):
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def require(condition, description):
    if not condition:
        raise AssertionError(description)


def sql(rows, **control):
    return dict(
        columns=[{"name": "value", "type": "integer"}],
        rows=rows,
        rows_affected=0,
        command_tag="SELECT",
        **control,
    )


def search(partial=False):
    responses = [
        {
            "status": 200,
            "took": 2,
            "hits": {
                "total": {"value": 100, "relation": "gte"},
                "hits": [{"_id": "doc:1", "_score": 1, "_source": {"title": "one"}}],
            },
        }
    ]
    if partial:
        responses.append({"status": 503, "took": 2, "error": "shard unavailable"})
    return {"responses": responses}


def benchmark_scenarios():
    metadata = [
        {
            "name": "docs",
            "shards": {},
            "indexes": {},
            "storage_status": {},
            "schema": {
                "document_schemas": {
                    "doc": {
                        "schema": {
                            "properties": {
                                "id": {"type": "integer"},
                                "title": {"type": "string"},
                            }
                        }
                    }
                }
            },
        }
    ]
    scenarios = [
        ("schema", ["schema"], metadata, 200),
        (
            "aggregation",
            ["sql", "--statement", "SELECT count(*) FROM docs"],
            sql([["9007199254740993"]]),
            200,
        ),
        (
            "hybrid_search",
            [
                "query",
                "--table",
                "docs",
                "--full-text-search",
                "one",
                "--semantic-search",
                "one",
            ],
            search(),
            200,
        ),
        (
            "long_document",
            ["lookup", "--table", "docs", "--key", "doc:1"],
            {"text": "🌲" * 20000},
            200,
        ),
        (
            "invalid_query",
            ["sql", "--statement", "SELECT bad("],
            {"code": "42601", "message": "do not echo TOKENSECRET", "position": 11},
            400,
        ),
        (
            "oversized_result",
            ["sql", "--statement", "SELECT value FROM docs", "--limit", "128"],
            sql([[str(i)] for i in range(100)]),
            200,
        ),
        (
            "partial_shards",
            ["query", "--table", "docs", "--full-text-search", "one"],
            search(True),
            200,
        ),
        (
            "uncertain_write",
            ["sql", "--statement", "COMMIT"],
            {
                "code": "40003",
                "message": "TOKENSECRET",
                "transaction_id": "0" * 32,
                "retryable": False,
                "transaction_status": "failed",
            },
            503,
        ),
    ]
    return scenarios


def check_review_regressions(binary):
    # Failures after submission must not acquire certainty from an error name
    # or from a legacy fatal() call. Every direct catalog route uses HTTP facts.
    receipt = "0123456789abcdef0123456789abcdef"
    diagnostic = {
        "error": {
            "code": "40003",
            "transaction_id": receipt,
            "mutation_outcome": "unconfirmed",
            "retryable": False,
            "message": "server prose must stay out of diagnostics",
        }
    }
    catalog_commands = [
        ["database", "create", "demo"],
        ["database", "drop", "demo"],
        ["database", "rename", "demo", "renamed"],
        ["namespace", "create", "--database", "demo", "--namespace", "app"],
        ["namespace", "drop", "--database", "demo", "--namespace", "app"],
        ["namespace", "rename", "demo", "app", "renamed"],
        ["tablespace", "create", "space"],
        ["tablespace", "drop", "space"],
        ["tablespace", "rename", "space", "renamed"],
    ]
    for arguments in catalog_commands:
        with server(diagnostic, status=500) as (url, calls):
            result, _ = invoke(binary, ["--agent", *arguments], environment(url))
        require(result.returncode != 0 and not result.stdout, str(arguments))
        require(len(calls) == 1, "catalog mutation replayed")
        errors = lines(result.stderr)
        require(len(errors) == 1, str(errors))
        failure = errors[0]["error"]
        require(
            failure["category"] == "http" and failure["http_status"] == 500,
            str(failure),
        )
        require(failure["execution"] == "unconfirmed", str(failure))
        require(failure["diagnostic"]["transaction_id"] == receipt, str(failure))
        require(failure["retryable"] is False, str(failure))
        require(b"server prose" not in result.stderr, "unsafe diagnostic prose")

    # Input validation remains explicitly rejected; error-name prefixes on
    # malformed successful responses are not evidence of non-execution.
    for args in (
        ["database", "create"],
        ["--output", "bad", "capabilities"],
        ["sql", "--statement"],
    ):
        with server({}) as (url, calls):
            result, _ = invoke(binary, ["--agent", *args], environment(url))
        failure = lines(result.stderr)[-1]["error"]
        require(result.returncode != 0 and not calls, str(args))
        require(
            failure["category"] == "argument" and failure["execution"] == "rejected",
            str(failure),
        )
    with server({"unexpected": True}) as (url, calls):
        result, _ = invoke(
            binary, ["--agent", "sql", "--statement", "COMMIT"], environment(url)
        )
    require(result.returncode != 0 and len(calls) == 1, "malformed mutation response")
    require(
        lines(result.stderr)[-1]["error"]["execution"] == "unconfirmed",
        str(result.stderr),
    )
    with server({"backup_id": "backup:1", "status": "bogus", "tables": []}) as (
        url,
        calls,
    ):
        result, _ = invoke(
            binary,
            [
                "--agent",
                "backup",
                "--connection",
                "fixture",
                "--location",
                "s3://bucket",
                "--backup-id",
                "backup:1",
            ],
            environment(url),
        )
    failure = lines(result.stderr)[-1]["error"]
    require(result.returncode != 0 and len(calls) == 1, "post-execution failure")
    require(
        failure["category"] == "command" and failure["execution"] == "unconfirmed",
        str(failure),
    )

    backup_args = [
        "backup",
        "--list",
        "--connection",
        "fixture",
        "--location",
        "s3://bucket",
    ]
    listing = {"backups": [], "next_cursor": "cursor:1"}
    for mode in ("--agent", "--no-agent"):
        for local_flag in ("--output", "-o"):
            with server(listing) as (url, calls):
                result, _ = invoke(
                    binary, [mode, *backup_args, local_flag, "json"], environment(url)
                )
            require(result.returncode == 0, str(result.stderr))
            require(json.loads(result.stdout) == listing, "local JSON output ignored")
            require(len(calls) == 1, "backup list replayed")
    for local in (
        backup_args + ["--output", "json"],
        ["table", "list", "--output", "json"],
        ["index", "list", "--table", "docs", "--verbose"],
    ):
        with server({}) as (url, calls):
            result, _ = invoke(
                binary,
                ["--agent", "--fingerprint", "ordered", *local],
                environment(url),
            )
        require(
            result.returncode != 0 and not calls,
            "local format bypassed policy validation",
        )
        require(
            lines(result.stderr)[-1]["error"]["execution"] == "rejected",
            str(result.stderr),
        )

    # Synthetic, deterministic storage identity. No private production state or
    # external crypto executable is needed. The product verifies the Ed25519
    # pair while loading the checkpoint, then signs and verifies the proof.
    with tempfile.TemporaryDirectory(prefix="antfly-cli-proof-") as directory:
        root = Path(directory)
        seed = bytes(range(32))
        public = bytes.fromhex(
            "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8"
        )
        for name, magic, suffix in [
            ("root_identity.checkpoint", b"AFROOTI1", b""),
            ("root_signing_identity.checkpoint", b"AFROOTS1", seed + public),
        ]:
            data = magic + struct.pack("<I", 1) + (17).to_bytes(16, "little") + suffix
            path = root / name
            path.write_bytes(data + struct.pack("<I", zlib.crc32(data)))
            path.chmod(0o600)
        arguments = [
            "internal",
            "store-root",
            "proof",
            "--replica-root-dir",
            str(root),
            "--metadata-incarnation",
            "1" * 32,
            "--node-id",
            "1",
            "--store-id",
            "1",
        ]
        original = None
        for flags in (
            ["--no-agent"],
            ["--agent"],
            ["--agent", "--output", "table"],
            ["--agent", "--output", "compact"],
            ["--agent", "--display-cell-bytes", "1", "--display-bytes", "1024"],
        ):
            result, _ = invoke(binary, [*flags, *arguments], environment())
            require(result.returncode == 0, str(result.stderr))
            document = json.loads(result.stdout)
            require(
                set(document) == {"identity", "signature"}, "proof artifact reformatted"
            )
            original = original or result.stdout
            require(
                result.stdout == original, "presentation flags changed artifact bytes"
            )
            proof = root / "proof.json"
            proof.write_bytes(result.stdout)
            # Real enroll parsing/signature verification must reach the HTTP
            # endpoint once; an HTTP failure proves no live enrollment occurs.
            with server(diagnostic, status=500) as (url, calls):
                enrolled, _ = invoke(
                    binary,
                    [
                        "--agent",
                        "internal",
                        "store-root",
                        "enroll",
                        "--file",
                        str(proof),
                    ],
                    environment(url),
                )
            require(enrolled.returncode != 0 and len(calls) == 1, str(enrolled.stderr))
            require(
                lines(enrolled.stderr)[-1]["error"]["category"] == "http",
                str(enrolled.stderr),
            )


def run(binary):
    report = []
    # Token counts are measured only when a tokenizer is installed. Never use a
    # byte/4 heuristic as if it measured an agent's end-to-end token use.
    try:
        import tiktoken

        encoding = tiktoken.get_encoding("o200k_base")
    except ImportError:
        encoding = None

    scenarios = benchmark_scenarios()
    for name, arguments, body, status in scenarios:
        for agent in (False, True):
            with server(body, status) as (url, calls):
                result, elapsed = invoke(
                    binary,
                    (["--agent"] if agent else ["--no-agent"]) + arguments,
                    environment(url),
                )
                require(
                    len(calls) == 1, f"{name}: unexpected replay or preflight: {calls}"
                )
            expected_failure = status >= 300 or name == "partial_shards"
            require(
                (result.returncode != 0) == expected_failure,
                f"{name}: status {result.returncode}: {result.stderr!r}",
            )
            parsed = json.loads(result.stdout) if agent and result.stdout else None
            if agent:
                diagnostics = lines(result.stderr)
                if expected_failure:
                    require(
                        any("error" in value for value in diagnostics),
                        f"{name}: missing structured error",
                    )
                    require(
                        b"TOKENSECRET" not in result.stderr,
                        "diagnostic exposed server prose",
                    )
                if status < 300:
                    require(parsed["agent_contract"] == 1, name)
                    require(
                        len(result.stdout) <= 16384,
                        f"{name}: byte cap includes envelope and newline",
                    )
                    require(
                        parsed["execution"]["status"]
                        == ("partial" if name == "partial_shards" else "complete"),
                        name,
                    )
                if name == "aggregation":
                    require(
                        parsed["data"]["rows"] == [["9007199254740993"]],
                        "integer precision changed",
                    )
                if name == "hybrid_search":
                    require(
                        parsed["execution"]["result_scope"] == "requested_top_k", name
                    )
                    require(
                        parsed["data"]["responses"][0]["hits"]["total"]["relation"]
                        == "gte",
                        name,
                    )
                if name in ("long_document", "oversized_result"):
                    require(parsed["display"]["omitted"], name)
                if name == "oversized_result":
                    require(parsed["display"]["returned_rows"] == 100, name)
                    require(parsed["display"]["displayed_rows"] == 20, name)
                    require(
                        json.loads(calls[0][2])["limit"] == 128,
                        "display cap changed SQL admission",
                    )
                if name == "schema":
                    require(parsed["data"][0]["database"] == "default", name)
                    require(
                        [column["name"] for column in parsed["data"][0]["columns"]]
                        == ["id", "title"],
                        name,
                    )
                if name == "uncertain_write":
                    diagnostic = diagnostics[-1]["error"]["diagnostic"]
                    require(
                        diagnostic["transaction_id"] == "0" * 32
                        and diagnostic["code"] == "40003",
                        name,
                    )
                    require(diagnostic["retryable"] is False, name)
            if not agent and status < 300:
                require(
                    b"+" in result.stdout and b"|" in result.stdout,
                    f"{name}: missing human table",
                )
                require(
                    not result.stdout.lstrip().startswith((b"{", b"[")),
                    f"{name}: raw JSON",
                )
                if name == "aggregation":
                    require(
                        b"9007199254740993" in result.stdout, "human integer precision"
                    )
                if name in ("long_document", "oversized_result"):
                    require(
                        b"Display shortened" in result.stdout,
                        f"{name}: hidden omission",
                    )
            output = result.stdout + result.stderr
            report.append(
                {
                    "scenario": name,
                    "mode": "agent" if agent else "ordinary",
                    "correct": True,
                    "incomplete_recognized": (
                        parsed["display"]["omitted"]
                        or parsed["execution"]["status"] == "partial"
                    )
                    if agent and parsed
                    else None,
                    "utf8_bytes": len(output),
                    "tool_output_tokens": len(encoding.encode(output.decode()))
                    if encoding
                    else None,
                    "requests": len(calls),
                    "cli_invocations": 1,
                    "latency_ms": elapsed,
                    "agent_turns": None,
                    "end_to_end_tokens": None,
                    "cost": None,
                }
            )

    for options, env, enveloped in [
        (["capabilities"], environment(ANTFLY_AGENT_MODE="1"), True),
        (["--no-agent", "capabilities"], environment(ANTFLY_AGENT_MODE="1"), False),
        (["--agent", "capabilities"], environment(ANTFLY_AGENT_MODE="0"), True),
        (["capabilities"], environment(CODEX_THREAD_ID="test"), True),
        (
            ["capabilities"],
            environment(ANTFLY_AGENT_MODE="0", CODEX_THREAD_ID="test"),
            False,
        ),
        (["--agent", "--output", "json", "capabilities"], environment(), False),
        (["--agent", "--output", "compact", "capabilities"], environment(), False),
    ]:
        result, _ = invoke(binary, options, env)
        require(result.returncode == 0, str(result.stderr))
        if enveloped or "--output" in options:
            value = json.loads(result.stdout)
            require(
                ("execution" in value and "control" in value) == enveloped, str(options)
            )
        else:
            require(
                b"|" in result.stdout and not result.stdout.lstrip().startswith(b"{"),
                str(options),
            )

    for options in [
        ["--output", "invalid", "--agent", "capabilities"],
        ["--fingerprint", "invalid", "--agent", "capabilities"],
        ["--agent", "--output", "json", "--fingerprint", "ordered", "capabilities"],
        ["--agent", "unknown"],
        ["--agent", "--display-rows"],
        ["--agent", "sql", "--statement"],
        ["--agent", "--display-bytes", "100", "capabilities"],
        ["--agent", "--max-scan-rows", "1", "lookup"],
        ["--agent", "--display-rows", "1", "--display-rows", "2", "capabilities"],
        ["--agent", "lookup", "--table", "docs", "--key"],
    ]:
        result, _ = invoke(binary, options, environment())
        require(result.returncode != 0 and not result.stdout, str(options))
        require(
            len(lines(result.stderr)) == 1 and "error" in lines(result.stderr)[0],
            str(result.stderr),
        )

    with server(
        {
            "columns": [{"name": "QUERY PLAN", "type": "string"}],
            "rows": [[json.dumps({"plan_version": 1, "Plan": {"node_type": "Scan"}})]],
            "command_tag": "EXPLAIN",
            "rows_affected": 0,
        }
    ) as (url, calls):
        result, _ = invoke(
            binary,
            ["--agent", "sql", "--explain", "--statement", "SELECT * FROM docs"],
            environment(url),
        )
        require(result.returncode == 0, str(result.stderr))
        value = json.loads(result.stdout)
        require(
            value["execution"]["scope"] == "plan" and value["data"]["planning_only"],
            str(value),
        )
        require(value["data"]["plan"]["Plan"]["node_type"] == "Scan", str(value))
        require(
            json.loads(calls[0][2])["statement"]
            == "EXPLAIN (FORMAT JSON) SELECT * FROM docs",
            str(calls),
        )

    with server({"status": "unexpected"}) as (url, calls):
        result, _ = invoke(
            binary,
            [
                "--agent",
                "--output",
                "invalid-format",
                "table",
                "create",
                "--table",
                "docs",
            ],
            environment(url),
        )
        require(
            result.returncode != 0 and not calls,
            "unsupported formatting must fail before a mutation",
        )

    for flag, option, planning_only in [
        ("--analyze", ", ANALYZE ON", False),
        ("--estimate", ", COSTS ON", True),
    ]:
        plan = {
            "plan_version": 1,
            "Plan": {"node_type": "Scan"},
            "planning_only": planning_only,
        }
        body = {
            "columns": [{"name": "QUERY PLAN", "type": "string"}],
            "rows": [[json.dumps(plan)]],
            "rows_affected": 0,
            "command_tag": "EXPLAIN",
        }
        with server(body) as (url, calls):
            result, _ = invoke(
                binary,
                ["--agent", "sql", flag, "--statement", "SELECT 1"],
                environment(url),
            )
        require(result.returncode == 0, str(result.stderr))
        value = json.loads(result.stdout)
        require(
            value["data"]["planning_only"] == planning_only
            and value["execution"]["planning_only"] == planning_only,
            "plan execution scope",
        )
        require(
            json.loads(calls[0][2])["statement"]
            == f"EXPLAIN (FORMAT JSON{option}) SELECT 1",
            "plan request flags",
        )
        require(len(calls) == 1, "plan request replayed")

    for mode in ("--agent", "--no-agent"):
        for ordering in ("ordered", "multiset"):
            with server(sql([["1"], ["2"]])) as (url, calls):
                result, _ = invoke(
                    binary,
                    [
                        mode,
                        "--fingerprint",
                        ordering,
                        "--display-rows",
                        "1",
                        "--display-bytes",
                        "1024",
                        "sql",
                        "--statement",
                        "SELECT 1",
                    ],
                    environment(url),
                )
            require(result.returncode == 0 and len(calls) == 1, str(result.stderr))
            if mode == "--agent":
                value = json.loads(result.stdout)
                record = value["fingerprint"]
                require(
                    record["scope"] == "complete_sql_result" and record["units"] == 2,
                    "fingerprint covers the complete received result",
                )
                require(
                    len(record["digest"]) == 64 and len(result.stdout) <= 1024,
                    "bounded fingerprint envelope",
                )
            else:
                require(
                    b"Fingerprint (complete_sql_result" in result.stdout,
                    "human fingerprint",
                )
    with server(search(True)) as (url, calls):
        result, _ = invoke(
            binary,
            [
                "--agent",
                "--fingerprint",
                "multiset",
                "query",
                "--table",
                "docs",
                "--full-text-search",
                "one",
            ],
            environment(url),
        )
    value = json.loads(result.stdout)
    require(
        result.returncode != 0 and value["fingerprint"]["digest"] is None,
        "partial execution claimed a complete fingerprint",
    )

    for output_format in ("table", "json", "compact"):
        body = sql([["9007199254740993"]])
        with server(body) as (url, calls):
            result, _ = invoke(
                binary,
                [
                    "--no-agent",
                    "--output",
                    output_format,
                    "sql",
                    "--statement",
                    "SELECT 1",
                ],
                environment(url),
            )
        require(result.returncode == 0, str(result.stderr))
        if output_format == "table":
            require(
                b"|" in result.stdout and b"9007199254740993" in result.stdout,
                "explicit human table",
            )
        else:
            require(
                json.loads(result.stdout) == body,
                "explicit JSON changed original response",
            )

    retrieval = {
        "status": "completed",
        "generation": "An answer for humans.",
        "hits": [
            {
                "_id": "doc:1",
                "_score": 0.5,
                "_source": {
                    "title": "One",
                    "body": "context should not swamp the answer",
                },
            }
        ],
    }
    with server(retrieval) as (url, calls):
        result, _ = invoke(
            binary,
            [
                "--no-agent",
                "agents",
                "retrieval",
                "--table",
                "docs",
                "--full-text-search",
                "one",
                "--generator",
                '{"provider":"antfly","model":"fixture"}',
                "--generate",
            ],
            environment(url),
        )
    require(result.returncode == 0, str(result.stderr))
    require(
        b"An answer for humans." in result.stdout
        and b"Sources" in result.stdout
        and b"One" in result.stdout,
        "human retrieval projection",
    )
    require(
        b"data:" not in result.stdout
        and b"context should not swamp" not in result.stdout,
        "raw retrieval transport leaked",
    )
    require(
        json.loads(calls[-1][2])["stream"] is False,
        "human retrieval requested streaming by default",
    )

    # Workflow payloads share presentation budgets without discarding lifecycle
    # controls. Explicit JSON remains the unmodified transport response.
    generator = '{"provider":"antfly","model":"fixture"}'
    workflow_cases = [
        (
            {
                **retrieval,
                "generation": "x" * 50000,
                "hits": [{"_id": "doc:1", "_source": {"body": "y" * 50000}}],
            },
            [
                "agents",
                "retrieval",
                "--table",
                "docs",
                "--full-text-search",
                "one",
                "--generator",
                generator,
                "--generate",
            ],
            {"status": "completed"},
        ),
        (
            {"status": "completed", "report": {"body": "x" * 50000}},
            [
                "agents",
                "research",
                "--table",
                "docs",
                "--query",
                "one",
                "--generator",
                generator,
            ],
            {"status": "completed"},
        ),
        (
            {
                "status": "completed",
                "session_id": "session:1",
                "query": {},
                "explanation": "x" * 50000,
            },
            ["agents", "query-builder", "--intent", "one", "--generator", generator],
            {"status": "completed", "session_id": "session:1"},
        ),
        (
            {
                "job_id": "job:1",
                "state": "succeeded",
                "phase": "done",
                "query": "x" * 50000,
            },
            ["agents", "research", "--resume-job", "job:1"],
            {"job_id": "job:1", "state": "succeeded", "phase": "done"},
        ),
    ]
    for payload, arguments, controls in workflow_cases:
        for raw in (False, True):
            flags = ["--agent", "--display-bytes", "1024"]
            if raw:
                flags += ["--output", "json"]
            with server(payload) as (url, calls):
                result, _ = invoke(binary, flags + arguments, environment(url))
            require(result.returncode == 0, str(result.stderr))
            require(len(calls) == 1, "workflow was replayed")
            value = json.loads(result.stdout)
            if raw:
                require(value == payload, "explicit workflow JSON changed")
                continue
            require(len(result.stdout) <= 1024, "workflow exceeded byte budget")
            require(value["execution"]["scope"] == "workflow", str(value))
            require(value["execution"]["status"] == "complete", str(value))
            require(value["display"]["omitted"], str(value))
            require(not value["display"]["budget_exempt"], str(value))
            for key, expected in controls.items():
                require(value["control"][key] == expected, str(value))

    with server(sql([["1"]])) as (url, calls):
        result, _ = invoke(
            binary,
            [
                "--agent",
                "--deadline-ms",
                "250",
                "--max-scan-rows",
                "12",
                "--memory-bytes",
                "1048576",
                "--spill-bytes",
                "4096",
                "sql",
                "--statement",
                "SELECT 1",
            ],
            environment(url),
        )
        require(result.returncode == 0, str(result.stderr))
        headers = {key.lower(): value for key, value in calls[0][1].items()}
        require(
            headers["x-antfly-timeout-ms"] == "250"
            and headers["x-antfly-sql-scan-rows"] == "12",
            str(headers),
        )

    with server(sql([["1"]]), delay=1.1) as (url, calls):
        result, _ = invoke(
            binary,
            ["--agent", "--progress", "sql", "--statement", "SELECT 1"],
            environment(url),
        )
        require(result.returncode == 0, str(result.stderr))
        events = lines(result.stderr)
        require(
            events[0]["event"] == "command_started"
            and events[-1]["event"] == "command_finished",
            str(events),
        )
        require(
            any(
                value["event"] == "command_running" and value["elapsed_ms"] >= 1000
                for value in events
            ),
            str(events),
        )
        json.loads(result.stdout)

    with server({"status": "committed_pending", "inserted": 1}) as (url, calls):
        result, _ = invoke(
            binary,
            [
                "--agent",
                "--display-cell-bytes",
                "1",
                "insert",
                "--table",
                "docs",
                "--key",
                "doc:1",
                "--document",
                "{}",
            ],
            environment(url),
        )
        require(result.returncode == 0, str(result.stderr))
        result_value = json.loads(result.stdout)
        require(
            result_value["data"]["status"] == "committed_pending"
            and result_value["display"]["budget_exempt"],
            str(result_value),
        )
        require(len(calls) == 1, "write was replayed")

    with server({"error": "redirect"}, status=307) as (url, calls):
        result, _ = invoke(
            binary,
            [
                "--agent",
                "insert",
                "--table",
                "docs",
                "--key",
                "doc:1",
                "--document",
                "{}",
            ],
            environment(url),
        )
        require(
            result.returncode != 0 and len(calls) == 1,
            "mutation followed a redirect or was replayed",
        )
        require(
            not result.stdout and "error" in lines(result.stderr)[-1],
            str(result.stderr),
        )

    # A successful write followed by a broken stdout pipe still has a known
    # execution outcome. Keep reconciliation fields, including opaque receipts.
    commit = {
        "columns": [],
        "rows": [],
        "rows_affected": 0,
        "command_tag": "COMMIT",
        "mutation_outcome": "committed_pending",
        "transaction_id": "0123456789abcdef0123456789abcdef",
    }
    opaque = {"status": "committed_pending", "inserted": 1}
    for payload, arguments, operation in [
        (commit, ["sql", "--statement", "COMMIT"], False),
        (
            opaque,
            ["insert", "--table", "docs", "--key", "doc:1", "--document", "{}"],
            True,
        ),
    ]:
        for output_format in ("auto", "json", "table"):
            with server(payload) as (url, calls):
                child = subprocess.Popen(
                    [binary, "--agent", "--output", output_format, *arguments],
                    env=environment(url),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                child.stdout.close()
                error_output = child.stderr.read()
                require(child.wait(timeout=10) != 0, "closed stdout reported success")
            require(len(calls) == 1, "delivery failure replayed mutation")
            error = lines(error_output)[-1]["error"]
            require(error["category"] == "delivery", str(error))
            require(error["execution"] == "complete", str(error))
            require(error["retryable"] is False, str(error))
            if operation:
                require(error["diagnostic"]["outcome"] == payload, str(error))
            else:
                controls = error["diagnostic"]["control"]
                for key in ("transaction_id", "mutation_outcome", "command_tag"):
                    require(controls[key] == payload[key], str(error))

    check_review_regressions(binary)

    child = subprocess.Popen(
        [binary, "--agent", "capabilities"],
        env=environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    child.stdout.close()
    error_output = child.stderr.read()
    require(
        child.wait(timeout=10) != 0, f"closed stdout reported success: {error_output!r}"
    )
    return {
        "tokenizer": "o200k_base" if encoding else None,
        "results": report,
        "limits": "This paired CLI fixture benchmark measures delivery correctness, bytes, requests and latency. It does not run a coding agent or measure end-to-end tokens, turns or cost.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = run(str(Path(args.binary).resolve()))
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(
        f"CLI agent process contract passed; {len(report['results'])} paired benchmark runs"
    )


if __name__ == "__main__":
    main()
