# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Native grep against the real query server, including retained multi-page reads."""

import base64
import json
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import requests
import test_auth as auth
from helpers import start_http_server

pytestmark = pytest.mark.reuse_antfly_process
auth_api = auth.auth_api


def grep_binary():
    binary = Path(
        os.environ.get(
            "ANTFLY_GREP_BIN",
            str(Path(__file__).resolve().parents[2] / "zig-out/bin/antfly-grep"),
        )
    )
    if not binary.is_file():
        pytest.fail(
            "Build antfly-grep or set ANTFLY_GREP_BIN before running this suite"
        )
    return str(binary.resolve())


def document(path, content):
    return {
        "grep": {
            "version": 1,
            "path": path,
            "content": content,
            "uri": "s3://grep-e2e/" + path,
            "revision": "rev-original",
            "indexed_at": 1700000000,
            "permissions": {"public": True, "readers": []},
        }
    }


def run_grep(
    tmp_path, endpoint, table, *flags, token=None, page_size=1, candidate_index=None
):
    config = tmp_path / "tables.json"
    config.write_text(
        json.dumps(
            {
                "version": 1,
                "endpoint": endpoint,
                "page_size": page_size,
                "tables": [
                    {
                        "name": table,
                        **(
                            {"candidate_index": candidate_index}
                            if candidate_index
                            else {}
                        ),
                    }
                ],
            }
        )
    )
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
    }
    env.update(HOME=str(tmp_path), XDG_CONFIG_HOME=str(tmp_path))
    if token:
        env["ANTFLY_GREP_TABLE_TOKEN"] = token
    return subprocess.run(
        [
            grep_binary(),
            "--no-config",
            "--table-config",
            str(config),
            "--retries",
            "0",
            *flags,
        ],
        env=env,
        capture_output=True,
        timeout=90,
    )


def test_native_grep_pagination_large_regions_and_permissions(stateful_api, tmp_path):
    table = f"grep_pages_{time.time_ns()}"
    stateful_api.create_table(table)
    large = document("b.py", "padding\n" * 32768 + "needle LARGE\n")
    large["grep"]["regions"] = [{"start_line": 1, "end_line": 32769}] * 5000
    denied = document("secret.py", "needle SECRET\n")
    denied["grep"]["permissions"] = {"public": False, "readers": ["other"]}
    stateful_api.batch_write(
        table,
        inserts={
            "a": document("a.py", "needle FIRST\n"),
            "b": large,
            "c": denied,
            "d": document("empty.py", "no match\n"),
        },
        sync_level="full_text",
    )
    result = run_grep(tmp_path, stateful_api.url, table, "--json", "needle")
    assert result.returncode == 0, result.stderr.decode()
    events = [json.loads(line) for line in result.stdout.splitlines()]
    matches = [event for event in events if event["type"] == "match"]
    assert [event["path"] for event in matches] == ["a.py", "b.py"]
    snapshots = {event["source"]["snapshot"] for event in matches}
    assert len(snapshots) == 1 and next(iter(snapshots))
    assert events[-1]["stats"]["complete"] is True
    assert events[-1]["stats"]["index_requests"] == 5
    assert b"SECRET" not in result.stdout


def forwarded_scan(tmp_path, api, table, first_page):
    requests_seen = []
    failures = []

    class Proxy(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests_seen.append(body)
                response = requests.post(
                    api.url + f"/tables/{table}/query", json=body, timeout=60
                )
                if len(requests_seen) == 1:
                    assert response.status_code == 200, response.text
                    first_page()
                self.send_response(response.status_code)
                self.send_header("Content-Length", str(len(response.content)))
                self.end_headers()
                self.wfile.write(response.content)
            except Exception as error:
                failures.append(str(error))
                self.send_error(500)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    thread = start_http_server(server)
    try:
        result = run_grep(
            tmp_path, f"http://127.0.0.1:{server.server_port}", table, "needle"
        )
        assert not failures, failures
        return result, requests_seen
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_native_grep_snapshot_survives_concurrent_mutation(stateful_api, tmp_path):
    table = f"grep_snapshot_{time.time_ns()}"
    stateful_api.create_table(table)
    stateful_api.batch_write(
        table,
        inserts={
            "a": document("a.py", "needle ORIGINAL_A\n"),
            "b": document("b.py", "needle ORIGINAL_B\n"),
        },
        sync_level="full_text",
    )

    def update():
        stateful_api.batch_write(
            table,
            inserts={
                "b": document("b.py", "needle CHANGED\n"),
                "c": document("c.py", "needle ADDED\n"),
            },
            sync_level="full_text",
        )

    result, requests_seen = forwarded_scan(tmp_path, stateful_api, table, update)
    assert result.returncode == 0, result.stderr.decode()
    assert b"ORIGINAL_A" in result.stdout and b"ORIGINAL_B" in result.stdout
    assert b"CHANGED" not in result.stdout and b"ADDED" not in result.stdout
    assert requests_seen[0]["order_by"] == [{"field": "_id", "desc": False}]
    assert "remote_snapshot" not in requests_seen[0]
    assert requests_seen[1]["search_after"] == ["a"]
    assert requests_seen[1]["remote_snapshot"]


@pytest.fixture
def short_cursor_api(monkeypatch, request):
    import conftest

    original = conftest._write_remote_content_e2e_config

    def configure(*args, **kwargs):
        path = original(*args, **kwargs)
        value = json.loads(path.read_text())
        value["lake_indexes"] = {"query_cursors": {"retention_ms": 1000}}
        path.write_text(json.dumps(value))
        return path

    monkeypatch.setattr(conftest, "_write_remote_content_e2e_config", configure)
    return request.getfixturevalue("stateful_api")


@pytest.mark.fresh_antfly_process
def test_native_grep_expired_snapshot_keeps_partial_results(short_cursor_api, tmp_path):
    if os.environ.get("ANTFLY_STATEFUL_URL"):
        pytest.skip("expiry needs a locally configured one-second cursor retention")
    api = short_cursor_api
    table = f"grep_expiry_{time.time_ns()}"
    api.create_table(table)
    api.batch_write(
        table,
        inserts={
            "a": document("a.py", "needle FIRST\n"),
            "b": document("b.py", "needle SECOND\n"),
        },
        sync_level="full_text",
    )
    result, seen = forwarded_scan(tmp_path, api, table, lambda: time.sleep(1.2))
    assert result.returncode == 2, result.stderr.decode()
    assert b"IndexSnapshotExpired" in result.stderr
    assert b"FIRST" in result.stdout and b"SECOND" not in result.stdout
    assert len(seen) == 2 and seen[1]["search_after"] == ["a"]


def test_native_grep_server_authorization(auth_api, tmp_path):
    auth_api.s.headers["Authorization"] = auth._basic_auth(
        "admin", auth.AUTH_BOOTSTRAP_PASSWORD
    )
    table = f"grep_auth_{time.time_ns()}"
    auth_api.create_table(table)
    auth_api.batch_write(
        table,
        {
            "inserts": {"a": document("public.py", "needle AUTHORIZED\n")},
            "sync_level": "full_text",
        },
    )
    username = f"grep_reader_{time.time_ns()}"
    auth_api.post(
        "/auth/v1/users/" + username,
        {
            "password": "local-test-only",
            "initial_policies": [
                {"resource": table, "resource_type": "table", "type": "read"}
            ],
        },
    )
    key = auth_api.post(f"/auth/v1/users/{username}/api-keys", {"name": "grep-test"})
    result = run_grep(tmp_path, auth_api.url, table, "needle", token=key["encoded"])
    assert result.returncode == 0, result.stderr.decode()
    assert b"AUTHORIZED" in result.stdout
    for token in (None, base64.b64encode(b"nonexistent-key:invalid-secret").decode()):
        denied = run_grep(tmp_path, auth_api.url, table, "needle", token=token)
        assert denied.returncode == 2
        assert b"IndexAuthorizationFailed" in denied.stderr
        assert b"AUTHORIZED" not in denied.stdout
    auth_api.delete(f"/auth/v1/users/{username}/api-keys/{key['key_id']}")
    revoked = run_grep(tmp_path, auth_api.url, table, "needle", token=key["encoded"])
    assert revoked.returncode == 2 and b"AUTHORIZED" not in revoked.stdout


def test_native_grep_certified_candidates_and_legacy_fallback(stateful_api, tmp_path):
    table = f"grep_candidates_{time.time_ns()}"
    stateful_api.post(
        f"/tables/{table}",
        {
            "num_shards": 1,
            "schema": {
                "default_type": "doc",
                "document_schemas": {
                    "doc": {
                        "schema": {
                            "type": "object",
                            "additionalProperties": True,
                            "properties": {
                                "grep_trigrams": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "x-antfly-types": ["keyword"],
                                },
                                "grep_trigrams_version": {
                                    "type": "string",
                                    "x-antfly-types": ["keyword"],
                                },
                            },
                        }
                    }
                },
            },
        },
    )
    rows = {}
    for key, content in [
        ("a", "TARGET first\n"),
        ("b", "irrelevant\n"),
        ("c", "TARGET legacy\n"),
    ]:
        row = document(key + ".py", content)
        if key != "c":
            raw = content.encode()
            row.update(
                grep_trigrams=sorted(
                    {raw[i : i + 3].hex() for i in range(len(raw) - 2)}
                ),
                grep_trigrams_version="raw-byte-trigrams-v1",
            )
        rows[key] = row
    stateful_api.batch_write(table, inserts=rows, sync_level="full_text")
    baseline = run_grep(tmp_path, stateful_api.url, table, "-Fl", "TARGET")
    accelerated = run_grep(
        tmp_path,
        stateful_api.url,
        table,
        "-Fl",
        "TARGET",
        candidate_index={"version": 1, "field": "grep_trigrams"},
    )
    assert baseline.returncode == accelerated.returncode == 0, (
        accelerated.stderr.decode()
    )
    assert baseline.stdout == accelerated.stdout == b"a.py\nc.py\n"

    for flags in (["-l", "TARGET.*first"], ["-Fl", "-e", "TARGET", "-e", "irrelevant"]):
        ordinary = run_grep(tmp_path, stateful_api.url, table, *flags)
        candidates = run_grep(
            tmp_path,
            stateful_api.url,
            table,
            *flags,
            candidate_index={"version": 1, "field": "grep_trigrams"},
        )
        assert ordinary.returncode == candidates.returncode == 0
        assert ordinary.stdout == candidates.stdout


def test_native_grep_narrow_listing_and_content_projection(stateful_api, tmp_path):
    table = f"grep_projection_{time.time_ns()}"
    stateful_api.create_table(table)
    item = document("large.py", "needle FIRST\n" + "padding\n" * 32768)
    item["grep"]["metadata"] = {"unused": "x" * 262144}
    denied = document("secret.py", "needle SECRET\n")
    denied["grep"]["permissions"] = {"public": False, "readers": ["other"]}
    stateful_api.batch_write(
        table, inserts={"a": item, "b": denied}, sync_level="full_text"
    )
    config = tmp_path / "projection.json"
    config.write_text(
        json.dumps(
            {
                "version": 1,
                "endpoint": stateful_api.url,
                "page_size": 1,
                "max_response_bytes": 4096,
                "tables": [{"name": table}],
            }
        )
    )
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
    }
    env.update(HOME=str(tmp_path), XDG_CONFIG_HOME=str(tmp_path))
    listing = subprocess.run(
        [grep_binary(), "--no-config", "--table-config", str(config), "--files"],
        env=env,
        capture_output=True,
        timeout=90,
    )
    assert listing.returncode == 0, listing.stderr.decode()
    assert listing.stdout == b"large.py\n"
    # A finite source ceiling requires content and retains the regular behavior.
    ceiling = subprocess.run(
        [
            grep_binary(),
            "--no-config",
            "--table-config",
            str(config),
            "--files",
            "--max-filesize=1k",
        ],
        env=env,
        capture_output=True,
        timeout=90,
    )
    assert ceiling.returncode == 2
    lexical = run_grep(tmp_path, stateful_api.url, table, "needle")
    assert lexical.returncode == 0, lexical.stderr.decode()
    assert b"FIRST" in lexical.stdout and b"SECRET" not in lexical.stdout


def test_native_grep_json_projects_versioned_contract(stateful_api, tmp_path):
    table = f"grep_json_projection_{time.time_ns()}"
    stateful_api.create_table(table)
    item = document("source.py", "needle FIRST\n")
    item["grep"].update(
        metadata={"owner": "雪"},
        regions=[{"start_line": 1, "end_line": 1}],
        unrelated_blob="x" * 262144,
    )
    stateful_api.batch_write(table, inserts={"a": item}, sync_level="full_text")
    config = tmp_path / "json-projection.json"
    config.write_text(
        json.dumps(
            {
                "version": 1,
                "endpoint": stateful_api.url,
                "page_size": 1,
                "max_response_bytes": 8192,
                "tables": [{"name": table}],
            }
        )
    )
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
    }
    env.update(HOME=str(tmp_path), XDG_CONFIG_HOME=str(tmp_path))
    result = subprocess.run(
        [
            grep_binary(),
            "--no-config",
            "--table-config",
            str(config),
            "--json",
            "needle",
        ],
        env=env,
        capture_output=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stderr.decode()
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    match = next(row for row in rows if row["type"] == "match")
    assert match["source"]["metadata"] == item["grep"]["metadata"]
    assert match["source"]["source_regions"] == item["grep"]["regions"]
    assert rows[-1]["stats"]["complete"]
