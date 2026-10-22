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
    tmp_path,
    endpoint,
    table,
    *flags,
    token=None,
    page_size=1,
    candidate_index=None,
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
        sync_level="full_index",
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


def forwarded_scan(
    tmp_path, api, table, first_page, *, pattern="needle", **grep_options
):
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
                    assert response.status_code == 200, (response.text, body)
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
            tmp_path,
            f"http://127.0.0.1:{server.server_port}",
            table,
            pattern,
            **grep_options,
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
        sync_level="full_index",
    )

    def update():
        stateful_api.batch_write(
            table,
            inserts={
                "b": document("b.py", "needle CHANGED\n"),
                "c": document("c.py", "needle ADDED\n"),
            },
            sync_level="full_index",
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
        sync_level="full_index",
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
    stateful_api.batch_write(table, inserts=rows, sync_level="full_index")
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
        table, inserts={"a": item, "b": denied}, sync_level="full_index"
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
    stateful_api.batch_write(table, inserts={"a": item}, sync_level="full_index")
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


@pytest.mark.parametrize("num_shards", [1, 3])
def test_native_grep_builtin_substring_and_regex_candidates(
    stateful_api, tmp_path, num_shards
):
    table = f"grep_text_candidates_{time.time_ns()}"
    stateful_api.post(
        f"/tables/{table}",
        {
            "num_shards": num_shards,
            "schema": {
                "default_type": "doc",
                "document_schemas": {
                    "doc": {
                        "schema": {
                            "type": "object",
                            "additionalProperties": True,
                            "properties": {
                                "grep": {
                                    "type": "object",
                                    "properties": {
                                        "content": {
                                            "type": "string",
                                            "x-antfly-types": ["text", "substring"],
                                        }
                                    },
                                }
                            },
                        }
                    }
                },
            },
        },
    )
    stateful_api.batch_write(
        table,
        inserts={
            "a": document("exact.py", "TARGET xyz\n"),
            "b": document("case.py", "target xyz\n"),
            "c": document("punctuation.py", "TAR-GET xyz\n"),
            "d": document("unicode.py", "雪TARGETxyz\n"),
            "e": document("miss.py", "irrelevant\n" * 16384),
            "f": document("lines.py", "TARGET\nxyz\n"),
        },
        sync_level="full_index",
    )
    # The server chooses the companion; no physical field names appear in
    # configuration. CLI regex semantics are still checked after retrieval.
    for flags, expected in (
        (["-F", "TARGET"], {"exact.py", "unicode.py", "lines.py"}),
        (["TAR.*xyz"], {"exact.py", "punctuation.py", "unicode.py"}),
        (
            ["-F", "-e", "TARGET", "-e", "irrelevant"],
            {"exact.py", "unicode.py", "lines.py", "miss.py"},
        ),
    ):
        result = run_grep(tmp_path, stateful_api.url, table, *flags, page_size=2)
        assert result.returncode == 0, result.stderr.decode()
        paths = {line.split(b":", 1)[0].decode() for line in result.stdout.splitlines()}
        assert paths == expected
    for flags in (["--json", "TARGET"], ["-v", "TARGET"], ["-F", "!!!"]):
        result = run_grep(tmp_path, stateful_api.url, table, *flags)
        assert result.returncode in (0, 1), result.stderr.decode()
    url = stateful_api.url + f"/tables/{table}/query"
    base = {
        "query": {"match_all": {}},
        "order_by": [{"field": "_id", "desc": False}],
        "limit": 100,
    }
    full = requests.post(url, json=base, timeout=60)
    selected = dict(base)
    selected["query"] = {"contains": "TARGET", "field": "grep.content"}
    selected["profile"] = True
    filtered = requests.post(url, json=selected, timeout=60)
    assert full.status_code == filtered.status_code == 200, filtered.text
    assert len(filtered.content) < len(full.content) // 8
    hits = filtered.json()["responses"][0]["hits"]["hits"]
    assert {h["_id"] for h in hits} == {"a", "d", "f"}
    profile = filtered.json()["responses"][0]["profile"]["value_verification"]
    assert profile["documents"] >= 3 and profile["bytes"] > 0
    assert any(
        plan["candidate_field"] == "grep.content._substring"
        for plan in profile["plans"]
    )
    assert all("content" in h["_source"]["grep"] for h in hits)

    def query_ids(query, execution=None):
        payload = {"query": query, "fields": [], "limit": 100}
        if execution is not None:
            payload["execution"] = {"value_verification": execution}
        response = requests.post(url, json=payload, timeout=60)
        assert response.status_code == 200, response.text
        return {h["_id"] for h in response.json()["responses"][0]["hits"]["hits"]}

    assert query_ids(
        {"equals": "TARGET xyz\n", "field": "grep.content"}, {"allow_scan": True}
    ) == {"a"}
    assert query_ids(
        {"contains": "target", "field": "grep.content", "case_sensitive": False},
        {"allow_scan": True},
    ) == {"a", "b", "d", "f"}
    assert query_ids({"matches": "^TAR.*xyz", "field": "grep.content"}) == {"a", "c"}
    assert query_ids({"matches": "TARGET\nxyz", "field": "grep.content"}) == {"f"}
    assert query_ids(
        {
            "bool": {
                "must": [{"match_all": {}}],
                "must_not": [{"contains": "TARGET", "field": "grep.content"}],
            }
        }
    ) == {"b", "c", "e"}
    refused = requests.post(
        url, json={"query": {"contains": "!", "field": "grep.content"}}, timeout=60
    )
    assert (
        refused.status_code == 422
        and "value_verification_requires_scan" in refused.text
    ), refused.text
    exhausted = requests.post(
        url,
        json={
            "query": {"contains": "TARGET", "field": "grep.content"},
            "count": True,
            "execution": {"value_verification": {"max_documents": 1}},
        },
        timeout=60,
    )
    assert (
        exhausted.status_code == 422
        and "value_verification_limit_exceeded" in exhausted.text
    ), exhausted.text

    def update_miss():
        stateful_api.batch_write(
            table,
            inserts={"e": document("miss.py", "TARGET NEW\n")},
            sync_level="full_index",
        )

    frozen, seen = forwarded_scan(
        tmp_path,
        stateful_api,
        table,
        update_miss,
        pattern="TARGET",
    )
    assert frozen.returncode == 0, frozen.stderr.decode()
    assert b"NEW" not in frozen.stdout and b"exact.py" in frozen.stdout
    assert len(seen) > 1
    assert all(q["query"] == seen[0]["query"] for q in seen)
    assert all("remote_snapshot" in q for q in seen[1:])
    assert all("grep_scan" not in q for q in seen)


def test_canonical_execution_rejects_removed_public_controls(stateful_api):
    table = f"canonical_execution_{time.time_ns()}"
    stateful_api.post(f"/tables/{table}", {"num_shards": 1})
    stateful_api.batch_write(
        table, inserts={"a": {"content": "needle"}}, sync_level="full_index"
    )
    url = stateful_api.url + f"/tables/{table}/query"
    base = {"query": {"match_all": {}}, "limit": 1}
    canonical = {
        **base,
        "execution": {"value_verification": {"max_documents": 1}},
    }
    response = requests.post(url, json=canonical, timeout=60)
    assert response.status_code == 200, response.text
    assert response.json()["responses"][0]["hits"]["total"]["value"] == 1
    for removed, value in (
        ("lake_read", {}),
        ("lake_visibility", "accepted"),
        ("value_search_execution", {}),
        ("execution", {"max_documents": 1}),
    ):
        response = requests.post(url, json={**base, removed: value}, timeout=60)
        assert response.status_code == 400, (removed, response.text)
    for removed in ("lake_visibility", "lake_read"):
        response = requests.post(
            stateful_api.url + "/sql",
            json={"statement": "SELECT 1", removed: "accepted"},
            timeout=60,
        )
        assert response.status_code == 400, (removed, response.text)


def test_logical_keyword_and_autocomplete_use_source_fields(stateful_api):
    table = f"logical_fields_{time.time_ns()}"
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
                            "properties": {
                                "title": {
                                    "type": "string",
                                    "x-antfly-types": [
                                        "text",
                                        "keyword",
                                        "search_as_you_type",
                                    ],
                                }
                            },
                        }
                    }
                },
            },
        },
    )
    stateful_api.batch_write(
        table,
        inserts={
            "a": {"title": "Distributed Systems"},
            "b": {"title": "distributed storage"},
            "c": {"title": "Distinct Sequences"},
        },
        sync_level="full_index",
    )
    url = stateful_api.url + f"/tables/{table}/query"
    for query in (
        {"equals": "Distributed Systems", "field": "title"},
        {"autocomplete": "distributed sy", "field": "title"},
    ):
        response = requests.post(
            url, json={"query": query, "profile": True, "limit": 10}, timeout=60
        )
        assert response.status_code == 200, response.text
        result = response.json()["responses"][0]
        assert {hit["_id"] for hit in result["hits"]["hits"]} == (
            {"a"} if "equals" in query else {"a", "b"}
        )
        if "equals" in query:
            plan = result["profile"]["value_verification"]["plans"][0]
            assert plan["field"] == "title" and plan["candidate_field"] is not None
            assert plan["verification"] == "lossless_keyword"
            assert result["profile"]["value_verification"]["documents"] == 0
            assert result["profile"]["value_verification"]["bytes"] == 0
    capabilities = stateful_api.get(f"/tables/{table}")["field_capabilities"]
    title = [cap for cap in capabilities if cap.get("field") == "title"]
    assert len(title) == 1, capabilities
    assert title[0]["kind"] == "source"
    assert {"autocomplete", "equals"} <= set(title[0]["value_operations"])
    assert {"text", "keyword", "search_as_you_type"} <= {
        rep["type"] for rep in title[0]["representations"]
    }
    assert all(
        "logical_field" not in cap and "emitted_name" not in cap for cap in capabilities
    )
    identity = next(cap for cap in capabilities if cap.get("field") == "_id")
    assert identity["kind"] == "reserved" and identity["value_operations"] == []
    score = next(cap for cap in capabilities if cap.get("field") == "_score")
    assert score["kind"] == "computed" and score["sort_requires_scoring"] is True
    assert score["value_operations"] == [] and score["representations"] == []


def test_logical_sort_uses_keyword_companion_at_public_boundary(stateful_api):
    table = f"logical_sort_{time.time_ns()}"
    stateful_api.post(
        f"/tables/{table}",
        {
            "num_shards": 1,
            "indexes": {"text": {"type": "full_text"}},
            "schema": {
                "default_type": "doc",
                "document_schemas": {
                    "doc": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "title": {
                                    "type": "string",
                                    "x-antfly-field": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "sortable": True,
                                            }
                                        },
                                    },
                                }
                            },
                        }
                    }
                },
            },
        },
    )
    stateful_api.batch_write(
        table,
        inserts={"a": {"title": "zebra"}, "b": {"title": "Apple"}},
        sync_level="full_index",
    )
    response = requests.post(
        stateful_api.url + f"/tables/{table}/query",
        json={"query": {"match_all": {}}, "order_by": [{"field": "title"}], "limit": 2},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert [hit["_id"] for hit in response.json()["responses"][0]["hits"]["hits"]] == [
        "b",
        "a",
    ]
    capabilities = stateful_api.get(f"/tables/{table}")["field_capabilities"]
    title = next(cap for cap in capabilities if cap.get("field") == "title")
    assert title["sortable"] is True, title
    assert any(
        rep.get("field") == "title.keyword" and rep["sortable"]
        for rep in title["representations"]
    ), title


def test_logical_named_autocomplete_keeps_declared_source_associations(stateful_api):
    table = f"logical_named_{time.time_ns()}"
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
                            "properties": {
                                "title": {
                                    "type": "string",
                                    "x-antfly-field": {
                                        "type": "text",
                                        "fields": {
                                            "suggest": {"type": "search_as_you_type"}
                                        },
                                    },
                                }
                            },
                        }
                    }
                },
            },
        },
    )
    stateful_api.batch_write(
        table,
        inserts={
            "a": {"title": "Distributed Systems"},
            "b": {"title": "Dinosaur Bones"},
            "long": {"title": "abcdefghijklmnopqrstuvwxyzzebra"},
            "short": {"title": "x"},
        },
        sync_level="full_index",
    )
    response = requests.post(
        stateful_api.url + f"/tables/{table}/query",
        json={"query": {"autocomplete": "distributed", "field": "title"}},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert [h["_id"] for h in response.json()["responses"][0]["hits"]["hits"]] == ["a"]
    for text, expected in [
        ("abcdefghijklmnopqrst", ["long"]),
        ("abcdefghijklmnopqrstu", ["long"]),
        ("abcdefghijklmnopqrstuvwxyzzebra", ["long"]),
        ("abcdefghijklmnopqrstuvwxyzzdog", []),
        ("x", ["short"]),
    ]:
        response = requests.post(
            stateful_api.url + f"/tables/{table}/query",
            json={"query": {"autocomplete": text, "field": "title"}},
            timeout=60,
        )
        assert response.status_code == 200, response.text
        assert [
            h["_id"] for h in response.json()["responses"][0]["hits"]["hits"]
        ] == expected
    capabilities = stateful_api.get(f"/tables/{table}")["field_capabilities"]
    assert any(
        cap.get("field") == "title"
        and "autocomplete" in cap.get("value_operations", [])
        for cap in capabilities
    )
    assert not any(cap.get("field") == "title.suggest" for cap in capabilities)


def test_logical_verification_reuses_sources_and_retained_predicate_facts(stateful_api):
    table = f"logical_reuse_{time.time_ns()}"
    stateful_api.create_table(table)
    stateful_api.batch_write(
        table,
        inserts={f"{i:03}": {"title": "needle alpha beta"} for i in range(20)},
        sync_level="full_index",
    )
    url = stateful_api.url + f"/tables/{table}/query"
    payload = {
        "query": {"contains": "needle", "field": "title"},
        "execution": {"value_verification": {"allow_scan": True}},
        "profile": True,
        "limit": 1,
        "order_by": [{"field": "_id", "desc": False}],
    }
    pages = []
    for _ in range(3):
        response = requests.post(url, json=payload, timeout=60)
        assert response.status_code == 200, response.text
        page = response.json()["responses"][0]
        pages.append(page)
        payload["search_after"] = page["hits"]["hits"][-1]["_sort"]
        payload["remote_snapshot"] = page["remote_snapshot"]
    assert [p["hits"]["hits"][0]["_id"] for p in pages] == ["000", "001", "002"]
    assert [p["profile"]["value_verification"]["documents"] for p in pages] == [
        20,
        0,
        0,
    ]
    assert all(p["hits"]["total"]["value"] == 20 for p in pages)

    # A warm count must borrow the complete predicate without source work,
    # including when the caller admits less verification than a cold scan needs.
    warm_count = {
        k: v for k, v in payload.items() if k not in ("search_after", "order_by")
    }
    warm_count["count"] = True
    warm_count["execution"] = {
        "value_verification": {"allow_scan": True, "max_documents": 1, "max_bytes": 1}
    }
    response = requests.post(url, json=warm_count, timeout=60)
    assert response.status_code == 200, response.text
    counted = response.json()["responses"][0]
    assert counted["hits"]["total"]["value"] == 20, counted
    assert counted["profile"]["value_verification"]["documents"] == 0, counted
    assert counted["profile"]["value_verification"]["bytes"] == 0, counted

    # Reuse is predicate-specific, and excludes ordinary mutable queries.
    payload.pop("search_after")
    payload.pop("remote_snapshot")
    payload.pop("order_by")
    payload["query"] = {
        "bool": {
            "should": [
                {"contains": term, "field": "title"}
                for term in ("needle", "alpha", "beta")
            ]
        }
    }
    response = requests.post(url, json=payload, timeout=60)
    assert response.status_code == 200, response.text
    result = response.json()["responses"][0]
    assert result["profile"]["value_verification"]["documents"] == 20
    assert result["profile"]["value_verification"]["bytes"] == 580

    payload["query"] = {"contains": "absent", "field": "title"}
    payload["remote_snapshot"] = pages[0]["remote_snapshot"]
    payload["order_by"] = [{"field": "_id", "desc": False}]
    response = requests.post(url, json=payload, timeout=60)
    assert response.status_code == 200, response.text
    missing = response.json()["responses"][0]
    assert missing["hits"]["total"]["value"] == 0
    assert missing["profile"]["value_verification"]["documents"] == 20

    # A newer cut must observe writes rather than reuse the earlier membership.
    stateful_api.batch_write(
        table, inserts={"000": {"title": "absent"}}, sync_level="full_index"
    )
    payload.pop("remote_snapshot")
    payload["query"] = {"contains": "needle", "field": "title"}
    response = requests.post(url, json=payload, timeout=60)
    assert response.status_code == 200, response.text
    fresh = response.json()["responses"][0]
    assert fresh["hits"]["total"]["value"] == 19
    assert fresh["profile"]["value_verification"]["documents"] == 20


@pytest.mark.parametrize("num_shards", [1, 3])
def test_logical_retained_boolean_counts_need_no_source_work(stateful_api, num_shards):
    table = f"logical_boolean_count_{num_shards}_{time.time_ns()}"
    stateful_api.post(f"/tables/{table}", {"num_shards": num_shards})
    stateful_api.batch_write(
        table,
        inserts={f"{i:03}": {"title": "needle alpha beta"} for i in range(20)},
        sync_level="full_index",
    )
    leaves = [
        {"contains": term, "field": "title"}
        for term in ("needle", "alpha", "beta", "absent")
    ]
    url = stateful_api.url + f"/tables/{table}/query"
    warm = requests.post(
        url,
        json={
            "query": {"bool": {"should": leaves}},
            "execution": {"value_verification": {"allow_scan": True}},
            "profile": True,
            "limit": 1,
            "order_by": [{"field": "_id", "desc": False}],
        },
        timeout=60,
    )
    assert warm.status_code == 200, warm.text
    snapshot = warm.json()["responses"][0]["remote_snapshot"]
    cases = [
        ({"must": leaves[:2]}, 20),
        ({"should": leaves[2:]}, 20),
        ({"should": [leaves[0], leaves[1], leaves[3]], "minimum_should_match": 2}, 20),
        ({"must": leaves[:1], "must_not": leaves[1:2]}, 0),
        ({"should": leaves[3:], "minimum_should_match": 0}, 20),
        (
            {"filter": leaves[0], "should": leaves[3:], "minimum_should_match": 1},
            0,
        ),
        (
            {
                "must": [leaves[0]],
                "filter": {"bool": {"should": [leaves[1], leaves[3]]}},
                "must_not": [leaves[3]],
            },
            20,
        ),
    ]
    for expression, expected in cases:
        counted = requests.post(
            url,
            json={
                "query": {"bool": expression},
                "count": True,
                "remote_snapshot": snapshot,
                "profile": True,
                "execution": {
                    "value_verification": {
                        "allow_scan": True,
                        "max_documents": 1,
                        "max_bytes": 1,
                    }
                },
            },
            timeout=60,
        )
        assert counted.status_code == 200, (expression, counted.text)
        result = counted.json()["responses"][0]
        assert result["hits"]["total"]["value"] == expected, result
        assert result["profile"]["value_verification"]["documents"] == 0, result
        assert result["profile"]["value_verification"]["bytes"] == 0, result


def test_logical_verification_work_profile(stateful_api):
    """Optional comparable artifact: run this case against either server binary."""
    import hashlib
    import platform

    report_path = os.environ.get("ANTFLY_LOGICAL_REUSE_REPORT")
    if not report_path:
        pytest.skip(
            "set ANTFLY_LOGICAL_REUSE_REPORT to record the bounded work profile"
        )
    table = f"logical_profile_{time.time_ns()}"
    stateful_api.create_table(table)
    text = "needle alpha beta " + "padding " * 1024
    documents = {f"{i:03}": {"title": text} for i in range(128)}
    stateful_api.batch_write(table, inserts=documents, sync_level="full_index")
    url = stateful_api.url + f"/tables/{table}/query"
    base = {
        "execution": {"value_verification": {"allow_scan": True}},
        "profile": True,
        "limit": 1,
        "fields": ["title"],
    }
    records = []

    def measured(name, payload):
        started = time.perf_counter_ns()
        response = requests.post(url, json=payload, timeout=60)
        elapsed = time.perf_counter_ns() - started
        assert response.status_code == 200, response.text
        result = response.json()["responses"][0]
        assert result["hits"]["total"]["value"] == 128
        records.append(
            {
                "name": name,
                "elapsed_ns": elapsed,
                "value_verification": result["profile"]["value_verification"],
            }
        )
        return result

    payload = dict(
        base,
        query={"contains": "needle", "field": "title"},
        order_by=[{"field": "_id"}],
    )
    for index in range(8):
        result = measured(f"page_{index}", payload)
        payload["remote_snapshot"] = result["remote_snapshot"]
        payload["search_after"] = result["hits"]["hits"][-1]["_sort"]
    for index in range(7):
        measured(
            "literal_or",
            dict(
                base,
                count=True,
                query={
                    "bool": {
                        "should": [
                            {"contains": word, "field": "title"}
                            for word in ("needle", "alpha", "beta")
                        ]
                    }
                },
            ),
        )
        # Mixed shapes exercise decoded-source sharing separately from fusion.
        measured(
            "mixed_or",
            dict(
                base,
                count=True,
                query={
                    "bool": {
                        "should": [
                            {"contains": "needle", "field": "title"},
                            {"matches": "alpha", "field": "title"},
                        ]
                    }
                },
            ),
        )
    binary = Path(os.environ["ANTFLY_BIN"])
    with binary.open("rb") as stream:
        binary_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    Path(report_path).write_text(
        json.dumps(
            {
                "binary_sha256": binary_sha256,
                "host": platform.platform(),
                "cpu_count": os.cpu_count(),
                "documents": 128,
                "source_sha256": hashlib.sha256(
                    json.dumps(documents, sort_keys=True).encode()
                ).hexdigest(),
                "records": records,
                "limitations": "Debug server, shared macOS host, warm caches; work counters are exact, wall times include HTTP and host contention.",
            },
            indent=2,
        )
        + "\n"
    )


def test_logical_mixed_streaming_exceeds_decoded_source_cache(stateful_api):
    """Mixed shapes retain membership, not a corpus of parsed source bodies."""
    table = f"logical_mixed_stream_{time.time_ns()}"
    stateful_api.create_table(table)
    documents = 4096
    title = "needle alpha beta " + "x" * 4096
    for start in range(0, documents, 256):
        stateful_api.batch_write(
            table,
            inserts={f"{i:05}": {"title": title} for i in range(start, start + 256)},
            sync_level="full_index",
        )
    response = requests.post(
        stateful_api.url + f"/tables/{table}/query",
        json={
            "query": {
                "bool": {
                    "should": [
                        *(
                            {"contains": word, "field": "title"}
                            for word in ("needle", "alpha", "beta")
                        ),
                        {"matches": "z[e]bra", "field": "title"},
                        {"equals": "missing", "field": "other"},
                    ]
                }
            },
            "count": True,
            "limit": 1,
            "fields": [],
            "profile": True,
            # Exactly one source pass fits. Repeated passes must fail admission.
            "execution": {
                "value_verification": {
                    "allow_scan": True,
                    "max_documents": documents,
                    "max_bytes": 20 * 1024 * 1024,
                    "max_regex_work": 1_000_000_000,
                }
            },
        },
        timeout=120,
    )
    assert response.status_code == 200, response.text
    result = response.json()["responses"][0]
    assert result["status"] == 200, result
    assert result["hits"]["total"] == {"value": documents, "relation": "exact"}
    assert result["profile"]["value_verification"]["documents"] == documents
    assert result["profile"]["value_verification"]["bytes"] == documents * len(
        json.dumps({"title": title}, separators=(",", ":")).encode()
    )


def test_logical_mixed_work_profile(stateful_api):
    """Opt-in reproducible work/latency comparison against an earlier binary."""
    import hashlib
    import platform

    report = os.environ.get("ANTFLY_LOGICAL_STREAM_REPORT")
    if not report:
        pytest.skip(
            "Set ANTFLY_LOGICAL_STREAM_REPORT to capture seven mixed-work trials"
        )
    table = f"stream_profile_{time.time_ns()}"
    stateful_api.create_table(table)
    n = 4096
    title = "needle alpha beta " + "x" * 4096
    for start in range(0, n, 256):
        stateful_api.batch_write(
            table,
            inserts={f"{i:05}": {"title": title} for i in range(start, start + 256)},
            sync_level="full_index",
        )
    leaves = [
        {"contains": word, "field": "title"} for word in ("needle", "alpha", "beta")
    ]
    records = []
    for trial in range(7):
        for name, query in (
            ("literal3", leaves),
            ("mixed4", leaves + [{"matches": "z[e]bra", "field": "title"}]),
            (
                "mixed5",
                leaves
                + [
                    {"matches": "z[e]bra", "field": "title"},
                    {"equals": "missing", "field": "other"},
                ],
            ),
        ):
            started = time.perf_counter_ns()
            response = requests.post(
                stateful_api.url + f"/tables/{table}/query",
                json={
                    "query": {"bool": {"should": query}},
                    "count": True,
                    "limit": 1,
                    "fields": [],
                    "profile": True,
                    "execution": {
                        "value_verification": {
                            "allow_scan": True,
                            "max_bytes": 256 * 1024 * 1024,
                            "max_regex_work": 1000000000,
                        }
                    },
                },
                timeout=120,
            )
            elapsed = time.perf_counter_ns() - started
            assert response.status_code == 200, response.text
            result = response.json()["responses"][0]
            assert result["status"] == 200, result
            assert result["hits"]["total"] == {"value": n, "relation": "exact"}, result
            records.append(
                {
                    "trial": trial,
                    "name": name,
                    "elapsed_ns": elapsed,
                    "work": result["profile"]["value_verification"],
                }
            )
    corpus = hashlib.sha256()
    for i in range(n):
        corpus.update(
            json.dumps([f"{i:05}", {"title": title}], separators=(",", ":")).encode()
        )
    with open(os.environ["ANTFLY_BIN"], "rb") as binary:
        binary_sha = hashlib.file_digest(binary, "sha256").hexdigest()
    Path(report).write_text(
        json.dumps(
            {
                "binary_sha256": binary_sha,
                "host": platform.platform(),
                "cpu_count": os.cpu_count(),
                "documents": n,
                "source_bytes": n
                * len(json.dumps({"title": title}, separators=(",", ":")).encode()),
                "corpus_sha256": corpus.hexdigest(),
                "records": records,
            },
            indent=2,
        )
        + "\n"
    )


def logical_process_reader(pid):
    """Low-overhead process CPU/RSS sampling; neither value is a query quota."""
    import platform

    if platform.system() == "Darwin":
        import ctypes

        class Info(ctypes.Structure):
            # SDK sys/resource.h rusage_info_v2: UUID then eighteen uint64s.
            _fields_ = [("uuid", ctypes.c_ubyte * 16), ("values", ctypes.c_uint64 * 18)]

        class Timebase(ctypes.Structure):
            _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]

        system = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
        system.mach_timebase_info.argtypes = [ctypes.POINTER(Timebase)]
        system.mach_timebase_info.restype = ctypes.c_int
        timebase = Timebase()
        assert system.mach_timebase_info(ctypes.byref(timebase)) == 0
        assert timebase.denom != 0
        lib = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        lib.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
        lib.proc_pid_rusage.restype = ctypes.c_int

        def read():
            value = Info()
            if lib.proc_pid_rusage(pid, 2, ctypes.byref(value)) != 0:
                raise OSError(ctypes.get_errno(), "proc_pid_rusage")
            return {
                "cpu_ns": (value.values[0] + value.values[1])
                * timebase.numer
                // timebase.denom,
                "rss_bytes": value.values[6],
                "footprint_bytes": value.values[7],
            }

        return read
    if platform.system() == "Linux":
        ticks = os.sysconf("SC_CLK_TCK")
        page_size = os.sysconf("SC_PAGESIZE")
        path = Path(f"/proc/{pid}/stat")

        def read():
            fields = path.read_text().rsplit(") ", 1)[1].split()
            return {
                "cpu_ns": (int(fields[11]) + int(fields[12])) * 1000000000 // ticks,
                "rss_bytes": int(fields[21]) * page_size,
                "footprint_bytes": None,
            }

        return read
    pytest.skip("process qualification requires macOS or Linux")


def qualification_query(body):
    # Qualification-only adapter for immutable binaries predating the public
    # execution envelope. Never used by CLI or server compatibility paths.
    if os.environ.get("ANTFLY_BENCH_LEGACY_VALUE_WIRE") != "1":
        return body
    body = dict(body)
    execution = body.pop("execution", None)
    if execution is not None:
        assert set(execution) == {"value_verification"}
        body["value_search_execution"] = execution["value_verification"]
    return body


def qualification_verification(result):
    key = (
        "value_search"
        if os.environ.get("ANTFLY_BENCH_LEGACY_VALUE_WIRE") == "1"
        else "value_verification"
    )
    return result["profile"][key]


def logical_process_io(pid):
    """Linux process counters include background I/O, not just the timed RPC."""
    path = Path(f"/proc/{pid}/io")
    if not path.is_file():
        return None
    counters = dict(line.split(":", 1) for line in path.read_text().splitlines())
    return {
        name: int(counters[name])
        for name in ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes")
    }


def test_logical_substring_ingestion_profile(stateful_api):
    """Opt-in production substring pipeline: full_index publication and cleanup."""
    import hashlib
    import random

    destination = os.environ.get("ANTFLY_LOGICAL_INGESTION_REPORT")
    if not destination:
        pytest.skip(
            "set ANTFLY_LOGICAL_INGESTION_REPORT to profile substring ingestion"
        )
    assert stateful_api._server is not None
    read_process = logical_process_reader(stateful_api._server.proc.pid)
    size = 180 * 1024
    random_source = random.Random(0x51FF1C)
    corpora = {
        "repetitive": ("needle-" * (size // 7 + 1))[:size],
        "source_code": (
            "pub fn function(value: usize) usize { return value + 997; }\n"
            * (size // 32)
        )[:size],
        "distinct_word": "".join(
            random_source.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(size)
        ),
    }
    records = []
    for name, content in corpora.items():
        table = f"substring_ingestion_{name}_{time.time_ns()}"
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
                                "properties": {
                                    "content": {
                                        "type": "string",
                                        "x-antfly-types": ["text", "substring"],
                                    }
                                },
                            }
                        }
                    },
                },
            },
        )
        corpus = {f"{i:04}": {"content": content} for i in range(8)}
        initial = read_process()
        start = time.perf_counter_ns()
        batches = []
        for offset in range(0, len(corpus), 2):
            before = read_process()
            io_before = logical_process_io(stateful_api._server.proc.pid)
            started = time.perf_counter_ns()
            keys = list(corpus)[offset : offset + 2]
            stateful_api.batch_write(
                table, inserts={k: corpus[k] for k in keys}, sync_level="full_index"
            )
            request_elapsed_ns = time.perf_counter_ns() - started
            after = read_process()
            io_after = logical_process_io(stateful_api._server.proc.pid)
            batches.append(
                {
                    "documents": len(keys),
                    "http_ms": request_elapsed_ns / 1e6,
                    "cpu_ms": (after["cpu_ns"] - before["cpu_ns"]) / 1e6,
                    "process_before": before,
                    "process_after": after,
                    "io_before": io_before,
                    "io_after": io_after,
                }
            )
        elapsed = time.perf_counter_ns() - start
        final = read_process()
        # Confirm the declared pipeline is actually admitted, then verify every
        # row through its logical operation; timing covers writes, not this check.
        caps = stateful_api.get(f"/tables/{table}")["field_capabilities"]
        field = next(c for c in caps if c.get("field") == "content")
        assert "substring" in {r["type"] for r in field["representations"]}, caps
        result = stateful_api.post(
            f"/tables/{table}/query",
            {
                "query": {"contains": content[:12], "field": "content"},
                "limit": 10,
            },
        )["responses"][0]
        assert {h["_id"] for h in result["hits"]["hits"]} == set(corpus), result
        records.append(
            {
                "name": name,
                "documents": len(corpus),
                "source_bytes": len(content.encode()) * len(corpus),
                "corpus_sha256": hashlib.sha256(
                    json.dumps(corpus, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
                "http_ms": sum(batch["http_ms"] for batch in batches),
                "wall_ms": elapsed / 1e6,
                "cpu_ms": (final["cpu_ns"] - initial["cpu_ns"]) / 1e6,
                "process_initial": initial,
                "process_final": final,
                "batches": batches,
            }
        )
    with Path(os.environ["ANTFLY_BIN"]).open("rb") as binary:
        binary_digest = hashlib.file_digest(binary, "sha256").hexdigest()
    Path(destination).write_text(
        json.dumps(
            {
                "protocol": 2,
                "scope": "Schema-enabled substring pipeline; full_index batch writes include publication and cleanup. http_ms sums RPC durations; wall_ms includes inter-batch instrumentation. Process CPU and Linux I/O counters include background work; kernel I/O counters do not establish cold-device traffic. RSS samples are endpoints, not peak-memory bounds.",
                "binary_sha256": binary_digest,
                "records": records,
            },
            separators=(",", ":"),
        )
    )


def test_logical_scalable_work_profile(stateful_api):
    """Opt-in optimized-build comparison with cold predicate membership."""
    import hashlib
    import platform
    import statistics

    destination = os.environ.get("ANTFLY_LOGICAL_SCALABLE_REPORT")
    if not destination:
        pytest.skip("set ANTFLY_LOGICAL_SCALABLE_REPORT to record cold verification")
    import threading

    assert stateful_api._server is not None, (
        "qualification requires a local child server"
    )
    pid = stateful_api._server.proc.pid
    read_process = logical_process_reader(pid)
    records = []
    for name, count, title, extra in [
        ("tiny", 4096, "needle alpha beta", {}),
        ("selected_large", 4096, "needle alpha beta " + "x" * 4096, {}),
        # Above the authority threshold, but the selected root dominates the
        # source. Projection should decline without a persistent read penalty.
        ("selected_dominant", 1024, "needle alpha beta " + "x" * 16384, {}),
        ("unselected_arrays", 1024, "needle alpha beta", {"unused": ["\n" * 16] * 512}),
        (
            "nested_source",
            1024,
            "needle alpha beta",
            {"regions": [{"start_line": 1, "end_line": 1}] * 1024},
        ),
    ]:
        table = f"logical_scalable_{name}_{time.time_ns()}"
        stateful_api.create_table(table)
        # Native standalone timers/workers have nonzero idle CPU. Measure it
        # before ingestion, independently of the workload's observed results.
        idle_samples = []
        for _ in range(3):
            idle_before = read_process()["cpu_ns"]
            idle_started = time.monotonic()
            time.sleep(1)
            idle_elapsed = time.monotonic() - idle_started
            idle_samples.append(
                {
                    "elapsed_ms": idle_elapsed * 1000,
                    "cpu_ms": (read_process()["cpu_ns"] - idle_before) / 1000000,
                }
            )
        idle_cpu_ms_per_second = statistics.median(
            sample["cpu_ms"] * 1000 / sample["elapsed_ms"] for sample in idle_samples
        )
        field = "grep.content" if name == "nested_source" else "title"
        missing_field = "grep.other" if name == "nested_source" else "other"
        if name == "nested_source":
            # Exercise the actual v1 source contract, including large valid
            # region metadata and the default nested content path.
            documents = {}
            for i in range(count):
                value = document(f"{i:06}.py", title)
                value["grep"]["regions"] = extra["regions"]
                documents[f"{i:06}"] = value
        else:
            documents = {f"{i:06}": {"title": title, **extra} for i in range(count)}
        corpus = json.dumps(documents, sort_keys=True, separators=(",", ":")).encode()
        ingestion_initial = read_process()
        ingestion_started = time.perf_counter_ns()
        ingestion_batches = []
        for offset in range(0, count, 128):
            keys = list(documents)[offset : offset + 128]
            before = read_process()
            started = time.perf_counter_ns()
            stateful_api.batch_write(
                table, inserts={k: documents[k] for k in keys}, sync_level="full_index"
            )
            elapsed = time.perf_counter_ns() - started
            after = read_process()
            ingestion_batches.append(
                {
                    "offset": offset,
                    "documents": len(keys),
                    "http_ms": elapsed / 1000000,
                    "cpu_ms": (after["cpu_ns"] - before["cpu_ns"]) / 1000000,
                    "process_before": before,
                    "process_after": after,
                }
            )
        ingestion_elapsed = time.perf_counter_ns() - ingestion_started
        ingestion_final = read_process()
        query = {
            "query": {
                "bool": {
                    "should": [
                        *[
                            {"contains": text, "field": field}
                            for text in ("needle", "alpha", "beta")
                        ],
                        {"matches": "z[e]bra", "field": field},
                        {"equals": "missing", "field": missing_field},
                    ]
                }
            },
            "count": True,
            "limit": 1,
            "fields": [],
            "profile": True,
            "execution": {
                "value_verification": {
                    "allow_scan": True,
                    "max_documents": count,
                    "max_bytes": 256 * 1024 * 1024,
                    "max_regex_work": 1000000000,
                }
            },
        }
        # Warm code and source caches explicitly; retain this observation and
        # then wait for background ingestion/compaction CPU to settle. Ordinary
        # queries have no retained predicate scope, and every timed trial below
        # must still verify the full corpus under its exact work budget.
        warm_cpu = read_process()["cpu_ns"]
        warm_started = time.perf_counter_ns()
        warm_response = requests.post(
            stateful_api.url + f"/tables/{table}/query",
            json=qualification_query(query),
            timeout=90,
        )
        warm_elapsed = (time.perf_counter_ns() - warm_started) / 1000000
        assert warm_response.status_code == 200, warm_response.text
        warm_result = warm_response.json()["responses"][0]
        assert warm_result["hits"]["total"]["value"] == count, warm_result
        warmup = {
            "http_ms": warm_elapsed,
            "cpu_ms": (read_process()["cpu_ns"] - warm_cpu) / 1000000,
            "profile": qualification_verification(warm_result),
        }
        assert warmup["profile"]["documents"] == count, warmup
        # Use complete one-second windows so Linux's CPU-accounting tick is
        # tolerated once per second, rather than in each short polling sample.
        # Three consecutive quiet windows also span delayed maintenance timers.
        settling = []
        quiet = 0
        settle_started = time.monotonic()
        previous_time = settle_started
        tick_ms = 1000 / os.sysconf("SC_CLK_TCK") if platform.system() == "Linux" else 0
        previous = read_process()
        while quiet < 3 and time.monotonic() - settle_started < 30:
            time.sleep(1)
            current = read_process()
            current_time = time.monotonic()
            cpu_ms = (current["cpu_ns"] - previous["cpu_ns"]) / 1000000
            cpu_limit_ms = (current_time - previous_time) * (
                idle_cpu_ms_per_second + 50
            ) + tick_ms
            settling.append(
                {
                    "elapsed_ms": (current_time - settle_started) * 1000,
                    "cpu_ms": cpu_ms,
                    "cpu_limit_ms": cpu_limit_ms,
                }
            )
            quiet = quiet + 1 if cpu_ms <= cpu_limit_ms else 0
            previous = current
            previous_time = current_time
        if quiet != 3:
            Path(destination).with_suffix(".failed.json").write_text(
                json.dumps(
                    {"workload": name, "warmup": warmup, "settling": settling}, indent=2
                )
                + "\n"
            )
        assert quiet == 3, ("server ingestion CPU did not settle", settling)
        runs = []
        initial_process = read_process()
        samples = [initial_process]
        sampling_errors = []
        stop = threading.Event()

        def watch():
            while not stop.wait(0.005):
                try:
                    samples.append(read_process())
                except OSError as error:
                    sampling_errors.append(str(error))
                    return

        watcher = threading.Thread(target=watch)
        watcher.start()
        try:
            for trial in range(9):
                # Context for shared-host scheduling noise, outside timing.
                # Load averages are host/VM-wide, not the server's CPU quota.
                host_load = os.getloadavg() if hasattr(os, "getloadavg") else None
                cpu_before = read_process()["cpu_ns"]
                started = time.perf_counter_ns()
                with requests.post(
                    stateful_api.url + f"/tables/{table}/query",
                    json=qualification_query(query),
                    timeout=90,
                    stream=True,
                ) as response:
                    response.raw.decode_content = True
                    first = response.raw.read(1)
                    first_byte_ms = (time.perf_counter_ns() - started) / 1000000
                    body = first + response.raw.read()
                    elapsed = (time.perf_counter_ns() - started) / 1000000
                    assert response.status_code == 200, body
                result = json.loads(body)["responses"][0]
                first_result_ms = (time.perf_counter_ns() - started) / 1000000
                assert result["hits"]["total"]["value"] == count, result
                profile = qualification_verification(result)
                assert profile["documents"] == count, profile
                if profile.get("verification_metrics_version") == 1:
                    if os.environ.get("ANTFLY_BENCH_VALUE_SOURCE_SCALAR") != "1":
                        assert profile["source_batches"] > 0, profile
                        assert profile["source_scalar_reads"] < count, profile
                        if (
                            not os.environ.get("ANTFLY_BENCH_VALUE_SOURCE_ROWS")
                            and profile["batch_refusals"] == 0
                        ):
                            # The medium-source case must explore useful small
                            # batches, rather than silently forcing scalar I/O
                            # because its byte-derived cap is below 16 rows.
                            assert profile["source_batches"] >= 8, profile
                    expected_projected = (
                        name != "tiny"
                        and os.environ.get("ANTFLY_BENCH_VALUE_SOURCE_FULL_DOM") != "1"
                    )
                    assert profile["projected_sources"] == (
                        count
                        if expected_projected
                        else profile.get("stored_field_sources", 0)
                    ), profile
                cpu_after = read_process()["cpu_ns"]
                runs.append(
                    {
                        "trial": trial,
                        "host_load_average": host_load,
                        "http_ms": elapsed,
                        "first_byte_ms": first_byte_ms,
                        "first_result_ms": first_result_ms,
                        "cpu_ms": (cpu_after - cpu_before) / 1000000,
                        "profile": profile,
                    }
                )
        finally:
            stop.set()
            watcher.join()
        assert not sampling_errors, sampling_errors
        final_process = read_process()
        samples.append(final_process)
        sample_path = os.environ.get("ANTFLY_LOGICAL_SAMPLE")
        if sample_path and name == "unselected_arrays":
            sample_log = Path(sample_path).with_suffix(".log")
            with sample_log.open("w") as log:
                sampler = subprocess.Popen(
                    [
                        "/usr/bin/sample",
                        str(pid),
                        "5",
                        "1",
                        "-mayDie",
                        "-file",
                        sample_path,
                    ],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                deadline = time.monotonic() + 45
                while sampler.poll() is None and time.monotonic() < deadline:
                    response = requests.post(
                        stateful_api.url + f"/tables/{table}/query",
                        json=qualification_query(query),
                        timeout=90,
                    )
                    assert response.status_code == 200, response.text
                assert sampler.wait(timeout=15) == 0, sample_log.read_text()
        records.append(
            {
                "name": name,
                "documents": count,
                "corpus_sha256": hashlib.sha256(corpus).hexdigest(),
                "ingestion": {
                    "metrics_version": 1,
                    "scope": "End-to-end full_index batch writes; process CPU includes background work. RSS samples are batch endpoints, not an allocation or peak-memory bound.",
                    "http_ms": ingestion_elapsed / 1000000,
                    "cpu_ms": (ingestion_final["cpu_ns"] - ingestion_initial["cpu_ns"])
                    / 1000000,
                    "process_initial": ingestion_initial,
                    "process_final": ingestion_final,
                    "batches": ingestion_batches,
                },
                "idle_samples": idle_samples,
                "idle_cpu_ms_per_second": idle_cpu_ms_per_second,
                "warmup": warmup,
                "settling": settling,
                "median_http_ms": statistics.median(r["http_ms"] for r in runs),
                "median_first_result_ms": statistics.median(
                    r["first_result_ms"] for r in runs
                ),
                "mean_process_cpu_ms": sum(r["cpu_ms"] for r in runs) / len(runs),
                "verified_mib_per_second": runs[0]["profile"]["bytes"]
                / (1024 * 1024)
                / (statistics.median(r["http_ms"] for r in runs) / 1000),
                "process_initial": initial_process,
                "process_final": final_process,
                "sampled_rss_peak_bytes": max(
                    sample["rss_bytes"] for sample in samples
                ),
                "sampled_footprint_peak_bytes": max(
                    (
                        sample["footprint_bytes"]
                        for sample in samples
                        if sample["footprint_bytes"] is not None
                    ),
                    default=None,
                ),
                "memory_samples": samples,
                "runs": runs,
            }
        )
    binary = Path(os.environ["ANTFLY_BIN"])
    digest = hashlib.sha256()
    with binary.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    Path(destination).write_text(
        json.dumps(
            {
                "benchmark_protocol_version": 4,
                "benchmark_driver_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "settling_rule": "three one-second pre-ingestion idle CPU samples; recorded untimed query, then three consecutive one-second windows below median idle CPU plus 5% of one core and one CPU-accounting tick per window; deadline 30 s",
                "platform": platform.platform(),
                "machine": platform.machine(),
                "logical_cpus": os.cpu_count(),
                "cpu_affinity": sorted(os.sched_getaffinity(0))
                if hasattr(os, "sched_getaffinity")
                else None,
                "cgroup_cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
                if Path("/sys/fs/cgroup/cpu.max").is_file()
                else None,
                "controls": {
                    name: os.environ.get(name)
                    for name in (
                        "ANTFLY_BENCH_VALUE_SOURCE_SCALAR",
                        "ANTFLY_BENCH_VALUE_SOURCE_FULL_DOM",
                        "ANTFLY_BENCH_VALUE_SOURCE_ROWS",
                        "ANTFLY_BENCH_VALUE_SOURCE_FULL_READ",
                    )
                },
                "binary_sha256": digest.hexdigest(),
                "scope": "ReleaseFast server; fresh predicate membership per request, warm filesystem caches; shared host; HTTP includes profiling; exact-count first result requires the complete response; process CPU includes background work; RSS/physical footprint sampled every 5 ms, not an allocation bound",
                "records": records,
            },
            indent=2,
        )
        + "\n"
    )


def test_logical_stored_field_reads_preserve_updates_and_original_budgets(stateful_api):
    api = stateful_api
    table = f"logical_ranges_{time.time_ns()}"
    api.create_table(table)
    docs = {
        f"{i:04}": {
            "nested": [{"name": "needle"}, {"name": [None, "tail"]}],
            "unused": "x" * (32 * 1024),
        }
        for i in range(96)
    }
    api.batch_write(table, inserts=docs, sync_level="full_index")
    query = {
        "query": {"contains": "needle", "field": "nested.name"},
        "count": True,
        "limit": 0,
        "fields": [],
        "profile": True,
        "execution": {
            "value_verification": {
                "allow_scan": True,
                "max_documents": 96,
                "max_bytes": 8 * 1024 * 1024,
            }
        },
    }

    def check(expected):
        response = requests.post(
            api.url + f"/tables/{table}/query", json=query, timeout=60
        )
        assert response.status_code == 200, response.text
        result = response.json()["responses"][0]
        assert not result.get("error"), result
        assert result["hits"]["total"]["value"] == expected, result
        profile = result["profile"]["value_verification"]
        assert profile["documents"] == 96, profile
        assert profile["bytes"] > 96 * 32 * 1024, profile
        if os.environ.get("ANTFLY_BENCH_VALUE_SOURCE_FULL_READ") != "1":
            assert profile["stored_field_sources"] > 0, profile
        return result

    check(96)
    # Fresh mutation authority must replace the old directory atomically.
    api.batch_write(
        table,
        inserts={"0000": {"nested": [{"name": "other"}], "unused": "y" * (32 * 1024)}},
        sync_level="full_index",
    )
    check(95)
    if api.supports_restart:
        api.restart_server()
        check(95)
    small_budget = {
        **query,
        "execution": {
            "value_verification": {
                **query["execution"]["value_verification"],
                "max_bytes": 1024,
            }
        },
    }
    rejected = requests.post(
        api.url + f"/tables/{table}/query", json=small_budget, timeout=60
    )
    assert rejected.status_code >= 400 or rejected.json()["responses"][0].get(
        "error"
    ), rejected.text


def test_logical_source_concurrency_profile(stateful_api):
    """Opt-in reproducible cold-process and concurrent source-read qualification.

    Restart clears process caches; filesystem/storage caches remain uncontrolled.
    Run the same fixture with container CPU and memory constraints for controls.
    """
    import concurrent.futures
    import hashlib
    import platform
    import statistics
    import threading

    destination = os.environ.get("ANTFLY_LOGICAL_CONCURRENCY_REPORT")
    if not destination:
        pytest.skip("set ANTFLY_LOGICAL_CONCURRENCY_REPORT to qualify concurrent reads")
    api = stateful_api
    if not api.supports_restart:
        pytest.skip("requires an isolated locally managed server")
    clients = [
        int(v) for v in os.environ.get("ANTFLY_LOGICAL_CLIENTS", "1,4").split(",")
    ]
    assert clients and all(1 <= n <= 32 for n in clients)
    table = f"logical_concurrent_{time.time_ns()}"
    api.create_table(table)
    count = 512
    docs = {
        f"{i:06}": {"title": "needle alpha beta", "unused": ["\n" * 16] * 1024}
        for i in range(count)
    }
    corpus = json.dumps(docs, sort_keys=True, separators=(",", ":")).encode()
    for start in range(0, count, 64):
        ids = list(docs)[start : start + 64]
        api.batch_write(
            table, inserts={i: docs[i] for i in ids}, sync_level="full_index"
        )
    query = {
        "query": {
            "bool": {
                "should": [
                    {"contains": text, "field": "title"}
                    for text in ("needle", "alpha", "beta")
                ]
            }
        },
        "count": True,
        "limit": 0,
        "fields": [],
        "profile": True,
        "execution": {
            "value_verification": {
                "allow_scan": True,
                "max_documents": count,
                "max_bytes": 64 * 1024 * 1024,
            }
        },
    }

    def io_stats(pid):
        path = Path(f"/proc/{pid}/io")
        if not path.is_file():
            return None
        counters = dict(line.split(":", 1) for line in path.read_text().splitlines())
        return {
            name: int(counters[name])
            for name in ("rchar", "syscr", "read_bytes", "write_bytes")
        }

    def restart_for_measurement():
        if os.environ.get("ANTFLY_LOGICAL_EVICT_FILE_CACHE") != "1":
            api.restart_server()
            return {"mode": "process_restart"}
        assert platform.system() == "Linux" and hasattr(os, "posix_fadvise"), (
            "file-cache advice requires Linux"
        )
        server = api._server
        # Only the freshly allocated fixture root is eligible. Never advise a
        # caller-supplied data directory or follow links outside that root.
        root = Path(server.root).resolve()
        assert root == Path(server.tempdir.name).resolve()
        assert root.name.startswith("antfly-zig-")
        advice = {
            "mode": "process_restart_with_file_cache_advice",
            "advised_files": 0,
            "advised_file_bytes": 0,
            "errors": [],
        }
        api.pause_server()
        try:
            for path in root.rglob("*"):
                if path.is_symlink() or not path.is_file():
                    continue
                fd = os.open(path, os.O_RDONLY)
                try:
                    os.fsync(fd)
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                    advice["advised_files"] += 1
                    advice["advised_file_bytes"] += os.fstat(fd).st_size
                except OSError as error:
                    advice["errors"].append(
                        {"file": str(path.relative_to(root)), "errno": error.errno}
                    )
                finally:
                    os.close(fd)
        finally:
            api.resume_server()
        assert not advice["errors"], advice
        assert advice["advised_files"] > 0, advice
        # This is an advisory OS request. It cannot evict device caches and
        # startup may reload pages; actual process I/O is recorded below.
        return advice

    records = []
    for concurrency in clients:
        cache_advice = restart_for_measurement()
        pid = api._server.proc.pid
        read_process = logical_process_reader(pid)
        restart_io = io_stats(pid)

        def request(barrier=None):
            if barrier is not None:
                barrier.wait(timeout=30)
            started = time.perf_counter_ns()
            with requests.post(
                api.url + f"/tables/{table}/query",
                json=qualification_query(query),
                timeout=90,
                stream=True,
            ) as response:
                response.raw.decode_content = True
                first = response.raw.read(1)
                first_byte_ms = (time.perf_counter_ns() - started) / 1e6
                body = first + response.raw.read()
                assert response.status_code == 200, body
            result = json.loads(body)["responses"][0]
            assert not result.get("error"), result
            assert result["hits"]["total"]["value"] == count, result
            profile = qualification_verification(result)
            assert profile["documents"] == count, profile
            return {
                "http_ms": (time.perf_counter_ns() - started) / 1e6,
                "first_byte_ms": first_byte_ms,
                "profile": profile,
            }

        cold_before = read_process()
        cold_io_before = io_stats(pid)
        cold_process = request()
        cold_after = read_process()
        cold_process.update(
            {
                "cpu_ms": (cold_after["cpu_ns"] - cold_before["cpu_ns"]) / 1e6,
                "process_before": cold_before,
                "process_after": cold_after,
                "io_before": cold_io_before,
                "io_after": io_stats(pid),
            }
        )
        runs = []
        # Create client workers before timing; synchronize each request group.
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            for trial in range(9):
                barrier = threading.Barrier(concurrency + 1)
                futures = [pool.submit(request, barrier) for _ in range(concurrency)]
                host_load = os.getloadavg() if hasattr(os, "getloadavg") else None
                before = read_process()
                io_before = io_stats(pid)
                started = time.perf_counter_ns()
                barrier.wait(timeout=30)
                responses = [future.result(timeout=100) for future in futures]
                elapsed = (time.perf_counter_ns() - started) / 1e6
                after = read_process()
                runs.append(
                    {
                        "trial": trial,
                        "group_ms": elapsed,
                        "host_load_average": host_load,
                        "cpu_ms": (after["cpu_ns"] - before["cpu_ns"]) / 1e6,
                        "process_before": before,
                        "process_after": after,
                        "io_before": io_before,
                        "io_after": io_stats(pid),
                        "responses": responses,
                    }
                )
        latencies = sorted(r["http_ms"] for trial in runs for r in trial["responses"])
        records.append(
            {
                "clients": concurrency,
                "cache_advice": cache_advice,
                "restart_io": restart_io,
                "cold_process": cold_process,
                "runs": runs,
                "median_request_ms": statistics.median(latencies),
                "p95_request_ms": latencies[(95 * len(latencies) + 99) // 100 - 1],
                "mean_cpu_ms_per_request": statistics.mean(
                    r["cpu_ms"] / concurrency for r in runs
                ),
                "requests_per_second": 1000
                * concurrency
                / statistics.mean(r["group_ms"] for r in runs),
            }
        )
    binary = Path(os.environ["ANTFLY_BIN"])
    with binary.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    Path(destination).write_text(
        json.dumps(
            {
                "protocol": 1,
                "binary_sha256": digest,
                "platform": platform.platform(),
                "corpus_sha256": hashlib.sha256(corpus).hexdigest(),
                "documents": count,
                "logical_cpus": os.cpu_count(),
                "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
                if Path("/sys/fs/cgroup/cpu.max").is_file()
                else None,
                "memory_max": Path("/sys/fs/cgroup/memory.max").read_text().strip()
                if Path("/sys/fs/cgroup/memory.max").is_file()
                else None,
                "controls": {
                    k: os.environ.get(k)
                    for k in (
                        "ANTFLY_BENCH_VALUE_SOURCE_FULL_READ",
                        "ANTFLY_BENCH_VALUE_SOURCE_SCALAR",
                        "ANTFLY_BENCH_VALUE_SOURCE_ROWS",
                    )
                },
                "scope": "Same-host native ISA; cold process after restart. Optional file-cache advice is limited to fixture files and is not a guarantee of cold storage; startup may reload pages. Process I/O counters accompany trials. Process CPU includes background work; endpoint RSS samples are not a peak bound.",
                "records": records,
            },
            indent=2,
        )
        + "\n"
    )


def test_native_grep_nested_authority_keeps_retained_revisions(stateful_api, tmp_path):
    api = stateful_api
    table = f"grep_nested_authority_{time.time_ns()}"
    api.create_table(table)
    docs = {}
    for i in range(96):
        row = document(f"{i:03}.py", f"needle {i}\n")
        row["grep"]["regions"] = [{"start_line": 1, "end_line": 1}] * 1024
        docs[f"{i:03}"] = row
    api.batch_write(table, inserts=docs, sync_level="full_index")
    url = api.url + f"/tables/{table}/query"
    payload = {
        "query": {"contains": "needle", "field": "grep.content"},
        "limit": 1,
        "fields": [],
        "profile": True,
        "order_by": [{"field": "_id", "desc": False}],
        "execution": {
            "value_verification": {
                "allow_scan": True,
                "max_documents": 96,
                "max_bytes": 8 * 1024 * 1024,
            }
        },
    }
    first = requests.post(url, json=payload, timeout=60)
    assert first.status_code == 200, first.text
    original = first.json()["responses"][0]
    assert original["hits"]["total"]["value"] == 96, original
    profile = original["profile"]["value_verification"]
    if os.environ.get("ANTFLY_BENCH_VALUE_SOURCE_FULL_READ") != "1":
        assert profile["stored_field_sources"] > 0, profile
    result = run_grep(tmp_path, api.url, table, "--json", "needle", page_size=128)
    assert result.returncode == 0, result.stderr.decode()
    events = [json.loads(line) for line in result.stdout.splitlines()]
    assert [event["path"] for event in events if event["type"] == "match"] == [
        f"{i:03}.py" for i in range(96)
    ]
    assert events[-1]["stats"]["complete"] is True
    changed = document("000.py", "UPDATED\n")
    changed["grep"]["regions"] = [{"start_line": 1, "end_line": 1}] * 1024
    api.batch_write(table, inserts={"000": changed}, sync_level="full_index")
    # A fresh predicate forces source reads through the retained cut rather
    # than reusing the previous predicate's membership facts.
    old = {
        **payload,
        "query": {"contains": "UPDATED", "field": "grep.content"},
        "remote_snapshot": original["remote_snapshot"],
    }
    previous = requests.post(url, json=old, timeout=60)
    assert previous.status_code == 200, previous.text
    assert previous.json()["responses"][0]["hits"]["total"]["value"] == 0, previous.text
    fresh = dict(old)
    fresh.pop("remote_snapshot")
    current = requests.post(url, json=fresh, timeout=60)
    assert current.status_code == 200, current.text
    assert current.json()["responses"][0]["hits"]["total"]["value"] == 1, current.text


def logical_phase_offsets(server):
    if os.environ.get("ANTFLY_BENCH_LOGICAL_SOURCE_PHASES") != "1":
        return {}
    paths = (getattr(server, "log_path", None), getattr(server, "data_log_path", None))
    return {path: path.stat().st_size for path in paths if path is not None}


def logical_phase_records(offsets):
    import re

    records = []
    for path, offset in offsets.items():
        with path.open("rb") as stream:
            stream.seek(offset)
            lines = stream.read().decode("utf-8", errors="replace").splitlines()
        for line in lines:
            match = re.search(
                r"antfly_bench_(logical_sources|logical_source_lookup) (.*)", line
            )
            if match:
                values = {
                    key: int(value)
                    for key, value in re.findall(r"(\w+)=(\d+)", match[2])
                }
                records.append({"phase": match[1], **values})
    return records


def test_logical_retained_source_hint_profile(stateful_api):
    """Opt-in repeated reader calibration with distinct, uncached predicates."""
    import hashlib
    import platform

    report = os.environ.get("ANTFLY_LOGICAL_HINT_REPORT")
    if not report:
        pytest.skip("Set ANTFLY_LOGICAL_HINT_REPORT to capture provider calibration")
    words = [f"needle{i:02}" for i in range(12)]
    title = "seed " + " ".join(words) + " " + "x" * 1024
    documents = {f"{i:06}": {"title": title, "unused": "z" * 16384} for i in range(512)}
    table = f"logical_hint_profile_{time.time_ns()}"
    stateful_api.create_table(table)
    for start in range(0, len(documents), 128):
        keys = list(documents)[start : start + 128]
        stateful_api.batch_write(
            table,
            inserts={key: documents[key] for key in keys},
            sync_level="full_index",
        )
    url = stateful_api.url + f"/tables/{table}/query"
    policy = {"value_verification": {"allow_scan": True}}
    warm = requests.post(
        url,
        json=qualification_query(
            {
                "query": {"contains": "seed", "field": "title"},
                "execution": policy,
                "order_by": [{"field": "_id", "desc": False}],
                "limit": 1,
            }
        ),
        timeout=90,
    )
    assert warm.status_code == 200, warm.text
    snapshot = warm.json()["responses"][0]["remote_snapshot"]
    assert stateful_api._server is not None
    read_process = logical_process_reader(stateful_api._server.proc.pid)
    runs = []
    for word in words:
        phase_offsets = logical_phase_offsets(stateful_api._server)
        before = read_process()
        start = time.perf_counter_ns()
        response = requests.post(
            url,
            json=qualification_query(
                {
                    "query": {"contains": word, "field": "title"},
                    "execution": policy,
                    "remote_snapshot": snapshot,
                    "count": True,
                    "fields": [],
                    "limit": 0,
                    "profile": True,
                }
            ),
            timeout=90,
        )
        elapsed = (time.perf_counter_ns() - start) / 1_000_000
        after = read_process()
        assert response.status_code == 200, response.text
        result = response.json()["responses"][0]
        assert result["hits"]["total"]["value"] == len(documents), result
        profile = qualification_verification(result)
        # Every predicate is new: membership reuse must not make calibration
        # look cheaper by skipping the provider reads being measured.
        assert profile["documents"] == len(documents), profile
        runs.append(
            {
                "predicate": word,
                "http_ms": elapsed,
                "cpu_ms": (after["cpu_ns"] - before["cpu_ns"]) / 1_000_000,
                "process_before": before,
                "process_after": after,
                "profile": profile,
                "source_phase_probes": logical_phase_records(phase_offsets),
            }
        )
    with Path(os.environ["ANTFLY_BIN"]).open("rb") as binary:
        binary_sha256 = hashlib.file_digest(binary, "sha256").hexdigest()
    destination = Path(report)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(
            {
                "protocol": 1,
                "platform": platform.platform(),
                "binary_sha256": binary_sha256,
                "scope": "Warm retained source, distinct unindexed predicates; every measured request verifies the entire corpus. Latency includes HTTP and host contention. No predicate-hit speedup is included.",
                "records": [
                    {
                        "name": "retained_distinct_predicates",
                        "documents": len(documents),
                        "corpus_sha256": hashlib.sha256(
                            json.dumps(documents, sort_keys=True).encode()
                        ).hexdigest(),
                        "runs": runs,
                    }
                ],
            },
            separators=(",", ":"),
        )
        + "\n"
    )


def test_logical_retained_page_profile(stateful_api):
    """Opt-in warm membership pages, with real ID ordering and no source reads."""
    import hashlib
    import platform

    report = os.environ.get("ANTFLY_LOGICAL_PAGES_REPORT")
    if not report:
        pytest.skip("Set ANTFLY_LOGICAL_PAGES_REPORT to measure retained pages")
    count = 16384
    documents = {
        f"{i:06}": {
            "title": "needle" if i % 16 == 0 else "other",
            "dense": "needle",
            "unused": "payload",
        }
        for i in range(count)
    }
    table = f"logical_pages_{time.time_ns()}"
    stateful_api.create_table(table)
    keys = list(documents)
    for start in range(0, count, 512):
        stateful_api.batch_write(
            table,
            inserts={key: documents[key] for key in keys[start : start + 512]},
            sync_level="full_index",
        )
    url = stateful_api.url + f"/tables/{table}/query"
    base = {
        "query": {"contains": "needle", "field": "dense"},
        "execution": {"value_verification": {"allow_scan": True}},
        "order_by": [{"field": "_id", "desc": False}],
        "fields": [],
    }
    warm = requests.post(
        url, json=qualification_query({**base, "limit": 1}), timeout=90
    )
    assert warm.status_code == 200, warm.text
    snapshot = warm.json()["responses"][0]["remote_snapshot"]
    assert stateful_api._server is not None
    read_process = logical_process_reader(stateful_api._server.proc.pid)
    records = []
    for label, field, selected in (
        ("dense", "dense", keys),
        ("sparse", "title", keys[::16]),
    ):
        query_base = {**base, "query": {"contains": "needle", "field": field}}
        complete_body = {
            **query_base,
            "remote_snapshot": snapshot,
            "count": True,
            "limit": 0,
        }
        complete_body.pop("order_by")
        complete = requests.post(
            url, json=qualification_query(complete_body), timeout=90
        )
        assert complete.status_code == 200, complete.text
        assert complete.json()["responses"][0]["hits"]["total"]["value"] == len(
            selected
        ), complete.text
        runs = []
        for _ in range(3):
            for offset in (0, len(selected) // 2, len(selected) - 64):
                body = {
                    **query_base,
                    "remote_snapshot": snapshot,
                    "profile": True,
                    "limit": 64,
                }
                if offset:
                    body["search_after"] = [selected[offset - 1]]
                phase_offsets = logical_phase_offsets(stateful_api._server)
                before = read_process()
                started = time.perf_counter_ns()
                response = requests.post(
                    url, json=qualification_query(body), timeout=90
                )
                elapsed = (time.perf_counter_ns() - started) / 1_000_000
                after = read_process()
                assert response.status_code == 200, response.text
                result = response.json()["responses"][0]
                assert [hit["_id"] for hit in result["hits"]["hits"]] == selected[
                    offset : offset + 64
                ], result
                profile = qualification_verification(result)
                assert profile["documents"] == 0, profile
                runs.append(
                    {
                        "offset": offset,
                        "http_ms": elapsed,
                        "cpu_ms": (after["cpu_ns"] - before["cpu_ns"]) / 1_000_000,
                        "process_before": before,
                        "process_after": after,
                        "profile": profile,
                        "source_phase_probes": logical_phase_records(phase_offsets),
                    }
                )
        records.append(
            {
                "name": "retained_pages_" + label,
                "documents": count,
                "matches": len(selected),
                "runs": runs,
            }
        )
    with Path(os.environ["ANTFLY_BIN"]).open("rb") as stream:
        binary_sha = hashlib.file_digest(stream, "sha256").hexdigest()
    destination = Path(report)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(
            {
                "protocol": 1,
                "platform": platform.platform(),
                "binary_sha256": binary_sha,
                "scope": "Warm retained complete membership, public ID ordering, three page positions. Every measured request must verify zero source documents. Phase-instrumented trials are separate from ordinary latency qualification.",
                "corpus_sha256": hashlib.sha256(
                    json.dumps(documents, sort_keys=True).encode()
                ).hexdigest(),
                "records": records,
            },
            separators=(",", ":"),
        )
        + "\n"
    )


@pytest.mark.parametrize("change", [None, "revision", "permissions"])
@pytest.mark.parametrize("parents", [1, 8])
def test_native_grep_visual_point_reads_reauthorize_without_retained_snapshot(
    stateful_api, tmp_path, change, parents
):
    """Real query server; only inference is a fixed-answer local fixture."""
    import hashlib

    image = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7S8AAAAASUVORK5CYII="
    )
    table = f"grep_visual_{time.time_ns()}"
    stateful_api.create_table(table)
    item = document("Drive/chart.png", "")
    item["grep"].update(
        version=2,
        evidence=[
            {
                "id": "image",
                "kind": "image",
                "asset": {
                    "field": "payloads.image",
                    "sha256": hashlib.sha256(image).hexdigest(),
                    "mime_type": "image/png",
                    "byte_length": len(image),
                    "recipe": "original-v1",
                },
            }
        ],
    )
    item["payloads"] = {
        "image": "data:image/png;base64," + base64.b64encode(image).decode(),
        "unrelated": "x" * 262144,
    }
    import copy

    identities = [f"source-{i}" for i in range(parents)]
    documents = {identity: copy.deepcopy(item) for identity in identities}
    for i, value in enumerate(documents.values()):
        value["grep"]["path"] = f"Drive/chart{i}.png"
    item = documents[identities[0]]
    stateful_api.batch_write(table, inserts=documents, sync_level="full_index")
    seen, responses, provider_calls = [], [], []

    class QueryProxy(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append(body)
            response = requests.post(
                stateful_api.url + f"/tables/{table}/query", json=body, timeout=60
            )
            try:
                responses.append(response.json())
            except ValueError:
                responses.append(
                    {
                        "http_status": response.status_code,
                        "body": response.text,
                        "logs": stateful_api.debug_logs()[-6000:],
                    }
                )
            if len(seen) == 1 and change:
                if change == "revision":
                    item["grep"]["revision"] = "new-revision"
                else:
                    item["grep"]["permissions"] = {"public": False, "readers": []}
                stateful_api.batch_write(
                    table, inserts={identities[0]: item}, sync_level="full_index"
                )
            self.send_response(response.status_code)
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

    class Decisions(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            provider_calls.append(body)
            data = [
                {
                    "input_index": i,
                    "id": entry["id"],
                    "answers": [
                        {"type": "predicate", "name": q["name"], "probability": 0.9}
                        for q in body["questions"]
                    ],
                }
                for i, entry in enumerate(body["inputs"])
            ]
            payload = json.dumps({"data": data}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    query = ThreadingHTTPServer(("127.0.0.1", 0), QueryProxy)
    provider = ThreadingHTTPServer(("127.0.0.1", 0), Decisions)
    threads = [start_http_server(query), start_http_server(provider)]
    try:
        result = run_grep(
            tmp_path,
            f"http://127.0.0.1:{query.server_port}",
            table,
            "--media=images",
            "--ask=Does this contain a chart?",
            "--model=fixture",
            "--endpoint",
            f"http://127.0.0.1:{provider.server_port}/decisions",
            "--no-cache",
            "--json",
            "--decision-concurrency=8",
            "--decision-batch-wait-ms=20",
            page_size=16,
        )
        assert len(seen) == 2, (seen, result.stderr.decode())
        assert seen[0]["order_by"] == [{"field": "_id", "desc": False}]
        assert responses[0]["responses"][0]["remote_snapshot"]
        point = seen[1]
        assert point["query"] == {"bool": {"filter": {"ids": identities}}}
        assert "execution" not in point
        assert not {"order_by", "search_after", "remote_snapshot"} & point.keys()
        assert set(point["fields"]) == {
            "grep.revision",
            "grep.permissions",
            "payloads.image",
        }
        assert "responses" in responses[1], responses[1]
        assert not responses[1]["responses"][0].get("remote_snapshot")
        if change:
            assert result.returncode == 2, result.stdout
            assert "Drive/chart0.png" not in json.dumps(provider_calls)
            assert sum(len(call["inputs"]) for call in provider_calls) == parents - 1
            expected = (
                b"EvidenceRevisionChanged"
                if change == "revision"
                else b"IndexAuthorizationFailed"
            )
            assert expected in result.stderr
        else:
            assert result.returncode == 0, result.stderr.decode()
            assert len(provider_calls) == 1
            assert len(provider_calls[0]["inputs"]) == parents
            assert json.loads(result.stdout.splitlines()[0])["type"] == "evidence"
    finally:
        for server, thread in zip((query, provider), threads, strict=True):
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()
