# Copyright 2026 Antfly, Inc.
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

from __future__ import annotations

import base64
import json
import re
import signal
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import pytest
import requests

from conftest import (
    DEFAULT_ANTFLY_BIN,
    StatefulAntflyServer,
    _swarm_stateful_command,
    _read_log_tail,
    antfly_public_api_url,
    find_free_port,
    lookup_key_path,
    maybe_preserve_tempdir,
    raise_request_error_with_logs,
    resolve_binary_path,
    wait_for_server,
)

AUTH_PUBLIC_API_ROOT = "/auth/v1"


def _basic_auth(username: str, password: str) -> str:
    raw = f"{username}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _wait_for_auth_server(url: str, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            resp = requests.get(f"{url}/status", timeout=2)
            if resp.status_code in (200, 401):
                return True
        except requests.ConnectionError:
            pass
        time.sleep(0.25)
    return False


def _wait_until(predicate, timeout: float = 30.0, interval: float = 0.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


def _try_lookup(api: "AuthApi", table_name: str, key: str):
    try:
        return api.lookup_key(table_name, key)
    except requests.RequestException:
        return None


class AuthApi:
    def __init__(self, base_url: str, server_ref: "SwarmAuthServer | SplitAuthServer"):
        self.url = base_url.rstrip("/")
        self.auth_url = self._auth_url_from_db_url(self.url)
        self.s = requests.Session()
        self.s.headers["Content-Type"] = "application/json"
        self.s.headers["Connection"] = "close"
        self._server = server_ref
        self._request_lock = threading.Lock()

    @staticmethod
    def _auth_url_from_db_url(db_url: str) -> str:
        rootless = db_url.removesuffix("/db/v1")
        return f"{rootless}{AUTH_PUBLIC_API_ROOT}"

    def _url_for(self, path: str) -> str:
        if path == AUTH_PUBLIC_API_ROOT:
            return self.auth_url
        if path.startswith(f"{AUTH_PUBLIC_API_ROOT}/"):
            return f"{self.auth_url}{path[len(AUTH_PUBLIC_API_ROOT):]}"
        return f"{self.url}{path}"

    def _check(self, response: requests.Response):
        if response.status_code >= 400:
            logs = self._server.debug_logs().strip()
            body = response.text.strip()
            if logs:
                raise requests.HTTPError(
                    f"{response.status_code} {response.reason} for url: {response.url} body={body}\nserver logs:\n{logs}",
                    response=response,
                )
            raise requests.HTTPError(
                f"{response.status_code} {response.reason} for url: {response.url} body={body}",
                response=response,
            )
        if not response.content:
            return {}
        return response.json()

    def get(self, path: str):
        with self._request_lock:
            try:
                response = self.s.get(self._url_for(path), timeout=30)
            except requests.RequestException as err:
                raise_request_error_with_logs(err, self._server)
            return self._check(response)

    def post(self, path: str, payload: dict):
        with self._request_lock:
            try:
                response = self.s.post(self._url_for(path), json=payload, timeout=30)
            except requests.RequestException as err:
                raise_request_error_with_logs(err, self._server)
            return self._check(response)

    def put(self, path: str, payload: dict):
        with self._request_lock:
            try:
                response = self.s.put(self._url_for(path), json=payload, timeout=30)
            except requests.RequestException as err:
                raise_request_error_with_logs(err, self._server)
            return self._check(response)

    def delete(self, path: str):
        with self._request_lock:
            try:
                response = self.s.delete(self._url_for(path), timeout=30)
            except requests.RequestException as err:
                raise_request_error_with_logs(err, self._server)
            return self._check(response)

    def create_table(self, table_name: str, payload: dict | None = None):
        body = payload or {"num_shards": 1}
        deadline = time.monotonic() + 5.0
        while True:
            try:
                with self._request_lock:
                    response = self.s.post(f"{self.url}/tables/{table_name}", json=body, timeout=30)
            except requests.RequestException:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
                continue
            if response.status_code not in (404, 500):
                return self._check(response)
            if time.monotonic() >= deadline:
                return self._check(response)
            time.sleep(0.1)

    def batch_write(self, table_name: str, payload: dict):
        return self.post(f"/tables/{table_name}/batch", payload)

    def lookup_key(self, table_name: str, key: str):
        return self.get(lookup_key_path(table_name, key))

    def scan_keys(self, table_name: str, payload: dict) -> list[dict]:
        response = self.s.post(f"{self.url}/tables/{table_name}/lookup", json=payload, timeout=30)
        if response.status_code >= 400:
            self._check(response)
        if not response.content:
            return []
        return [json.loads(line) for line in response.text.splitlines() if line.strip()]


class SwarmAuthServer:
    def __init__(self, binary: str, host: str, port: int):
        self.url = f"http://{host}:{port}"
        self.api_url = antfly_public_api_url(self.url)
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-auth-e2e-")
        root = Path(self.tempdir.name)
        self.root = root
        self.log_path = root / "server.log"
        self.log_file = self.log_path.open("w")
        command = _swarm_stateful_command(binary, host=host, port=port, root=root)
        command.extend(["--auth", "true"])
        self.proc = subprocess.Popen(
            command,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            cwd=root,
        )
        if not wait_for_server(self.api_url, allow_unauthorized=True):
            self.stop()
            out = _read_log_tail(self.log_path)
            raise RuntimeError(f"Auth swarm failed to start at {self.api_url}\n{out}")
        self.metadata_admin_url = self._poll_metadata_admin_url()

    def debug_logs(self) -> str:
        self.log_file.flush()
        return _read_log_tail(self.log_path)

    def _poll_metadata_admin_url(self) -> str:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            logs = _read_log_tail(self.log_path)
            matches = re.findall(r"(?:swarm )?metadata admin api listening on (http://[^\s]+)", logs)
            if matches:
                return matches[-1].rstrip("/")
            time.sleep(0.1)
        return ""

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log_file.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


class SplitAuthServer:
    def __init__(self, binary: str, host: str, port: int):
        self._server = StatefulAntflyServer(binary, host, port, auth_enabled=True)
        self.url = self._server.url
        self.api_url = self._server.api_url

    def debug_logs(self) -> str:
        return self._server.debug_logs()

    def stop(self) -> None:
        self._server.stop()


@pytest.fixture
def auth_api():
    binary = resolve_binary_path(str(DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")
    if Path(binary).name != "antfly":
        pytest.skip("auth parity requires the unified antfly CLI")

    port = find_free_port()
    server = SwarmAuthServer(binary, "127.0.0.1", port)
    try:
        yield AuthApi(server.api_url, server)
    finally:
        server.stop()


@pytest.fixture
def stateful_auth_api():
    binary = resolve_binary_path(str(DEFAULT_ANTFLY_BIN))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")
    if Path(binary).name != "antfly":
        pytest.skip("auth parity requires the unified antfly CLI")

    port = find_free_port()
    server = SplitAuthServer(binary, "127.0.0.1", port)
    try:
        yield AuthApi(server.api_url, server)
    finally:
        server.stop()


def test_swarm_auth_defaults_to_local_admin_user(auth_api: AuthApi):
    response = auth_api.s.get(f"{auth_api.url}/status", timeout=30)
    assert response.status_code == 401

    auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    me = auth_api.get("/auth/v1/me")
    assert me["username"] == "admin"
    assert any(
        permission["resource_type"] == "*" and permission["resource"] == "*" and permission["type"] == "admin"
        for permission in me["permissions"]
    )


def test_swarm_auth_user_and_api_key_flow(auth_api: AuthApi):
    auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")

    created = auth_api.post(
        "/auth/v1/users/alice",
        {
            "password": "secret",
            "initial_policies": [
                {
                    "resource": "docs",
                    "resource_type": "table",
                    "type": "read",
                }
            ],
        },
    )
    assert created["username"] == "alice"

    row_filter = auth_api.put("/auth/v1/users/alice/row-filters/docs", {"term": {"tier": "gold"}})
    assert row_filter["table"] == "docs"
    assert row_filter["filter"]["term"]["tier"] == "gold"

    api_key = auth_api.post("/auth/v1/users/alice/api-keys", {"name": "ci"})
    assert api_key["username"] == "alice"
    assert api_key["key_secret"]
    assert api_key["encoded"]

    subjects = auth_api.get("/auth/v1/subjects")
    assert any(subject["subject"] == "alice" and subject["kind"] == "user" for subject in subjects)

    auth_api.s.headers["Authorization"] = f"Bearer {api_key['encoded']}"
    me = auth_api.get("/auth/v1/me")
    assert me["username"] == "alice"
    assert any(
        permission["resource_type"] == "table" and permission["resource"] == "docs" and permission["type"] == "read"
        for permission in me["permissions"]
    )


def test_swarm_auth_api_keys_follow_owner_permissions(auth_api: AuthApi):
    auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    auth_api.create_table("docs")
    auth_api.batch_write(
        "docs",
        {
            "inserts": {
                "doc-1": {
                    "title": "hello",
                    "body": "world",
                }
            },
            "sync_level": "write",
        },
    )

    auth_api.post("/auth/v1/users/alice", {"password": "password123"})
    auth_api.post(
        "/auth/v1/users/alice/permissions",
        {
            "resource": "*",
            "resource_type": "table",
            "type": "read",
        },
    )
    auth_api.post(
        "/auth/v1/users/alice/permissions",
        {
            "resource": "docs",
            "resource_type": "table",
            "type": "write",
        },
    )

    full_key = auth_api.post("/auth/v1/users/alice/api-keys", {"name": "full-access key"})
    assert full_key["username"] == "alice"
    assert full_key["encoded"]

    auth_api.s.headers["Authorization"] = f"ApiKey {full_key['encoded']}"
    assert auth_api.get("/status")["auth_enabled"] is True
    tables = auth_api.get("/tables")
    assert any(table["name"] == "docs" for table in tables)

    auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    read_only_key = auth_api.post(
        "/auth/v1/users/alice/api-keys",
        {
            "name": "read-only key",
            "permissions": [
                {
                    "resource": "docs",
                    "resource_type": "table",
                    "type": "read",
                }
            ],
        },
    )
    assert len(read_only_key["permissions"]) == 1

    auth_api.s.headers["Authorization"] = f"Bearer {read_only_key['encoded']}"
    assert auth_api.get("/status")["auth_enabled"] is True
    found = _wait_until(lambda: _try_lookup(auth_api, "docs", "doc-1"))
    assert found is not None
    assert found["title"] == "hello"

    write_resp = auth_api.s.post(
        f"{auth_api.url}/tables/docs/batch",
        json={"inserts": {"doc-2": {"title": "blocked"}}},
        timeout=30,
    )
    assert write_resp.status_code == 403

    tables_resp = auth_api.s.get(f"{auth_api.url}/tables", timeout=30)
    assert tables_resp.status_code == 403

    auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    escalated = auth_api.s.post(
        f"{auth_api.auth_url}/users/alice/api-keys",
        json={
            "name": "escalated key",
            "permissions": [
                {
                    "resource": "*",
                    "resource_type": "*",
                    "type": "admin",
                }
            ],
        },
        timeout=30,
    )
    assert escalated.status_code == 403

    fake_encoded = base64.b64encode(b"fakeid:fakesecret").decode("ascii")
    auth_api.s.headers["Authorization"] = f"ApiKey {fake_encoded}"
    invalid = auth_api.s.get(f"{auth_api.url}/status", timeout=30)
    assert invalid.status_code == 401

    auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    keys = auth_api.get("/auth/v1/users/alice/api-keys")
    assert len(keys) == 2
    auth_api.delete(f"/auth/v1/users/alice/api-keys/{full_key['key_id']}")

    auth_api.s.headers["Authorization"] = f"ApiKey {full_key['encoded']}"
    deleted = auth_api.s.get(f"{auth_api.url}/status", timeout=30)
    assert deleted.status_code == 401

    auth_api.s.headers["Authorization"] = f"ApiKey {read_only_key['encoded']}"
    assert auth_api.get("/status")["auth_enabled"] is True

    auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    remaining = auth_api.get("/auth/v1/users/alice/api-keys")
    assert len(remaining) == 1
    assert remaining[0]["key_id"] == read_only_key["key_id"]


def test_swarm_auth_enforces_row_filters_on_lookup_and_query(auth_api: AuthApi):
    auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    auth_api.create_table("docs")
    auth_api.batch_write(
        "docs",
        {
            "inserts": {
                "doc:gold": {
                    "title": "gold doc",
                    "body": "visible body",
                    "tier": "gold",
                },
                "doc:silver": {
                    "title": "silver doc",
                    "body": "hidden body",
                    "tier": "silver",
                },
            },
            "sync_level": "full_text",
        },
    )
    auth_api.post(
        "/auth/v1/users/reader",
        {
            "password": "reader",
            "initial_policies": [
                {
                    "resource": "docs",
                    "resource_type": "table",
                    "type": "read",
                }
            ],
        },
    )
    auth_api.put("/auth/v1/users/reader/row-filters/docs", {"term": {"tier": "gold"}})

    auth_api.s.headers["Authorization"] = _basic_auth("reader", "reader")

    visible = _wait_until(lambda: _try_lookup(auth_api, "docs", "doc:gold"))
    assert visible["title"] == "gold doc"

    hidden_lookup = auth_api.s.get(f"{auth_api.url}/tables/docs/lookup/doc:silver", timeout=30)
    assert hidden_lookup.status_code == 404

    scan_result = _wait_until(
        lambda: auth_api.scan_keys(
            "docs",
            {
                "from": "doc:",
                "to": "doc;",
                "inclusive_from": True,
                "fields": ["title", "tier"],
            },
        )
    )
    assert scan_result is not None
    assert [entry["key"] for entry in scan_result] == ["doc:gold"]
    assert scan_result[0]["tier"] == "gold"
    assert scan_result[0]["title"] == "gold doc"

    query_result = _wait_until(
        lambda: auth_api.post(
            "/tables/docs/query",
            {
                "full_text_search": {"match_all": {}},
                "limit": 10,
            },
        )
    )
    assert query_result is not None
    hits = query_result["responses"][0]["hits"]["hits"]
    assert [hit["_id"] for hit in hits] == ["doc:gold"]


def test_stateful_auth_defaults_to_local_admin_user(stateful_auth_api: AuthApi):
    response = stateful_auth_api.s.get(f"{stateful_auth_api.url}/status", timeout=30)
    assert response.status_code == 401

    stateful_auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    me = stateful_auth_api.get("/auth/v1/me")
    assert me["username"] == "admin"
    assert any(
        permission["resource_type"] == "*" and permission["resource"] == "*" and permission["type"] == "admin"
        for permission in me["permissions"]
    )


def test_stateful_auth_enforces_table_permissions(stateful_auth_api: AuthApi):
    stateful_auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    stateful_auth_api.create_table("docs")
    stateful_auth_api.batch_write(
        "docs",
        {
            "inserts": {
                "doc-1": {
                    "title": "hello",
                    "body": "world",
                }
            },
            "sync_level": "write",
        },
    )
    stateful_auth_api.post(
        "/auth/v1/users/reader",
        {
            "password": "reader",
            "initial_policies": [
                {
                    "resource": "docs",
                    "resource_type": "table",
                    "type": "read",
                }
            ],
        },
    )

    stateful_auth_api.s.headers["Authorization"] = _basic_auth("reader", "reader")
    found = _wait_until(lambda: _try_lookup(stateful_auth_api, "docs", "doc-1"))
    assert found is not None
    assert found["title"] == "hello"

    write_resp = stateful_auth_api.s.post(
        f"{stateful_auth_api.url}/tables/docs/batch",
        json={"inserts": {"doc-2": {"title": "blocked"}}},
        timeout=30,
    )
    assert write_resp.status_code == 403

    tables_resp = stateful_auth_api.s.get(f"{stateful_auth_api.url}/tables", timeout=30)
    assert tables_resp.status_code == 403

    admin_resp = stateful_auth_api.s.get(f"{stateful_auth_api.auth_url}/users", timeout=30)
    assert admin_resp.status_code == 403


def test_stateful_auth_enforces_row_filters_on_lookup_and_query(stateful_auth_api: AuthApi):
    stateful_auth_api.s.headers["Authorization"] = _basic_auth("admin", "admin")
    stateful_auth_api.create_table("docs")
    stateful_auth_api.batch_write(
        "docs",
        {
            "inserts": {
                "doc:gold": {
                    "title": "gold doc",
                    "body": "visible body",
                    "tier": "gold",
                },
                "doc:silver": {
                    "title": "silver doc",
                    "body": "hidden body",
                    "tier": "silver",
                },
            },
            "sync_level": "full_text",
        },
    )
    stateful_auth_api.post(
        "/auth/v1/users/reader",
        {
            "password": "reader",
            "initial_policies": [
                {
                    "resource": "docs",
                    "resource_type": "table",
                    "type": "read",
                }
            ],
        },
    )
    stateful_auth_api.put("/auth/v1/users/reader/row-filters/docs", {"term": {"tier": "gold"}})

    stateful_auth_api.s.headers["Authorization"] = _basic_auth("reader", "reader")

    visible = _wait_until(lambda: _try_lookup(stateful_auth_api, "docs", "doc:gold"))
    assert visible["title"] == "gold doc"

    hidden_lookup = stateful_auth_api.s.get(f"{stateful_auth_api.url}/tables/docs/lookup/doc:silver", timeout=30)
    assert hidden_lookup.status_code == 404

    scan_result = _wait_until(
        lambda: stateful_auth_api.scan_keys(
            "docs",
            {
                "from": "doc:",
                "to": "doc;",
                "inclusive_from": True,
                "fields": ["title", "tier"],
            },
        )
    )
    assert scan_result is not None
    assert [entry["key"] for entry in scan_result] == ["doc:gold"]
    assert scan_result[0]["tier"] == "gold"
    assert scan_result[0]["title"] == "gold doc"

    query_result = _wait_until(
        lambda: stateful_auth_api.post(
            "/tables/docs/query",
            {
                "full_text_search": {"match_all": {}},
                "limit": 10,
            },
        )
    )
    assert query_result is not None
    hits = query_result["responses"][0]["hits"]["hits"]
    assert [hit["_id"] for hit in hits] == ["doc:gold"]

    filtered_query = stateful_auth_api.post(
        "/tables/docs/query",
        {
            "full_text_search": {"match_all": {}},
            "filter_query": {"term": {"tier": "silver"}},
            "limit": 10,
        },
    )
    assert filtered_query["responses"][0]["hits"]["total"] == 0
