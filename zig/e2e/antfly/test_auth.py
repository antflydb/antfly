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

from __future__ import annotations

import base64
import json
import os
import re
import signal
import subprocess
import tempfile
import threading
import time
from contextlib import ExitStack
from pathlib import Path
from urllib.parse import urlencode

import pytest
import requests
from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StatefulAntflyServer,
    _read_log_tail,
    _standalone_stateful_command,
    antfly_public_api_url,
    lookup_key_path,
    maybe_preserve_tempdir,
    raise_if_server_process_exited,
    raise_request_error_with_logs,
    resolve_binary_path,
    wait_for_server,
)
from port_reservations import LoopbackPortReservations, find_free_port

AUTH_PUBLIC_API_ROOT = "/auth/v1"
AUTH_STARTUP_TIMEOUT_SECONDS = 30.0
AUTH_SETUP_RETRY_TIMEOUT_SECONDS = 30.0


def _basic_auth(username: str, password: str) -> str:
    raw = f"{username}:{password}".encode()
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


def _wait_for_admin_auth(
    auth_url: str, timeout: float = AUTH_STARTUP_TIMEOUT_SECONDS
) -> bool:
    deadline = time.monotonic() + timeout
    session = requests.Session()
    session.headers["Connection"] = "close"
    session.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    while time.monotonic() < deadline:
        try:
            # Password verification in a Debug build can exceed two seconds.
            # Give one verification the remaining setup budget; repeatedly
            # timing it out only starts more expensive verification work.
            request_timeout = max(0.1, deadline - time.monotonic())
            response = session.get(f"{auth_url}/me", timeout=request_timeout)
            if (
                response.status_code == 200
                and response.json().get("username") == "admin"
            ):
                return True
        except (ValueError, requests.RequestException):
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


def _try_lookup(api: AuthApi, table_name: str, key: str):
    try:
        return api.lookup_key(table_name, key)
    except requests.HTTPError as err:
        if err.response is not None and err.response.status_code == 404:
            return None
        raise
    except requests.RequestException:
        return None


class AuthApi:
    def __init__(
        self, base_url: str, server_ref: StandaloneAuthServer | SplitAuthServer
    ):
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
            return f"{self.auth_url}{path[len(AUTH_PUBLIC_API_ROOT) :]}"
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
        deadline = time.monotonic() + 5.0
        with self._request_lock:
            while True:
                raise_if_server_process_exited(self._server)
                try:
                    response = self.s.get(self._url_for(path), timeout=30)
                    break
                except requests.RequestException as err:
                    if time.monotonic() >= deadline:
                        raise_request_error_with_logs(err, self._server)
                    time.sleep(0.1)
            return self._check(response)

    def post(self, path: str, payload: dict):
        with self._request_lock:
            raise_if_server_process_exited(self._server)
            try:
                response = self.s.post(self._url_for(path), json=payload, timeout=30)
            except requests.RequestException as err:
                raise_request_error_with_logs(err, self._server)
            return self._check(response)

    def put(self, path: str, payload: dict):
        with self._request_lock:
            raise_if_server_process_exited(self._server)
            try:
                response = self.s.put(self._url_for(path), json=payload, timeout=30)
            except requests.RequestException as err:
                raise_request_error_with_logs(err, self._server)
            return self._check(response)

    def delete(self, path: str):
        with self._request_lock:
            raise_if_server_process_exited(self._server)
            try:
                response = self.s.delete(self._url_for(path), timeout=30)
            except requests.RequestException as err:
                raise_request_error_with_logs(err, self._server)
            return self._check(response)

    def request_raw(self, method: str, path: str, **kwargs) -> requests.Response:
        """Issue a request whose non-2xx response is part of the assertion contract."""
        with self._request_lock:
            raise_if_server_process_exited(self._server)
            try:
                return self.s.request(method, self._url_for(path), **kwargs)
            except requests.RequestException as err:
                raise_request_error_with_logs(err, self._server)

    def create_table(self, table_name: str, payload: dict | None = None):
        body = payload or {"num_shards": 1}
        deadline = time.monotonic() + AUTH_SETUP_RETRY_TIMEOUT_SECONDS
        while True:
            raise_if_server_process_exited(self._server)
            try:
                with self._request_lock:
                    response = self.s.post(
                        f"{self.url}/tables/{table_name}", json=body, timeout=30
                    )
            except requests.RequestException as err:
                if time.monotonic() >= deadline:
                    raise_request_error_with_logs(err, self._server)
                time.sleep(0.1)
                continue
            if response.status_code not in (404, 500, 503):
                return self._check(response)
            if time.monotonic() >= deadline:
                return self._check(response)
            time.sleep(0.1)

    def batch_write(self, table_name: str, payload: dict):
        return self.post(f"/tables/{table_name}/batch", payload)

    def lookup_key(self, table_name: str, key: str):
        return self.get(lookup_key_path(table_name, key))

    def scan_keys(self, table_name: str, payload: dict) -> list[dict]:
        raise_if_server_process_exited(self._server)
        response = self.s.post(
            f"{self.url}/tables/{table_name}/documents", json=payload, timeout=30
        )
        if response.status_code >= 400:
            self._check(response)
        if not response.content:
            return []
        return [json.loads(line) for line in response.text.splitlines() if line.strip()]


class StandaloneAuthServer:
    def __init__(
        self, binary: str, host: str, port: int, *, storage_engine: str | None = None
    ):
        with ExitStack() as setup:
            self.port_reservations = LoopbackPortReservations(host)
            setup.callback(self.port_reservations.close)
            port = self.port_reservations.reserve_requested(port)
            self.url = f"http://{host}:{port}"
            self.api_url = antfly_public_api_url(self.url)
            self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-auth-e2e-")
            setup.callback(self.tempdir.cleanup)
            root = Path(self.tempdir.name)
            self.root = root
            self.log_path = root / "server.log"
            self.log_file = setup.enter_context(self.log_path.open("w"))
            command = _standalone_stateful_command(
                binary, host=host, port=port, root=root
            )
            command.extend(["--auth", "true"])
            if storage_engine is not None:
                command.extend(["--storage-engine", storage_engine])
                if storage_engine == "lite":
                    command.extend(["--storage-path", str(root / "db.aflite")])
            self.proc: subprocess.Popen[str] | None = None
            setup.pop_all()
        try:
            self.proc = self.port_reservations.handoff_to(
                (port,),
                lambda: subprocess.Popen(
                    command,
                    env={
                        **os.environ,
                        "ANTFLY_BOOTSTRAP_ADMIN_PASSWORD": AUTH_BOOTSTRAP_PASSWORD,
                    },
                    stdout=self.log_file,
                    stderr=subprocess.STDOUT,
                    cwd=root,
                ),
            )
        except BaseException:
            self.stop()
            raise
        if not wait_for_server(
            self.api_url, allow_unauthorized=True, processes=[("server", self.proc)]
        ):
            self.stop()
            out = _read_log_tail(self.log_path)
            raise RuntimeError(
                f"Auth standalone failed to start at {self.api_url}\n{out}"
            )
        self.metadata_admin_url = self._poll_metadata_admin_url()
        if not _wait_for_admin_auth(AuthApi._auth_url_from_db_url(self.api_url)):
            out = self.debug_logs()
            self.stop()
            raise RuntimeError(
                f"Auth standalone failed to initialize admin auth at {self.api_url}\n{out}"
            )

    def debug_logs(self) -> str:
        self.log_file.flush()
        return _read_log_tail(self.log_path)

    def _poll_metadata_admin_url(self) -> str:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            logs = _read_log_tail(self.log_path)
            matches = re.findall(
                r"(?:standalone )?metadata admin api listening on (http://[^\s]+)", logs
            )
            if matches:
                return matches[-1].rstrip("/")
            time.sleep(0.1)
        return ""

    def stop(self) -> None:
        self.port_reservations.close()
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
        if not _wait_for_admin_auth(AuthApi._auth_url_from_db_url(self.api_url)):
            out = self.debug_logs()
            self.stop()
            raise RuntimeError(
                f"Stateful auth server failed to initialize admin auth at {self.api_url}\n{out}"
            )

    def debug_logs(self) -> str:
        return self._server.debug_logs()

    def stop(self) -> None:
        self._server.stop()


@pytest.fixture
def auth_api():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")
    if Path(binary).name != "antfly":
        pytest.skip("auth parity requires the unified antfly CLI")

    port = find_free_port()
    server = StandaloneAuthServer(binary, "127.0.0.1", port)
    try:
        yield AuthApi(server.api_url, server)
    finally:
        server.stop()


@pytest.fixture
def stateful_auth_api():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
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


def test_standalone_auth_uses_configured_admin_password(auth_api: AuthApi):
    rejected = auth_api.s.get(
        f"{auth_api.auth_url}/me",
        headers={"Authorization": _basic_auth("admin", "admin")},
        timeout=30,
    )
    assert rejected.status_code == 401
    response = auth_api.s.get(f"{auth_api.url}/status", timeout=30)
    assert response.status_code == 401

    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    me = auth_api.get("/auth/v1/me")
    assert me["username"] == "admin"
    assert any(
        permission["resource_type"] == "*"
        and permission["resource"] == "*"
        and permission["type"] == "admin"
        for permission in me["permissions"]
    )


def test_standalone_auth_user_and_api_key_flow(auth_api: AuthApi):
    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)

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

    row_filter = auth_api.put(
        "/auth/v1/users/alice/row-filters/docs", {"term": {"tier": "gold"}}
    )
    assert row_filter["table"] == "docs"
    assert row_filter["filter"]["term"]["tier"] == "gold"

    api_key = auth_api.post("/auth/v1/users/alice/api-keys", {"name": "ci"})
    assert api_key["username"] == "alice"
    assert api_key["key_secret"]
    assert api_key["encoded"]

    subjects = auth_api.get("/auth/v1/subjects")
    assert any(
        subject["subject"] == "alice" and subject["kind"] == "user"
        for subject in subjects
    )

    auth_api.s.headers["Authorization"] = f"Bearer {api_key['encoded']}"
    me = auth_api.get("/auth/v1/me")
    assert me["username"] == "alice"
    assert any(
        permission["resource_type"] == "table"
        and permission["resource"] == "docs"
        and permission["type"] == "read"
        for permission in me["permissions"]
    )


def test_standalone_auth_api_keys_follow_owner_permissions(auth_api: AuthApi):
    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
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
            "sync_level": "full_index",
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

    full_key = auth_api.post(
        "/auth/v1/users/alice/api-keys", {"name": "full-access key"}
    )
    assert full_key["username"] == "alice"
    assert full_key["encoded"]

    auth_api.s.headers["Authorization"] = f"ApiKey {full_key['encoded']}"
    assert auth_api.get("/status")["auth_enabled"] is True
    tables = auth_api.get("/tables")
    assert any(table["name"] == "docs" for table in tables)

    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
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
    assert tables_resp.status_code == 200
    assert [table["name"] for table in tables_resp.json()] == ["docs"]

    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
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

    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    keys = auth_api.get("/auth/v1/users/alice/api-keys")
    assert len(keys) == 2
    auth_api.delete(f"/auth/v1/users/alice/api-keys/{full_key['key_id']}")

    auth_api.s.headers["Authorization"] = f"ApiKey {full_key['encoded']}"
    deleted = auth_api.s.get(f"{auth_api.url}/status", timeout=30)
    assert deleted.status_code == 401

    auth_api.s.headers["Authorization"] = f"ApiKey {read_only_key['encoded']}"
    assert auth_api.get("/status")["auth_enabled"] is True

    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    remaining = auth_api.get("/auth/v1/users/alice/api-keys")
    assert len(remaining) == 1
    assert remaining[0]["key_id"] == read_only_key["key_id"]


def test_standalone_auth_enforces_row_filters_on_lookup_and_scan(auth_api: AuthApi):
    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
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
            "sync_level": "full_index",
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

    hidden_lookup = auth_api.s.get(
        f"{auth_api.url}/tables/docs/documents/doc:silver", timeout=30
    )
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
    assert [entry["_id"] for entry in scan_result] == ["doc:gold"]
    assert scan_result[0]["tier"] == "gold"
    assert scan_result[0]["title"] == "gold doc"


def test_stateful_auth_uses_configured_admin_password(stateful_auth_api: AuthApi):
    response = stateful_auth_api.s.get(f"{stateful_auth_api.url}/status", timeout=30)
    assert response.status_code == 401

    stateful_auth_api.s.headers["Authorization"] = _basic_auth(
        "admin", AUTH_BOOTSTRAP_PASSWORD
    )
    me = stateful_auth_api.get("/auth/v1/me")
    assert me["username"] == "admin"
    assert any(
        permission["resource_type"] == "*"
        and permission["resource"] == "*"
        and permission["type"] == "admin"
        for permission in me["permissions"]
    )


def test_stateful_auth_enforces_table_permissions(stateful_auth_api: AuthApi):
    stateful_auth_api.s.headers["Authorization"] = _basic_auth(
        "admin", AUTH_BOOTSTRAP_PASSWORD
    )
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
            "sync_level": "full_index",
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

    write_resp = stateful_auth_api.request_raw(
        "POST",
        "/tables/docs/batch",
        json={"inserts": {"doc-2": {"title": "blocked"}}},
        timeout=30,
    )
    assert write_resp.status_code == 403

    tables_resp = stateful_auth_api.request_raw("GET", "/tables", timeout=30)
    assert tables_resp.status_code == 200
    assert [table["name"] for table in tables_resp.json()] == ["docs"]

    admin_resp = stateful_auth_api.request_raw("GET", "/auth/v1/users", timeout=30)
    assert admin_resp.status_code == 403


def test_stateful_auth_enforces_row_filters_on_lookup_and_scan(
    stateful_auth_api: AuthApi,
):
    stateful_auth_api.s.headers["Authorization"] = _basic_auth(
        "admin", AUTH_BOOTSTRAP_PASSWORD
    )
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
            "sync_level": "full_index",
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
    stateful_auth_api.put(
        "/auth/v1/users/reader/row-filters/docs", {"term": {"tier": "gold"}}
    )

    stateful_auth_api.s.headers["Authorization"] = _basic_auth("reader", "reader")

    visible = _wait_until(lambda: _try_lookup(stateful_auth_api, "docs", "doc:gold"))
    assert visible is not None
    assert visible["title"] == "gold doc"

    hidden_lookup = stateful_auth_api.s.get(
        f"{stateful_auth_api.url}/tables/docs/documents/doc:silver", timeout=30
    )
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
    assert [entry["_id"] for entry in scan_result] == ["doc:gold"]
    assert scan_result[0]["tier"] == "gold"
    assert scan_result[0]["title"] == "gold doc"


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("fixture_name", ["auth_api", "stateful_auth_api"])
def test_system_catalog_scoped_permissions_and_row_filters(request, fixture_name):
    api = request.getfixturevalue(fixture_name)
    api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    api.post("/databases/tenant", {})
    for namespace in ("allowed", "secret"):
        api.post(f"/databases/tenant/namespaces/{namespace}", {})
        path = f"/databases/tenant/namespaces/{namespace}/tables/events"
        api.post(path, {"num_shards": 1})
        api.post(
            path + "/batch",
            {
                "inserts": {"gold": {"tier": "gold"}, "silver": {"tier": "silver"}},
                "sync_level": "full_index",
            },
        )
    api.post(
        "/auth/v1/users/reader",
        {
            "password": "reader",
            "initial_policies": [
                {
                    "table_target": {
                        "database": "tenant",
                        "namespace": "allowed",
                        "table": "events",
                    },
                    "resource_type": "table",
                    "type": "read",
                }
            ],
        },
    )
    api.put(
        "/auth/v1/users/reader/row-filters/events?database=tenant&namespace=allowed",
        {"term": {"tier": "gold"}},
    )
    api.s.headers["Authorization"] = _basic_auth("reader", "reader")
    for path in ("/databases/tenant/namespaces/allowed/tables/events",):
        assert api.get(path + "/documents/gold") == {"tier": "gold"}
        hidden = api.s.get(api.url + path + "/documents/silver", timeout=30)
        assert hidden.status_code == 404, hidden.text
        result = api.post(
            path + "/query", {"full_text_search": {"match_all": {}}, "limit": 10}
        )
        response = result["responses"][0]
        assert response["table"] == "tenant.allowed.events"
        assert response["hits"]["total"]["value"] == 1
        assert [hit["_id"] for hit in response["hits"]["hits"]] == ["gold"]
        joined = api.post(
            path + "/query",
            {
                "full_text_search": {"match_all": {}},
                "join": {
                    "right_target": {
                        "database": "tenant",
                        "namespace": "allowed",
                        "table": "events",
                    },
                    "on": {"left_field": "tier", "right_field": "tier"},
                    "right_fields": ["tier"],
                },
                "limit": 10,
            },
        )["responses"][0]
        assert len(joined["hits"]["hits"]) == 1, joined
        assert (
            joined["hits"]["hits"][0]["_source"]["tenant.allowed.events.tier"] == "gold"
        )
        forbidden = api.s.post(
            api.url + path + "/batch", json={"inserts": {"bad": {}}}, timeout=30
        )
        assert forbidden.status_code == 403, forbidden.text
        forbidden = api.s.post(
            api.url + path + "/indexes/graph_idx/graph-metrics/rank:pause",
            json={},
            timeout=30,
        )
        assert forbidden.status_code == 403, forbidden.text
    for path in ("/databases/tenant/namespaces/secret/tables/events",):
        forbidden = api.s.get(api.url + path + "/documents/gold", timeout=30)
        assert forbidden.status_code == 403, forbidden.text
    line = {
        "table_target": {
            "database": "tenant",
            "namespace": "allowed",
            "table": "events",
        },
        "full_text_search": {"match_all": {}},
        "limit": 10,
    }
    allowed = api.s.post(
        api.url + "/query",
        data=json.dumps(line) + "\n",
        headers={"Content-Type": "application/x-ndjson"},
        timeout=30,
    )
    assert allowed.status_code == 200, allowed.text
    assert allowed.json()["responses"][0]["table"] == "tenant.allowed.events"
    assert allowed.json()["responses"][0]["hits"]["total"]["value"] == 1
    denied = api.s.post(
        api.url + "/query",
        data=json.dumps(line)
        + "\n"
        + json.dumps(
            {
                **line,
                "table_target": {
                    "database": "tenant",
                    "namespace": "secret",
                    "table": "events",
                },
            }
        )
        + "\n",
        headers={"Content-Type": "application/x-ndjson"},
        timeout=30,
    )
    assert denied.status_code == 403, denied.text
    api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    original = api.get("/databases/tenant/namespaces/allowed/tables/events")["table_id"]
    api.post("/databases/tenant/namespaces/allowed/rename", {"name": "moved"})
    assert (
        api.get("/databases/tenant/namespaces/moved/tables/events")["table_id"]
        == original
    )
    missing = api.s.get(
        api.url + "/databases/tenant/namespaces/allowed/tables/events", timeout=30
    )
    assert missing.status_code == 404, missing.text
    api.s.headers["Authorization"] = _basic_auth("reader", "reader")
    denied = api.s.get(
        api.url + "/databases/tenant/namespaces/moved/tables/events/documents/gold",
        timeout=30,
    )
    assert denied.status_code == 403, denied.text
    api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    api.delete("/databases/tenant/namespaces/moved/tables/events")
    api.delete("/databases/tenant/namespaces/secret/tables/events")
    api.delete("/databases/tenant")


def test_system_catalog_exact_star_grant_is_not_global(auth_api):
    api = auth_api
    api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    api.post("/tables/%21", {"num_shards": 1})
    for name in ("%2A", "other"):
        api.post("/tables/" + name, {"num_shards": 1})
        api.post(
            "/tables/" + name + "/batch",
            {
                "inserts": {"gold": {"tier": "gold"}, "silver": {"tier": "silver"}},
                "sync_level": "full_index",
            },
        )
    api.post(
        "/auth/v1/users/star_reader",
        {
            "password": "reader",
            "initial_policies": [
                {
                    "resource_type": "table",
                    "table_target": {"table": "*"},
                    "type": "read",
                }
            ],
        },
    )
    api.put(
        "/auth/v1/users/star_reader/row-filters/%2A?database=default&namespace=public",
        {"term": {"tier": "gold"}},
    )
    api.s.headers["Authorization"] = _basic_auth("star_reader", "reader")
    assert api.get("/tables/%2A/documents/gold") == {"tier": "gold"}
    assert (
        api.s.get(api.url + "/tables/%2A/documents/silver", timeout=30).status_code
        == 404
    )
    assert (
        api.s.get(api.url + "/tables/other/documents/gold", timeout=30).status_code
        == 403
    )
    assert api.get("/tables/%2A")["name"] == "*"
    assert api.s.get(api.url + "/tables/other", timeout=30).status_code == 403
    assert [table["name"] for table in api.get("/tables")] == ["*"]
    # A bounded page can contain only denied rows. Its cursor must advance
    # without exposing private names or granting access on the next request.
    response = api.s.get(api.url + "/tables", params={"limit": 1}, timeout=30)
    assert response.status_code == 200, response.text
    assert response.json() == []
    cursor = response.headers["X-Antfly-Next-Cursor"]
    response = api.s.get(
        api.url + "/tables", params={"limit": 1, "cursor": cursor}, timeout=30
    )
    assert response.status_code == 200, response.text
    assert [table["name"] for table in response.json()] == ["*"]
    cursor = response.headers["X-Antfly-Next-Cursor"]
    response = api.s.get(
        api.url + "/tables", params={"limit": 1, "cursor": cursor}, timeout=30
    )
    assert response.status_code == 200, response.text
    assert response.json() == []
    assert "X-Antfly-Next-Cursor" not in response.headers
    api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    api.delete("/tables/%2A")
    api.delete("/tables/other")
    api.delete("/tables/%21")


def test_resolver_entity_state_requires_source_scopes_on_api_keys(auth_api: AuthApi):
    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    destination = "curation_entities"
    sources = ["curation_notes", "curation_memory"]
    auth_api.create_table(destination)
    graph = {
        "type": "graph",
        "source": {
            "artifact": "kg",
            "format": "extraction_graph",
            "mention_edge_type": "mentions",
        },
        "artifact": {
            "name": "kg",
            "kind": "asset",
            "source": {"type": "field", "value": "kg"},
            "content_type": "application/json",
        },
        "edge_types": [{"name": "mentions"}],
        "resolvers": [
            {
                "name": "concepts",
                "table": destination,
                "promote": False,
                "source_artifact": "kg",
                "resolution_artifact": "resolved",
                "key_template": "concept/{{uslug _entity.text}}",
            }
        ],
    }
    for source in sources:
        auth_api.create_table(
            source, {"num_shards": 1, "indexes": {"knowledge": graph}}
        )
        auth_api.post(
            f"/tables/{source}/resolvers/concepts/suppressions",
            {
                "keys": ["concept/noise", "concept/private"],
                "texts": ["private source text"],
            },
        )
    # The owner has broad access; the key's narrower scopes must survive fanout.
    auth_api.post(
        "/auth/v1/users/curator",
        {
            "password": "password123",
            "initial_policies": [
                {"resource": "*", "resource_type": "table", "type": "read"}
            ],
        },
    )
    keys = []
    for resources in ([destination], [destination, *sources]):
        keys.append(
            auth_api.post(
                "/auth/v1/users/curator/api-keys",
                {
                    "name": f"curation-{len(resources)}",
                    "permissions": [
                        {"resource": table, "resource_type": "table", "type": "read"}
                        for table in resources
                    ],
                },
            )["encoded"]
        )
    suffix = f"/tables/{destination}/entities/concept%2Fnoise/redirects"
    for prefix in ("", "/databases/default/namespaces/public"):
        auth_api.s.headers["Authorization"] = f"Bearer {keys[0]}"
        denied = auth_api.s.get(f"{auth_api.url}{prefix}{suffix}", timeout=30)
        assert denied.status_code == 403, denied.text
        assert "private source text" not in denied.text
        auth_api.s.headers["Authorization"] = f"Bearer {keys[1]}"
        allowed = auth_api.get(prefix + suffix)
        # Independent legacy sources fold into one exact-template namespace.
        assert allowed["complete"] and len(allowed["results"]) == 1, allowed
        for row in allowed["results"]:
            assert row["curation"]["suppress"]["keys"] == ["concept/noise"]
            assert row["curation"]["suppress"]["texts"] == []
        assert "private source text" not in str(allowed)
        assert "concept/private" not in str(allowed)

    # Explicit adoption moves authority to the destination. Reading its
    # namespace no longer borrows private source policy or needs source scopes.
    auth_api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    auth_api.post(f"/tables/{destination}/entities/concept%2Fnoise:suppress", {})
    auth_api.s.headers["Authorization"] = f"Bearer {keys[0]}"
    for prefix in ("", "/databases/default/namespaces/public"):
        owned = auth_api.get(prefix + suffix)
        assert owned["complete"] and len(owned["results"]) == 1, owned
        assert owned["results"][0]["curation"]["suppress"]["keys"] == ["concept/noise"]
        assert "private source text" not in str(owned)


@pytest.mark.parametrize("candidate_search", ["exact_key", "prefix"])
def test_resolver_query_enforces_destination_scopes_and_row_filters(
    auth_api: AuthApi, candidate_search
):
    api = auth_api
    admin = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
    api.s.headers["Authorization"] = admin
    api.create_table("resolver_entities")
    api.batch_write(
        "resolver_entities",
        {
            "inserts": {
                "concept/alpha": {
                    "entity_type": "concept",
                    "canonical_name": "CONFIDENTIAL_ENTITY",
                    "tier": "private",
                },
                "concept/beta": {
                    "entity_type": "concept",
                    "canonical_name": "Public entity",
                    "tier": "public",
                },
            },
            "sync_level": "full_index",
        },
    )
    graph = {
        "type": "graph",
        "source": {
            "artifact": "kg",
            "format": "extraction_graph",
            "mention_edge_type": "mentions",
        },
        "artifact": {
            "name": "kg",
            "kind": "asset",
            "source": {"type": "field", "value": "kg"},
            "content_type": "application/json",
        },
        "edge_types": [{"name": "mentions"}],
        "resolvers": [
            {
                "name": "concepts",
                "table": "resolver_entities",
                "promote": False,
                "source_artifact": "kg",
                "resolution_artifact": "resolved",
                "key_template": "concept/{{uslug _entity.text}}",
                "candidate_search": candidate_search,
                "scorer": {
                    "comparisons": [],
                    "combine": {"bias": 5},
                    "decision": {"match": 0.9, "review": 0.6},
                },
            }
        ],
    }
    api.create_table(
        "resolver_notes", {"num_shards": 1, "indexes": {"knowledge": graph}}
    )
    api.post(
        "/auth/v1/users/query_reader",
        {
            "password": "password123",
            "initial_policies": [
                {"resource": "*", "resource_type": "table", "type": "read"},
            ],
        },
    )
    restricted = api.post(
        "/auth/v1/users/query_reader/api-keys",
        {
            "name": "source only",
            "permissions": [
                {
                    "resource": "resolver_notes",
                    "resource_type": "table",
                    "type": "read",
                },
            ],
        },
    )["encoded"]
    allowed = api.post(
        "/auth/v1/users/query_reader/api-keys",
        {
            "name": "source and destination",
            "permissions": [
                {"resource": table, "resource_type": "table", "type": "read"}
                for table in ("resolver_notes", "resolver_entities")
            ],
        },
    )["encoded"]

    def resolve(prefix, text):
        return api.s.post(
            api.url + prefix + "/tables/resolver_notes/resolvers/concepts:resolve",
            json={
                "extraction": {
                    "entities": [{"id": "a", "text": text, "label": "concept"}]
                },
            },
            timeout=30,
        )

    for prefix in ("", "/databases/default/namespaces/public"):
        api.s.headers["Authorization"] = f"Bearer {restricted}"
        denied = resolve(prefix, "Alpha")
        assert denied.status_code == 403, denied.text
        assert "CONFIDENTIAL_ENTITY" not in denied.text
        api.s.headers["Authorization"] = f"Bearer {allowed}"
        visible = resolve(prefix, "Alpha")
        assert visible.status_code == 200, visible.text
        assert "CONFIDENTIAL_ENTITY" in visible.text
    api.s.headers["Authorization"] = admin
    api.put(
        "/auth/v1/users/query_reader/row-filters/resolver_entities",
        {"term": {"tier": "public"}},
    )
    api.s.headers["Authorization"] = f"Bearer {allowed}"
    hidden = api.s.get(
        api.url + lookup_key_path("resolver_entities", "concept/alpha"), timeout=30
    )
    assert hidden.status_code == 404, hidden.text
    for prefix in ("", "/databases/default/namespaces/public"):
        filtered = resolve(prefix, "Alpha")
        assert filtered.status_code == 200, filtered.text
        assert "CONFIDENTIAL_ENTITY" not in filtered.text
        public = resolve(prefix, "Beta")
        assert public.status_code == 200, public.text
        assert (
            public.json()["resolution"]["entities"][0]["canonical_name"]
            == "Public entity"
        )
    # A readable tombstone cannot grant access to a different survivor table.
    api.s.headers["Authorization"] = admin
    api.create_table("private_survivors")
    api.batch_write(
        "private_survivors",
        {
            "inserts": {
                "concept/hidden": {
                    "entity_type": "concept",
                    "canonical_name": "CONFIDENTIAL_SURVIVOR",
                }
            },
            "sync_level": "full_index",
        },
    )
    api.batch_write(
        "resolver_entities",
        {
            "inserts": {
                "concept/redirect": {
                    "entity_type": "concept",
                    "canonical_name": "Redirect",
                    "tier": "public",
                    "merged_into": "concept/hidden",
                    "merged_into_table": "private_survivors",
                }
            },
            "sync_level": "full_index",
        },
    )
    api.s.headers["Authorization"] = f"Bearer {allowed}"
    denied = resolve("", "Redirect")
    assert denied.status_code == 403, denied.text
    assert "CONFIDENTIAL_SURVIVOR" not in denied.text


@pytest.mark.parametrize("storage_engine", ["local", "lite"])
def test_memory_evaluation_pages_and_indexed_candidates_with_admin_key(storage_engine):
    """Exercise exact-ID hydration and lexical search through real owner archives."""
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists() or Path(binary).name != "antfly":
        pytest.skip("requires the unified antfly CLI")
    server = StandaloneAuthServer(
        binary, "127.0.0.1", find_free_port(), storage_engine=storage_engine
    )
    try:
        api = AuthApi(server.api_url, server)
        api.s.headers["Authorization"] = _basic_auth("admin", AUTH_BOOTSTRAP_PASSWORD)
        key = api.post("/auth/v1/users/admin/api-keys", {"name": "memory-regression"})
        api.s.headers["Authorization"] = f"Bearer {key['encoded']}"
        root = server.url + "/memory/v1/spaces"
        counter = 0

        def call(method, suffix, body=None):
            nonlocal counter
            counter += 1
            response = api.s.request(
                method,
                root + suffix,
                json=body,
                headers={"Idempotency-Key": f"memory-{counter}"},
                timeout=30,
            )
            return api._check(response)

        call("POST", "", {"space": "qualification"})
        space = "/qualification"

        def revision():
            return call("GET", space)["state"]["revision"]

        cases = [
            {
                "id": f"case-{i}",
                "task": "identity",
                "input": {"left": "KG", "right": "knowledge graph"},
                "expected": "same",
            }
            for i in range(3)
        ]
        call(
            "POST",
            space + "/evaluation-datasets",
            {"id": "labels", "expected_revision": revision(), "cases": cases},
        )
        judge = {"provider": "codex", "model": "external", "revision": "pinned-client"}
        call(
            "POST",
            space + "/evaluations",
            {
                "id": "run",
                "dataset": "labels",
                "expected_revision": revision(),
                "mode": "external",
                "judge": judge,
            },
        )
        tasks = call("GET", space + "/evaluations/run/tasks")["items"]
        assert len(tasks) == 3
        assert call("POST", space + "/evaluations/run/tasks", {})["items"] == tasks
        assert (
            len(call("GET", space + "/evaluation-datasets/labels/cases")["items"]) == 3
        )
        assert all("expected" not in task["input"] for task in tasks)
        for endpoint in (
            "/evaluation-datasets/labels/cases",
            "/evaluations/run/tasks",
            "/evaluations/run/results",
        ):
            first = call("GET", space + endpoint + "?limit=1")
            assert len(first["items"]) == 1
            assert first["next_cursor"]
            page_request = {"limit": 1, "cursor": first["next_cursor"]}
            second = (
                call("POST", space + endpoint, page_request)
                if endpoint.endswith("/tasks")
                else call("GET", space + endpoint + "?" + urlencode(page_request))
            )
            assert len(second["items"]) == 1
            assert first["items"] != second["items"]
        verdicts = [
            {
                **{
                    field: task[field]
                    for field in ("case_id", "case_revision", "input_fingerprint")
                },
                "prediction": {"label": "same"},
            }
            for task in tasks
        ]
        call(
            "POST",
            space + "/evaluations/run/verdicts",
            {"expected_revision": revision(), "verdicts": verdicts, "model": judge},
        )
        assert call("GET", space + "/evaluations/run")["identity"]["accuracy"] == 1
        assert len(call("GET", space + "/evaluations/run/results")["items"]) == 3
        claims = [
            {
                "id": f"claim-{i}",
                "subject": "production" if i < 2 else "unrelated",
                "predicate": "deploys",
                "value": f"value-{i}",
                "statement": "Deployment uses Helm",
                "evidence": [{"id": "e"}],
            }
            for i in range(3)
        ]
        call(
            "POST",
            space + "/observations",
            {
                "evidence": [{"id": "e", "text": "Deployment uses Helm"}],
                "assertions": claims,
            },
        )
        deadline = time.monotonic() + 60
        while True:
            try:
                call("POST", space + "/maintenance", {})
                page = call(
                    "POST",
                    space + "/memories/claim-0/supersession-candidates",
                    {"mode": "lexical"},
                )
            except requests.HTTPError as error:
                # Standalone advances memory in the background. Both mutation
                # CAS and read-snapshot fences require a fresh bounded attempt.
                response = error.response
                payload = response.json() if response is not None else {}
                if response is None or not (
                    (
                        response.status_code == 409
                        and payload.get("code") == "MemoryRevisionConflict"
                    )
                    or (
                        response.status_code == 503 and payload.get("retryable") is True
                    )
                ):
                    raise
                assert time.monotonic() < deadline
                time.sleep(0.1)
                continue
            if page["retrieval"] == "indexed":
                assert [
                    item["view"]["memory"]["assertion"]["id"] for item in page["items"]
                ] == ["claim-1"]
                table = api.get("/tables/memory_qualification")
                assert (
                    sum(
                        name.startswith("memory_recall_text_")
                        for name in table["indexes"]
                    )
                    >= 2
                )
                break
            assert time.monotonic() < deadline, page
            time.sleep(0.1)
    finally:
        server.stop()
