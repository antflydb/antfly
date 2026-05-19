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

import os
import re
import subprocess
import time
from typing import Any

import pytest
import requests

from conftest import (
    ANTFLY_PUBLIC_API_ROOT,
    DEFAULT_ANTFLY_BIN,
    StatefulAntflyServer,
    find_free_port,
    lookup_key_path,
    raise_request_error_with_logs,
    resolve_binary_path,
    wait_for_server,
    with_api_root,
)
from helpers import wait_until

pytestmark = pytest.mark.postgres_integration

DEFAULT_PG_DSN = "postgres://localhost:5432/postgres?sslmode=disable"
PSQL_BIN = os.environ.get("ANTFLY_TEST_PSQL_BIN", "/opt/homebrew/opt/postgresql@18/bin/psql")


def _pg_dsn() -> str:
    return os.environ.get("ANTFLY_TEST_PG_DSN") or os.environ.get("PG_DSN") or DEFAULT_PG_DSN


def _pg_available() -> bool:
    try:
        subprocess.run(
            [PSQL_BIN, _pg_dsn(), "-tAc", "select 1"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return False
    return True


def _pg_supports_logical_replication() -> bool:
    if not _pg_available():
        return False
    try:
        result = subprocess.run(
            [PSQL_BIN, _pg_dsn(), "-tAc", "show wal_level"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.SubprocessError:
        return False
    return result.stdout.strip().lower() == "logical"


def _run_psql(sql: str) -> None:
    subprocess.run(
        [PSQL_BIN, _pg_dsn(), "-v", "ON_ERROR_STOP=1", "-c", sql],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _run_psql_best_effort(sql: str) -> None:
    try:
        subprocess.run(
            [PSQL_BIN, _pg_dsn(), "-v", "ON_ERROR_STOP=1", "-c", sql],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        pass


def _cleanup_reseed_artifacts_best_effort() -> None:
    _run_psql_best_effort(
        """
        DO $$
        DECLARE rec record;
        BEGIN
          FOR rec IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'antfly_pub_cdc_%' LOOP
            EXECUTE 'DROP PUBLICATION IF EXISTS ' || quote_ident(rec.pubname);
          END LOOP;
        END $$;
        """
    )
    _run_psql_best_effort(
        """
        select pg_drop_replication_slot(slot_name)
        from pg_replication_slots
        where slot_name like 'antfly_cdc_%' and not active;
        """
    )


def _psql_scalar_best_effort(sql: str) -> str:
    try:
        result = subprocess.run(
            [PSQL_BIN, _pg_dsn(), "-tAc", sql],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip()


def _lookup_doc(stateful_api, table_name: str, key: str) -> dict[str, Any] | None:
    try:
        return stateful_api.lookup_key(table_name, key)
    except requests.RequestException:
        return None


def _lookup_doc_if(stateful_api, table_name: str, key: str, predicate) -> dict[str, Any] | None:
    doc = _lookup_doc(stateful_api, table_name, key)
    if doc is None:
        return None
    return doc if predicate(doc) else None


def _get_table_if_visible(stateful_api, table_name: str) -> dict[str, Any] | None:
    try:
        return stateful_api.get_table(table_name)
    except requests.RequestException:
        return None


def _wait_until_absent(stateful_api, table_name: str, key: str, *, timeout_s: float, interval_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _lookup_doc(stateful_api, table_name, key) is None:
            return
        time.sleep(interval_s)
    raise AssertionError(f"{table_name}:{key} remained visible after CDC delete")


def _server_logs(stateful_api) -> str:
    server = getattr(stateful_api, "_server", None)
    if server is None:
        return ""
    return server.debug_logs().strip()


def _metadata_admin_base_url(stateful_api) -> str:
    server = getattr(stateful_api, "_server", None)
    if server is not None:
        metadata_admin_url = getattr(server, "metadata_admin_url", None)
        if metadata_admin_url:
            return metadata_admin_url.rstrip("/")
    env_url = os.environ.get("ANTFLY_METADATA_ADMIN_URL")
    if env_url:
        return env_url.rstrip("/")
    logs = _server_logs(stateful_api)
    matches = re.findall(r"(?:swarm )?metadata admin api listening on (http://[^\s]+)", logs)
    if matches:
        return matches[-1].rstrip("/")
    return ""


def _metadata_admin_snapshot(stateful_api) -> str:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        return ""
    try:
        response = requests.get(f"{base_url}/metadata/v1/admin/snapshot", timeout=5)
        response.raise_for_status()
    except requests.RequestException:
        return ""
    return response.text.strip()


def _metadata_admin_snapshot_json(stateful_api) -> dict[str, Any]:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        return {}
    try:
        response = requests.get(f"{base_url}/metadata/v1/admin/snapshot", timeout=5)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _metadata_replication_statuses(stateful_api) -> str:
    records = _metadata_replication_status_records(stateful_api)
    return repr(records) if records else ""


def _metadata_replication_status_records(stateful_api) -> list[dict[str, Any]]:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        return []
    try:
        response = requests.get(f"{base_url}/metadata/v1/admin/snapshot", timeout=5)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        return []
    statuses = payload.get("replication_source_statuses")
    if not isinstance(statuses, list):
        return []
    return [status for status in statuses if isinstance(status, dict)]


def _metadata_status(stateful_api) -> dict[str, Any]:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        return {}
    try:
        response = requests.get(f"{base_url}/metadata/v1/status", timeout=5)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _reseed_exact_cutover(stateful_api, table_name: str, source_ordinal: int) -> dict[str, Any]:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        raise AssertionError(
            "metadata admin base url unavailable for CDC reseed\n"
            f"{_server_logs(stateful_api)}"
        )
    response = requests.post(
        f"{base_url}/internal/v1/tables/{table_name}/replication-sources/{source_ordinal}/reseed-exact-cutover",
        headers={"Connection": "close"},
        timeout=30,
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        raise_request_error_with_logs(exc, getattr(stateful_api, "_server", None))
    return response.json()


def _pg_replication_debug(slot_name: str, publication_name: str) -> str:
    slot_info = _psql_scalar_best_effort(
        "select coalesce(slot_name,'') || '|' || coalesce(active::text,'') || '|' || "
        "coalesce(confirmed_flush_lsn::text,'') || '|' || coalesce(restart_lsn::text,'') "
        f"from pg_replication_slots where slot_name = '{slot_name}'"
    )
    change_count = _psql_scalar_best_effort(
        "select count(*) from pg_logical_slot_peek_binary_changes("
        f"'{slot_name}', NULL, 16, 'proto_version', '2', 'publication_names', '{publication_name}')"
    )
    return f"slot={slot_info!r} pending_changes={change_count!r}"


def _tier_profile_foreign_source(table_name: str) -> dict[str, Any]:
    return {
        "type": "postgres",
        "dsn": _pg_dsn(),
        "postgres_table": table_name,
        "columns": [
            {"name": "tier", "type": "text"},
            {"name": "label", "type": "text"},
            {"name": "address_id", "type": "text"},
        ],
    }


def _address_foreign_source(table_name: str) -> dict[str, Any]:
    return {
        "type": "postgres",
        "dsn": _pg_dsn(),
        "postgres_table": table_name,
        "columns": [
            {"name": "id", "type": "text"},
            {"name": "city", "type": "text"},
            {"name": "region", "type": "text"},
        ],
    }


def _drop_replication_slot_when_inactive(slot_name: str, *, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        active = _psql_scalar_best_effort(
            f"select coalesce(active::text,'') from pg_replication_slots where slot_name = '{slot_name}'"
        ).lower()
        if active in {"", "f", "false"}:
            _run_psql_best_effort(
                "select pg_drop_replication_slot('{slot}') "
                "from pg_replication_slots where slot_name = '{slot}' and not active;".format(slot=slot_name)
            )
            remaining = _psql_scalar_best_effort(
                f"select coalesce(slot_name,'') from pg_replication_slots where slot_name = '{slot_name}'"
            )
            if not remaining:
                return
        time.sleep(0.25)
    raise AssertionError(f"replication slot {slot_name} remained active and could not be dropped")


def _create_table_via_metadata_admin(stateful_api, table_name: str, payload: dict[str, Any]) -> None:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        raise AssertionError(
            "metadata admin base url unavailable for CDC E2E\n"
            f"{_server_logs(stateful_api)}"
        )
    response = requests.post(
        f"{base_url}/internal/v1/tables/{table_name}",
        json=payload,
        headers={"Content-Type": "application/json", "Connection": "close"},
        timeout=30,
    )
    if response.status_code != 201:
        body = response.text.strip()
        raise AssertionError(
            f"metadata internal create failed: {response.status_code} {body}\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_admin_snapshot(stateful_api)}"
        )


@pytest.fixture(scope="function")
def cdc_stateful_api():
    base_url = os.environ.get("ANTFLY_STATEFUL_URL")
    server = None
    default_root = os.environ.get("ANTFLY_STATEFUL_API_ROOT")
    if not base_url:
        binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
        if not os.path.exists(binary):
            pytest.skip(f"Public API binary not found: {binary}")
        port = find_free_port()
        server = StatefulAntflyServer(binary, "127.0.0.1", port)
        base_url = server.url
        if default_root is None and os.path.basename(binary) == "antfly":
            default_root = ANTFLY_PUBLIC_API_ROOT

    base = with_api_root(base_url, default_root if default_root is not None else "")

    if not wait_for_server(base, timeout=10):
        pytest.skip(f"Public API at {base} is not reachable")

    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Connection"] = "close"

    class PublicApi:
        def __init__(self, session: requests.Session, base_url: str, server_ref):
            self.s = session
            self.url = base_url.rstrip("/")
            self._server = server_ref

        def _raise_request_error(self, err: requests.RequestException) -> None:
            raise_request_error_with_logs(err, self._server)

        def _check(self, response: requests.Response) -> Any:
            if response.status_code >= 400:
                body = response.text.strip()
                logs = self._server.debug_logs().strip() if self._server is not None else ""
                if body:
                    if logs:
                        raise requests.HTTPError(
                            f"{response.status_code} {response.reason} for url: {response.url} body={body}\nserver logs:\n{logs}",
                            response=response,
                        )
                    raise requests.HTTPError(
                        f"{response.status_code} {response.reason} for url: {response.url} body={body}",
                        response=response,
                    )
                if logs:
                    raise requests.HTTPError(
                        f"{response.status_code} {response.reason} for url: {response.url}\nserver logs:\n{logs}",
                        response=response,
                    )
                response.raise_for_status()
            if not response.content:
                return {}
            return response.json()

        def _request(self, method: str, path: str, payload: dict | None = None) -> requests.Response:
            try:
                return self.s.request(method, f"{self.url}{path}", json=payload, timeout=30)
            except requests.RequestException as err:
                self._raise_request_error(err)
                raise AssertionError("unreachable")

        def get_table(self, table_name: str) -> dict[str, Any]:
            return self._check(self._request("GET", f"/tables/{table_name}"))

        def lookup_key(self, table_name: str, key: str) -> dict[str, Any] | None:
            response = self._request("GET", lookup_key_path(table_name, key))
            if response.status_code == 404:
                return None
            return self._check(response)

        def query_table(self, table_name: str, payload: dict[str, Any]) -> dict[str, Any]:
            return self._check(self._request("POST", f"/tables/{table_name}/query", payload))

        def restart_server(self) -> None:
            server = self._server
            if server is None or not hasattr(server, "restart"):
                raise AssertionError("restart is only available for locally managed stateful servers")
            self.s.close()
            server.restart()
            if not wait_for_server(self.url, timeout=20):
                logs = server.debug_logs().strip()
                raise AssertionError(f"stateful server failed to restart at {self.url}\n{logs}")
            new_session = requests.Session()
            new_session.headers["Content-Type"] = "application/json"
            new_session.headers["Connection"] = "close"
            self.s = new_session

    api = PublicApi(session, base, server)
    try:
        yield api
    finally:
        session.close()
        if server is not None:
            server.stop()


@pytest.fixture(scope="function")
def stateful_api(cdc_stateful_api):
    return cdc_stateful_api


@pytest.fixture(scope="function")
def pg_cdc_source():
    if not _pg_available():
        pytest.skip("local PostgreSQL is unavailable for CDC E2E")
    if not _pg_supports_logical_replication():
        pytest.skip("local PostgreSQL is not configured with wal_level=logical")

    suffix = str(time.time_ns())
    table_name = f"antfly_e2e_pg_cdc_{suffix}"
    slot_name = f"antfly_e2e_slot_{suffix}"
    publication_name = f"antfly_e2e_pub_{suffix}"

    _run_psql(
        f"""
        create table {table_name} (
            id text primary key,
            name text not null,
            tier text not null
        );
        insert into {table_name} (id, name, tier) values
            ('user-1', 'Alice', 'gold');
        """
    )

    try:
        yield {
            "table_name": table_name,
            "slot_name": slot_name,
            "publication_name": publication_name,
        }
    finally:
        _run_psql_best_effort(f"drop publication if exists {publication_name};")
        _run_psql_best_effort(
            "select pg_drop_replication_slot('{slot}') "
            "from pg_replication_slots where slot_name = '{slot}' and not active;".format(slot=slot_name)
        )
        _run_psql_best_effort(f"drop table if exists {table_name};")


def test_stateful_postgres_cdc_snapshot_and_streaming(stateful_api, pg_cdc_source):
    table_name = f"cdc_docs_{time.time_ns()}"
    create_payload = {
        "num_shards": 1,
        "replication_sources": [
            {
                "type": "postgres",
                "dsn": _pg_dsn(),
                "postgres_table": pg_cdc_source["table_name"],
                "key_template": "id",
                "slot_name": pg_cdc_source["slot_name"],
                "publication_name": pg_cdc_source["publication_name"],
                "on_delete": [{"op": "$delete_document"}],
            }
        ],
    }
    _create_table_via_metadata_admin(stateful_api, table_name, create_payload)

    created = wait_until(
        lambda: _get_table_if_visible(stateful_api, table_name),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert created is not None, (
        "CDC table did not become visible through the public API after metadata create\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_admin_snapshot(stateful_api)}"
    )
    assert created["name"] == table_name

    table_detail = stateful_api.get_table(table_name)
    assert table_detail["replication_sources"][0]["slot_name"] == pg_cdc_source["slot_name"]
    assert table_detail["replication_sources"][0]["publication_name"] == pg_cdc_source["publication_name"]

    snapshot_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-1",
            lambda doc: doc.get("name") == "Alice" and doc.get("tier") == "gold",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert snapshot_doc is not None, (
        "CDC snapshot did not import the initial PostgreSQL row\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )
    _run_psql(
        f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-2', 'Bob', 'silver');
        """
    )

    inserted_doc = wait_until(
            lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-2",
            lambda doc: doc.get("name") == "Bob" and doc.get("tier") == "silver",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert inserted_doc is not None, (
        "CDC stream did not apply the inserted PostgreSQL row\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}\n"
        f"{_pg_replication_debug(pg_cdc_source['slot_name'], pg_cdc_source['publication_name'])}"
    )

    _run_psql(
        f"""
        update {pg_cdc_source["table_name"]}
        set name = 'Alicia', tier = 'platinum'
        where id = 'user-1';
        """
    )

    updated_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-1",
            lambda doc: doc.get("name") == "Alicia" and doc.get("tier") == "platinum",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert updated_doc is not None, (
        "CDC stream did not apply the updated PostgreSQL row\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )
    status_after_update = _metadata_status(stateful_api)
    assert status_after_update.get("projected_replication_source_statuses_streaming", 0) >= 1
    assert status_after_update.get("projected_replication_source_statuses_with_consecutive_failures", 0) == 0
    assert status_after_update.get("projected_replication_source_consecutive_failures_max", 0) == 0
    assert "projected_replication_source_lag_millis_max" in status_after_update
    assert "projected_replication_source_observed_lag_millis_max" in status_after_update
    assert (
        status_after_update.get("projected_replication_source_observed_lag_millis_max", 0)
        >= status_after_update.get("projected_replication_source_lag_millis_max", 0)
    )
    assert status_after_update.get("projected_replication_source_statuses_with_source_commit_timestamp", 0) >= 1
    matching_after_update = [
        status
        for status in _metadata_replication_status_records(stateful_api)
        if status.get("slot_name") == pg_cdc_source["slot_name"]
        and status.get("publication_name") == pg_cdc_source["publication_name"]
    ]
    assert any(status.get("last_source_commit_at_ms", 0) > 0 for status in matching_after_update), (
        "replication status did not expose source commit timestamps after streamed update\n"
        f"{matching_after_update!r}"
    )
    assert status_after_update.get("projected_replication_source_last_change_applied_at_ms_max", 0) > 0

    _run_psql(f"delete from {pg_cdc_source['table_name']} where id = 'user-2';")
    _wait_until_absent(stateful_api, table_name, "user-2", timeout_s=30.0, interval_s=0.25)


def test_stateful_postgres_cdc_table_joins_with_foreign_lookup(stateful_api, pg_cdc_source):
    table_name = f"cdc_join_docs_{time.time_ns()}"
    lookup_table = f"antfly_e2e_pg_tier_lookup_{time.time_ns()}"
    joined_field = "pg_tier_lookup.label"
    _run_psql(
        f"""
        create table {lookup_table} (
            tier text primary key,
            label text not null
        );
        insert into {lookup_table} (tier, label) values
            ('gold', 'Gold Tier'),
            ('silver', 'Silver Tier'),
            ('platinum', 'Platinum Tier');
        """
    )
    try:
        create_payload = {
            "num_shards": 1,
            "replication_sources": [
                {
                    "type": "postgres",
                    "dsn": _pg_dsn(),
                    "postgres_table": pg_cdc_source["table_name"],
                    "key_template": "id",
                    "slot_name": pg_cdc_source["slot_name"],
                    "publication_name": pg_cdc_source["publication_name"],
                    "on_delete": [{"op": "$delete_document"}],
                }
            ],
        }
        _create_table_via_metadata_admin(stateful_api, table_name, create_payload)

        snapshot_doc = wait_until(
            lambda: _lookup_doc_if(
                stateful_api,
                table_name,
                "user-1",
                lambda doc: doc.get("name") == "Alice" and doc.get("tier") == "gold",
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert snapshot_doc is not None, (
            "CDC snapshot did not import before foreign join coverage\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_replication_statuses(stateful_api)}"
        )

        def joined_rows() -> dict[str, dict[str, Any]] | None:
            try:
                result = stateful_api.query_table(
                    table_name,
                    {
                        "limit": 10,
                        "fields": ["name", "tier"],
                        "join": {
                            "right_table": "pg_tier_lookup",
                            "join_type": "left",
                            "on": {
                                "left_field": "tier",
                                "right_field": "tier",
                                "operator": "eq",
                            },
                            "right_fields": ["label"],
                        },
                        "foreign_sources": {
                            "pg_tier_lookup": {
                                "type": "postgres",
                                "dsn": _pg_dsn(),
                                "postgres_table": lookup_table,
                                "columns": [
                                    {"name": "tier", "type": "text"},
                                    {"name": "label", "type": "text"},
                                ],
                            }
                        },
                    },
                )
            except requests.RequestException:
                return None
            responses = result.get("responses", [])
            if not responses:
                return None
            hits = responses[0].get("hits", {}).get("hits", [])
            rows = {
                hit.get("_id", ""): hit.get("_source", {})
                for hit in hits
                if isinstance(hit, dict) and isinstance(hit.get("_source"), dict)
            }
            return rows if rows else None

        joined_snapshot = wait_until(
            lambda: (
                rows
                if (rows := joined_rows()) is not None
                and rows.get("user-1", {}).get(joined_field) == "Gold Tier"
                else None
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert joined_snapshot is not None, (
            "foreign lookup join did not reflect CDC snapshot row\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_replication_statuses(stateful_api)}"
        )

        _run_psql(
            f"""
            insert into {pg_cdc_source["table_name"]} (id, name, tier)
            values ('user-2', 'Bob', 'silver');
            """
        )

        joined_insert = wait_until(
            lambda: (
                rows
                if (rows := joined_rows()) is not None
                and rows.get("user-2", {}).get("name") == "Bob"
                and rows.get("user-2", {}).get(joined_field) == "Silver Tier"
                else None
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert joined_insert is not None, (
            "foreign lookup join did not reflect CDC streamed insert\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_replication_statuses(stateful_api)}"
        )

        _run_psql(
            f"""
            update {pg_cdc_source["table_name"]}
            set tier = 'platinum'
            where id = 'user-1';
            """
        )

        joined_update = wait_until(
            lambda: (
                rows
                if (rows := joined_rows()) is not None
                and rows.get("user-1", {}).get("tier") == "platinum"
                and rows.get("user-1", {}).get(joined_field) == "Platinum Tier"
                else None
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert joined_update is not None, (
            "foreign lookup join did not reflect CDC streamed update\n"
            f"direct_doc={_lookup_doc(stateful_api, table_name, 'user-1')!r}\n"
            f"joined_rows={joined_rows()!r}\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_replication_statuses(stateful_api)}\n"
            f"{_pg_replication_debug(pg_cdc_source['slot_name'], pg_cdc_source['publication_name'])}"
        )
    finally:
        _run_psql_best_effort(f"drop table if exists {lookup_table};")


def test_stateful_postgres_cdc_table_joins_with_nested_foreign_lookup(stateful_api, pg_cdc_source):
    table_name = f"cdc_nested_join_docs_{time.time_ns()}"
    lookup_table = f"antfly_e2e_pg_tier_profile_{time.time_ns()}"
    address_table = f"antfly_e2e_pg_tier_addresses_{time.time_ns()}"
    label_field = "pg_tier_profiles.label"
    city_field = "pg_tier_profiles.pg_addresses.city"
    region_field = "pg_tier_profiles.pg_addresses.region"
    _run_psql(
        f"""
        create table {lookup_table} (
            tier text primary key,
            label text not null,
            address_id text not null
        );
        create table {address_table} (
            id text primary key,
            city text not null,
            region text not null
        );
        insert into {address_table} (id, city, region) values
            ('addr-1', 'Seattle', 'wa'),
            ('addr-2', 'Portland', 'or'),
            ('addr-3', 'San Francisco', 'ca');
        insert into {lookup_table} (tier, label, address_id) values
            ('gold', 'Gold Tier', 'addr-1'),
            ('silver', 'Silver Tier', 'addr-2'),
            ('platinum', 'Platinum Tier', 'addr-3');
        """
    )
    try:
        create_payload = {
            "num_shards": 1,
            "replication_sources": [
                {
                    "type": "postgres",
                    "dsn": _pg_dsn(),
                    "postgres_table": pg_cdc_source["table_name"],
                    "key_template": "id",
                    "slot_name": pg_cdc_source["slot_name"],
                    "publication_name": pg_cdc_source["publication_name"],
                    "on_delete": [{"op": "$delete_document"}],
                }
            ],
        }
        _create_table_via_metadata_admin(stateful_api, table_name, create_payload)

        snapshot_doc = wait_until(
            lambda: _lookup_doc_if(
                stateful_api,
                table_name,
                "user-1",
                lambda doc: doc.get("name") == "Alice" and doc.get("tier") == "gold",
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert snapshot_doc is not None, (
            "CDC snapshot did not import before nested foreign join coverage\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_replication_statuses(stateful_api)}"
        )

        def joined_rows() -> dict[str, dict[str, Any]] | None:
            try:
                result = stateful_api.query_table(
                    table_name,
                    {
                        "limit": 10,
                        "fields": ["name", "tier"],
                        "join": {
                            "right_table": "pg_tier_profiles",
                            "join_type": "left",
                            "on": {
                                "left_field": "tier",
                                "right_field": "tier",
                                "operator": "eq",
                            },
                            "right_fields": ["label", "pg_addresses.city", "pg_addresses.region"],
                            "nested_join": {
                                "right_table": "pg_addresses",
                                "join_type": "left",
                                "on": {
                                    "left_field": "address_id",
                                    "right_field": "id",
                                    "operator": "eq",
                                },
                                "right_fields": ["city", "region"],
                            },
                        },
                        "foreign_sources": {
                            "pg_tier_profiles": _tier_profile_foreign_source(lookup_table),
                            "pg_addresses": _address_foreign_source(address_table),
                        },
                    },
                )
            except requests.RequestException:
                return None
            responses = result.get("responses", [])
            if not responses:
                return None
            hits = responses[0].get("hits", {}).get("hits", [])
            rows = {
                hit.get("_id", ""): hit.get("_source", {})
                for hit in hits
                if isinstance(hit, dict) and isinstance(hit.get("_source"), dict)
            }
            return rows if rows else None

        joined_snapshot = wait_until(
            lambda: (
                rows
                if (rows := joined_rows()) is not None
                and rows.get("user-1", {}).get(label_field) == "Gold Tier"
                and rows.get("user-1", {}).get(city_field) == "Seattle"
                and rows.get("user-1", {}).get(region_field) == "wa"
                else None
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert joined_snapshot is not None, (
            "nested foreign lookup join did not reflect CDC snapshot row\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_replication_statuses(stateful_api)}"
        )

        _run_psql(
            f"""
            insert into {pg_cdc_source["table_name"]} (id, name, tier)
            values ('user-2', 'Bob', 'silver');
            """
        )

        joined_insert = wait_until(
            lambda: (
                rows
                if (rows := joined_rows()) is not None
                and rows.get("user-2", {}).get("name") == "Bob"
                and rows.get("user-2", {}).get(label_field) == "Silver Tier"
                and rows.get("user-2", {}).get(city_field) == "Portland"
                and rows.get("user-2", {}).get(region_field) == "or"
                else None
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert joined_insert is not None, (
            "nested foreign lookup join did not reflect CDC streamed insert\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_replication_statuses(stateful_api)}"
        )

        _run_psql(
            f"""
            update {pg_cdc_source["table_name"]}
            set tier = 'platinum'
            where id = 'user-1';
            """
        )

        joined_update = wait_until(
            lambda: (
                rows
                if (rows := joined_rows()) is not None
                and rows.get("user-1", {}).get("tier") == "platinum"
                and rows.get("user-1", {}).get(label_field) == "Platinum Tier"
                and rows.get("user-1", {}).get(city_field) == "San Francisco"
                and rows.get("user-1", {}).get(region_field) == "ca"
                else None
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert joined_update is not None, (
            "nested foreign lookup join did not reflect CDC streamed update\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_replication_statuses(stateful_api)}\n"
            f"{_pg_replication_debug(pg_cdc_source['slot_name'], pg_cdc_source['publication_name'])}"
        )
    finally:
        _run_psql_best_effort(f"drop table if exists {lookup_table};")
        _run_psql_best_effort(f"drop table if exists {address_table};")


def test_stateful_postgres_cdc_resumes_after_restart(stateful_api, pg_cdc_source):
    table_name = f"cdc_restart_docs_{time.time_ns()}"
    create_payload = {
        "num_shards": 1,
        "replication_sources": [
            {
                "type": "postgres",
                "dsn": _pg_dsn(),
                "postgres_table": pg_cdc_source["table_name"],
                "key_template": "id",
                "slot_name": pg_cdc_source["slot_name"],
                "publication_name": pg_cdc_source["publication_name"],
                "on_delete": [{"op": "$delete_document"}],
            }
        ],
    }
    _create_table_via_metadata_admin(stateful_api, table_name, create_payload)

    snapshot_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-1",
            lambda doc: doc.get("name") == "Alice" and doc.get("tier") == "gold",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert snapshot_doc is not None, (
        "CDC snapshot did not import before restart\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )

    _run_psql(
        f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-2', 'Bob', 'silver');
        """
    )
    inserted_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-2",
            lambda doc: doc.get("name") == "Bob" and doc.get("tier") == "silver",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert inserted_doc is not None, (
        "CDC stream did not apply before restart\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )

    matching_before_restart = wait_until(
        lambda: [
            status
            for status in _metadata_replication_status_records(stateful_api)
            if status.get("slot_name") == pg_cdc_source["slot_name"]
            and status.get("publication_name") == pg_cdc_source["publication_name"]
            and status.get("stream_checkpoint")
        ],
        timeout_s=30.0,
        interval_s=0.25,
    )
    statuses_before_restart = _metadata_replication_status_records(stateful_api)
    assert matching_before_restart, f"unexpected replication statuses before restart: {statuses_before_restart!r}"
    assert any(
        status.get("phase") == "cutover_prepared"
        and status.get("prepared_checkpoint")
        for status in matching_before_restart
    ) or any(
        status.get("phase") == "streaming"
        and status.get("prepared_checkpoint")
        for status in matching_before_restart
    ), f"missing cutover-prepared status before restart: {statuses_before_restart!r}"
    prepared_checkpoint_before_restart = next(
        (status.get("prepared_checkpoint", "") for status in matching_before_restart if status.get("prepared_checkpoint")),
        "",
    )
    cutover_mode_before_restart = next(
        (status.get("cutover_mode", "") for status in matching_before_restart if status.get("cutover_mode")),
        "",
    )
    stream_checkpoint_before_restart = next(
        (status.get("stream_checkpoint", "") for status in matching_before_restart if status.get("stream_checkpoint")),
        "",
    )
    assert prepared_checkpoint_before_restart, (
        "missing prepared_checkpoint before restart\n"
        f"{statuses_before_restart!r}"
    )
    assert cutover_mode_before_restart == "exported_snapshot", (
        "unexpected cutover_mode before restart\n"
        f"{statuses_before_restart!r}"
    )
    assert stream_checkpoint_before_restart, (
        "missing stream_checkpoint before restart after streamed insert\n"
        f"{statuses_before_restart!r}"
    )

    stateful_api.restart_server()

    recovered_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-1",
            lambda doc: doc.get("name") == "Alice" and doc.get("tier") == "gold",
        ),
        timeout_s=60.0,
        interval_s=0.25,
    )
    assert recovered_doc is not None, (
        "CDC table state did not recover after restart\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_admin_snapshot(stateful_api)}"
    )

    statuses_after_restart = _metadata_replication_status_records(stateful_api)
    matching_after_restart = [
        status
        for status in statuses_after_restart
        if status.get("slot_name") == pg_cdc_source["slot_name"]
        and status.get("publication_name") == pg_cdc_source["publication_name"]
    ]
    assert matching_after_restart, (
        "missing replication status after restart\n"
        f"{_metadata_admin_snapshot(stateful_api)}"
    )
    assert any(
        status.get("prepared_checkpoint") == prepared_checkpoint_before_restart
        for status in matching_after_restart
    ), (
        "prepared_checkpoint did not persist across restart\n"
        f"before={prepared_checkpoint_before_restart!r} after={matching_after_restart!r}"
    )
    assert any(
        status.get("cutover_mode") == "exported_snapshot"
        for status in matching_after_restart
    ), (
        "cutover_mode did not persist across restart\n"
        f"{matching_after_restart!r}"
    )
    assert any(
        status.get("stream_checkpoint") == stream_checkpoint_before_restart
        for status in matching_after_restart
    ), (
        "stream_checkpoint did not persist across restart\n"
        f"before={stream_checkpoint_before_restart!r} after={matching_after_restart!r}"
    )
    status_after_restart = _metadata_status(stateful_api)
    assert status_after_restart.get("projected_replication_source_statuses_streaming", 0) >= 1
    assert status_after_restart.get("projected_replication_source_statuses_exact_cutover", 0) == 1
    assert status_after_restart.get("projected_replication_source_statuses_non_exact_cutover", 0) == 0
    assert status_after_restart.get("projected_replication_source_statuses_exported_snapshot", 0) == 1
    assert status_after_restart.get("projected_replication_source_statuses_with_success_timestamp", 0) >= 1
    assert status_after_restart.get("projected_replication_source_statuses_with_change_timestamp", 0) >= 1
    assert "projected_replication_source_lag_millis_max" in status_after_restart
    assert "projected_replication_source_observed_lag_millis_max" in status_after_restart

    _run_psql(
        f"""
        update {pg_cdc_source["table_name"]}
        set name = 'Alicia', tier = 'platinum'
        where id = 'user-1';
        """
    )

    updated_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-1",
            lambda doc: doc.get("name") == "Alicia" and doc.get("tier") == "platinum",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert updated_doc is not None, (
        "CDC stream did not resume after restart for update\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}\n"
        f"{_pg_replication_debug(pg_cdc_source['slot_name'], pg_cdc_source['publication_name'])}"
    )

    _run_psql(f"delete from {pg_cdc_source['table_name']} where id = 'user-2';")
    _wait_until_absent(stateful_api, table_name, "user-2", timeout_s=30.0, interval_s=0.25)


def test_stateful_postgres_cdc_marks_existing_slot_resume(stateful_api, pg_cdc_source):
    table_name = f"cdc_existing_slot_docs_{time.time_ns()}"
    _run_psql(f"create publication {pg_cdc_source['publication_name']} for table {pg_cdc_source['table_name']};")
    _run_psql(
        "select * from pg_create_logical_replication_slot('{slot}', 'pgoutput');".format(
            slot=pg_cdc_source["slot_name"]
        )
    )

    create_payload = {
        "num_shards": 1,
        "replication_sources": [
            {
                "type": "postgres",
                "dsn": _pg_dsn(),
                "postgres_table": pg_cdc_source["table_name"],
                "key_template": "id",
                "slot_name": pg_cdc_source["slot_name"],
                "publication_name": pg_cdc_source["publication_name"],
                "on_delete": [{"op": "$delete_document"}],
            }
        ],
    }
    _create_table_via_metadata_admin(stateful_api, table_name, create_payload)

    snapshot_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-1",
            lambda doc: doc.get("name") == "Alice" and doc.get("tier") == "gold",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert snapshot_doc is not None, (
        "CDC snapshot did not import through existing-slot path\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )

    matching = wait_until(
        lambda: [
            status
            for status in _metadata_replication_status_records(stateful_api)
            if status.get("slot_name") == pg_cdc_source["slot_name"]
            and status.get("publication_name") == pg_cdc_source["publication_name"]
            and status.get("cutover_mode") == "slot_resumed"
        ],
        timeout_s=30.0,
        interval_s=0.25,
    )
    statuses = _metadata_replication_status_records(stateful_api)
    assert matching, (
        "existing logical slot was not surfaced as slot_resumed\n"
        f"{statuses!r}\n"
        f"{_server_logs(stateful_api)}"
    )
    existing_slot_status = _metadata_status(stateful_api)
    assert existing_slot_status.get("projected_replication_source_statuses_exact_cutover", 0) == 0
    assert existing_slot_status.get("projected_replication_source_statuses_non_exact_cutover", 0) == 1
    assert existing_slot_status.get("projected_replication_source_statuses_slot_resumed", 0) == 1
    assert existing_slot_status.get("projected_replication_source_statuses_reseed_recommended", 0) >= 1
    action_hints = _metadata_admin_snapshot_json(stateful_api).get("replication_source_action_hints", [])
    assert any(
        hint.get("table_name") == table_name
        and hint.get("source_ordinal") == 0
        and hint.get("action") == "reseed_exact_cutover"
        and "reseed-exact-cutover" in hint.get("reseed_exact_cutover_path", "")
        for hint in action_hints
    ), (
        "existing-slot CDC source did not surface reseed recommendation\n"
        f"{_metadata_admin_snapshot(stateful_api)}"
    )
    source_detail = wait_until(
        lambda: next(
            (
                detail["replication_sources"][0]
                for detail in [_get_table_if_visible(stateful_api, table_name)]
                if detail is not None
                and detail.get("replication_sources")
                and isinstance(detail["replication_sources"][0], dict)
                and isinstance(detail["replication_sources"][0].get("status"), dict)
                and isinstance(detail["replication_sources"][0].get("action_hint"), dict)
            ),
            None,
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert source_detail["status"]["cutover_mode"] == "slot_resumed"
    assert source_detail["status"]["slot_name"] == pg_cdc_source["slot_name"]
    assert source_detail["action_hint"]["action"] == "reseed_exact_cutover"
    assert "reseed-exact-cutover" in source_detail["action_hint"]["reseed_exact_cutover_path"]

    _run_psql(
        f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-2', 'Bob', 'silver');
        """
    )
    inserted_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-2",
            lambda doc: doc.get("name") == "Bob" and doc.get("tier") == "silver",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert inserted_doc is not None, (
        "CDC stream did not apply through existing-slot path\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}\n"
        f"{_pg_replication_debug(pg_cdc_source['slot_name'], pg_cdc_source['publication_name'])}"
    )


def test_stateful_postgres_cdc_rejects_existing_slot_when_exact_cutover_is_required(
    stateful_api, pg_cdc_source
):
    table_name = f"cdc_exact_cutover_docs_{time.time_ns()}"
    _run_psql(f"create publication {pg_cdc_source['publication_name']} for table {pg_cdc_source['table_name']};")
    _run_psql(
        "select * from pg_create_logical_replication_slot('{slot}', 'pgoutput');".format(
            slot=pg_cdc_source["slot_name"]
        )
    )

    create_payload = {
        "num_shards": 1,
        "replication_sources": [
            {
                "type": "postgres",
                "dsn": _pg_dsn(),
                "postgres_table": pg_cdc_source["table_name"],
                "key_template": "id",
                "slot_name": pg_cdc_source["slot_name"],
                "publication_name": pg_cdc_source["publication_name"],
                "require_exact_cutover": True,
                "on_delete": [{"op": "$delete_document"}],
            }
        ],
    }
    _create_table_via_metadata_admin(stateful_api, table_name, create_payload)

    failed = wait_until(
        lambda: next(
            (
                status
                for status in _metadata_replication_status_records(stateful_api)
                if status.get("slot_name") == pg_cdc_source["slot_name"]
                and status.get("publication_name") == pg_cdc_source["publication_name"]
                and status.get("phase") == "failed"
                and status.get("last_error") == "ReplicationExactCutoverRequired"
            ),
            None,
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert failed is not None, (
        "existing logical slot did not fail exact-cutover enforcement\n"
        f"{_metadata_replication_statuses(stateful_api)}\n"
        f"{_server_logs(stateful_api)}"
    )
    assert failed.get("failure_class") == "terminal"
    assert failed.get("cutover_mode", "") == ""

    exact_required_status = _metadata_status(stateful_api)
    assert exact_required_status.get("projected_replication_source_statuses_exact_cutover", 0) == 0
    assert exact_required_status.get("projected_replication_source_statuses_non_exact_cutover", 0) == 0
    assert exact_required_status.get("projected_replication_source_statuses_terminal_failed", 0) >= 1
    assert exact_required_status.get("projected_replication_source_statuses_reseed_recommended", 0) >= 1

    time.sleep(1.0)
    assert _lookup_doc(stateful_api, table_name, "user-1") is None, (
        "exact-cutover-required table should not import snapshot rows through slot_resumed fallback\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )


def test_stateful_postgres_cdc_reseed_exact_cutover_recovers_from_existing_slot(
    stateful_api, pg_cdc_source
):
    table_name = f"cdc_reseed_exact_cutover_docs_{time.time_ns()}"
    _cleanup_reseed_artifacts_best_effort()
    _run_psql(f"create publication {pg_cdc_source['publication_name']} for table {pg_cdc_source['table_name']};")
    _run_psql(
        "select * from pg_create_logical_replication_slot('{slot}', 'pgoutput');".format(
            slot=pg_cdc_source["slot_name"]
        )
    )
    new_slot_name = ""
    new_publication_name = ""
    try:
        create_payload = {
            "num_shards": 1,
            "replication_sources": [
                {
                    "type": "postgres",
                    "dsn": _pg_dsn(),
                    "postgres_table": pg_cdc_source["table_name"],
                    "key_template": "id",
                    "slot_name": pg_cdc_source["slot_name"],
                    "publication_name": pg_cdc_source["publication_name"],
                    "require_exact_cutover": True,
                    "on_delete": [{"op": "$delete_document"}],
                }
            ],
        }
        _create_table_via_metadata_admin(stateful_api, table_name, create_payload)

        failed = wait_until(
            lambda: next(
                (
                    status
                    for status in _metadata_replication_status_records(stateful_api)
                    if status.get("slot_name") == pg_cdc_source["slot_name"]
                    and status.get("publication_name") == pg_cdc_source["publication_name"]
                    and status.get("phase") == "failed"
                    and status.get("last_error") == "ReplicationExactCutoverRequired"
                ),
                None,
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert failed is not None, (
            "expected initial exact-cutover failure before reseed\n"
            f"{_metadata_replication_statuses(stateful_api)}\n"
            f"{_server_logs(stateful_api)}"
        )

        reseed_result = _reseed_exact_cutover(stateful_api, table_name, 0)
        new_slot_name = reseed_result["slot_name"]
        new_publication_name = reseed_result["publication_name"]
        assert new_slot_name != pg_cdc_source["slot_name"]
        assert new_publication_name != pg_cdc_source["publication_name"]

        table_detail = wait_until(
            lambda: next(
                (
                    detail
                    for detail in [_get_table_if_visible(stateful_api, table_name)]
                    if detail is not None
                    and detail["replication_sources"][0]["slot_name"] == new_slot_name
                    and detail["replication_sources"][0]["publication_name"] == new_publication_name
                ),
                None,
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert table_detail["replication_sources"][0]["slot_name"] == new_slot_name
        assert table_detail["replication_sources"][0]["publication_name"] == new_publication_name
        assert table_detail["replication_sources"][0]["require_exact_cutover"] is True

        snapshot_doc = wait_until(
            lambda: _lookup_doc_if(
                stateful_api,
                table_name,
                "user-1",
                lambda doc: doc.get("name") == "Alice" and doc.get("tier") == "gold",
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert snapshot_doc is not None, (
            "reseeded exact cutover did not import snapshot rows\n"
            f"{_metadata_replication_statuses(stateful_api)}\n"
            f"{_server_logs(stateful_api)}"
        )

        reseeded_status = wait_until(
            lambda: next(
                (
                    status
                    for status in _metadata_replication_status_records(stateful_api)
                    if status.get("slot_name") == new_slot_name
                    and status.get("publication_name") == new_publication_name
                    and status.get("cutover_mode") == "exported_snapshot"
                ),
                None,
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert reseeded_status is not None, (
            "reseeded exact cutover did not surface exported_snapshot status\n"
            f"{_metadata_replication_statuses(stateful_api)}"
        )

        _run_psql(
            f"""
            insert into {pg_cdc_source["table_name"]} (id, name, tier)
            values ('user-2', 'Bob', 'silver');
            """
        )
        inserted_doc = wait_until(
            lambda: _lookup_doc_if(
                stateful_api,
                table_name,
                "user-2",
                lambda doc: doc.get("name") == "Bob" and doc.get("tier") == "silver",
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert inserted_doc is not None, (
            "reseeded exact cutover did not resume streaming on the fresh slot\n"
            f"{_metadata_replication_statuses(stateful_api)}\n"
            f"{_pg_replication_debug(new_slot_name, new_publication_name)}"
        )
    finally:
        if new_publication_name:
            _run_psql_best_effort(f"drop publication if exists {new_publication_name};")
        if new_slot_name:
            _drop_replication_slot_when_inactive(new_slot_name)


def test_stateful_postgres_cdc_recovers_publication_loss_but_marks_missing_slot_terminal(
    stateful_api, pg_cdc_source
):
    table_name = f"cdc_recovery_docs_{time.time_ns()}"
    create_payload = {
        "num_shards": 1,
        "replication_sources": [
            {
                "type": "postgres",
                "dsn": _pg_dsn(),
                "postgres_table": pg_cdc_source["table_name"],
                "key_template": "id",
                "slot_name": pg_cdc_source["slot_name"],
                "publication_name": pg_cdc_source["publication_name"],
                "on_delete": [{"op": "$delete_document"}],
            }
        ],
    }
    _create_table_via_metadata_admin(stateful_api, table_name, create_payload)

    snapshot_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-1",
            lambda doc: doc.get("name") == "Alice" and doc.get("tier") == "gold",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert snapshot_doc is not None, (
        "CDC snapshot did not import before recovery checks\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )

    _run_psql_best_effort(f"drop publication if exists {pg_cdc_source['publication_name']};")
    recreated_publication = wait_until(
        lambda: _psql_scalar_best_effort(
            f"select coalesce(pubname,'') from pg_publication where pubname = '{pg_cdc_source['publication_name']}'"
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert recreated_publication == pg_cdc_source["publication_name"], (
        "CDC polling did not recreate the dropped publication\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )

    _run_psql(
        f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-2', 'Bob', 'silver');
        """
    )

    inserted_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-2",
            lambda doc: doc.get("name") == "Bob" and doc.get("tier") == "silver",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert inserted_doc is not None, (
        "CDC stream did not recover after publication loss\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}\n"
        f"{_pg_replication_debug(pg_cdc_source['slot_name'], pg_cdc_source['publication_name'])}"
    )

    _drop_replication_slot_when_inactive(pg_cdc_source["slot_name"])
    _run_psql(
        f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-3', 'Carol', 'bronze');
        """
    )

    failed_statuses = wait_until(
        lambda: [
            status
            for status in _metadata_replication_status_records(stateful_api)
            if status.get("slot_name") == pg_cdc_source["slot_name"]
            and status.get("publication_name") == pg_cdc_source["publication_name"]
            and status.get("phase") == "streaming_failed"
            and status.get("failure_class") == "terminal"
        ],
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert failed_statuses, (
        "missing terminal CDC status after logical slot loss\n"
        f"{_metadata_replication_statuses(stateful_api)}\n"
        f"{_server_logs(stateful_api)}"
    )
    failed_summary = _metadata_status(stateful_api)
    assert failed_summary.get("projected_replication_source_statuses_terminal_failed", 0) >= 1
    assert failed_summary.get("projected_replication_source_statuses_with_last_error", 0) >= 1
    assert failed_summary.get("projected_replication_source_statuses_slot_missing_failed", 0) >= 1
    assert any(
        status.get("last_error") == "ForeignReplicationSlotMissing"
        for status in failed_statuses
    ), (
        "slot loss was not surfaced explicitly\n"
        f"{failed_statuses!r}"
    )
