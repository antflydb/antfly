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

import json
import os
import re
import subprocess
import time
from typing import Any

import pytest
import requests
from conftest import (
    ANTFLY_PUBLIC_API_ROOT,
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    EMBEDDINGGEMMA2_MODEL,
    INFERENCE_PUBLIC_API_ROOT,
    StandaloneAntflyServer,
    StatefulAntflyServer,
    internal_service_headers,
    lookup_key_path,
    raise_request_error_with_logs,
    resolve_binary_path,
    wait_for_server,
    with_api_root,
)
from helpers import wait_until
from port_reservations import find_free_port

pytestmark = pytest.mark.postgres_integration

DEFAULT_PG_DSN = "postgres://localhost:5432/postgres?sslmode=disable"
PSQL_BIN = os.environ.get(
    "ANTFLY_TEST_PSQL_BIN", "/opt/homebrew/opt/postgresql@18/bin/psql"
)


def _pg_dsn() -> str:
    return (
        os.environ.get("ANTFLY_TEST_PG_DSN")
        or os.environ.get("PG_DSN")
        or DEFAULT_PG_DSN
    )


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


def _cleanup_stale_e2e_cdc_resources() -> None:
    # The Antfly process owns the active slot for a test. Pytest tears the
    # independent PostgreSQL fixture down before that process on some runs, so
    # its immediate cleanup can legitimately find the slot active. The next
    # case runs only after the prior process has exited; reclaim those narrowly
    # named inactive resources here before PostgreSQL's bounded slot pool is
    # consumed. Never terminate or remove an active slot.
    deadline = time.monotonic() + 5.0
    while True:
        _run_psql_best_effort("""
            select pg_drop_replication_slot(slot_name)
            from pg_replication_slots
            where slot_name like 'antfly_e2e_%' and not active;
            """)
        remaining = _psql_scalar_best_effort(
            "select count(*) from pg_replication_slots "
            "where slot_name like 'antfly_e2e_%';"
        )
        if remaining in {"", "0"} or time.monotonic() >= deadline:
            break
        time.sleep(0.1)
    _run_psql_best_effort("""
        DO $$
        DECLARE rec record;
        BEGIN
          FOR rec IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'antfly_e2e_%' LOOP
            EXECUTE 'DROP PUBLICATION IF EXISTS ' || quote_ident(rec.pubname);
          END LOOP;
        END $$;
        """)


def _lookup_doc(stateful_api, table_name: str, key: str) -> dict[str, Any] | None:
    try:
        return stateful_api.lookup_key(table_name, key)
    except requests.RequestException:
        return None


def _lookup_doc_if(
    stateful_api, table_name: str, key: str, predicate
) -> dict[str, Any] | None:
    doc = _lookup_doc(stateful_api, table_name, key)
    if doc is None:
        return None
    return doc if predicate(doc) else None


def _get_table_if_visible(stateful_api, table_name: str) -> dict[str, Any] | None:
    try:
        return stateful_api.get_table(table_name)
    except requests.RequestException:
        return None


def _wait_until_absent(
    stateful_api, table_name: str, key: str, *, timeout_s: float, interval_s: float
) -> None:
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
    matches = re.findall(
        r"(?:standalone )?metadata admin api listening on (http://[^\s]+)", logs
    )
    if matches:
        return matches[-1].rstrip("/")
    return ""


def _is_standalone(stateful_api) -> bool:
    return getattr(stateful_api, "mode", "cluster") == "standalone"


def _require_cluster(stateful_api, reason: str) -> None:
    # Standalone has no metadata admin API, so scenarios that drive the
    # internal create, reseed, or cutover admin routes stay cluster-only.
    if _is_standalone(stateful_api):
        pytest.skip(f"cluster-only CDC scenario: {reason}")


def _public_replication_status_records(stateful_api) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        tables = stateful_api._check(stateful_api._request("GET", "/tables"))
    except (requests.RequestException, AssertionError):
        return records
    for table in tables if isinstance(tables, list) else []:
        name = table.get("name") if isinstance(table, dict) else None
        if not name:
            continue
        response = stateful_api._request("GET", f"/tables/{name}/sources/managed")
        if response.status_code != 200:
            continue
        progress = response.json().get("progress")
        if isinstance(progress, list):
            records.extend(record for record in progress if isinstance(record, dict))
    return records


def _status_counters_from_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Standalone has no metadata status endpoint; derive its CDC counters from
    the same status records the public managed-sources API returns."""

    def count(predicate) -> int:
        return sum(1 for record in records if predicate(record))

    def max_of(field: str) -> int:
        return max((int(record.get(field, 0) or 0) for record in records), default=0)

    lag_max = max_of("lag_millis")
    return {
        "projected_replication_source_statuses_streaming": count(
            lambda r: r.get("phase") == "streaming"
        ),
        "projected_replication_source_statuses_with_consecutive_failures": count(
            lambda r: int(r.get("consecutive_failures", 0) or 0) > 0
        ),
        "projected_replication_source_consecutive_failures_max": max_of(
            "consecutive_failures"
        ),
        "projected_replication_source_lag_millis_max": lag_max,
        "projected_replication_source_observed_lag_millis_max": lag_max,
        "projected_replication_source_statuses_with_source_commit_timestamp": count(
            lambda r: int(r.get("last_source_commit_at_ms", 0) or 0) > 0
        ),
        "projected_replication_source_last_change_applied_at_ms_max": max_of(
            "last_change_applied_at_ms"
        ),
        "projected_replication_source_statuses_exact_cutover": count(
            lambda r: int(r.get("cutover_authority_id", 0) or 0) != 0
        ),
        "projected_replication_source_statuses_non_exact_cutover": count(
            lambda r: int(r.get("cutover_authority_id", 0) or 0) == 0
        ),
        "projected_replication_source_statuses_exported_snapshot": count(
            lambda r: r.get("cutover_mode") == "exported_snapshot"
        ),
        "projected_replication_source_statuses_with_success_timestamp": count(
            lambda r: int(r.get("last_success_at_ms", 0) or 0) > 0
        ),
        "projected_replication_source_statuses_with_change_timestamp": count(
            lambda r: int(r.get("last_change_applied_at_ms", 0) or 0) > 0
        ),
    }


def _metadata_admin_snapshot(stateful_api) -> str:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        return ""
    try:
        response = requests.get(
            f"{base_url}/metadata/v1/admin/snapshot",
            headers=internal_service_headers(),
            timeout=5,
        )
        response.raise_for_status()
    except requests.RequestException:
        return ""
    return response.text.strip()


def _metadata_admin_snapshot_json(stateful_api) -> dict[str, Any]:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        return {}
    try:
        response = requests.get(
            f"{base_url}/metadata/v1/admin/snapshot",
            headers=internal_service_headers(),
            timeout=5,
        )
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
        return _public_replication_status_records(stateful_api)
    try:
        response = requests.get(
            f"{base_url}/metadata/v1/admin/snapshot",
            headers=internal_service_headers(),
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        return []
    statuses = payload.get("replication_source_statuses")
    if not isinstance(statuses, list):
        return []
    return [status for status in statuses if isinstance(status, dict)]


def _metadata_replication_status_records_for_source(
    stateful_api, external_table: str, source_ordinal: int = 0
) -> list[dict[str, Any]]:
    """Match a logical source without assuming its fenced physical slot names."""
    return [
        status
        for status in _metadata_replication_status_records(stateful_api)
        if status.get("external_table") == external_table
        and status.get("source_ordinal") == source_ordinal
    ]


def _metadata_status(stateful_api) -> dict[str, Any]:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        if _is_standalone(stateful_api):
            return _status_counters_from_records(
                _public_replication_status_records(stateful_api)
            )
        return {}
    try:
        response = requests.get(f"{base_url}/metadata/v1/status", timeout=5)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _reseed_exact_cutover(
    stateful_api, table_name: str, source_ordinal: int
) -> dict[str, Any]:
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        raise AssertionError(
            "metadata admin base url unavailable for CDC reseed\n"
            f"{_server_logs(stateful_api)}"
        )
    response = requests.post(
        f"{base_url}/internal/v1/tables/{table_name}/replication-sources/{source_ordinal}/reseed-exact-cutover",
        headers={"Connection": "close", **internal_service_headers()},
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


def _drop_replication_slot_when_inactive(
    slot_name: str, *, timeout_s: float = 10.0
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        active = _psql_scalar_best_effort(
            f"select coalesce(active::text,'') from pg_replication_slots where slot_name = '{slot_name}'"
        ).lower()
        if active in {"", "f", "false"}:
            _run_psql_best_effort(
                f"select pg_drop_replication_slot('{slot_name}') "
                f"from pg_replication_slots where slot_name = '{slot_name}' and not active;"
            )
            remaining = _psql_scalar_best_effort(
                f"select coalesce(slot_name,'') from pg_replication_slots where slot_name = '{slot_name}'"
            )
            if not remaining:
                return
        time.sleep(0.25)
    raise AssertionError(
        f"replication slot {slot_name} remained active and could not be dropped"
    )


def _create_table_public(
    stateful_api, table_name: str, payload: dict[str, Any]
) -> None:
    # Exercise the same normalization and authorization path users call. When
    # routes target another table, this is also where the caller's durable
    # destination grant is sealed into the catalog definition.
    response = stateful_api._request("POST", f"/tables/{table_name}", payload)
    if response.status_code not in (200, 201):
        body = response.text.strip()
        raise AssertionError(
            f"public CDC table create failed: {response.status_code} {body}\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_admin_snapshot(stateful_api)}"
        )


def _create_table_via_metadata_admin(
    stateful_api, table_name: str, payload: dict[str, Any]
) -> None:
    if _is_standalone(stateful_api):
        # Standalone has no internal metadata route; the public create runs
        # the same catalog derivation against its single local owner.
        _create_table_public(stateful_api, table_name, payload)
        return
    base_url = _metadata_admin_base_url(stateful_api)
    if not base_url:
        raise AssertionError(
            "metadata admin base url unavailable for CDC E2E\n"
            f"{_server_logs(stateful_api)}"
        )
    response = requests.post(
        f"{base_url}/internal/v1/tables/{table_name}",
        json=payload,
        headers={
            "Content-Type": "application/json",
            "Connection": "close",
            **internal_service_headers(),
        },
        timeout=30,
    )
    if response.status_code != 201:
        body = response.text.strip()
        raise AssertionError(
            f"metadata internal create failed: {response.status_code} {body}\n"
            f"{_server_logs(stateful_api)}\n"
            f"{_metadata_admin_snapshot(stateful_api)}"
        )


def _metadata_table_id(stateful_api, table_name: str) -> int | None:
    if _is_standalone(stateful_api):
        public_tables = stateful_api._check(stateful_api._request("GET", "/tables"))
        for public_table in public_tables:
            if public_table.get("name") == table_name:
                return int(public_table["table_id"])
        return None
    payload = _metadata_admin_snapshot_json(stateful_api)
    for table in payload.get("tables", []):
        if isinstance(table, dict) and table.get("name") == table_name:
            return int(table["table_id"])
    # Public creates have a logical catalog name and an opaque physical name.
    # Join the public label by immutable ID, and require the physical record
    # to exist in this snapshot rather than treating public visibility as proof.
    public_tables = stateful_api._check(stateful_api._request("GET", "/tables"))
    for public_table in public_tables:
        if public_table.get("name") != table_name:
            continue
        table_id = int(public_table["table_id"])
        if any(
            isinstance(table, dict) and int(table["table_id"]) == table_id
            for table in payload.get("tables", [])
        ):
            return table_id
    return None


@pytest.fixture(scope="function", params=["cluster", "standalone"])
def cdc_stateful_api(request, pg_cdc_source):
    # Keep the provider fixture alive until after the managed server stops so
    # its exact-cutover slots are inactive and can be removed deterministically.
    del pg_cdc_source
    mode = request.param
    base_url = os.environ.get("ANTFLY_STATEFUL_URL")
    if base_url and mode == "standalone":
        pytest.skip("ANTFLY_STATEFUL_URL selects one external server")
    server = None
    default_root = os.environ.get("ANTFLY_STATEFUL_API_ROOT")
    if not base_url:
        binary = resolve_binary_path(
            os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
        )
        if not os.path.exists(binary):
            pytest.skip(f"Public API binary not found: {binary}")
        port = find_free_port()
        if mode == "standalone":
            server = StandaloneAntflyServer(binary, "127.0.0.1", port)
        else:
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
            self.mode = mode

        def _raise_request_error(self, err: requests.RequestException) -> None:
            raise_request_error_with_logs(err, self._server)

        def _check(self, response: requests.Response) -> Any:
            if response.status_code >= 400:
                body = response.text.strip()
                logs = (
                    self._server.debug_logs().strip()
                    if self._server is not None
                    else ""
                )
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

        def _request(
            self, method: str, path: str, payload: dict | None = None
        ) -> requests.Response:
            try:
                return self.s.request(
                    method, f"{self.url}{path}", json=payload, timeout=30
                )
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

        def query_table(
            self, table_name: str, payload: dict[str, Any]
        ) -> dict[str, Any]:
            return self._check(
                self._request("POST", f"/tables/{table_name}/query", payload)
            )

        def restart_server(self) -> None:
            server = self._server
            if server is None or not hasattr(server, "restart"):
                raise AssertionError(
                    "restart is only available for locally managed stateful servers"
                )
            self.s.close()
            server.restart()
            if not wait_for_server(self.url, timeout=20):
                logs = server.debug_logs().strip()
                raise AssertionError(
                    f"stateful server failed to restart at {self.url}\n{logs}"
                )
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

    _cleanup_stale_e2e_cdc_resources()

    suffix = str(time.time_ns())
    table_name = f"antfly_e2e_pg_cdc_{suffix}"
    slot_name = f"antfly_e2e_slot_{suffix}"
    publication_name = f"antfly_e2e_pub_{suffix}"

    _run_psql(f"""
        create table {table_name} (
            id text primary key,
            name text not null,
            tier text not null
        );
        insert into {table_name} (id, name, tier) values
            ('user-1', 'Alice', 'gold');
        """)

    source = {
        "table_name": table_name,
        "slot_name": slot_name,
        "publication_name": publication_name,
        "cleanup_slot_names": [slot_name],
        "cleanup_publication_names": [publication_name],
    }
    try:
        yield source
    finally:
        for cleanup_slot_name in source["cleanup_slot_names"]:
            physical_prefix = cleanup_slot_name[:26]
            _run_psql_best_effort(
                "select pg_drop_replication_slot(slot_name) "
                "from pg_replication_slots "
                "where not active and (slot_name = '{slot}' "
                "or starts_with(slot_name, '{prefix}_af_'));".format(
                    slot=cleanup_slot_name, prefix=physical_prefix
                )
            )
        for cleanup_publication_name in source["cleanup_publication_names"]:
            physical_prefix = cleanup_publication_name[:26]
            _run_psql_best_effort(
                "DO $$ DECLARE rec record; BEGIN "
                "FOR rec IN SELECT pubname FROM pg_publication "
                "WHERE pubname = '{publication}' "
                "OR starts_with(pubname, '{prefix}_af_') LOOP "
                "EXECUTE 'DROP PUBLICATION IF EXISTS ' || quote_ident(rec.pubname); "
                "END LOOP; END $$;".format(
                    publication=cleanup_publication_name, prefix=physical_prefix
                )
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
    _create_table_public(stateful_api, table_name, create_payload)

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
    assert (
        table_detail["replication_sources"][0]["slot_name"]
        == pg_cdc_source["slot_name"]
    )
    assert (
        table_detail["replication_sources"][0]["publication_name"]
        == pg_cdc_source["publication_name"]
    )
    table_id = _metadata_table_id(stateful_api, table_name)
    assert table_id is not None

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
        f"{_metadata_replication_statuses(stateful_api)}\n"
        f"{_metadata_admin_snapshot(stateful_api)}"
    )
    _run_psql(f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-2', 'Bob', 'silver');
        """)

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

    _run_psql(f"""
        update {pg_cdc_source["table_name"]}
        set name = 'Alicia', tier = 'platinum'
        where id = 'user-1';
        """)

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
    assert (
        status_after_update.get("projected_replication_source_statuses_streaming", 0)
        >= 1
    )
    assert (
        status_after_update.get(
            "projected_replication_source_statuses_with_consecutive_failures", 0
        )
        == 0
    )
    assert (
        status_after_update.get(
            "projected_replication_source_consecutive_failures_max", 0
        )
        == 0
    )
    assert "projected_replication_source_lag_millis_max" in status_after_update
    assert "projected_replication_source_observed_lag_millis_max" in status_after_update
    assert status_after_update.get(
        "projected_replication_source_observed_lag_millis_max", 0
    ) >= status_after_update.get("projected_replication_source_lag_millis_max", 0)
    assert (
        status_after_update.get(
            "projected_replication_source_statuses_with_source_commit_timestamp", 0
        )
        >= 1
    )
    matching_after_update = wait_until(
        lambda: [
            status
            for status in _metadata_replication_status_records(stateful_api)
            if int(status.get("table_id", 0)) == table_id
            and int(status.get("source_ordinal", -1)) == 0
            and status.get("last_source_commit_at_ms", 0) > 0
        ],
        timeout_s=30.0,
        interval_s=0.1,
    )
    assert matching_after_update, (
        "replication status did not expose source commit timestamps after streamed update\n"
        f"{matching_after_update!r}"
    )
    assert (
        status_after_update.get(
            "projected_replication_source_last_change_applied_at_ms_max", 0
        )
        > 0
    )

    _run_psql(f"delete from {pg_cdc_source['table_name']} where id = 'user-2';")
    _wait_until_absent(
        stateful_api, table_name, "user-2", timeout_s=30.0, interval_s=0.25
    )


def test_stateful_postgres_cdc_emits_committed_destination_changefeed(pg_cdc_source):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    table = f"cdc_feed_{time.time_ns()}"
    server = StatefulAntflyServer(binary, "127.0.0.1", 0, auth_enabled=True)
    try:
        with requests.Session() as client:
            client.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            response = client.post(
                f"{server.api_url}/tables/{table}",
                json={
                    "num_shards": 1,
                    "replication_sources": [
                        {
                            "type": "postgres",
                            "dsn": _pg_dsn(),
                            "postgres_table": pg_cdc_source["table_name"],
                            "key_template": "id",
                            "slot_name": pg_cdc_source["slot_name"],
                            "publication_name": pg_cdc_source["publication_name"],
                        }
                    ],
                },
                timeout=30,
            )
            assert response.status_code in (200, 201), response.text

            def lookup(key):
                reply = client.get(
                    f"{server.api_url}{lookup_key_path(table, key)}", timeout=5
                )
                if reply.status_code == 404:
                    return None
                reply.raise_for_status()
                return reply.json()

            assert wait_until(
                lambda: lookup("user-1"), timeout_s=30.0, interval_s=0.25
            ), server.debug_logs()
            feeds = f"{server.api_url}/changefeeds"
            reply = client.post(
                feeds,
                headers={"Idempotency-Key": table},
                json={"tables": [table], "start": "now", "include": "rows"},
                timeout=30,
            )
            assert reply.status_code == 201, reply.text
            created = reply.json()
            feed = f"{feeds}/{created['id']}"
            cursor = created["cursor"]
            _run_psql(f"""
                begin;
                insert into {pg_cdc_source["table_name"]} (id, name, tier)
                    values ('rolled-back', 'Uncommitted', 'silver');
                rollback;
                insert into {pg_cdc_source["table_name"]} (id, name, tier)
                    values ('user-2', 'Outbound', 'silver');
            """)
            event = None
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                page = client.get(f"{feed}/events", params={"after": cursor}, timeout=5)
                if page.status_code == 503 or (
                    page.status_code == 409 and "changefeed_retry_required" in page.text
                ):
                    time.sleep(0.25)
                    continue
                page.raise_for_status()
                latest = page.headers["Antfly-Changefeed-Cursor"]
                body = page.json()
                for item in body["events"]:
                    name = (item.get("after") or {}).get("name")
                    assert name != "Uncommitted"
                    if name == "Outbound":
                        event = item
                        break
                if event is not None:
                    break
                if latest != cursor:
                    acknowledged = client.post(
                        f"{feed}/ack", json={"cursor": latest}, timeout=5
                    )
                    acknowledged.raise_for_status()
                    cursor = latest
                time.sleep(0.25)
            assert event is not None, server.debug_logs()
            assert event["kind"] == "document_committed"
            assert event["before"] is None
            assert body["transaction"]["complete"] is True
            assert event["transaction_id"] == body["transaction"]["transaction_id"]
            assert lookup("user-2") is not None
            assert lookup("rolled-back") is None

            server.restart()
            replay = client.get(f"{feed}/events", params={"after": cursor}, timeout=5)
            replay.raise_for_status()
            assert replay.headers["Antfly-Changefeed-Cursor"] == latest
            assert replay.json() == body

            ack = client.post(f"{feed}/ack", json={"cursor": latest}, timeout=5)
            ack.raise_for_status()
            assert ack.json()["cursor"] == latest

            def close():
                reply = client.delete(feed, timeout=5)
                if reply.status_code == 503 or (
                    reply.status_code == 409
                    and "changefeed_retry_required" in reply.text
                ):
                    return False
                assert reply.status_code == 204, reply.text
                return True

            assert wait_until(close, timeout_s=12.0, interval_s=0.25), (
                server.debug_logs()
            )
    finally:
        server.stop()


def test_stateful_postgres_cdc_table_joins_with_foreign_lookup(
    stateful_api, pg_cdc_source
):
    table_name = f"cdc_join_docs_{time.time_ns()}"
    lookup_table = f"antfly_e2e_pg_tier_lookup_{time.time_ns()}"
    joined_field = "pg_tier_lookup.label"
    _run_psql(f"""
        create table {lookup_table} (
            tier text primary key,
            label text not null
        );
        insert into {lookup_table} (tier, label) values
            ('gold', 'Gold Tier'),
            ('silver', 'Silver Tier'),
            ('platinum', 'Platinum Tier');
        """)
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

        _run_psql(f"""
            insert into {pg_cdc_source["table_name"]} (id, name, tier)
            values ('user-2', 'Bob', 'silver');
            """)

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

        _run_psql(f"""
            update {pg_cdc_source["table_name"]}
            set tier = 'platinum'
            where id = 'user-1';
            """)

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


def test_stateful_postgres_cdc_table_joins_with_nested_foreign_lookup(
    stateful_api, pg_cdc_source
):
    table_name = f"cdc_nested_join_docs_{time.time_ns()}"
    lookup_table = f"antfly_e2e_pg_tier_profile_{time.time_ns()}"
    address_table = f"antfly_e2e_pg_tier_addresses_{time.time_ns()}"
    label_field = "pg_tier_profiles.label"
    city_field = "pg_tier_profiles.pg_addresses.city"
    region_field = "pg_tier_profiles.pg_addresses.region"
    _run_psql(f"""
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
        """)
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
                            "right_fields": [
                                "label",
                                "pg_addresses.city",
                                "pg_addresses.region",
                            ],
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
                            "pg_tier_profiles": _tier_profile_foreign_source(
                                lookup_table
                            ),
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

        _run_psql(f"""
            insert into {pg_cdc_source["table_name"]} (id, name, tier)
            values ('user-2', 'Bob', 'silver');
            """)

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

        _run_psql(f"""
            update {pg_cdc_source["table_name"]}
            set tier = 'platinum'
            where id = 'user-1';
            """)

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
    table_id = _metadata_table_id(stateful_api, table_name)
    assert table_id is not None

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

    _run_psql(f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-2', 'Bob', 'silver');
        """)
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
            if int(status.get("table_id", 0)) == table_id
            and int(status.get("source_ordinal", -1)) == 0
            and status.get("stream_checkpoint")
        ],
        timeout_s=30.0,
        interval_s=0.25,
    )
    statuses_before_restart = _metadata_replication_status_records(stateful_api)
    assert matching_before_restart, (
        f"unexpected replication statuses before restart: {statuses_before_restart!r}"
    )
    assert any(
        status.get("phase") == "cutover_prepared" and status.get("prepared_checkpoint")
        for status in matching_before_restart
    ) or any(
        status.get("phase") == "streaming" and status.get("prepared_checkpoint")
        for status in matching_before_restart
    ), f"missing cutover-prepared status before restart: {statuses_before_restart!r}"
    prepared_checkpoint_before_restart = next(
        (
            status.get("prepared_checkpoint", "")
            for status in matching_before_restart
            if status.get("prepared_checkpoint")
        ),
        "",
    )
    cutover_mode_before_restart = next(
        (
            status.get("cutover_mode", "")
            for status in matching_before_restart
            if status.get("cutover_mode")
        ),
        "",
    )
    stream_checkpoint_before_restart = next(
        (
            status.get("stream_checkpoint", "")
            for status in matching_before_restart
            if status.get("stream_checkpoint")
        ),
        "",
    )
    assert prepared_checkpoint_before_restart, (
        f"missing prepared_checkpoint before restart\n{statuses_before_restart!r}"
    )
    assert cutover_mode_before_restart == "exported_snapshot", (
        f"unexpected cutover_mode before restart\n{statuses_before_restart!r}"
    )
    assert stream_checkpoint_before_restart, (
        "missing stream_checkpoint before restart after streamed insert\n"
        f"{statuses_before_restart!r}"
    )
    physical_slot_name = matching_before_restart[0]["slot_name"]
    physical_publication_name = matching_before_restart[0]["publication_name"]

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

    # Document recovery and the metadata replication status both replay from
    # durable state after restart, but through separate paths (the data
    # group's storage recovery vs. the metadata group's Raft log replay).
    # `recovered_doc` only proves the data group has caught up; the metadata
    # group can still be a few log entries behind at that exact instant,
    # especially under load. Poll here instead of reading the status once,
    # the same way the pre-restart checks above poll for status records to
    # appear.
    def matching_statuses_caught_up() -> list[dict[str, Any]] | None:
        statuses = _metadata_replication_status_records(stateful_api)
        matching = [
            status
            for status in statuses
            if int(status.get("table_id", 0)) == table_id
            and int(status.get("source_ordinal", -1)) == 0
        ]
        if not matching:
            return None
        if not any(
            status.get("prepared_checkpoint") == prepared_checkpoint_before_restart
            and status.get("stream_checkpoint") == stream_checkpoint_before_restart
            for status in matching
        ):
            return None
        return matching

    matching_after_restart = wait_until(
        matching_statuses_caught_up,
        timeout_s=30.0,
        interval_s=0.25,
    )
    statuses_after_restart = _metadata_replication_status_records(stateful_api)
    assert matching_after_restart, (
        "replication status after restart never caught up to the "
        "pre-restart checkpoint\n"
        f"before=(prepared={prepared_checkpoint_before_restart!r}, "
        f"stream={stream_checkpoint_before_restart!r}) "
        f"after={statuses_after_restart!r}"
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
    ), f"cutover_mode did not persist across restart\n{matching_after_restart!r}"
    assert all(
        status.get("slot_name") == physical_slot_name
        and status.get("publication_name") == physical_publication_name
        for status in matching_after_restart
    ), "restart changed the exact-cutover PostgreSQL resource identity"
    assert any(
        status.get("stream_checkpoint") == stream_checkpoint_before_restart
        for status in matching_after_restart
    ), (
        "stream_checkpoint did not persist across restart\n"
        f"before={stream_checkpoint_before_restart!r} after={matching_after_restart!r}"
    )
    status_after_restart = _metadata_status(stateful_api)
    assert (
        status_after_restart.get("projected_replication_source_statuses_streaming", 0)
        >= 1
    )
    assert (
        status_after_restart.get(
            "projected_replication_source_statuses_exact_cutover", 0
        )
        == 1
    )
    assert (
        status_after_restart.get(
            "projected_replication_source_statuses_non_exact_cutover", 0
        )
        == 0
    )
    assert (
        status_after_restart.get(
            "projected_replication_source_statuses_exported_snapshot", 0
        )
        == 1
    )
    assert (
        status_after_restart.get(
            "projected_replication_source_statuses_with_success_timestamp", 0
        )
        >= 1
    )
    assert (
        status_after_restart.get(
            "projected_replication_source_statuses_with_change_timestamp", 0
        )
        >= 1
    )
    assert "projected_replication_source_lag_millis_max" in status_after_restart
    assert (
        "projected_replication_source_observed_lag_millis_max" in status_after_restart
    )

    _run_psql(f"""
        update {pg_cdc_source["table_name"]}
        set name = 'Alicia', tier = 'platinum'
        where id = 'user-1';
        """)

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
    _wait_until_absent(
        stateful_api, table_name, "user-2", timeout_s=30.0, interval_s=0.25
    )


def test_stateful_postgres_cdc_exact_cutover_isolates_preexisting_logical_slot(
    stateful_api, pg_cdc_source
):
    _require_cluster(stateful_api, "drives exact-cutover admin state")
    table_name = f"cdc_existing_slot_docs_{time.time_ns()}"
    _run_psql(
        f"create publication {pg_cdc_source['publication_name']} for table {pg_cdc_source['table_name']};"
    )
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
    table_id = _metadata_table_id(stateful_api, table_name)
    assert table_id is not None

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
            if int(status.get("table_id", 0)) == table_id
            and int(status.get("source_ordinal", -1)) == 0
            and status.get("cutover_mode") == "exported_snapshot"
        ],
        timeout_s=30.0,
        interval_s=0.25,
    )
    statuses = _metadata_replication_status_records(stateful_api)
    assert matching, (
        "exact cutover did not publish its isolated physical resources\n"
        f"{statuses!r}\n"
        f"{_server_logs(stateful_api)}"
    )
    physical_status = matching[0]
    assert physical_status["slot_name"] != pg_cdc_source["slot_name"]
    assert physical_status["publication_name"] != pg_cdc_source["publication_name"]
    assert "_af_" in physical_status["slot_name"]
    assert "_af_" in physical_status["publication_name"]
    assert (
        _psql_scalar_best_effort(
            "select coalesce(slot_name,'') from pg_replication_slots "
            f"where slot_name = '{pg_cdc_source['slot_name']}'"
        )
        == pg_cdc_source["slot_name"]
    ), "exact cutover adopted or removed the caller's preexisting logical slot"
    existing_slot_status = _metadata_status(stateful_api)
    assert (
        existing_slot_status.get(
            "projected_replication_source_statuses_exact_cutover", 0
        )
        == 1
    )
    assert (
        existing_slot_status.get(
            "projected_replication_source_statuses_non_exact_cutover", 0
        )
        == 0
    )
    assert (
        existing_slot_status.get(
            "projected_replication_source_statuses_slot_resumed", 0
        )
        == 0
    )
    assert (
        existing_slot_status.get(
            "projected_replication_source_statuses_reseed_recommended", 0
        )
        == 0
    )
    action_hints = _metadata_admin_snapshot_json(stateful_api).get(
        "replication_source_action_hints", []
    )
    assert not any(
        hint.get("table_name") == table_name
        and hint.get("source_ordinal") == 0
        and hint.get("action") == "reseed_exact_cutover"
        for hint in action_hints
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
            ),
            None,
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert source_detail["status"]["cutover_mode"] == "exported_snapshot"
    assert source_detail["status"]["slot_name"] == physical_status["slot_name"]
    assert not isinstance(source_detail.get("action_hint"), dict)

    _run_psql(f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-2', 'Bob', 'silver');
        """)
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
        "CDC stream did not apply through authority-scoped exact-cutover path\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}\n"
        f"{_pg_replication_debug(physical_status['slot_name'], physical_status['publication_name'])}"
    )


def test_stateful_postgres_cdc_required_exact_cutover_isolates_logical_name_collision(
    stateful_api, pg_cdc_source
):
    _require_cluster(stateful_api, "drives exact-cutover admin state")
    table_name = f"cdc_exact_cutover_docs_{time.time_ns()}"
    _run_psql(
        f"create publication {pg_cdc_source['publication_name']} for table {pg_cdc_source['table_name']};"
    )
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
    table_id = _metadata_table_id(stateful_api, table_name)
    assert table_id is not None

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
        "required exact cutover did not import its exported snapshot\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )
    matching = wait_until(
        lambda: [
            status
            for status in _metadata_replication_status_records(stateful_api)
            if int(status.get("table_id", 0)) == table_id
            and int(status.get("source_ordinal", -1)) == 0
            and status.get("cutover_mode") == "exported_snapshot"
            and status.get("phase") == "streaming"
        ],
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert matching, (
        "required exact cutover did not publish a streaming observation\n"
        f"{_metadata_replication_statuses(stateful_api)}\n"
        f"{_server_logs(stateful_api)}"
    )
    physical_status = matching[0]
    assert physical_status["slot_name"] != pg_cdc_source["slot_name"]
    assert physical_status["publication_name"] != pg_cdc_source["publication_name"]
    assert "_af_" in physical_status["slot_name"]
    assert "_af_" in physical_status["publication_name"]
    assert physical_status.get("last_error", "") == ""
    assert (
        _psql_scalar_best_effort(
            "select coalesce(slot_name,'') from pg_replication_slots "
            f"where slot_name = '{pg_cdc_source['slot_name']}'"
        )
        == pg_cdc_source["slot_name"]
    ), "required exact cutover adopted or removed the caller's logical slot"

    exact_required_status = _metadata_status(stateful_api)
    assert (
        exact_required_status.get(
            "projected_replication_source_statuses_exact_cutover", 0
        )
        == 1
    )
    assert (
        exact_required_status.get(
            "projected_replication_source_statuses_non_exact_cutover", 0
        )
        == 0
    )
    assert (
        exact_required_status.get(
            "projected_replication_source_statuses_terminal_failed", 0
        )
        == 0
    )
    assert (
        exact_required_status.get(
            "projected_replication_source_statuses_reseed_recommended", 0
        )
        == 0
    )


def test_stateful_postgres_cdc_reseed_rotates_exact_cutover_authority(
    stateful_api, pg_cdc_source
):
    _require_cluster(stateful_api, "uses the metadata reseed admin route")
    table_name = f"cdc_reseed_exact_cutover_docs_{time.time_ns()}"
    _run_psql(
        f"create publication {pg_cdc_source['publication_name']} for table {pg_cdc_source['table_name']};"
    )
    _run_psql(
        "select * from pg_create_logical_replication_slot('{slot}', 'pgoutput');".format(
            slot=pg_cdc_source["slot_name"]
        )
    )
    new_slot_name = ""
    new_publication_name = ""
    new_physical_slot_name = ""
    new_physical_publication_name = ""
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

        initial_status = wait_until(
            lambda: next(
                (
                    status
                    for status in _metadata_replication_status_records_for_source(
                        stateful_api, pg_cdc_source["table_name"]
                    )
                    if status.get("cutover_mode") == "exported_snapshot"
                    and status.get("phase") == "streaming"
                ),
                None,
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert initial_status is not None, (
            "expected an initial authority-scoped exact cutover before reseed\n"
            f"{_metadata_replication_statuses(stateful_api)}\n"
            f"{_server_logs(stateful_api)}"
        )
        initial_slot_name = initial_status["slot_name"]
        initial_publication_name = initial_status["publication_name"]
        initial_authority_id = initial_status["cutover_authority_id"]

        reseed_result = _reseed_exact_cutover(stateful_api, table_name, 0)
        new_slot_name = reseed_result["slot_name"]
        new_publication_name = reseed_result["publication_name"]
        assert new_slot_name != pg_cdc_source["slot_name"]
        assert new_publication_name != pg_cdc_source["publication_name"]
        pg_cdc_source["cleanup_slot_names"].append(new_slot_name)
        pg_cdc_source["cleanup_publication_names"].append(new_publication_name)

        table_detail = wait_until(
            lambda: next(
                (
                    detail
                    for detail in [_get_table_if_visible(stateful_api, table_name)]
                    if detail is not None
                    and detail["replication_sources"][0]["slot_name"] == new_slot_name
                    and detail["replication_sources"][0]["publication_name"]
                    == new_publication_name
                ),
                None,
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert table_detail["replication_sources"][0]["slot_name"] == new_slot_name
        assert (
            table_detail["replication_sources"][0]["publication_name"]
            == new_publication_name
        )
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
                    for status in _metadata_replication_status_records_for_source(
                        stateful_api, pg_cdc_source["table_name"]
                    )
                    if status.get("cutover_mode") == "exported_snapshot"
                    and status.get("phase") == "streaming"
                    and status.get("cutover_authority_id") != initial_authority_id
                ),
                None,
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        assert reseeded_status is not None, (
            "reseeded exact cutover did not publish a replacement authority\n"
            f"{_metadata_replication_statuses(stateful_api)}"
        )
        new_physical_slot_name = reseeded_status["slot_name"]
        new_physical_publication_name = reseeded_status["publication_name"]
        assert new_physical_slot_name != initial_slot_name
        assert new_physical_publication_name != initial_publication_name
        assert new_physical_slot_name != new_slot_name
        assert new_physical_publication_name != new_publication_name
        assert "_af_" in new_physical_slot_name
        assert "_af_" in new_physical_publication_name

        _run_psql(f"""
            insert into {pg_cdc_source["table_name"]} (id, name, tier)
            values ('user-2', 'Bob', 'silver');
            """)
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
            f"{_pg_replication_debug(new_physical_slot_name, new_physical_publication_name)}"
        )
    finally:
        # The provider fixture owns cleanup after the managed server stops, so
        # both the configured bases and authority-scoped physical names are
        # inactive before they are removed.
        pass


def test_stateful_postgres_cdc_recovers_publication_loss_but_marks_missing_slot_terminal(
    stateful_api, pg_cdc_source
):
    _require_cluster(stateful_api, "inspects metadata admin status")
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
    table_id = _metadata_table_id(stateful_api, table_name)
    assert table_id is not None

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

    active_status = wait_until(
        lambda: next(
            (
                status
                for status in _metadata_replication_status_records(stateful_api)
                if int(status.get("table_id", 0)) == table_id
                and int(status.get("source_ordinal", -1)) == 0
                and status.get("phase") == "streaming"
            ),
            None,
        ),
        timeout_s=30.0,
        interval_s=0.1,
    )
    assert active_status is not None
    physical_slot_name = active_status["slot_name"]
    physical_publication_name = active_status["publication_name"]

    _run_psql_best_effort(f"drop publication if exists {physical_publication_name};")
    recreated_publication = wait_until(
        lambda: _psql_scalar_best_effort(
            f"select coalesce(pubname,'') from pg_publication where pubname = '{physical_publication_name}'"
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert recreated_publication == physical_publication_name, (
        "CDC polling did not recreate the dropped publication\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )

    _run_psql(f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-2', 'Bob', 'silver');
        """)

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
        f"{_pg_replication_debug(physical_slot_name, physical_publication_name)}"
    )

    _drop_replication_slot_when_inactive(physical_slot_name)
    _run_psql(f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-3', 'Carol', 'bronze');
        """)

    failed_statuses = wait_until(
        lambda: [
            status
            for status in _metadata_replication_status_records(stateful_api)
            if int(status.get("table_id", 0)) == table_id
            and int(status.get("source_ordinal", -1)) == 0
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
    assert (
        failed_summary.get("projected_replication_source_statuses_terminal_failed", 0)
        >= 1
    )
    assert (
        failed_summary.get("projected_replication_source_statuses_with_last_error", 0)
        >= 1
    )
    assert (
        failed_summary.get(
            "projected_replication_source_statuses_slot_missing_failed", 0
        )
        >= 1
    )
    assert any(
        status.get("last_error") == "ForeignReplicationSlotMissing"
        for status in failed_statuses
    ), f"slot loss was not surfaced explicitly\n{failed_statuses!r}"


@pytest.mark.e2e_resource("antfly_process")
def test_standalone_accepts_postgres_replication_sources(pg_cdc_source):
    # Standalone runs the CDC coordinators: a source is accepted, reports
    # cdc_supported, and its snapshot row arrives. The parametrized
    # cdc_stateful_api fixture runs the full scenarios against standalone too.
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not os.path.exists(binary):
        pytest.skip(f"Public API binary not found: {binary}")
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    table_name = f"cdc_standalone_{time.time_ns()}"
    try:
        response = requests.post(
            f"{server.api_url}/tables/{table_name}",
            json={
                "replication_sources": [
                    {
                        "type": "postgres",
                        "dsn": _pg_dsn(),
                        "postgres_table": pg_cdc_source["table_name"],
                        "key_template": "id",
                        "slot_name": pg_cdc_source["slot_name"],
                        "publication_name": pg_cdc_source["publication_name"],
                    }
                ]
            },
            timeout=30,
        )
        assert response.status_code == 200, response.text

        def snapshot_arrived():
            doc = requests.get(
                server.api_url + lookup_key_path(table_name, "user-1"), timeout=30
            )
            return doc.status_code == 200 and doc.json().get("name") == "Alice"

        assert wait_until(snapshot_arrived, timeout_s=60, interval_s=0.5), (
            "snapshot row never arrived"
        )
        managed = requests.get(
            f"{server.api_url}/tables/{table_name}/sources/managed", timeout=30
        ).json()
        assert managed["cdc_supported"] is True, managed
        assert managed["progress"], managed
        assert managed["progress"][0]["last_error"] == "", managed
    finally:
        server.stop()


@pytest.mark.real_model
@pytest.mark.parametrize("cdc_stateful_api", ["standalone"], indirect=True)
def test_standalone_postgres_cdc_snapshot_completes_with_embeddings_index(
    cdc_stateful_api, pg_cdc_source, embeddinggemma2_model_available
):
    # Regression coverage for the snapshot/embedding deadlock: a table with
    # both Postgres CDC replication_sources and an embeddings index used to
    # hang forever on its first snapshot. The snapshot backfill opens a
    # bulk-ingest window for the whole batch loop and applied every batch
    # inside it waiting for full_index visibility, but the dense embedder's
    # replay session is admission-gated against that very same open window
    # (dense_publication_admission.zig) and can never become ready while it
    # stays open -- and the window never closes because the batch loop is
    # stuck waiting on it. Standalone-only: the cluster e2e harness has no
    # inference service and rejects embeddings indexes at create.
    _ = embeddinggemma2_model_available
    stateful_api = cdc_stateful_api

    # A cold model load (safetensors parse plus backend init) can take
    # longer than the ordinary request timeouts used below; warm it before
    # timing anything that depends on it, the same way the quickstart real-
    # model tests do.
    base_url = stateful_api.url.removesuffix(ANTFLY_PUBLIC_API_ROOT)
    warmup = stateful_api.s.post(
        f"{base_url}{INFERENCE_PUBLIC_API_ROOT}/embed",
        json={"model": EMBEDDINGGEMMA2_MODEL, "input": "warmup"},
        timeout=120,
    )
    assert warmup.ok, (warmup.status_code, warmup.text)

    table_name = f"cdc_embeddings_{time.time_ns()}"
    index_name = "name_idx"
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
            }
        ],
        "indexes": {
            index_name: {
                "type": "embeddings",
                "template": "{{name}}",
                "embedder": {
                    "provider": "antfly",
                    "model": EMBEDDINGGEMMA2_MODEL,
                },
            }
        },
    }
    _create_table_public(stateful_api, table_name, create_payload)

    # Before the fix, this never comes back: the snapshot batch loop is
    # blocked waiting on enrichment visibility that the open bulk-ingest
    # window itself prevents.
    snapshot_doc = wait_until(
        lambda: _lookup_doc_if(
            stateful_api,
            table_name,
            "user-1",
            lambda doc: doc.get("name") == "Alice",
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert snapshot_doc is not None, (
        "CDC snapshot into a table with an embeddings index did not complete\n"
        f"{_server_logs(stateful_api)}\n"
        f"{_metadata_replication_statuses(stateful_api)}"
    )

    def hit_ids(query: str) -> list[str] | None:
        try:
            result = stateful_api.query_table(
                table_name,
                {
                    "semantic_search": query,
                    "indexes": [index_name],
                    "fields": ["name"],
                    "limit": 5,
                },
            )
        except requests.RequestException:
            return None
        responses = result.get("responses", [])
        if not responses:
            return None
        return [hit.get("_id") for hit in responses[0].get("hits", {}).get("hits", [])]

    # The snapshot row's embedding materializes asynchronously once the
    # bulk-ingest window closes (it is written and plainly readable
    # immediately; only the embedding lags). Confirm it actually becomes
    # searchable, not just written.
    assert wait_until(
        lambda: "user-1" in (hit_ids("Alice") or []),
        timeout_s=30.0,
        interval_s=0.5,
    ), (
        "snapshot-backfilled row never became searchable through the embeddings index\n"
        f"{_server_logs(stateful_api)}"
    )

    managed = stateful_api._check(
        stateful_api._request("GET", f"/tables/{table_name}/sources/managed")
    )
    assert managed["cdc_supported"] is True, managed
    assert managed["progress"][0]["last_error"] == "", managed

    # A streamed insert after the snapshot embeds before the write returns,
    # same as any other write, because streaming apply never opens a
    # bulk-ingest window.
    _run_psql(f"""
        insert into {pg_cdc_source["table_name"]} (id, name, tier)
        values ('user-2', 'Bob', 'silver');
        """)

    assert wait_until(
        lambda: "user-2" in (hit_ids("Bob") or []),
        timeout_s=30.0,
        interval_s=0.5,
    ), (
        "streamed insert into a table with an embeddings index never became searchable\n"
        f"{_server_logs(stateful_api)}"
    )

    # No `on_delete` configured on this source: a deleted Postgres row
    # deletes the document by default, since the table is a full mirror.
    _run_psql(f"delete from {pg_cdc_source['table_name']} where id = 'user-2';")
    _wait_until_absent(
        stateful_api, table_name, "user-2", timeout_s=30.0, interval_s=0.25
    )


@pytest.mark.parametrize(
    "deployment, extra_rows",
    [
        ("standalone", 0),
        ("standalone", 16_384),
        ("distributed", 0),
        ("distributed-mixed", 0),
    ],
    ids=[
        "standalone-single-row",
        "standalone-multi-file",
        "distributed-single-row",
        "distributed-mixed-providers",
    ],
)
def test_postgres_bootstraps_and_streams_to_writable_iceberg(
    pg_cdc_source, tmp_path, deployment, extra_rows, request
):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if deployment == "standalone":
        config = tmp_path / "antfly.json"
        config.write_text(
            json.dumps(
                {
                    "deployment_mode": "standalone",
                    "storage": {
                        "engine": "local",
                        "local": {"base_dir": str(tmp_path / "native")},
                    },
                }
            )
        )
        server = StandaloneAntflyServer(binary, "127.0.0.1", 0, config_path=config)
    else:
        from test_secrets import TableObjectStorage

        artifacts = TableObjectStorage()
        request.addfinalizer(artifacts.close)
        config = {
            "deployment_mode": "distributed",
            "storage": {
                "engine": "local",
                "artifacts": {"connection": "cdc-artifacts", "bucket": "antfly"},
            },
            "connections": {
                "cdc-artifacts": {
                    "kind": "external_io",
                    "capabilities": ["storage.primary"],
                    "external_io": {
                        "protocol": "s3",
                        "endpoint": artifacts.endpoint,
                        "use_ssl": False,
                        "addressing_style": "path",
                        "region": "us-east-1",
                        "buckets": ["antfly"],
                        "credentials": {
                            "source": "static",
                            "access_key_id": "fixture",
                            "secret_access_key": "fixture",
                        },
                    },
                }
            },
        }

        def node_config(role):
            return {
                **config,
                "storage": {
                    **config["storage"],
                    "local": {"base_dir": str(tmp_path / role)},
                },
            }

        server = StatefulAntflyServer(
            binary,
            "127.0.0.1",
            0,
            auth_enabled=True,
            metadata_config=node_config("metadata"),
            data_config=node_config("data"),
        )
    table = "/tables/cdc_iceberg"
    native_table = "cdc_native"
    mixed = deployment == "distributed-mixed"
    native_before = {"id": "native-row", "name": "Native", "tier": "before"}
    native_after = {**native_before, "tier": "after"}
    provider = pg_cdc_source["table_name"]
    client = requests.Session()
    if deployment != "standalone":
        client.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)

    def call(method, path, body=None):
        try:
            reply = client.request(method, server.api_url + path, json=body, timeout=40)
        except requests.RequestException as error:
            pytest.fail(f"{method} {path} {error}\n{server.debug_logs()}")

        assert reply.ok, (
            f"{method} {path} {reply.status_code} {reply.text}\n{server.debug_logs()}"
        )
        return reply.json()

    def committed(minimum):
        properties = call("GET", table + "/lake/catalog")["metadata"]["properties"]
        return (
            properties
            if int(properties.get("antfly.wal.coverage", 0)) >= minimum
            else None
        )

    def rows():
        return call(
            "POST",
            "/sql",
            {
                "statement": "SELECT id, name FROM cdc_iceberg WHERE id IN ('user-1', 'user-2')"
            },
        )["rows"]

    def check_row_count():
        result = call("POST", "/sql", {"statement": "SELECT COUNT(*) FROM cdc_iceberg"})
        assert int(result["rows"][0][0]) == extra_rows + 1

    try:
        _run_psql(f"alter table {provider} replica identity full")
        if extra_rows:
            _run_psql(f"""
                insert into {provider} (id, name, tier)
                select 'seed-' || id, 'Seed', 'gold'
                from generate_series(1, {extra_rows}) as seed(id)
            """)
        call(
            "POST",
            table,
            {
                "storage": {"engine": "object"},
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "enforce_types": True,
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    name: {"type": "string"}
                                    for name in ("id", "name", "tier")
                                },
                                "required": ["id", "name", "tier"],
                                "additionalProperties": False,
                            }
                        },
                    },
                    "base_source": {
                        "kind": "external",
                        "table_id": "cdc-iceberg",
                        "format": "iceberg",
                        "uri": (tmp_path / "warehouse").as_uri(),
                        "write_policy": "iceberg_writer",
                        "catalog": {"type": "managed"},
                        "row_identity": {"key_columns": ["id"]},
                    },
                },
            },
        )
        call(
            "POST",
            table + "/lake/catalog",
            {
                "commit_id": "initialize",
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": index, "name": name, "required": True, "type": "string"}
                        for index, name in enumerate(("id", "name", "tier"), 1)
                    ],
                },
            },
        )
        attached = call("GET", table + "/sources/managed")
        assert attached["cdc_supported"], attached
        call(
            "POST",
            table + "/sources/managed",
            {
                "table_id": attached["table_id"],
                "expected_sources_hash": attached["sources_hash"],
                "replication_sources": [
                    {
                        "type": "postgres",
                        "dsn": _pg_dsn(),
                        "postgres_table": provider,
                        "key_template": "id",
                        "slot_name": pg_cdc_source["slot_name"],
                        "publication_name": pg_cdc_source["publication_name"],
                    }
                ],
            },
        )
        initial = wait_until(lambda: committed(1), timeout_s=60, interval_s=0.5)
        assert initial, server.debug_logs()
        cut = initial["antfly.cdc.bootstrap-cut"]
        assert cut == initial["antfly.cdc.checkpoint"]
        assert rows() == [["user-1", "Alice"]]
        check_row_count()

        feed = None
        if deployment != "standalone":
            if mixed:
                call("POST", f"/tables/{native_table}", {"num_shards": 1})
                call(
                    "POST",
                    f"/tables/{native_table}/batch",
                    {"inserts": {"native-row": native_before}},
                )
            feeds = f"{server.api_url}/changefeeds"
            created = _changefeed_call(
                client,
                "POST",
                feeds,
                headers={"Idempotency-Key": "postgres-iceberg-bootstrap"},
                json={
                    "tables": ["cdc_iceberg", native_table]
                    if mixed
                    else ["cdc_iceberg"],
                    "start": "snapshot_then_changes",
                    "include": "rows",
                },
            ).json()
            feed = f"{feeds}/{created['id']}"

        server.pause(crash=True)
        _run_psql(
            f"update {provider} set id = 'user-2', name = 'Bob' where id = 'user-1'"
        )
        server.resume()
        if mixed:
            call(
                "POST",
                f"/tables/{native_table}/batch",
                {"inserts": {"native-row": native_after}},
            )
        resumed = wait_until(lambda: committed(2), timeout_s=60, interval_s=0.5)
        assert resumed, server.debug_logs()
        assert resumed["antfly.cdc.bootstrap-cut"] == cut
        assert resumed["antfly.cdc.checkpoint"] != cut
        assert rows() == [["user-2", "Bob"]]
        check_row_count()
        if feed is not None:
            # Read the original cut after the catalog has committed a newer snapshot.
            cursor = created["cursor"]
            expected = {"user-1": {"id": "user-1", "name": "Alice", "tier": "gold"}}
            if mixed:
                expected["native-row"] = native_before
            snapshot_rows = {}
            for _ in range(len(expected)):
                bootstrap = _changefeed_call(
                    client, "GET", f"{feed}/events", params={"after": cursor}
                )
                body = bootstrap.json()
                for event in body["events"]:
                    row = event["after"]
                    assert row["id"] not in snapshot_rows
                    snapshot_rows[row["id"]] = row
                cursor = bootstrap.headers["Antfly-Changefeed-Cursor"]
                _changefeed_call(client, "POST", f"{feed}/ack", json={"cursor": cursor})
                if body["snapshot"]["complete"]:
                    break
            else:
                pytest.fail("changefeed did not complete its pinned snapshot")
            assert snapshot_rows == expected

            expected_operations = (
                {"insert", "delete", "update"} if mixed else {"insert", "delete"}
            )
            events = {}
            for _ in range(len(expected_operations)):
                params = {"after": cursor}
                page = _changefeed_call(client, "GET", f"{feed}/events", params=params)
                replay = _changefeed_call(
                    client, "GET", f"{feed}/events", params=params
                )
                assert replay.content == page.content
                cursor = page.headers["Antfly-Changefeed-Cursor"]
                assert replay.headers["Antfly-Changefeed-Cursor"] == cursor
                body = page.json()
                assert body["transaction"]["complete"]
                for event in body["events"]:
                    assert event["operation"] not in events
                    events[event["operation"]] = event
                _changefeed_call(client, "POST", f"{feed}/ack", json={"cursor": cursor})
                if set(events) == expected_operations:
                    break
            assert set(events) == expected_operations
            assert events["delete"]["before"] == {
                "id": "user-1",
                "name": "Alice",
                "tier": "gold",
            }
            assert events["insert"]["after"] == {
                "id": "user-2",
                "name": "Bob",
                "tier": "gold",
            }
            if mixed:
                assert events["update"]["before"] == native_before
                assert events["update"]["after"] == native_after
            _changefeed_call(client, "DELETE", feed)
    finally:
        client.close()
        server.stop()


def test_public_changefeed_bootstraps_multiple_partitions_and_replays():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    table = f"multi_partition_feed_{time.time_ns()}"
    server = StatefulAntflyServer(binary, "127.0.0.1", 0, auth_enabled=True)
    try:
        with requests.Session() as client:
            client.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            table_url = f"{server.api_url}/tables/{table}"
            created_table = client.post(table_url, json={"num_shards": 3}, timeout=30)
            assert created_table.status_code in (200, 201), created_table.text
            seeds = {f"{letter}-seed": {"phase": "snapshot"} for letter in "amz"}

            def write(rows):
                reply = client.post(
                    f"{table_url}/batch", json={"inserts": rows}, timeout=15
                )
                reply.raise_for_status()

            write(seeds)
            table_state = client.get(table_url, timeout=10)
            table_state.raise_for_status()
            assert len(table_state.json()["shards"]) == 3
            feeds = f"{server.api_url}/changefeeds"
            definition = {
                "tables": [table],
                "start": "snapshot_then_changes",
                "include": "rows",
            }
            headers = {"Idempotency-Key": table}

            def call(method, url, **options):
                expires = time.monotonic() + 30
                while True:
                    reply = client.request(method, url, timeout=15, **options)
                    retryable = reply.status_code == 503 or (
                        reply.status_code == 409
                        and "changefeed_retry_required" in reply.text
                    )
                    if not retryable or time.monotonic() >= expires:
                        reply.raise_for_status()
                        return reply
                    time.sleep(0.2)

            enrollment = call("POST", feeds, headers=headers, json=definition).json()
            assert (
                call("POST", feeds, headers=headers, json=definition).json()
                == enrollment
            )
            feed = f"{feeds}/{enrollment['id']}"
            cursor = enrollment["cursor"]
            write({"post-cut": {"phase": "stream"}})
            delivered = set()
            for page_number in range(8):
                reply = call(
                    "GET", f"{feed}/events", params={"after": cursor, "limit": 1}
                )
                page = reply.json()
                next_cursor = reply.headers["Antfly-Changefeed-Cursor"]
                if page_number == 0:
                    replay = call(
                        "GET", f"{feed}/events", params={"after": cursor, "limit": 1}
                    )
                    assert replay.content == reply.content
                    assert replay.headers["Antfly-Changefeed-Cursor"] == next_cursor
                for event in page["events"]:
                    assert event["kind"] == "document_snapshot"
                    assert event["after"]["phase"] == "snapshot"
                    key = event["key"]["value"]
                    assert key not in delivered
                    delivered.add(key)
                call("POST", f"{feed}/ack", json={"cursor": next_cursor})
                cursor = next_cursor
                if page["snapshot"]["complete"]:
                    break
            else:
                pytest.fail(server.debug_logs())
            assert delivered == set(seeds)
            for _ in range(8):
                reply = call("GET", f"{feed}/events", params={"after": cursor})
                events = reply.json()["events"]
                if events:
                    assert [
                        (event["key"]["value"], event["after"]) for event in events
                    ] == [("post-cut", {"phase": "stream"})]
                    break
                cursor = reply.headers["Antfly-Changefeed-Cursor"]
                call("POST", f"{feed}/ack", json={"cursor": cursor})
            else:
                pytest.fail(server.debug_logs())
            call("DELETE", feed)
    finally:
        server.stop()


def test_public_changefeed_multiple_tables_use_independent_row_filters():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StatefulAntflyServer(binary, "127.0.0.1", 0, auth_enabled=True)
    try:
        with requests.Session() as admin, requests.Session() as reader:
            admin.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            reader.auth = ("reader", "reader")
            tables = {"feed_gold": "gold", "feed_silver": "silver"}
            feeds = f"{server.api_url}/changefeeds"
            grants = [
                {"resource": table, "resource_type": "table", "type": "replicate"}
                for table in tables
            ]
            expected = {}
            for table, expected_tier in tables.items():
                url = f"{server.api_url}/tables/{table}"
                admin.post(url, json={"num_shards": 1}, timeout=30).raise_for_status()
                status = admin.get(url, timeout=15)
                status.raise_for_status()
                expected[str(status.json()["table_id"])] = expected_tier
                admin.post(
                    f"{url}/batch",
                    json={
                        "inserts": {tier: {"tier": tier} for tier in tables.values()}
                    },
                    timeout=15,
                ).raise_for_status()
            users = f"{server.url}/auth/v1/users"
            admin.post(
                f"{users}/limited",
                json={"password": "limited", "initial_policies": grants[:1]},
                timeout=15,
            ).raise_for_status()
            admin.post(
                f"{users}/reader",
                json={"password": "reader", "initial_policies": grants},
                timeout=15,
            ).raise_for_status()
            for table, tier in tables.items():
                admin.put(
                    f"{users}/reader/row-filters/{table}",
                    json={"term": {"tier": tier}},
                    timeout=15,
                ).raise_for_status()

            definition = {
                "tables": list(tables),
                "start": "snapshot_then_changes",
                "include": "rows",
            }
            headers = {"Idempotency-Key": "multi-table-snapshot"}
            assert (
                reader.post(
                    feeds,
                    auth=("limited", "limited"),
                    json=definition,
                    headers=headers,
                    timeout=15,
                ).status_code
                == 403
            )

            def call(method, url, **options):
                deadline = time.monotonic() + 30
                while True:
                    reply = reader.request(method, url, timeout=15, **options)
                    if (
                        reply.status_code != 503
                        and not (
                            reply.status_code == 409
                            and "changefeed_retry_required" in reply.text
                        )
                    ) or time.monotonic() >= deadline:
                        reply.raise_for_status()
                        return reply
                    time.sleep(0.2)

            enrollment = call("POST", feeds, json=definition, headers=headers).json()
            definition["tables"].reverse()
            assert (
                call("POST", feeds, json=definition, headers=headers).json()
                == enrollment
            )
            feed = f"{feeds}/{enrollment['id']}"
            assert (
                reader.get(feed, auth=("limited", "limited"), timeout=15).status_code
                == 403
            )
            cursor = enrollment["cursor"]
            seen = {}
            for _ in range(6):
                reply = call(
                    "GET", f"{feed}/events", params={"after": cursor, "limit": 1}
                )
                page = reply.json()
                cursor = reply.headers["Antfly-Changefeed-Cursor"]
                for event in page["events"]:
                    assert event["table_id"] not in seen
                    seen[event["table_id"]] = event["after"]["tier"]
                call("POST", f"{feed}/ack", json={"cursor": cursor})
                if page["snapshot"]["complete"]:
                    break
            else:
                pytest.fail(server.debug_logs())
            assert seen == expected

            admin.put(
                f"{users}/reader/row-filters/feed_silver",
                json={"term": {"tier": "gold"}},
                timeout=15,
            ).raise_for_status()
            denied = reader.get(f"{feed}/events", params={"after": cursor}, timeout=15)
            assert denied.status_code == 409, denied.text
            assert "events" not in denied.json()
            inspection = reader.get(feed, timeout=15)
            assert inspection.status_code == 409, inspection.text
            assert "unacknowledged_deliveries" not in inspection.json()
    finally:
        server.stop()


def _changefeed_call(client, method, url, **options):
    deadline = time.monotonic() + 30
    while True:
        reply = client.request(method, url, timeout=15, **options)
        retryable = reply.status_code == 503 or (
            reply.status_code == 409 and "changefeed_retry_required" in reply.text
        )
        if not retryable or time.monotonic() >= deadline:
            reply.raise_for_status()
            return reply
        time.sleep(0.2)


def test_public_changefeed_projects_exact_before_and_after_predicates():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StatefulAntflyServer(binary, "127.0.0.1", 0, auth_enabled=True)
    try:
        with requests.Session() as client:
            client.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            name = "changefeed_predicates"
            feeds = f"{server.api_url}/changefeeds"
            table = f"{server.api_url}/tables/{name}"

            _changefeed_call(client, "POST", table, json={"num_shards": 1})
            _changefeed_call(
                client,
                "POST",
                f"{table}/batch",
                json={
                    "inserts": {
                        key: {"eligible": eligible, "visible": "before", "hidden": key}
                        for key, eligible in [
                            ("enter", False),
                            ("update", True),
                            ("leave", True),
                            ("delete", True),
                        ]
                    }
                },
            )
            created = _changefeed_call(
                client,
                "POST",
                feeds,
                headers={"Idempotency-Key": name},
                json={
                    "tables": [name],
                    "selectors": [
                        {
                            "table": name,
                            "fields": ["visible"],
                            "predicates": [{"field": "eligible", "value": True}],
                        }
                    ],
                    "include": "rows",
                },
            ).json()
            feed = f"{feeds}/{created['id']}"
            progress = _changefeed_call(client, "GET", feed).json()
            assert progress["phase"] == "streaming"
            assert progress["acknowledged_cursor"] == created["cursor"]
            assert progress["unacknowledged_deliveries"] == 0
            assert progress["oldest_unacknowledged_at"] is None
            _changefeed_call(
                client,
                "POST",
                f"{table}/batch",
                json={
                    "inserts": {
                        key: {"eligible": eligible, "visible": "after", "hidden": key}
                        for key, eligible in [
                            ("enter", True),
                            ("update", True),
                            ("leave", False),
                            ("excluded", False),
                        ]
                    },
                    "deletes": ["delete"],
                },
            )
            cursor = created["cursor"]
            seen = {}
            for _ in range(12):
                page = _changefeed_call(
                    client,
                    "GET",
                    f"{feed}/events",
                    params={"after": cursor, "limit": 1},
                )
                cursor = page.headers["Antfly-Changefeed-Cursor"]
                events = page.json()["events"]
                if events and not seen:
                    progress = _changefeed_call(client, "GET", feed).json()
                    assert progress["unacknowledged_deliveries"] == 1
                    assert progress["oldest_unacknowledged_at"] > 0
                for event in events:
                    key = event["key"]["value"]
                    assert key not in seen
                    seen[key] = (event["operation"], event["before"], event["after"])
                _changefeed_call(client, "POST", f"{feed}/ack", json={"cursor": cursor})
                if len(seen) == 4:
                    break
            before = {"visible": "before"}
            after = {"visible": "after"}
            assert seen == {
                "enter": ("enter", None, after),
                "update": ("update", before, after),
                "leave": ("leave", before, None),
                "delete": ("delete", before, None),
            }
            _changefeed_call(client, "DELETE", feed)
    finally:
        server.stop()


def test_public_changefeed_bootstraps_and_streams_native_sql_rows():
    from decimal import Decimal

    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StatefulAntflyServer(binary, "127.0.0.1", 0, auth_enabled=True)
    try:
        with requests.Session() as client:
            client.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            feeds = f"{server.api_url}/changefeeds"
            amount = "12345678901234567890.12345678"

            def sql(statement):
                reply = client.post(
                    f"{server.api_url}/sql", json={"statement": statement}, timeout=40
                )
                assert reply.ok, reply.text

            sql(
                "CREATE TABLE cdc_rows (id BIGINT PRIMARY KEY, amount NUMERIC(38,8), label TEXT)"
            )
            sql(
                f"INSERT INTO cdc_rows (id, amount, label) VALUES (1, {amount}, 'before')"
            )
            created = _changefeed_call(
                client,
                "POST",
                feeds,
                headers={"Idempotency-Key": "cdc-sql-rows"},
                json={
                    "tables": ["cdc_rows"],
                    "start": "snapshot_then_changes",
                    "include": "rows",
                },
            ).json()
            feed = f"{feeds}/{created['id']}"
            sql("UPDATE cdc_rows SET label = 'after' WHERE id = 1")
            cursor = created["cursor"]
            observed = []
            for _ in range(8):
                page = _changefeed_call(
                    client,
                    "GET",
                    f"{feed}/events",
                    params={"after": cursor, "limit": 1},
                )
                cursor = page.headers["Antfly-Changefeed-Cursor"]
                observed.extend(page.json(parse_float=Decimal)["events"])
                _changefeed_call(client, "POST", f"{feed}/ack", json={"cursor": cursor})
                if len(observed) == 2:
                    break
            expected = {"id": 1, "amount": Decimal(amount), "label": "before"}
            assert len(observed) == 2, server.debug_logs()
            assert observed[0]["kind"] == "document_snapshot"
            assert observed[0]["after"] == expected
            assert observed[1]["kind"] == "document_committed"
            assert observed[1]["before"] == expected
            assert observed[1]["after"] == {**expected, "label": "after"}
            assert observed[1]["key"] == observed[0]["key"]
            _changefeed_call(client, "DELETE", feed)
    finally:
        server.stop()


def test_public_changefeed_retains_ttl_tombstone():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StatefulAntflyServer(binary, "127.0.0.1", 0, auth_enabled=True)
    try:
        with requests.Session() as client:
            client.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            name = "changefeed_ttl"
            table = f"{server.api_url}/tables/{name}"
            feeds = f"{server.api_url}/changefeeds"
            _changefeed_call(
                client,
                "POST",
                table,
                json={
                    "num_shards": 1,
                    "schema": {"ttl": {"field": "expires_at", "duration": "1s"}},
                },
            )
            created = _changefeed_call(
                client,
                "POST",
                feeds,
                headers={"Idempotency-Key": name},
                json={"tables": [name], "include": "rows"},
            ).json()
            feed = f"{feeds}/{created['id']}"
            _changefeed_call(
                client,
                "POST",
                f"{table}/batch",
                json={
                    "inserts": {
                        "expiring": {
                            "value": "history",
                            "expires_at": "2000-01-01T00:00:00Z",
                        }
                    }
                },
            )
            cursor = created["cursor"]
            events = []
            deadline = time.monotonic() + 60
            while len(events) < 2 and time.monotonic() < deadline:
                page = _changefeed_call(
                    client, "GET", f"{feed}/events", params={"after": cursor}
                )
                cursor = page.headers["Antfly-Changefeed-Cursor"]
                events.extend(page.json()["events"])
                _changefeed_call(client, "POST", f"{feed}/ack", json={"cursor": cursor})
                if not page.json()["events"]:
                    time.sleep(0.2)
            assert [event["operation"] for event in events] == ["insert", "delete"], (
                server.debug_logs()
            )
            assert events[1]["before"]["value"] == "history"
            assert events[1]["after"] is None
            assert events[1]["key"] == events[0]["key"]
            assert events[1]["event_id"] != events[0]["event_id"]
            _changefeed_call(client, "DELETE", feed)
    finally:
        server.stop()


def test_public_changefeed_retains_direct_enrichment_transaction():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StatefulAntflyServer(binary, "127.0.0.1", 0, auth_enabled=True)
    try:
        with requests.Session() as client:
            client.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            tables = ["changefeed_direct_primary", "changefeed_direct_audit"]
            feeds = f"{server.api_url}/changefeeds"
            for name in tables:
                _changefeed_call(
                    client,
                    "POST",
                    f"{server.api_url}/tables/{name}",
                    json={"num_shards": 1},
                )
            created = _changefeed_call(
                client,
                "POST",
                feeds,
                headers={"Idempotency-Key": tables[0]},
                json={"tables": tables, "include": "rows"},
            ).json()
            feed = f"{feeds}/{created['id']}"
            reply = client.post(
                f"{server.api_url}/batch",
                timeout=30,
                json={
                    "commit_requires": "direct_enrichments",
                    "tables": {
                        name: {"inserts": {"row": {"value": name}}} for name in tables
                    },
                },
            )
            assert reply.ok, (reply.text, server.debug_logs())
            committed = reply.json()
            assert all(committed["tables"][name]["inserted"] == 1 for name in tables)
            cursor = created["cursor"]
            events = []
            deadline = time.monotonic() + 30
            while len(events) < 2 and time.monotonic() < deadline:
                page = _changefeed_call(
                    client, "GET", f"{feed}/events", params={"after": cursor}
                )
                cursor = page.headers["Antfly-Changefeed-Cursor"]
                observed = page.json()["events"]
                events.extend(observed)
                if not observed:
                    time.sleep(0.2)
            assert sorted(event["after"]["value"] for event in events) == sorted(tables)
            assert all(event["operation"] == "insert" for event in events)
            _changefeed_call(client, "POST", f"{feed}/ack", json={"cursor": cursor})
            _changefeed_call(client, "DELETE", feed)
    finally:
        server.stop()


def test_public_changefeed_renew_retains_root_beyond_original_deadline():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StatefulAntflyServer(binary, "127.0.0.1", 0, auth_enabled=True)
    try:
        with requests.Session() as client:
            client.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            name = "changefeed_renewal"
            table = f"{server.api_url}/tables/{name}"
            feeds = f"{server.api_url}/changefeeds"
            _changefeed_call(client, "POST", table, json={"num_shards": 1})
            created = _changefeed_call(
                client,
                "POST",
                feeds,
                headers={"Idempotency-Key": name},
                json={"tables": [name], "include": "rows", "lease_seconds": 45},
            ).json()
            feed = f"{feeds}/{created['id']}"
            expires_at = created["expires_at"] + 120
            unauthenticated = requests.post(
                f"{feed}/renew", json={"expires_at": expires_at}, timeout=15
            )
            assert unauthenticated.status_code == 403
            for _ in range(2):
                renewed = _changefeed_call(
                    client, "POST", f"{feed}/renew", json={"expires_at": expires_at}
                ).json()
                assert renewed == {"expires_at": expires_at}
            progress = _changefeed_call(client, "GET", feed).json()
            assert progress["acknowledged_cursor"] == created["cursor"]
            assert progress["expires_at"] == expires_at
            time.sleep(max(0, created["expires_at"] + 1 - time.time()))
            _changefeed_call(
                client, "POST", f"{table}/batch", json={"inserts": {"after": {}}}
            )
            deadline = time.monotonic() + 15
            cursor = created["cursor"]
            while True:
                page = _changefeed_call(
                    client, "GET", f"{feed}/events", params={"after": cursor}
                )
                cursor = page.headers["Antfly-Changefeed-Cursor"]
                events = page.json()["events"]
                if events or time.monotonic() >= deadline:
                    break
                time.sleep(0.2)
            assert [event["operation"] for event in events] == ["insert"]
            _changefeed_call(client, "POST", f"{feed}/ack", json={"cursor": cursor})
            _changefeed_call(client, "DELETE", feed)
    finally:
        server.stop()
