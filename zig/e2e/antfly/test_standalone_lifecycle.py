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

"""CI-safe standalone lifecycle coverage that does not download model weights."""

from __future__ import annotations

import os
import time
from pathlib import Path
from urllib.parse import quote

import pytest
import requests

from conftest import ready_index_status
from helpers import assert_created_index, wait_until
from test_standalone import (
    DEFAULT_ANTFLY_BIN,
    EmbeddedInferenceStandaloneServer,
    _resolve_binary_path,
)


@pytest.fixture(scope="function")
def standalone_lifecycle_server(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    binary = _resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.fail(f"Antfly binary not found: {binary}")

    # Keep enrichment batches independently cancellable while table generations
    # retire. The test injects an offline rate-limited provider below.
    monkeypatch.setenv("ANTFLY_ENRICHMENT_EMBED_BATCH_ITEMS", "1")
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    server = EmbeddedInferenceStandaloneServer(binary, models_dir, "")
    try:
        yield server
    finally:
        server.stop()


def _json_request(
    session: requests.Session,
    server: EmbeddedInferenceStandaloneServer,
    method: str,
    path: str,
    *,
    payload: dict | None = None,
) -> dict | list:
    try:
        response = session.request(
            method,
            f"{server.url}{path}",
            json=payload,
            timeout=30,
        )
    except requests.RequestException as exc:
        process_status = server.proc.poll() if server.proc is not None else None
        raise AssertionError(
            f"{method} {path} failed before receiving a response: {exc!r}; "
            f"server_exit_status={process_status}\n"
            f"server logs:\n{server.debug_logs()}"
        ) from exc
    if response.status_code >= 400:
        raise AssertionError(
            f"{method} {path} failed: {response.status_code} {response.text}\n"
            f"server logs:\n{server.debug_logs()}"
        )
    if not response.content:
        return {}
    return response.json()


def test_standalone_drop_drains_pending_enrichment_work(
    standalone_lifecycle_server: EmbeddedInferenceStandaloneServer,
    rate_limited_openai_embedder,
):
    server = standalone_lifecycle_server
    hot_tables = [f"standalone_ci_drop_{time.time_ns()}_{index}" for index in range(3)]
    survivor = f"standalone_ci_survivor_{time.time_ns()}"
    table_names = [*hot_tables, survivor]
    created_tables: set[str] = set()
    session = requests.Session()
    session.headers["Connection"] = "close"

    try:
        rate_limited_openai_embedder.allow_all_requests()
        for table_name in table_names:
            created = _json_request(
                session,
                server,
                "POST",
                f"/tables/{table_name}",
                payload={"num_shards": 1},
            )
            assert isinstance(created, dict)
            assert created["name"] == table_name
            created_tables.add(table_name)
            if table_name == survivor:
                continue
            created_index = _json_request(
                session,
                server,
                "POST",
                f"/tables/{table_name}/indexes/semantic_idx",
                payload={
                    "name": "semantic_idx",
                    "type": "embeddings",
                    "field": "body",
                    "dimension": 3,
                    "embedder": {
                        "provider": "openai",
                        "model": "text-embedding-3-small",
                        "url": rate_limited_openai_embedder.url,
                    },
                },
            )
            assert_created_index(created_index, "semantic_idx", "embeddings")
            assert (
                wait_until(
                    lambda table_name=table_name: ready_index_status(
                        _json_request(
                            session,
                            server,
                            "GET",
                            f"/tables/{table_name}/indexes/semantic_idx",
                        )
                    ),
                    timeout_s=30.0,
                    interval_s=0.1,
                )
                is not None
            ), (
                f"index did not become ready for {table_name}\nserver logs:\n{server.debug_logs()}"
            )

        rate_limited_openai_embedder.deny_requests()
        documents = {
            f"doc:{index:02d}": {
                "body": f"pending embedded inference lifecycle document {index}",
            }
            for index in range(24)
        }
        for table_name in hot_tables:
            batch = _json_request(
                session,
                server,
                "POST",
                f"/tables/{table_name}/batch",
                payload={"inserts": documents, "sync_level": "write"},
            )
            assert isinstance(batch, dict)
            assert batch["inserted"] == len(documents)

        latest_statuses: dict[str, dict] = {}

        def pending_work() -> dict | None:
            for table_name in hot_tables:
                try:
                    detail = _json_request(
                        session,
                        server,
                        "GET",
                        f"/tables/{table_name}/indexes/semantic_idx",
                    )
                except (AssertionError, requests.RequestException, ValueError):
                    continue
                assert isinstance(detail, dict)
                latest_statuses[table_name] = detail
                status = detail.get("status", {})
                coverage = status.get("coverage", {})
                provider_limited = (
                    rate_limited_openai_embedder.stats()["rate_limited_requests"] > 0
                )
                work_pending = (
                    int(coverage.get("pending", 0)) > 0
                    or int(status.get("replay_applied_sequence", 0))
                    < int(status.get("replay_target_sequence", 0))
                    or status.get("catch_up_active") is True
                    or status.get("backfill_active") is True
                )
                if provider_limited and work_pending:
                    return detail
            return None

        assert wait_until(pending_work, timeout_s=30.0, interval_s=0.05) is not None, (
            "standalone enrichment work never became pending: "
            f"{latest_statuses}; provider={rate_limited_openai_embedder.stats()}\n"
            f"server logs:\n{server.debug_logs()}"
        )

        for table_name in hot_tables:
            _json_request(session, server, "DELETE", f"/tables/{table_name}")

        def dropped_tables_absent() -> bool | None:
            try:
                tables = _json_request(session, server, "GET", "/tables")
            except (AssertionError, requests.RequestException, ValueError):
                return None
            assert isinstance(tables, list)
            names = {table["name"] for table in tables}
            return True if not names.intersection(hot_tables) else None

        assert wait_until(dropped_tables_absent, timeout_s=30.0, interval_s=0.1)

        survivor_batch = _json_request(
            session,
            server,
            "POST",
            f"/tables/{survivor}/batch",
            payload={
                "inserts": {
                    "doc:survivor": {"body": "the unrelated owner remains writable"}
                },
                "sync_level": "write",
            },
        )
        assert isinstance(survivor_batch, dict)
        assert survivor_batch["inserted"] == 1
        survivor_doc = _json_request(
            session,
            server,
            "GET",
            f"/tables/{survivor}/documents/{quote('doc:survivor', safe='')}",
        )
        assert isinstance(survivor_doc, dict)
        assert survivor_doc["body"] == "the unrelated owner remains writable"

        status = requests.get(f"{server.url}/status", timeout=30)
        status.raise_for_status()
    finally:
        rate_limited_openai_embedder.allow_all_requests()
        for table_name in sorted(created_tables):
            try:
                _json_request(session, server, "DELETE", f"/tables/{table_name}")
            except (AssertionError, requests.RequestException, ValueError):
                pass
        session.close()
