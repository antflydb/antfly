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

"""Stateful public API backup and restore tests."""

from __future__ import annotations

import os
import tempfile
import time

import pytest
import requests

from helpers import wait_until


def _lookup_doc(stateful_api, table_name: str, key: str) -> dict | None:
    try:
        return stateful_api.lookup_key(table_name, key)
    except requests.HTTPError:
        return None


def _lookup_docs(stateful_api, table_names: tuple[str, ...], key: str) -> dict[str, dict] | None:
    docs: dict[str, dict] = {}
    for table_name in table_names:
        doc = _lookup_doc(stateful_api, table_name, key)
        if doc is None:
            return None
        docs[table_name] = doc
    return docs


def _wait_until_absent(stateful_api, table_name: str, key: str, *, timeout_s: float, interval_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _lookup_doc(stateful_api, table_name, key) is None:
            return
        time.sleep(interval_s)
    raise AssertionError(f"{table_name}:{key} remained visible after delete")


def _lookup_table(stateful_api, table_name: str) -> dict | None:
    try:
        return stateful_api.get_table(table_name)
    except requests.HTTPError:
        return None


def _wait_until_table_absent(stateful_api, table_name: str, *, timeout_s: float, interval_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _lookup_table(stateful_api, table_name) is None:
            return
        time.sleep(interval_s)
    raise AssertionError(f"{table_name} remained visible after delete")


def _top_hit(stateful_api, table_name: str, query: str, expected_id: str) -> dict | None:
    try:
        result = stateful_api.query_table(
            table_name,
            {
                "full_text_search": {
                    "match": {
                        "field": "content",
                        "text": query,
                    },
                },
                "limit": 5,
            },
        )
    except requests.HTTPError:
        return None

    responses = result.get("responses", [])
    if not responses:
        return None
    hits = responses[0].get("hits", {}).get("hits", [])
    if not hits:
        return None
    for hit in hits:
        if hit.get("_id") == expected_id:
            return result
    return None


def _semantic_top_hit(stateful_api, table_name: str, query: str, index_name: str, expected_id: str) -> dict | None:
    try:
        result = stateful_api.query_table(
            table_name,
            {
                "semantic_search": query,
                "indexes": [index_name],
                "limit": 5,
            },
        )
    except requests.HTTPError:
        return None

    responses = result.get("responses", [])
    if not responses:
        return None
    hits = responses[0].get("hits", {}).get("hits", [])
    if not hits:
        return None
    for hit in hits:
        if hit.get("_id") == expected_id:
            return result
    return None


def _write_single_doc(stateful_api, table_name: str, key: str, *, title: str, content: str) -> None:
    batch = stateful_api.batch_write(
        table_name,
        inserts={
            key: {
                "title": title,
                "content": content,
            }
        },
        sync_level="full_text",
    )
    assert batch["inserted"] == 1


def _integration_enabled(env_name: str) -> bool:
    value = os.environ.get(env_name, "")
    return value != "" and value not in {"0", "false", "False"}


def _remote_backup_location(backend: str) -> str:
    if backend == "s3":
        enable_env = "OBJECTSTORE_S3_INTEGRATION"
        bucket_env = "OBJECTSTORE_S3_TEST_BUCKET"
        scheme = "s3"
    elif backend == "gs":
        enable_env = "OBJECTSTORE_GCS_INTEGRATION"
        bucket_env = "OBJECTSTORE_GCS_TEST_BUCKET"
        scheme = "gs"
    else:
        raise AssertionError(f"unsupported backend: {backend}")

    if not _integration_enabled(enable_env):
        pytest.skip(f"set {enable_env}=1 to enable {scheme} backup integration tests")

    bucket = os.environ.get(bucket_env)
    if not bucket:
        pytest.skip(f"missing env {bucket_env}")

    prefix = f"antfly-backup-e2e/{scheme}/{time.time_ns()}"
    return f"{scheme}://{bucket}/{prefix}"


def test_table_backup_restore_round_trip(backup_api):
    table_name = f"backup_restore_{time.time_ns()}"
    backup_id = f"backup-{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1, description="backup restore docs")
    assert created["name"] == table_name
    assert "full_text_index_v0" in created["indexes"]

    docs = {
        "doc:db": {
            "title": "Distributed Databases",
            "content": "Distributed databases replicate state across nodes and coordinate writes with consensus.",
        },
        "doc:vector": {
            "title": "Vector Search",
            "content": "Vector search uses embeddings to retrieve semantically similar documents.",
        },
        "doc:raft": {
            "title": "Raft Consensus",
            "content": "Raft coordinates leaders and followers to keep replicated logs consistent.",
        },
    }
    batch = backup_api.batch_write(table_name, inserts=docs, sync_level="full_text")
    assert batch["inserted"] == len(docs)
    assert wait_until(
        lambda: _top_hit(backup_api, table_name, "distributed consensus", "doc:db"),
        timeout_s=60.0,
        interval_s=1.0,
    )

    with tempfile.TemporaryDirectory(prefix="antfly-backup-") as backup_dir:
        location = f"file://{backup_dir}"

        backup = backup_api.backup_table(table_name, backup_id=backup_id, location=location)
        assert backup["backup"] == "successful"

        deleted = backup_api.delete_table(table_name)
        assert deleted == {}

        _wait_until_table_absent(backup_api, table_name, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_name, "doc:db", timeout_s=10.0, interval_s=0.5)

        restore = backup_api.restore_table(table_name, backup_id=backup_id, location=location)
        assert restore["restore"] == "triggered"

        restored_doc = wait_until(
            lambda: _lookup_doc(backup_api, table_name, "doc:db"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        assert restored_doc is not None, "restored document did not reappear"
        assert restored_doc["title"] == "Distributed Databases"
        assert "consensus" in restored_doc["content"]
        assert wait_until(
            lambda: _top_hit(backup_api, table_name, "distributed consensus", "doc:db"),
            timeout_s=60.0,
            interval_s=1.0,
        )


def test_table_backup_restore_round_trip_managed_chunked_semantic(backup_api, openai_embedder):
    table_name = f"backup_chunked_semantic_{time.time_ns()}"
    backup_id = f"backup-chunked-semantic-{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1, description="chunked semantic backup docs")
    assert created["name"] == table_name

    assert (
        backup_api.create_index(
            table_name,
            "semantic_chunked_idx",
            {
                "name": "semantic_chunked_idx",
                "type": "embeddings",
                "field": "content",
                "dimension": 3,
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": openai_embedder,
                },
                "chunker": {
                    "provider": "antfly",
                    "model": "fixed-bert-tokenizer",
                    "store_chunks": True,
                    "text": {
                        "target_tokens": 4,
                        "overlap_tokens": 1,
                        "separator": " ",
                    },
                },
            },
        )
        == {}
    )

    ready = backup_api.wait_index_ready(table_name, "semantic_chunked_idx", timeout_s=30.0, interval_s=0.5)
    assert ready is not None

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha backup",
                "content": "alpha body alpha body alpha body alpha body alpha tail",
            },
            "doc:b": {
                "title": "Beta backup",
                "content": "beta body beta body beta body beta tail",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    assert wait_until(
        lambda: _semantic_top_hit(backup_api, table_name, "alpha concept", "semantic_chunked_idx", "doc:a"),
        timeout_s=30.0,
        interval_s=0.5,
    )

    before_scan = backup_api.scan_keys(
        table_name,
        {
            "from": "doc:a",
            "to": "doc:a;",
            "inclusive_from": True,
            "fields": ["title", "_chunks"],
        },
    )
    assert len(before_scan) == 1
    before_chunks = before_scan[0]["_chunks"]["semantic_chunked_idx_chunks"]
    assert len(before_chunks) >= 2

    with tempfile.TemporaryDirectory(prefix="antfly-backup-chunked-semantic-") as backup_dir:
        location = f"file://{backup_dir}"

        backup = backup_api.backup_table(table_name, backup_id=backup_id, location=location)
        assert backup["backup"] == "successful"

        deleted = backup_api.delete_table(table_name)
        assert deleted == {}

        _wait_until_table_absent(backup_api, table_name, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_name, "doc:a", timeout_s=10.0, interval_s=0.5)

        restore = backup_api.restore_table(table_name, backup_id=backup_id, location=location)
        assert restore["restore"] == "triggered"

        restored_doc = wait_until(
            lambda: _lookup_doc(backup_api, table_name, "doc:a"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        assert restored_doc is not None
        assert restored_doc["title"] == "Alpha backup"

        ready_after_restore = backup_api.wait_index_ready(table_name, "semantic_chunked_idx", timeout_s=180.0, interval_s=1.0)
        if ready_after_restore is None:
            after_status = backup_api.get_index(table_name, "semantic_chunked_idx")
            raise AssertionError(f"semantic restore index did not become query-ready; status={after_status}")

        semantic_after = wait_until(
            lambda: _semantic_top_hit(backup_api, table_name, "alpha concept", "semantic_chunked_idx", "doc:a"),
            timeout_s=120.0,
            interval_s=1.0,
        )
        if semantic_after is None:
            after_status = backup_api.get_index(table_name, "semantic_chunked_idx")
            after_scan = backup_api.scan_keys(
                table_name,
                {
                    "from": "doc:a",
                    "to": "doc:a;",
                    "inclusive_from": True,
                    "fields": ["title", "_chunks", "_embeddings"],
                },
            )
            after_query = backup_api.query_table(
                table_name,
                {
                    "semantic_search": "alpha concept",
                    "indexes": ["semantic_chunked_idx"],
                    "limit": 5,
                    "fields": ["title", "_chunks", "_embeddings"],
                },
            )
            raise AssertionError(
                f"semantic restore query did not recover; status={after_status}, scan={after_scan}, query={after_query}"
            )
        assert semantic_after

        after_scan = wait_until(
            lambda: (
                scan
                if (
                    (scan := backup_api.scan_keys(
                        table_name,
                        {
                            "from": "doc:a",
                            "to": "doc:a;",
                            "inclusive_from": True,
                            "fields": ["title", "_chunks"],
                        },
                    ))
                    and len(scan) == 1
                    and scan[0].get("_chunks", {}).get("semantic_chunked_idx_chunks")
                )
                else None
            ),
            timeout_s=60.0,
            interval_s=1.0,
        )
        assert after_scan is not None
        assert after_scan[0]["title"] == "Alpha backup"
        after_chunks = after_scan[0]["_chunks"]["semantic_chunked_idx_chunks"]
        assert len(after_chunks) >= 2



def test_cluster_backup_restore_round_trip(backup_api):
    table_a = f"cluster_backup_a_{time.time_ns()}"
    table_b = f"cluster_backup_b_{time.time_ns()}"
    backup_id = f"cluster-backup-{time.time_ns()}"

    for table_name, title in (
        (table_a, "Cluster Backup Alpha"),
        (table_b, "Cluster Backup Beta"),
    ):
        created = backup_api.create_table(table_name, num_shards=1, description=f"{table_name} docs")
        assert created["name"] == table_name
        batch = backup_api.batch_write(
            table_name,
            inserts={
                "doc:1": {
                    "title": title,
                    "content": f"{title} survives backup and restore.",
                }
            },
            sync_level="full_text",
        )
        assert batch["inserted"] == 1
        assert wait_until(
            lambda tn=table_name, q=title.lower(), doc_id="doc:1": _top_hit(backup_api, tn, q, doc_id),
            timeout_s=60.0,
            interval_s=1.0,
        )

    with tempfile.TemporaryDirectory(prefix="antfly-cluster-backup-") as backup_dir:
        location = f"file://{backup_dir}"

        backup = backup_api.cluster_backup(backup_id=backup_id, location=location)
        assert backup["backup_id"] == backup_id
        assert backup["status"] == "completed"
        assert {table["name"] for table in backup["tables"]} == {table_a, table_b}

        listed = backup_api.list_backups(location=location)
        backups = listed["backups"]
        matched = [item for item in backups if item["backup_id"] == backup_id]
        assert len(matched) == 1
        assert set(matched[0]["tables"]) == {table_a, table_b}

        backup_api.delete_table(table_a)
        backup_api.delete_table(table_b)
        _wait_until_table_absent(backup_api, table_a, timeout_s=10.0, interval_s=0.5)
        _wait_until_table_absent(backup_api, table_b, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_a, "doc:1", timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_b, "doc:1", timeout_s=10.0, interval_s=0.5)

        restore = backup_api.cluster_restore(
            backup_id=backup_id,
            location=location,
            restore_mode="fail_if_exists",
        )
        assert restore["status"] == "triggered"
        assert {table["status"] for table in restore["tables"]} == {"triggered"}

        for table_name, expected_title in (
            (table_a, "Cluster Backup Alpha"),
            (table_b, "Cluster Backup Beta"),
        ):
            restored_doc = wait_until(
                lambda tn=table_name: _lookup_doc(backup_api, tn, "doc:1"),
                timeout_s=60.0,
                interval_s=1.0,
            )
            assert restored_doc is not None
            assert restored_doc["title"] == expected_title


@pytest.mark.objectstore_integration
@pytest.mark.parametrize("backend", ["s3", "gs"])
def test_cluster_backup_restore_round_trip_remote_backend(backup_api, backend: str):
    location = _remote_backup_location(backend)
    table_a = f"cluster_{backend}_a_{time.time_ns()}"
    table_b = f"cluster_{backend}_b_{time.time_ns()}"
    backup_id = f"cluster-{backend}-backup-{time.time_ns()}"

    for table_name, title in (
        (table_a, f"{backend.upper()} Backup Alpha"),
        (table_b, f"{backend.upper()} Backup Beta"),
    ):
        created = backup_api.create_table(table_name, num_shards=1, description=f"{table_name} docs")
        assert created["name"] == table_name
        batch = backup_api.batch_write(
            table_name,
            inserts={
                "doc:1": {
                    "title": title,
                    "content": f"{title} survives remote backup and restore.",
                }
            },
            sync_level="full_text",
        )
        assert batch["inserted"] == 1
        assert wait_until(
            lambda tn=table_name, q=title.lower(), doc_id="doc:1": _top_hit(backup_api, tn, q, doc_id),
            timeout_s=60.0,
            interval_s=1.0,
        )

    backup = backup_api.cluster_backup(backup_id=backup_id, location=location)
    assert backup["backup_id"] == backup_id
    assert backup["status"] == "completed"
    assert {table["name"] for table in backup["tables"]} == {table_a, table_b}

    listed = backup_api.list_backups(location=location)
    backups = listed["backups"]
    matched = [item for item in backups if item["backup_id"] == backup_id]
    assert len(matched) == 1
    assert set(matched[0]["tables"]) == {table_a, table_b}
    assert matched[0]["location"] == location

    backup_api.delete_table(table_a)
    backup_api.delete_table(table_b)
    _wait_until_table_absent(backup_api, table_a, timeout_s=10.0, interval_s=0.5)
    _wait_until_table_absent(backup_api, table_b, timeout_s=10.0, interval_s=0.5)

    restore = backup_api.cluster_restore(
        backup_id=backup_id,
        location=location,
        restore_mode="fail_if_exists",
    )
    assert restore["status"] == "triggered"
    assert {table["status"] for table in restore["tables"]} == {"triggered"}

    for table_name, expected_title in (
        (table_a, f"{backend.upper()} Backup Alpha"),
        (table_b, f"{backend.upper()} Backup Beta"),
    ):
        restored_doc = wait_until(
            lambda tn=table_name: _lookup_doc(backup_api, tn, "doc:1"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        assert restored_doc is not None
        assert restored_doc["title"] == expected_title


def test_cluster_restore_modes(backup_api):
    table_a = f"cluster_modes_a_{time.time_ns()}"
    table_b = f"cluster_modes_b_{time.time_ns()}"
    backup_id = f"cluster-modes-{time.time_ns()}"

    for table_name, title in (
        (table_a, "Original Alpha"),
        (table_b, "Original Beta"),
    ):
        created = backup_api.create_table(table_name, num_shards=1, description=f"{table_name} docs")
        assert created["name"] == table_name
        _write_single_doc(backup_api, table_name, "doc:1", title=title, content=f"{title} backup source")
        assert wait_until(
            lambda tn=table_name, q=title.lower(), doc_id="doc:1": _top_hit(backup_api, tn, q, doc_id),
            timeout_s=60.0,
            interval_s=1.0,
        )

    with tempfile.TemporaryDirectory(prefix="antfly-cluster-modes-") as backup_dir:
        location = f"file://{backup_dir}"

        backup = backup_api.cluster_backup(backup_id=backup_id, location=location)
        assert backup["status"] == "completed"

        fail_resp = backup_api._request(
            "POST",
            "/restore",
            {
                "backup_id": backup_id,
                "location": location,
                "restore_mode": "fail_if_exists",
            },
        )
        assert fail_resp.status_code == 400
        assert "already exists" in fail_resp.text

        _write_single_doc(backup_api, table_a, "doc:1", title="Mutated Alpha", content="mutated alpha")
        _write_single_doc(backup_api, table_b, "doc:1", title="Mutated Beta", content="mutated beta")
        mutated_a = wait_until(
            lambda: _lookup_doc(backup_api, table_a, "doc:1"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        mutated_b = wait_until(
            lambda: _lookup_doc(backup_api, table_b, "doc:1"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        assert mutated_a is not None and mutated_a["title"] == "Mutated Alpha"
        assert mutated_b is not None and mutated_b["title"] == "Mutated Beta"

        skip_restore = backup_api.cluster_restore(
            backup_id=backup_id,
            location=location,
            restore_mode="skip_if_exists",
        )
        assert skip_restore["status"] == "triggered"
        assert {table["status"] for table in skip_restore["tables"]} == {"skipped"}

        skipped_a = _lookup_doc(backup_api, table_a, "doc:1")
        skipped_b = _lookup_doc(backup_api, table_b, "doc:1")
        assert skipped_a is not None and skipped_a["title"] == "Mutated Alpha"
        assert skipped_b is not None and skipped_b["title"] == "Mutated Beta"

        overwrite_restore = backup_api.cluster_restore(
            backup_id=backup_id,
            location=location,
            restore_mode="overwrite",
        )
        assert overwrite_restore["status"] == "triggered"
        assert {table["status"] for table in overwrite_restore["tables"]} == {"triggered"}

        restored_docs = wait_until(
            lambda: _lookup_docs(backup_api, (table_a, table_b), "doc:1"),
            timeout_s=60.0,
            interval_s=1.0,
        )
        assert restored_docs is not None
        assert restored_docs[table_a]["title"] == "Original Alpha"
        assert restored_docs[table_b]["title"] == "Original Beta"


def test_cluster_backup_restore_partial_statuses(backup_api):
    table_name = f"cluster_partial_{time.time_ns()}"
    backup_id = f"cluster-partial-{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1, description="partial backup docs")
    assert created["name"] == table_name
    _write_single_doc(backup_api, table_name, "doc:1", title="Partial Table", content="table survives partial backup")
    assert wait_until(
        lambda: _top_hit(backup_api, table_name, "partial table", "doc:1"),
        timeout_s=60.0,
        interval_s=1.0,
    )

    with tempfile.TemporaryDirectory(prefix="antfly-cluster-partial-") as backup_dir:
        location = f"file://{backup_dir}"

        backup = backup_api.cluster_backup(
            backup_id=backup_id,
            location=location,
            table_names=[table_name, "missing"],
        )
        assert backup["status"] == "partial"
        by_name = {table["name"]: table for table in backup["tables"]}
        assert by_name[table_name]["status"] == "completed"
        assert by_name["missing"]["status"] == "failed"
        assert "not found" in by_name["missing"]["error"]

        listed = backup_api.list_backups(location=location)
        matched = [item for item in listed["backups"] if item["backup_id"] == backup_id]
        assert len(matched) == 1
        assert matched[0]["tables"] == [table_name]

        backup_api.delete_table(table_name)
        _wait_until_table_absent(backup_api, table_name, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_name, "doc:1", timeout_s=10.0, interval_s=0.5)

        restore = backup_api.cluster_restore(
            backup_id=backup_id,
            location=location,
            table_names=[table_name, "missing"],
        )
        assert restore["status"] == "partial"
        restore_by_name = {table["name"]: table for table in restore["tables"]}
        assert restore_by_name[table_name]["status"] == "triggered"
        assert restore_by_name["missing"]["status"] == "failed"
        assert "backup does not include table" in restore_by_name["missing"]["error"]

        restored_doc = wait_until(
            lambda: _lookup_doc(backup_api, table_name, "doc:1"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        assert restored_doc is not None
        assert restored_doc["title"] == "Partial Table"


def test_backup_restore_request_validation(backup_api):
    with tempfile.TemporaryDirectory(prefix="antfly-backup-validate-") as backup_dir:
        location = f"file://{backup_dir}"
        table_name = f"validate_backup_case_{time.time_ns()}"

        created = backup_api.create_table(table_name, num_shards=1)
        assert created["name"] == table_name

        invalid_cases = (
            ("POST", f"/tables/{table_name}/backup", {}, "invalid backup request"),
            ("POST", f"/tables/{table_name}/restore", {}, "invalid restore request"),
            ("POST", "/backup", {}, "invalid backup request"),
            ("POST", "/restore", {}, "invalid restore request"),
            (
                "POST",
                f"/tables/{table_name}/backup",
                {"backup_id": "snap", "location": "ftp://bucket/path"},
                "unsupported backup location",
            ),
            (
                "POST",
                f"/tables/{table_name}/restore",
                {"backup_id": "snap", "location": "ftp://bucket/path"},
                "unsupported backup location",
            ),
            (
                "POST",
                "/backup",
                {"backup_id": "snap", "location": "ftp://bucket/path"},
                "unsupported backup location",
            ),
            (
                "POST",
                "/restore",
                {"backup_id": "snap", "location": "ftp://bucket/path"},
                "unsupported backup location",
            ),
            (
                "POST",
                "/restore",
                {"backup_id": "snap", "location": location, "restore_mode": "bogus"},
                "invalid restore request",
            ),
        )

        for method, path, payload, expected in invalid_cases:
            response = backup_api._request(method, path, payload)
            assert response.status_code == 400
            assert expected in response.text

        missing_location = backup_api.s.get(f"{backup_api.url}/backups", timeout=30)
        assert missing_location.status_code == 400
        assert "Missing required query parameter: location" in missing_location.text

        unsupported_location = backup_api.s.get(
            f"{backup_api.url}/backups?location=ftp://bucket/path",
            timeout=30,
        )
        assert unsupported_location.status_code == 400
        assert "unsupported backup location" in unsupported_location.text


def test_list_backups_empty_location(backup_api):
    with tempfile.TemporaryDirectory(prefix="antfly-empty-backups-") as backup_dir:
        location = f"file://{backup_dir}"
        listed = backup_api.list_backups(location=location)
        assert listed == {"backups": []}


def test_restore_missing_backup_returns_bad_request(backup_api):
    table_name = f"restore_missing_{time.time_ns()}"
    missing_backup_id = f"missing-{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    with tempfile.TemporaryDirectory(prefix="antfly-missing-restore-") as backup_dir:
        location = f"file://{backup_dir}"

        table_restore = backup_api._request(
            "POST",
            f"/tables/{table_name}/restore",
            {
                "backup_id": missing_backup_id,
                "location": location,
            },
        )
        assert table_restore.status_code == 400
        assert "restore target already exists" in table_restore.text

        cluster_restore = backup_api._request(
            "POST",
            "/restore",
            {
                "backup_id": missing_backup_id,
                "location": location,
                "restore_mode": "fail_if_exists",
            },
        )
        assert cluster_restore.status_code == 400
        assert "invalid restore request" in cluster_restore.text
