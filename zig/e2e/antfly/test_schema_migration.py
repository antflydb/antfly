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

"""Stateful public API schema migration tests."""

from __future__ import annotations

import time

from helpers import wait_until


SCHEMA_MIGRATION_REBUILD_TIMEOUT_S = 240.0


def _index_stats(index_status: dict) -> dict:
    return index_status["status"]


def _index_names(index_list: list[dict]) -> set[str]:
    return {entry["config"]["name"] for entry in index_list}


def test_schema_migration_full_text_rebuild(stateful_api):
    table_name = f"schema_migration_{time.time_ns()}"
    num_docs = 1000

    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name
    assert "full_text_index_v0" in created["indexes"]

    inserts = {
        f"doc-{i:04d}": {
            "title": f"Document {i}",
            "content": f"This is the content of document number {i} with some searchable text.",
        }
        for i in range(num_docs)
    }
    batch = stateful_api.batch_write(table_name, inserts=inserts, sync_level="full_text")
    assert batch["inserted"] == num_docs

    initial_index = wait_until(
        lambda: _ready_index(stateful_api, table_name, "full_text_index_v0", expected_docs=num_docs),
        timeout_s=SCHEMA_MIGRATION_REBUILD_TIMEOUT_S,
    )
    assert initial_index is not None, "full_text_index_v0 did not finish rebuilding in time"

    updated = stateful_api.update_schema(
        table_name,
        {
            "document_schemas": {
                "default": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "title": {
                                "type": "string",
                                "x-antfly-types": ["text"],
                                "x-antfly-include-in-all": True,
                            },
                            "content": {
                                "type": "string",
                                "x-antfly-types": ["text"],
                                "x-antfly-include-in-all": True,
                            },
                        },
                    }
                }
            }
        },
    )
    assert updated["schema"]["version"] == 1
    updated_migration = updated.get("migration")
    if updated_migration is not None:
        assert updated_migration["state"] == "rebuilding"
        assert updated_migration["read_schema"]["version"] == 0

    table_status = stateful_api.get_table(table_name)
    migration = table_status.get("migration")
    if migration is not None:
        # Migration may already be complete if the reconciler is fast.
        assert migration["state"] == "rebuilding"
        assert migration["read_schema"]["version"] == 0
    assert table_status["schema"]["version"] == 1

    index_names = _index_names(stateful_api.list_indexes(table_name))
    assert "full_text_index_v1" in index_names

    rebuilt_index = wait_until(
        lambda: _ready_index(stateful_api, table_name, "full_text_index_v1", expected_docs=num_docs),
        timeout_s=SCHEMA_MIGRATION_REBUILD_TIMEOUT_S,
    )
    assert rebuilt_index is not None, "full_text_index_v1 did not finish rebuilding in time"

    stable_table = wait_until(
        lambda: _stable_table(stateful_api, table_name, expected_version=1),
        timeout_s=SCHEMA_MIGRATION_REBUILD_TIMEOUT_S,
        interval_s=2.0,
    )
    assert stable_table is not None, "schema migration did not reach a stable table state"

    stable_indexes = _index_names(stateful_api.list_indexes(table_name))
    assert "full_text_index_v0" not in stable_indexes
    assert "full_text_index_v1" in stable_indexes

    assert stable_table["schema"]["version"] == 1

    doc = stateful_api.lookup_key(table_name, "doc-0500")
    assert doc["title"] == "Document 500"
    assert "searchable text" in doc["content"]


def _ready_index(stateful_api, table_name: str, index_name: str, *, expected_docs: int) -> dict | None:
    try:
        stats = _index_stats(stateful_api.get_index(table_name, index_name))
    except Exception:
        return None
    if stats.get("backfill_active", False):
        return None
    total_indexed = stats.get("doc_count", 0)
    if total_indexed < expected_docs:
        return None
    return stats


def _stable_table(stateful_api, table_name: str, *, expected_version: int) -> dict | None:
    try:
        table = stateful_api.get_table(table_name)
    except Exception:
        return None
    if table.get("migration") is not None:
        return None
    if table["schema"]["version"] != expected_version:
        return None
    return table
