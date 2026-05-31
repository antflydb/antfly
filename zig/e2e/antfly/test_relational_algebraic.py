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

"""End-to-end tests for algebraic (aggregation) index provisioning on relational
tables.

These drive the real server to verify, end to end:
  - the provisioned-table server can provision an explicit `algebraic` index on
    a relational table (table_provisioner now understands the algebraic kind);
    this previously failed with UnsupportedCreateTableRequest, which also broke
    table operations when such an index was present;
  - switching a table to relational mode auto-creates a schema-derived algebraic
    index (named algebraic_index_v0) with no user configuration.

The tests assert provisioning and that the table accepts writes. They do not
assert aggregation query results: serving aggregations from the algebraic index
on the single-node provisioned path is a separate, unfinished problem.
"""

from __future__ import annotations

import time

from helpers import wait_until

RELATIONAL_SCHEMA = {
    "version": 0,
    "storage_mode": "relational",
    "default_type": "row",
    "enforce_types": True,
    "document_schemas": {
        "row": {
            "schema": {
                "type": "object",
                "properties": {
                    "status": {"type": "keyword"},
                    "amount": {"type": "numeric"},
                },
                "required": ["status", "amount"],
                "additionalProperties": False,
            }
        }
    },
}

ROWS = {
    "row:1": {"status": "active", "amount": 10.0},
    "row:2": {"status": "archived", "amount": 20.0},
    "row:3": {"status": "active", "amount": 30.0},
}


def _index_entries(stateful_api, table_name):
    listed = stateful_api.list_indexes(table_name)
    return listed if isinstance(listed, list) else listed.get("indexes", [])


def _index_names(stateful_api, table_name):
    names = set()
    for entry in _index_entries(stateful_api, table_name):
        if not isinstance(entry, dict):
            continue
        config = entry.get("config", {})
        name = config.get("name") or entry.get("name")
        if name:
            names.add(name)
    return names


def _algebraic_index_name(stateful_api, table_name):
    for entry in _index_entries(stateful_api, table_name):
        if not isinstance(entry, dict):
            continue
        config = entry.get("config", {})
        if config.get("type") == "algebraic":
            return config.get("name") or entry.get("name") or True
    return None


def test_relational_explicit_algebraic_index_provisions(stateful_api):
    """An explicitly-created algebraic index on a relational table provisions and
    appears in the index list. This previously failed to provision with
    UnsupportedCreateTableRequest (table_provisioner did not understand the
    algebraic index kind), which broke table operations."""
    table_name = f"rel_alg_explicit_{time.time_ns()}"
    stateful_api.create_table(table_name, num_shards=1)
    stateful_api.update_schema(table_name, RELATIONAL_SCHEMA)

    stateful_api.create_index(table_name, "agg_idx", {"type": "algebraic", "derive_from_schema": True})
    assert "agg_idx" in _index_names(stateful_api, table_name)

    # Writes succeed against a table carrying an algebraic index (the index is
    # provisioned into the live DB, not rejected).
    result = stateful_api.batch_write(table_name, inserts=ROWS, sync_level="full_index")
    assert result.get("inserted") == 3


def test_relational_table_autocreates_algebraic_index(stateful_api):
    """Switching a table to relational mode auto-creates a schema-derived
    algebraic index (algebraic_index_v0) with no user configuration."""
    table_name = f"rel_alg_auto_{time.time_ns()}"
    stateful_api.create_table(table_name, num_shards=1)
    stateful_api.update_schema(table_name, RELATIONAL_SCHEMA)

    # No explicit index created -- the relational schema update injects one.
    alg = wait_until(lambda: _algebraic_index_name(stateful_api, table_name), timeout_s=30.0, interval_s=0.5)
    assert alg is not None, f"no algebraic index auto-created; indexes={_index_names(stateful_api, table_name)}"

    # The auto-created index is provisioned: writes against the table succeed.
    result = stateful_api.batch_write(table_name, inserts=ROWS, sync_level="full_index")
    assert result.get("inserted") == 3
