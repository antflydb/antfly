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

"""Stateful public API linear merge tests."""

from __future__ import annotations

import time

from helpers import wait_until


def _lookup_doc(stateful_api, table_name: str, key: str) -> dict | None:
    try:
        return stateful_api.lookup_key(table_name, key)
    except Exception:
        return None


def _wait_for_doc(stateful_api, table_name: str, key: str) -> dict | None:
    return wait_until(
        lambda: _lookup_doc(stateful_api, table_name, key),
        timeout_s=20.0,
        interval_s=0.2,
    )


def test_linear_merge_is_idempotent_for_unchanged_docs(stateful_api):
    table_name = f"linear_merge_{time.time_ns()}"
    created = stateful_api.create_table(
        table_name, num_shards=1, description="linear merge docs"
    )
    assert created["name"] == table_name

    records = {
        "docs/configuration.md": {
            "title": "Configuration",
            "content": "Configuration guide here.",
            "filepath": "docs/configuration.md",
            "type": "markdown",
        },
        "docs/getting-started.md": {
            "title": "Getting Started",
            "content": "This is the getting started guide.",
            "filepath": "docs/getting-started.md",
            "type": "markdown",
        },
        "docs/installation.md": {
            "title": "Installation",
            "content": "Installation instructions here.",
            "filepath": "docs/installation.md",
            "type": "markdown",
        },
    }

    first = stateful_api.linear_merge(table_name, records=records, sync_level="write")
    assert first["status"] == "success"
    assert first["upserted"] == 3
    assert first["skipped"] == 0
    assert first["deleted"] == 0
    assert first["next_cursor"] == "docs/installation.md"

    assert (
        _wait_for_doc(stateful_api, table_name, "docs/getting-started.md") is not None
    )
    assert _wait_for_doc(stateful_api, table_name, "docs/installation.md") is not None
    assert _wait_for_doc(stateful_api, table_name, "docs/configuration.md") is not None

    second = stateful_api.linear_merge(table_name, records=records, sync_level="write")
    assert second["status"] == "success"
    assert second["upserted"] == 0
    assert second["skipped"] == 3
    assert second["deleted"] == 0

    modified = {
        **records,
        "docs/getting-started.md": {
            "title": "Getting Started",
            "content": "This is the UPDATED getting started guide.",
            "filepath": "docs/getting-started.md",
            "type": "markdown",
        },
    }
    third = stateful_api.linear_merge(table_name, records=modified, sync_level="write")
    assert third["status"] == "success"
    assert third["upserted"] == 1
    assert third["skipped"] == 2
    assert third["deleted"] == 0

    updated = wait_until(
        lambda: _lookup_doc(stateful_api, table_name, "docs/getting-started.md"),
        timeout_s=20.0,
        interval_s=0.2,
    )
    assert updated is not None
    assert updated["content"] == "This is the UPDATED getting started guide."
