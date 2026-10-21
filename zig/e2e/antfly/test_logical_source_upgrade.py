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

"""Upgrade root-only source authority without rewriting primary documents.

Set ANTFLY_LOGICAL_SOURCE_BASELINE_BIN to a preserved DPD1-writing server.
The baseline must otherwise support the current index generation witness.
"""

import os
import time
from pathlib import Path

import pytest
import requests


@pytest.mark.skipif(
    not os.environ.get("ANTFLY_LOGICAL_SOURCE_BASELINE_BIN"),
    reason="requires a preserved root-authority server binary",
)
def test_logical_source_authority_upgrade_and_rollback(stateful_api):
    api = stateful_api
    server = api._server
    if not api.supports_restart or server is None:
        pytest.skip("requires a locally managed server for binary replacement")
    baseline = str(Path(os.environ["ANTFLY_LOGICAL_SOURCE_BASELINE_BIN"]).resolve())
    assert Path(baseline).is_file(), baseline
    target = server.binary
    assert Path(target).resolve() != Path(baseline)
    table = f"source_upgrade_{time.time_ns()}"
    count = 96
    body = {"grep": {"content": "needle", "regions": ["\\n" * 16] * 1024}}
    payload = {
        "query": {"contains": "needle", "field": "grep.content"},
        "count": True,
        "limit": 0,
        "fields": [],
        "profile": True,
        "value_search_execution": {
            "allow_scan": True,
            "max_documents": count,
            "max_bytes": 16 * 1024 * 1024,
        },
    }

    def switch(binary):
        api.pause_server()
        server.binary = binary
        api.resume_server()

    def query(expected):
        response = requests.post(
            api.url + f"/tables/{table}/query", json=payload, timeout=60
        )
        assert response.status_code == 200, response.text
        result = response.json()["responses"][0]
        assert not result.get("error"), result
        assert result["hits"]["total"]["value"] == expected, result
        profile = result["profile"]["value_search"]
        assert profile["documents"] == count, profile
        assert profile["bytes"] > count * 32 * 1024, profile
        return profile

    def await_projection(expected):
        deadline = time.monotonic() + 60
        while True:
            profile = query(expected)
            if profile.get("stored_field_sources", 0) > 0:
                return
            assert time.monotonic() < deadline, (profile, api.debug_logs())
            time.sleep(0.2)

    try:
        switch(baseline)
        api.create_table(table)
        api.batch_write(
            table,
            inserts={f"{i:03}": body for i in range(count)},
            sync_level="full_index",
        )
        query(count)
        switch(target)
        # Legacy reads remain exact while writable-owner maintenance upgrades
        # the disposable directory. No source re-ingestion/reindex is needed.
        await_projection(count)
        api.restart_server()
        await_projection(count)
        # The old reader rejects unknown DPD2 metadata and uses original bytes.
        switch(baseline)
        query(count)
        changed = {"grep": {**body["grep"], "content": "other"}}
        api.batch_write(table, inserts={"000": changed}, sync_level="full_index")
        query(count - 1)
        switch(target)
        await_projection(count - 1)
    finally:
        if server.binary != target:
            switch(target)
