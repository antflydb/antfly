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

"""Cross-version generation admission and public repair for named autocomplete.

Set ANTFLY_GREP_UPGRADE_BASELINE_BIN to the preserved pre-witness binary.
The native unit fixture separately creates actual historical prefix-only terms;
this test checks the process upgrade, public error/status and repair workflow.
"""

import json
import os
import time
from pathlib import Path

import pytest
import requests

from conftest import finish_create_table


@pytest.mark.skipif(
    not os.environ.get("ANTFLY_GREP_UPGRADE_BASELINE_BIN"),
    reason="requires a preserved pre-witness server binary",
)
def test_named_autocomplete_generation_upgrade_requires_public_repair(stateful_api):
    api = stateful_api
    server = api._server
    if not api.supports_restart or server is None:
        pytest.skip("requires a locally managed server for binary replacement")
    baseline = str(Path(os.environ["ANTFLY_GREP_UPGRADE_BASELINE_BIN"]).resolve())
    assert Path(baseline).is_file(), baseline
    target = server.binary
    assert Path(target).resolve() != Path(baseline), "upgrade needs distinct binaries"
    table = f"named_upgrade_{time.time_ns()}"
    healthy = f"ordinary_upgrade_{time.time_ns()}"
    long_word = "abcdefghijklmnopqrstuvwxyzzebra"

    def switch(binary):
        api.pause_server()
        server.binary = binary
        api.resume_server()

    def query(name, leaf):
        return requests.post(
            api.url + f"/tables/{name}/query",
            json={"query": leaf},
            timeout=60,
        )

    def ids(response):
        response.raise_for_status()
        result = response.json()["responses"][0]
        assert not result.get("error"), result
        return [hit["_id"] for hit in result["hits"]["hits"]]

    try:
        switch(baseline)
        created = api._request(
            "POST",
            f"/tables/{table}",
            payload={
                "num_shards": 1,
                "schema": {
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "title": {
                                        "type": "string",
                                        "x-antfly-field": {
                                            "type": "text",
                                            "fields": {
                                                "suggest": {
                                                    "type": "search_as_you_type"
                                                }
                                            },
                                        },
                                    }
                                },
                            }
                        }
                    },
                },
            },
        )
        finish_create_table(api, table, created)
        api.batch_write(
            table,
            inserts={"long": {"title": long_word}, "short": {"title": "x"}},
            sync_level="full_index",
        )
        api.create_table(healthy)
        api.batch_write(
            healthy, inserts={"a": {"title": "needle"}}, sync_level="full_index"
        )
        assert ids(query(table, {"autocomplete": long_word, "field": "title"})) == [
            "long"
        ]
        switch(target)

        rejected = query(table, {"autocomplete": long_word, "field": "title"})
        # A successful empty hit list would silently hide the omitted terms.
        # Require an explicit failure, whether the route wraps it per query or
        # returns an HTTP-level admission error.
        if rejected.status_code < 400:
            body = rejected.json()
            assert (
                body.get("error")
                or body.get("errors")
                or any(item.get("error") for item in body.get("responses", []))
            ), body
        status = api.get(f"/tables/{table}/indexes/full_text_index_v0")
        assert "TextProjectionProvenanceMismatch" in json.dumps(status), status
        assert ids(query(healthy, {"match": "needle", "field": "title"})) == ["a"]

        repair_path = f"/tables/{table}/repair/run"
        repaired = api.post(
            repair_path,
            {"target": "index", "index": "full_text_index_v0", "force": True},
        )
        assert repaired["result"]["unsupported"] == 0, repaired
        deadline = time.monotonic() + 60
        while True:
            response = query(table, {"autocomplete": long_word, "field": "title"})
            if response.status_code == 200:
                result = response.json().get("responses", [{}])[0]
                if not result.get("error") and "hits" in result:
                    assert ids(response) == ["long"]
                    break
            assert time.monotonic() < deadline, (
                repaired,
                response.text,
                api.debug_logs(),
            )
            time.sleep(0.2)
            repaired = api.post(
                repair_path, {"target": "index", "index": "full_text_index_v0"}
            )
            assert repaired["result"]["unsupported"] == 0, repaired

        for text, expected in [
            ("x", ["short"]),
            (long_word, ["long"]),
            ("abcdefghijklmnopqrstuvwxyzzdog", []),
        ]:
            assert (
                ids(query(table, {"autocomplete": text, "field": "title"})) == expected
            )
        api.restart_server()
        assert ids(query(table, {"autocomplete": long_word, "field": "title"})) == [
            "long"
        ]
        assert ids(query(healthy, {"match": "needle", "field": "title"})) == ["a"]
    finally:
        # Return the fixture to its original binary before ordinary teardown.
        if server.binary != target:
            switch(target)
