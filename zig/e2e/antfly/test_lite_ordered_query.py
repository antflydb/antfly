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

"""Ordered first pages work on every engine; only Lite serves them live."""

import os

import conftest as harness
import pytest
import requests

ORDER = [{"field": "_id", "desc": False}]
IDS = ["doc/a", "doc/b", "doc/c", "other/d"]


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("storage_engine", ["local", "lite"])
def test_ordered_first_page_on_every_engine(storage_engine):
    binary = harness.resolve_binary_path(
        os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
    )
    server = harness.StandaloneAntflyServer(
        binary, "127.0.0.1", 0, storage_engine=storage_engine
    )
    failed = True
    try:
        with requests.Session() as session:
            session.headers["Connection"] = "close"

            def post(path, body):
                return session.post(server.api_url + path, json=body, timeout=30)

            def query(body):
                response = post("/tables/docs/query", body)
                assert response.ok, response.text
                result = response.json()["responses"][0]
                if storage_engine == "lite":
                    assert result.get("live_read") is True
                    assert result.get("remote_snapshot") is None
                else:
                    assert result.get("live_read") is None
                    assert result["remote_snapshot"]
                return result

            assert post("/tables/docs", {"num_shards": 1}).ok
            batch = post(
                "/tables/docs/batch",
                {
                    "inserts": {
                        doc_id: {"title": doc_id, "n": index}
                        for index, doc_id in enumerate(reversed(IDS))
                    },
                    # The writer chooses visibility; Lite indexes lag at "write".
                    "sync_level": "full_index",
                },
            )
            assert batch.ok, batch.text

            scan = query({"order_by": ORDER, "limit": 10, "fields": ["title"]})
            hits = scan["hits"]["hits"]
            assert [hit["_id"] for hit in hits] == IDS
            assert [hit["_source"]["title"] for hit in hits] == IDS
            assert hits[0]["_sort"] == ["doc/a"]

            lookup = query(
                {
                    "query": {"doc_id": {"ids": ["doc/c", "doc/a", "missing"]}},
                    "order_by": ORDER,
                    "limit": 3,
                }
            )
            assert [hit["_id"] for hit in lookup["hits"]["hits"]] == ["doc/a", "doc/c"]

            prefix = query(
                {
                    "filter_prefix": "doc/",
                    "query": {
                        "bool": {"must_not": [{"doc_id": {"ids": ["doc/a", "doc/b"]}}]}
                    },
                    "order_by": ORDER,
                    "limit": 1,
                }
            )
            assert [hit["_id"] for hit in prefix["hits"]["hits"]] == ["doc/c"]

            # A live page has nothing to pin a continuation to.
            cursor = post(
                "/tables/docs/query",
                {"order_by": ORDER, "limit": 1, "search_after": hits[0]["_sort"]},
            )
            assert cursor.status_code == 409, cursor.text

            # A committed insert can still be absent from Lite's live index.
            # Source cleanup must reconcile primary rows and wait for indexing,
            # rather than treating an empty live query as proof of absence.
            pending = post(
                "/tables/docs/batch",
                {"inserts": {"doc/pending": {"title": "pending"}}, "sync_level": "write"},
            )
            assert pending.ok, pending.text
            cleanup = session.post(
                server.api_url + "/tables/docs/merge",
                json={
                    "scoped_merge": {
                        "key_range": {"from": "doc/", "to": "doc0"},
                        "records": {},
                        "retain_ids": ["doc/a"],
                        "last_merged_id": "doc/",
                        "delete_missing": True,
                        "sync_level": "full_index",
                    }
                },
                headers={"Content-Type": "application/vnd.antfly.scoped-merge.v1+json"},
                timeout=30,
            )
            assert cleanup.ok, cleanup.text
            assert cleanup.json()["deleted"] == 3
            remaining = query({"order_by": ORDER, "limit": 10})
            assert [hit["_id"] for hit in remaining["hits"]["hits"]] == [
                "doc/a",
                "other/d",
            ]
        failed = False
    finally:
        server.stop(test_failed=failed)
