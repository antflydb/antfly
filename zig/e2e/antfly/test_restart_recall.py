# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the License at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations.

"""Clean-process restart regressions for persisted query recall."""

from __future__ import annotations

import time

import pytest

from helpers import query_hits_total_value


DOCUMENT_COUNT = 5_000
PUBLISHED_COUNT = 4_237
BATCH_SIZE = 100


def _query_total(stateful_api, table_name: str, state: str | None = None) -> int:
    query: dict[str, object] = {
        "full_text_search": {"match": "catalog", "field": "title"},
        "limit": 1,
    }
    if state is not None:
        query["filter_query"] = {"term": state, "field": "state"}
    response = stateful_api.query_table(table_name, query)
    responses = response.get("responses", [])
    assert len(responses) == 1, response
    return query_hits_total_value(responses[0]["hits"])


def _assert_expected_recall(stateful_api, table_name: str) -> None:
    totals = {
        "published": _query_total(stateful_api, table_name, "published"),
        "draft": _query_total(stateful_api, table_name, "draft"),
        "unfiltered": _query_total(stateful_api, table_name),
    }
    assert totals == {
        "published": PUBLISHED_COUNT,
        "draft": DOCUMENT_COUNT - PUBLISHED_COUNT,
        "unfiltered": DOCUMENT_COUNT,
    }


@pytest.mark.slow
def test_high_frequency_keyword_filter_recall_survives_clean_restarts(stateful_api):
    """Exercise public ingestion, durable keyword postings, and process recovery."""

    table_name = f"restart_keyword_recall_{time.time_ns()}"
    created = stateful_api.post(
        f"/tables/{table_name}",
        {
            "num_shards": 1,
            "schema": {
                "default_type": "product",
                "document_schemas": {
                    "product": {
                        "schema": {
                            "type": "object",
                            "additionalProperties": True,
                            "properties": {
                                "title": {
                                    "type": "string",
                                    "x-antfly-types": ["text"],
                                },
                                "state": {
                                    "type": "string",
                                    "x-antfly-types": ["keyword"],
                                },
                            },
                        }
                    }
                },
            },
        },
    )
    assert created["name"] == table_name

    for batch_start in range(0, DOCUMENT_COUNT, BATCH_SIZE):
        inserts = {
            f"doc:{doc_id:04d}": {
                "title": f"Catalog document {doc_id}",
                "state": "published" if doc_id < PUBLISHED_COUNT else "draft",
            }
            for doc_id in range(batch_start, batch_start + BATCH_SIZE)
        }
        result = stateful_api.batch_write(
            table_name,
            inserts=inserts,
            sync_level="full_index",
        )
        assert result["inserted"] == BATCH_SIZE

    _assert_expected_recall(stateful_api, table_name)

    # Use two complete process cycles to catch recovery state that is consumed
    # or repaired during only the first reopen.
    for _ in range(2):
        try:
            stateful_api.restart_server()
        except AssertionError as exc:
            pytest.skip(str(exc))
        _assert_expected_recall(stateful_api, table_name)
