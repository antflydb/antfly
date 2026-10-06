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

"""Pure contracts for fixture table-create admission and uncertain outcomes."""

import copy
import json
from types import SimpleNamespace

import pytest
import requests
import test_backup_restore as backups


def _response(status, body, headers=None):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode()
    response.headers.update(headers or {})
    response.url = "http://localhost/db/v1/tables/unique"
    response.request = requests.Request("GET", response.url).prepare()
    return response


def _harness(monkeypatch, observations, post_response=None):
    now = [0.0]
    monkeypatch.setattr(backups.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        backups.time, "sleep", lambda delay: now.__setitem__(0, now[0] + delay)
    )
    reads, writes = [], []
    pending = iter(observations)

    def get(url, **kwargs):
        reads.append((url, kwargs))
        value = next(pending, _response(404, {}))
        if isinstance(value, Exception):
            raise value
        return value

    def post(url, **kwargs):
        writes.append((url, kwargs))
        return (
            post_response
            if post_response is not None
            else _response(
                409,
                {"error": "outcome unknown"},
                {"X-Antfly-Raft-Mutation-Outcome": "unknown-v1"},
            )
        )

    cluster = SimpleNamespace(
        data_api_urls=["http://localhost/db/v1"],
        assert_processes_alive=lambda: None,
        debug_logs=lambda: "reconciliation diagnostics",
    )
    return cluster, SimpleNamespace(get=get, post=post), reads, writes, now


def _table(schema=None):
    return {
        "name": "unique",
        "description": "owned fixture",
        "shards": {"a": {}, "b": {}, "c": {}},
        "schema": schema,
    }


def _definition(schema=None):
    return {"num_shards": 3, "description": "owned fixture", "schema": schema}


@pytest.mark.parametrize("intermediate", [404, 503, requests.Timeout("lost GET")])
def test_unknown_create_observes_matching_definition_without_post_replay(
    monkeypatch, intermediate
):
    observed = _table(
        {
            "version": 1,
            "storage_mode": "document",
            "enforce_types": False,
            "default_type": "doc",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "type": "object",
                        "additionalProperties": True,
                        "x-antfly-dynamic-indexing": {"mode": "infer_types"},
                    }
                }
            },
        }
    )
    cluster, session, reads, writes, _ = _harness(
        monkeypatch,
        [
            _response(404, {}),
            intermediate
            if isinstance(intermediate, Exception)
            else _response(intermediate, {}),
            _response(200, observed),
        ],
    )
    assert (
        backups._create_cluster_table_when_admitted(
            cluster, session, "unique", _definition()
        )
        == observed
    )
    assert len(writes) == 1
    assert len(reads) == 3
    assert reads[-1][1]["timeout"] < writes[0][1]["timeout"]


def test_unknown_create_matches_typed_schema_defaults_and_generated_values(monkeypatch):
    schema = {
        "storage_mode": "relational",
        "default_type": "row",
        "document_schemas": {
            "row": {
                "schema": {
                    "type": "object",
                    "properties": {"n": {"type": "integer"}},
                    "additionalProperties": False,
                }
            }
        },
        "column_defaults": [
            {
                "column": "n",
                "expression": {
                    "op": "literal",
                    "type": "integer",
                    "value": "9007199254740993",
                },
            }
        ],
    }
    actual = {
        **copy.deepcopy(schema),
        "version": 1,
        "enforce_types": True,
        "checks": [],
        "ttl": None,
    }
    cluster, session, _, writes, _ = _harness(
        monkeypatch, [_response(404, {}), _response(200, _table(actual))]
    )
    backups._create_cluster_table_when_admitted(
        cluster, session, "unique", _definition(schema)
    )
    assert len(writes) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "other"),
        ("description", "other"),
        ("shards", {"one": {}}),
        ("schema", {"storage_mode": "relational"}),
        (
            "schema",
            {
                "column_defaults": [
                    {"column": "n", "expression": {"op": "literal", "value": 5}}
                ]
            },
        ),
        ("schema", {"foreign_keys": [{"name": "unexpected"}]}),
    ],
)
def test_unknown_create_rejects_semantic_mismatch(monkeypatch, field, value):
    observed = {**_table(), field: value}
    cluster, session, _, writes, _ = _harness(
        monkeypatch, [_response(404, {}), _response(200, observed)]
    )
    with pytest.raises(AssertionError, match="mismatch.*") as error:
        backups._create_cluster_table_when_admitted(
            cluster, session, "unique", _definition()
        )
    assert "reconciliation diagnostics" in str(error.value)
    assert "last_observation_status=200" in str(error.value)
    assert len(writes) == 1


def test_preexisting_matching_table_is_not_accepted(monkeypatch):
    cluster, session, _, writes, _ = _harness(monkeypatch, [_response(200, _table())])
    with pytest.raises(AssertionError, match="already exists before create"):
        backups._create_cluster_table_when_admitted(
            cluster, session, "unique", _definition()
        )
    assert writes == []


def test_unknown_create_observation_uses_original_deadline(monkeypatch):
    cluster, session, reads, writes, now = _harness(monkeypatch, [_response(404, {})])
    with pytest.raises(AssertionError, match="observation deadline exceeded") as error:
        backups._create_cluster_table_when_admitted(
            cluster, session, "unique", _definition(), timeout_s=0.25
        )
    assert len(writes) == 1
    assert now[0] == pytest.approx(0.25)
    assert [r[1]["timeout"] for r in reads] == pytest.approx([0.25, 0.25, 0.15, 0.05])
    assert "last_status=409" in str(error.value)


def test_contradictory_unknown_non_admission_is_not_observed_or_replayed(monkeypatch):
    cluster, session, reads, writes, _ = _harness(
        monkeypatch,
        [_response(404, {})],
        _response(
            503,
            {"code": "metadata_leader_unavailable", "retryable": True},
            {
                "X-Antfly-Raft-Mutation-Outcome": "unknown-v1",
                "X-Antfly-Metadata-Mutation-Not-Admitted": "true",
            },
        ),
    )
    with pytest.raises(AssertionError):
        backups._create_cluster_table_when_admitted(
            cluster, session, "unique", _definition()
        )
    assert len(writes) == len(reads) == 1


def test_unknown_create_unmodeled_options_fail_closed(monkeypatch):
    cluster, session, _, writes, _ = _harness(
        monkeypatch, [_response(404, {}), _response(200, _table())]
    )
    with pytest.raises(AssertionError, match="unsupported definition fields"):
        backups._create_cluster_table_when_admitted(
            cluster, session, "unique", {**_definition(), "tablespace_name": "unknown"}
        )
    assert len(writes) == 1
