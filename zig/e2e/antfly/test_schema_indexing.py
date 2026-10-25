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

"""Document-schema indexing semantics: explicit declarations versus dynamic indexing.

Regression coverage for antflydb/antfly#839: a property declared with
`x-antfly-index: false` (or another non-text declaration) must stay out of the
text index whatever `additionalProperties` says about undeclared fields.
"""

import time

import pytest

pytestmark = pytest.mark.reuse_antfly_process

ROW = {
    "body": "zebrafish",
    "stored_only": "xylophone",
    "attachment": "aardvark",
    "undeclared": "quokka",
    "meta": {"label": "lemur", "secret": {"token": "walrus"}, "loose": "ibex"},
}

WORDS = {
    "body": "zebrafish",
    "stored_only": "xylophone",
    "attachment": "aardvark",
    "undeclared": "quokka",
    "meta.label": "lemur",
    "meta.secret.token": "walrus",
    "meta.loose": "ibex",
}

PROPERTIES = {
    "body": {"type": "string", "x-antfly-types": ["text"]},
    "stored_only": {"type": "string", "x-antfly-index": False},
    "attachment": {"type": "string", "x-antfly-types": ["blob"]},
    "meta": {
        "type": "object",
        "properties": {
            "label": {"type": "string"},
            "secret": {
                "type": "object",
                "x-antfly-index": False,
                "properties": {"token": {"type": "string"}},
            },
        },
    },
}


def _document_schema(additional_properties, *, infer_types: bool = False) -> dict:
    schema: dict = {"type": "object", "properties": PROPERTIES}
    if additional_properties is not None:
        schema["additionalProperties"] = additional_properties
    if infer_types:
        schema["x-antfly-dynamic-indexing"] = {"mode": "infer_types"}
    return {"default_type": "doc", "document_schemas": {"doc": {"schema": schema}}}


def _create_table_with_schema(stateful_api, table_name: str, schema: dict) -> dict:
    return stateful_api.post(
        f"/tables/{table_name}", {"num_shards": 1, "schema": schema}
    )


def _hits(stateful_api, table_name: str, field: str) -> int:
    response = stateful_api.query_table(
        table_name,
        {"full_text_search": {"match": WORDS[field], "field": field}, "limit": 5},
    )
    hits = response["responses"][0]["hits"]
    total = hits.get("total")
    if isinstance(total, dict):
        return int(total.get("value", 0))
    return len(hits.get("hits", []))


@pytest.mark.parametrize(
    ("label", "additional_properties", "infer_types"),
    [
        ("unset", None, False),
        ("true", True, False),
        ("infer_types", True, True),
    ],
)
def test_explicit_declarations_win_over_dynamic_indexing(
    stateful_api, label, additional_properties, infer_types
):
    table = f"schema_index_{label}_{time.time_ns()}"
    _create_table_with_schema(
        stateful_api,
        table,
        _document_schema(additional_properties, infer_types=infer_types),
    )
    stateful_api.batch_write(
        table, inserts={"d1": {"_type": "doc", **ROW}}, sync_level="full_text"
    )

    # The stored declaration is echoed back verbatim.
    stored = stateful_api.get_table(table)["schema"]["document_schemas"]["doc"][
        "schema"
    ]
    assert stored["properties"]["stored_only"] == {
        "type": "string",
        "x-antfly-index": False,
    }

    # Declared text is indexed in every configuration.
    assert _hits(stateful_api, table, "body") == 1
    assert _hits(stateful_api, table, "meta.label") == 1

    # Explicit declarations own their paths: neither `x-antfly-index: false`
    # nor a non-text type is picked up by the dynamic mapper.
    assert _hits(stateful_api, table, "stored_only") == 0
    assert _hits(stateful_api, table, "attachment") == 0
    assert _hits(stateful_api, table, "meta.secret.token") == 0

    # Undeclared fields are indexed only when dynamic indexing is enabled.
    dynamic = additional_properties is True
    assert _hits(stateful_api, table, "undeclared") == (1 if dynamic else 0)
    assert _hits(stateful_api, table, "meta.loose") == (1 if dynamic else 0)


def test_additional_properties_false_rejects_undeclared_fields(stateful_api):
    table = f"schema_index_closed_{time.time_ns()}"
    _create_table_with_schema(stateful_api, table, _document_schema(False))

    rejected = stateful_api._request(
        "POST",
        f"/tables/{table}/batch",
        {"inserts": {"d1": {"_type": "doc", **ROW}}, "sync_level": "full_text"},
    )
    assert rejected.status_code == 400, rejected.text

    declared_only = {key: value for key, value in ROW.items() if key != "undeclared"}
    declared_only["meta"] = {
        key: value for key, value in ROW["meta"].items() if key != "loose"
    }
    stateful_api.batch_write(
        table, inserts={"d1": {"_type": "doc", **declared_only}}, sync_level="full_text"
    )
    assert _hits(stateful_api, table, "body") == 1
    assert _hits(stateful_api, table, "stored_only") == 0
    assert _hits(stateful_api, table, "attachment") == 0


def test_disabled_root_subfields_are_scoped_to_document_type(stateful_api):
    table = f"schema_disabled_subfields_{time.time_ns()}"
    body = {"type": "string", "x-antfly-field": {"type": "text"}}
    payload = {
        "type": "string",
        "x-antfly-field": {
            "type": "text",
            "index": False,
            "fields": {"exact": {"type": "keyword"}},
        },
    }
    _create_table_with_schema(
        stateful_api,
        table,
        {
            "default_type": "alpha",
            "document_schemas": {
                name: {
                    "schema": {
                        "type": "object",
                        "properties": {"body": body, "payload": mapping},
                    }
                }
                for name, mapping in (
                    ("alpha", {"type": "string", "x-antfly-index": False}),
                    ("beta", payload),
                )
            },
        },
    )
    stateful_api.batch_write(
        table,
        inserts={
            name: {"_type": name, "body": "quasar", "payload": "hiddenvalue"}
            for name in ("alpha", "beta")
        },
        sync_level="full_text",
    )
    for field, expected in (("payload.exact", ["beta"]), ("payload", [])):
        response = stateful_api.query_table(
            table,
            {"full_text_search": {"match": "hiddenvalue", "field": field}, "limit": 5},
        )
        assert [h["_id"] for h in response["responses"][0]["hits"]["hits"]] == expected


@pytest.mark.parametrize(
    "match_type,mapping_type",
    [("number", "number"), ("boolean", "boolean"), ("date", "date")],
)
def test_type_filtered_dynamic_exclusions_preserve_declared_text(
    stateful_api, match_type, mapping_type
):
    table = f"schema_dynamic_exclusion_{match_type}_{time.time_ns()}"
    _create_table_with_schema(
        stateful_api,
        table,
        {
            "default_type": "doc",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "type": "object",
                        "properties": {"body": {"type": "string"}},
                    }
                }
            },
            "dynamic_templates": [
                {
                    "name": "disabled_other_type",
                    "match": "*",
                    "match_mapping_type": match_type,
                    "mapping": {"type": mapping_type, "index": False},
                }
            ],
        },
    )
    stateful_api.batch_write(
        table, inserts={"d1": {"body": WORDS["body"]}}, sync_level="full_text"
    )
    assert _hits(stateful_api, table, "body") == 1
