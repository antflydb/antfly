# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

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
