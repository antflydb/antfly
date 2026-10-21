"""Generated catalog clients preserve errors and committed mutation outcomes."""

import httpx
import pytest

from antfly.client_generated.api.table_management import (
    clear_database_tablespace,
    clear_namespace_tablespace,
    create_database,
    create_namespace,
    create_query_source,
    create_tablespace,
    get_database,
    set_database_tablespace,
    set_namespace_tablespace,
)
from antfly.client_generated.client import Client
from antfly.client_generated.models.catalog_mutation_visibility_pending import CatalogMutationVisibilityPending
from antfly.client_generated.models.error import Error


@pytest.mark.parametrize(
    "operation",
    [
        create_database,
        create_namespace,
        create_tablespace,
        create_query_source,
        set_database_tablespace,
        clear_database_tablespace,
        set_namespace_tablespace,
        clear_namespace_tablespace,
    ],
)
def test_committed_visibility_pending_is_a_typed_success(operation):
    result = operation._parse_response(
        client=Client(base_url="http://unused", raise_on_unexpected_status=True),
        response=httpx.Response(202, json={"status": "committed_visibility_pending"}),
    )
    assert isinstance(result, CatalogMutationVisibilityPending)
    assert result.status == "committed_visibility_pending"


@pytest.mark.parametrize(
    "operation,status,code",
    [
        (get_database, 404, "CatalogNotFound"),
        (create_database, 409, "CatalogAlreadyExists"),
        (create_tablespace, 400, "InvalidTablespacePlacementPolicy"),
    ],
)
def test_catalog_errors_preserve_shared_envelope(operation, status, code):
    result = operation._parse_response(
        client=Client(base_url="http://unused", raise_on_unexpected_status=True),
        response=httpx.Response(status, json={"error": code, "code": code}),
    )
    assert isinstance(result, Error)
    assert result.error == code
    assert result.code == code


def test_table_capabilities_preserve_logical_groups_and_concrete_views():
    from antfly.client_generated.api.table_management import get_table
    from antfly.client_generated.models.table_status import TableStatus
    from antfly.client_generated.types import UNSET

    representation = {
        "field": "title._keyword",
        "type": "keyword",
        "query_modes": ["exact"],
        "sortable": True,
        "provenance": "document_schema",
        "missing_null_policy": "missing_rejected",
        "sort_lifecycle_state": "declared",
        "doc_value_coverage": "schema_declared",
        "queryability_state": "declared",
        "analyzer": "keyword",
    }
    capabilities = [
        {
            "field": "title",
            "kind": "source",
            "value_operations": ["equals", "contains", "matches"],
            "sortable": True,
            "sort_lifecycle_state": "declared",
            "sort_requires_scoring": False,
            "representations": [representation],
        },
        {
            "field": "_score",
            "kind": "computed",
            "value_operations": [],
            "sortable": True,
            "sort_lifecycle_state": "queryable",
            "sort_requires_scoring": True,
            "representations": [],
        },
        {
            "kind": "unassociated",
            "value_operations": [],
            "sortable": False,
            "sort_lifecycle_state": "unsupported",
            "sort_requires_scoring": False,
            "representations": [{**representation, "field": "unknown._keyword", "provenance": "observed_dynamic"}],
        },
    ]
    result = get_table._parse_response(
        client=Client(base_url="http://unused", raise_on_unexpected_status=True),
        response=httpx.Response(
            200,
            json={
                "name": "docs",
                "indexes": {},
                "shards": {},
                "storage_status": {},
                "field_capabilities": capabilities,
            },
        ),
    )
    assert isinstance(result, TableStatus)
    assert isinstance(result.field_capabilities, list)
    source, score, unknown = result.field_capabilities
    assert source.field == "title"
    assert source.representations[0].field == "title._keyword"
    assert score.sort_requires_scoring is True
    assert unknown.field is UNSET
    assert unknown.representations[0].field == "unknown._keyword"
    assert result.to_dict()["field_capabilities"] == capabilities
