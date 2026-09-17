"""Generated catalog clients preserve errors and committed mutation outcomes."""

import httpx
import pytest

from antfly.client_generated.api.table_management import (
    clear_database_tablespace,
    clear_namespace_tablespace,
    create_database,
    create_namespace,
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
