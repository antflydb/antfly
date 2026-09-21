"""Public relational storage-mode request and response contracts."""

import httpx
import pytest

from antfly.client_generated.models.create_table_request import CreateTableRequest
from antfly.client_generated.models.foreign_key_action import ForeignKeyAction
from antfly.client_generated.models.relational_check_constraint import RelationalCheckConstraint
from antfly.client_generated.models.relational_comparison_op import RelationalComparisonOp
from antfly.client_generated.models.relational_foreign_key_constraint import RelationalForeignKeyConstraint
from antfly.client_generated.models.relational_index_definition import RelationalIndexDefinition
from antfly.client_generated.models.relational_index_key import RelationalIndexKey
from antfly.client_generated.models.relational_index_key_direction import RelationalIndexKeyDirection
from antfly.client_generated.models.relational_index_key_nulls import RelationalIndexKeyNulls
from antfly.client_generated.models.relational_row import RelationalRow
from antfly.client_generated.models.relational_row_mutation_request import RelationalRowMutationRequest
from antfly.client_generated.models.relational_row_query_request import RelationalRowQueryRequest
from antfly.client_generated.models.relational_unique_constraint import RelationalUniqueConstraint
from antfly.client_generated.models.table_schema import TableSchema
from antfly.client_generated.models.table_status import TableStatus
from antfly.client_generated.models.table_storage_mode import TableStorageMode


def test_zero_schema_epoch_is_distinct_from_omitted_query_epoch() -> None:
    assert "schema_version" not in RelationalRowQueryRequest(fields=["id"]).to_dict()
    query = RelationalRowQueryRequest(fields=["id"], index="by_id", schema_version=0)
    assert query.to_dict()["schema_version"] == 0
    assert RelationalRowQueryRequest.from_dict(query.to_dict()).schema_version == 0
    mutation = RelationalRowMutationRequest(schema_version=0, mutations=[])
    assert mutation.to_dict()["schema_version"] == 0
    assert RelationalRowMutationRequest.from_dict(mutation.to_dict()).schema_version == 0


@pytest.mark.parametrize("mode", list(TableStorageMode))
def test_storage_mode_is_typed_and_survives_round_trips(mode: TableStorageMode) -> None:
    schema = TableSchema(storage_mode=mode)
    request = CreateTableRequest(schema=schema)
    encoded = request.to_dict()
    assert encoded["schema"]["storage_mode"] == mode.value
    decoded = CreateTableRequest.from_dict(encoded)
    assert isinstance(decoded.schema, TableSchema)
    assert decoded.schema.storage_mode is mode
    status = TableStatus.from_dict(
        {"name": "rows", "schema": schema.to_dict(), "indexes": {}, "shards": {}, "storage_status": {}}
    )
    assert isinstance(status.schema, TableSchema)
    assert status.schema.storage_mode is mode
    assert status.to_dict()["schema"]["storage_mode"] == mode.value


def test_check_constraint_exact_integer_and_null_round_trip() -> None:
    checks = [
        RelationalCheckConstraint(name="positive", column="id", op=RelationalComparisonOp.GT, value="9007199254740992"),
        RelationalCheckConstraint(name="present", column="name", op=RelationalComparisonOp.IS_NOT_NULL),
    ]
    encoded = TableSchema(storage_mode=TableStorageMode.RELATIONAL, checks=checks).to_dict()
    assert encoded["checks"][0]["value"] == "9007199254740992"
    assert "value" not in encoded["checks"][1]
    assert TableSchema.from_dict(encoded).to_dict() == encoded


def test_storage_mode_omission_preserves_the_document_default() -> None:
    assert "storage_mode" not in TableSchema().to_dict()
    assert "storage_mode" not in TableSchema.from_dict({}).to_dict()


def test_composite_index_declarations_and_explicit_drop_round_trip() -> None:
    declarations = [
        RelationalIndexDefinition(
            name="tenant_id",
            keys=[
                RelationalIndexKey(column="tenant", collation="ci"),
                RelationalIndexKey(
                    column="id", direction=RelationalIndexKeyDirection.DESC, nulls=RelationalIndexKeyNulls.LAST
                ),
            ],
        )
    ]
    for indexes in (declarations, []):
        encoded = TableSchema(storage_mode=TableStorageMode.RELATIONAL, relational_indexes=indexes).to_dict()
        assert "relational_indexes" in encoded
        decoded = TableSchema.from_dict(encoded)
        assert decoded.to_dict() == encoded
        assert isinstance(decoded.relational_indexes, list)
        assert len(decoded.relational_indexes) == len(indexes)
    assert "relational_indexes" not in TableSchema().to_dict()


def test_generated_composite_foreign_key_and_unique_contracts() -> None:
    schema = TableSchema(
        storage_mode=TableStorageMode.RELATIONAL,
        unique_constraints=[RelationalUniqueConstraint(name="tenant_id", columns=["tenant", "id"])],
        foreign_keys=[
            RelationalForeignKeyConstraint(
                name="parent",
                child_columns=["tenant", "parent_id"],
                parent_table="parents",
                parent_columns=["tenant", "id"],
                on_delete=ForeignKeyAction.CASCADE,
            )
        ],
    )
    assert TableSchema.from_dict(schema.to_dict()).to_dict() == schema.to_dict()
    assert schema.to_dict()["foreign_keys"][0]["on_delete"] == "cascade"


def test_generated_typed_rows_preserve_integer_and_version_precision() -> None:
    row = {"_id": "a", "row": {"id": 9223372036854775807}, "version": "18446744073709551615", "schema_version": 7}
    assert RelationalRow.from_dict(row).to_dict() == row
    mutation = {
        "schema_version": 7,
        "mutations": [
            {"key": "a", "expected_version": row["version"], "row": row["row"]},
            {"key": "b", "expected_version": "1"},
        ],
    }
    assert RelationalRowMutationRequest.from_dict(mutation).to_dict() == mutation


def test_generated_row_query_returns_ndjson_text_without_number_coercion() -> None:
    from antfly.client_generated.api.data_operations.query_relational_rows import _parse_response
    from antfly.client_generated.client import Client

    page = '{"_id":"a","row":{"id":9223372036854775807},"version":"18446744073709551615","schema_version":7}\n'
    assert (
        _parse_response(client=Client(base_url="http://example.invalid"), response=httpx.Response(200, text=page))
        == page
    )


def test_generated_constraint_recovery_routes_and_acceptance() -> None:
    from antfly.client_generated.api.data_operations.retire_relational_constraints import _get_kwargs, _parse_response
    from antfly.client_generated.api.data_operations.retry_relational_constraints import _get_kwargs as retry_kwargs
    from antfly.client_generated.client import Client
    from antfly.client_generated.models.relational_constraint_retirement_request import (
        RelationalConstraintRetirementRequest,
    )
    from antfly.client_generated.models.relational_constraint_retry_request import RelationalConstraintRetryRequest

    request = RelationalConstraintRetirementRequest(schema_version=7, drop=True)
    encoded = _get_kwargs("parent table", body=request)
    assert encoded["url"] == "/db/v1/tables/parent%20table/constraints/retire"
    assert encoded["json"] == {"schema_version": 7, "drop": True}
    assert retry_kwargs("parents", body=RelationalConstraintRetryRequest(schema_version=7))["json"] == {
        "schema_version": 7
    }
    accepted = _parse_response(
        client=Client(base_url="http://example.invalid"), response=httpx.Response(202, json={"status": "accepted"})
    )
    assert accepted is not None
    assert accepted.to_dict() == {"status": "accepted"}
