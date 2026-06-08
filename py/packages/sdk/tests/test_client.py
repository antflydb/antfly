"""Tests for Antfly client."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from httpx import Timeout

from antfly import AntflyClient, AntflyException  # noqa: E402
from antfly.client import normalize_base_url  # noqa: E402
from antfly.client_generated.models.rows_aggregate_having import RowsAggregateHaving  # noqa: E402
from antfly.client_generated.models.rows_aggregate_having_predicate import RowsAggregateHavingPredicate  # noqa: E402
from antfly.client_generated.models.rows_aggregate_having_predicate_op import (  # noqa: E402
    RowsAggregateHavingPredicateOp,
)
from antfly.client_generated.models.rows_aggregate_plan_request import RowsAggregatePlanRequest  # noqa: E402
from antfly.client_generated.models.rows_aggregate_request import RowsAggregateRequest  # noqa: E402
from antfly.client_generated.models.rows_aggregate_spec import RowsAggregateSpec  # noqa: E402
from antfly.client_generated.models.rows_array_length_projection import RowsArrayLengthProjection  # noqa: E402
from antfly.client_generated.models.rows_array_update_transform import RowsArrayUpdateTransform  # noqa: E402
from antfly.client_generated.models.rows_array_update_transform_op import RowsArrayUpdateTransformOp  # noqa: E402
from antfly.client_generated.models.row_operation import RowOperation  # noqa: E402
from antfly.client_generated.models.row_operation_op import RowOperationOp  # noqa: E402
from antfly.client_generated.models.rows_batch_request import RowsBatchRequest  # noqa: E402
from antfly.client_generated.models.rows_coalesce_operand import RowsCoalesceOperand  # noqa: E402
from antfly.client_generated.models.rows_coalesce_projection import RowsCoalesceProjection  # noqa: E402
from antfly.client_generated.models.rows_conflict_target import RowsConflictTarget  # noqa: E402
from antfly.client_generated.models.rows_conflict_unique_target import RowsConflictUniqueTarget  # noqa: E402
from antfly.client_generated.models.rows_cte import RowsCte  # noqa: E402
from antfly.client_generated.models.rows_expression import RowsExpression  # noqa: E402
from antfly.client_generated.models.rows_expression_assignment_map import RowsExpressionAssignmentMap  # noqa: E402
from antfly.client_generated.models.rows_expression_case_branch import RowsExpressionCaseBranch  # noqa: E402
from antfly.client_generated.models.rows_expression_condition import RowsExpressionCondition  # noqa: E402
from antfly.client_generated.models.rows_expression_condition_op import RowsExpressionConditionOp  # noqa: E402
from antfly.client_generated.models.rows_expression_op import RowsExpressionOp  # noqa: E402
from antfly.client_generated.models.rows_expression_projection import RowsExpressionProjection  # noqa: E402
from antfly.client_generated.models.rows_expression_source import RowsExpressionSource  # noqa: E402
from antfly.client_generated.models.rows_field_alias_projection import RowsFieldAliasProjection  # noqa: E402
from antfly.client_generated.models.rows_field_patch import RowsFieldPatch  # noqa: E402
from antfly.client_generated.models.rows_join_on import RowsJoinOn  # noqa: E402
from antfly.client_generated.models.rows_join_plan_request import RowsJoinPlanRequest  # noqa: E402
from antfly.client_generated.models.rows_join_projection import RowsJoinProjection  # noqa: E402
from antfly.client_generated.models.rows_join_projection_side import RowsJoinProjectionSide  # noqa: E402
from antfly.client_generated.models.rows_join_request import RowsJoinRequest  # noqa: E402
from antfly.client_generated.models.rows_join_request_join_type import RowsJoinRequestJoinType  # noqa: E402
from antfly.client_generated.models.rows_json_extract_projection import RowsJsonExtractProjection  # noqa: E402
from antfly.client_generated.models.rows_json_set_transform import RowsJsonSetTransform  # noqa: E402
from antfly.client_generated.models.rows_lateral_correlation import RowsLateralCorrelation  # noqa: E402
from antfly.client_generated.models.rows_lateral_plan_request import RowsLateralPlanRequest  # noqa: E402
from antfly.client_generated.models.rows_lateral_request import RowsLateralRequest  # noqa: E402
from antfly.client_generated.models.rows_mutation_source_request import RowsMutationSourceRequest  # noqa: E402
from antfly.client_generated.models.rows_mutation_source_request_op import RowsMutationSourceRequestOp  # noqa: E402
from antfly.client_generated.models.rows_mutation_source_result_set import RowsMutationSourceResultSet  # noqa: E402
from antfly.client_generated.models.rows_mutation_source_result_set_returning_item import (  # noqa: E402
    RowsMutationSourceResultSetReturningItem,
)
from antfly.client_generated.models.rows_query_order import RowsQueryOrder  # noqa: E402
from antfly.client_generated.models.rows_query_order_direction import RowsQueryOrderDirection  # noqa: E402
from antfly.client_generated.models.rows_query_request import RowsQueryRequest  # noqa: E402
from antfly.client_generated.models.rows_query_request_where import RowsQueryRequestWhere  # noqa: E402
from antfly.client_generated.models.rows_row_claim import RowsRowClaim  # noqa: E402
from antfly.client_generated.models.rows_row_claim_mode import RowsRowClaimMode  # noqa: E402
from antfly.client_generated.models.rows_row_document import RowsRowDocument  # noqa: E402
from antfly.client_generated.models.rows_numeric_increment import RowsNumericIncrement  # noqa: E402
from antfly.client_generated.models.rows_on_conflict import RowsOnConflict  # noqa: E402
from antfly.client_generated.models.rows_on_conflict_action import RowsOnConflictAction  # noqa: E402
from antfly.client_generated.models.rows_unique_predicate import RowsUniquePredicate  # noqa: E402
from antfly.client_generated.models.rows_unique_predicate_group import RowsUniquePredicateGroup  # noqa: E402
from antfly.client_generated.models.rows_unique_predicate_op import RowsUniquePredicateOp  # noqa: E402


class TestAntflyClient:
    """Test cases for AntflyClient."""

    @patch("antfly.client.Client")
    def test_client_initialization(self, mock_client: MagicMock) -> None:
        """Test client initialization with and without auth."""
        # Without auth
        client = AntflyClient(base_url="http://localhost:8080")
        assert client.base_url == "http://localhost:8080"
        mock_client.assert_called_once_with(
            base_url="http://localhost:8080",
            timeout=Timeout(30.0),
            httpx_args={},
        )

        # With auth
        mock_client.reset_mock()
        client = AntflyClient(base_url="http://localhost:8080/", username="admin", password="password")
        assert client.base_url == "http://localhost:8080"
        mock_client.assert_called_once_with(
            base_url="http://localhost:8080",
            timeout=Timeout(30.0),
            httpx_args={"auth": ("admin", "password")},
        )

    def test_normalize_base_url(self) -> None:
        assert normalize_base_url("http://localhost:8080") == "http://localhost:8080"
        assert normalize_base_url("http://localhost:8080/") == "http://localhost:8080"
        assert normalize_base_url("http://localhost:8080/db/v1") == "http://localhost:8080"
        assert normalize_base_url("http://localhost:8080/auth/v1") == "http://localhost:8080"
        assert normalize_base_url("http://localhost:8080/ai/v1") == "http://localhost:8080"
        assert (
            normalize_base_url("https://platform.antfly.io/cloud/v1/instance")
            == "https://platform.antfly.io/cloud/v1/instance"
        )
        assert (
            normalize_base_url("https://platform.antfly.io/cloud/v1/instance/db/v1")
            == "https://platform.antfly.io/cloud/v1/instance"
        )

    def test_rows_plan_request_models_serialize_join_and_lateral(self) -> None:
        join_plan = RowsJoinPlanRequest(
            ctes=[
                RowsCte(
                    name="open_orders",
                    query=RowsQueryRequest(
                        where=RowsQueryRequestWhere.from_dict({"field": "status", "op": "eq", "value": "open"})
                    ),
                )
            ],
            join=RowsJoinRequest(
                left=RowsQueryRequest(source_cte="open_orders", select=["tenant_id", "order_id"]),
                right=RowsQueryRequest(
                    where=RowsQueryRequestWhere.from_dict({"field": "kind", "op": "eq", "value": "customer"}),
                    select=["tenant_id", "customer_id"],
                ),
                on=[RowsJoinOn(left_field="tenant_id", right_field="tenant_id")],
                join_type=RowsJoinRequestJoinType.LEFT,
                select=[
                    RowsJoinProjection(
                        as_="order_id",
                        side=RowsJoinProjectionSide.LEFT,
                        field="order_id",
                    ),
                    RowsJoinProjection(
                        as_="customer_id",
                        side=RowsJoinProjectionSide.RIGHT,
                        field="customer_id",
                    ),
                ],
                order_by=[RowsQueryOrder(field="order_id", direction=RowsQueryOrderDirection.ASC)],
                limit=25,
            ),
        )

        join_dict = join_plan.to_dict()
        assert join_dict["ctes"][0]["name"] == "open_orders"
        assert join_dict["join"]["left"]["source_cte"] == "open_orders"
        assert join_dict["join"]["join_type"] == "left"
        assert join_dict["join"]["select"][1]["side"] == "right"

        lateral_plan = RowsLateralPlanRequest(
            ctes=[RowsCte(name="tenants", query=RowsQueryRequest(select=["tenant_id"]))],
            lateral=RowsLateralRequest(
                left=RowsQueryRequest(source_cte="tenants", select=["tenant_id"]),
                right=RowsQueryRequest(
                    where=RowsQueryRequestWhere.from_dict({"field": "kind", "op": "eq", "value": "event"}),
                    limit=3,
                ),
                correlations=[RowsLateralCorrelation(left_field="tenant_id", right_field="tenant_id")],
                select=[
                    RowsJoinProjection(
                        as_="tenant_id",
                        side=RowsJoinProjectionSide.LEFT,
                        field="tenant_id",
                    ),
                    RowsJoinProjection(
                        as_="event_id",
                        side=RowsJoinProjectionSide.RIGHT,
                        field="id",
                    ),
                ],
            ),
        )

        lateral_dict = lateral_plan.to_dict()
        assert lateral_dict["lateral"]["right"]["limit"] == 3
        assert lateral_dict["lateral"]["correlations"][0]["right_field"] == "tenant_id"

    def test_rows_mutation_source_models_serialize_claims_and_returning(self) -> None:
        case_expr = RowsExpression(
            op=RowsExpressionOp.CASE,
            cases=[
                RowsExpressionCaseBranch(
                    when=RowsExpressionCondition(
                        lhs=RowsExpression(field="status"),
                        op=RowsExpressionConditionOp.EQ,
                        rhs=RowsExpression(value="ready"),
                    ),
                    then=RowsExpression(value="claimed:ready"),
                )
            ],
            else_=RowsExpression(field="status"),
        )
        request = RowsMutationSourceRequest(
            op=RowsMutationSourceRequestOp.UPDATE,
            source=RowsQueryRequest(
                where=RowsQueryRequestWhere.from_dict({"field": "status", "op": "eq", "value": "ready"}),
                row_claim=RowsRowClaim(
                    mode=RowsRowClaimMode.FOR_UPDATE,
                    owner_id="worker:1",
                    transaction_id="00112233445566778899aabbccddeeff",
                    skip_locked=True,
                    lease_ms=45000,
                ),
                order_by=[RowsQueryOrder(field="created_at", direction=RowsQueryOrderDirection.ASC)],
                limit=5,
            ),
            patch_expr=RowsExpressionAssignmentMap.from_dict({"status": case_expr.to_dict()}),
            returning=["id", "status"],
            returning_expressions=[
                RowsExpressionProjection.from_dict({"as": "status_label", "expr": {"field": "status"}})
            ],
        )

        request_dict = request.to_dict()
        assert request_dict["op"] == "update"
        assert request_dict["source"]["row_claim"]["skip_locked"] is True
        assert request_dict["source"]["row_claim"]["lease_ms"] == 45000
        assert request_dict["patch_expr"]["status"]["op"] == "case"
        assert request_dict["patch_expr"]["status"]["cases"][0]["when"]["lhs"]["field"] == "status"
        assert request_dict["returning_expressions"][0]["as"] == "status_label"

        result = RowsMutationSourceResultSet(
            matched=2,
            staged=1,
            returning=[RowsMutationSourceResultSetReturningItem.from_dict({"id": "r1", "status": "claimed:ready"})],
        )

        result_dict = result.to_dict()
        assert result_dict["matched"] == 2
        assert result_dict["staged"] == 1
        assert result_dict["returning"][0]["status"] == "claimed:ready"

    def test_rows_batch_models_serialize_typed_conflict_and_transforms(self) -> None:
        patch = RowsFieldPatch.from_dict({"status": "active"})
        increment = RowsNumericIncrement.from_dict({"amount": 2.5})
        patch_expr = RowsExpressionAssignmentMap.from_dict(
            {"status_key": {"op": "lower", "args": [{"field": "status", "source": "proposed"}]}}
        )
        json_set = RowsJsonSetTransform(field="metadata", path=["billing", "plan"], value="pro")
        array_update = RowsArrayUpdateTransform(
            field="tags",
            op=RowsArrayUpdateTransformOp.ADD_TO_SET,
            value="paid",
        )
        partial_target = RowsUniquePredicateGroup(
            all_=[RowsUniquePredicate(field="status", op=RowsUniquePredicateOp.EQ, value="active")]
        )
        on_conflict = RowsOnConflict(
            target=RowsConflictTarget(
                unique=RowsConflictUniqueTarget(
                    name="usage_records_active_email_key",
                    where=partial_target,
                )
            ),
            action=RowsOnConflictAction.UPDATE,
            patch=patch,
            increment=increment,
            patch_expr=patch_expr,
            json_set=[json_set],
            array_update=[array_update],
            where_expression=RowsExpressionCondition(
                lhs=RowsExpression(field="status", source=RowsExpressionSource.PROPOSED),
                op=RowsExpressionConditionOp.IS_NOT_NULL,
            ),
        )
        batch = RowsBatchRequest(
            operations=[
                RowOperation(
                    op=RowOperationOp.INSERT,
                    row=RowsRowDocument.from_dict({"id": "u2", "email": "ada@example.test", "status": "active"}),
                    on_conflict=on_conflict,
                    returning=["id", "status"],
                    returning_expressions=[
                        RowsExpressionProjection.from_dict({"as": "status_label", "expr": {"field": "status"}})
                    ],
                )
            ]
        )

        batch_dict = batch.to_dict()
        conflict = batch_dict["operations"][0]["on_conflict"]
        assert conflict["target"]["unique"]["where"]["all"][0]["field"] == "status"
        assert conflict["patch"]["status"] == "active"
        assert conflict["increment"]["amount"] == 2.5
        assert conflict["json_set"][0]["path"] == ["billing", "plan"]
        assert conflict["array_update"][0]["op"] == "add_to_set"
        assert batch_dict["operations"][0]["returning_expressions"][0]["as"] == "status_label"

    def test_rows_aggregate_plan_models_serialize_typed_having(self) -> None:
        plan = RowsAggregatePlanRequest(
            aggregate=RowsAggregateRequest(
                source=RowsQueryRequest(
                    where=RowsQueryRequestWhere.from_dict({"field": "status", "op": "eq", "value": "open"})
                ),
                group_by=["customer_id"],
                aggregations=[RowsAggregateSpec(name="amount_sum", op="sum", field="amount")],
                having=RowsAggregateHaving(
                    all_=[
                        RowsAggregateHavingPredicate(
                            field="amount_sum",
                            op=RowsAggregateHavingPredicateOp.GT,
                            value=100,
                        )
                    ]
                ),
                order_by=[RowsQueryOrder(field="amount_sum", direction=RowsQueryOrderDirection.DESC)],
                limit=10,
            ),
        )

        plan_dict = plan.to_dict()
        assert plan_dict["aggregate"]["having"]["all"][0] == {
            "field": "amount_sum",
            "op": "gt",
            "value": 100,
        }

    def test_rows_query_models_serialize_typed_compact_projections(self) -> None:
        request = RowsQueryRequest(
            select=["id"],
            json_extract=[
                RowsJsonExtractProjection(
                    as_="plan",
                    field="metadata",
                    path=["billing", "plan"],
                    as_text=True,
                )
            ],
            array_length=[RowsArrayLengthProjection(as_="tag_count", field="tags")],
            coalesce=[
                RowsCoalesceProjection(
                    as_="name_or_email",
                    operands=[
                        RowsCoalesceOperand(field="display_name"),
                        RowsCoalesceOperand(field="email"),
                        RowsCoalesceOperand(value="unknown"),
                    ],
                )
            ],
            field_aliases=[RowsFieldAliasProjection(as_="raw_id", field="id")],
        )

        request_dict = request.to_dict()
        assert request_dict["json_extract"][0]["path"] == ["billing", "plan"]
        assert request_dict["json_extract"][0]["as_text"] is True
        assert request_dict["array_length"][0] == {"as": "tag_count", "field": "tags"}
        assert request_dict["coalesce"][0]["operands"][2] == {"value": "unknown"}
        assert request_dict["field_aliases"][0] == {"as": "raw_id", "field": "id"}

    @patch("antfly.client.AuthenticatedClient")
    def test_token_auth(self, mock_client: MagicMock) -> None:
        AntflyClient(base_url="https://platform.antfly.io/cloud/v1/instance", token="antflydb_test")
        mock_client.assert_called_once_with(
            base_url="https://platform.antfly.io/cloud/v1/instance",
            token="antflydb_test",
            prefix="Bearer",
            timeout=Timeout(30.0),
            httpx_args={},
        )

    @patch("antfly.client.Client")
    def test_list_tables(self, mock_client_class: MagicMock) -> None:
        """Test listing tables."""
        client = AntflyClient(base_url="http://localhost:8080")

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        # Re-create client so it picks up the mock
        client = AntflyClient(base_url="http://localhost:8080")
        tables = client.list_tables()

        assert tables == []
        mock_httpx.request.assert_called_once_with("GET", "/db/v1/tables")

    @patch("antfly.client.Client")
    def test_create_table(self, mock_client_class: MagicMock) -> None:
        """Test creating a table."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"name": "test_table", "shards": {}, "indexes": {}}

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        result = client.create_table(name="test_table", num_shards=2)

        assert result["name"] == "test_table"
        mock_httpx.request.assert_called_once_with("POST", "/db/v1/tables/test_table", json={"num_shards": 2})

    @patch("antfly.client.Client")
    def test_create_table_failure(self, mock_client_class: MagicMock) -> None:
        """Test handling of create table failure."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "bad request"
        mock_response.json.return_value = {"error": "table already exists"}

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException) as exc_info:
            client.create_table(name="test_table")

        assert "table already exists" in str(exc_info.value)

    @patch("antfly.client.Client")
    @patch("antfly.client.lookup_key")
    def test_get_record(self, mock_lookup_key: MagicMock, mock_client_class: MagicMock) -> None:
        """Test getting a record by key."""
        mock_response = Mock()
        mock_response.to_dict.return_value = {"name": "John Doe"}
        mock_lookup_key.sync.return_value = mock_response

        client = AntflyClient(base_url="http://localhost:8080")
        record = client.get(table="users", key="user:1")

        assert record == {"name": "John Doe"}
        mock_lookup_key.sync.assert_called_once_with(table_name="users", key="user:1", client=client._client)

    @patch("antfly.client.Client")
    @patch("antfly.client.lookup_key")
    def test_get_record_failure(self, mock_lookup_key: MagicMock, mock_client_class: MagicMock) -> None:
        """Test handling of get record failure."""
        mock_lookup_key.sync.return_value = None

        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException) as exc_info:
            client.get(table="users", key="user:1")

        assert "Failed to get key 'user:1' from table 'users'" in str(exc_info.value)
