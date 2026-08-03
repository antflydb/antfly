"""Tests for Antfly client."""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest
from httpx import Timeout

from antfly import AntflyClient, AntflyException  # noqa: E402
from antfly.client import normalize_base_url  # noqa: E402
from antfly.client_generated.models.graph_index_stats import GraphIndexStats  # noqa: E402
from antfly.client_generated.models.graph_index_stats_index_type import GraphIndexStatsIndexType  # noqa: E402
from antfly.client_generated.models.graph_metric_runtime_stats import GraphMetricRuntimeStats  # noqa: E402
from antfly.client_generated.models.graph_metric_runtime_stats_role import GraphMetricRuntimeStatsRole  # noqa: E402
from antfly.client_generated.models.row_filter_entry import RowFilterEntry  # noqa: E402
from antfly.client_generated.models.row_filter_entry_filter import RowFilterEntryFilter  # noqa: E402
from antfly.client_generated.models.row_operation import RowOperation  # noqa: E402
from antfly.client_generated.models.row_operation_op import RowOperationOp  # noqa: E402
from antfly.client_generated.models.rows_aggregate_having import RowsAggregateHaving  # noqa: E402
from antfly.client_generated.models.rows_aggregate_having_predicate import RowsAggregateHavingPredicate  # noqa: E402
from antfly.client_generated.models.rows_aggregate_having_predicate_op import (  # noqa: E402
    RowsAggregateHavingPredicateOp,
)
from antfly.client_generated.models.rows_aggregate_plan_request import RowsAggregatePlanRequest  # noqa: E402
from antfly.client_generated.models.rows_aggregate_request import RowsAggregateRequest  # noqa: E402
from antfly.client_generated.models.rows_aggregate_spec import RowsAggregateSpec  # noqa: E402
from antfly.client_generated.models.rows_aggregate_spec_op import RowsAggregateSpecOp  # noqa: E402
from antfly.client_generated.models.rows_array_length_projection import RowsArrayLengthProjection  # noqa: E402
from antfly.client_generated.models.rows_array_update_transform import RowsArrayUpdateTransform  # noqa: E402
from antfly.client_generated.models.rows_array_update_transform_op import RowsArrayUpdateTransformOp  # noqa: E402
from antfly.client_generated.models.rows_batch_request import RowsBatchRequest  # noqa: E402
from antfly.client_generated.models.rows_coalesce_field_operand import RowsCoalesceFieldOperand  # noqa: E402
from antfly.client_generated.models.rows_coalesce_projection import RowsCoalesceProjection  # noqa: E402
from antfly.client_generated.models.rows_coalesce_value_operand import RowsCoalesceValueOperand  # noqa: E402
from antfly.client_generated.models.rows_conflict_unique_target import RowsConflictUniqueTarget  # noqa: E402
from antfly.client_generated.models.rows_cte import RowsCte  # noqa: E402
from antfly.client_generated.models.rows_expression_array_contains_predicate import (  # noqa: E402
    RowsExpressionArrayContainsPredicate,
)
from antfly.client_generated.models.rows_expression_assignment_map import RowsExpressionAssignmentMap  # noqa: E402
from antfly.client_generated.models.rows_expression_case_branch import RowsExpressionCaseBranch  # noqa: E402
from antfly.client_generated.models.rows_expression_condition import RowsExpressionCondition  # noqa: E402
from antfly.client_generated.models.rows_expression_condition_group import RowsExpressionConditionGroup  # noqa: E402
from antfly.client_generated.models.rows_expression_condition_op import RowsExpressionConditionOp  # noqa: E402
from antfly.client_generated.models.rows_expression_field import RowsExpressionField  # noqa: E402
from antfly.client_generated.models.rows_expression_field_source import RowsExpressionFieldSource  # noqa: E402
from antfly.client_generated.models.rows_expression_operator import RowsExpressionOperator  # noqa: E402
from antfly.client_generated.models.rows_expression_operator_op import RowsExpressionOperatorOp  # noqa: E402
from antfly.client_generated.models.rows_expression_projection import RowsExpressionProjection  # noqa: E402
from antfly.client_generated.models.rows_expression_value import RowsExpressionValue  # noqa: E402
from antfly.client_generated.models.rows_field_alias_projection import RowsFieldAliasProjection  # noqa: E402
from antfly.client_generated.models.rows_field_patch import RowsFieldPatch  # noqa: E402
from antfly.client_generated.models.rows_get_request import RowsGetRequest  # noqa: E402
from antfly.client_generated.models.rows_get_result import RowsGetResult  # noqa: E402
from antfly.client_generated.models.rows_get_result_row import RowsGetResultRow  # noqa: E402
from antfly.client_generated.models.rows_get_result_set import RowsGetResultSet  # noqa: E402
from antfly.client_generated.models.rows_insert_source_assignment import RowsInsertSourceAssignment  # noqa: E402
from antfly.client_generated.models.rows_insert_source_request import RowsInsertSourceRequest  # noqa: E402
from antfly.client_generated.models.rows_insert_source_request_op import RowsInsertSourceRequestOp  # noqa: E402
from antfly.client_generated.models.rows_join_on import RowsJoinOn  # noqa: E402
from antfly.client_generated.models.rows_join_plan_request import RowsJoinPlanRequest  # noqa: E402
from antfly.client_generated.models.rows_join_projection import RowsJoinProjection  # noqa: E402
from antfly.client_generated.models.rows_join_projection_side import RowsJoinProjectionSide  # noqa: E402
from antfly.client_generated.models.rows_join_request import RowsJoinRequest  # noqa: E402
from antfly.client_generated.models.rows_join_request_join_type import RowsJoinRequestJoinType  # noqa: E402
from antfly.client_generated.models.rows_joined_mutation_source_assignment import (  # noqa: E402
    RowsJoinedMutationSourceAssignment,
)
from antfly.client_generated.models.rows_joined_mutation_source_assignment_side import (  # noqa: E402
    RowsJoinedMutationSourceAssignmentSide,
)
from antfly.client_generated.models.rows_joined_mutation_source_request import (  # noqa: E402
    RowsJoinedMutationSourceRequest,
)
from antfly.client_generated.models.rows_joined_mutation_source_request_op import (  # noqa: E402
    RowsJoinedMutationSourceRequestOp,
)
from antfly.client_generated.models.rows_joined_mutation_source_request_target_side import (  # noqa: E402
    RowsJoinedMutationSourceRequestTargetSide,
)
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
from antfly.client_generated.models.rows_numeric_increment import RowsNumericIncrement  # noqa: E402
from antfly.client_generated.models.rows_on_conflict import RowsOnConflict  # noqa: E402
from antfly.client_generated.models.rows_on_conflict_action import RowsOnConflictAction  # noqa: E402
from antfly.client_generated.models.rows_query_order_field import RowsQueryOrderField  # noqa: E402
from antfly.client_generated.models.rows_query_order_field_direction import RowsQueryOrderFieldDirection  # noqa: E402
from antfly.client_generated.models.rows_query_request import RowsQueryRequest  # noqa: E402
from antfly.client_generated.models.rows_row_claim import RowsRowClaim  # noqa: E402
from antfly.client_generated.models.rows_row_claim_mode import RowsRowClaimMode  # noqa: E402
from antfly.client_generated.models.rows_row_document import RowsRowDocument  # noqa: E402
from antfly.client_generated.models.rows_unique_predicate import RowsUniquePredicate  # noqa: E402
from antfly.client_generated.models.rows_unique_predicate_group import RowsUniquePredicateGroup  # noqa: E402
from antfly.client_generated.models.rows_unique_predicate_op import RowsUniquePredicateOp  # noqa: E402
from antfly.client_generated.models.rows_where_atom import RowsWhereAtom  # noqa: E402
from antfly.client_generated.models.rows_where_atom_op import RowsWhereAtomOp  # noqa: E402
from antfly.client_generated.models.rows_where_branch_all import RowsWhereBranchAll  # noqa: E402
from antfly.client_generated.models.rows_where_branch_atom import RowsWhereBranchAtom  # noqa: E402
from antfly.client_generated.models.rows_where_branch_atom_op import RowsWhereBranchAtomOp  # noqa: E402
from antfly.client_generated.models.rows_where_type_0 import RowsWhereType0  # noqa: E402
from antfly.client_generated.models.rows_where_type_0_op import RowsWhereType0Op  # noqa: E402
from antfly.client_generated.models.rows_window_frame import RowsWindowFrame  # noqa: E402
from antfly.client_generated.models.rows_window_frame_end import RowsWindowFrameEnd  # noqa: E402
from antfly.client_generated.models.rows_window_frame_start import RowsWindowFrameStart  # noqa: E402
from antfly.client_generated.models.rows_window_frame_unit import RowsWindowFrameUnit  # noqa: E402
from antfly.client_generated.models.rows_window_spec import RowsWindowSpec  # noqa: E402
from antfly.client_generated.models.sort_profile import SortProfile  # noqa: E402
from antfly.client_generated.models.sql_statement_response import SqlStatementResponse  # noqa: E402
from antfly.client_generated.models.sql_statement_response_kind import SqlStatementResponseKind  # noqa: E402
from antfly.client_generated.models.transform_op_type import TransformOpType  # noqa: E402
from antfly.client_generated.types import UNSET, Unset  # noqa: E402

type RowsExpression = RowsExpressionField | RowsExpressionOperator | RowsExpressionValue


def where_atom(field: str, op: RowsWhereType0Op, value: object) -> RowsWhereType0:
    return RowsWhereType0(field=field, op=op, value=value, case_insensitive=UNSET, negated=UNSET)


def row_order(field: str, direction: str = "asc") -> RowsQueryOrderField:
    return RowsQueryOrderField(field=field, direction=RowsQueryOrderFieldDirection(direction))


def doc_key_range(start: str | None = None, end: str | None = None) -> dict[str, str]:
    result = {}
    if start is not None:
        result["start"] = start
    if end is not None:
        result["end"] = end
    return result


def expr_field(field: str, source: str | None = None) -> RowsExpressionField:
    return RowsExpressionField(
        field=field,
        source=UNSET if source is None else RowsExpressionFieldSource(source),
    )


def expr_value(value: object) -> RowsExpressionValue:
    return RowsExpressionValue(value=value)


def expr_op(op: RowsExpressionOperatorOp, args: list[RowsExpression]) -> RowsExpressionOperator:
    return RowsExpressionOperator(op=op, args=args)


def coalesce_field(field: str) -> RowsCoalesceFieldOperand:
    return RowsCoalesceFieldOperand(field=field)


def coalesce_value(value: object) -> RowsCoalesceValueOperand:
    return RowsCoalesceValueOperand(value=value)


class TestAntflyClient:
    """Test cases for AntflyClient."""

    def test_transform_operator_names_are_stable(self) -> None:
        assert {member.name: member.value for member in TransformOpType} == {
            "SET": "$set",
            "SET_ON_INSERT": "$setOnInsert",
            "UNSET": "$unset",
            "INC": "$inc",
            "ADD_TO_SET": "$addToSet",
            "MAX": "$max",
        }

    def test_sort_profile_uses_closed_public_diagnostic_shape(self) -> None:
        """SortProfile keeps stable fields typed and drops internal counters."""
        profile = SortProfile.from_dict(
            {
                "plan": "native_doc_values_top_n",
                "candidate_count": 7,
                "native_doc_value_load_us": 13,
                "collector_heap_peak": 5,
            }
        )

        assert profile.plan == "native_doc_values_top_n"
        assert profile.candidate_count == 7
        encoded = profile.to_dict()

        assert "native_doc_value_load_us" not in encoded
        assert "collector_heap_peak" not in encoded

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

    def test_graph_index_stats_model_serializes_metric_runtime(self) -> None:
        stats = GraphIndexStats(
            index_type=GraphIndexStatsIndexType.GRAPH,
            total_edges=4,
            graph_metric_runtime=GraphMetricRuntimeStats(
                enabled=True,
                role=GraphMetricRuntimeStatsRole.WORKER_POOL,
                owner_id_hash=17,
                worker_count=3,
                takeover_count=2,
                lost_leases=1,
                total_pages_claimed=6,
                last_pages_completed=3,
                last_budget_exhausted=True,
            ),
        )

        stats_dict = stats.to_dict()
        assert stats_dict["graph_metric_runtime"]["role"] == "worker_pool"
        assert stats_dict["graph_metric_runtime"]["owner_id_hash"] == 17
        assert stats_dict["graph_metric_runtime"]["last_budget_exhausted"] is True

        round_tripped = GraphIndexStats.from_dict(stats_dict)
        assert isinstance(round_tripped.graph_metric_runtime, GraphMetricRuntimeStats)
        assert round_tripped.graph_metric_runtime.role == GraphMetricRuntimeStatsRole.WORKER_POOL
        assert round_tripped.graph_metric_runtime.worker_count == 3
        assert round_tripped.graph_metric_runtime.total_pages_claimed == 6

    def test_row_filter_entry_model_serializes_policy_filter(self) -> None:
        entry = RowFilterEntry(
            table="usage_records",
            filter_=RowFilterEntryFilter.from_dict({"term": {"tenant_id": "t1"}}),
        )

        entry_dict = entry.to_dict()
        assert entry_dict == {
            "table": "usage_records",
            "filter": {"term": {"tenant_id": "t1"}},
        }

        round_tripped = RowFilterEntry.from_dict(entry_dict)
        assert round_tripped.table == "usage_records"
        assert round_tripped.filter_["term"] == {"tenant_id": "t1"}

    def test_rows_get_models_serialize_structured_identity_results(self) -> None:
        request = RowsGetRequest(
            keys=[{"primary": {"tenant_id": "t1", "user_id": "u1"}}],
            include_physical_key=True,
        )
        assert request.to_dict() == {
            "keys": [{"primary": {"tenant_id": "t1", "user_id": "u1"}}],
            "include_physical_key": True,
        }

        row = RowsGetResultRow.from_dict({"tenant_id": "t1", "user_id": "u1", "status": "ready"})
        result = RowsGetResult(
            identity={"primary": {"tenant_id": "t1", "user_id": "u1"}},
            found=True,
            row=row,
            version=7,
            physical_key="row:t1:u1",
        )
        result_set = RowsGetResultSet(rows=[result])

        result_dict = result_set.to_dict()
        assert result_dict["rows"][0]["found"] is True
        assert result_dict["rows"][0]["row"]["status"] == "ready"
        assert result_dict["rows"][0]["physical_key"] == "row:t1:u1"

        round_tripped = RowsGetResultSet.from_dict(result_dict)
        assert not isinstance(round_tripped.rows, Unset)
        round_tripped_row = round_tripped.rows[0]
        assert not isinstance(round_tripped_row.row, Unset)
        assert round_tripped_row.row["status"] == "ready"
        assert round_tripped_row.identity == {"primary": {"tenant_id": "t1", "user_id": "u1"}}

    def test_rows_plan_request_models_serialize_join_and_lateral(self) -> None:
        join_plan = RowsJoinPlanRequest(
            ctes=[
                RowsCte(
                    name="open_orders",
                    query=RowsQueryRequest(where=where_atom("status", RowsWhereType0Op.EQ, "open")),
                    max_rows=100,
                    max_bytes=4096,
                )
            ],
            join=RowsJoinRequest(
                left=RowsQueryRequest(source_cte="open_orders", select=["tenant_id", "order_id"]),
                right=RowsQueryRequest(
                    where=where_atom("kind", RowsWhereType0Op.EQ, "customer"),
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
                order_by=[row_order("order_id")],
                limit=25,
            ),
        )

        join_dict = join_plan.to_dict()
        assert join_dict["ctes"][0]["name"] == "open_orders"
        assert join_dict["ctes"][0]["max_rows"] == 100
        assert join_dict["ctes"][0]["max_bytes"] == 4096
        assert join_dict["join"]["left"]["source_cte"] == "open_orders"
        assert join_dict["join"]["join_type"] == "left"
        assert join_dict["join"]["select"][1]["side"] == "right"

        lateral_plan = RowsLateralPlanRequest(
            ctes=[RowsCte(name="tenants", query=RowsQueryRequest(select=["tenant_id"]))],
            lateral=RowsLateralRequest(
                left=RowsQueryRequest(source_cte="tenants", select=["tenant_id"]),
                right=RowsQueryRequest(
                    where=where_atom("kind", RowsWhereType0Op.EQ, "event"),
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
        case_expr = {
            "op": "case",
            "cases": [
                RowsExpressionCaseBranch(
                    when=RowsExpressionCondition(
                        lhs=expr_field("status"),
                        op=RowsExpressionConditionOp.EQ,
                        rhs=expr_value("ready"),
                    ),
                    then=expr_value("claimed:ready"),
                ).to_dict()
            ],
            "else": expr_field("status").to_dict(),
        }
        request = RowsMutationSourceRequest(
            op=RowsMutationSourceRequestOp.UPDATE,
            source=RowsQueryRequest(
                where=where_atom("status", RowsWhereType0Op.EQ, "ready"),
                row_claim=RowsRowClaim(
                    mode=RowsRowClaimMode.FOR_UPDATE,
                    owner_id="worker:1",
                    transaction_id="00112233445566778899aabbccddeeff",
                    skip_locked=True,
                    lease_ms=45000,
                ),
                order_by=[row_order("created_at")],
                limit=5,
            ),
            patch_expr=RowsExpressionAssignmentMap.from_dict({"status": case_expr}),
            json_set=[
                RowsJsonSetTransform(
                    field="metadata",
                    path=["claim", "status_key"],
                    expr=expr_op(RowsExpressionOperatorOp.LOWER, [expr_field("status", "existing")]),
                )
            ],
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
        assert request_dict["json_set"][0]["path"] == ["claim", "status_key"]
        assert request_dict["json_set"][0]["expr"]["args"][0]["source"] == "existing"
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

    def test_rows_joined_mutation_source_model_serializes_typed_contract(self) -> None:
        request = RowsJoinedMutationSourceRequest(
            op=RowsJoinedMutationSourceRequestOp.UPDATE,
            source_table="source_records",
            target_side=RowsJoinedMutationSourceRequestTargetSide.LEFT,
            join=RowsJoinRequest(
                left=RowsQueryRequest(
                    where=where_atom("status", RowsWhereType0Op.EQ, "ready"),
                    row_claim=RowsRowClaim(
                        mode=RowsRowClaimMode.FOR_UPDATE,
                        owner_id="worker:joined",
                        transaction_id="00112233445566778899aabbccddeeff",
                        skip_locked=True,
                    ),
                ),
                right=RowsQueryRequest(where=where_atom("source_status", RowsWhereType0Op.EQ, "source")),
                on=[RowsJoinOn(left_field="source_id", right_field="source_pk")],
                order_by=[row_order("amount", "desc")],
                limit=5,
            ),
            source_assignments=[
                RowsJoinedMutationSourceAssignment(
                    target_field="quantity",
                    side=RowsJoinedMutationSourceAssignmentSide.RIGHT,
                    field="source_quantity",
                )
            ],
            patch=RowsFieldPatch.from_dict({"status": "synced"}),
            patch_expr=RowsExpressionAssignmentMap.from_dict(
                {"status_key": {"op": "lower", "args": [{"field": "status"}]}}
            ),
            returning=["id", "quantity"],
            returning_expressions=[
                RowsExpressionProjection.from_dict({"as": "status_key", "expr": {"field": "status_key"}})
            ],
        )

        request_dict = request.to_dict()
        assert request_dict["source_table"] == "source_records"
        assert request_dict["target_side"] == "left"
        assert request_dict["join"]["left"]["row_claim"]["skip_locked"] is True
        assert request_dict["join"]["on"][0]["right_field"] == "source_pk"
        assert request_dict["source_assignments"][0] == {
            "target_field": "quantity",
            "side": "right",
            "field": "source_quantity",
        }
        assert request_dict["patch_expr"]["status_key"]["op"] == "lower"
        assert request_dict["returning_expressions"][0]["as"] == "status_key"

    def test_rows_insert_source_model_serializes_typed_contract(self) -> None:
        request = RowsInsertSourceRequest(
            op=RowsInsertSourceRequestOp.INSERT,
            source_table="archived_records",
            source=RowsQueryRequest(
                where=where_atom("status", RowsWhereType0Op.EQ, "ready"),
                order_by=[row_order("amount", "desc")],
                limit=5,
            ),
            assignments=[
                RowsInsertSourceAssignment(target_field="id", expr=expr_field("source_id")),
                RowsInsertSourceAssignment(
                    target_field="status",
                    expr=expr_op(RowsExpressionOperatorOp.LOWER, [expr_field("status")]),
                ),
                RowsInsertSourceAssignment(
                    target_field="amount",
                    expr=expr_op(RowsExpressionOperatorOp.ADD, [expr_field("amount"), expr_value(1)]),
                ),
            ],
            on_conflict=RowsOnConflict(target={"primary": True}, action=RowsOnConflictAction.NOTHING),
            returning=["id", "status"],
            returning_expressions=[
                RowsExpressionProjection.from_dict(
                    {"as": "amount_plus_one", "expr": {"op": "add", "args": [{"field": "amount"}, {"value": 1}]}}
                )
            ],
        )

        request_dict = request.to_dict()
        assert request_dict["op"] == "insert"
        assert request_dict["source_table"] == "archived_records"
        assert request_dict["source"]["where"]["field"] == "status"
        assert request_dict["assignments"][0] == {"target_field": "id", "expr": {"field": "source_id"}}
        assert request_dict["assignments"][1]["expr"]["op"] == "lower"
        assert request_dict["assignments"][2]["expr"]["args"][1]["value"] == 1
        assert request_dict["on_conflict"] == {"target": {"primary": True}, "action": "nothing"}
        assert request_dict["returning_expressions"][0]["as"] == "amount_plus_one"

        round_tripped = RowsInsertSourceRequest.from_dict(request_dict)
        assert round_tripped.source_table == "archived_records"
        assert round_tripped.assignments[1].target_field == "status"

    def test_rows_expression_condition_models_serialize_ast_dicts(self) -> None:
        json_extract = {
            "op": "json_extract",
            "args": [{"field": "metadata"}],
            "path": ["billing", "plan"],
            "as_text": True,
        }
        assert json_extract["args"][0] == expr_field("metadata").to_dict()

        assert RowsExpressionCondition(
            lhs=expr_field("status"),
            op=RowsExpressionConditionOp.IS_NOT_NULL,
        ).to_dict() == {"lhs": {"field": "status"}, "op": "is_not_null"}
        assert RowsExpressionConditionGroup(
            all_=[
                RowsExpressionCondition(
                    lhs=expr_field("status"),
                    op=RowsExpressionConditionOp.EQ,
                    rhs=expr_value("ready"),
                )
            ]
        ).to_dict() == {"all": [{"lhs": {"field": "status"}, "op": "eq", "rhs": {"value": "ready"}}]}

    def test_rows_where_branch_models_serialize_typed_branch_shapes(self) -> None:
        branch_atom = RowsWhereBranchAtom(
            field="priority",
            op=RowsWhereBranchAtomOp.GT,
            value=10,
            case_insensitive=UNSET,
            negated=UNSET,
        )
        structured_branch_atom = RowsWhereBranchAtom(
            field="tags",
            op=RowsWhereBranchAtomOp.ARRAY_CONTAINS,
            value="paid",
            case_insensitive=UNSET,
            negated=UNSET,
        )
        branch_all = RowsWhereBranchAll(
            all_=[
                RowsWhereAtom(
                    field="tier",
                    op=RowsWhereAtomOp.EQ,
                    value="enterprise",
                    case_insensitive=UNSET,
                    negated=UNSET,
                ),
                RowsWhereAtom(
                    field="metadata",
                    op=RowsWhereAtomOp.JSON_PATH_EQ,
                    path=["billing", "plan"],
                    value="pro",
                    case_insensitive=UNSET,
                    negated=UNSET,
                ),
                RowsWhereAtom(
                    field="email",
                    op=RowsWhereAtomOp.TEXT_PATTERN,
                    pattern="%@example.test",
                    case_insensitive=True,
                    negated=UNSET,
                ),
            ]
        )

        assert branch_atom.to_dict() == {"field": "priority", "op": "gt", "value": 10}
        assert structured_branch_atom.to_dict() == {"field": "tags", "op": "array_contains", "value": "paid"}
        assert branch_all.to_dict() == {
            "all": [
                {"field": "tier", "op": "eq", "value": "enterprise"},
                {"field": "metadata", "op": "json_path_eq", "path": ["billing", "plan"], "value": "pro"},
                {"case_insensitive": True, "field": "email", "op": "text_pattern", "pattern": "%@example.test"},
            ]
        }
        assert RowsWhereBranchAtom.from_dict(branch_atom.to_dict()).op == RowsWhereBranchAtomOp.GT
        assert (
            RowsWhereBranchAtom.from_dict(structured_branch_atom.to_dict()).op == RowsWhereBranchAtomOp.ARRAY_CONTAINS
        )
        assert RowsWhereBranchAll.from_dict(branch_all.to_dict()).all_[0].field == "tier"
        assert RowsWhereBranchAll.from_dict(branch_all.to_dict()).all_[1].op == RowsWhereAtomOp.JSON_PATH_EQ

    def test_rows_batch_models_serialize_typed_conflict_and_transforms(self) -> None:
        patch = RowsFieldPatch.from_dict({"status": "active"})
        increment = RowsNumericIncrement.from_dict({"amount": 2.5})
        patch_expr = RowsExpressionAssignmentMap.from_dict(
            {"status_key": {"op": "lower", "args": [{"field": "status", "source": "proposed"}]}}
        )
        json_set = RowsJsonSetTransform(field="metadata", path=["billing", "plan"], value="pro")
        json_set_expr = RowsJsonSetTransform(
            field="metadata",
            path=["billing", "status_key"],
            expr=expr_op(RowsExpressionOperatorOp.LOWER, [expr_field("status", "proposed")]),
        )
        array_update = RowsArrayUpdateTransform(
            field="tags",
            op=RowsArrayUpdateTransformOp.ADD_TO_SET,
            value="paid",
        )
        partial_target = RowsUniquePredicateGroup(
            all_=[RowsUniquePredicate(field="status", op=RowsUniquePredicateOp.EQ, value="active")]
        )
        on_conflict = RowsOnConflict(
            target={
                "unique": RowsConflictUniqueTarget(
                    name="usage_records_active_email_key",
                    where=partial_target,
                ).to_dict()
            },
            action=RowsOnConflictAction.UPDATE,
            patch=patch,
            increment=increment,
            patch_expr=patch_expr,
            json_set=[json_set, json_set_expr],
            array_update=[array_update],
            where_expression=RowsExpressionCondition(
                lhs=expr_field("status", "proposed"),
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
        assert conflict["json_set"][1]["path"] == ["billing", "status_key"]
        assert conflict["json_set"][1]["expr"]["args"][0]["source"] == "proposed"
        assert conflict["array_update"][0]["op"] == "add_to_set"
        assert batch_dict["operations"][0]["returning_expressions"][0]["as"] == "status_label"

    def test_rows_aggregate_plan_models_serialize_typed_having(self) -> None:
        plan = RowsAggregatePlanRequest(
            ranges=[doc_key_range("row:a", "row:z")],
            aggregate=RowsAggregateRequest(
                source=RowsQueryRequest(where=where_atom("status", RowsWhereType0Op.EQ, "open")),
                group_by=["customer_id"],
                aggregations=[
                    RowsAggregateSpec(name="amount_sum", op=RowsAggregateSpecOp.SUM, field="amount"),
                    RowsAggregateSpec(name="row_count", op=RowsAggregateSpecOp.COUNT),
                    RowsAggregateSpec(
                        name="recent_statuses",
                        op=RowsAggregateSpecOp.ARRAY_AGG,
                        field="status",
                        distinct=True,
                        distinct_max_items=128,
                        array_max_items=64,
                        array_order_by=[row_order("created_at", "desc")],
                    ),
                ],
                having=RowsAggregateHaving(
                    all_=[
                        RowsAggregateHavingPredicate(
                            field="amount_sum",
                            op=RowsAggregateHavingPredicateOp.GT,
                            value=100,
                        ),
                        RowsAggregateHavingPredicate(
                            field="optional_amount",
                            op=RowsAggregateHavingPredicateOp.IS_NOT_DISTINCT,
                            value=None,
                        ),
                    ]
                ),
                order_by=[row_order("amount_sum", "desc")],
                limit=10,
            ),
        )

        plan_dict = plan.to_dict()
        assert plan_dict["aggregate"]["having"]["all"][0] == {
            "field": "amount_sum",
            "op": "gt",
            "value": 100,
        }
        assert plan_dict["aggregate"]["having"]["all"][1] == {
            "field": "optional_amount",
            "op": "is_not_distinct",
            "value": None,
        }
        assert plan_dict["aggregate"]["aggregations"][1] == {"name": "row_count", "op": "count"}
        assert plan_dict["aggregate"]["aggregations"][2]["array_max_items"] == 64
        assert plan_dict["aggregate"]["aggregations"][2]["distinct_max_items"] == 128
        assert plan_dict["ranges"] == [{"end": "row:z", "start": "row:a"}]

        assert RowsAggregateSpec.from_dict({"name": "row_count", "op": "count"}).to_dict() == {
            "name": "row_count",
            "op": "count",
        }

        assert RowsAggregateHavingPredicate.from_dict(
            {"field": "optional_amount", "op": "is_not_distinct", "value": None}
        ).to_dict() == {"field": "optional_amount", "op": "is_not_distinct", "value": None}

    def test_rows_window_models_serialize_function_and_frame_contracts(self) -> None:
        frame = RowsWindowFrame(
            unit=RowsWindowFrameUnit.ROWS,
            start=RowsWindowFrameStart.OFFSET_PRECEDING,
            start_offset=1,
            end=RowsWindowFrameEnd.OFFSET_FOLLOWING,
            end_offset=1,
        )
        lag = RowsWindowSpec(
            as_="previous_amount",
            function="lag",
            order_by=[row_order("created_at")],
            expr=expr_field("amount"),
            default=None,
            frame=frame,
        )

        assert lag.to_dict()["default"] is None
        assert lag.to_dict()["frame"] == {
            "unit": "rows",
            "start": "offset_preceding",
            "start_offset": 1,
            "end": "offset_following",
            "end_offset": 1,
        }

    def test_rows_join_and_lateral_plan_models_serialize_declared_ranges(self) -> None:
        join_plan = RowsJoinPlanRequest(
            left_ranges=[doc_key_range("row:orders:", "row:orders;")],
            right_ranges=[doc_key_range("row:customers:", "row:customers;")],
            join=RowsJoinRequest(
                left=RowsQueryRequest(),
                right=RowsQueryRequest(),
                on=[RowsJoinOn(left_field="customer_id", right_field="id")],
            ),
        )
        join_dict = join_plan.to_dict()
        assert join_dict["left_ranges"] == [{"end": "row:orders;", "start": "row:orders:"}]
        assert join_dict["right_ranges"] == [{"end": "row:customers;", "start": "row:customers:"}]

        lateral_plan = RowsLateralPlanRequest(
            left_ranges=[doc_key_range("row:org:", "row:org;")],
            right_ranges=[doc_key_range("row:bal:", "row:bal;")],
            lateral=RowsLateralRequest(
                left=RowsQueryRequest(),
                right=RowsQueryRequest(limit=1),
                correlations=[RowsLateralCorrelation(left_field="id", right_field="organization_id")],
            ),
        )
        lateral_dict = lateral_plan.to_dict()
        assert lateral_dict["left_ranges"] == [{"end": "row:org;", "start": "row:org:"}]
        assert lateral_dict["right_ranges"] == [{"end": "row:bal;", "start": "row:bal:"}]

    def test_rows_query_models_serialize_typed_where_tree(self) -> None:
        request = RowsQueryRequest(
            where={
                "all": [
                    RowsWhereAtom(
                        field="status",
                        op=RowsWhereAtomOp.EQ,
                        value="ready",
                        case_insensitive=UNSET,
                        negated=UNSET,
                    ).to_dict(),
                    RowsWhereAtom(
                        field="tags",
                        op=RowsWhereAtomOp.ARRAY_CONTAINS,
                        value="paid",
                        case_insensitive=UNSET,
                        negated=UNSET,
                    ).to_dict(),
                    RowsWhereAtom(
                        field="metadata",
                        op=RowsWhereAtomOp.JSON_PATH_EQ,
                        path=["billing", "plan"],
                        value="pro",
                        case_insensitive=UNSET,
                        negated=UNSET,
                    ).to_dict(),
                    RowsWhereAtom(
                        field="email",
                        op=RowsWhereAtomOp.TEXT_PATTERN,
                        pattern="%@example.test",
                        case_insensitive=True,
                        negated=UNSET,
                    ).to_dict(),
                    RowsWhereAtom(
                        field="archived_at",
                        op=RowsWhereAtomOp.IS_DISTINCT,
                        value=None,
                        case_insensitive=UNSET,
                        negated=UNSET,
                    ).to_dict(),
                ],
                "any": [
                    {"field": "priority", "op": "gt", "value": 10},
                    {"field": "tags", "op": "array_contains", "value": "paid"},
                    {"field": "archived_at", "op": "is_distinct", "value": None},
                    {
                        "all": [
                            RowsWhereAtom(
                                field="tier",
                                op=RowsWhereAtomOp.EQ,
                                value="enterprise",
                                case_insensitive=UNSET,
                                negated=UNSET,
                            ).to_dict(),
                            RowsWhereAtom(
                                field="metadata",
                                op=RowsWhereAtomOp.JSON_PATH_EQ,
                                path=["billing", "plan"],
                                value="pro",
                                case_insensitive=UNSET,
                                negated=UNSET,
                            ).to_dict(),
                            RowsWhereAtom(
                                field="disabled_at",
                                op=RowsWhereAtomOp.IS_DISTINCT,
                                value=None,
                                case_insensitive=UNSET,
                                negated=UNSET,
                            ).to_dict(),
                        ]
                    },
                ],
                "not": [{"field": "tags", "op": "array_contains", "value": "cold"}],
            }
        )

        request_dict = request.to_dict()
        where = request_dict["where"]
        assert where["all"][1]["op"] == "array_contains"
        assert where["all"][2]["path"] == ["billing", "plan"]
        assert where["all"][3]["case_insensitive"] is True
        assert where["all"][4] == {"field": "archived_at", "op": "is_distinct", "value": None}
        assert where["any"][1] == {"field": "tags", "op": "array_contains", "value": "paid"}
        assert where["any"][2] == {"field": "archived_at", "op": "is_distinct", "value": None}
        assert where["any"][3]["all"][0]["value"] == "enterprise"
        assert where["any"][3]["all"][1] == {
            "field": "metadata",
            "op": "json_path_eq",
            "path": ["billing", "plan"],
            "value": "pro",
        }
        assert where["any"][3]["all"][2] == {"field": "disabled_at", "op": "is_distinct", "value": None}
        assert where["not"][0] == {"field": "tags", "op": "array_contains", "value": "cold"}

        assert RowsQueryRequest(where=where_atom("archived_at", RowsWhereType0Op.IS_DISTINCT, None)).to_dict()[
            "where"
        ] == {"field": "archived_at", "op": "is_distinct", "value": None}

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
            expression_array_contains=[
                RowsExpressionArrayContainsPredicate(
                    expr=expr_op(RowsExpressionOperatorOp.STRING_TO_ARRAY, [expr_field("scope")]),
                    value=["read"],
                )
            ],
            coalesce=[
                RowsCoalesceProjection(
                    as_="name_or_email",
                    operands=[
                        coalesce_field("display_name"),
                        coalesce_field("email"),
                        coalesce_value("unknown"),
                        coalesce_value(None),
                    ],
                )
            ],
            field_aliases=[RowsFieldAliasProjection(as_="raw_id", field="id")],
        )

        request_dict = request.to_dict()
        assert request_dict["json_extract"][0]["path"] == ["billing", "plan"]
        assert request_dict["json_extract"][0]["as_text"] is True
        assert request_dict["array_length"][0] == {"as": "tag_count", "field": "tags"}
        assert request_dict["expression_array_contains"] == [
            {"expr": {"args": [{"field": "scope"}], "op": "string_to_array"}, "value": ["read"]}
        ]
        assert request_dict["coalesce"][0]["operands"][2] == {"value": "unknown"}
        assert request_dict["coalesce"][0]["operands"][3] == {"value": None}
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
    def test_execute_sql(self, mock_client_class: MagicMock) -> None:
        """Test executing a SQL statement."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "kind": "read",
            "session_id": 42,
            "statement_kind": "select",
            "result": {"rows": [{"id": "doc-1"}]},
        }

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        result = client.execute_sql(
            "select * from documents",
            session_id=41,
            database="main",
            namespace="public",
            read_only=True,
        )

        assert isinstance(result, SqlStatementResponse)
        assert result.kind == SqlStatementResponseKind.READ
        assert result.session_id == 42
        assert result.statement_kind == "select"
        mock_httpx.request.assert_called_once_with(
            "POST",
            "/db/v1/sql",
            json={
                "sql": "select * from documents",
                "session_id": 41,
                "database": "main",
                "namespace": "public",
                "read_only": True,
            },
        )

    @patch("antfly.client.Client")
    def test_query_preserves_sorted_cursor_contract(self, mock_client_class: MagicMock) -> None:
        """High-level query forwards order_by/search_after/profile and returns generated response model."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "responses": [
                {
                    "took": 3,
                    "status": 200,
                    "hits": {
                        "hits": [
                            {
                                "_id": "doc:2",
                                "_score": 1.0,
                                "_sort": ["2026-01-01T00:00:00Z", "doc:2"],
                                "_source": {"created_at": "2026-01-01T00:00:00Z"},
                            }
                        ]
                    },
                }
            ]
        }

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        result = client.query(
            table="docs",
            query={"match_all": {}},
            order_by=[{"field": "created_at", "desc": True}],
            search_after=["2025-12-31T00:00:00Z", "doc:1"],
            limit=10,
            profile=False,
        )

        mock_httpx.request.assert_called_once_with(
            "POST",
            "/db/v1/tables/docs/query",
            json={
                "query": {"match_all": {}},
                "limit": 10,
                "order_by": [{"field": "created_at", "desc": True}],
                "search_after": ["2025-12-31T00:00:00Z", "doc:1"],
                "profile": False,
            },
        )
        assert not isinstance(result.responses, Unset)
        query_result = result.responses[0]
        assert not isinstance(query_result.hits, Unset)
        assert not isinstance(query_result.hits.hits, Unset)
        hit = query_result.hits.hits[0]
        assert hit.field_id == "doc:2"
        assert hit.field_sort == ["2026-01-01T00:00:00Z", "doc:2"]

    def test_query_rejects_ambiguous_aggregation_aliases(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException, match="either aggregations or facets"):
            client.query(table="docs", aggregations={"a": {}}, facets={"b": {}})

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

    @patch("antfly.client.Client")
    def test_batch_rejects_oversized_request(self, mock_client_class: MagicMock) -> None:
        """Test client-side write request size enforcement."""
        mock_httpx = MagicMock()
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx
        client = AntflyClient(base_url="http://localhost:8080", max_write_request_bytes=32)

        with pytest.raises(AntflyException) as exc_info:
            client.batch(table="users", inserts={"user:1": {"bio": "x" * 128}})

        assert "exceeding max write request size 32" in str(exc_info.value)
        mock_httpx.request.assert_not_called()

    @patch("antfly.client.Client")
    def test_batch_sends_exact_checked_bytes(self, mock_client_class: MagicMock) -> None:
        """Test batch sends the same bytes used for request-size enforcement."""
        expected_body = {"inserts": {"user:1": {"name": "Zoë"}}, "deletes": []}
        expected_content = json.dumps(expected_body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"inserted": 1}

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080", max_write_request_bytes=len(expected_content))
        client.batch(table="users", inserts={"user:1": {"name": "Zoë"}})

        mock_httpx.request.assert_called_once_with(
            "POST",
            "/db/v1/tables/users/batch",
            content=expected_content,
            headers={"Content-Type": "application/json"},
        )
