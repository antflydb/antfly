from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_evaluation_scope import QueryEvaluationScope
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_evaluation_aggregations import QueryEvaluationAggregations
    from ..models.query_evaluation_compute import QueryEvaluationCompute
    from ..models.query_evaluation_order_by_item import QueryEvaluationOrderByItem
    from ..models.query_evaluation_where import QueryEvaluationWhere


T = TypeVar("T", bound="QueryEvaluation")


@_attrs_define
class QueryEvaluation:
    """Evaluate expressions after global retrieval merging, before final
    offset/limit. Candidates require candidate_count; matches require
    max_rows and fail if the full qualifying population exceeds that budget.
    Cursor pagination, reranking, pruning, and ordinary aggregations cannot
    be combined with evaluation. NULL inputs skip inference; errors fail.

        Attributes:
            scope (QueryEvaluationScope):
            compute (QueryEvaluationCompute):
            graph_query (str | Unset): Evaluate completed bindings of this named graph MATCH instead of retrieval hits.
                Fields use alias.document.path or alias.key. Existing graph aggregates cannot be combined with this stage.
            candidate_count (int | Unset):
            max_rows (int | Unset):
            where (QueryEvaluationWhere | Unset): Exactly one of eq, neq, lt, lte, gt, gte (two expressions), is_null
                (expression), not (predicate), and, or (predicate arrays). Comparisons propagate NULL.
            order_by (list[QueryEvaluationOrderByItem] | Unset):
            aggregations (QueryEvaluationAggregations | Unset):
    """

    scope: QueryEvaluationScope
    compute: QueryEvaluationCompute
    graph_query: str | Unset = UNSET
    candidate_count: int | Unset = UNSET
    max_rows: int | Unset = UNSET
    where: QueryEvaluationWhere | Unset = UNSET
    order_by: list[QueryEvaluationOrderByItem] | Unset = UNSET
    aggregations: QueryEvaluationAggregations | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        scope = self.scope.value

        compute = self.compute.to_dict()

        graph_query = self.graph_query

        candidate_count = self.candidate_count

        max_rows = self.max_rows

        where: dict[str, Any] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = self.where.to_dict()

        order_by: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = []
            for order_by_item_data in self.order_by:
                order_by_item = order_by_item_data.to_dict()
                order_by.append(order_by_item)

        aggregations: dict[str, Any] | Unset = UNSET
        if not isinstance(self.aggregations, Unset):
            aggregations = self.aggregations.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "scope": scope,
                "compute": compute,
            }
        )
        if graph_query is not UNSET:
            field_dict["graph_query"] = graph_query
        if candidate_count is not UNSET:
            field_dict["candidate_count"] = candidate_count
        if max_rows is not UNSET:
            field_dict["max_rows"] = max_rows
        if where is not UNSET:
            field_dict["where"] = where
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if aggregations is not UNSET:
            field_dict["aggregations"] = aggregations

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_evaluation_aggregations import QueryEvaluationAggregations
        from ..models.query_evaluation_compute import QueryEvaluationCompute
        from ..models.query_evaluation_order_by_item import QueryEvaluationOrderByItem
        from ..models.query_evaluation_where import QueryEvaluationWhere

        d = dict(src_dict)
        scope = QueryEvaluationScope(d.pop("scope"))

        compute = QueryEvaluationCompute.from_dict(d.pop("compute"))

        graph_query = d.pop("graph_query", UNSET)

        candidate_count = d.pop("candidate_count", UNSET)

        max_rows = d.pop("max_rows", UNSET)

        _where = d.pop("where", UNSET)
        where: QueryEvaluationWhere | Unset
        if isinstance(_where, Unset):
            where = UNSET
        else:
            where = QueryEvaluationWhere.from_dict(_where)

        _order_by = d.pop("order_by", UNSET)
        order_by: list[QueryEvaluationOrderByItem] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:
                order_by_item = QueryEvaluationOrderByItem.from_dict(order_by_item_data)

                order_by.append(order_by_item)

        _aggregations = d.pop("aggregations", UNSET)
        aggregations: QueryEvaluationAggregations | Unset
        if isinstance(_aggregations, Unset):
            aggregations = UNSET
        else:
            aggregations = QueryEvaluationAggregations.from_dict(_aggregations)

        query_evaluation = cls(
            scope=scope,
            compute=compute,
            graph_query=graph_query,
            candidate_count=candidate_count,
            max_rows=max_rows,
            where=where,
            order_by=order_by,
            aggregations=aggregations,
        )

        return query_evaluation
