from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_aggregate_having import RowsAggregateHaving
    from ..models.rows_aggregate_spec import RowsAggregateSpec
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
    from ..models.rows_expression_projection import RowsExpressionProjection
    from ..models.rows_query_order_expression import RowsQueryOrderExpression
    from ..models.rows_query_order_field import RowsQueryOrderField
    from ..models.rows_query_request import RowsQueryRequest


T = TypeVar("T", bound="RowsAggregateRequest")


@_attrs_define
class RowsAggregateRequest:
    """
    Attributes:
        source (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        group_by (list[str] | Unset):
        group_expressions (list[RowsExpressionProjection] | Unset): Named expression group keys. These are evaluated for
            each source row, included in the grouping key, and emitted under their `as` names in aggregate result rows.
        aggregations (list[RowsAggregateSpec] | Unset): Metric specs to compute for each group. May be empty or omitted
            only when group_by or group_expressions is non-empty, which returns one row per distinct group key.
        having (RowsAggregateHaving | Unset): Conjunction of emitted aggregate-output predicates for HAVING.
        having_expressions (list[RowsExpressionCondition] | Unset): Expression predicates over aggregate output fields,
            evaluated after grouping. Field references bind to group keys or aggregation names.
        having_any (list[RowsExpressionConditionGroup] | Unset): Disjunction of aggregate-output expression predicate
            groups. Each group is a conjunction; the aggregate row passes when at least one group matches.
        having_not (list[RowsExpressionConditionGroup] | Unset): Negated aggregate-output expression predicate groups.
            Each group is a conjunction; the aggregate row is rejected when any group matches.
        order_by (list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset):
        limit (int | Unset):
        offset (int | Unset):
    """

    source: RowsQueryRequest
    group_by: list[str] | Unset = UNSET
    group_expressions: list[RowsExpressionProjection] | Unset = UNSET
    aggregations: list[RowsAggregateSpec] | Unset = UNSET
    having: RowsAggregateHaving | Unset = UNSET
    having_expressions: list[RowsExpressionCondition] | Unset = UNSET
    having_any: list[RowsExpressionConditionGroup] | Unset = UNSET
    having_not: list[RowsExpressionConditionGroup] | Unset = UNSET
    order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
    limit: int | Unset = UNSET
    offset: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_query_order_field import RowsQueryOrderField

        source = self.source.to_dict()

        group_by: list[str] | Unset = UNSET
        if not isinstance(self.group_by, Unset):
            group_by = self.group_by

        group_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.group_expressions, Unset):
            group_expressions = []
            for group_expressions_item_data in self.group_expressions:
                group_expressions_item = group_expressions_item_data.to_dict()
                group_expressions.append(group_expressions_item)

        aggregations: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.aggregations, Unset):
            aggregations = []
            for aggregations_item_data in self.aggregations:
                aggregations_item = aggregations_item_data.to_dict()
                aggregations.append(aggregations_item)

        having: dict[str, Any] | Unset = UNSET
        if not isinstance(self.having, Unset):
            having = self.having.to_dict()

        having_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.having_expressions, Unset):
            having_expressions = []
            for having_expressions_item_data in self.having_expressions:
                having_expressions_item = having_expressions_item_data.to_dict()
                having_expressions.append(having_expressions_item)

        having_any: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.having_any, Unset):
            having_any = []
            for having_any_item_data in self.having_any:
                having_any_item = having_any_item_data.to_dict()
                having_any.append(having_any_item)

        having_not: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.having_not, Unset):
            having_not = []
            for having_not_item_data in self.having_not:
                having_not_item = having_not_item_data.to_dict()
                having_not.append(having_not_item)

        order_by: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = []
            for order_by_item_data in self.order_by:
                order_by_item: dict[str, Any]
                if isinstance(order_by_item_data, RowsQueryOrderField):
                    order_by_item = order_by_item_data.to_dict()
                else:
                    order_by_item = order_by_item_data.to_dict()

                order_by.append(order_by_item)

        limit = self.limit

        offset = self.offset

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "source": source,
            }
        )
        if group_by is not UNSET:
            field_dict["group_by"] = group_by
        if group_expressions is not UNSET:
            field_dict["group_expressions"] = group_expressions
        if aggregations is not UNSET:
            field_dict["aggregations"] = aggregations
        if having is not UNSET:
            field_dict["having"] = having
        if having_expressions is not UNSET:
            field_dict["having_expressions"] = having_expressions
        if having_any is not UNSET:
            field_dict["having_any"] = having_any
        if having_not is not UNSET:
            field_dict["having_not"] = having_not
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if limit is not UNSET:
            field_dict["limit"] = limit
        if offset is not UNSET:
            field_dict["offset"] = offset

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_aggregate_having import RowsAggregateHaving
        from ..models.rows_aggregate_spec import RowsAggregateSpec
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
        from ..models.rows_expression_projection import RowsExpressionProjection
        from ..models.rows_query_order_expression import RowsQueryOrderExpression
        from ..models.rows_query_order_field import RowsQueryOrderField
        from ..models.rows_query_request import RowsQueryRequest

        d = dict(src_dict)
        source = RowsQueryRequest.from_dict(d.pop("source"))

        group_by = cast(list[str], d.pop("group_by", UNSET))

        _group_expressions = d.pop("group_expressions", UNSET)
        group_expressions: list[RowsExpressionProjection] | Unset = UNSET
        if _group_expressions is not UNSET:
            group_expressions = []
            for group_expressions_item_data in _group_expressions:
                group_expressions_item = RowsExpressionProjection.from_dict(group_expressions_item_data)

                group_expressions.append(group_expressions_item)

        _aggregations = d.pop("aggregations", UNSET)
        aggregations: list[RowsAggregateSpec] | Unset = UNSET
        if _aggregations is not UNSET:
            aggregations = []
            for aggregations_item_data in _aggregations:
                aggregations_item = RowsAggregateSpec.from_dict(aggregations_item_data)

                aggregations.append(aggregations_item)

        _having = d.pop("having", UNSET)
        having: RowsAggregateHaving | Unset
        if isinstance(_having, Unset):
            having = UNSET
        else:
            having = RowsAggregateHaving.from_dict(_having)

        _having_expressions = d.pop("having_expressions", UNSET)
        having_expressions: list[RowsExpressionCondition] | Unset = UNSET
        if _having_expressions is not UNSET:
            having_expressions = []
            for having_expressions_item_data in _having_expressions:
                having_expressions_item = RowsExpressionCondition.from_dict(having_expressions_item_data)

                having_expressions.append(having_expressions_item)

        _having_any = d.pop("having_any", UNSET)
        having_any: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _having_any is not UNSET:
            having_any = []
            for having_any_item_data in _having_any:
                having_any_item = RowsExpressionConditionGroup.from_dict(having_any_item_data)

                having_any.append(having_any_item)

        _having_not = d.pop("having_not", UNSET)
        having_not: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _having_not is not UNSET:
            having_not = []
            for having_not_item_data in _having_not:
                having_not_item = RowsExpressionConditionGroup.from_dict(having_not_item_data)

                having_not.append(having_not_item)

        _order_by = d.pop("order_by", UNSET)
        order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:

                def _parse_order_by_item(data: object) -> RowsQueryOrderExpression | RowsQueryOrderField:
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_rows_query_order_type_0 = RowsQueryOrderField.from_dict(data)

                        return componentsschemas_rows_query_order_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_rows_query_order_type_1 = RowsQueryOrderExpression.from_dict(data)

                    return componentsschemas_rows_query_order_type_1

                order_by_item = _parse_order_by_item(order_by_item_data)

                order_by.append(order_by_item)

        limit = d.pop("limit", UNSET)

        offset = d.pop("offset", UNSET)

        rows_aggregate_request = cls(
            source=source,
            group_by=group_by,
            group_expressions=group_expressions,
            aggregations=aggregations,
            having=having,
            having_expressions=having_expressions,
            having_any=having_any,
            having_not=having_not,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

        return rows_aggregate_request
