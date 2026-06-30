from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_array_contains_predicate import RowsExpressionArrayContainsPredicate
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
    from ..models.rows_join_projection import RowsJoinProjection
    from ..models.rows_lateral_correlation import RowsLateralCorrelation
    from ..models.rows_query_order_expression import RowsQueryOrderExpression
    from ..models.rows_query_order_field import RowsQueryOrderField
    from ..models.rows_query_request import RowsQueryRequest


T = TypeVar("T", bound="RowsLateralRequest")


@_attrs_define
class RowsLateralRequest:
    """Typed bounded lateral plan. The right side must include a limit and can read an ordered CTE through `source_cte`.

    Attributes:
        left (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        right (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        correlations (list[RowsLateralCorrelation]):
        match_expression_where (list[RowsExpressionCondition] | Unset): Post-match computed predicates over the left row
            and each bounded right row. Unqualified fields bind to the left row; fields with `source: source` bind to the
            right row.
        match_expression_any (list[RowsExpressionConditionGroup] | Unset): OR groups of post-match computed predicates
            over the left row and each bounded right row.
        match_expression_not (list[RowsExpressionConditionGroup] | Unset): NOT groups of post-match computed predicates
            over the left row and each bounded right row.
        match_expression_array_contains (list[RowsExpressionArrayContainsPredicate] | Unset): Post-match computed array-
            containment predicates over the left row and each bounded right row.
        select (list[RowsJoinProjection] | Unset):
        order_by (list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset):
        limit (int | Unset):
        offset (int | Unset):
    """

    left: RowsQueryRequest
    right: RowsQueryRequest
    correlations: list[RowsLateralCorrelation]
    match_expression_where: list[RowsExpressionCondition] | Unset = UNSET
    match_expression_any: list[RowsExpressionConditionGroup] | Unset = UNSET
    match_expression_not: list[RowsExpressionConditionGroup] | Unset = UNSET
    match_expression_array_contains: list[RowsExpressionArrayContainsPredicate] | Unset = UNSET
    select: list[RowsJoinProjection] | Unset = UNSET
    order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
    limit: int | Unset = UNSET
    offset: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_query_order_field import RowsQueryOrderField

        left = self.left.to_dict()

        right = self.right.to_dict()

        correlations = []
        for correlations_item_data in self.correlations:
            correlations_item = correlations_item_data.to_dict()
            correlations.append(correlations_item)

        match_expression_where: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.match_expression_where, Unset):
            match_expression_where = []
            for match_expression_where_item_data in self.match_expression_where:
                match_expression_where_item = match_expression_where_item_data.to_dict()
                match_expression_where.append(match_expression_where_item)

        match_expression_any: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.match_expression_any, Unset):
            match_expression_any = []
            for match_expression_any_item_data in self.match_expression_any:
                match_expression_any_item = match_expression_any_item_data.to_dict()
                match_expression_any.append(match_expression_any_item)

        match_expression_not: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.match_expression_not, Unset):
            match_expression_not = []
            for match_expression_not_item_data in self.match_expression_not:
                match_expression_not_item = match_expression_not_item_data.to_dict()
                match_expression_not.append(match_expression_not_item)

        match_expression_array_contains: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.match_expression_array_contains, Unset):
            match_expression_array_contains = []
            for match_expression_array_contains_item_data in self.match_expression_array_contains:
                match_expression_array_contains_item = match_expression_array_contains_item_data.to_dict()
                match_expression_array_contains.append(match_expression_array_contains_item)

        select: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.select, Unset):
            select = []
            for select_item_data in self.select:
                select_item = select_item_data.to_dict()
                select.append(select_item)

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
                "left": left,
                "right": right,
                "correlations": correlations,
            }
        )
        if match_expression_where is not UNSET:
            field_dict["match_expression_where"] = match_expression_where
        if match_expression_any is not UNSET:
            field_dict["match_expression_any"] = match_expression_any
        if match_expression_not is not UNSET:
            field_dict["match_expression_not"] = match_expression_not
        if match_expression_array_contains is not UNSET:
            field_dict["match_expression_array_contains"] = match_expression_array_contains
        if select is not UNSET:
            field_dict["select"] = select
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if limit is not UNSET:
            field_dict["limit"] = limit
        if offset is not UNSET:
            field_dict["offset"] = offset

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_array_contains_predicate import RowsExpressionArrayContainsPredicate
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
        from ..models.rows_join_projection import RowsJoinProjection
        from ..models.rows_lateral_correlation import RowsLateralCorrelation
        from ..models.rows_query_order_expression import RowsQueryOrderExpression
        from ..models.rows_query_order_field import RowsQueryOrderField
        from ..models.rows_query_request import RowsQueryRequest

        d = dict(src_dict)
        left = RowsQueryRequest.from_dict(d.pop("left"))

        right = RowsQueryRequest.from_dict(d.pop("right"))

        correlations = []
        _correlations = d.pop("correlations")
        for correlations_item_data in _correlations:
            correlations_item = RowsLateralCorrelation.from_dict(correlations_item_data)

            correlations.append(correlations_item)

        _match_expression_where = d.pop("match_expression_where", UNSET)
        match_expression_where: list[RowsExpressionCondition] | Unset = UNSET
        if _match_expression_where is not UNSET:
            match_expression_where = []
            for match_expression_where_item_data in _match_expression_where:
                match_expression_where_item = RowsExpressionCondition.from_dict(match_expression_where_item_data)

                match_expression_where.append(match_expression_where_item)

        _match_expression_any = d.pop("match_expression_any", UNSET)
        match_expression_any: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _match_expression_any is not UNSET:
            match_expression_any = []
            for match_expression_any_item_data in _match_expression_any:
                match_expression_any_item = RowsExpressionConditionGroup.from_dict(match_expression_any_item_data)

                match_expression_any.append(match_expression_any_item)

        _match_expression_not = d.pop("match_expression_not", UNSET)
        match_expression_not: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _match_expression_not is not UNSET:
            match_expression_not = []
            for match_expression_not_item_data in _match_expression_not:
                match_expression_not_item = RowsExpressionConditionGroup.from_dict(match_expression_not_item_data)

                match_expression_not.append(match_expression_not_item)

        _match_expression_array_contains = d.pop("match_expression_array_contains", UNSET)
        match_expression_array_contains: list[RowsExpressionArrayContainsPredicate] | Unset = UNSET
        if _match_expression_array_contains is not UNSET:
            match_expression_array_contains = []
            for match_expression_array_contains_item_data in _match_expression_array_contains:
                match_expression_array_contains_item = RowsExpressionArrayContainsPredicate.from_dict(
                    match_expression_array_contains_item_data
                )

                match_expression_array_contains.append(match_expression_array_contains_item)

        _select = d.pop("select", UNSET)
        select: list[RowsJoinProjection] | Unset = UNSET
        if _select is not UNSET:
            select = []
            for select_item_data in _select:
                select_item = RowsJoinProjection.from_dict(select_item_data)

                select.append(select_item)

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

        rows_lateral_request = cls(
            left=left,
            right=right,
            correlations=correlations,
            match_expression_where=match_expression_where,
            match_expression_any=match_expression_any,
            match_expression_not=match_expression_not,
            match_expression_array_contains=match_expression_array_contains,
            select=select,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

        return rows_lateral_request
