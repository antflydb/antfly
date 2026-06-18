from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_join_request_join_type import RowsJoinRequestJoinType
from ..models.rows_join_strategy import RowsJoinStrategy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_array_contains_predicate import RowsExpressionArrayContainsPredicate
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
    from ..models.rows_join_on import RowsJoinOn
    from ..models.rows_join_projection import RowsJoinProjection
    from ..models.rows_query_order_expression import RowsQueryOrderExpression
    from ..models.rows_query_order_field import RowsQueryOrderField
    from ..models.rows_query_request import RowsQueryRequest


T = TypeVar("T", bound="RowsJoinRequest")


@_attrs_define
class RowsJoinRequest:
    """Typed equality join plan. Each side is a full row-query request and can read an ordered CTE through `source_cte`.

    Attributes:
        left (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        right (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        on (list[RowsJoinOn]):
        match_expression_where (list[RowsExpressionCondition] | Unset): Post-match computed predicates over the joined
            rows. Unqualified fields bind to the left row; fields with `source: source` bind to the right row.
        match_expression_any (list[RowsExpressionConditionGroup] | Unset): OR groups of post-match computed predicates
            over the joined rows.
        match_expression_not (list[RowsExpressionConditionGroup] | Unset): NOT groups of post-match computed predicates
            over the joined rows.
        match_expression_array_contains (list[RowsExpressionArrayContainsPredicate] | Unset): Post-match computed array-
            containment predicates over the joined rows.
        join_type (RowsJoinRequestJoinType | Unset):
        strategy (RowsJoinStrategy | Unset): Physical join strategy requested by the typed plan. `auto` lets Antfly
            choose from proven local/routed capabilities; `merge` requires both join inputs to be proven ordered by the
            leading join keys.
        select (list[RowsJoinProjection] | Unset):
        order_by (list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset):
        limit (int | Unset):
        offset (int | Unset):
    """

    left: RowsQueryRequest
    right: RowsQueryRequest
    on: list[RowsJoinOn]
    match_expression_where: list[RowsExpressionCondition] | Unset = UNSET
    match_expression_any: list[RowsExpressionConditionGroup] | Unset = UNSET
    match_expression_not: list[RowsExpressionConditionGroup] | Unset = UNSET
    match_expression_array_contains: list[RowsExpressionArrayContainsPredicate] | Unset = UNSET
    join_type: RowsJoinRequestJoinType | Unset = UNSET
    strategy: RowsJoinStrategy | Unset = UNSET
    select: list[RowsJoinProjection] | Unset = UNSET
    order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
    limit: int | Unset = UNSET
    offset: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_query_order_field import RowsQueryOrderField

        left = self.left.to_dict()

        right = self.right.to_dict()

        on = []
        for on_item_data in self.on:
            on_item = on_item_data.to_dict()
            on.append(on_item)

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

        join_type: str | Unset = UNSET
        if not isinstance(self.join_type, Unset):
            join_type = self.join_type.value

        strategy: str | Unset = UNSET
        if not isinstance(self.strategy, Unset):
            strategy = self.strategy.value

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
                "on": on,
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
        if join_type is not UNSET:
            field_dict["join_type"] = join_type
        if strategy is not UNSET:
            field_dict["strategy"] = strategy
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
        from ..models.rows_join_on import RowsJoinOn
        from ..models.rows_join_projection import RowsJoinProjection
        from ..models.rows_query_order_expression import RowsQueryOrderExpression
        from ..models.rows_query_order_field import RowsQueryOrderField
        from ..models.rows_query_request import RowsQueryRequest

        d = dict(src_dict)
        left = RowsQueryRequest.from_dict(d.pop("left"))

        right = RowsQueryRequest.from_dict(d.pop("right"))

        on = []
        _on = d.pop("on")
        for on_item_data in _on:
            on_item = RowsJoinOn.from_dict(on_item_data)

            on.append(on_item)

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

        _join_type = d.pop("join_type", UNSET)
        join_type: RowsJoinRequestJoinType | Unset
        if isinstance(_join_type, Unset):
            join_type = UNSET
        else:
            join_type = RowsJoinRequestJoinType(_join_type)

        _strategy = d.pop("strategy", UNSET)
        strategy: RowsJoinStrategy | Unset
        if isinstance(_strategy, Unset):
            strategy = UNSET
        else:
            strategy = RowsJoinStrategy(_strategy)

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

        rows_join_request = cls(
            left=left,
            right=right,
            on=on,
            match_expression_where=match_expression_where,
            match_expression_any=match_expression_any,
            match_expression_not=match_expression_not,
            match_expression_array_contains=match_expression_array_contains,
            join_type=join_type,
            strategy=strategy,
            select=select,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

        return rows_join_request
