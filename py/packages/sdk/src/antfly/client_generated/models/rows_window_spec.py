from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_value import RowsExpressionValue
    from ..models.rows_query_order_expression import RowsQueryOrderExpression
    from ..models.rows_query_order_field import RowsQueryOrderField
    from ..models.rows_window_frame import RowsWindowFrame


T = TypeVar("T", bound="RowsWindowSpec")


@_attrs_define
class RowsWindowSpec:
    """
    Attributes:
        as_ (str):
        function (str): Window function name. Supported values are `row_number`, `rank`, `dense_rank`, `percent_rank`,
            `cume_dist`, `ntile`, `lag`, `lead`, `first_value`, `last_value`, `nth_value`, `count`, `sum`, `avg`, `min`, and
            `max`.
        order_by (list[RowsQueryOrderExpression | RowsQueryOrderField]):
        partition_by (list[str] | Unset):
        expr (RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset): Shared typed row-expression
            AST. A node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported
            operators are the shared row-local expression surface used by schema
            predicates, mutation expressions, query projections, filters, grouping,
            ordering, and SQL lowering.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
        offset (int | Unset):
        default (Any | Unset):
        frame (RowsWindowFrame | Unset):
    """

    as_: str
    function: str
    order_by: list[RowsQueryOrderExpression | RowsQueryOrderField]
    partition_by: list[str] | Unset = UNSET
    expr: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset = UNSET
    offset: int | Unset = UNSET
    default: Any | Unset = UNSET
    frame: RowsWindowFrame | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue
        from ..models.rows_query_order_field import RowsQueryOrderField

        as_ = self.as_

        function = self.function

        order_by = []
        for order_by_item_data in self.order_by:
            order_by_item: dict[str, Any]
            if isinstance(order_by_item_data, RowsQueryOrderField):
                order_by_item = order_by_item_data.to_dict()
            else:
                order_by_item = order_by_item_data.to_dict()

            order_by.append(order_by_item)

        partition_by: list[str] | Unset = UNSET
        if not isinstance(self.partition_by, Unset):
            partition_by = self.partition_by

        expr: dict[str, Any] | Unset
        if isinstance(self.expr, Unset):
            expr = UNSET
        elif isinstance(self.expr, RowsExpressionField):
            expr = self.expr.to_dict()
        elif isinstance(self.expr, RowsExpressionValue):
            expr = self.expr.to_dict()
        else:
            expr = self.expr.to_dict()

        offset = self.offset

        default = self.default

        frame: dict[str, Any] | Unset = UNSET
        if not isinstance(self.frame, Unset):
            frame = self.frame.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "as": as_,
                "function": function,
                "order_by": order_by,
            }
        )
        if partition_by is not UNSET:
            field_dict["partition_by"] = partition_by
        if expr is not UNSET:
            field_dict["expr"] = expr
        if offset is not UNSET:
            field_dict["offset"] = offset
        if default is not UNSET:
            field_dict["default"] = default
        if frame is not UNSET:
            field_dict["frame"] = frame

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_operator import RowsExpressionOperator
        from ..models.rows_expression_value import RowsExpressionValue
        from ..models.rows_query_order_expression import RowsQueryOrderExpression
        from ..models.rows_query_order_field import RowsQueryOrderField
        from ..models.rows_window_frame import RowsWindowFrame

        d = dict(src_dict)
        as_ = d.pop("as")

        function = d.pop("function")

        order_by = []
        _order_by = d.pop("order_by")
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

        partition_by = cast(list[str], d.pop("partition_by", UNSET))

        def _parse_expr(data: object) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_rows_expression_type_0 = RowsExpressionField.from_dict(data)

                return componentsschemas_rows_expression_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_rows_expression_type_1 = RowsExpressionValue.from_dict(data)

                return componentsschemas_rows_expression_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_rows_expression_type_2 = RowsExpressionOperator.from_dict(data)

            return componentsschemas_rows_expression_type_2

        expr = _parse_expr(d.pop("expr", UNSET))

        offset = d.pop("offset", UNSET)

        default = d.pop("default", UNSET)

        _frame = d.pop("frame", UNSET)
        frame: RowsWindowFrame | Unset
        if isinstance(_frame, Unset):
            frame = UNSET
        else:
            frame = RowsWindowFrame.from_dict(_frame)

        rows_window_spec = cls(
            as_=as_,
            function=function,
            order_by=order_by,
            partition_by=partition_by,
            expr=expr,
            offset=offset,
            default=default,
            frame=frame,
        )

        return rows_window_spec
