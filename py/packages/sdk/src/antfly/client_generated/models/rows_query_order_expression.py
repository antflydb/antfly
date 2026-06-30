from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_query_order_expression_direction import RowsQueryOrderExpressionDirection
from ..models.rows_query_order_expression_null_test import RowsQueryOrderExpressionNullTest
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_value import RowsExpressionValue


T = TypeVar("T", bound="RowsQueryOrderExpression")


@_attrs_define
class RowsQueryOrderExpression:
    """
    Attributes:
        expr (RowsExpressionField | RowsExpressionOperator | RowsExpressionValue): Shared typed row-expression AST. A
            node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported
            operators are the shared row-local expression surface used by schema
            predicates, mutation expressions, query projections, filters, grouping,
            ordering, and SQL lowering.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
        null_test (RowsQueryOrderExpressionNullTest | Unset):
        direction (RowsQueryOrderExpressionDirection | Unset):
    """

    expr: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue
    null_test: RowsQueryOrderExpressionNullTest | Unset = UNSET
    direction: RowsQueryOrderExpressionDirection | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue

        expr: dict[str, Any]
        if isinstance(self.expr, RowsExpressionField):
            expr = self.expr.to_dict()
        elif isinstance(self.expr, RowsExpressionValue):
            expr = self.expr.to_dict()
        else:
            expr = self.expr.to_dict()

        null_test: str | Unset = UNSET
        if not isinstance(self.null_test, Unset):
            null_test = self.null_test.value

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "expr": expr,
            }
        )
        if null_test is not UNSET:
            field_dict["null_test"] = null_test
        if direction is not UNSET:
            field_dict["direction"] = direction

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_operator import RowsExpressionOperator
        from ..models.rows_expression_value import RowsExpressionValue

        d = dict(src_dict)

        def _parse_expr(data: object) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue:
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

        expr = _parse_expr(d.pop("expr"))

        _null_test = d.pop("null_test", UNSET)
        null_test: RowsQueryOrderExpressionNullTest | Unset
        if isinstance(_null_test, Unset):
            null_test = UNSET
        else:
            null_test = RowsQueryOrderExpressionNullTest(_null_test)

        _direction = d.pop("direction", UNSET)
        direction: RowsQueryOrderExpressionDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = RowsQueryOrderExpressionDirection(_direction)

        rows_query_order_expression = cls(
            expr=expr,
            null_test=null_test,
            direction=direction,
        )

        return rows_query_order_expression
