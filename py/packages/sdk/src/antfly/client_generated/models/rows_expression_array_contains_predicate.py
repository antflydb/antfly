from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_value import RowsExpressionValue


T = TypeVar("T", bound="RowsExpressionArrayContainsPredicate")


@_attrs_define
class RowsExpressionArrayContainsPredicate:
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
        value (list[Any]):
    """

    expr: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue
    value: list[Any]

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

        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "expr": expr,
                "value": value,
            }
        )

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

        value = cast(list[Any], d.pop("value"))

        rows_expression_array_contains_predicate = cls(
            expr=expr,
            value=value,
        )

        return rows_expression_array_contains_predicate
