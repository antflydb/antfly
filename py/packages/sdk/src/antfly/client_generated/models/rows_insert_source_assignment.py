from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_value import RowsExpressionValue


T = TypeVar("T", bound="RowsInsertSourceAssignment")


@_attrs_define
class RowsInsertSourceAssignment:
    """Target-column assignment for a typed insert-source plan.

    Attributes:
        target_field (str): Declared target-table relational field to populate.
        expr (RowsExpressionField | RowsExpressionOperator | RowsExpressionValue): Shared typed row-expression AST. A
            node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported
            operators are the shared row-local expression surface used by schema
            predicates, mutation expressions, query projections, filters, grouping,
            ordering, and SQL lowering.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
    """

    target_field: str
    expr: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue

        target_field = self.target_field

        expr: dict[str, Any]
        if isinstance(self.expr, RowsExpressionField):
            expr = self.expr.to_dict()
        elif isinstance(self.expr, RowsExpressionValue):
            expr = self.expr.to_dict()
        else:
            expr = self.expr.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "target_field": target_field,
                "expr": expr,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_operator import RowsExpressionOperator
        from ..models.rows_expression_value import RowsExpressionValue

        d = dict(src_dict)
        target_field = d.pop("target_field")

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

        rows_insert_source_assignment = cls(
            target_field=target_field,
            expr=expr,
        )

        return rows_insert_source_assignment
