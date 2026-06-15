from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_value import RowsExpressionValue


T = TypeVar("T", bound="RowsJsonSetTransform")


@_attrs_define
class RowsJsonSetTransform:
    """JSON path assignment for a declared `json` column. Exactly one of `value` or `expr` must be supplied.

    Attributes:
        field (str): Declared `json` column to update.
        path (list[str]): Non-empty path under the JSON column.
        value (Any | Unset): JSON value to write at the path.
        expr (RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset): Shared typed row-expression
            AST. A node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported
            operators are the shared row-local expression surface used by schema
            predicates, mutation expressions, query projections, filters, grouping,
            ordering, and SQL lowering.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
    """

    field: str
    path: list[str]
    value: Any | Unset = UNSET
    expr: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue

        field = self.field

        path = self.path

        value = self.value

        expr: dict[str, Any] | Unset
        if isinstance(self.expr, Unset):
            expr = UNSET
        elif isinstance(self.expr, RowsExpressionField):
            expr = self.expr.to_dict()
        elif isinstance(self.expr, RowsExpressionValue):
            expr = self.expr.to_dict()
        else:
            expr = self.expr.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
                "path": path,
            }
        )
        if value is not UNSET:
            field_dict["value"] = value
        if expr is not UNSET:
            field_dict["expr"] = expr

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_operator import RowsExpressionOperator
        from ..models.rows_expression_value import RowsExpressionValue

        d = dict(src_dict)
        field = d.pop("field")

        path = cast(list[str], d.pop("path"))

        value = d.pop("value", UNSET)

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

        rows_json_set_transform = cls(
            field=field,
            path=path,
            value=value,
            expr=expr,
        )

        return rows_json_set_transform
