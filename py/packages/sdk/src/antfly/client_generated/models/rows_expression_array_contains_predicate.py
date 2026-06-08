from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.rows_expression import RowsExpression


T = TypeVar("T", bound="RowsExpressionArrayContainsPredicate")


@_attrs_define
class RowsExpressionArrayContainsPredicate:
    """
    Attributes:
        expr (RowsExpression): Shared typed row-expression AST. A node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported operators
            include `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, numeric
            `add`/`sub`/`mul`/`div`, `cast`, `json_extract`, `array_length`,
            `string_to_array`, and searched `case` with `cases` and `else`.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
        value (list[Any]):
    """

    expr: RowsExpression
    value: list[Any]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        expr = self.expr.to_dict()

        value = self.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "expr": expr,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression import RowsExpression

        d = dict(src_dict)
        expr = RowsExpression.from_dict(d.pop("expr"))

        value = cast(list[Any], d.pop("value"))

        rows_expression_array_contains_predicate = cls(
            expr=expr,
            value=value,
        )

        rows_expression_array_contains_predicate.additional_properties = d
        return rows_expression_array_contains_predicate

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
