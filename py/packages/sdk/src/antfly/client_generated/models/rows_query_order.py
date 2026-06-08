from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.rows_query_order_direction import RowsQueryOrderDirection
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression import RowsExpression


T = TypeVar("T", bound="RowsQueryOrder")


@_attrs_define
class RowsQueryOrder:
    """Ordered row-stream key. `field` names an output/base field; `expression` carries a typed row-expression AST for
    computed ordering.

        Attributes:
            direction (RowsQueryOrderDirection):
            field (str | Unset):
            expression (RowsExpression | Unset): Shared typed row-expression AST. A node is exactly one of `{ "field":
                "name" }`,
                `{ "value": ... }`, or an operator node such as
                `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported operators
                include `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, numeric
                `add`/`sub`/`mul`/`div`, `cast`, `json_extract`, `array_length`,
                `string_to_array`, and searched `case` with `cases` and `else`.
                Mutation expressions may set `source` to `existing` or `proposed`; query
                expressions use the default row source.
    """

    direction: RowsQueryOrderDirection
    field: str | Unset = UNSET
    expression: RowsExpression | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        direction = self.direction.value

        field = self.field

        expression: dict[str, Any] | Unset = UNSET
        if not isinstance(self.expression, Unset):
            expression = self.expression.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "direction": direction,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if expression is not UNSET:
            field_dict["expression"] = expression

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression import RowsExpression

        d = dict(src_dict)
        direction = RowsQueryOrderDirection(d.pop("direction"))

        field = d.pop("field", UNSET)

        _expression = d.pop("expression", UNSET)
        expression: RowsExpression | Unset
        if isinstance(_expression, Unset):
            expression = UNSET
        else:
            expression = RowsExpression.from_dict(_expression)

        rows_query_order = cls(
            direction=direction,
            field=field,
            expression=expression,
        )

        rows_query_order.additional_properties = d
        return rows_query_order

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
