from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression import RowsExpression
    from ..models.rows_query_order import RowsQueryOrder


T = TypeVar("T", bound="RowsWindowSpec")


@_attrs_define
class RowsWindowSpec:
    """
    Attributes:
        as_ (str):
        function (str):
        order_by (list[RowsQueryOrder]):
        partition_by (list[str] | Unset):
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
        offset (int | Unset):
        default (Any | Unset):
    """

    as_: str
    function: str
    order_by: list[RowsQueryOrder]
    partition_by: list[str] | Unset = UNSET
    field: str | Unset = UNSET
    expression: RowsExpression | Unset = UNSET
    offset: int | Unset = UNSET
    default: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        as_ = self.as_

        function = self.function

        order_by = []
        for order_by_item_data in self.order_by:
            order_by_item = order_by_item_data.to_dict()
            order_by.append(order_by_item)

        partition_by: list[str] | Unset = UNSET
        if not isinstance(self.partition_by, Unset):
            partition_by = self.partition_by

        field = self.field

        expression: dict[str, Any] | Unset = UNSET
        if not isinstance(self.expression, Unset):
            expression = self.expression.to_dict()

        offset = self.offset

        default = self.default

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "as": as_,
                "function": function,
                "order_by": order_by,
            }
        )
        if partition_by is not UNSET:
            field_dict["partition_by"] = partition_by
        if field is not UNSET:
            field_dict["field"] = field
        if expression is not UNSET:
            field_dict["expression"] = expression
        if offset is not UNSET:
            field_dict["offset"] = offset
        if default is not UNSET:
            field_dict["default"] = default

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression import RowsExpression
        from ..models.rows_query_order import RowsQueryOrder

        d = dict(src_dict)
        as_ = d.pop("as")

        function = d.pop("function")

        order_by = []
        _order_by = d.pop("order_by")
        for order_by_item_data in _order_by:
            order_by_item = RowsQueryOrder.from_dict(order_by_item_data)

            order_by.append(order_by_item)

        partition_by = cast(list[str], d.pop("partition_by", UNSET))

        field = d.pop("field", UNSET)

        _expression = d.pop("expression", UNSET)
        expression: RowsExpression | Unset
        if isinstance(_expression, Unset):
            expression = UNSET
        else:
            expression = RowsExpression.from_dict(_expression)

        offset = d.pop("offset", UNSET)

        default = d.pop("default", UNSET)

        rows_window_spec = cls(
            as_=as_,
            function=function,
            order_by=order_by,
            partition_by=partition_by,
            field=field,
            expression=expression,
            offset=offset,
            default=default,
        )

        rows_window_spec.additional_properties = d
        return rows_window_spec

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
