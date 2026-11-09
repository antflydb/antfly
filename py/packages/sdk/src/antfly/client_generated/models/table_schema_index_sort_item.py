from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.table_schema_index_sort_item_order import TableSchemaIndexSortItemOrder
from ..types import UNSET, Unset

T = TypeVar("T", bound="TableSchemaIndexSortItem")


@_attrs_define
class TableSchemaIndexSortItem:
    """
    Attributes:
        field (str):
        order (TableSchemaIndexSortItemOrder | Unset):  Default: TableSchemaIndexSortItemOrder.ASC.
    """

    field: str
    order: TableSchemaIndexSortItemOrder | Unset = TableSchemaIndexSortItemOrder.ASC

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        order: str | Unset = UNSET
        if not isinstance(self.order, Unset):
            order = self.order.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )
        if order is not UNSET:
            field_dict["order"] = order

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        _order = d.pop("order", UNSET)
        order: TableSchemaIndexSortItemOrder | Unset
        if isinstance(_order, Unset):
            order = UNSET
        else:
            order = TableSchemaIndexSortItemOrder(_order)

        table_schema_index_sort_item = cls(
            field=field,
            order=order,
        )

        return table_schema_index_sort_item
