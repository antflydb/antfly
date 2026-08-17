from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.hierarchy_children_order_by_item_field import HierarchyChildrenOrderByItemField
from ..types import UNSET, Unset

T = TypeVar("T", bound="HierarchyChildrenOrderByItem")


@_attrs_define
class HierarchyChildrenOrderByItem:
    """
    Attributes:
        field (HierarchyChildrenOrderByItemField):
        desc (bool | Unset):  Default: False.
    """

    field: HierarchyChildrenOrderByItemField
    desc: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        field = self.field.value

        desc = self.desc

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )
        if desc is not UNSET:
            field_dict["desc"] = desc

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = HierarchyChildrenOrderByItemField(d.pop("field"))

        desc = d.pop("desc", UNSET)

        hierarchy_children_order_by_item = cls(
            field=field,
            desc=desc,
        )

        return hierarchy_children_order_by_item
