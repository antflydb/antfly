from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.hierarchy_child_parent_level import HierarchyChildParentLevel

T = TypeVar("T", bound="HierarchyChildParent")


@_attrs_define
class HierarchyChildParent:
    """
    Attributes:
        level (HierarchyChildParentLevel):
        id (str):
    """

    level: HierarchyChildParentLevel
    id: str

    def to_dict(self) -> dict[str, Any]:
        level = self.level.value

        id = self.id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "level": level,
                "id": id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        level = HierarchyChildParentLevel(d.pop("level"))

        id = d.pop("id")

        hierarchy_child_parent = cls(
            level=level,
            id=id,
        )

        return hierarchy_child_parent
