from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.hierarchy_children_level import HierarchyChildrenLevel

if TYPE_CHECKING:
    from ..models.hierarchy_child_parent import HierarchyChildParent


T = TypeVar("T", bound="HierarchyChildren")


@_attrs_define
class HierarchyChildren:
    """
    Attributes:
        parent (HierarchyChildParent):
        level (HierarchyChildrenLevel): Child level to enumerate. Unit traversal reads the versioned extraction
            hierarchy rather than a relevance index, so empty and failed units are included.
    """

    parent: HierarchyChildParent
    level: HierarchyChildrenLevel

    def to_dict(self) -> dict[str, Any]:
        parent = self.parent.to_dict()

        level = self.level.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "parent": parent,
                "level": level,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_child_parent import HierarchyChildParent

        d = dict(src_dict)
        parent = HierarchyChildParent.from_dict(d.pop("parent"))

        level = HierarchyChildrenLevel(d.pop("level"))

        hierarchy_children = cls(
            parent=parent,
            level=level,
        )

        return hierarchy_children
