from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.hierarchy_children_level import HierarchyChildrenLevel
from ..types import UNSET, Unset

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
            Member traversal requires artifact and seeks that producer's native storage
            prefix within the selected source. It includes generic assets and chunks.
            Scalar member payloads are returned as {"$": value} in _source so
            every query hit retains an object-shaped projection. Continuation
            requires the remote_snapshot returned with the first page.
        artifact (str | Unset): Producer name; required for member traversal and omitted for unit traversal.
    """

    parent: HierarchyChildParent
    level: HierarchyChildrenLevel
    artifact: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        parent = self.parent.to_dict()

        level = self.level.value

        artifact = self.artifact

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "parent": parent,
                "level": level,
            }
        )
        if artifact is not UNSET:
            field_dict["artifact"] = artifact

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_child_parent import HierarchyChildParent

        d = dict(src_dict)
        parent = HierarchyChildParent.from_dict(d.pop("parent"))

        level = HierarchyChildrenLevel(d.pop("level"))

        artifact = d.pop("artifact", UNSET)

        hierarchy_children = cls(
            parent=parent,
            level=level,
            artifact=artifact,
        )

        return hierarchy_children
