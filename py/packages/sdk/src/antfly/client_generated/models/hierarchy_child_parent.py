from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.hierarchy_child_parent_level import HierarchyChildParentLevel
from ..types import UNSET, Unset

T = TypeVar("T", bound="HierarchyChildParent")


@_attrs_define
class HierarchyChildParent:
    """
    Attributes:
        level (HierarchyChildParentLevel):
        id (str | Unset):
        ids (list[str] | Unset): Bounded source selection for member traversal; unit traversal requires id.
    """

    level: HierarchyChildParentLevel
    id: str | Unset = UNSET
    ids: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        level = self.level.value

        id = self.id

        ids: list[str] | Unset = UNSET
        if not isinstance(self.ids, Unset):
            ids = self.ids

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "level": level,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if ids is not UNSET:
            field_dict["ids"] = ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        level = HierarchyChildParentLevel(d.pop("level"))

        id = d.pop("id", UNSET)

        ids = cast(list[str], d.pop("ids", UNSET))

        hierarchy_child_parent = cls(
            level=level,
            id=id,
            ids=ids,
        )

        return hierarchy_child_parent
