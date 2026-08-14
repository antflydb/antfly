from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.hierarchy_children_level import HierarchyChildrenLevel
from ..types import UNSET, Unset

T = TypeVar("T", bound="HierarchyChildren")


@_attrs_define
class HierarchyChildren:
    """
    Attributes:
        level (HierarchyChildrenLevel | Unset): Descendant level to attach to each returned source hit. Default:
            HierarchyChildrenLevel.CHUNK.
        limit (int | Unset): Maximum child hits attached to each parent, independent of the top-level query limit.
            Default: 3.
        fields (list[str] | Unset): Fields to include in each child hit. Omit to use the top-level fields projection.
    """

    level: HierarchyChildrenLevel | Unset = HierarchyChildrenLevel.CHUNK
    limit: int | Unset = 3
    fields: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        level: str | Unset = UNSET
        if not isinstance(self.level, Unset):
            level = self.level.value

        limit = self.limit

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if level is not UNSET:
            field_dict["level"] = level
        if limit is not UNSET:
            field_dict["limit"] = limit
        if fields is not UNSET:
            field_dict["fields"] = fields

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _level = d.pop("level", UNSET)
        level: HierarchyChildrenLevel | Unset
        if isinstance(_level, Unset):
            level = UNSET
        else:
            level = HierarchyChildrenLevel(_level)

        limit = d.pop("limit", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        hierarchy_children = cls(
            level=level,
            limit=limit,
            fields=fields,
        )

        return hierarchy_children
