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
        fields (list[str]): Fields to include in each child hit. This projection is required because
            source and child records commonly have different schemas. Use an empty
            array to return child identity and hierarchy metadata without stored fields.
        level (HierarchyChildrenLevel | Unset): Descendant level to attach to each returned source hit. Default:
            HierarchyChildrenLevel.CHUNK.
        limit (int | Unset): Maximum child hits attached to each parent, independent of the top-level query limit.
            Default: 3.
    """

    fields: list[str]
    level: HierarchyChildrenLevel | Unset = HierarchyChildrenLevel.CHUNK
    limit: int | Unset = 3

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields

        level: str | Unset = UNSET
        if not isinstance(self.level, Unset):
            level = self.level.value

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "fields": fields,
            }
        )
        if level is not UNSET:
            field_dict["level"] = level
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields"))

        _level = d.pop("level", UNSET)
        level: HierarchyChildrenLevel | Unset
        if isinstance(_level, Unset):
            level = UNSET
        else:
            level = HierarchyChildrenLevel(_level)

        limit = d.pop("limit", UNSET)

        hierarchy_children = cls(
            fields=fields,
            level=level,
            limit=limit,
        )

        return hierarchy_children
