from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_ancestor import HierarchyAncestor


T = TypeVar("T", bound="QueryHitHierarchyAncestors")


@_attrs_define
class QueryHitHierarchyAncestors:
    """
    Attributes:
        source (HierarchyAncestor):
        unit (HierarchyAncestor | Unset):
    """

    source: HierarchyAncestor
    unit: HierarchyAncestor | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        source = self.source.to_dict()

        unit: dict[str, Any] | Unset = UNSET
        if not isinstance(self.unit, Unset):
            unit = self.unit.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "source": source,
            }
        )
        if unit is not UNSET:
            field_dict["unit"] = unit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_ancestor import HierarchyAncestor

        d = dict(src_dict)
        source = HierarchyAncestor.from_dict(d.pop("source"))

        _unit = d.pop("unit", UNSET)
        unit: HierarchyAncestor | Unset
        if isinstance(_unit, Unset):
            unit = UNSET
        else:
            unit = HierarchyAncestor.from_dict(_unit)

        query_hit_hierarchy_ancestors = cls(
            source=source,
            unit=unit,
        )

        return query_hit_hierarchy_ancestors
