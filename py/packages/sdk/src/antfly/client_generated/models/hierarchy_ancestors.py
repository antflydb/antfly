from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_projection import HierarchyProjection


T = TypeVar("T", bound="HierarchyAncestors")


@_attrs_define
class HierarchyAncestors:
    """
    Attributes:
        source (HierarchyProjection | Unset):
        unit (HierarchyProjection | Unset):
    """

    source: HierarchyProjection | Unset = UNSET
    unit: HierarchyProjection | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.to_dict()

        unit: dict[str, Any] | Unset = UNSET
        if not isinstance(self.unit, Unset):
            unit = self.unit.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if source is not UNSET:
            field_dict["source"] = source
        if unit is not UNSET:
            field_dict["unit"] = unit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_projection import HierarchyProjection

        d = dict(src_dict)
        _source = d.pop("source", UNSET)
        source: HierarchyProjection | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = HierarchyProjection.from_dict(_source)

        _unit = d.pop("unit", UNSET)
        unit: HierarchyProjection | Unset
        if isinstance(_unit, Unset):
            unit = UNSET
        else:
            unit = HierarchyProjection.from_dict(_unit)

        hierarchy_ancestors = cls(
            source=source,
            unit=unit,
        )

        return hierarchy_ancestors
