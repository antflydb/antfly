from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_match_context import HierarchyMatchContext
    from ..models.hierarchy_match_hit_source import HierarchyMatchHitSource


T = TypeVar("T", bound="HierarchyMatchHit")


@_attrs_define
class HierarchyMatchHit:
    """
    Attributes:
        field_id (str):
        field_score (float): Relevance score, normalized so higher values always rank first.
        field_distance (float | Unset): Raw vector distance when this hit came directly from a dense-vector search;
            lower values are better.
        field_source (HierarchyMatchHitSource | Unset):
        hierarchy (HierarchyMatchContext | Unset):
    """

    field_id: str
    field_score: float
    field_distance: float | Unset = UNSET
    field_source: HierarchyMatchHitSource | Unset = UNSET
    hierarchy: HierarchyMatchContext | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        field_id = self.field_id

        field_score = self.field_score

        field_distance = self.field_distance

        field_source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_source, Unset):
            field_source = self.field_source.to_dict()

        hierarchy: dict[str, Any] | Unset = UNSET
        if not isinstance(self.hierarchy, Unset):
            hierarchy = self.hierarchy.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "_id": field_id,
                "_score": field_score,
            }
        )
        if field_distance is not UNSET:
            field_dict["_distance"] = field_distance
        if field_source is not UNSET:
            field_dict["_source"] = field_source
        if hierarchy is not UNSET:
            field_dict["hierarchy"] = hierarchy

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_match_context import HierarchyMatchContext
        from ..models.hierarchy_match_hit_source import HierarchyMatchHitSource

        d = dict(src_dict)
        field_id = d.pop("_id")

        field_score = d.pop("_score")

        field_distance = d.pop("_distance", UNSET)

        _field_source = d.pop("_source", UNSET)
        field_source: HierarchyMatchHitSource | Unset
        if isinstance(_field_source, Unset):
            field_source = UNSET
        else:
            field_source = HierarchyMatchHitSource.from_dict(_field_source)

        _hierarchy = d.pop("hierarchy", UNSET)
        hierarchy: HierarchyMatchContext | Unset
        if isinstance(_hierarchy, Unset):
            hierarchy = UNSET
        else:
            hierarchy = HierarchyMatchContext.from_dict(_hierarchy)

        hierarchy_match_hit = cls(
            field_id=field_id,
            field_score=field_score,
            field_distance=field_distance,
            field_source=field_source,
            hierarchy=hierarchy,
        )

        return hierarchy_match_hit
