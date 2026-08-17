from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.hierarchy_group_by_level import HierarchyGroupByLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_matches import HierarchyMatches


T = TypeVar("T", bound="HierarchyGroupBy")


@_attrs_define
class HierarchyGroupBy:
    """
    Attributes:
        level (HierarchyGroupByLevel): Hierarchy level used to group the records matched by the targeted index.
            Unit groups are relevance-ranked and do not accept `order_by`, `search_after`,
            or `search_before`; use `hierarchy.children` for sequential, cursor-paginated
            unit traversal.
        matches (HierarchyMatches | Unset):
    """

    level: HierarchyGroupByLevel
    matches: HierarchyMatches | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        level = self.level.value

        matches: dict[str, Any] | Unset = UNSET
        if not isinstance(self.matches, Unset):
            matches = self.matches.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "level": level,
            }
        )
        if matches is not UNSET:
            field_dict["matches"] = matches

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_matches import HierarchyMatches

        d = dict(src_dict)
        level = HierarchyGroupByLevel(d.pop("level"))

        _matches = d.pop("matches", UNSET)
        matches: HierarchyMatches | Unset
        if isinstance(_matches, Unset):
            matches = UNSET
        else:
            matches = HierarchyMatches.from_dict(_matches)

        hierarchy_group_by = cls(
            level=level,
            matches=matches,
        )

        return hierarchy_group_by
