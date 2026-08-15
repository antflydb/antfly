from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_ancestors import HierarchyAncestors
    from ..models.hierarchy_group_by import HierarchyGroupBy


T = TypeVar("T", bound="QueryHierarchy")


@_attrs_define
class QueryHierarchy:
    """Returns direct index matches with optional projected ancestor context, or groups
    those matches at a hierarchy level through `group_by`. A group's nested `matches`
    projection is independently bounded and defaults to three hits while the top-level
    `limit` continues to control the number of groups.

    Ancestor and nested-match field projections are always explicit to keep response
    size predictable. The presence of this object selects the canonical contract:
    without `group_by`, including when the object is empty, direct index matches are
    returned. `ancestors` only controls projected context and never changes result
    cardinality. Omit `hierarchy` entirely to retain the legacy default result shape.

        Attributes:
            group_by (HierarchyGroupBy | Unset):
            ancestors (HierarchyAncestors | Unset):
    """

    group_by: HierarchyGroupBy | Unset = UNSET
    ancestors: HierarchyAncestors | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        group_by: dict[str, Any] | Unset = UNSET
        if not isinstance(self.group_by, Unset):
            group_by = self.group_by.to_dict()

        ancestors: dict[str, Any] | Unset = UNSET
        if not isinstance(self.ancestors, Unset):
            ancestors = self.ancestors.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if group_by is not UNSET:
            field_dict["group_by"] = group_by
        if ancestors is not UNSET:
            field_dict["ancestors"] = ancestors

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_ancestors import HierarchyAncestors
        from ..models.hierarchy_group_by import HierarchyGroupBy

        d = dict(src_dict)
        _group_by = d.pop("group_by", UNSET)
        group_by: HierarchyGroupBy | Unset
        if isinstance(_group_by, Unset):
            group_by = UNSET
        else:
            group_by = HierarchyGroupBy.from_dict(_group_by)

        _ancestors = d.pop("ancestors", UNSET)
        ancestors: HierarchyAncestors | Unset
        if isinstance(_ancestors, Unset):
            ancestors = UNSET
        else:
            ancestors = HierarchyAncestors.from_dict(_ancestors)

        query_hierarchy = cls(
            group_by=group_by,
            ancestors=ancestors,
        )

        return query_hierarchy
