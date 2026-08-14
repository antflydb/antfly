from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_hierarchy_include_item import QueryHierarchyIncludeItem
from ..models.query_hierarchy_return_level import QueryHierarchyReturnLevel
from ..models.query_hierarchy_rollup import QueryHierarchyRollup
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
            return_level (QueryHierarchyReturnLevel | Unset): Legacy result-shape control. Prefer group_by or ancestors.
            rollup (QueryHierarchyRollup | Unset):
            include (list[QueryHierarchyIncludeItem] | Unset):
            max_children_per_parent (int | Unset): Legacy child limit. Prefer group_by.matches.limit.
    """

    group_by: HierarchyGroupBy | Unset = UNSET
    ancestors: HierarchyAncestors | Unset = UNSET
    return_level: QueryHierarchyReturnLevel | Unset = UNSET
    rollup: QueryHierarchyRollup | Unset = UNSET
    include: list[QueryHierarchyIncludeItem] | Unset = UNSET
    max_children_per_parent: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        group_by: dict[str, Any] | Unset = UNSET
        if not isinstance(self.group_by, Unset):
            group_by = self.group_by.to_dict()

        ancestors: dict[str, Any] | Unset = UNSET
        if not isinstance(self.ancestors, Unset):
            ancestors = self.ancestors.to_dict()

        return_level: str | Unset = UNSET
        if not isinstance(self.return_level, Unset):
            return_level = self.return_level.value

        rollup: str | Unset = UNSET
        if not isinstance(self.rollup, Unset):
            rollup = self.rollup.value

        include: list[str] | Unset = UNSET
        if not isinstance(self.include, Unset):
            include = []
            for include_item_data in self.include:
                include_item = include_item_data.value
                include.append(include_item)

        max_children_per_parent = self.max_children_per_parent

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if group_by is not UNSET:
            field_dict["group_by"] = group_by
        if ancestors is not UNSET:
            field_dict["ancestors"] = ancestors
        if return_level is not UNSET:
            field_dict["return_level"] = return_level
        if rollup is not UNSET:
            field_dict["rollup"] = rollup
        if include is not UNSET:
            field_dict["include"] = include
        if max_children_per_parent is not UNSET:
            field_dict["max_children_per_parent"] = max_children_per_parent

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

        _return_level = d.pop("return_level", UNSET)
        return_level: QueryHierarchyReturnLevel | Unset
        if isinstance(_return_level, Unset):
            return_level = UNSET
        else:
            return_level = QueryHierarchyReturnLevel(_return_level)

        _rollup = d.pop("rollup", UNSET)
        rollup: QueryHierarchyRollup | Unset
        if isinstance(_rollup, Unset):
            rollup = UNSET
        else:
            rollup = QueryHierarchyRollup(_rollup)

        _include = d.pop("include", UNSET)
        include: list[QueryHierarchyIncludeItem] | Unset = UNSET
        if _include is not UNSET:
            include = []
            for include_item_data in _include:
                include_item = QueryHierarchyIncludeItem(include_item_data)

                include.append(include_item)

        max_children_per_parent = d.pop("max_children_per_parent", UNSET)

        query_hierarchy = cls(
            group_by=group_by,
            ancestors=ancestors,
            return_level=return_level,
            rollup=rollup,
            include=include,
            max_children_per_parent=max_children_per_parent,
        )

        return query_hierarchy
