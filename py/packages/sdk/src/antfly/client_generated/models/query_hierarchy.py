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
    from ..models.hierarchy_children import HierarchyChildren


T = TypeVar("T", bound="QueryHierarchy")


@_attrs_define
class QueryHierarchy:
    """Controls the hierarchy level returned by a query, optional bounded child hits,
    and projected ancestor context. `children` is analogous to a bounded nested
    inner-hit request: it defaults to three children per parent while the top-level
    `limit` continues to control the number of returned hits.

    The legacy `rollup`, `include`, and `max_children_per_parent` fields remain
    supported for compatibility. Prefer `children` and `ancestors` for new clients.

        Attributes:
            return_level (QueryHierarchyReturnLevel | Unset): Hierarchy level represented by each top-level hit.
            children (HierarchyChildren | Unset):
            ancestors (HierarchyAncestors | Unset):
            rollup (QueryHierarchyRollup | Unset):
            include (list[QueryHierarchyIncludeItem] | Unset):
            max_children_per_parent (int | Unset): Legacy child limit. Prefer children.limit.
    """

    return_level: QueryHierarchyReturnLevel | Unset = UNSET
    children: HierarchyChildren | Unset = UNSET
    ancestors: HierarchyAncestors | Unset = UNSET
    rollup: QueryHierarchyRollup | Unset = UNSET
    include: list[QueryHierarchyIncludeItem] | Unset = UNSET
    max_children_per_parent: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        return_level: str | Unset = UNSET
        if not isinstance(self.return_level, Unset):
            return_level = self.return_level.value

        children: dict[str, Any] | Unset = UNSET
        if not isinstance(self.children, Unset):
            children = self.children.to_dict()

        ancestors: dict[str, Any] | Unset = UNSET
        if not isinstance(self.ancestors, Unset):
            ancestors = self.ancestors.to_dict()

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
        if return_level is not UNSET:
            field_dict["return_level"] = return_level
        if children is not UNSET:
            field_dict["children"] = children
        if ancestors is not UNSET:
            field_dict["ancestors"] = ancestors
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
        from ..models.hierarchy_children import HierarchyChildren

        d = dict(src_dict)
        _return_level = d.pop("return_level", UNSET)
        return_level: QueryHierarchyReturnLevel | Unset
        if isinstance(_return_level, Unset):
            return_level = UNSET
        else:
            return_level = QueryHierarchyReturnLevel(_return_level)

        _children = d.pop("children", UNSET)
        children: HierarchyChildren | Unset
        if isinstance(_children, Unset):
            children = UNSET
        else:
            children = HierarchyChildren.from_dict(_children)

        _ancestors = d.pop("ancestors", UNSET)
        ancestors: HierarchyAncestors | Unset
        if isinstance(_ancestors, Unset):
            ancestors = UNSET
        else:
            ancestors = HierarchyAncestors.from_dict(_ancestors)

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
            return_level=return_level,
            children=children,
            ancestors=ancestors,
            rollup=rollup,
            include=include,
            max_children_per_parent=max_children_per_parent,
        )

        return query_hierarchy
