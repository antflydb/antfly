from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_hierarchy_include_item import QueryHierarchyIncludeItem
from ..models.query_hierarchy_result_mode import QueryHierarchyResultMode
from ..models.query_hierarchy_return_level import QueryHierarchyReturnLevel
from ..models.query_hierarchy_rollup import QueryHierarchyRollup
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_ancestors import HierarchyAncestors
    from ..models.hierarchy_children import HierarchyChildren


T = TypeVar("T", bound="QueryHierarchy")


@_attrs_define
class QueryHierarchy:
    """Controls whether a query returns direct matches or source documents, optional
    bounded child hits, and projected ancestor context. `children` is analogous to a bounded nested
    inner-hit request: it defaults to three children per parent while the top-level
    `limit` continues to control the number of returned hits.

    The new controls are deliberately exclusive: `matches` may use `ancestors`,
    while `sources` may use `children`. When `result_mode` is omitted, `ancestors`
    implies `matches` and `children` implies `sources`. Ancestor and child field
    projections are always explicit to keep response size predictable.

    The legacy `return_level`, `rollup`, `include`, and
    `max_children_per_parent` fields remain supported in legacy-only requests.
    Do not mix legacy and new controls in one request.

        Attributes:
            result_mode (QueryHierarchyResultMode | Unset): Top-level result shape. `matches` returns the records matched by
                the
                targeted index; their actual hierarchy level is reported in each hit's
                `hierarchy.level`. `sources` groups matches by source document.
            children (HierarchyChildren | Unset):
            ancestors (HierarchyAncestors | Unset):
            return_level (QueryHierarchyReturnLevel | Unset): Legacy result-shape control. Prefer result_mode.
            rollup (QueryHierarchyRollup | Unset):
            include (list[QueryHierarchyIncludeItem] | Unset):
            max_children_per_parent (int | Unset): Legacy child limit. Prefer children.limit.
    """

    result_mode: QueryHierarchyResultMode | Unset = UNSET
    children: HierarchyChildren | Unset = UNSET
    ancestors: HierarchyAncestors | Unset = UNSET
    return_level: QueryHierarchyReturnLevel | Unset = UNSET
    rollup: QueryHierarchyRollup | Unset = UNSET
    include: list[QueryHierarchyIncludeItem] | Unset = UNSET
    max_children_per_parent: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        result_mode: str | Unset = UNSET
        if not isinstance(self.result_mode, Unset):
            result_mode = self.result_mode.value

        children: dict[str, Any] | Unset = UNSET
        if not isinstance(self.children, Unset):
            children = self.children.to_dict()

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
        if result_mode is not UNSET:
            field_dict["result_mode"] = result_mode
        if children is not UNSET:
            field_dict["children"] = children
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
        from ..models.hierarchy_children import HierarchyChildren

        d = dict(src_dict)
        _result_mode = d.pop("result_mode", UNSET)
        result_mode: QueryHierarchyResultMode | Unset
        if isinstance(_result_mode, Unset):
            result_mode = UNSET
        else:
            result_mode = QueryHierarchyResultMode(_result_mode)

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
            result_mode=result_mode,
            children=children,
            ancestors=ancestors,
            return_level=return_level,
            rollup=rollup,
            include=include,
            max_children_per_parent=max_children_per_parent,
        )

        return query_hierarchy
