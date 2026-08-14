from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_hit_hierarchy_level import QueryHitHierarchyLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_artifact import HierarchyArtifact
    from ..models.query_hit_hierarchy_ancestors import QueryHitHierarchyAncestors


T = TypeVar("T", bound="HierarchyMatchContext")


@_attrs_define
class HierarchyMatchContext:
    """
    Attributes:
        level (QueryHitHierarchyLevel | Unset):
        parent_doc_key (str | Unset):
        parent_unit_id (str | Unset):
        artifact (HierarchyArtifact | Unset):
        ancestors (QueryHitHierarchyAncestors | Unset):
    """

    level: QueryHitHierarchyLevel | Unset = UNSET
    parent_doc_key: str | Unset = UNSET
    parent_unit_id: str | Unset = UNSET
    artifact: HierarchyArtifact | Unset = UNSET
    ancestors: QueryHitHierarchyAncestors | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        level: str | Unset = UNSET
        if not isinstance(self.level, Unset):
            level = self.level.value

        parent_doc_key = self.parent_doc_key

        parent_unit_id = self.parent_unit_id

        artifact: dict[str, Any] | Unset = UNSET
        if not isinstance(self.artifact, Unset):
            artifact = self.artifact.to_dict()

        ancestors: dict[str, Any] | Unset = UNSET
        if not isinstance(self.ancestors, Unset):
            ancestors = self.ancestors.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if level is not UNSET:
            field_dict["level"] = level
        if parent_doc_key is not UNSET:
            field_dict["parent_doc_key"] = parent_doc_key
        if parent_unit_id is not UNSET:
            field_dict["parent_unit_id"] = parent_unit_id
        if artifact is not UNSET:
            field_dict["artifact"] = artifact
        if ancestors is not UNSET:
            field_dict["ancestors"] = ancestors

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_artifact import HierarchyArtifact
        from ..models.query_hit_hierarchy_ancestors import QueryHitHierarchyAncestors

        d = dict(src_dict)
        _level = d.pop("level", UNSET)
        level: QueryHitHierarchyLevel | Unset
        if isinstance(_level, Unset):
            level = UNSET
        else:
            level = QueryHitHierarchyLevel(_level)

        parent_doc_key = d.pop("parent_doc_key", UNSET)

        parent_unit_id = d.pop("parent_unit_id", UNSET)

        _artifact = d.pop("artifact", UNSET)
        artifact: HierarchyArtifact | Unset
        if isinstance(_artifact, Unset):
            artifact = UNSET
        else:
            artifact = HierarchyArtifact.from_dict(_artifact)

        _ancestors = d.pop("ancestors", UNSET)
        ancestors: QueryHitHierarchyAncestors | Unset
        if isinstance(_ancestors, Unset):
            ancestors = UNSET
        else:
            ancestors = QueryHitHierarchyAncestors.from_dict(_ancestors)

        hierarchy_match_context = cls(
            level=level,
            parent_doc_key=parent_doc_key,
            parent_unit_id=parent_unit_id,
            artifact=artifact,
            ancestors=ancestors,
        )

        return hierarchy_match_context
