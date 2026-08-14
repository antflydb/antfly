from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_hit_hierarchy_level import QueryHitHierarchyLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_hit_hierarchy_ancestors import QueryHitHierarchyAncestors
    from ..models.query_hit_hierarchy_artifact import QueryHitHierarchyArtifact
    from ..models.query_hit_hierarchy_chunks_item import QueryHitHierarchyChunksItem
    from ..models.query_hit_hierarchy_matches_item import QueryHitHierarchyMatchesItem


T = TypeVar("T", bound="QueryHitHierarchy")


@_attrs_define
class QueryHitHierarchy:
    """Stable ancestry envelope for derived document hierarchy hits. Present when
    the hit is a derived unit/chunk/embedding artifact or when a source-level
    group includes nested matches. Standard fields include `level`,
    `parent_doc_key`, optional `parent_unit_id`, `artifact`, `matches`, and
    `ancestors` with response-local or requested DB-backed source/unit context when available.
    Legacy rollup requests continue to use `chunks` instead of `matches`.

        Attributes:
            level (QueryHitHierarchyLevel | Unset): Hierarchy level represented by this hit.
            parent_doc_key (str | Unset): Source document key that owns this derived hit.
            parent_unit_id (str | Unset): Unit identifier when the hit is attached to a document unit.
            artifact (QueryHitHierarchyArtifact | Unset): Artifact identity with `name`, `kind`, and optional `unit_id` or
                `chunk_id`.
            ancestors (QueryHitHierarchyAncestors | Unset): Ancestor context. Includes `source.id` for derived hits,
                `source.document` for materialized source rollups or requested source hydration, and `unit.document` for direct
                unit hits or requested unit hydration when the unit payload is present.
            matches (list[QueryHitHierarchyMatchesItem] | Unset): Matching descendant hits attached by the canonical
                hierarchy.group_by request.
            chunks (list[QueryHitHierarchyChunksItem] | Unset): Legacy child chunk hits included for source-level rollups.
    """

    level: QueryHitHierarchyLevel | Unset = UNSET
    parent_doc_key: str | Unset = UNSET
    parent_unit_id: str | Unset = UNSET
    artifact: QueryHitHierarchyArtifact | Unset = UNSET
    ancestors: QueryHitHierarchyAncestors | Unset = UNSET
    matches: list[QueryHitHierarchyMatchesItem] | Unset = UNSET
    chunks: list[QueryHitHierarchyChunksItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

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

        matches: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.matches, Unset):
            matches = []
            for matches_item_data in self.matches:
                matches_item = matches_item_data.to_dict()
                matches.append(matches_item)

        chunks: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.chunks, Unset):
            chunks = []
            for chunks_item_data in self.chunks:
                chunks_item = chunks_item_data.to_dict()
                chunks.append(chunks_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
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
        if matches is not UNSET:
            field_dict["matches"] = matches
        if chunks is not UNSET:
            field_dict["chunks"] = chunks

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_hit_hierarchy_ancestors import QueryHitHierarchyAncestors
        from ..models.query_hit_hierarchy_artifact import QueryHitHierarchyArtifact
        from ..models.query_hit_hierarchy_chunks_item import QueryHitHierarchyChunksItem
        from ..models.query_hit_hierarchy_matches_item import QueryHitHierarchyMatchesItem

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
        artifact: QueryHitHierarchyArtifact | Unset
        if isinstance(_artifact, Unset):
            artifact = UNSET
        else:
            artifact = QueryHitHierarchyArtifact.from_dict(_artifact)

        _ancestors = d.pop("ancestors", UNSET)
        ancestors: QueryHitHierarchyAncestors | Unset
        if isinstance(_ancestors, Unset):
            ancestors = UNSET
        else:
            ancestors = QueryHitHierarchyAncestors.from_dict(_ancestors)

        _matches = d.pop("matches", UNSET)
        matches: list[QueryHitHierarchyMatchesItem] | Unset = UNSET
        if _matches is not UNSET:
            matches = []
            for matches_item_data in _matches:
                matches_item = QueryHitHierarchyMatchesItem.from_dict(matches_item_data)

                matches.append(matches_item)

        _chunks = d.pop("chunks", UNSET)
        chunks: list[QueryHitHierarchyChunksItem] | Unset = UNSET
        if _chunks is not UNSET:
            chunks = []
            for chunks_item_data in _chunks:
                chunks_item = QueryHitHierarchyChunksItem.from_dict(chunks_item_data)

                chunks.append(chunks_item)

        query_hit_hierarchy = cls(
            level=level,
            parent_doc_key=parent_doc_key,
            parent_unit_id=parent_unit_id,
            artifact=artifact,
            ancestors=ancestors,
            matches=matches,
            chunks=chunks,
        )

        query_hit_hierarchy.additional_properties = d
        return query_hit_hierarchy

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
