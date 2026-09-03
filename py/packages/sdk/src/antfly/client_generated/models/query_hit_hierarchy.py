from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_hit_hierarchy_level import QueryHitHierarchyLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_artifact import HierarchyArtifact
    from ..models.hierarchy_evidence import HierarchyEvidence
    from ..models.hierarchy_match_hit import HierarchyMatchHit
    from ..models.query_hit_hierarchy_ancestors import QueryHitHierarchyAncestors


T = TypeVar("T", bound="QueryHitHierarchy")


@_attrs_define
class QueryHitHierarchy:
    """
    Attributes:
        level (QueryHitHierarchyLevel | Unset):
        parent_doc_key (str | Unset): Source document key that owns this derived hit.
        parent_unit_id (str | Unset): Unit identifier when the hit is attached to a document unit.
        artifact (HierarchyArtifact | Unset):
        matched_artifact (HierarchyArtifact | Unset):
        ancestors (QueryHitHierarchyAncestors | Unset):
        evidence (HierarchyEvidence | Unset):
        matches (list[HierarchyMatchHit] | Unset): Matching descendant hits attached by the canonical hierarchy.group_by
            request.
        position (str | Unset): Opaque format-neutral position used for sequential hierarchy traversal.
        revision (str | Unset): Composite source hierarchy revision to which the position and cursor are bound.
        chunks (list[HierarchyMatchHit] | Unset): Deprecated child chunk hits included by the v0.2-compatible implicit
            source rollup.
    """

    level: QueryHitHierarchyLevel | Unset = UNSET
    parent_doc_key: str | Unset = UNSET
    parent_unit_id: str | Unset = UNSET
    artifact: HierarchyArtifact | Unset = UNSET
    matched_artifact: HierarchyArtifact | Unset = UNSET
    ancestors: QueryHitHierarchyAncestors | Unset = UNSET
    evidence: HierarchyEvidence | Unset = UNSET
    matches: list[HierarchyMatchHit] | Unset = UNSET
    position: str | Unset = UNSET
    revision: str | Unset = UNSET
    chunks: list[HierarchyMatchHit] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        level: str | Unset = UNSET
        if not isinstance(self.level, Unset):
            level = self.level.value

        parent_doc_key = self.parent_doc_key

        parent_unit_id = self.parent_unit_id

        artifact: dict[str, Any] | Unset = UNSET
        if not isinstance(self.artifact, Unset):
            artifact = self.artifact.to_dict()

        matched_artifact: dict[str, Any] | Unset = UNSET
        if not isinstance(self.matched_artifact, Unset):
            matched_artifact = self.matched_artifact.to_dict()

        ancestors: dict[str, Any] | Unset = UNSET
        if not isinstance(self.ancestors, Unset):
            ancestors = self.ancestors.to_dict()

        evidence: dict[str, Any] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = self.evidence.to_dict()

        matches: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.matches, Unset):
            matches = []
            for matches_item_data in self.matches:
                matches_item = matches_item_data.to_dict()
                matches.append(matches_item)

        position = self.position

        revision = self.revision

        chunks: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.chunks, Unset):
            chunks = []
            for chunks_item_data in self.chunks:
                chunks_item = chunks_item_data.to_dict()
                chunks.append(chunks_item)

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
        if matched_artifact is not UNSET:
            field_dict["matched_artifact"] = matched_artifact
        if ancestors is not UNSET:
            field_dict["ancestors"] = ancestors
        if evidence is not UNSET:
            field_dict["evidence"] = evidence
        if matches is not UNSET:
            field_dict["matches"] = matches
        if position is not UNSET:
            field_dict["position"] = position
        if revision is not UNSET:
            field_dict["revision"] = revision
        if chunks is not UNSET:
            field_dict["chunks"] = chunks

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_artifact import HierarchyArtifact
        from ..models.hierarchy_evidence import HierarchyEvidence
        from ..models.hierarchy_match_hit import HierarchyMatchHit
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

        _matched_artifact = d.pop("matched_artifact", UNSET)
        matched_artifact: HierarchyArtifact | Unset
        if isinstance(_matched_artifact, Unset):
            matched_artifact = UNSET
        else:
            matched_artifact = HierarchyArtifact.from_dict(_matched_artifact)

        _ancestors = d.pop("ancestors", UNSET)
        ancestors: QueryHitHierarchyAncestors | Unset
        if isinstance(_ancestors, Unset):
            ancestors = UNSET
        else:
            ancestors = QueryHitHierarchyAncestors.from_dict(_ancestors)

        _evidence = d.pop("evidence", UNSET)
        evidence: HierarchyEvidence | Unset
        if isinstance(_evidence, Unset):
            evidence = UNSET
        else:
            evidence = HierarchyEvidence.from_dict(_evidence)

        _matches = d.pop("matches", UNSET)
        matches: list[HierarchyMatchHit] | Unset = UNSET
        if _matches is not UNSET:
            matches = []
            for matches_item_data in _matches:
                matches_item = HierarchyMatchHit.from_dict(matches_item_data)

                matches.append(matches_item)

        position = d.pop("position", UNSET)

        revision = d.pop("revision", UNSET)

        _chunks = d.pop("chunks", UNSET)
        chunks: list[HierarchyMatchHit] | Unset = UNSET
        if _chunks is not UNSET:
            chunks = []
            for chunks_item_data in _chunks:
                chunks_item = HierarchyMatchHit.from_dict(chunks_item_data)

                chunks.append(chunks_item)

        query_hit_hierarchy = cls(
            level=level,
            parent_doc_key=parent_doc_key,
            parent_unit_id=parent_unit_id,
            artifact=artifact,
            matched_artifact=matched_artifact,
            ancestors=ancestors,
            evidence=evidence,
            matches=matches,
            position=position,
            revision=revision,
            chunks=chunks,
        )

        return query_hit_hierarchy
