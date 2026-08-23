from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_resolver_config_candidate_search import GraphResolverConfigCandidateSearch
from ..models.graph_resolver_config_fusion_combine import GraphResolverConfigFusionCombine
from ..models.graph_resolver_config_source_artifact_kind import GraphResolverConfigSourceArtifactKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphResolverConfig")


@_attrs_define
class GraphResolverConfig:
    """Versioned entity resolver attached to an artifact-backed graph index.

    Attributes:
        name (str):
        table (str):
        source_artifact (str):
        resolution_artifact (str):
        key_template (str):
        source_artifact_kind (GraphResolverConfigSourceArtifactKind | Unset):  Default:
            GraphResolverConfigSourceArtifactKind.ASSET.
        type_must_match (bool | Unset):  Default: True.
        scorer_json (str | Unset):
        candidate_search (GraphResolverConfigCandidateSearch | Unset):
        candidate_ann_index (str | Unset):
        candidate_limit (int | Unset):
        name_embedding (str | Unset):
        name_embedding_dims (int | Unset):
        fusion_combine (GraphResolverConfigFusionCombine | Unset):
        fusion_trust (float | Unset):
        fusion_prior (float | Unset):
        fusion_prior_weight (float | Unset):
        config_generation (int | Unset):
    """

    name: str
    table: str
    source_artifact: str
    resolution_artifact: str
    key_template: str
    source_artifact_kind: GraphResolverConfigSourceArtifactKind | Unset = GraphResolverConfigSourceArtifactKind.ASSET
    type_must_match: bool | Unset = True
    scorer_json: str | Unset = UNSET
    candidate_search: GraphResolverConfigCandidateSearch | Unset = UNSET
    candidate_ann_index: str | Unset = UNSET
    candidate_limit: int | Unset = UNSET
    name_embedding: str | Unset = UNSET
    name_embedding_dims: int | Unset = UNSET
    fusion_combine: GraphResolverConfigFusionCombine | Unset = UNSET
    fusion_trust: float | Unset = UNSET
    fusion_prior: float | Unset = UNSET
    fusion_prior_weight: float | Unset = UNSET
    config_generation: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        table = self.table

        source_artifact = self.source_artifact

        resolution_artifact = self.resolution_artifact

        key_template = self.key_template

        source_artifact_kind: str | Unset = UNSET
        if not isinstance(self.source_artifact_kind, Unset):
            source_artifact_kind = self.source_artifact_kind.value

        type_must_match = self.type_must_match

        scorer_json = self.scorer_json

        candidate_search: str | Unset = UNSET
        if not isinstance(self.candidate_search, Unset):
            candidate_search = self.candidate_search.value

        candidate_ann_index = self.candidate_ann_index

        candidate_limit = self.candidate_limit

        name_embedding = self.name_embedding

        name_embedding_dims = self.name_embedding_dims

        fusion_combine: str | Unset = UNSET
        if not isinstance(self.fusion_combine, Unset):
            fusion_combine = self.fusion_combine.value

        fusion_trust = self.fusion_trust

        fusion_prior = self.fusion_prior

        fusion_prior_weight = self.fusion_prior_weight

        config_generation = self.config_generation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "table": table,
                "source_artifact": source_artifact,
                "resolution_artifact": resolution_artifact,
                "key_template": key_template,
            }
        )
        if source_artifact_kind is not UNSET:
            field_dict["source_artifact_kind"] = source_artifact_kind
        if type_must_match is not UNSET:
            field_dict["type_must_match"] = type_must_match
        if scorer_json is not UNSET:
            field_dict["scorer_json"] = scorer_json
        if candidate_search is not UNSET:
            field_dict["candidate_search"] = candidate_search
        if candidate_ann_index is not UNSET:
            field_dict["candidate_ann_index"] = candidate_ann_index
        if candidate_limit is not UNSET:
            field_dict["candidate_limit"] = candidate_limit
        if name_embedding is not UNSET:
            field_dict["name_embedding"] = name_embedding
        if name_embedding_dims is not UNSET:
            field_dict["name_embedding_dims"] = name_embedding_dims
        if fusion_combine is not UNSET:
            field_dict["fusion_combine"] = fusion_combine
        if fusion_trust is not UNSET:
            field_dict["fusion_trust"] = fusion_trust
        if fusion_prior is not UNSET:
            field_dict["fusion_prior"] = fusion_prior
        if fusion_prior_weight is not UNSET:
            field_dict["fusion_prior_weight"] = fusion_prior_weight
        if config_generation is not UNSET:
            field_dict["config_generation"] = config_generation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        table = d.pop("table")

        source_artifact = d.pop("source_artifact")

        resolution_artifact = d.pop("resolution_artifact")

        key_template = d.pop("key_template")

        _source_artifact_kind = d.pop("source_artifact_kind", UNSET)
        source_artifact_kind: GraphResolverConfigSourceArtifactKind | Unset
        if isinstance(_source_artifact_kind, Unset):
            source_artifact_kind = UNSET
        else:
            source_artifact_kind = GraphResolverConfigSourceArtifactKind(_source_artifact_kind)

        type_must_match = d.pop("type_must_match", UNSET)

        scorer_json = d.pop("scorer_json", UNSET)

        _candidate_search = d.pop("candidate_search", UNSET)
        candidate_search: GraphResolverConfigCandidateSearch | Unset
        if isinstance(_candidate_search, Unset):
            candidate_search = UNSET
        else:
            candidate_search = GraphResolverConfigCandidateSearch(_candidate_search)

        candidate_ann_index = d.pop("candidate_ann_index", UNSET)

        candidate_limit = d.pop("candidate_limit", UNSET)

        name_embedding = d.pop("name_embedding", UNSET)

        name_embedding_dims = d.pop("name_embedding_dims", UNSET)

        _fusion_combine = d.pop("fusion_combine", UNSET)
        fusion_combine: GraphResolverConfigFusionCombine | Unset
        if isinstance(_fusion_combine, Unset):
            fusion_combine = UNSET
        else:
            fusion_combine = GraphResolverConfigFusionCombine(_fusion_combine)

        fusion_trust = d.pop("fusion_trust", UNSET)

        fusion_prior = d.pop("fusion_prior", UNSET)

        fusion_prior_weight = d.pop("fusion_prior_weight", UNSET)

        config_generation = d.pop("config_generation", UNSET)

        graph_resolver_config = cls(
            name=name,
            table=table,
            source_artifact=source_artifact,
            resolution_artifact=resolution_artifact,
            key_template=key_template,
            source_artifact_kind=source_artifact_kind,
            type_must_match=type_must_match,
            scorer_json=scorer_json,
            candidate_search=candidate_search,
            candidate_ann_index=candidate_ann_index,
            candidate_limit=candidate_limit,
            name_embedding=name_embedding,
            name_embedding_dims=name_embedding_dims,
            fusion_combine=fusion_combine,
            fusion_trust=fusion_trust,
            fusion_prior=fusion_prior,
            fusion_prior_weight=fusion_prior_weight,
            config_generation=config_generation,
        )

        return graph_resolver_config
