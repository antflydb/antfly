from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.created_graph_artifact_producer_config import CreatedGraphArtifactProducerConfig
    from ..models.created_graph_artifact_source_config import CreatedGraphArtifactSourceConfig
    from ..models.created_provider_config import CreatedProviderConfig
    from ..models.edge_type_config import EdgeTypeConfig
    from ..models.graph_algebraic_planning_config import GraphAlgebraicPlanningConfig
    from ..models.graph_resolver_config import GraphResolverConfig


T = TypeVar("T", bound="CreatedGraphIndexConfig")


@_attrs_define
class CreatedGraphIndexConfig:
    """Credential-free normalized graph configuration returned after creation.

    Attributes:
        summarizer (CreatedProviderConfig | Unset): Credential-free provider configuration returned after index
            creation. Only non-secret provider settings are represented.
        template (str | Unset):
        edge_types (list[EdgeTypeConfig] | Unset):
        max_edges_per_document (int | Unset):
        sources (list[CreatedGraphArtifactSourceConfig] | Unset):
        artifact (CreatedGraphArtifactProducerConfig | Unset): Credential-free graph artifact producer configuration
            returned after creation.
        algebraic_planning (GraphAlgebraicPlanningConfig | Unset): Optional algebraic planning features for graph
            traversal.
        resolvers (list[GraphResolverConfig] | Unset):
    """

    summarizer: CreatedProviderConfig | Unset = UNSET
    template: str | Unset = UNSET
    edge_types: list[EdgeTypeConfig] | Unset = UNSET
    max_edges_per_document: int | Unset = UNSET
    sources: list[CreatedGraphArtifactSourceConfig] | Unset = UNSET
    artifact: CreatedGraphArtifactProducerConfig | Unset = UNSET
    algebraic_planning: GraphAlgebraicPlanningConfig | Unset = UNSET
    resolvers: list[GraphResolverConfig] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        summarizer: dict[str, Any] | Unset = UNSET
        if not isinstance(self.summarizer, Unset):
            summarizer = self.summarizer.to_dict()

        template = self.template

        edge_types: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = []
            for edge_types_item_data in self.edge_types:
                edge_types_item = edge_types_item_data.to_dict()
                edge_types.append(edge_types_item)

        max_edges_per_document = self.max_edges_per_document

        sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = []
            for sources_item_data in self.sources:
                sources_item = sources_item_data.to_dict()
                sources.append(sources_item)

        artifact: dict[str, Any] | Unset = UNSET
        if not isinstance(self.artifact, Unset):
            artifact = self.artifact.to_dict()

        algebraic_planning: dict[str, Any] | Unset = UNSET
        if not isinstance(self.algebraic_planning, Unset):
            algebraic_planning = self.algebraic_planning.to_dict()

        resolvers: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.resolvers, Unset):
            resolvers = []
            for resolvers_item_data in self.resolvers:
                resolvers_item = resolvers_item_data.to_dict()
                resolvers.append(resolvers_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if summarizer is not UNSET:
            field_dict["summarizer"] = summarizer
        if template is not UNSET:
            field_dict["template"] = template
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if max_edges_per_document is not UNSET:
            field_dict["max_edges_per_document"] = max_edges_per_document
        if sources is not UNSET:
            field_dict["sources"] = sources
        if artifact is not UNSET:
            field_dict["artifact"] = artifact
        if algebraic_planning is not UNSET:
            field_dict["algebraic_planning"] = algebraic_planning
        if resolvers is not UNSET:
            field_dict["resolvers"] = resolvers

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.created_graph_artifact_producer_config import CreatedGraphArtifactProducerConfig
        from ..models.created_graph_artifact_source_config import CreatedGraphArtifactSourceConfig
        from ..models.created_provider_config import CreatedProviderConfig
        from ..models.edge_type_config import EdgeTypeConfig
        from ..models.graph_algebraic_planning_config import GraphAlgebraicPlanningConfig
        from ..models.graph_resolver_config import GraphResolverConfig

        d = dict(src_dict)
        _summarizer = d.pop("summarizer", UNSET)
        summarizer: CreatedProviderConfig | Unset
        if isinstance(_summarizer, Unset):
            summarizer = UNSET
        else:
            summarizer = CreatedProviderConfig.from_dict(_summarizer)

        template = d.pop("template", UNSET)

        _edge_types = d.pop("edge_types", UNSET)
        edge_types: list[EdgeTypeConfig] | Unset = UNSET
        if _edge_types is not UNSET:
            edge_types = []
            for edge_types_item_data in _edge_types:
                edge_types_item = EdgeTypeConfig.from_dict(edge_types_item_data)

                edge_types.append(edge_types_item)

        max_edges_per_document = d.pop("max_edges_per_document", UNSET)

        _sources = d.pop("sources", UNSET)
        sources: list[CreatedGraphArtifactSourceConfig] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = CreatedGraphArtifactSourceConfig.from_dict(sources_item_data)

                sources.append(sources_item)

        _artifact = d.pop("artifact", UNSET)
        artifact: CreatedGraphArtifactProducerConfig | Unset
        if isinstance(_artifact, Unset):
            artifact = UNSET
        else:
            artifact = CreatedGraphArtifactProducerConfig.from_dict(_artifact)

        _algebraic_planning = d.pop("algebraic_planning", UNSET)
        algebraic_planning: GraphAlgebraicPlanningConfig | Unset
        if isinstance(_algebraic_planning, Unset):
            algebraic_planning = UNSET
        else:
            algebraic_planning = GraphAlgebraicPlanningConfig.from_dict(_algebraic_planning)

        _resolvers = d.pop("resolvers", UNSET)
        resolvers: list[GraphResolverConfig] | Unset = UNSET
        if _resolvers is not UNSET:
            resolvers = []
            for resolvers_item_data in _resolvers:
                resolvers_item = GraphResolverConfig.from_dict(resolvers_item_data)

                resolvers.append(resolvers_item)

        created_graph_index_config = cls(
            summarizer=summarizer,
            template=template,
            edge_types=edge_types,
            max_edges_per_document=max_edges_per_document,
            sources=sources,
            artifact=artifact,
            algebraic_planning=algebraic_planning,
            resolvers=resolvers,
        )

        created_graph_index_config.additional_properties = d
        return created_graph_index_config

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
