from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.created_graph_artifact_producer_config import CreatedGraphArtifactProducerConfig
    from ..models.created_provider_config import CreatedProviderConfig
    from ..models.edge_type_config import EdgeTypeConfig
    from ..models.graph_algebraic_planning_config import GraphAlgebraicPlanningConfig
    from ..models.graph_artifact_context_config import GraphArtifactContextConfig
    from ..models.graph_artifact_edge_mapping_config import GraphArtifactEdgeMappingConfig
    from ..models.graph_artifact_node_mapping_config import GraphArtifactNodeMappingConfig
    from ..models.graph_artifact_source_config import GraphArtifactSourceConfig
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
        sources (list[GraphArtifactSourceConfig] | Unset):
        source (GraphArtifactSourceConfig | Unset): Artifact stream materialized into graph edges.
        artifact (CreatedGraphArtifactProducerConfig | Unset): Credential-free graph artifact producer configuration
            returned after creation.
        nodes (GraphArtifactNodeMappingConfig | Unset): Maps each artifact item to graph node identifiers.
        edge (GraphArtifactEdgeMappingConfig | Unset): Maps each artifact item to an edge type, weight, and public
            metadata.
        context (GraphArtifactContextConfig | Unset): Document fields made available to graph mapping templates through
            `_doc.value`.
        algebraic_planning (GraphAlgebraicPlanningConfig | Unset): Optional algebraic planning features for graph
            traversal.
        resolvers (list[GraphResolverConfig] | Unset):
    """

    summarizer: CreatedProviderConfig | Unset = UNSET
    template: str | Unset = UNSET
    edge_types: list[EdgeTypeConfig] | Unset = UNSET
    max_edges_per_document: int | Unset = UNSET
    sources: list[GraphArtifactSourceConfig] | Unset = UNSET
    source: GraphArtifactSourceConfig | Unset = UNSET
    artifact: CreatedGraphArtifactProducerConfig | Unset = UNSET
    nodes: GraphArtifactNodeMappingConfig | Unset = UNSET
    edge: GraphArtifactEdgeMappingConfig | Unset = UNSET
    context: GraphArtifactContextConfig | Unset = UNSET
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

        source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.to_dict()

        artifact: dict[str, Any] | Unset = UNSET
        if not isinstance(self.artifact, Unset):
            artifact = self.artifact.to_dict()

        nodes: dict[str, Any] | Unset = UNSET
        if not isinstance(self.nodes, Unset):
            nodes = self.nodes.to_dict()

        edge: dict[str, Any] | Unset = UNSET
        if not isinstance(self.edge, Unset):
            edge = self.edge.to_dict()

        context: dict[str, Any] | Unset = UNSET
        if not isinstance(self.context, Unset):
            context = self.context.to_dict()

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
        if source is not UNSET:
            field_dict["source"] = source
        if artifact is not UNSET:
            field_dict["artifact"] = artifact
        if nodes is not UNSET:
            field_dict["nodes"] = nodes
        if edge is not UNSET:
            field_dict["edge"] = edge
        if context is not UNSET:
            field_dict["context"] = context
        if algebraic_planning is not UNSET:
            field_dict["algebraic_planning"] = algebraic_planning
        if resolvers is not UNSET:
            field_dict["resolvers"] = resolvers

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.created_graph_artifact_producer_config import CreatedGraphArtifactProducerConfig
        from ..models.created_provider_config import CreatedProviderConfig
        from ..models.edge_type_config import EdgeTypeConfig
        from ..models.graph_algebraic_planning_config import GraphAlgebraicPlanningConfig
        from ..models.graph_artifact_context_config import GraphArtifactContextConfig
        from ..models.graph_artifact_edge_mapping_config import GraphArtifactEdgeMappingConfig
        from ..models.graph_artifact_node_mapping_config import GraphArtifactNodeMappingConfig
        from ..models.graph_artifact_source_config import GraphArtifactSourceConfig
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
        sources: list[GraphArtifactSourceConfig] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = GraphArtifactSourceConfig.from_dict(sources_item_data)

                sources.append(sources_item)

        _source = d.pop("source", UNSET)
        source: GraphArtifactSourceConfig | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = GraphArtifactSourceConfig.from_dict(_source)

        _artifact = d.pop("artifact", UNSET)
        artifact: CreatedGraphArtifactProducerConfig | Unset
        if isinstance(_artifact, Unset):
            artifact = UNSET
        else:
            artifact = CreatedGraphArtifactProducerConfig.from_dict(_artifact)

        _nodes = d.pop("nodes", UNSET)
        nodes: GraphArtifactNodeMappingConfig | Unset
        if isinstance(_nodes, Unset):
            nodes = UNSET
        else:
            nodes = GraphArtifactNodeMappingConfig.from_dict(_nodes)

        _edge = d.pop("edge", UNSET)
        edge: GraphArtifactEdgeMappingConfig | Unset
        if isinstance(_edge, Unset):
            edge = UNSET
        else:
            edge = GraphArtifactEdgeMappingConfig.from_dict(_edge)

        _context = d.pop("context", UNSET)
        context: GraphArtifactContextConfig | Unset
        if isinstance(_context, Unset):
            context = UNSET
        else:
            context = GraphArtifactContextConfig.from_dict(_context)

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
            source=source,
            artifact=artifact,
            nodes=nodes,
            edge=edge,
            context=context,
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
