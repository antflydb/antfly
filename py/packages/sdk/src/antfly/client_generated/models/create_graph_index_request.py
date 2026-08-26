from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.create_graph_index_request_type import CreateGraphIndexRequestType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.edge_type_config import EdgeTypeConfig
    from ..models.enrichment_config import EnrichmentConfig
    from ..models.generator_config import GeneratorConfig
    from ..models.graph_algebraic_planning_config import GraphAlgebraicPlanningConfig
    from ..models.graph_artifact_producer_config import GraphArtifactProducerConfig
    from ..models.graph_artifact_source_config import GraphArtifactSourceConfig
    from ..models.graph_resolver_config import GraphResolverConfig


T = TypeVar("T", bound="CreateGraphIndexRequest")


@_attrs_define
class CreateGraphIndexRequest:
    """Create a graph index.

    Attributes:
        type_ (CreateGraphIndexRequestType):
        description (str | Unset): Optional description of the index and its purpose
        version (int | Unset): Version of the index implementation. Defaults to 0. Default: 0.
        enrichments (list[EnrichmentConfig] | Unset): Inline managed enrichment definitions required by this index.
        sources (list[GraphArtifactSourceConfig] | Unset): Ordered chunk or JSON asset streams whose edge-like values
            are unioned into this graph index. Artifact names must be unique within the array because the artifact name is
            the source identity. Earlier sources win when multiple sources materialize the same edge identity. Requires
            index_capabilities.artifact_sources=true and is rejected by serverless deployments.
        summarizer (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
        template (str | Unset): Handlebars template for generating summarizer input text.
            Uses document fields as template variables.
            Same pattern as EmbeddingsConfig template.
             Example: {{title}}
            {{content}}.
        edge_types (list[EdgeTypeConfig] | Unset): List of edge types with their configurations
        max_edges_per_document (int | Unset): Maximum number of distinct visible edges materialized per document after
            source precedence and identity deduplication. Zero uses the server safety limit (currently 1,000,000).
            Independent aggregate reconciliation budgets bound work across overlapping source manifests.
        source (GraphArtifactSourceConfig | Unset): Artifact stream materialized into graph edges. Artifact-backed graph
            sources require index_capabilities.artifact_sources=true and are rejected by serverless deployments.
        artifact (GraphArtifactProducerConfig | Unset): Asset producer used by an artifact-backed graph index.
        algebraic_planning (GraphAlgebraicPlanningConfig | Unset): Optional algebraic planning features for graph
            traversal.
        resolvers (list[GraphResolverConfig] | Unset):
    """

    type_: CreateGraphIndexRequestType
    description: str | Unset = UNSET
    version: int | Unset = 0
    enrichments: list[EnrichmentConfig] | Unset = UNSET
    sources: list[GraphArtifactSourceConfig] | Unset = UNSET
    summarizer: GeneratorConfig | Unset = UNSET
    template: str | Unset = UNSET
    edge_types: list[EdgeTypeConfig] | Unset = UNSET
    max_edges_per_document: int | Unset = UNSET
    source: GraphArtifactSourceConfig | Unset = UNSET
    artifact: GraphArtifactProducerConfig | Unset = UNSET
    algebraic_planning: GraphAlgebraicPlanningConfig | Unset = UNSET
    resolvers: list[GraphResolverConfig] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        description = self.description

        version = self.version

        enrichments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.enrichments, Unset):
            enrichments = []
            for enrichments_item_data in self.enrichments:
                enrichments_item = enrichments_item_data.to_dict()
                enrichments.append(enrichments_item)

        sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = []
            for sources_item_data in self.sources:
                sources_item = sources_item_data.to_dict()
                sources.append(sources_item)

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

        source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.to_dict()

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
        field_dict.update(
            {
                "type": type_,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if version is not UNSET:
            field_dict["version"] = version
        if enrichments is not UNSET:
            field_dict["enrichments"] = enrichments
        if sources is not UNSET:
            field_dict["sources"] = sources
        if summarizer is not UNSET:
            field_dict["summarizer"] = summarizer
        if template is not UNSET:
            field_dict["template"] = template
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if max_edges_per_document is not UNSET:
            field_dict["max_edges_per_document"] = max_edges_per_document
        if source is not UNSET:
            field_dict["source"] = source
        if artifact is not UNSET:
            field_dict["artifact"] = artifact
        if algebraic_planning is not UNSET:
            field_dict["algebraic_planning"] = algebraic_planning
        if resolvers is not UNSET:
            field_dict["resolvers"] = resolvers

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.edge_type_config import EdgeTypeConfig
        from ..models.enrichment_config import EnrichmentConfig
        from ..models.generator_config import GeneratorConfig
        from ..models.graph_algebraic_planning_config import GraphAlgebraicPlanningConfig
        from ..models.graph_artifact_producer_config import GraphArtifactProducerConfig
        from ..models.graph_artifact_source_config import GraphArtifactSourceConfig
        from ..models.graph_resolver_config import GraphResolverConfig

        d = dict(src_dict)
        type_ = CreateGraphIndexRequestType(d.pop("type"))

        description = d.pop("description", UNSET)

        version = d.pop("version", UNSET)

        _enrichments = d.pop("enrichments", UNSET)
        enrichments: list[EnrichmentConfig] | Unset = UNSET
        if _enrichments is not UNSET:
            enrichments = []
            for enrichments_item_data in _enrichments:
                enrichments_item = EnrichmentConfig.from_dict(enrichments_item_data)

                enrichments.append(enrichments_item)

        _sources = d.pop("sources", UNSET)
        sources: list[GraphArtifactSourceConfig] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = GraphArtifactSourceConfig.from_dict(sources_item_data)

                sources.append(sources_item)

        _summarizer = d.pop("summarizer", UNSET)
        summarizer: GeneratorConfig | Unset
        if isinstance(_summarizer, Unset):
            summarizer = UNSET
        else:
            summarizer = GeneratorConfig.from_dict(_summarizer)

        template = d.pop("template", UNSET)

        _edge_types = d.pop("edge_types", UNSET)
        edge_types: list[EdgeTypeConfig] | Unset = UNSET
        if _edge_types is not UNSET:
            edge_types = []
            for edge_types_item_data in _edge_types:
                edge_types_item = EdgeTypeConfig.from_dict(edge_types_item_data)

                edge_types.append(edge_types_item)

        max_edges_per_document = d.pop("max_edges_per_document", UNSET)

        _source = d.pop("source", UNSET)
        source: GraphArtifactSourceConfig | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = GraphArtifactSourceConfig.from_dict(_source)

        _artifact = d.pop("artifact", UNSET)
        artifact: GraphArtifactProducerConfig | Unset
        if isinstance(_artifact, Unset):
            artifact = UNSET
        else:
            artifact = GraphArtifactProducerConfig.from_dict(_artifact)

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

        create_graph_index_request = cls(
            type_=type_,
            description=description,
            version=version,
            enrichments=enrichments,
            sources=sources,
            summarizer=summarizer,
            template=template,
            edge_types=edge_types,
            max_edges_per_document=max_edges_per_document,
            source=source,
            artifact=artifact,
            algebraic_planning=algebraic_planning,
            resolvers=resolvers,
        )

        create_graph_index_request.additional_properties = d
        return create_graph_index_request

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
