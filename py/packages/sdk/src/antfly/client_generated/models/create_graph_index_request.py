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


T = TypeVar("T", bound="CreateGraphIndexRequest")


@_attrs_define
class CreateGraphIndexRequest:
    """Create a graph index.

    Attributes:
        type_ (CreateGraphIndexRequestType):
        description (str | Unset): Optional description of the index and its purpose
        version (int | Unset): Version of the index implementation. Defaults to 0. Default: 0.
        enrichments (list[EnrichmentConfig] | Unset): Inline managed enrichment definitions required by this index.
        summarizer (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
        template (str | Unset): Handlebars template for generating summarizer input text.
            Uses document fields as template variables.
            Same pattern as EmbeddingsConfig template.
             Example: {{title}}
            {{content}}.
        edge_types (list[EdgeTypeConfig] | Unset): List of edge types with their configurations
        max_edges_per_document (int | Unset): Maximum number of edges per document (0 = unlimited)
    """

    type_: CreateGraphIndexRequestType
    description: str | Unset = UNSET
    version: int | Unset = 0
    enrichments: list[EnrichmentConfig] | Unset = UNSET
    summarizer: GeneratorConfig | Unset = UNSET
    template: str | Unset = UNSET
    edge_types: list[EdgeTypeConfig] | Unset = UNSET
    max_edges_per_document: int | Unset = UNSET
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
        if summarizer is not UNSET:
            field_dict["summarizer"] = summarizer
        if template is not UNSET:
            field_dict["template"] = template
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if max_edges_per_document is not UNSET:
            field_dict["max_edges_per_document"] = max_edges_per_document

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.edge_type_config import EdgeTypeConfig
        from ..models.enrichment_config import EnrichmentConfig
        from ..models.generator_config import GeneratorConfig

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

        create_graph_index_request = cls(
            type_=type_,
            description=description,
            version=version,
            enrichments=enrichments,
            summarizer=summarizer,
            template=template,
            edge_types=edge_types,
            max_edges_per_document=max_edges_per_document,
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
