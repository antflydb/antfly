from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.edge_type_config import EdgeTypeConfig
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="GraphIndexConfig")


@_attrs_define
class GraphIndexConfig:
    """Configuration for graph index type

    Attributes:
        summarizer (Union[Unset, GeneratorConfig]): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
        template (Union[Unset, str]): Handlebars template for generating summarizer input text.
            Uses document fields as template variables.
            Same pattern as EmbeddingsConfig template.
             Example: {{title}}
            {{content}}.
        edge_types (Union[Unset, list['EdgeTypeConfig']]): List of edge types with their configurations
        max_edges_per_document (Union[Unset, int]): Maximum number of edges per document (0 = unlimited)
    """

    summarizer: Union[Unset, "GeneratorConfig"] = UNSET
    template: Union[Unset, str] = UNSET
    edge_types: Union[Unset, list["EdgeTypeConfig"]] = UNSET
    max_edges_per_document: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        summarizer: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.summarizer, Unset):
            summarizer = self.summarizer.to_dict()

        template = self.template

        edge_types: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = []
            for edge_types_item_data in self.edge_types:
                edge_types_item = edge_types_item_data.to_dict()
                edge_types.append(edge_types_item)

        max_edges_per_document = self.max_edges_per_document

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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.edge_type_config import EdgeTypeConfig
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        _summarizer = d.pop("summarizer", UNSET)
        summarizer: Union[Unset, GeneratorConfig]
        if isinstance(_summarizer, Unset):
            summarizer = UNSET
        else:
            summarizer = GeneratorConfig.from_dict(_summarizer)

        template = d.pop("template", UNSET)

        edge_types = []
        _edge_types = d.pop("edge_types", UNSET)
        for edge_types_item_data in _edge_types or []:
            edge_types_item = EdgeTypeConfig.from_dict(edge_types_item_data)

            edge_types.append(edge_types_item)

        max_edges_per_document = d.pop("max_edges_per_document", UNSET)

        graph_index_config = cls(
            summarizer=summarizer,
            template=template,
            edge_types=edge_types,
            max_edges_per_document=max_edges_per_document,
        )

        graph_index_config.additional_properties = d
        return graph_index_config

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
