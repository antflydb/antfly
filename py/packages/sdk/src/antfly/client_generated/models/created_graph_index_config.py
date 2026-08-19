from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.created_provider_config import CreatedProviderConfig
    from ..models.edge_type_config import EdgeTypeConfig


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
    """

    summarizer: CreatedProviderConfig | Unset = UNSET
    template: str | Unset = UNSET
    edge_types: list[EdgeTypeConfig] | Unset = UNSET
    max_edges_per_document: int | Unset = UNSET
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
        from ..models.created_provider_config import CreatedProviderConfig
        from ..models.edge_type_config import EdgeTypeConfig

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

        created_graph_index_config = cls(
            summarizer=summarizer,
            template=template,
            edge_types=edge_types,
            max_edges_per_document=max_edges_per_document,
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
