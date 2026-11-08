from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_definition import GraphDefinition


T = TypeVar("T", bound="GraphCatalogEntry")


@_attrs_define
class GraphCatalogEntry:
    """
    Attributes:
        graph_id (str): Opaque decimal catalog identity
        revision (str): Lossless decimal definition revision used for conditional updates
        name (str):
        definition (GraphDefinition):
    """

    graph_id: str
    revision: str
    name: str
    definition: GraphDefinition
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        graph_id = self.graph_id

        revision = self.revision

        name = self.name

        definition = self.definition.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "graph_id": graph_id,
                "revision": revision,
                "name": name,
                "definition": definition,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_definition import GraphDefinition

        d = dict(src_dict)
        graph_id = d.pop("graph_id")

        revision = d.pop("revision")

        name = d.pop("name")

        definition = GraphDefinition.from_dict(d.pop("definition"))

        graph_catalog_entry = cls(
            graph_id=graph_id,
            revision=revision,
            name=name,
            definition=definition,
        )

        graph_catalog_entry.additional_properties = d
        return graph_catalog_entry

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
