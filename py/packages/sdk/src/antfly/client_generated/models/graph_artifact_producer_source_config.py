from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_artifact_producer_source_config_type import GraphArtifactProducerSourceConfigType

T = TypeVar("T", bound="GraphArtifactProducerSourceConfig")


@_attrs_define
class GraphArtifactProducerSourceConfig:
    """Document input used by an artifact producer. Field sources read one document field; template sources render a
    Handlebars template.

        Attributes:
            type_ (GraphArtifactProducerSourceConfigType):
            value (str):
    """

    type_: GraphArtifactProducerSourceConfigType
    value: str

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = GraphArtifactProducerSourceConfigType(d.pop("type"))

        value = d.pop("value")

        graph_artifact_producer_source_config = cls(
            type_=type_,
            value=value,
        )

        return graph_artifact_producer_source_config
