from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_artifact_edge_mapping_config_metadata import GraphArtifactEdgeMappingConfigMetadata


T = TypeVar("T", bound="GraphArtifactEdgeMappingConfig")


@_attrs_define
class GraphArtifactEdgeMappingConfig:
    """Maps each artifact item to a relationship identity, type, weight, and public metadata.

    Attributes:
        edge_id (float | str | Unset): A literal string or finite numeric value, or a Handlebars template evaluated for
            each materialized graph item.
        type_ (float | str | Unset): A literal string or finite numeric value, or a Handlebars template evaluated for
            each materialized graph item.
        weight (float | str | Unset): A literal string or finite numeric value, or a Handlebars template evaluated for
            each materialized graph item.
        metadata (GraphArtifactEdgeMappingConfigMetadata | Unset): JSON metadata template copied onto each materialized
            edge. Sensitive keys are omitted from create responses.
    """

    edge_id: float | str | Unset = UNSET
    type_: float | str | Unset = UNSET
    weight: float | str | Unset = UNSET
    metadata: GraphArtifactEdgeMappingConfigMetadata | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        edge_id: float | str | Unset
        if isinstance(self.edge_id, Unset):
            edge_id = UNSET
        else:
            edge_id = self.edge_id

        type_: float | str | Unset
        if isinstance(self.type_, Unset):
            type_ = UNSET
        else:
            type_ = self.type_

        weight: float | str | Unset
        if isinstance(self.weight, Unset):
            weight = UNSET
        else:
            weight = self.weight

        metadata: dict[str, Any] | Unset = UNSET
        if not isinstance(self.metadata, Unset):
            metadata = self.metadata.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if edge_id is not UNSET:
            field_dict["edge_id"] = edge_id
        if type_ is not UNSET:
            field_dict["type"] = type_
        if weight is not UNSET:
            field_dict["weight"] = weight
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_artifact_edge_mapping_config_metadata import GraphArtifactEdgeMappingConfigMetadata

        d = dict(src_dict)

        def _parse_edge_id(data: object) -> float | str | Unset:
            if isinstance(data, Unset):
                return data
            return cast(float | str | Unset, data)

        edge_id = _parse_edge_id(d.pop("edge_id", UNSET))

        def _parse_type_(data: object) -> float | str | Unset:
            if isinstance(data, Unset):
                return data
            return cast(float | str | Unset, data)

        type_ = _parse_type_(d.pop("type", UNSET))

        def _parse_weight(data: object) -> float | str | Unset:
            if isinstance(data, Unset):
                return data
            return cast(float | str | Unset, data)

        weight = _parse_weight(d.pop("weight", UNSET))

        _metadata = d.pop("metadata", UNSET)
        metadata: GraphArtifactEdgeMappingConfigMetadata | Unset
        if isinstance(_metadata, Unset):
            metadata = UNSET
        else:
            metadata = GraphArtifactEdgeMappingConfigMetadata.from_dict(_metadata)

        graph_artifact_edge_mapping_config = cls(
            edge_id=edge_id,
            type_=type_,
            weight=weight,
            metadata=metadata,
        )

        return graph_artifact_edge_mapping_config
