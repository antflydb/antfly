from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.graph_artifact_node_mapping_config_model import GraphArtifactNodeMappingConfigModel
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphArtifactNodeMappingConfig")


@_attrs_define
class GraphArtifactNodeMappingConfig:
    """Maps each artifact item to graph node identifiers.

    Attributes:
        model (GraphArtifactNodeMappingConfigModel | Unset):  Default: GraphArtifactNodeMappingConfigModel.DOCUMENT.
        source (str | Unset): Owner identity for edges materialized from one artifact item. Omit it to use the source
            document key. When present, it must be exactly the source document key template so edge state remains colocated
            with its routing and range-split owner.
        target (float | str | Unset): A literal string or finite numeric value, or a Handlebars template evaluated for
            each materialized graph item.
    """

    model: GraphArtifactNodeMappingConfigModel | Unset = GraphArtifactNodeMappingConfigModel.DOCUMENT
    source: str | Unset = UNSET
    target: float | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        model: str | Unset = UNSET
        if not isinstance(self.model, Unset):
            model = self.model.value

        source = self.source

        target: float | str | Unset
        if isinstance(self.target, Unset):
            target = UNSET
        else:
            target = self.target

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if model is not UNSET:
            field_dict["model"] = model
        if source is not UNSET:
            field_dict["source"] = source
        if target is not UNSET:
            field_dict["target"] = target

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _model = d.pop("model", UNSET)
        model: GraphArtifactNodeMappingConfigModel | Unset
        if isinstance(_model, Unset):
            model = UNSET
        else:
            model = GraphArtifactNodeMappingConfigModel(_model)

        source = d.pop("source", UNSET)

        def _parse_target(data: object) -> float | str | Unset:
            if isinstance(data, Unset):
                return data
            return cast(float | str | Unset, data)

        target = _parse_target(d.pop("target", UNSET))

        graph_artifact_node_mapping_config = cls(
            model=model,
            source=source,
            target=target,
        )

        return graph_artifact_node_mapping_config
