from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_index_source_nodes_model import GraphIndexSourceNodesModel
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphIndexSourceNodes")


@_attrs_define
class GraphIndexSourceNodes:
    """
    Attributes:
        model (GraphIndexSourceNodesModel | Unset):  Default: GraphIndexSourceNodesModel.DOCUMENT.
        source (str | Unset): Template for the source node identifier.
        target (str | Unset): Template for the target node identifier.
    """

    model: GraphIndexSourceNodesModel | Unset = GraphIndexSourceNodesModel.DOCUMENT
    source: str | Unset = UNSET
    target: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        model: str | Unset = UNSET
        if not isinstance(self.model, Unset):
            model = self.model.value

        source = self.source

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
        model: GraphIndexSourceNodesModel | Unset
        if isinstance(_model, Unset):
            model = UNSET
        else:
            model = GraphIndexSourceNodesModel(_model)

        source = d.pop("source", UNSET)

        target = d.pop("target", UNSET)

        graph_index_source_nodes = cls(
            model=model,
            source=source,
            target=target,
        )

        return graph_index_source_nodes
