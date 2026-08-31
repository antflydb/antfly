from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_path_edge_direction import GraphPathEdgeDirection
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_path_edge_metadata import GraphPathEdgeMetadata
    from ..models.graph_path_endpoint import GraphPathEndpoint


T = TypeVar("T", bound="GraphPathEdge")


@_attrs_define
class GraphPathEdge:
    """One edge in a canonical path. `from` and `to` are the exact ordered traversal endpoints, not unqualified physical
    edge keys, so identity remains unambiguous across tables and for equal keys in different tables.

        Attributes:
            from_ (GraphPathEndpoint):
            to (GraphPathEndpoint):
            direction (GraphPathEdgeDirection): Physical stored-edge orientation relative to this path edge's `from`
                endpoint. `out` means the stored relationship points from `from` to `to`; `in` means the path traversed a
                relationship stored from `to` to `from`. This keeps paths lossless when a `both` query encounters reciprocal
                relationships.
            type_ (str): Durable graph edge type. Values must be valid UTF-8 and encode to at most 64 KiB; `maxLength` is
                the standard-schema code-point ceiling and `x-antfly-max-utf8-bytes` carries the exact wire-byte limit.
            weight (float): Finite durable edge weight. max_weight_product paths further require values in [0,1].
            metadata (GraphPathEdgeMetadata | Unset):
    """

    from_: GraphPathEndpoint
    to: GraphPathEndpoint
    direction: GraphPathEdgeDirection
    type_: str
    weight: float
    metadata: GraphPathEdgeMetadata | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from_ = self.from_.to_dict()

        to = self.to.to_dict()

        direction = self.direction.value

        type_ = self.type_

        weight = self.weight

        metadata: dict[str, Any] | Unset = UNSET
        if not isinstance(self.metadata, Unset):
            metadata = self.metadata.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "from": from_,
                "to": to,
                "direction": direction,
                "type": type_,
                "weight": weight,
            }
        )
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_path_edge_metadata import GraphPathEdgeMetadata
        from ..models.graph_path_endpoint import GraphPathEndpoint

        d = dict(src_dict)
        from_ = GraphPathEndpoint.from_dict(d.pop("from"))

        to = GraphPathEndpoint.from_dict(d.pop("to"))

        direction = GraphPathEdgeDirection(d.pop("direction"))

        type_ = d.pop("type")

        weight = d.pop("weight")

        _metadata = d.pop("metadata", UNSET)
        metadata: GraphPathEdgeMetadata | Unset
        if isinstance(_metadata, Unset):
            metadata = UNSET
        else:
            metadata = GraphPathEdgeMetadata.from_dict(_metadata)

        graph_path_edge = cls(
            from_=from_,
            to=to,
            direction=direction,
            type_=type_,
            weight=weight,
            metadata=metadata,
        )

        return graph_path_edge
