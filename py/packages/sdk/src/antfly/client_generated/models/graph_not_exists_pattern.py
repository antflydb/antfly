from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_match_edge import GraphMatchEdge


T = TypeVar("T", bound="GraphNotExistsPattern")


@_attrs_define
class GraphNotExistsPattern:
    """
    Attributes:
        edges (list[GraphMatchEdge]):
    """

    edges: list[GraphMatchEdge]

    def to_dict(self) -> dict[str, Any]:
        edges = []
        for edges_item_data in self.edges:
            edges_item = edges_item_data.to_dict()
            edges.append(edges_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "edges": edges,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_match_edge import GraphMatchEdge

        d = dict(src_dict)
        edges = []
        _edges = d.pop("edges")
        for edges_item_data in _edges:
            edges_item = GraphMatchEdge.from_dict(edges_item_data)

            edges.append(edges_item)

        graph_not_exists_pattern = cls(
            edges=edges,
        )

        return graph_not_exists_pattern
