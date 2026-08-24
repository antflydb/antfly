from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_shortest_path import GraphShortestPath


T = TypeVar("T", bound="GraphShortestPathQuery")


@_attrs_define
class GraphShortestPathQuery:
    """
    Attributes:
        index (str):
        shortest_path (GraphShortestPath): Find the best outgoing path from `from` to `to`.
    """

    index: str
    shortest_path: GraphShortestPath

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        shortest_path = self.shortest_path.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "index": index,
                "shortest_path": shortest_path,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_shortest_path import GraphShortestPath

        d = dict(src_dict)
        index = d.pop("index")

        shortest_path = GraphShortestPath.from_dict(d.pop("shortest_path"))

        graph_shortest_path_query = cls(
            index=index,
            shortest_path=shortest_path,
        )

        return graph_shortest_path_query
