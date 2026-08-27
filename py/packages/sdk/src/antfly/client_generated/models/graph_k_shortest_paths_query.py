from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_k_shortest_paths import GraphKShortestPaths


T = TypeVar("T", bound="GraphKShortestPathsQuery")


@_attrs_define
class GraphKShortestPathsQuery:
    """
    Attributes:
        index (str):
        k_shortest_paths (GraphKShortestPaths): Find up to `k` loopless paths from `from` to `to` in the requested
            stored-edge direction.
    """

    index: str
    k_shortest_paths: GraphKShortestPaths

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        k_shortest_paths = self.k_shortest_paths.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "index": index,
                "k_shortest_paths": k_shortest_paths,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_k_shortest_paths import GraphKShortestPaths

        d = dict(src_dict)
        index = d.pop("index")

        k_shortest_paths = GraphKShortestPaths.from_dict(d.pop("k_shortest_paths"))

        graph_k_shortest_paths_query = cls(
            index=index,
            k_shortest_paths=k_shortest_paths,
        )

        return graph_k_shortest_paths_query
