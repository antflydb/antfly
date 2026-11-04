from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_traversal import GraphTraversal


T = TypeVar("T", bound="GraphTraverseQuery")


@_attrs_define
class GraphTraverseQuery:
    """
    Attributes:
        index (str):
        traverse (GraphTraversal): Breadth-first traversal by exact table-qualified node identity. With include_paths,
            visited state and deduplication are independent per start node, preserving paths between roots and overlapping
            root provenance. The first node in each path identifies its root. Without include_paths, result nodes are
            deduplicated across starts. Direction defaults to `out`; use `both` to traverse a relationship as undirected
            without storing a reciprocal edge.
    """

    index: str
    traverse: GraphTraversal

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        traverse = self.traverse.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "index": index,
                "traverse": traverse,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_traversal import GraphTraversal

        if set(src_dict) - {
            "index",
            "traverse",
        }:
            raise ValueError("Unexpected properties for GraphTraverseQuery")
        d = dict(src_dict)
        index = d.pop("index")

        traverse = GraphTraversal.from_dict(d.pop("traverse"))

        graph_traverse_query = cls(
            index=index,
            traverse=traverse,
        )

        return graph_traverse_query
