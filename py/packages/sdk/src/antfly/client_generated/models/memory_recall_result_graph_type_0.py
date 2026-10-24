from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryRecallResultGraphType0")


@_attrs_define
class MemoryRecallResultGraphType0:
    """
    Attributes:
        nodes (int):
        edges (int):
        iterations (int):
        complete (bool):
        omitted (int):
        algorithm (str | Unset):
    """

    nodes: int
    edges: int
    iterations: int
    complete: bool
    omitted: int
    algorithm: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        nodes = self.nodes

        edges = self.edges

        iterations = self.iterations

        complete = self.complete

        omitted = self.omitted

        algorithm = self.algorithm

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "nodes": nodes,
                "edges": edges,
                "iterations": iterations,
                "complete": complete,
                "omitted": omitted,
            }
        )
        if algorithm is not UNSET:
            field_dict["algorithm"] = algorithm

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        nodes = d.pop("nodes")

        edges = d.pop("edges")

        iterations = d.pop("iterations")

        complete = d.pop("complete")

        omitted = d.pop("omitted")

        algorithm = d.pop("algorithm", UNSET)

        memory_recall_result_graph_type_0 = cls(
            nodes=nodes,
            edges=edges,
            iterations=iterations,
            complete=complete,
            omitted=omitted,
            algorithm=algorithm,
        )

        return memory_recall_result_graph_type_0
