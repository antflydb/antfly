from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphQueryLimits")


@_attrs_define
class GraphQueryLimits:
    """
    Attributes:
        memory_bytes (int | Unset):
        scanned_anchors (int | Unset):
        explored_edges (int | Unset):
        intermediate_states (int | Unset):
        path_depth (int | Unset):
        recursion_depth (int | Unset):
    """

    memory_bytes: int | Unset = UNSET
    scanned_anchors: int | Unset = UNSET
    explored_edges: int | Unset = UNSET
    intermediate_states: int | Unset = UNSET
    path_depth: int | Unset = UNSET
    recursion_depth: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        memory_bytes = self.memory_bytes

        scanned_anchors = self.scanned_anchors

        explored_edges = self.explored_edges

        intermediate_states = self.intermediate_states

        path_depth = self.path_depth

        recursion_depth = self.recursion_depth

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if memory_bytes is not UNSET:
            field_dict["memory_bytes"] = memory_bytes
        if scanned_anchors is not UNSET:
            field_dict["scanned_anchors"] = scanned_anchors
        if explored_edges is not UNSET:
            field_dict["explored_edges"] = explored_edges
        if intermediate_states is not UNSET:
            field_dict["intermediate_states"] = intermediate_states
        if path_depth is not UNSET:
            field_dict["path_depth"] = path_depth
        if recursion_depth is not UNSET:
            field_dict["recursion_depth"] = recursion_depth

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        memory_bytes = d.pop("memory_bytes", UNSET)

        scanned_anchors = d.pop("scanned_anchors", UNSET)

        explored_edges = d.pop("explored_edges", UNSET)

        intermediate_states = d.pop("intermediate_states", UNSET)

        path_depth = d.pop("path_depth", UNSET)

        recursion_depth = d.pop("recursion_depth", UNSET)

        graph_query_limits = cls(
            memory_bytes=memory_bytes,
            scanned_anchors=scanned_anchors,
            explored_edges=explored_edges,
            intermediate_states=intermediate_states,
            path_depth=path_depth,
            recursion_depth=recursion_depth,
        )

        return graph_query_limits
