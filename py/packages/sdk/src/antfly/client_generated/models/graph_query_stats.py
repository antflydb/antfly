from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphQueryStats")


@_attrs_define
class GraphQueryStats:
    """
    Attributes:
        scanned_anchors (int | Unset):
        explored_edges (int | Unset):
        intermediate_states (int | Unset):
        mutations (int | Unset):
        memory_bytes (int | Unset):
        search_calls (int | Unset):
        search_candidates (int | Unset):
        search_approximate (bool | Unset):
    """

    scanned_anchors: int | Unset = UNSET
    explored_edges: int | Unset = UNSET
    intermediate_states: int | Unset = UNSET
    mutations: int | Unset = UNSET
    memory_bytes: int | Unset = UNSET
    search_calls: int | Unset = UNSET
    search_candidates: int | Unset = UNSET
    search_approximate: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        scanned_anchors = self.scanned_anchors

        explored_edges = self.explored_edges

        intermediate_states = self.intermediate_states

        mutations = self.mutations

        memory_bytes = self.memory_bytes

        search_calls = self.search_calls

        search_candidates = self.search_candidates

        search_approximate = self.search_approximate

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if scanned_anchors is not UNSET:
            field_dict["scanned_anchors"] = scanned_anchors
        if explored_edges is not UNSET:
            field_dict["explored_edges"] = explored_edges
        if intermediate_states is not UNSET:
            field_dict["intermediate_states"] = intermediate_states
        if mutations is not UNSET:
            field_dict["mutations"] = mutations
        if memory_bytes is not UNSET:
            field_dict["memory_bytes"] = memory_bytes
        if search_calls is not UNSET:
            field_dict["search_calls"] = search_calls
        if search_candidates is not UNSET:
            field_dict["search_candidates"] = search_candidates
        if search_approximate is not UNSET:
            field_dict["search_approximate"] = search_approximate

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        scanned_anchors = d.pop("scanned_anchors", UNSET)

        explored_edges = d.pop("explored_edges", UNSET)

        intermediate_states = d.pop("intermediate_states", UNSET)

        mutations = d.pop("mutations", UNSET)

        memory_bytes = d.pop("memory_bytes", UNSET)

        search_calls = d.pop("search_calls", UNSET)

        search_candidates = d.pop("search_candidates", UNSET)

        search_approximate = d.pop("search_approximate", UNSET)

        graph_query_stats = cls(
            scanned_anchors=scanned_anchors,
            explored_edges=explored_edges,
            intermediate_states=intermediate_states,
            mutations=mutations,
            memory_bytes=memory_bytes,
            search_calls=search_calls,
            search_candidates=search_candidates,
            search_approximate=search_approximate,
        )

        return graph_query_stats
