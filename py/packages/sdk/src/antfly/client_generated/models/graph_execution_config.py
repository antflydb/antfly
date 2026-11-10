from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphExecutionConfig")


@_attrs_define
class GraphExecutionConfig:
    """Operator-owned ceilings shared by all named graph operations in one admitted query. Public requests cannot raise
    these values. Explored work, scanned anchors, and exact-distinct capacity are request-consumptive; intermediate and
    retained-state ceilings bound simultaneously live state.

        Attributes:
            max_explored_nodes (int | Unset): Cumulative node expansions across every named operation and path spur in one
                request. Default: 100000.
            max_explored_edges (int | Unset): Cumulative edge expansions across every named operation and path spur in one
                request. Default: 1000000.
            max_explored_edge_bytes (int | Unset): Cumulative owned bytes of materialized edges read while executing one
                request. Default: 67108864.
            max_scanned_anchors (int | Unset): Cumulative source anchors scanned by exact MATCH operations in one request.
                Default: 1000000.
            max_intermediate_states (int | Unset): Maximum simultaneously live traversal, pathfinding, or MATCH frontier
                states. Default: 100000.
            max_retained_state_bytes (int | Unset): Maximum simultaneously retained bytes of admitted graph execution state.
                Default: 67108864.
            max_distinct_identities (int | Unset): Cumulative exact-distinct identities admitted across all named aggregates
                in one request. Default: 100000.
            max_distinct_state_bytes (int | Unset): Request-consumptive bytes for exact-distinct identities, indexes, and
                ownership transfer. Default: 16777216.
    """

    max_explored_nodes: int | Unset = 100000
    max_explored_edges: int | Unset = 1000000
    max_explored_edge_bytes: int | Unset = 67108864
    max_scanned_anchors: int | Unset = 1000000
    max_intermediate_states: int | Unset = 100000
    max_retained_state_bytes: int | Unset = 67108864
    max_distinct_identities: int | Unset = 100000
    max_distinct_state_bytes: int | Unset = 16777216

    def to_dict(self) -> dict[str, Any]:
        max_explored_nodes = self.max_explored_nodes

        max_explored_edges = self.max_explored_edges

        max_explored_edge_bytes = self.max_explored_edge_bytes

        max_scanned_anchors = self.max_scanned_anchors

        max_intermediate_states = self.max_intermediate_states

        max_retained_state_bytes = self.max_retained_state_bytes

        max_distinct_identities = self.max_distinct_identities

        max_distinct_state_bytes = self.max_distinct_state_bytes

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_explored_nodes is not UNSET:
            field_dict["max_explored_nodes"] = max_explored_nodes
        if max_explored_edges is not UNSET:
            field_dict["max_explored_edges"] = max_explored_edges
        if max_explored_edge_bytes is not UNSET:
            field_dict["max_explored_edge_bytes"] = max_explored_edge_bytes
        if max_scanned_anchors is not UNSET:
            field_dict["max_scanned_anchors"] = max_scanned_anchors
        if max_intermediate_states is not UNSET:
            field_dict["max_intermediate_states"] = max_intermediate_states
        if max_retained_state_bytes is not UNSET:
            field_dict["max_retained_state_bytes"] = max_retained_state_bytes
        if max_distinct_identities is not UNSET:
            field_dict["max_distinct_identities"] = max_distinct_identities
        if max_distinct_state_bytes is not UNSET:
            field_dict["max_distinct_state_bytes"] = max_distinct_state_bytes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_explored_nodes = d.pop("max_explored_nodes", UNSET)

        max_explored_edges = d.pop("max_explored_edges", UNSET)

        max_explored_edge_bytes = d.pop("max_explored_edge_bytes", UNSET)

        max_scanned_anchors = d.pop("max_scanned_anchors", UNSET)

        max_intermediate_states = d.pop("max_intermediate_states", UNSET)

        max_retained_state_bytes = d.pop("max_retained_state_bytes", UNSET)

        max_distinct_identities = d.pop("max_distinct_identities", UNSET)

        max_distinct_state_bytes = d.pop("max_distinct_state_bytes", UNSET)

        graph_execution_config = cls(
            max_explored_nodes=max_explored_nodes,
            max_explored_edges=max_explored_edges,
            max_explored_edge_bytes=max_explored_edge_bytes,
            max_scanned_anchors=max_scanned_anchors,
            max_intermediate_states=max_intermediate_states,
            max_retained_state_bytes=max_retained_state_bytes,
            max_distinct_identities=max_distinct_identities,
            max_distinct_state_bytes=max_distinct_state_bytes,
        )

        return graph_execution_config
