from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_path_endpoint import GraphPathEndpoint
    from ..models.node_filter import NodeFilter


T = TypeVar("T", bound="GraphNodeSelector")


@_attrs_define
class GraphNodeSelector:
    """Select graph nodes by exactly one of keys, identities, or result_ref. Unqualified keys retain legacy cross-table
    wildcard semantics; identities are exact.

        Attributes:
            keys (list[str] | Unset): Legacy list of node keys, matching the key in any reachable table. Prefer identities
                when keys may collide across tables.
            identities (list[GraphPathEndpoint] | Unset): Exact node identities. Omitted table means the query table.
            result_ref (str | Unset): Reference to search results to use as nodes:
                - "$full_text_results" - use full-text search results
                - "$embeddings_results.index_name" - use vector search results from specific index
            limit (int | Unset): Maximum number of nodes to select from result_ref; invalid with keys or identities.
            node_filter (NodeFilter | Unset): Filter nodes during graph traversal using existing query primitives
    """

    keys: list[str] | Unset = UNSET
    identities: list[GraphPathEndpoint] | Unset = UNSET
    result_ref: str | Unset = UNSET
    limit: int | Unset = UNSET
    node_filter: NodeFilter | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        keys: list[str] | Unset = UNSET
        if not isinstance(self.keys, Unset):
            keys = self.keys

        identities: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.identities, Unset):
            identities = []
            for identities_item_data in self.identities:
                identities_item = identities_item_data.to_dict()
                identities.append(identities_item)

        result_ref = self.result_ref

        limit = self.limit

        node_filter: dict[str, Any] | Unset = UNSET
        if not isinstance(self.node_filter, Unset):
            node_filter = self.node_filter.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if keys is not UNSET:
            field_dict["keys"] = keys
        if identities is not UNSET:
            field_dict["identities"] = identities
        if result_ref is not UNSET:
            field_dict["result_ref"] = result_ref
        if limit is not UNSET:
            field_dict["limit"] = limit
        if node_filter is not UNSET:
            field_dict["node_filter"] = node_filter

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_path_endpoint import GraphPathEndpoint
        from ..models.node_filter import NodeFilter

        d = dict(src_dict)
        keys = cast(list[str], d.pop("keys", UNSET))

        _identities = d.pop("identities", UNSET)
        identities: list[GraphPathEndpoint] | Unset = UNSET
        if _identities is not UNSET:
            identities = []
            for identities_item_data in _identities:
                identities_item = GraphPathEndpoint.from_dict(identities_item_data)

                identities.append(identities_item)

        result_ref = d.pop("result_ref", UNSET)

        limit = d.pop("limit", UNSET)

        _node_filter = d.pop("node_filter", UNSET)
        node_filter: NodeFilter | Unset
        if isinstance(_node_filter, Unset):
            node_filter = UNSET
        else:
            node_filter = NodeFilter.from_dict(_node_filter)

        graph_node_selector = cls(
            keys=keys,
            identities=identities,
            result_ref=result_ref,
            limit=limit,
            node_filter=node_filter,
        )

        return graph_node_selector
