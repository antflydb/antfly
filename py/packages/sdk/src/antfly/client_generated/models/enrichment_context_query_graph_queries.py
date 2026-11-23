from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="EnrichmentContextQueryGraphQueries")


@_attrs_define
class EnrichmentContextQueryGraphQueries:
    """Graph operations (QueryRequest shape). Exactly one `traverse` from the dependent row (or `start: {keys: [{"$source":
    "/pointer"}]}`) with only `edge_types`, `direction`, `max_depth` (at most 3), and the context options `max_fanout`
    (edges per visited node, default 100) and `include_generated` (also follow field-derived and enrichment-generated
    edges) is a live traversal: dependents watch every visited node's edges and every reached row, members render with
    depth, edge type, and direction, and `max_documents` bounds the rows reached. A `filter_query` beside it narrows the
    reached rows and stays live. Anything else (`match`, shortest paths, `edge_filter`, `edge_weight`, `limit`, metrics,
    deeper traversals, several operations) runs in the query engine as a snapshot.

    """

    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enrichment_context_query_graph_queries = cls()

        enrichment_context_query_graph_queries.additional_properties = d
        return enrichment_context_query_graph_queries

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
