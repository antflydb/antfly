from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="EnrichmentContextSearchRequest")


@_attrs_define
class EnrichmentContextSearchRequest:
    """A full public QueryRequest (`full_text_search`, `semantic_search` with `indexes`, hybrid `merge_config`, `reranker`,
    `filter_query`, `exclusion_query`, `order_by`, `graph_queries`, ...) with `{"$source":"/pointer"}` bindings
    anywhere, executed by the table's query engine. `limit` (at most 1000) is the member count. `table`, paging
    (`offset`, `search_after`, `search_before`), `count`, and `aggregations` are rejected. Distributed tables refuse
    `semantic_search` (data servers have no query embedder); pass precomputed vectors in `embeddings` instead.

    """

    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enrichment_context_search_request = cls()

        enrichment_context_search_request.additional_properties = d
        return enrichment_context_search_request

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
