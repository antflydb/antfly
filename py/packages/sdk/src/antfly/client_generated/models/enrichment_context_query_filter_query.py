from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="EnrichmentContextQueryFilterQuery")


@_attrs_define
class EnrichmentContextQueryFilterQuery:
    """`{"conjuncts":[...]}` (or one bare conjunct) of: `term`/`terms` (equality, indexed subscriptions; at most one bound
    `terms` set), `range` (`gt`/`gte`/`lt`/`lte`, numbers or strings such as ISO dates), `prefix`, `match` (filter-mode
    full-text with `operator` and|or, evaluated with the pinned `antfly.context.simple.v1` analyzer: lowercase ASCII
    words), and `similar` (`{vector, threshold, metric: cosine|dot, dims, model}` over a vector stored in a document
    field; `model` names the embedding space and is part of the declaration). Values are literals or typed bindings
    `{"$source":"/json/pointer"}` from the source document. Disjunctions, scored full-text, fuzzy/regex, and nearest-k
    are rejected for live tracking.

    """

    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enrichment_context_query_filter_query = cls()

        enrichment_context_query_filter_query.additional_properties = d
        return enrichment_context_query_filter_query

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
