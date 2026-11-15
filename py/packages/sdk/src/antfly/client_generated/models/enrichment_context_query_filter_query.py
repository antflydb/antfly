from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="EnrichmentContextQueryFilterQuery")


@_attrs_define
class EnrichmentContextQueryFilterQuery:
    """An Antfly `filter_query` (Query shape): `bool` (`must`/`filter`/`should`/`must_not`/`minimum_should_match`, filter-
    context semantics), `conjuncts`, `disjuncts` (`min`), `match_all`, `match_none`, and leaves. Leaves evaluated by
    context resolution itself: `term`/`terms` (equality; at the top of the conjunction they form indexed subscriptions,
    at most one bound `terms` set), `range` (`gt`/`gte`/`lt`/`lte`), `numeric_range`/`term_range`
    (`min`/`max`/`inclusive_*`), `prefix`, `match` and `match_phrase` (pinned `antfly.context.simple.v1` analyzer:
    lowercase ASCII words; `operator` and|or), and `similar` (`{vector, threshold, metric: cosine|dot, dims, model}`
    over a vector stored in a document field). Every other row-decidable leaf (wildcard, regexp, fuzzy, date and IP
    ranges, geo, bool fields, `exists`, doc ids, `equals`/`contains`/`matches`) is lowered at admission into the storage
    engine's per-row filter and evaluated per changed row. Values are literals or typed bindings
    `{"$source":"/json/pointer"}` from the source document (in storage-evaluated leaves, string-valued positions only).
    Corpus-relative text queries (`query_string`, `multi_match`, analyzer-specific `match`) are rejected for live
    tracking; use a snapshot `search.request`.

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
