from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="EnrichmentContextQueryFilterQuery")


@_attrs_define
class EnrichmentContextQueryFilterQuery:
    """A `{"conjuncts":[...]}` of `term`/`terms` equality conjuncts (or one bare conjunct). Values are literals or typed
    bindings `{"$source":"/json/pointer"}` from the source document (never text interpolation); at most one bound
    `terms` set. Ranges, disjunctions, full-text, and semantic top-k are rejected at admission. Members are canonically
    ordered by key.

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
