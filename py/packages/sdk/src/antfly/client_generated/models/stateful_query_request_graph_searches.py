from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.legacy_graph_query import LegacyGraphQuery


T = TypeVar("T", bound="StatefulQueryRequestGraphSearches")


@_attrs_define
class StatefulQueryRequestGraphSearches:
    """Deprecated compatibility alias for the v0.2 graph query contract.
    Use `graph_queries`; requests containing both fields are rejected.
    Legacy operation names remain opaque and byte-for-byte compatible;
    canonical GraphIdentifier rules apply only to `graph_queries`.
    The request-wide limit of 64 operations also applies here to bound
    execution work during the compatibility window.

    """

    additional_properties: dict[str, LegacyGraphQuery] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.legacy_graph_query import LegacyGraphQuery

        d = dict(src_dict)
        stateful_query_request_graph_searches = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = LegacyGraphQuery.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        stateful_query_request_graph_searches.additional_properties = additional_properties
        return stateful_query_request_graph_searches

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> LegacyGraphQuery:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: LegacyGraphQuery) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
