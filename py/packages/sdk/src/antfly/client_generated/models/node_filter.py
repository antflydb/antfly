from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_document_query import GraphDocumentQuery


T = TypeVar("T", bound="NodeFilter")


@_attrs_define
class NodeFilter:
    """Filter nodes during graph traversal using existing query primitives

    Attributes:
        filter_query (GraphDocumentQuery | Unset): A document-query expression in either public
            QueryRequest.filter_query syntax or canonical Antfly filter AST syntax. Graph queries embed this existing
            document query language; alias-to-alias predicates belong in GraphMatch.where.
        filter_prefix (str | Unset): Filter by key prefix
    """

    filter_query: GraphDocumentQuery | Unset = UNSET
    filter_prefix: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        filter_query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filter_query, Unset):
            filter_query = self.filter_query.to_dict()

        filter_prefix = self.filter_prefix

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if filter_query is not UNSET:
            field_dict["filter_query"] = filter_query
        if filter_prefix is not UNSET:
            field_dict["filter_prefix"] = filter_prefix

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_document_query import GraphDocumentQuery

        d = dict(src_dict)
        _filter_query = d.pop("filter_query", UNSET)
        filter_query: GraphDocumentQuery | Unset
        if isinstance(_filter_query, Unset):
            filter_query = UNSET
        else:
            filter_query = GraphDocumentQuery.from_dict(_filter_query)

        filter_prefix = d.pop("filter_prefix", UNSET)

        node_filter = cls(
            filter_query=filter_query,
            filter_prefix=filter_prefix,
        )

        node_filter.additional_properties = d
        return node_filter

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
