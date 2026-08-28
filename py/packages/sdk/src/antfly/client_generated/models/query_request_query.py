from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="QueryRequestQuery")


@_attrs_define
class QueryRequestQuery:
    """Canonical public query AST. Prefer this field for new clients.

    Boolean clauses are normalized before planning:
    - `bool.must` is scoring query input.
    - `bool.filter` is non-scoring query input.
    - `bool.must_not` is non-scoring exclusion query input.

    Filter branches accept the same query variants as `filter_query` and
    `exclusion_query`. Structured clauses use the native document-value
    path; text clauses are resolved through the text index before scoring.

        Example:
            {'bool': {'must': [{'match': {'field': 'body', 'text': 'computer'}}], 'filter': [{'term': {'path': '/tenant',
                'value': 'acme'}}], 'must_not': [{'exists': {'path': '/deleted_at'}}]}}

    """

    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        query_request_query = cls()

        query_request_query.additional_properties = d
        return query_request_query

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
