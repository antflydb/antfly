from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphDocumentWildcardFilter")


@_attrs_define
class GraphDocumentWildcardFilter:
    """
    Attributes:
        wildcard (str):
        field (str):
    """

    wildcard: str
    field: str

    def to_dict(self) -> dict[str, Any]:
        wildcard = self.wildcard

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "wildcard": wildcard,
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        wildcard = d.pop("wildcard")

        field = d.pop("field")

        graph_document_wildcard_filter = cls(
            wildcard=wildcard,
            field=field,
        )

        return graph_document_wildcard_filter
