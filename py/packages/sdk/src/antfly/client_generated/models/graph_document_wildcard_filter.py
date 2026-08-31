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
        path (str): RFC 6901 JSON Pointer to the stored-document value.
    """

    wildcard: str
    path: str

    def to_dict(self) -> dict[str, Any]:
        wildcard = self.wildcard

        path = self.path

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "wildcard": wildcard,
                "path": path,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        wildcard = d.pop("wildcard")

        path = d.pop("path")

        graph_document_wildcard_filter = cls(
            wildcard=wildcard,
            path=path,
        )

        return graph_document_wildcard_filter
