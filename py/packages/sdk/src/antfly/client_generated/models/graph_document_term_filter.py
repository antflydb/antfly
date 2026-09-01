from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphDocumentTermFilter")


@_attrs_define
class GraphDocumentTermFilter:
    """
    Attributes:
        term (str):
        path (str): RFC 6901 JSON Pointer to the stored-document value.
    """

    term: str
    path: str

    def to_dict(self) -> dict[str, Any]:
        term = self.term

        path = self.path

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "term": term,
                "path": path,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        term = d.pop("term")

        path = d.pop("path")

        graph_document_term_filter = cls(
            term=term,
            path=path,
        )

        return graph_document_term_filter
