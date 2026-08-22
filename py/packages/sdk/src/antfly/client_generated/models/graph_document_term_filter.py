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
        field (str):
    """

    term: str
    field: str

    def to_dict(self) -> dict[str, Any]:
        term = self.term

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "term": term,
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        term = d.pop("term")

        field = d.pop("field")

        graph_document_term_filter = cls(
            term=term,
            field=field,
        )

        return graph_document_term_filter
