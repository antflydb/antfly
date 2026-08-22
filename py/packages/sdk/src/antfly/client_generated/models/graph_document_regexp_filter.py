from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphDocumentRegexpFilter")


@_attrs_define
class GraphDocumentRegexpFilter:
    """
    Attributes:
        regexp (str):
        field (str):
    """

    regexp: str
    field: str

    def to_dict(self) -> dict[str, Any]:
        regexp = self.regexp

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "regexp": regexp,
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        regexp = d.pop("regexp")

        field = d.pop("field")

        graph_document_regexp_filter = cls(
            regexp=regexp,
            field=field,
        )

        return graph_document_regexp_filter
