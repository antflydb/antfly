from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphDocumentPrefixFilter")


@_attrs_define
class GraphDocumentPrefixFilter:
    """
    Attributes:
        prefix (str):
        field (str):
    """

    prefix: str
    field: str

    def to_dict(self) -> dict[str, Any]:
        prefix = self.prefix

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "prefix": prefix,
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        prefix = d.pop("prefix")

        field = d.pop("field")

        graph_document_prefix_filter = cls(
            prefix=prefix,
            field=field,
        )

        return graph_document_prefix_filter
