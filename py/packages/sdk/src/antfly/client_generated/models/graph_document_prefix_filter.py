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
        path (str): RFC 6901 JSON Pointer to the stored-document value.
    """

    prefix: str
    path: str

    def to_dict(self) -> dict[str, Any]:
        prefix = self.prefix

        path = self.path

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "prefix": prefix,
                "path": path,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        prefix = d.pop("prefix")

        path = d.pop("path")

        graph_document_prefix_filter = cls(
            prefix=prefix,
            path=path,
        )

        return graph_document_prefix_filter
