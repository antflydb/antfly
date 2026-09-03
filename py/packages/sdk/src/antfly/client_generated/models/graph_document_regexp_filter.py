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
        path (str): RFC 6901 JSON Pointer to the stored-document value.
    """

    regexp: str
    path: str

    def to_dict(self) -> dict[str, Any]:
        regexp = self.regexp

        path = self.path

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "regexp": regexp,
                "path": path,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        regexp = d.pop("regexp")

        path = d.pop("path")

        graph_document_regexp_filter = cls(
            regexp=regexp,
            path=path,
        )

        return graph_document_regexp_filter
