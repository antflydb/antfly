from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphDocumentBoolFieldFilter")


@_attrs_define
class GraphDocumentBoolFieldFilter:
    """
    Attributes:
        bool_ (bool):
        path (str): RFC 6901 JSON Pointer to the stored-document value.
    """

    bool_: bool
    path: str

    def to_dict(self) -> dict[str, Any]:
        bool_ = self.bool_

        path = self.path

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "bool": bool_,
                "path": path,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        bool_ = d.pop("bool")

        path = d.pop("path")

        graph_document_bool_field_filter = cls(
            bool_=bool_,
            path=path,
        )

        return graph_document_bool_field_filter
