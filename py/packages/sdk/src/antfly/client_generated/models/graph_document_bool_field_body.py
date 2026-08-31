from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphDocumentBoolFieldBody")


@_attrs_define
class GraphDocumentBoolFieldBody:
    """
    Attributes:
        path (str): RFC 6901 JSON Pointer to the stored-document value.
        value (bool):
    """

    path: str
    value: bool

    def to_dict(self) -> dict[str, Any]:
        path = self.path

        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "path": path,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        path = d.pop("path")

        value = d.pop("value")

        graph_document_bool_field_body = cls(
            path=path,
            value=value,
        )

        return graph_document_bool_field_body
