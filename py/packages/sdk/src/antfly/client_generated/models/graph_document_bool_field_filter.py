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
        field (str):
    """

    bool_: bool
    field: str

    def to_dict(self) -> dict[str, Any]:
        bool_ = self.bool_

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "bool": bool_,
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        bool_ = d.pop("bool")

        field = d.pop("field")

        graph_document_bool_field_filter = cls(
            bool_=bool_,
            field=field,
        )

        return graph_document_bool_field_filter
