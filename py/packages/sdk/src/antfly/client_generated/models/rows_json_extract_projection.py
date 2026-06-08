from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsJsonExtractProjection")


@_attrs_define
class RowsJsonExtractProjection:
    """Compact JSON path projection over a declared `json` column.

    Attributes:
        as_ (str): Output field name.
        field (str): Declared `json` column to read.
        path (Any): Non-empty JSON path, encoded as a dot path string or array of path components.
        as_text (bool | Unset): Return the extracted value as text, matching SQL `->>` behavior. Default: False.
    """

    as_: str
    field: str
    path: Any
    as_text: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        as_ = self.as_

        field = self.field

        path = self.path

        as_text = self.as_text

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "as": as_,
                "field": field,
                "path": path,
            }
        )
        if as_text is not UNSET:
            field_dict["as_text"] = as_text

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        as_ = d.pop("as")

        field = d.pop("field")

        path = d.pop("path")

        as_text = d.pop("as_text", UNSET)

        rows_json_extract_projection = cls(
            as_=as_,
            field=field,
            path=path,
            as_text=as_text,
        )

        return rows_json_extract_projection
