from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="RowsJsonSetTransform")


@_attrs_define
class RowsJsonSetTransform:
    """JSON path assignment for a declared `json` column.

    Attributes:
        field (str): Declared `json` column to update.
        path (list[str]): Non-empty path under the JSON column.
        value (Any): JSON value to write at the path.
    """

    field: str
    path: list[str]
    value: Any

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        path = self.path

        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
                "path": path,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        path = cast(list[str], d.pop("path"))

        value = d.pop("value")

        rows_json_set_transform = cls(
            field=field,
            path=path,
            value=value,
        )

        return rows_json_set_transform
