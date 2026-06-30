from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RowsJoinOn")


@_attrs_define
class RowsJoinOn:
    """
    Attributes:
        left_field (str):
        right_field (str):
    """

    left_field: str
    right_field: str

    def to_dict(self) -> dict[str, Any]:
        left_field = self.left_field

        right_field = self.right_field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "left_field": left_field,
                "right_field": right_field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        left_field = d.pop("left_field")

        right_field = d.pop("right_field")

        rows_join_on = cls(
            left_field=left_field,
            right_field=right_field,
        )

        return rows_join_on
