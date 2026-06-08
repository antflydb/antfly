from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_array_update_transform_op import RowsArrayUpdateTransformOp

T = TypeVar("T", bound="RowsArrayUpdateTransform")


@_attrs_define
class RowsArrayUpdateTransform:
    """Array transform for a declared `array` column.

    Attributes:
        field (str): Declared `array` column to update.
        op (RowsArrayUpdateTransformOp):
        value (Any): JSON value to append, remove, or add if absent.
    """

    field: str
    op: RowsArrayUpdateTransformOp
    value: Any

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        op = self.op.value

        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
                "op": op,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        op = RowsArrayUpdateTransformOp(d.pop("op"))

        value = d.pop("value")

        rows_array_update_transform = cls(
            field=field,
            op=op,
            value=value,
        )

        return rows_array_update_transform
