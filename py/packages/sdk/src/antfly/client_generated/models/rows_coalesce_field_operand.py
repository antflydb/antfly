from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RowsCoalesceFieldOperand")


@_attrs_define
class RowsCoalesceFieldOperand:
    """
    Attributes:
        field (str): Declared column to read.
    """

    field: str

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        rows_coalesce_field_operand = cls(
            field=field,
        )

        return rows_coalesce_field_operand
