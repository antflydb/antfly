from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RowsCoalesceValueOperand")


@_attrs_define
class RowsCoalesceValueOperand:
    """
    Attributes:
        value (Any): Literal JSON fallback value.
    """

    value: Any

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        value = d.pop("value")

        rows_coalesce_value_operand = cls(
            value=value,
        )

        return rows_coalesce_value_operand
