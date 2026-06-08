from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsCoalesceOperand")


@_attrs_define
class RowsCoalesceOperand:
    """Compact COALESCE operand. Exactly one of `field` or `value` is accepted by the server.

    Attributes:
        field (str | Unset): Declared column to read.
        value (Any | Unset): Literal JSON fallback value.
    """

    field: str | Unset = UNSET
    value: Any | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if field is not UNSET:
            field_dict["field"] = field
        if value is not UNSET:
            field_dict["value"] = value

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field", UNSET)

        value = d.pop("value", UNSET)

        rows_coalesce_operand = cls(
            field=field,
            value=value,
        )

        return rows_coalesce_operand
