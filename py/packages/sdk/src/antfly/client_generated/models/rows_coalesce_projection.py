from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.rows_coalesce_operand import RowsCoalesceOperand


T = TypeVar("T", bound="RowsCoalesceProjection")


@_attrs_define
class RowsCoalesceProjection:
    """Compact COALESCE projection.

    Attributes:
        as_ (str): Output field name.
        operands (list[RowsCoalesceOperand]):
    """

    as_: str
    operands: list[RowsCoalesceOperand]

    def to_dict(self) -> dict[str, Any]:
        as_ = self.as_

        operands = []
        for operands_item_data in self.operands:
            operands_item = operands_item_data.to_dict()
            operands.append(operands_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "as": as_,
                "operands": operands,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_coalesce_operand import RowsCoalesceOperand

        d = dict(src_dict)
        as_ = d.pop("as")

        operands = []
        _operands = d.pop("operands")
        for operands_item_data in _operands:
            operands_item = RowsCoalesceOperand.from_dict(operands_item_data)

            operands.append(operands_item)

        rows_coalesce_projection = cls(
            as_=as_,
            operands=operands,
        )

        return rows_coalesce_projection
