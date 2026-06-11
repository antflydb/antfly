from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.rows_coalesce_field_operand import RowsCoalesceFieldOperand
    from ..models.rows_coalesce_value_operand import RowsCoalesceValueOperand


T = TypeVar("T", bound="RowsCoalesceProjection")


@_attrs_define
class RowsCoalesceProjection:
    """Compact COALESCE projection.

    Attributes:
        as_ (str): Output field name.
        operands (list[RowsCoalesceFieldOperand | RowsCoalesceValueOperand]):
    """

    as_: str
    operands: list[RowsCoalesceFieldOperand | RowsCoalesceValueOperand]

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_coalesce_field_operand import RowsCoalesceFieldOperand

        as_ = self.as_

        operands = []
        for operands_item_data in self.operands:
            operands_item: dict[str, Any]
            if isinstance(operands_item_data, RowsCoalesceFieldOperand):
                operands_item = operands_item_data.to_dict()
            else:
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
        from ..models.rows_coalesce_field_operand import RowsCoalesceFieldOperand
        from ..models.rows_coalesce_value_operand import RowsCoalesceValueOperand

        d = dict(src_dict)
        as_ = d.pop("as")

        operands = []
        _operands = d.pop("operands")
        for operands_item_data in _operands:

            def _parse_operands_item(data: object) -> RowsCoalesceFieldOperand | RowsCoalesceValueOperand:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_rows_coalesce_operand_type_0 = RowsCoalesceFieldOperand.from_dict(data)

                    return componentsschemas_rows_coalesce_operand_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_rows_coalesce_operand_type_1 = RowsCoalesceValueOperand.from_dict(data)

                return componentsschemas_rows_coalesce_operand_type_1

            operands_item = _parse_operands_item(operands_item_data)

            operands.append(operands_item)

        rows_coalesce_projection = cls(
            as_=as_,
            operands=operands,
        )

        return rows_coalesce_projection
