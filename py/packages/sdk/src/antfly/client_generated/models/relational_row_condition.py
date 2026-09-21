from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_comparison_op import RelationalComparisonOp
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalRowCondition")


@_attrs_define
class RelationalRowCondition:
    """
    Attributes:
        column (str):
        op (RelationalComparisonOp):
        value (Any | Unset): Typed scalar operand. Omission means NULL. Integer columns also accept exact decimal
            strings.
        collation (str | Unset):
    """

    column: str
    op: RelationalComparisonOp
    value: Any | Unset = UNSET
    collation: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        column = self.column

        op = self.op.value

        value = self.value

        collation = self.collation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "column": column,
                "op": op,
            }
        )
        if value is not UNSET:
            field_dict["value"] = value
        if collation is not UNSET:
            field_dict["collation"] = collation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        column = d.pop("column")

        op = RelationalComparisonOp(d.pop("op"))

        value = d.pop("value", UNSET)

        collation = d.pop("collation", UNSET)

        relational_row_condition = cls(
            column=column,
            op=op,
            value=value,
            collation=collation,
        )

        return relational_row_condition
