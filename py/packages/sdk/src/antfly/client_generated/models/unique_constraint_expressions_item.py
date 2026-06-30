from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.unique_constraint_expressions_item_op import UniqueConstraintExpressionsItemOp

T = TypeVar("T", bound="UniqueConstraintExpressionsItem")


@_attrs_define
class UniqueConstraintExpressionsItem:
    """
    Attributes:
        op (UniqueConstraintExpressionsItemOp):
        field (str):
    """

    op: UniqueConstraintExpressionsItemOp
    field: str

    def to_dict(self) -> dict[str, Any]:
        op = self.op.value

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "op": op,
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        op = UniqueConstraintExpressionsItemOp(d.pop("op"))

        field = d.pop("field")

        unique_constraint_expressions_item = cls(
            op=op,
            field=field,
        )

        return unique_constraint_expressions_item
