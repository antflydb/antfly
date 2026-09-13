from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_comparison_op import RelationalComparisonOp
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalCheckConstraint")


@_attrs_define
class RelationalCheckConstraint:
    """A typed scalar CHECK. New writes are checked from schema publication;
    existing rows are validated separately. SQL UNKNOWN satisfies CHECK.
    Comparison values must match the column type. Integer values may also
    use exact decimal strings to avoid client-side floating-point rounding.

        Attributes:
            name (str):
            column (str):
            op (RelationalComparisonOp):
            value (Any | Unset): Scalar comparison operand. Omission represents NULL. Null tests require a NULL operand.
            collation (str | Unset): String comparison collation; uses the same rules as ordered indexes.
    """

    name: str
    column: str
    op: RelationalComparisonOp
    value: Any | Unset = UNSET
    collation: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        column = self.column

        op = self.op.value

        value = self.value

        collation = self.collation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
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
        name = d.pop("name")

        column = d.pop("column")

        op = RelationalComparisonOp(d.pop("op"))

        value = d.pop("value", UNSET)

        collation = d.pop("collation", UNSET)

        relational_check_constraint = cls(
            name=name,
            column=column,
            op=op,
            value=value,
            collation=collation,
        )

        return relational_check_constraint
