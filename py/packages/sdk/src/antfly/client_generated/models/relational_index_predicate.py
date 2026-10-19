from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_comparison_op import RelationalComparisonOp
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalIndexPredicate")


@_attrs_define
class RelationalIndexPredicate:
    """A typed partial-index conjunct. Specify either a constant truth value,
    or a column and comparison operation, never both. Only TRUE is indexed;
    FALSE and SQL UNKNOWN are excluded. A constant false conjunct represents
    an empty index without inventing a column dependency.

        Attributes:
            column (str | Unset):
            op (RelationalComparisonOp | Unset):
            value (Any | Unset): Typed scalar operand; integer columns also accept exact decimal strings. Omission means
                NULL.
            collation (str | Unset): String comparison collation with the same semantics as ordered keys.
            constant (bool | Unset): Constant membership truth. SQL UNKNOWN is normalized to false for index membership.
                Requires reader capability version 26.
    """

    column: str | Unset = UNSET
    op: RelationalComparisonOp | Unset = UNSET
    value: Any | Unset = UNSET
    collation: str | Unset = UNSET
    constant: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        column = self.column

        op: str | Unset = UNSET
        if not isinstance(self.op, Unset):
            op = self.op.value

        value = self.value

        collation = self.collation

        constant = self.constant

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if column is not UNSET:
            field_dict["column"] = column
        if op is not UNSET:
            field_dict["op"] = op
        if value is not UNSET:
            field_dict["value"] = value
        if collation is not UNSET:
            field_dict["collation"] = collation
        if constant is not UNSET:
            field_dict["constant"] = constant

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        column = d.pop("column", UNSET)

        _op = d.pop("op", UNSET)
        op: RelationalComparisonOp | Unset
        if isinstance(_op, Unset):
            op = UNSET
        else:
            op = RelationalComparisonOp(_op)

        value = d.pop("value", UNSET)

        collation = d.pop("collation", UNSET)

        constant = d.pop("constant", UNSET)

        relational_index_predicate = cls(
            column=column,
            op=op,
            value=value,
            collation=collation,
            constant=constant,
        )

        return relational_index_predicate
