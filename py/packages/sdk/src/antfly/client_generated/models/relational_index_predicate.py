from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_comparison_op import RelationalComparisonOp
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalIndexPredicate")


@_attrs_define
class RelationalIndexPredicate:
    """A typed partial-index conjunct. Only TRUE is indexed; FALSE and SQL UNKNOWN are excluded.

    Attributes:
        column (str):
        op (RelationalComparisonOp):
        value (Any | Unset): Typed scalar operand; integer columns also accept exact decimal strings. Omission means
            NULL.
        collation (str | Unset): String comparison collation with the same semantics as ordered keys.
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

        relational_index_predicate = cls(
            column=column,
            op=op,
            value=value,
            collation=collation,
        )

        return relational_index_predicate
