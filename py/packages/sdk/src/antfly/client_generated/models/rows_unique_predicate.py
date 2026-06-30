from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_unique_predicate_op import RowsUniquePredicateOp
from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsUniquePredicate")


@_attrs_define
class RowsUniquePredicate:
    """Predicate atom that must match a partial unique constraint definition.

    Attributes:
        field (str):
        op (RowsUniquePredicateOp):
        value (Any | Unset): Predicate comparison value. Omit for null-test operators.
    """

    field: str
    op: RowsUniquePredicateOp
    value: Any | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        op = self.op.value

        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
                "op": op,
            }
        )
        if value is not UNSET:
            field_dict["value"] = value

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        op = RowsUniquePredicateOp(d.pop("op"))

        value = d.pop("value", UNSET)

        rows_unique_predicate = cls(
            field=field,
            op=op,
            value=value,
        )

        return rows_unique_predicate
