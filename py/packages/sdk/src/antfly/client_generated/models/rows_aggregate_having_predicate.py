from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_aggregate_having_predicate_op import RowsAggregateHavingPredicateOp
from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsAggregateHavingPredicate")


@_attrs_define
class RowsAggregateHavingPredicate:
    """Predicate over emitted aggregate output fields, evaluated after grouping.

    Attributes:
        field (str): Emitted aggregate output field name, usually an aggregation `name`, group key, or expression group
            alias.
        op (RowsAggregateHavingPredicateOp):
        value (Any | Unset): Comparison value. Omit for `is_null` and `is_not_null`.
    """

    field: str
    op: RowsAggregateHavingPredicateOp
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

        op = RowsAggregateHavingPredicateOp(d.pop("op"))

        value = d.pop("value", UNSET)

        rows_aggregate_having_predicate = cls(
            field=field,
            op=op,
            value=value,
        )

        return rows_aggregate_having_predicate
