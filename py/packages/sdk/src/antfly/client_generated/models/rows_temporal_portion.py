from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RowsTemporalPortion")


@_attrs_define
class RowsTemporalPortion:
    """Application-time temporal slice for update/delete mutation-source plans.

    Attributes:
        period (str): Period name declared on the relational table schema.
        from_ (Any): Inclusive lower bound encoded as the period start column's JSON type.
        to (Any): Exclusive upper bound encoded as the period end column's JSON type.
    """

    period: str
    from_: Any
    to: Any

    def to_dict(self) -> dict[str, Any]:
        period = self.period

        from_ = self.from_

        to = self.to

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "period": period,
                "from": from_,
                "to": to,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        period = d.pop("period")

        from_ = d.pop("from")

        to = d.pop("to")

        rows_temporal_portion = cls(
            period=period,
            from_=from_,
            to=to,
        )

        return rows_temporal_portion
