from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_period_range_type import RelationalPeriodRangeType
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalPeriod")


@_attrs_define
class RelationalPeriod:
    """Application-time period over a start and end column.

    Attributes:
        name (str): Period name used by temporal constraints and `FOR PORTION OF` mutation-source plans.
        start_column (str): Inclusive period start column.
        end_column (str): Exclusive period end column.
        range_type (RelationalPeriodRangeType | Unset): Optional PostgreSQL range type that produced this period when
            lowering range-column temporal DDL.
    """

    name: str
    start_column: str
    end_column: str
    range_type: RelationalPeriodRangeType | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        start_column = self.start_column

        end_column = self.end_column

        range_type: str | Unset = UNSET
        if not isinstance(self.range_type, Unset):
            range_type = self.range_type.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "start_column": start_column,
                "end_column": end_column,
            }
        )
        if range_type is not UNSET:
            field_dict["range_type"] = range_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        start_column = d.pop("start_column")

        end_column = d.pop("end_column")

        _range_type = d.pop("range_type", UNSET)
        range_type: RelationalPeriodRangeType | Unset
        if isinstance(_range_type, Unset):
            range_type = UNSET
        else:
            range_type = RelationalPeriodRangeType(_range_type)

        relational_period = cls(
            name=name,
            start_column=start_column,
            end_column=end_column,
            range_type=range_type,
        )

        return relational_period
