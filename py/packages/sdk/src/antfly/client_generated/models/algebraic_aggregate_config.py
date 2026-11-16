from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.algebraic_aggregate_config_op import AlgebraicAggregateConfigOp
from ..types import UNSET, Unset

T = TypeVar("T", bound="AlgebraicAggregateConfig")


@_attrs_define
class AlgebraicAggregateConfig:
    """
    Attributes:
        name (str):
        op (AlgebraicAggregateConfigOp):
        group_by (list[str] | Unset):
        time (str | Unset): Schema datetime column used for temporal grouping. Requires bucket.
        bucket (str | Unset): UTC calendar unit (minute, hour, day, week, month, quarter, year) or positive fixed
            elapsed duration such as 5m. Requires time. Fixed buckets align to the Unix epoch.
        measure (str | Unset): Required except for count. Omitted count means COUNT(*); a supplied column means
            COUNT(column), excluding SQL NULL values.
    """

    name: str
    op: AlgebraicAggregateConfigOp
    group_by: list[str] | Unset = UNSET
    time: str | Unset = UNSET
    bucket: str | Unset = UNSET
    measure: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        op = self.op.value

        group_by: list[str] | Unset = UNSET
        if not isinstance(self.group_by, Unset):
            group_by = self.group_by

        time = self.time

        bucket = self.bucket

        measure = self.measure

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "op": op,
            }
        )
        if group_by is not UNSET:
            field_dict["group_by"] = group_by
        if time is not UNSET:
            field_dict["time"] = time
        if bucket is not UNSET:
            field_dict["bucket"] = bucket
        if measure is not UNSET:
            field_dict["measure"] = measure

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        op = AlgebraicAggregateConfigOp(d.pop("op"))

        group_by = cast(list[str], d.pop("group_by", UNSET))

        time = d.pop("time", UNSET)

        bucket = d.pop("bucket", UNSET)

        measure = d.pop("measure", UNSET)

        algebraic_aggregate_config = cls(
            name=name,
            op=op,
            group_by=group_by,
            time=time,
            bucket=bucket,
            measure=measure,
        )

        return algebraic_aggregate_config
