from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_join_projection_side import RowsJoinProjectionSide

T = TypeVar("T", bound="RowsJoinProjection")


@_attrs_define
class RowsJoinProjection:
    """
    Attributes:
        as_ (str):
        side (RowsJoinProjectionSide):
        field (str):
    """

    as_: str
    side: RowsJoinProjectionSide
    field: str

    def to_dict(self) -> dict[str, Any]:
        as_ = self.as_

        side = self.side.value

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "as": as_,
                "side": side,
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        as_ = d.pop("as")

        side = RowsJoinProjectionSide(d.pop("side"))

        field = d.pop("field")

        rows_join_projection = cls(
            as_=as_,
            side=side,
            field=field,
        )

        return rows_join_projection
