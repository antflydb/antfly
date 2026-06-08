from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

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
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        as_ = self.as_

        side = self.side.value

        field = self.field

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
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

        rows_join_projection.additional_properties = d
        return rows_join_projection

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
