from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="RowsLateralCorrelation")


@_attrs_define
class RowsLateralCorrelation:
    """
    Attributes:
        left_field (str):
        right_field (str):
    """

    left_field: str
    right_field: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        left_field = self.left_field

        right_field = self.right_field

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "left_field": left_field,
                "right_field": right_field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        left_field = d.pop("left_field")

        right_field = d.pop("right_field")

        rows_lateral_correlation = cls(
            left_field=left_field,
            right_field=right_field,
        )

        rows_lateral_correlation.additional_properties = d
        return rows_lateral_correlation

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
