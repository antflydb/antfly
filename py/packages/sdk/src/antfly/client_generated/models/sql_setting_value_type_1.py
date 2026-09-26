from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SqlSettingValueType1")


@_attrs_define
class SqlSettingValueType1:
    """
    Attributes:
        integer (int):
    """

    integer: int

    def to_dict(self) -> dict[str, Any]:
        integer = self.integer

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "integer": integer,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        integer = d.pop("integer")

        sql_setting_value_type_1 = cls(
            integer=integer,
        )

        return sql_setting_value_type_1
