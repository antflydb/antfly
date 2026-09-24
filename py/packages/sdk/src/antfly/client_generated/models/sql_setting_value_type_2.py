from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SqlSettingValueType2")


@_attrs_define
class SqlSettingValueType2:
    """
    Attributes:
        string (str):
    """

    string: str

    def to_dict(self) -> dict[str, Any]:
        string = self.string

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "string": string,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        string = d.pop("string")

        sql_setting_value_type_2 = cls(
            string=string,
        )

        return sql_setting_value_type_2
