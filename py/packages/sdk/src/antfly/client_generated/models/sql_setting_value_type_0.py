from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SqlSettingValueType0")


@_attrs_define
class SqlSettingValueType0:
    """
    Attributes:
        boolean (bool):
    """

    boolean: bool

    def to_dict(self) -> dict[str, Any]:
        boolean = self.boolean

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "boolean": boolean,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        boolean = d.pop("boolean")

        sql_setting_value_type_0 = cls(
            boolean=boolean,
        )

        return sql_setting_value_type_0
