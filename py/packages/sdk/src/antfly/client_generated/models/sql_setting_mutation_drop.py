from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SqlSettingMutationDrop")


@_attrs_define
class SqlSettingMutationDrop:
    """
    Attributes:
        drop (str):
    """

    drop: str

    def to_dict(self) -> dict[str, Any]:
        drop = self.drop

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "drop": drop,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        drop = d.pop("drop")

        sql_setting_mutation_drop = cls(
            drop=drop,
        )

        return sql_setting_mutation_drop
