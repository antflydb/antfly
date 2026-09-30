from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.sql_setting_put import SqlSettingPut


T = TypeVar("T", bound="SqlSettingMutationPut")


@_attrs_define
class SqlSettingMutationPut:
    """
    Attributes:
        put (SqlSettingPut):
    """

    put: SqlSettingPut

    def to_dict(self) -> dict[str, Any]:
        put = self.put.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "put": put,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_setting_put import SqlSettingPut

        d = dict(src_dict)
        put = SqlSettingPut.from_dict(d.pop("put"))

        sql_setting_mutation_put = cls(
            put=put,
        )

        return sql_setting_mutation_put
