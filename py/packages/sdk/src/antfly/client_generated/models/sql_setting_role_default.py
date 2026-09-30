from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.sql_setting_value_type_0 import SqlSettingValueType0
    from ..models.sql_setting_value_type_1 import SqlSettingValueType1
    from ..models.sql_setting_value_type_2 import SqlSettingValueType2


T = TypeVar("T", bound="SqlSettingRoleDefault")


@_attrs_define
class SqlSettingRoleDefault:
    """
    Attributes:
        principal (str):
        database (str):
        value (SqlSettingValueType0 | SqlSettingValueType1 | SqlSettingValueType2): One typed setting value, matching
            the declared kind.
    """

    principal: str
    database: str
    value: SqlSettingValueType0 | SqlSettingValueType1 | SqlSettingValueType2

    def to_dict(self) -> dict[str, Any]:
        from ..models.sql_setting_value_type_0 import SqlSettingValueType0
        from ..models.sql_setting_value_type_1 import SqlSettingValueType1

        principal = self.principal

        database = self.database

        value: dict[str, Any]
        if isinstance(self.value, SqlSettingValueType0):
            value = self.value.to_dict()
        elif isinstance(self.value, SqlSettingValueType1):
            value = self.value.to_dict()
        else:
            value = self.value.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "principal": principal,
                "database": database,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_setting_value_type_0 import SqlSettingValueType0
        from ..models.sql_setting_value_type_1 import SqlSettingValueType1
        from ..models.sql_setting_value_type_2 import SqlSettingValueType2

        d = dict(src_dict)
        principal = d.pop("principal")

        database = d.pop("database")

        def _parse_value(data: object) -> SqlSettingValueType0 | SqlSettingValueType1 | SqlSettingValueType2:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_sql_setting_value_type_0 = SqlSettingValueType0.from_dict(data)

                return componentsschemas_sql_setting_value_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_sql_setting_value_type_1 = SqlSettingValueType1.from_dict(data)

                return componentsschemas_sql_setting_value_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_sql_setting_value_type_2 = SqlSettingValueType2.from_dict(data)

            return componentsschemas_sql_setting_value_type_2

        value = _parse_value(d.pop("value"))

        sql_setting_role_default = cls(
            principal=principal,
            database=database,
            value=value,
        )

        return sql_setting_role_default
