from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.sql_setting_value_type_0 import SqlSettingValueType0
    from ..models.sql_setting_value_type_1 import SqlSettingValueType1
    from ..models.sql_setting_value_type_2 import SqlSettingValueType2


T = TypeVar("T", bound="SqlSettingDatabaseDefault")


@_attrs_define
class SqlSettingDatabaseDefault:
    """
    Attributes:
        database (str):
        value (SqlSettingValueType0 | SqlSettingValueType1 | SqlSettingValueType2): One typed setting value, matching
            the declared kind.
    """

    database: str
    value: SqlSettingValueType0 | SqlSettingValueType1 | SqlSettingValueType2

    def to_dict(self) -> dict[str, Any]:
        from ..models.sql_setting_value_type_0 import SqlSettingValueType0
        from ..models.sql_setting_value_type_1 import SqlSettingValueType1

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

        sql_setting_database_default = cls(
            database=database,
            value=value,
        )

        return sql_setting_database_default
