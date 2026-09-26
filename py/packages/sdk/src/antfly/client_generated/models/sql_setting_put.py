from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_setting_put_kind import SqlSettingPutKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_setting_database_default import SqlSettingDatabaseDefault
    from ..models.sql_setting_role_default import SqlSettingRoleDefault
    from ..models.sql_setting_value_type_0 import SqlSettingValueType0
    from ..models.sql_setting_value_type_1 import SqlSettingValueType1
    from ..models.sql_setting_value_type_2 import SqlSettingValueType2


T = TypeVar("T", bound="SqlSettingPut")


@_attrs_define
class SqlSettingPut:
    """
    Attributes:
        name (str):
        kind (SqlSettingPutKind):
        default (SqlSettingValueType0 | SqlSettingValueType1 | SqlSettingValueType2): One typed setting value, matching
            the declared kind.
        policy_sensitive (bool | Unset):
        session_writable (bool | Unset):
        database_defaults (list[SqlSettingDatabaseDefault] | Unset):
        role_defaults (list[SqlSettingRoleDefault] | Unset):
    """

    name: str
    kind: SqlSettingPutKind
    default: SqlSettingValueType0 | SqlSettingValueType1 | SqlSettingValueType2
    policy_sensitive: bool | Unset = UNSET
    session_writable: bool | Unset = UNSET
    database_defaults: list[SqlSettingDatabaseDefault] | Unset = UNSET
    role_defaults: list[SqlSettingRoleDefault] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.sql_setting_value_type_0 import SqlSettingValueType0
        from ..models.sql_setting_value_type_1 import SqlSettingValueType1

        name = self.name

        kind = self.kind.value

        default: dict[str, Any]
        if isinstance(self.default, SqlSettingValueType0):
            default = self.default.to_dict()
        elif isinstance(self.default, SqlSettingValueType1):
            default = self.default.to_dict()
        else:
            default = self.default.to_dict()

        policy_sensitive = self.policy_sensitive

        session_writable = self.session_writable

        database_defaults: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.database_defaults, Unset):
            database_defaults = []
            for database_defaults_item_data in self.database_defaults:
                database_defaults_item = database_defaults_item_data.to_dict()
                database_defaults.append(database_defaults_item)

        role_defaults: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.role_defaults, Unset):
            role_defaults = []
            for role_defaults_item_data in self.role_defaults:
                role_defaults_item = role_defaults_item_data.to_dict()
                role_defaults.append(role_defaults_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "kind": kind,
                "default": default,
            }
        )
        if policy_sensitive is not UNSET:
            field_dict["policy_sensitive"] = policy_sensitive
        if session_writable is not UNSET:
            field_dict["session_writable"] = session_writable
        if database_defaults is not UNSET:
            field_dict["database_defaults"] = database_defaults
        if role_defaults is not UNSET:
            field_dict["role_defaults"] = role_defaults

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_setting_database_default import SqlSettingDatabaseDefault
        from ..models.sql_setting_role_default import SqlSettingRoleDefault
        from ..models.sql_setting_value_type_0 import SqlSettingValueType0
        from ..models.sql_setting_value_type_1 import SqlSettingValueType1
        from ..models.sql_setting_value_type_2 import SqlSettingValueType2

        d = dict(src_dict)
        name = d.pop("name")

        kind = SqlSettingPutKind(d.pop("kind"))

        def _parse_default(data: object) -> SqlSettingValueType0 | SqlSettingValueType1 | SqlSettingValueType2:
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

        default = _parse_default(d.pop("default"))

        policy_sensitive = d.pop("policy_sensitive", UNSET)

        session_writable = d.pop("session_writable", UNSET)

        _database_defaults = d.pop("database_defaults", UNSET)
        database_defaults: list[SqlSettingDatabaseDefault] | Unset = UNSET
        if _database_defaults is not UNSET:
            database_defaults = []
            for database_defaults_item_data in _database_defaults:
                database_defaults_item = SqlSettingDatabaseDefault.from_dict(database_defaults_item_data)

                database_defaults.append(database_defaults_item)

        _role_defaults = d.pop("role_defaults", UNSET)
        role_defaults: list[SqlSettingRoleDefault] | Unset = UNSET
        if _role_defaults is not UNSET:
            role_defaults = []
            for role_defaults_item_data in _role_defaults:
                role_defaults_item = SqlSettingRoleDefault.from_dict(role_defaults_item_data)

                role_defaults.append(role_defaults_item)

        sql_setting_put = cls(
            name=name,
            kind=kind,
            default=default,
            policy_sensitive=policy_sensitive,
            session_writable=session_writable,
            database_defaults=database_defaults,
            role_defaults=role_defaults,
        )

        return sql_setting_put
