from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="NamespaceCatalogRecord")


@_attrs_define
class NamespaceCatalogRecord:
    """Namespace catalog object inside a database. PostgreSQL schemas map to Antfly namespaces.

    Attributes:
        namespace_id (int): Stable namespace catalog identifier. Example: 222.
        database_id (int): Parent database identifier. Example: 22.
        database_name (str): Parent database name. Example: tenant_ops.
        name (str): Namespace name. Example: analytics.
        tablespace_name (None | str | Unset): Optional durable tablespace binding inherited by new table placement
            policy in this namespace. Example: fastspace.
    """

    namespace_id: int
    database_id: int
    database_name: str
    name: str
    tablespace_name: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        namespace_id = self.namespace_id

        database_id = self.database_id

        database_name = self.database_name

        name = self.name

        tablespace_name: None | str | Unset
        if isinstance(self.tablespace_name, Unset):
            tablespace_name = UNSET
        else:
            tablespace_name = self.tablespace_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "namespace_id": namespace_id,
                "database_id": database_id,
                "database_name": database_name,
                "name": name,
            }
        )
        if tablespace_name is not UNSET:
            field_dict["tablespace_name"] = tablespace_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        namespace_id = d.pop("namespace_id")

        database_id = d.pop("database_id")

        database_name = d.pop("database_name")

        name = d.pop("name")

        def _parse_tablespace_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        tablespace_name = _parse_tablespace_name(d.pop("tablespace_name", UNSET))

        namespace_catalog_record = cls(
            namespace_id=namespace_id,
            database_id=database_id,
            database_name=database_name,
            name=name,
            tablespace_name=tablespace_name,
        )

        namespace_catalog_record.additional_properties = d
        return namespace_catalog_record

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
