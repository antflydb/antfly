from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DatabaseCatalogRecord")


@_attrs_define
class DatabaseCatalogRecord:
    """Database catalog object. Tables and namespaces resolve under a database before authorization and routing.

    Attributes:
        database_id (int): Stable database catalog identifier. Example: 22.
        name (str): Database name. Example: tenant_ops.
        settings_json (str): JSON-encoded database settings owned by the catalog. Example: {}.
        tablespace_name (None | str | Unset): Optional durable tablespace binding inherited by new namespace/table
            placement policy. Example: fastspace.
    """

    database_id: int
    name: str
    settings_json: str
    tablespace_name: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        database_id = self.database_id

        name = self.name

        settings_json = self.settings_json

        tablespace_name: None | str | Unset
        if isinstance(self.tablespace_name, Unset):
            tablespace_name = UNSET
        else:
            tablespace_name = self.tablespace_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "database_id": database_id,
                "name": name,
                "settings_json": settings_json,
            }
        )
        if tablespace_name is not UNSET:
            field_dict["tablespace_name"] = tablespace_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        database_id = d.pop("database_id")

        name = d.pop("name")

        settings_json = d.pop("settings_json")

        def _parse_tablespace_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        tablespace_name = _parse_tablespace_name(d.pop("tablespace_name", UNSET))

        database_catalog_record = cls(
            database_id=database_id,
            name=name,
            settings_json=settings_json,
            tablespace_name=tablespace_name,
        )

        database_catalog_record.additional_properties = d
        return database_catalog_record

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
