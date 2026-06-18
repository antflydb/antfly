from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TablespaceCatalogRecord")


@_attrs_define
class TablespaceCatalogRecord:
    """Tablespace catalog object. SQL `CREATE TABLESPACE` maps to this lifecycle surface.

    Attributes:
        tablespace_id (int): Stable tablespace catalog identifier. Example: 42.
        name (str): Tablespace name. Example: fastspace.
        location_json (str): JSON-encoded location descriptor. String locations are encoded as JSON strings. Example:
            "/var/lib/antfly/fastspace".
        placement_policy_json (str): JSON-encoded placement policy reserved for native placement planning. Example: {}.
    """

    tablespace_id: int
    name: str
    location_json: str
    placement_policy_json: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        tablespace_id = self.tablespace_id

        name = self.name

        location_json = self.location_json

        placement_policy_json = self.placement_policy_json

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "tablespace_id": tablespace_id,
                "name": name,
                "location_json": location_json,
                "placement_policy_json": placement_policy_json,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        tablespace_id = d.pop("tablespace_id")

        name = d.pop("name")

        location_json = d.pop("location_json")

        placement_policy_json = d.pop("placement_policy_json")

        tablespace_catalog_record = cls(
            tablespace_id=tablespace_id,
            name=name,
            location_json=location_json,
            placement_policy_json=placement_policy_json,
        )

        tablespace_catalog_record.additional_properties = d
        return tablespace_catalog_record

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
