import datetime
from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="BackupInfo")


@_attrs_define
class BackupInfo:
    """
    Attributes:
        backup_id (str): The backup identifier Example: cluster-backup-2025-01-15.
        timestamp (datetime.datetime): When the backup was created Example: 2025-01-15T10:30:00Z.
        tables (list[str]): Tables included in the backup Example: ['users', 'products'].
        location (str): Storage location of the backup Example: s3://mybucket/antfly-backups/cluster/2025-01-15.
        antfly_version (Union[Unset, str]): Antfly version that created the backup Example: v1.0.0.
    """

    backup_id: str
    timestamp: datetime.datetime
    tables: list[str]
    location: str
    antfly_version: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        timestamp = self.timestamp.isoformat()

        tables = self.tables

        location = self.location

        antfly_version = self.antfly_version

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "backup_id": backup_id,
                "timestamp": timestamp,
                "tables": tables,
                "location": location,
            }
        )
        if antfly_version is not UNSET:
            field_dict["antfly_version"] = antfly_version

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        backup_id = d.pop("backup_id")

        timestamp = isoparse(d.pop("timestamp"))

        tables = cast(list[str], d.pop("tables"))

        location = d.pop("location")

        antfly_version = d.pop("antfly_version", UNSET)

        backup_info = cls(
            backup_id=backup_id,
            timestamp=timestamp,
            tables=tables,
            location=location,
            antfly_version=antfly_version,
        )

        backup_info.additional_properties = d
        return backup_info

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
