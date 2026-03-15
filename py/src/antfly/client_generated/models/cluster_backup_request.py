from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ClusterBackupRequest")


@_attrs_define
class ClusterBackupRequest:
    """
    Attributes:
        backup_id (str): Unique identifier for this backup. Used to reference the backup for restore operations.
            Choose a meaningful name that includes date/version information.
             Example: cluster-backup-2025-01-15.
        location (str): Storage location for the backup. Supports multiple backends:
            - Local filesystem: `file:///path/to/backup`
            - Amazon S3: `s3://bucket-name/path/to/backup`

            The backup includes all table data, indexes, and metadata.
             Example: s3://mybucket/antfly-backups/cluster/2025-01-15.
        table_names (Union[Unset, list[str]]): Optional list of tables to backup. If omitted, all tables are backed up.
             Example: ['users', 'products'].
    """

    backup_id: str
    location: str
    table_names: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        location = self.location

        table_names: Union[Unset, list[str]] = UNSET
        if not isinstance(self.table_names, Unset):
            table_names = self.table_names

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "backup_id": backup_id,
                "location": location,
            }
        )
        if table_names is not UNSET:
            field_dict["table_names"] = table_names

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        backup_id = d.pop("backup_id")

        location = d.pop("location")

        table_names = cast(list[str], d.pop("table_names", UNSET))

        cluster_backup_request = cls(
            backup_id=backup_id,
            location=location,
            table_names=table_names,
        )

        cluster_backup_request.additional_properties = d
        return cluster_backup_request

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
