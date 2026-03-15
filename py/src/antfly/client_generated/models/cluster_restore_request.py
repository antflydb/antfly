from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cluster_restore_request_restore_mode import ClusterRestoreRequestRestoreMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="ClusterRestoreRequest")


@_attrs_define
class ClusterRestoreRequest:
    """
    Attributes:
        backup_id (str): Unique identifier of the backup to restore from.
             Example: cluster-backup-2025-01-15.
        location (str): Storage location where the backup is stored.
             Example: s3://mybucket/antfly-backups/cluster/2025-01-15.
        table_names (Union[Unset, list[str]]): Optional list of tables to restore. If omitted, all tables in the backup
            are restored.
             Example: ['users', 'products'].
        restore_mode (Union[Unset, ClusterRestoreRequestRestoreMode]): How to handle existing tables:
            - `fail_if_exists`: Abort if any table already exists (default)
            - `skip_if_exists`: Skip existing tables, restore others
            - `overwrite`: Drop and recreate existing tables
             Default: ClusterRestoreRequestRestoreMode.FAIL_IF_EXISTS. Example: skip_if_exists.
    """

    backup_id: str
    location: str
    table_names: Union[Unset, list[str]] = UNSET
    restore_mode: Union[Unset, ClusterRestoreRequestRestoreMode] = ClusterRestoreRequestRestoreMode.FAIL_IF_EXISTS
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        location = self.location

        table_names: Union[Unset, list[str]] = UNSET
        if not isinstance(self.table_names, Unset):
            table_names = self.table_names

        restore_mode: Union[Unset, str] = UNSET
        if not isinstance(self.restore_mode, Unset):
            restore_mode = self.restore_mode.value

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
        if restore_mode is not UNSET:
            field_dict["restore_mode"] = restore_mode

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        backup_id = d.pop("backup_id")

        location = d.pop("location")

        table_names = cast(list[str], d.pop("table_names", UNSET))

        _restore_mode = d.pop("restore_mode", UNSET)
        restore_mode: Union[Unset, ClusterRestoreRequestRestoreMode]
        if isinstance(_restore_mode, Unset):
            restore_mode = UNSET
        else:
            restore_mode = ClusterRestoreRequestRestoreMode(_restore_mode)

        cluster_restore_request = cls(
            backup_id=backup_id,
            location=location,
            table_names=table_names,
            restore_mode=restore_mode,
        )

        cluster_restore_request.additional_properties = d
        return cluster_restore_request

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
