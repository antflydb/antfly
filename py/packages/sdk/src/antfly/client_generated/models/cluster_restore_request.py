from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

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
        connection (str): Required configured `external_io` connection with the `restore.read` capability.
        table_names (list[str] | Unset): Optional list of tables to restore. If omitted, all tables in the backup are
            restored.
             Example: ['users', 'products'].
        restore_mode (ClusterRestoreRequestRestoreMode | Unset): How to handle existing tables:
            - `fail_if_exists`: Abort if any table already exists (default)
            - `skip_if_exists`: Skip existing tables, restore others
            - `overwrite`: Atomically replace existing table generations after staging and validation
             Default: ClusterRestoreRequestRestoreMode.FAIL_IF_EXISTS. Example: skip_if_exists.
    """

    backup_id: str
    location: str
    connection: str
    table_names: list[str] | Unset = UNSET
    restore_mode: ClusterRestoreRequestRestoreMode | Unset = ClusterRestoreRequestRestoreMode.FAIL_IF_EXISTS

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        location = self.location

        connection = self.connection

        table_names: list[str] | Unset = UNSET
        if not isinstance(self.table_names, Unset):
            table_names = self.table_names

        restore_mode: str | Unset = UNSET
        if not isinstance(self.restore_mode, Unset):
            restore_mode = self.restore_mode.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "backup_id": backup_id,
                "location": location,
                "connection": connection,
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

        connection = d.pop("connection")

        table_names = cast(list[str], d.pop("table_names", UNSET))

        _restore_mode = d.pop("restore_mode", UNSET)
        restore_mode: ClusterRestoreRequestRestoreMode | Unset
        if isinstance(_restore_mode, Unset):
            restore_mode = UNSET
        else:
            restore_mode = ClusterRestoreRequestRestoreMode(_restore_mode)

        cluster_restore_request = cls(
            backup_id=backup_id,
            location=location,
            connection=connection,
            table_names=table_names,
            restore_mode=restore_mode,
        )

        return cluster_restore_request
