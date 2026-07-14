from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.cluster_backup_request_format import ClusterBackupRequestFormat
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
            - Scoped filesystem connection: `file:///logical/path`
            - Amazon S3: `s3://bucket-name/path/to/backup`
            - Google Cloud Storage: `gs://bucket-name/path/to/backup`

            The backup includes all table data, indexes, and metadata.
             Example: s3://mybucket/antfly-backups/cluster/2025-01-15.
        connection (str): Required configured `external_io` connection with the `backup.write` capability.
        format_ (ClusterBackupRequestFormat | Unset): Backup format to use:
            - `native`: Engine-specific physical snapshot (fast backup and restore, same-backend only)
            - `portable`: Cross-backend logical backup in AFB format (slower restore due to index rebuild, but can be
            restored by any Antfly backend)

            On restore, the format is auto-detected from file magic bytes.
             Default: ClusterBackupRequestFormat.PORTABLE. Example: portable.
        table_names (list[str] | Unset): Optional list of tables to backup. If omitted, all tables are backed up,
            up to the cluster backup limit of 4096 tables. Requests above that limit
            fail before any table backup is created.
             Example: ['users', 'products'].
    """

    backup_id: str
    location: str
    connection: str
    format_: ClusterBackupRequestFormat | Unset = ClusterBackupRequestFormat.PORTABLE
    table_names: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        location = self.location

        connection = self.connection

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        table_names: list[str] | Unset = UNSET
        if not isinstance(self.table_names, Unset):
            table_names = self.table_names

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "backup_id": backup_id,
                "location": location,
                "connection": connection,
            }
        )
        if format_ is not UNSET:
            field_dict["format"] = format_
        if table_names is not UNSET:
            field_dict["table_names"] = table_names

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        backup_id = d.pop("backup_id")

        location = d.pop("location")

        connection = d.pop("connection")

        _format_ = d.pop("format", UNSET)
        format_: ClusterBackupRequestFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = ClusterBackupRequestFormat(_format_)

        table_names = cast(list[str], d.pop("table_names", UNSET))

        cluster_backup_request = cls(
            backup_id=backup_id,
            location=location,
            connection=connection,
            format_=format_,
            table_names=table_names,
        )

        return cluster_backup_request
