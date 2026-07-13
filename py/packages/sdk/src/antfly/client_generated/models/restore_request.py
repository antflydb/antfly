from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RestoreRequest")


@_attrs_define
class RestoreRequest:
    """
    Attributes:
        backup_id (str): Identifier of the published backup to restore. Example: backup-2025-01-15-v2.
        location (str): Storage location containing the backup. The server detects the
            native or portable format from the published manifest and artifact.
             Example: s3://mybucket/antfly-backups/users-table/2025-01-15.
        connection (str): ID of a configured `external_io` connection with `restore.read`.
            Object locations enforce bucket and prefix scopes; filesystem URI
            paths resolve beneath the connection root.
    """

    backup_id: str
    location: str
    connection: str

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        location = self.location

        connection = self.connection

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "backup_id": backup_id,
                "location": location,
                "connection": connection,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        backup_id = d.pop("backup_id")

        location = d.pop("location")

        connection = d.pop("connection")

        restore_request = cls(
            backup_id=backup_id,
            location=location,
            connection=connection,
        )

        return restore_request
