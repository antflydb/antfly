from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.backup_request_format import BackupRequestFormat
from ..types import UNSET, Unset

T = TypeVar("T", bound="BackupRequest")


@_attrs_define
class BackupRequest:
    """
    Attributes:
        backup_id (str): Unique identifier for this backup. Used to reference the backup for restore operations.
            Choose a meaningful name that includes date/version information.
             Example: backup-2025-01-15-v2.
        location (str): Storage location for the backup. Supports multiple backends:
            - Local filesystem: `file:///path/to/backup`
            - Amazon S3: `s3://bucket-name/path/to/backup`

            The backup includes all table data, indexes, and metadata for the specified table.
             Example: s3://mybucket/antfly-backups/users-table/2025-01-15.
        format_ (BackupRequestFormat | Unset): Backup format to use:
            - `native`: Engine-specific physical snapshot (fast backup and restore, same-backend only)
            - `portable`: Cross-backend logical backup in AFB format (slower restore due to index rebuild, but can be
            restored by any Antfly backend)

            On restore, the format is auto-detected from file magic bytes.
             Default: BackupRequestFormat.PORTABLE. Example: portable.
    """

    backup_id: str
    location: str
    format_: BackupRequestFormat | Unset = BackupRequestFormat.PORTABLE
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        location = self.location

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "backup_id": backup_id,
                "location": location,
            }
        )
        if format_ is not UNSET:
            field_dict["format"] = format_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        backup_id = d.pop("backup_id")

        location = d.pop("location")

        _format_ = d.pop("format", UNSET)
        format_: BackupRequestFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = BackupRequestFormat(_format_)

        backup_request = cls(
            backup_id=backup_id,
            location=location,
            format_=format_,
        )

        backup_request.additional_properties = d
        return backup_request

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
