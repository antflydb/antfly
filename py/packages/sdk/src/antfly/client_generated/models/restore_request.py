from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.restore_routine_mapping import RestoreRoutineMapping


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
        routine_mappings (list[RestoreRoutineMapping] | Unset): Explicit immutable routine identity handoff for trigger
            attachments.
            Cross-catalog restores require a mapping for every referenced trigger
            function. Destination functions must already exist and be authorized
            compatible zero-argument trigger functions. Routine bodies and names
            are never imported or rebound by this operation. Mappings are part
            of the durable job and its idempotency identity.
    """

    backup_id: str
    location: str
    connection: str
    routine_mappings: list[RestoreRoutineMapping] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        location = self.location

        connection = self.connection

        routine_mappings: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.routine_mappings, Unset):
            routine_mappings = []
            for routine_mappings_item_data in self.routine_mappings:
                routine_mappings_item = routine_mappings_item_data.to_dict()
                routine_mappings.append(routine_mappings_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "backup_id": backup_id,
                "location": location,
                "connection": connection,
            }
        )
        if routine_mappings is not UNSET:
            field_dict["routine_mappings"] = routine_mappings

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.restore_routine_mapping import RestoreRoutineMapping

        d = dict(src_dict)
        backup_id = d.pop("backup_id")

        location = d.pop("location")

        connection = d.pop("connection")

        _routine_mappings = d.pop("routine_mappings", UNSET)
        routine_mappings: list[RestoreRoutineMapping] | Unset = UNSET
        if _routine_mappings is not UNSET:
            routine_mappings = []
            for routine_mappings_item_data in _routine_mappings:
                routine_mappings_item = RestoreRoutineMapping.from_dict(routine_mappings_item_data)

                routine_mappings.append(routine_mappings_item)

        restore_request = cls(
            backup_id=backup_id,
            location=location,
            connection=connection,
            routine_mappings=routine_mappings,
        )

        return restore_request
