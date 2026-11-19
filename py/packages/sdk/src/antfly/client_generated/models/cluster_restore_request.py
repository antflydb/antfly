from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.cluster_restore_request_restore_mode import ClusterRestoreRequestRestoreMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.restore_routine_mapping import RestoreRoutineMapping


T = TypeVar("T", bound="ClusterRestoreRequest")


@_attrs_define
class ClusterRestoreRequest:
    """Native cohort restores use the existing asynchronous restore job to provision
    hidden fresh generations, import rows, rebuild indexes and coordinated constraints,
    and publish the dependency-complete target set atomically. Document, relational,
    and mixed native cohorts use the same workflow (at most 128 tables/4096 ranges).
    Skipping a live parent cannot substitute it for a parent generation required by
    a restored child. Overwrite retains the old generation until validation and
    cutover; cancellation after publication completes publication rather than rollback.
    Reserved destination authorization is immutable: changing principal requires
    canceling the old job and creating a new restore.

        Attributes:
            backup_id (str): Unique identifier of the backup to restore from.
                 Example: cluster-backup-2025-01-15.
            location (str): Storage location where the backup is stored.
                 Example: s3://mybucket/antfly-backups/cluster/2025-01-15.
            connection (str): Required configured `external_io` connection with the `restore.read` capability.
            routine_mappings (list[RestoreRoutineMapping] | Unset): Explicit immutable routine identity handoff for trigger
                attachments.
                Cross-catalog restores require a mapping for every referenced trigger
                function. Destination functions must already exist and be authorized
                compatible zero-argument trigger functions. Routine bodies and names
                are never imported or rebound by this operation. Mappings are part
                of the durable job and its idempotency identity.
            table_names (list[str] | Unset): Optional list of tables to restore. If omitted, all tables in the backup are
                restored,
                up to the cluster restore limit of 4096 tables. Larger backups must be restored
                in explicit batches of at most 256 tables.
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
    routine_mappings: list[RestoreRoutineMapping] | Unset = UNSET
    table_names: list[str] | Unset = UNSET
    restore_mode: ClusterRestoreRequestRestoreMode | Unset = ClusterRestoreRequestRestoreMode.FAIL_IF_EXISTS

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
        if routine_mappings is not UNSET:
            field_dict["routine_mappings"] = routine_mappings
        if table_names is not UNSET:
            field_dict["table_names"] = table_names
        if restore_mode is not UNSET:
            field_dict["restore_mode"] = restore_mode

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
            routine_mappings=routine_mappings,
            table_names=table_names,
            restore_mode=restore_mode,
        )

        return cluster_restore_request
