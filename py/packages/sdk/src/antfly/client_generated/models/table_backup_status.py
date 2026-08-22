from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.table_backup_status_code import TableBackupStatusCode
from ..models.table_backup_status_status import TableBackupStatusStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="TableBackupStatus")


@_attrs_define
class TableBackupStatus:
    """Outcome for one table in a cluster backup. `successful` is the legacy
    spelling emitted by pre-Zig coordinators; new coordinators emit `completed`.

    An `ambiguous` outcome includes `error`, `code`, `retryable: false`,
    `backup_id`, and `artifact_backup_id` so callers can reconcile the retained
    generation without retrying blindly. Failed and skipped outcomes may include
    `error`; other fields are omitted when they do not apply.

        Attributes:
            name (str): Table name Example: users.
            status (TableBackupStatusStatus):  Example: completed.
            error (str | Unset): Human-readable failure, skip reason, or reconciliation guidance.
            code (TableBackupStatusCode | Unset): Stable machine-readable code for an ambiguous outcome.
            retryable (bool | Unset): False for an ambiguous outcome; inspect the retained attempt before retrying.
            backup_id (str | Unset): Logical per-table backup ID retained by an ambiguous cluster attempt.
            artifact_backup_id (str | Unset): Opaque artifact generation retained by an ambiguous cluster attempt.
    """

    name: str
    status: TableBackupStatusStatus
    error: str | Unset = UNSET
    code: TableBackupStatusCode | Unset = UNSET
    retryable: bool | Unset = UNSET
    backup_id: str | Unset = UNSET
    artifact_backup_id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        status = self.status.value

        error = self.error

        code: str | Unset = UNSET
        if not isinstance(self.code, Unset):
            code = self.code.value

        retryable = self.retryable

        backup_id = self.backup_id

        artifact_backup_id = self.artifact_backup_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "status": status,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error
        if code is not UNSET:
            field_dict["code"] = code
        if retryable is not UNSET:
            field_dict["retryable"] = retryable
        if backup_id is not UNSET:
            field_dict["backup_id"] = backup_id
        if artifact_backup_id is not UNSET:
            field_dict["artifact_backup_id"] = artifact_backup_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        status = TableBackupStatusStatus(d.pop("status"))

        error = d.pop("error", UNSET)

        _code = d.pop("code", UNSET)
        code: TableBackupStatusCode | Unset
        if isinstance(_code, Unset):
            code = UNSET
        else:
            code = TableBackupStatusCode(_code)

        retryable = d.pop("retryable", UNSET)

        backup_id = d.pop("backup_id", UNSET)

        artifact_backup_id = d.pop("artifact_backup_id", UNSET)

        table_backup_status = cls(
            name=name,
            status=status,
            error=error,
            code=code,
            retryable=retryable,
            backup_id=backup_id,
            artifact_backup_id=artifact_backup_id,
        )

        return table_backup_status
