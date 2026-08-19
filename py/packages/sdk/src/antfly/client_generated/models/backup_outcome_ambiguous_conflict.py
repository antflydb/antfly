from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.backup_outcome_ambiguous_conflict_code import BackupOutcomeAmbiguousConflictCode
from ..types import UNSET, Unset

T = TypeVar("T", bound="BackupOutcomeAmbiguousConflict")


@_attrs_define
class BackupOutcomeAmbiguousConflict:
    """
    Attributes:
        code (BackupOutcomeAmbiguousConflictCode):
        error (str): Legacy human-readable error text. Use `code` for branching.
        message (str):
        retryable (bool):
        backup_id (str): Logical backup ID whose outcome must be inspected before retrying.
        artifact_backup_id (str | Unset): Opaque artifact generation retained by an ambiguous attempt. This is for
            reconciliation, not as a replacement logical backup ID.
    """

    code: BackupOutcomeAmbiguousConflictCode
    error: str
    message: str
    retryable: bool
    backup_id: str
    artifact_backup_id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        code = self.code.value

        error = self.error

        message = self.message

        retryable = self.retryable

        backup_id = self.backup_id

        artifact_backup_id = self.artifact_backup_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "error": error,
                "message": message,
                "retryable": retryable,
                "backup_id": backup_id,
            }
        )
        if artifact_backup_id is not UNSET:
            field_dict["artifact_backup_id"] = artifact_backup_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = BackupOutcomeAmbiguousConflictCode(d.pop("code"))

        error = d.pop("error")

        message = d.pop("message")

        retryable = d.pop("retryable")

        backup_id = d.pop("backup_id")

        artifact_backup_id = d.pop("artifact_backup_id", UNSET)

        backup_outcome_ambiguous_conflict = cls(
            code=code,
            error=error,
            message=message,
            retryable=retryable,
            backup_id=backup_id,
            artifact_backup_id=artifact_backup_id,
        )

        return backup_outcome_ambiguous_conflict
