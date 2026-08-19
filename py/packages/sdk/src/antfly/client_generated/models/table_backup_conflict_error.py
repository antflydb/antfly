from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.table_backup_conflict_error_code import TableBackupConflictErrorCode

T = TypeVar("T", bound="TableBackupConflictError")


@_attrs_define
class TableBackupConflictError:
    """A non-retryable table-backup conflict. For an ambiguous outcome, inspect the requested backup ID before deciding
    whether to start new work.

        Attributes:
            code (TableBackupConflictErrorCode):
            error (str): Legacy human-readable error text. Use `code` for branching.
            message (str):
            retryable (bool):
    """

    code: TableBackupConflictErrorCode
    error: str
    message: str
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        code = self.code.value

        error = self.error

        message = self.message

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "error": error,
                "message": message,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = TableBackupConflictErrorCode(d.pop("code"))

        error = d.pop("error")

        message = d.pop("message")

        retryable = d.pop("retryable")

        table_backup_conflict_error = cls(
            code=code,
            error=error,
            message=message,
            retryable=retryable,
        )

        return table_backup_conflict_error
