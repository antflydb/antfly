from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.table_storage_unreadable_error_code import TableStorageUnreadableErrorCode

T = TypeVar("T", bound="TableStorageUnreadableError")


@_attrs_define
class TableStorageUnreadableError:
    """A non-retryable table-storage integrity or format failure.

    Attributes:
        code (TableStorageUnreadableErrorCode): Stable client-routing code for unreadable table storage.
        error (str): Concrete storage error class, such as InvalidManifest.
        message (str): Human-readable summary.
        retryable (bool): Always false; recovery requires repair, restore, or table replacement.
    """

    code: TableStorageUnreadableErrorCode
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
        code = TableStorageUnreadableErrorCode(d.pop("code"))

        error = d.pop("error")

        message = d.pop("message")

        retryable = d.pop("retryable")

        table_storage_unreadable_error = cls(
            code=code,
            error=error,
            message=message,
            retryable=retryable,
        )

        return table_storage_unreadable_error
