from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.storage_resource_exhausted_error_code import StorageResourceExhaustedErrorCode
from ..models.storage_resource_exhausted_error_error import StorageResourceExhaustedErrorError

T = TypeVar("T", bound="StorageResourceExhaustedError")


@_attrs_define
class StorageResourceExhaustedError:
    """Actionable retry contract for temporary storage descriptor exhaustion.

    Attributes:
        code (StorageResourceExhaustedErrorCode):
        error (StorageResourceExhaustedErrorError):
        message (str):
        retryable (bool):
        retry_after_ms (int):
    """

    code: StorageResourceExhaustedErrorCode
    error: StorageResourceExhaustedErrorError
    message: str
    retryable: bool
    retry_after_ms: int

    def to_dict(self) -> dict[str, Any]:
        code = self.code.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        retry_after_ms = self.retry_after_ms

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "error": error,
                "message": message,
                "retryable": retryable,
                "retry_after_ms": retry_after_ms,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = StorageResourceExhaustedErrorCode(d.pop("code"))

        error = StorageResourceExhaustedErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        retry_after_ms = d.pop("retry_after_ms")

        storage_resource_exhausted_error = cls(
            code=code,
            error=error,
            message=message,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
        )

        return storage_resource_exhausted_error
