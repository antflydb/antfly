from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.metadata_leader_unavailable_error_code import MetadataLeaderUnavailableErrorCode
from ..models.metadata_leader_unavailable_error_error import MetadataLeaderUnavailableErrorError

T = TypeVar("T", bound="MetadataLeaderUnavailableError")


@_attrs_define
class MetadataLeaderUnavailableError:
    """The request could not establish authority with the current metadata leader.

    Attributes:
        code (MetadataLeaderUnavailableErrorCode):
        error (MetadataLeaderUnavailableErrorError):
        message (str):
        retryable (bool):
        retry_after_ms (int):
    """

    code: MetadataLeaderUnavailableErrorCode
    error: MetadataLeaderUnavailableErrorError
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
        code = MetadataLeaderUnavailableErrorCode(d.pop("code"))

        error = MetadataLeaderUnavailableErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        retry_after_ms = d.pop("retry_after_ms")

        metadata_leader_unavailable_error = cls(
            code=code,
            error=error,
            message=message,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
        )

        return metadata_leader_unavailable_error
