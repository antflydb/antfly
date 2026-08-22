from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.metadata_capability_unavailable_error_code import MetadataCapabilityUnavailableErrorCode
from ..models.metadata_capability_unavailable_error_error import MetadataCapabilityUnavailableErrorError
from ..models.metadata_capability_unavailable_error_required_capability import (
    MetadataCapabilityUnavailableErrorRequiredCapability,
)

T = TypeVar("T", bound="MetadataCapabilityUnavailableError")


@_attrs_define
class MetadataCapabilityUnavailableError:
    """The metadata service does not yet provide the consistency capability required by backup.

    Attributes:
        code (MetadataCapabilityUnavailableErrorCode):
        error (MetadataCapabilityUnavailableErrorError): Legacy human-readable error text. Use `code` for branching.
        message (str):
        required_capability (MetadataCapabilityUnavailableErrorRequiredCapability):
        retryable (bool):
        retry_after_ms (int):
    """

    code: MetadataCapabilityUnavailableErrorCode
    error: MetadataCapabilityUnavailableErrorError
    message: str
    required_capability: MetadataCapabilityUnavailableErrorRequiredCapability
    retryable: bool
    retry_after_ms: int

    def to_dict(self) -> dict[str, Any]:
        code = self.code.value

        error = self.error.value

        message = self.message

        required_capability = self.required_capability.value

        retryable = self.retryable

        retry_after_ms = self.retry_after_ms

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "error": error,
                "message": message,
                "required_capability": required_capability,
                "retryable": retryable,
                "retry_after_ms": retry_after_ms,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = MetadataCapabilityUnavailableErrorCode(d.pop("code"))

        error = MetadataCapabilityUnavailableErrorError(d.pop("error"))

        message = d.pop("message")

        required_capability = MetadataCapabilityUnavailableErrorRequiredCapability(d.pop("required_capability"))

        retryable = d.pop("retryable")

        retry_after_ms = d.pop("retry_after_ms")

        metadata_capability_unavailable_error = cls(
            code=code,
            error=error,
            message=message,
            required_capability=required_capability,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
        )

        return metadata_capability_unavailable_error
