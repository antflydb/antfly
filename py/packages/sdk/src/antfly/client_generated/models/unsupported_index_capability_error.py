from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.unsupported_index_capability_error_error import UnsupportedIndexCapabilityErrorError
from ..models.unsupported_index_capability_error_message import UnsupportedIndexCapabilityErrorMessage

T = TypeVar("T", bound="UnsupportedIndexCapabilityError")


@_attrs_define
class UnsupportedIndexCapabilityError:
    """The requested index depends on a capability unavailable in the current deployment.

    Attributes:
        error (UnsupportedIndexCapabilityErrorError):
        message (UnsupportedIndexCapabilityErrorMessage):
        retryable (bool):
    """

    error: UnsupportedIndexCapabilityErrorError
    message: UnsupportedIndexCapabilityErrorMessage
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        error = self.error.value

        message = self.message.value

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "error": error,
                "message": message,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        error = UnsupportedIndexCapabilityErrorError(d.pop("error"))

        message = UnsupportedIndexCapabilityErrorMessage(d.pop("message"))

        retryable = d.pop("retryable")

        unsupported_index_capability_error = cls(
            error=error,
            message=message,
            retryable=retryable,
        )

        return unsupported_index_capability_error
