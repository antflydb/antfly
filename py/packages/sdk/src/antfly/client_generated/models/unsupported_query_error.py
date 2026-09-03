from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.unsupported_query_error_error import UnsupportedQueryErrorError
from ..models.unsupported_query_error_status import UnsupportedQueryErrorStatus

T = TypeVar("T", bound="UnsupportedQueryError")


@_attrs_define
class UnsupportedQueryError:
    """A query requests an unsupported feature without a more specific structured diagnostic.

    Attributes:
        status (UnsupportedQueryErrorStatus):
        error (UnsupportedQueryErrorError): Stable machine-readable error code.
        message (str): Human-readable error summary.
        retryable (bool): Retrying the same request cannot succeed without changing it.
    """

    status: UnsupportedQueryErrorStatus
    error: UnsupportedQueryErrorError
    message: str
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = UnsupportedQueryErrorStatus(d.pop("status"))

        error = UnsupportedQueryErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        unsupported_query_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
        )

        return unsupported_query_error
