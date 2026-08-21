from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_temporarily_unavailable_error_code import QueryTemporarilyUnavailableErrorCode

T = TypeVar("T", bound="QueryTemporarilyUnavailableError")


@_attrs_define
class QueryTemporarilyUnavailableError:
    """A transient query dependency or read-availability failure that is safe to retry.

    Attributes:
        code (QueryTemporarilyUnavailableErrorCode): Stable machine-readable retry classification.
        message (str): Human-readable error summary.
        retryable (bool): Always true; retry after the response's Retry-After delay.
    """

    code: QueryTemporarilyUnavailableErrorCode
    message: str
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        code = self.code.value

        message = self.message

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "message": message,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = QueryTemporarilyUnavailableErrorCode(d.pop("code"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        query_temporarily_unavailable_error = cls(
            code=code,
            message=message,
            retryable=retryable,
        )

        return query_temporarily_unavailable_error
