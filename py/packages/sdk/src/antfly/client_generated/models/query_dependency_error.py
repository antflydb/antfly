from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_dependency_error_code import QueryDependencyErrorCode

T = TypeVar("T", bound="QueryDependencyError")


@_attrs_define
class QueryDependencyError:
    """A stable failure envelope for query embedding and reranking dependencies.

    Attributes:
        code (QueryDependencyErrorCode):
        error (str): Legacy alias of code. Use code for programmatic handling.
        message (str):
        retryable (bool):
    """

    code: QueryDependencyErrorCode
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
        code = QueryDependencyErrorCode(d.pop("code"))

        error = d.pop("error")

        message = d.pop("message")

        retryable = d.pop("retryable")

        query_dependency_error = cls(
            code=code,
            error=error,
            message=message,
            retryable=retryable,
        )

        return query_dependency_error
