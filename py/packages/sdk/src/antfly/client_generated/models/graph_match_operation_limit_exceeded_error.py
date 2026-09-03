from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_match_operation_limit_exceeded_error_error import GraphMatchOperationLimitExceededErrorError
from ..models.graph_match_operation_limit_exceeded_error_status import GraphMatchOperationLimitExceededErrorStatus

T = TypeVar("T", bound="GraphMatchOperationLimitExceededError")


@_attrs_define
class GraphMatchOperationLimitExceededError:
    """
    Attributes:
        status (GraphMatchOperationLimitExceededErrorStatus):
        error (GraphMatchOperationLimitExceededErrorError):
        message (str):
        retryable (bool):
        maximum (int): Maximum named MATCH operations accepted in one request.
        actual (int): Named MATCH operations supplied by the request.
    """

    status: GraphMatchOperationLimitExceededErrorStatus
    error: GraphMatchOperationLimitExceededErrorError
    message: str
    retryable: bool
    maximum: int
    actual: int

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        maximum = self.maximum

        actual = self.actual

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "retryable": retryable,
                "maximum": maximum,
                "actual": actual,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = GraphMatchOperationLimitExceededErrorStatus(d.pop("status"))

        error = GraphMatchOperationLimitExceededErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        maximum = d.pop("maximum")

        actual = d.pop("actual")

        graph_match_operation_limit_exceeded_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
            maximum=maximum,
            actual=actual,
        )

        return graph_match_operation_limit_exceeded_error
