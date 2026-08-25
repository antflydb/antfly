from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_query_mode_unsupported_error_error import GraphQueryModeUnsupportedErrorError
from ..models.graph_query_mode_unsupported_error_reason import GraphQueryModeUnsupportedErrorReason
from ..models.graph_query_mode_unsupported_error_status import GraphQueryModeUnsupportedErrorStatus

T = TypeVar("T", bound="GraphQueryModeUnsupportedError")


@_attrs_define
class GraphQueryModeUnsupportedError:
    """
    Attributes:
        status (GraphQueryModeUnsupportedErrorStatus):
        error (GraphQueryModeUnsupportedErrorError):
        message (str):
        retryable (bool):
        operation (str): Named graph operation that cannot execute exactly, or `$request` for a request-wide constraint.
        mode (str): Graph operation mode, or `graph_queries` for a request-wide constraint.
        reason (GraphQueryModeUnsupportedErrorReason): Stable machine-readable constraint that prevents exact public
            execution.
    """

    status: GraphQueryModeUnsupportedErrorStatus
    error: GraphQueryModeUnsupportedErrorError
    message: str
    retryable: bool
    operation: str
    mode: str
    reason: GraphQueryModeUnsupportedErrorReason

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        operation = self.operation

        mode = self.mode

        reason = self.reason.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "retryable": retryable,
                "operation": operation,
                "mode": mode,
                "reason": reason,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = GraphQueryModeUnsupportedErrorStatus(d.pop("status"))

        error = GraphQueryModeUnsupportedErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        operation = d.pop("operation")

        mode = d.pop("mode")

        reason = GraphQueryModeUnsupportedErrorReason(d.pop("reason"))

        graph_query_mode_unsupported_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
            operation=operation,
            mode=mode,
            reason=reason,
        )

        return graph_query_mode_unsupported_error
