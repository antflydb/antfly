from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_cross_range_mode_unsupported_error_error import GraphCrossRangeModeUnsupportedErrorError
from ..models.graph_cross_range_mode_unsupported_error_reason import GraphCrossRangeModeUnsupportedErrorReason
from ..models.graph_cross_range_mode_unsupported_error_status import GraphCrossRangeModeUnsupportedErrorStatus

T = TypeVar("T", bound="GraphCrossRangeModeUnsupportedError")


@_attrs_define
class GraphCrossRangeModeUnsupportedError:
    """
    Attributes:
        status (GraphCrossRangeModeUnsupportedErrorStatus):
        error (GraphCrossRangeModeUnsupportedErrorError):
        message (str):
        retryable (bool):
        operation (str): Named graph operation that cannot execute exactly, or `$request` for a request-wide constraint.
        mode (str): Graph operation mode, or `graph_queries` for a request-wide constraint.
        reason (GraphCrossRangeModeUnsupportedErrorReason): Stable machine-readable constraint that prevents exact
            cross-range execution.
    """

    status: GraphCrossRangeModeUnsupportedErrorStatus
    error: GraphCrossRangeModeUnsupportedErrorError
    message: str
    retryable: bool
    operation: str
    mode: str
    reason: GraphCrossRangeModeUnsupportedErrorReason

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
        status = GraphCrossRangeModeUnsupportedErrorStatus(d.pop("status"))

        error = GraphCrossRangeModeUnsupportedErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        operation = d.pop("operation")

        mode = d.pop("mode")

        reason = GraphCrossRangeModeUnsupportedErrorReason(d.pop("reason"))

        graph_cross_range_mode_unsupported_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
            operation=operation,
            mode=mode,
            reason=reason,
        )

        return graph_cross_range_mode_unsupported_error
