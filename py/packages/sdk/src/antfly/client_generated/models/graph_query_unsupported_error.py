from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_query_unsupported_error_error import GraphQueryUnsupportedErrorError
from ..models.graph_query_unsupported_error_reason import GraphQueryUnsupportedErrorReason
from ..models.graph_query_unsupported_error_status import GraphQueryUnsupportedErrorStatus

T = TypeVar("T", bound="GraphQueryUnsupportedError")


@_attrs_define
class GraphQueryUnsupportedError:
    """
    Attributes:
        status (GraphQueryUnsupportedErrorStatus):
        error (GraphQueryUnsupportedErrorError):
        message (str):
        retryable (bool):
        operation (str): Named graph operation that cannot execute exactly, or `$request` for a request-wide constraint.
        feature (str): Graph operation feature, such as `match` or `traverse`, or the rejected request feature, such as
            `order_by`, when `operation` is `$request`.
        reason (GraphQueryUnsupportedErrorReason): Stable machine-readable constraint that prevents exact public
            execution.
    """

    status: GraphQueryUnsupportedErrorStatus
    error: GraphQueryUnsupportedErrorError
    message: str
    retryable: bool
    operation: str
    feature: str
    reason: GraphQueryUnsupportedErrorReason

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        retryable = self.retryable

        operation = self.operation

        feature = self.feature

        reason = self.reason.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "retryable": retryable,
                "operation": operation,
                "feature": feature,
                "reason": reason,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = GraphQueryUnsupportedErrorStatus(d.pop("status"))

        error = GraphQueryUnsupportedErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        operation = d.pop("operation")

        feature = d.pop("feature")

        reason = GraphQueryUnsupportedErrorReason(d.pop("reason"))

        graph_query_unsupported_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
            operation=operation,
            feature=feature,
            reason=reason,
        )

        return graph_query_unsupported_error
