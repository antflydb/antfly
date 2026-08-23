from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_cross_range_mode_unsupported_error_error import GraphCrossRangeModeUnsupportedErrorError
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
    """

    status: GraphCrossRangeModeUnsupportedErrorStatus
    error: GraphCrossRangeModeUnsupportedErrorError
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
        status = GraphCrossRangeModeUnsupportedErrorStatus(d.pop("status"))

        error = GraphCrossRangeModeUnsupportedErrorError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        graph_cross_range_mode_unsupported_error = cls(
            status=status,
            error=error,
            message=message,
            retryable=retryable,
        )

        return graph_cross_range_mode_unsupported_error
