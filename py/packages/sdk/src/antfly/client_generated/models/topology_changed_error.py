from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.topology_changed_error_action import TopologyChangedErrorAction
from ..models.topology_changed_error_error import TopologyChangedErrorError
from ..models.topology_changed_error_status import TopologyChangedErrorStatus

T = TypeVar("T", bound="TopologyChangedError")


@_attrs_define
class TopologyChangedError:
    """The table topology changed while a query was running after Antfly's bounded internal retry.

    Attributes:
        status (TopologyChangedErrorStatus):
        error (TopologyChangedErrorError): Stable machine-readable error code.
        message (str): Human-readable explanation of why the query must be retried.
        action (TopologyChangedErrorAction): Stable client action for recovering from the conflict.
        retryable (bool): Retrying the complete query against fresh topology may succeed.
    """

    status: TopologyChangedErrorStatus
    error: TopologyChangedErrorError
    message: str
    action: TopologyChangedErrorAction
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        action = self.action.value

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "action": action,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = TopologyChangedErrorStatus(d.pop("status"))

        error = TopologyChangedErrorError(d.pop("error"))

        message = d.pop("message")

        action = TopologyChangedErrorAction(d.pop("action"))

        retryable = d.pop("retryable")

        topology_changed_error = cls(
            status=status,
            error=error,
            message=message,
            action=action,
            retryable=retryable,
        )

        return topology_changed_error
