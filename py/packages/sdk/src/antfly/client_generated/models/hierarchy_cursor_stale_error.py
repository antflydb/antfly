from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.hierarchy_cursor_stale_error_action import HierarchyCursorStaleErrorAction
from ..models.hierarchy_cursor_stale_error_error import HierarchyCursorStaleErrorError
from ..models.hierarchy_cursor_stale_error_restart_without import HierarchyCursorStaleErrorRestartWithout
from ..models.hierarchy_cursor_stale_error_status import HierarchyCursorStaleErrorStatus

T = TypeVar("T", bound="HierarchyCursorStaleError")


@_attrs_define
class HierarchyCursorStaleError:
    """A hierarchy traversal cursor bound to an older source-artifact revision.

    Attributes:
        status (HierarchyCursorStaleErrorStatus):
        error (HierarchyCursorStaleErrorError): Stable machine-readable error code.
        message (str): Human-readable explanation of why traversal cannot continue.
        action (HierarchyCursorStaleErrorAction): Stable client action for recovering from the conflict.
        restart_without (HierarchyCursorStaleErrorRestartWithout): Request field to omit when restarting at the first
            unit.
        retryable (bool): Retrying the same cursor cannot succeed; restart traversal instead.
    """

    status: HierarchyCursorStaleErrorStatus
    error: HierarchyCursorStaleErrorError
    message: str
    action: HierarchyCursorStaleErrorAction
    restart_without: HierarchyCursorStaleErrorRestartWithout
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        action = self.action.value

        restart_without = self.restart_without.value

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "action": action,
                "restart_without": restart_without,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = HierarchyCursorStaleErrorStatus(d.pop("status"))

        error = HierarchyCursorStaleErrorError(d.pop("error"))

        message = d.pop("message")

        action = HierarchyCursorStaleErrorAction(d.pop("action"))

        restart_without = HierarchyCursorStaleErrorRestartWithout(d.pop("restart_without"))

        retryable = d.pop("retryable")

        hierarchy_cursor_stale_error = cls(
            status=status,
            error=error,
            message=message,
            action=action,
            restart_without=restart_without,
            retryable=retryable,
        )

        return hierarchy_cursor_stale_error
