from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.unsupported_hierarchy_grouping_error_action import UnsupportedHierarchyGroupingErrorAction
from ..models.unsupported_hierarchy_grouping_error_error import UnsupportedHierarchyGroupingErrorError
from ..models.unsupported_hierarchy_grouping_error_field import UnsupportedHierarchyGroupingErrorField
from ..models.unsupported_hierarchy_grouping_error_reason import UnsupportedHierarchyGroupingErrorReason
from ..models.unsupported_hierarchy_grouping_error_status import UnsupportedHierarchyGroupingErrorStatus

T = TypeVar("T", bound="UnsupportedHierarchyGroupingError")


@_attrs_define
class UnsupportedHierarchyGroupingError:
    """A requested hierarchy grouping level cannot represent every member because at least one selected source lacks
    durable document-unit identity.

        Attributes:
            status (UnsupportedHierarchyGroupingErrorStatus):
            error (UnsupportedHierarchyGroupingErrorError): Stable machine-readable error code.
            message (str): Human-readable remediation using only public query fields.
            reason (UnsupportedHierarchyGroupingErrorReason): Stable reason the requested hierarchy level is unavailable.
            field (UnsupportedHierarchyGroupingErrorField): Public request field that selected the unsupported grouping
                level.
            action (UnsupportedHierarchyGroupingErrorAction): Select source grouping, omit group_by for direct members, or
                use an index whose every source is unit-backed.
            retryable (bool): Retrying the same request cannot succeed without changing its hierarchy grouping.
    """

    status: UnsupportedHierarchyGroupingErrorStatus
    error: UnsupportedHierarchyGroupingErrorError
    message: str
    reason: UnsupportedHierarchyGroupingErrorReason
    field: UnsupportedHierarchyGroupingErrorField
    action: UnsupportedHierarchyGroupingErrorAction
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        error = self.error.value

        message = self.message

        reason = self.reason.value

        field = self.field.value

        action = self.action.value

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "error": error,
                "message": message,
                "reason": reason,
                "field": field,
                "action": action,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = UnsupportedHierarchyGroupingErrorStatus(d.pop("status"))

        error = UnsupportedHierarchyGroupingErrorError(d.pop("error"))

        message = d.pop("message")

        reason = UnsupportedHierarchyGroupingErrorReason(d.pop("reason"))

        field = UnsupportedHierarchyGroupingErrorField(d.pop("field"))

        action = UnsupportedHierarchyGroupingErrorAction(d.pop("action"))

        retryable = d.pop("retryable")

        unsupported_hierarchy_grouping_error = cls(
            status=status,
            error=error,
            message=message,
            reason=reason,
            field=field,
            action=action,
            retryable=retryable,
        )

        return unsupported_hierarchy_grouping_error
