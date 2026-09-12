from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="BatchCommittedFailure")


@_attrs_define
class BatchCommittedFailure:
    """Additive details for a committed batch that needs operator action. The
    open string code is forward-compatible with older SDKs; clients should
    treat unknown codes as non-retryable when `retryable` is false.

        Attributes:
            code (str): Stable machine-readable failure code, such as `graph_metric_materialization_rejected`.
            message (str): Actionable operator guidance.
            retryable (bool): Whether replaying the document mutation is safe. Committed repair outcomes are false.
            reason (str | Unset): Optional stable reason within the failure category, such as `build_budget_exceeded`.
    """

    code: str
    message: str
    retryable: bool
    reason: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        code = self.code

        message = self.message

        retryable = self.retryable

        reason = self.reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "message": message,
                "retryable": retryable,
            }
        )
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = d.pop("code")

        message = d.pop("message")

        retryable = d.pop("retryable")

        reason = d.pop("reason", UNSET)

        batch_committed_failure = cls(
            code=code,
            message=message,
            retryable=retryable,
            reason=reason,
        )

        batch_committed_failure.additional_properties = d
        return batch_committed_failure

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
