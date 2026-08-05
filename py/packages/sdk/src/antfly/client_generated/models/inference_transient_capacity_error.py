from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_transient_capacity_error_reason import InferenceTransientCapacityErrorReason

T = TypeVar("T", bound="InferenceTransientCapacityError")


@_attrs_define
class InferenceTransientCapacityError:
    """Actionable retry contract for temporary inference-capacity failures.

    Attributes:
        error (str): Stable machine-readable error code
        message (str): Human-readable error description
        reason (InferenceTransientCapacityErrorReason): Machine-readable capacity source
        retryable (bool): Always true for a transient-capacity response
        retry_after_ms (int): Minimum retry delay in milliseconds
    """

    error: str
    message: str
    reason: InferenceTransientCapacityErrorReason
    retryable: bool
    retry_after_ms: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        error = self.error

        message = self.message

        reason = self.reason.value

        retryable = self.retryable

        retry_after_ms = self.retry_after_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "error": error,
                "message": message,
                "reason": reason,
                "retryable": retryable,
                "retry_after_ms": retry_after_ms,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        error = d.pop("error")

        message = d.pop("message")

        reason = InferenceTransientCapacityErrorReason(d.pop("reason"))

        retryable = d.pop("retryable")

        retry_after_ms = d.pop("retry_after_ms")

        inference_transient_capacity_error = cls(
            error=error,
            message=message,
            reason=reason,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
        )

        inference_transient_capacity_error.additional_properties = d
        return inference_transient_capacity_error

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
