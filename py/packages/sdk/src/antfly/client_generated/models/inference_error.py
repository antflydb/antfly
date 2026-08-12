from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_error_reason import InferenceErrorReason
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceError")


@_attrs_define
class InferenceError:
    """
    Attributes:
        error (str): Stable machine-readable error code
        message (str | Unset): Human-readable error description
        reason (InferenceErrorReason | Unset): Machine-readable capacity source when the failure is retryable
        retryable (bool | Unset): Whether retrying the request may succeed
        retry_after_ms (int | Unset): Minimum retry delay in milliseconds
    """

    error: str
    message: str | Unset = UNSET
    reason: InferenceErrorReason | Unset = UNSET
    retryable: bool | Unset = UNSET
    retry_after_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        error = self.error

        message = self.message

        reason: str | Unset = UNSET
        if not isinstance(self.reason, Unset):
            reason = self.reason.value

        retryable = self.retryable

        retry_after_ms = self.retry_after_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "error": error,
            }
        )
        if message is not UNSET:
            field_dict["message"] = message
        if reason is not UNSET:
            field_dict["reason"] = reason
        if retryable is not UNSET:
            field_dict["retryable"] = retryable
        if retry_after_ms is not UNSET:
            field_dict["retry_after_ms"] = retry_after_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        error = d.pop("error")

        message = d.pop("message", UNSET)

        _reason = d.pop("reason", UNSET)
        reason: InferenceErrorReason | Unset
        if isinstance(_reason, Unset):
            reason = UNSET
        else:
            reason = InferenceErrorReason(_reason)

        retryable = d.pop("retryable", UNSET)

        retry_after_ms = d.pop("retry_after_ms", UNSET)

        inference_error = cls(
            error=error,
            message=message,
            reason=reason,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
        )

        inference_error.additional_properties = d
        return inference_error

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
