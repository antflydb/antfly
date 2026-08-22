from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="Error")


@_attrs_define
class Error:
    """
    Attributes:
        error (str): Legacy human-readable error text. Example: An error message.
        code (str | Unset): Optional stable machine-readable error code for programmatic handling.
        message (str | Unset): Human-readable error description when supplied by the endpoint.
        retryable (bool | Unset): Whether retrying the operation may succeed without changing the request.
        retry_after_ms (int | Unset): Suggested minimum retry delay in milliseconds.
    """

    error: str
    code: str | Unset = UNSET
    message: str | Unset = UNSET
    retryable: bool | Unset = UNSET
    retry_after_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        error = self.error

        code = self.code

        message = self.message

        retryable = self.retryable

        retry_after_ms = self.retry_after_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "error": error,
            }
        )
        if code is not UNSET:
            field_dict["code"] = code
        if message is not UNSET:
            field_dict["message"] = message
        if retryable is not UNSET:
            field_dict["retryable"] = retryable
        if retry_after_ms is not UNSET:
            field_dict["retry_after_ms"] = retry_after_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        error = d.pop("error")

        code = d.pop("code", UNSET)

        message = d.pop("message", UNSET)

        retryable = d.pop("retryable", UNSET)

        retry_after_ms = d.pop("retry_after_ms", UNSET)

        error = cls(
            error=error,
            code=code,
            message=message,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
        )

        error.additional_properties = d
        return error

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
