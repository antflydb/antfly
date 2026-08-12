from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceGenerateBatchError")


@_attrs_define
class InferenceGenerateBatchError:
    """
    Attributes:
        code (str): Stable machine-readable item error code, including `CONTENT_TOO_LARGE` for aggregate media-budget
            failures.
        message (str):
        retryable (bool):  Default: False.
        retry_after_ms (int | None | Unset): Minimum retry delay in milliseconds for a retryable capacity failure.
    """

    code: str
    message: str
    retryable: bool = False
    retry_after_ms: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        code = self.code

        message = self.message

        retryable = self.retryable

        retry_after_ms: int | None | Unset
        if isinstance(self.retry_after_ms, Unset):
            retry_after_ms = UNSET
        else:
            retry_after_ms = self.retry_after_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "message": message,
                "retryable": retryable,
            }
        )
        if retry_after_ms is not UNSET:
            field_dict["retry_after_ms"] = retry_after_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = d.pop("code")

        message = d.pop("message")

        retryable = d.pop("retryable")

        def _parse_retry_after_ms(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        retry_after_ms = _parse_retry_after_ms(d.pop("retry_after_ms", UNSET))

        inference_generate_batch_error = cls(
            code=code,
            message=message,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
        )

        inference_generate_batch_error.additional_properties = d
        return inference_generate_batch_error

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
