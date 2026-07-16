from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.dense_repair_backpressure_error_code import DenseRepairBackpressureErrorCode

T = TypeVar("T", bound="DenseRepairBackpressureError")


@_attrs_define
class DenseRepairBackpressureError:
    """A dense-index rebuild is retaining replay history and the node has reached its hard safety budget.

    Attributes:
        code (DenseRepairBackpressureErrorCode):
        message (str):
        retryable (bool):
        retry_after_ms (int): Suggested delay before retrying the write.
    """

    code: DenseRepairBackpressureErrorCode
    message: str
    retryable: bool
    retry_after_ms: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        code = self.code.value

        message = self.message

        retryable = self.retryable

        retry_after_ms = self.retry_after_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "message": message,
                "retryable": retryable,
                "retry_after_ms": retry_after_ms,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = DenseRepairBackpressureErrorCode(d.pop("code"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        retry_after_ms = d.pop("retry_after_ms")

        dense_repair_backpressure_error = cls(
            code=code,
            message=message,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
        )

        dense_repair_backpressure_error.additional_properties = d
        return dense_repair_backpressure_error

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
