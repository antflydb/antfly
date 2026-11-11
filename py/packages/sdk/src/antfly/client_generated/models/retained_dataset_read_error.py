from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RetainedDatasetReadError")


@_attrs_define
class RetainedDatasetReadError:
    """
    Attributes:
        code (str): Stable machine-readable domain failure Example: retained_series_numeric_overflow.
        message (str):
        retryable (bool):
    """

    code: str
    message: str
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        code = self.code

        message = self.message

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "message": message,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = d.pop("code")

        message = d.pop("message")

        retryable = d.pop("retryable")

        retained_dataset_read_error = cls(
            code=code,
            message=message,
            retryable=retryable,
        )

        return retained_dataset_read_error
