from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="CreateRetainedSubscriptionBody")


@_attrs_define
class CreateRetainedSubscriptionBody:
    """
    Attributes:
        start_ns (str | Unset): Inclusive signed decimal nanoseconds.
        end_ns (str | Unset): Exclusive signed decimal nanoseconds.
        window_ns (str | Unset): Sliding window width in decimal nanoseconds, aligned to the dataset interval.
    """

    start_ns: str | Unset = UNSET
    end_ns: str | Unset = UNSET
    window_ns: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        start_ns = self.start_ns

        end_ns = self.end_ns

        window_ns = self.window_ns

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if start_ns is not UNSET:
            field_dict["start_ns"] = start_ns
        if end_ns is not UNSET:
            field_dict["end_ns"] = end_ns
        if window_ns is not UNSET:
            field_dict["window_ns"] = window_ns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        start_ns = d.pop("start_ns", UNSET)

        end_ns = d.pop("end_ns", UNSET)

        window_ns = d.pop("window_ns", UNSET)

        create_retained_subscription_body = cls(
            start_ns=start_ns,
            end_ns=end_ns,
            window_ns=window_ns,
        )

        return create_retained_subscription_body
