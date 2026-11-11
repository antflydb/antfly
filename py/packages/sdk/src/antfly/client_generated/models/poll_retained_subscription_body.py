from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="PollRetainedSubscriptionBody")


@_attrs_define
class PollRetainedSubscriptionBody:
    """
    Attributes:
        cursor (str):
    """

    cursor: str

    def to_dict(self) -> dict[str, Any]:
        cursor = self.cursor

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "cursor": cursor,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        cursor = d.pop("cursor")

        poll_retained_subscription_body = cls(
            cursor=cursor,
        )

        return poll_retained_subscription_body
