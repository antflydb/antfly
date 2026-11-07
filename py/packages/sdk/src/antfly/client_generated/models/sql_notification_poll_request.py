from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLNotificationPollRequest")


@_attrs_define
class SQLNotificationPollRequest:
    """
    Attributes:
        acknowledge (str | Unset): Cursor from a previously received page. Acknowledge only after consuming every
            notification in that page; omitted acknowledgements replay the same unconsumed prefix.
        limit (int | Unset):  Default: 128.
    """

    acknowledge: str | Unset = UNSET
    limit: int | Unset = 128

    def to_dict(self) -> dict[str, Any]:
        acknowledge = self.acknowledge

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if acknowledge is not UNSET:
            field_dict["acknowledge"] = acknowledge
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        acknowledge = d.pop("acknowledge", UNSET)

        limit = d.pop("limit", UNSET)

        sql_notification_poll_request = cls(
            acknowledge=acknowledge,
            limit=limit,
        )

        return sql_notification_poll_request
