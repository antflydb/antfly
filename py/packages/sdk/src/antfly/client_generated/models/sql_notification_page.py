from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_notification import SQLNotification


T = TypeVar("T", bound="SQLNotificationPage")


@_attrs_define
class SQLNotificationPage:
    """
    Attributes:
        notifications (list[SQLNotification]):
        pending (bool): More queue work remains, or a committed notification is waiting for its visibility barrier. Poll
            again without replaying any SQL.
        cursor (str | Unset): Opaque connection-bound acknowledgement cursor for this page.
    """

    notifications: list[SQLNotification]
    pending: bool
    cursor: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        notifications = []
        for notifications_item_data in self.notifications:
            notifications_item = notifications_item_data.to_dict()
            notifications.append(notifications_item)

        pending = self.pending

        cursor = self.cursor

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "notifications": notifications,
                "pending": pending,
            }
        )
        if cursor is not UNSET:
            field_dict["cursor"] = cursor

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_notification import SQLNotification

        d = dict(src_dict)
        notifications = []
        _notifications = d.pop("notifications")
        for notifications_item_data in _notifications:
            notifications_item = SQLNotification.from_dict(notifications_item_data)

            notifications.append(notifications_item)

        pending = d.pop("pending")

        cursor = d.pop("cursor", UNSET)

        sql_notification_page = cls(
            notifications=notifications,
            pending=pending,
            cursor=cursor,
        )

        return sql_notification_page
