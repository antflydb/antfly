from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLNotification")


@_attrs_define
class SQLNotification:
    """
    Attributes:
        channel (str):
        payload (str): UTF-8 payload, fewer than 8000 bytes.
        sender_pid (int): Sender backend identifier; equals pgwire BackendKeyData for wire clients.
    """

    channel: str
    payload: str
    sender_pid: int

    def to_dict(self) -> dict[str, Any]:
        channel = self.channel

        payload = self.payload

        sender_pid = self.sender_pid

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "channel": channel,
                "payload": payload,
                "sender_pid": sender_pid,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        channel = d.pop("channel")

        payload = d.pop("payload")

        sender_pid = d.pop("sender_pid")

        sql_notification = cls(
            channel=channel,
            payload=payload,
            sender_pid=sender_pid,
        )

        return sql_notification
