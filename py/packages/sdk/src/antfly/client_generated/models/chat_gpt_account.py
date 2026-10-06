from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ChatGPTAccount")


@_attrs_define
class ChatGPTAccount:
    """
    Attributes:
        connection_id (str):
        email (str):
        label (str):
        connected (bool):
        plan_enabled (bool):
    """

    connection_id: str
    email: str
    label: str
    connected: bool
    plan_enabled: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        connection_id = self.connection_id

        email = self.email

        label = self.label

        connected = self.connected

        plan_enabled = self.plan_enabled

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connection_id": connection_id,
                "email": email,
                "label": label,
                "connected": connected,
                "plan_enabled": plan_enabled,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        connection_id = d.pop("connection_id")

        email = d.pop("email")

        label = d.pop("label")

        connected = d.pop("connected")

        plan_enabled = d.pop("plan_enabled")

        chat_gpt_account = cls(
            connection_id=connection_id,
            email=email,
            label=label,
            connected=connected,
            plan_enabled=plan_enabled,
        )

        chat_gpt_account.additional_properties = d
        return chat_gpt_account

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
