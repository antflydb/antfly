from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ChatGPTBegin")


@_attrs_define
class ChatGPTBegin:
    """
    Attributes:
        attempt_id (str):
        authorization_url (str):
        expires_at (int):
    """

    attempt_id: str
    authorization_url: str
    expires_at: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        attempt_id = self.attempt_id

        authorization_url = self.authorization_url

        expires_at = self.expires_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "attempt_id": attempt_id,
                "authorization_url": authorization_url,
                "expires_at": expires_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        attempt_id = d.pop("attempt_id")

        authorization_url = d.pop("authorization_url")

        expires_at = d.pop("expires_at")

        chat_gpt_begin = cls(
            attempt_id=attempt_id,
            authorization_url=authorization_url,
            expires_at=expires_at,
        )

        chat_gpt_begin.additional_properties = d
        return chat_gpt_begin

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
