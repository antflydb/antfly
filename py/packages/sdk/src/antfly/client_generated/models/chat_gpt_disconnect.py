from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ChatGPTDisconnect")


@_attrs_define
class ChatGPTDisconnect:
    """
    Attributes:
        revocation_confirmed (bool):
    """

    revocation_confirmed: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        revocation_confirmed = self.revocation_confirmed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "revocation_confirmed": revocation_confirmed,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        revocation_confirmed = d.pop("revocation_confirmed")

        chat_gpt_disconnect = cls(
            revocation_confirmed=revocation_confirmed,
        )

        chat_gpt_disconnect.additional_properties = d
        return chat_gpt_disconnect

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
