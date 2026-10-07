from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chat_gpt_outcome_status import ChatGPTOutcomeStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="ChatGPTOutcome")


@_attrs_define
class ChatGPTOutcome:
    """
    Attributes:
        status (ChatGPTOutcomeStatus):
        connection_id (str | Unset):
        error (str | Unset):
    """

    status: ChatGPTOutcomeStatus
    connection_id: str | Unset = UNSET
    error: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        connection_id = self.connection_id

        error = self.error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
            }
        )
        if connection_id is not UNSET:
            field_dict["connection_id"] = connection_id
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = ChatGPTOutcomeStatus(d.pop("status"))

        connection_id = d.pop("connection_id", UNSET)

        error = d.pop("error", UNSET)

        chat_gpt_outcome = cls(
            status=status,
            connection_id=connection_id,
            error=error,
        )

        chat_gpt_outcome.additional_properties = d
        return chat_gpt_outcome

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
