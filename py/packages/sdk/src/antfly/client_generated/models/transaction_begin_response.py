from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TransactionBeginResponse")


@_attrs_define
class TransactionBeginResponse:
    """
    Attributes:
        transaction_id (str):
        begin_timestamp (int):
        sync_level (str):
    """

    transaction_id: str
    begin_timestamp: int
    sync_level: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        transaction_id = self.transaction_id

        begin_timestamp = self.begin_timestamp

        sync_level = self.sync_level

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "transaction_id": transaction_id,
                "begin_timestamp": begin_timestamp,
                "sync_level": sync_level,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        transaction_id = d.pop("transaction_id")

        begin_timestamp = d.pop("begin_timestamp")

        sync_level = d.pop("sync_level")

        transaction_begin_response = cls(
            transaction_id=transaction_id,
            begin_timestamp=begin_timestamp,
            sync_level=sync_level,
        )

        transaction_begin_response.additional_properties = d
        return transaction_begin_response

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
