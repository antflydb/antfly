from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TransactionSavepointResponse")


@_attrs_define
class TransactionSavepointResponse:
    """
    Attributes:
        status (str):
        transaction_id (str):
        savepoint_id (int):
    """

    status: str
    transaction_id: str
    savepoint_id: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status

        transaction_id = self.transaction_id

        savepoint_id = self.savepoint_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
                "transaction_id": transaction_id,
                "savepoint_id": savepoint_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = d.pop("status")

        transaction_id = d.pop("transaction_id")

        savepoint_id = d.pop("savepoint_id")

        transaction_savepoint_response = cls(
            status=status,
            transaction_id=transaction_id,
            savepoint_id=savepoint_id,
        )

        transaction_savepoint_response.additional_properties = d
        return transaction_savepoint_response

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
