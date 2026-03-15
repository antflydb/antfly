from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TransactionCommitResponseConflict")


@_attrs_define
class TransactionCommitResponseConflict:
    """Details about the conflict that caused an abort (only present when status is "aborted")

    Attributes:
        table (Union[Unset, str]): Table where the conflict was detected
        key (Union[Unset, str]): Key that had a version mismatch
        message (Union[Unset, str]): Human-readable conflict description
    """

    table: Union[Unset, str] = UNSET
    key: Union[Unset, str] = UNSET
    message: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        key = self.key

        message = self.message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if table is not UNSET:
            field_dict["table"] = table
        if key is not UNSET:
            field_dict["key"] = key
        if message is not UNSET:
            field_dict["message"] = message

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table", UNSET)

        key = d.pop("key", UNSET)

        message = d.pop("message", UNSET)

        transaction_commit_response_conflict = cls(
            table=table,
            key=key,
            message=message,
        )

        transaction_commit_response_conflict.additional_properties = d
        return transaction_commit_response_conflict

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
