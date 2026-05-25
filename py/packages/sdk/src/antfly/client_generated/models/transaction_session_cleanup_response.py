from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TransactionSessionCleanupResponse")


@_attrs_define
class TransactionSessionCleanupResponse:
    """
    Attributes:
        removed (int | Unset):
        cutoff_ns (int | Unset):
    """

    removed: int | Unset = UNSET
    cutoff_ns: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        removed = self.removed

        cutoff_ns = self.cutoff_ns

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if removed is not UNSET:
            field_dict["removed"] = removed
        if cutoff_ns is not UNSET:
            field_dict["cutoff_ns"] = cutoff_ns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        removed = d.pop("removed", UNSET)

        cutoff_ns = d.pop("cutoff_ns", UNSET)

        transaction_session_cleanup_response = cls(
            removed=removed,
            cutoff_ns=cutoff_ns,
        )

        transaction_session_cleanup_response.additional_properties = d
        return transaction_session_cleanup_response

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
